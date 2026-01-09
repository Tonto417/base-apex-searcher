use std::{net::SocketAddr};

use alloy::rpc::types::Filter;
use eyre::WrapErr;
use prometheus::{IntCounter, HistogramVec};

use alloy_sol_types::SolEvent;



// Reuse the Sync event definition here so helpers which rely on it can compile
use alloy::sol;
sol! {
    #[sol(rpc)]
    event Sync(uint112 reserve0, uint112 reserve1);
}

/// If `Sync::SIGNATURE` is hex bytes, decode to 32 bytes; otherwise compute keccak256 of an ABI signature
pub fn decode_signature() -> eyre::Result<[u8; 32]> {
    let sig = Sync::SIGNATURE;

    if sig.starts_with("0x") || sig.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut s = sig.trim_start_matches("0x").to_string();
        if s.len() % 2 == 1 {
            s = format!("0{}", s);
        }
        let bytes = hex::decode(&s).wrap_err("decoding Sync::SIGNATURE hex string")?;
        let arr: [u8; 32] = bytes.as_slice().try_into().wrap_err("Sync::SIGNATURE must be 32 bytes")?;
        return Ok(arr);
    }

    // Otherwise compute keccak256
    use tiny_keccak::{Keccak, Hasher};
    let mut hasher = Keccak::v256();
    hasher.update(sig.as_bytes());
    let mut out = [0u8; 32];
    hasher.finalize(&mut out);
    Ok(out)
}

// Expose backoff helpers for reuse and testing
pub mod backoff;

use futures_util::StreamExt;

/// Public test helper to run a reconnection loop using an abstract subscribe operation.
/// This is intentionally public so integration tests can call it. It is generic over
/// the subscribe operation and stream item types to support mocks.
pub async fn run_loop_with_subscribe_op<F, Fut, S, T, E, Proc>(
    mut subscribe_op: F,
    mut process_item: Proc,
    base_backoff: std::time::Duration,
    max_backoff: std::time::Duration,
    jitter_pct: u8,
    max_subscribe_attempts: Option<u32>,
) -> eyre::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<S, E>>,
    S: futures_util::stream::Stream<Item = Result<T, E>> + Send + 'static + Unpin,
    Proc: FnMut(T) + Send + 'static,
    E: std::fmt::Debug,
{
    loop {
        // Try to subscribe with backoff
        let sub_res = crate::backoff::retry_async_with_backoff(
            || subscribe_op(),
            base_backoff,
            max_backoff,
            jitter_pct,
            max_subscribe_attempts,
        )
        .await;

        let mut stream = match sub_res {
            Ok(s) => s,
            Err(_) => {
                // Exhausted subscribe attempts
                return Err(eyre::eyre!("subscribe failed after retries"));
            }
        };

        // Process the stream until it ends or yields an error
        while let Some(item) = stream.next().await {
            match item {
                Ok(t) => process_item(t),
                Err(_e) => {
                    // Treat item errors as decode failures; stop processing and reconnect
                    break;
                }
            }
        }

        // Continue the reconnection loop
    }
}

/// Lightweight provider trait used for testing. Implement this for mock providers
/// to exercise `run_loop_with_provider_factory` in an integration style test.
pub trait ProviderLike {
    type Item;
    type Error: std::fmt::Debug;
    type Stream: futures_util::stream::Stream<Item = Result<Self::Item, Self::Error>> + Send + Unpin + 'static;

    fn subscribe_logs(&self) -> Result<Self::Stream, Self::Error>;
}

/// Run a reconnection loop that obtains providers via `factory` (async), calls
/// `subscribe_logs` on the provider, and processes items with `process_item`.
/// This is useful to test `main`-style logic with an injected mock provider.
pub async fn run_loop_with_provider_factory<F, Fut, P, Proc>(
    mut factory: F,
    mut process_item: Proc,
    base_backoff: std::time::Duration,
    max_backoff: std::time::Duration,
    jitter_pct: u8,
    max_subscribe_attempts: Option<u32>,
) -> eyre::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<P, ()>>,
    P: ProviderLike,
    Proc: FnMut(P::Item) + Send + 'static,
{
    loop {
        // Try to obtain a provider with retry/backoff
        let prov_res = crate::backoff::retry_async_with_backoff(
            || factory(),
            base_backoff,
            max_backoff,
            jitter_pct,
            max_subscribe_attempts,
        )
        .await;

        let provider = match prov_res {
            Ok(p) => p,
            Err(_) => return Err(eyre::eyre!("failed to obtain provider after retries")),
        };

        // Now subscribe via the provider
        let sub_res = provider.subscribe_logs();
        let mut stream = match sub_res {
            Ok(s) => s,
            Err(_) => {
                // subscription failed; reconnect
                continue;
            }
        };

        while let Some(item) = stream.next().await {
            match item {
                Ok(t) => process_item(t),
                Err(_) => break, // decode error -> reconnect
            }
        }

        // continue loop and reconnect
    }
}
pub fn build_filter() -> eyre::Result<Filter> {
    let sig_arr = decode_signature()?;
    Ok(Filter::new().event_signature(sig_arr))
}

/// Compute shadow price given reserves. Returns None when reserve0 == 0.
pub fn compute_shadow_price(reserve0: u128, reserve1: u128) -> Option<f64> {
    let r0 = reserve0 as f64;
    let r1 = reserve1 as f64;
    if r0 > 0.0 {
        Some(r1 / r0)
    } else {
        None
    }
}

/// Record metrics helper (counter + latency histogram). Latency is in ms.
pub fn record_metrics(metrics: &(IntCounter, HistogramVec), latency_ms: f64) {
    let (counter, latency_hist) = metrics;
    counter.inc();
    latency_hist.with_label_values(&["price_update"]).observe(latency_ms);
}

#[cfg(not(test))]
pub fn init_metrics(addr: Option<SocketAddr>) -> Option<(IntCounter, HistogramVec)> {
    if let Some(bind_addr) = addr {
        use prometheus::{Encoder, TextEncoder, register_int_counter, HistogramOpts};

        // Register metrics
        let update_counter = register_int_counter!("apex_price_updates_total", "Total price updates processed").ok()?;
        let opts = HistogramOpts::new("apex_price_latency_ms", "Processing latency (ms)")
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0]);
        let latency_hist = HistogramVec::new(opts, &["handler"]).ok()?;
        prometheus::default_registry().register(Box::new(latency_hist.clone())).ok()?;

        // Spawn a tiny blocking HTTP server in a background thread to serve /metrics and /healthz
        let bind_str = bind_addr.to_string();
        std::thread::spawn(move || {
            let server = tiny_http::Server::http(&bind_str).expect("metrics bind failed");
            for request in server.incoming_requests() {
                match request.url() {
                    "/metrics" => {
                        let encoder = TextEncoder::new();
                        let metric_families = prometheus::gather();
                        let mut buffer = Vec::new();
                        encoder.encode(&metric_families, &mut buffer).unwrap_or_default();
                        let response = tiny_http::Response::from_string(String::from_utf8_lossy(&buffer).to_string())
                            .with_status_code(200);
                        let _ = request.respond(response);
                    }
                    "/healthz" => {
                        let response = tiny_http::Response::from_string("ok")
                            .with_status_code(200);
                        let _ = request.respond(response);
                    }
                    _ => {
                        let response = tiny_http::Response::from_string("not found").with_status_code(404);
                        let _ = request.respond(response);
                    }
                }
            }
        });

        Some((update_counter, latency_hist))
    } else {
        None
    }
}

#[cfg(test)]
pub fn init_metrics(_addr: Option<SocketAddr>) -> Option<(IntCounter, HistogramVec)> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{IntCounter, HistogramOpts, HistogramVec};

    #[test]
    fn test_compute_shadow_price_nonzero() {
        let p = compute_shadow_price(10u128, 25u128);
        assert!(p.is_some());
        let v = p.unwrap();
        assert!((v - 2.5).abs() < 1e-12);
    }

    #[test]
    fn test_compute_shadow_price_zero() {
        let p = compute_shadow_price(0u128, 100u128);
        assert!(p.is_none());
    }

    #[test]
    fn test_decode_signature_len() {
        let sig = decode_signature().expect("decode_signature should succeed");
        assert_eq!(sig.len(), 32);
    }

    #[test]
    fn test_build_filter_ok() {
        let f = build_filter();
        assert!(f.is_ok());
    }

    #[test]
    fn test_record_metrics_increments_counter() {
        let counter = IntCounter::new("test_apex_price_updates_total", "test counter").unwrap();
        let opts = HistogramOpts::new("test_apex_price_latency_ms", "test latency");
        let hist = HistogramVec::new(opts, &["handler"]).unwrap();

        record_metrics(&(counter.clone(), hist), 12.3);
        assert_eq!(counter.get(), 1);
    }
}
