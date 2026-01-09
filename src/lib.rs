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
