use std::{net::SocketAddr, time::{Duration, Instant}};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use clap::Parser;
use eyre::WrapErr;
use futures_util::StreamExt;
use tracing::{info, warn, error};
use tracing_subscriber::EnvFilter;



// Keep your CLI struct
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[arg(long)]
    rpc_url: Option<String>,
    #[arg(long, default_value = "info")]
    log_level: String,
    #[arg(long)]
    metrics_addr: Option<SocketAddr>,
    /// Base backoff in milliseconds
    #[arg(long, default_value = "500")]
    backoff_base_ms: u64,

    /// Max backoff in milliseconds
    #[arg(long, default_value = "30000")]
    backoff_max_ms: u64,

    /// Jitter percentage (0-100)
    #[arg(long, default_value = "25")]
    backoff_jitter_pct: u8,
}

#[tokio::main]
#[allow(unreachable_code)]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt().with_env_filter(EnvFilter::new(&cli.log_level)).init();

    let rpc_url = cli.rpc_url.or_else(|| std::env::var("RPC_URL").ok())
        .ok_or_else(|| eyre::eyre!("RPC_URL missing"))?;

    // Import from your library module
    use apex_searcher::{Sync, build_filter, compute_shadow_price, record_metrics, init_metrics, backoff};

    info!("🚀 APEX SEARCHER LIVE");
    let metrics = init_metrics(cli.metrics_addr);

    // RECONNECTION LOOP
    // Backoff state for connect and subscribe attempts
    let mut connect_attempt: u32 = 0;
    let mut subscribe_attempt: u32 = 0;
    let base_backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(30);

    loop {
        // Compute backoff for connect attempts
        let provider = match ProviderBuilder::new()
            .connect_ws(WsConnect::new(&rpc_url))
            .await {
                Ok(p) => {
                    connect_attempt = 0; // reset on success
                    p
                }
                Err(e) => {
                    connect_attempt = connect_attempt.saturating_add(1);
                    let delay = backoff::backoff_delay(connect_attempt, base_backoff, max_backoff, cli.backoff_jitter_pct);
                    error!(error = %e, delay = ?delay, "WS connection failed; retrying");
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

        let filter = build_filter().wrap_err("building log filter")?;

        let sub = match provider.subscribe_logs(&filter).await {
            Ok(s) => {
                subscribe_attempt = 0; // reset on success
                s
            }
            Err(e) => {
                subscribe_attempt = subscribe_attempt.saturating_add(1);
                let delay = backoff::backoff_delay(subscribe_attempt, base_backoff, max_backoff, cli.backoff_jitter_pct);
                error!(error = %e, delay = ?delay, "Subscribe failed; retrying");
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        let mut stream = sub.into_stream();
        info!("📡 Connected and Streaming Logs...");

        // Process items until the stream returns None or Ctrl-C is received
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown requested");
                    return Ok(());
                }
                item = stream.next() => {
                    match item {
                        Some(log) => {
                            let start_time = Instant::now();
                            match log.log_decode::<Sync>() {
                                Ok(decoded) => {
                                    if let Some(price) = compute_shadow_price(
                                        decoded.inner.reserve0.to::<u128>(),
                                        decoded.inner.reserve1.to::<u128>(),
                                    ) {
                                        let latency = start_time.elapsed().as_micros();
                                        info!(latency_us = latency, pool = %log.address(), price = price, "📈 MARKET SYNC");

                                        if let Some(m) = &metrics {
                                            record_metrics(m, (latency as f64) / 1000.0);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "failed to decode log; continuing");
                                }
                            }
                        }
                        None => {
                            warn!("Stream ended; reconnecting");
                            break;
                        }
                    }
                }
            }
        }

        warn!("Stream disconnected. Reconnecting...");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}




#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn backoff_no_jitter_grows_exponentially_and_caps() {
        let base = Duration::from_millis(500);
        let max = Duration::from_millis(30000);

        // attempt 1 -> 500ms
        assert_eq!(apex_searcher::backoff::backoff_delay(1, base, max, 0), Duration::from_millis(500));
        // attempt 2 -> 1000ms
        assert_eq!(apex_searcher::backoff::backoff_delay(2, base, max, 0), Duration::from_millis(1000));
        // attempt 4 -> 4000ms
        assert_eq!(apex_searcher::backoff::backoff_delay(4, base, max, 0), Duration::from_millis(4000));

        // large attempt should cap at max
        assert_eq!(apex_searcher::backoff::backoff_delay(10, base, max, 0), max);
    }

    #[test]
    fn backoff_with_jitter_within_bounds() {
        let base = Duration::from_millis(500);
        let max = Duration::from_millis(30000);
        let j = 25u8;

        for attempt in 1..10 {
            let d = apex_searcher::backoff::backoff_delay(attempt, base, max, j);
            assert!(d >= Duration::from_millis(1));
            assert!(d <= max);
        }
    }
}