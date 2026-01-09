use std::{net::SocketAddr, time::Instant};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use clap::Parser;
use eyre::WrapErr;
use futures_util::StreamExt;
use tracing::info;
use tracing_subscriber::EnvFilter;

// I've kept YOUR CLI structure here
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[arg(long)]
    rpc_url: Option<String>,
    #[arg(long, default_value = "info")]
    log_level: String,
    #[arg(long)]
    metrics_addr: Option<SocketAddr>,
}


#[tokio::main]
async fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt().with_env_filter(EnvFilter::new(&cli.log_level)).init();

    // Loading RPC URL from Env or CLI (Your logic)
    let rpc_url = cli.rpc_url.or_else(|| std::env::var("RPC_URL").ok())
        .ok_or_else(|| eyre::eyre!("RPC_URL missing"))?;

    use apex_searcher::{Sync, build_filter, compute_shadow_price, record_metrics, init_metrics};

    info!("🚀 APEX SEARCHER LIVE | MODE: OPTIMIZED SYNC");

    // Optional metrics server (no-op in tests)
    let metrics = init_metrics(cli.metrics_addr);

    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(&rpc_url))
        .await
        .wrap_err("WS Connection Failed")?;

    // Build filter (handles decoding/signature formats) and subscribe
    let filter = build_filter()?;
    let sub = provider.subscribe_logs(&filter).await?;
    let mut stream = sub.into_stream();

    while let Some(log) = stream.next().await {
        let start_time = Instant::now();

        // Decode and handle 'Sync' events
        if let Ok(decoded) = log.log_decode::<Sync>() && let Some(price) = compute_shadow_price(
            decoded.inner.data.reserve0.to::<u128>(),
            decoded.inner.data.reserve1.to::<u128>(),
        ) {
            let latency = start_time.elapsed().as_micros();

            info!(
                latency_us = latency,
                pool = %log.address(),
                price = %format!("{:.10}", price),
                "📈 MARKET SYNC"
            );

            if let Some((counter, latency_hist)) = &metrics {
                let latency_ms = (latency as f64) / 1000.0;
                record_metrics(&(counter.clone(), latency_hist.clone()), latency_ms);
            }
        }
    }
    Ok(())
}