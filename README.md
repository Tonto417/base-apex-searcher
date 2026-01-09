# base-apex-searcher

A small, high-speed log watcher that connects to an Ethereum-compatible
RPC WebSocket endpoint and monitors `Sync` events (Uniswap-like pool syncs).
It computes a simple "shadow price" (reserve1 / reserve0), logs it with
latency, and optionally exposes Prometheus metrics.

## Quick start

1) Build:

   cargo build --release

2) Run (example):

   # Set RPC URL (or pass --rpc-url)
   $env:RPC_URL = "wss://your-wss-endpoint"
   cargo run --release -- --log-level info --metrics-addr 127.0.0.1:9000

3) Observability:

   - Logs use `tracing` and respect `--log-level` or `RUST_LOG`.
   - If `--metrics-addr` is set, Prometheus metrics are exposed at:
     - `http://<metrics-addr>/metrics`
     - `http://<metrics-addr>/healthz`

## Configuration

- `--rpc-url` or `RPC_URL` environment variable: WebSocket RPC endpoint
- `--log-level` or `LOG_LEVEL` env var: logging verbosity (default: info)
- `--metrics-addr` or `METRICS_ADDR` env var: optional metrics bind address

## Development

- Run tests: `cargo test`
- Format: `cargo fmt`
- Lint: `cargo clippy`

## Next steps

- Add integration tests that run a simulated provider stream
- Add more telemetry (gauges, summary percentiles)
- Add `--dry-run` mode to simulate incoming events locally

---

If you'd like, I can add a simulated log generator with a `--simulate` flag next.
