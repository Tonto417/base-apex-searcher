Title: chore: add CLI, logging, metrics, tests & CI

Summary
-------
This PR adds CLI configuration, structured logging, optional Prometheus metrics exposure, unit and integration tests, and a CI workflow. The change moves some helpers into a library for easier testing and wires a small, robust metrics endpoint (using tiny_http) to avoid version conflicts in hyper.

What changed
------------
- Added CLI parsing (clap) and `Config` handling for `--rpc-url`, `--metrics-addr`, and `--log-level`.
- Structured logs using `tracing` and `tracing-subscriber`.
- Implemented event signature handling (hex or keccak of ABI signature) and `build_filter()` helper.
- Added Prometheus metrics: update counter and latency histogram; small background metrics server using `tiny_http` and `/healthz` endpoint (no server in test runs).
- Moved helper functions into `src/lib.rs` to facilitate testing (signature decoding, filter builder, compute helpers, metrics helpers).
- Added unit tests for core helpers and an integration test `tests/integration_metrics.rs` that starts metrics on an ephemeral port and asserts exposure.
- Added a GitHub Actions workflow `.github/workflows/ci.yml` to run `cargo fmt`, `cargo clippy`, build and test.
- Updated `README.md` with run instructions and metrics notes.

Files of note
-------------
- src/main.rs — CLI, runtime loop, logging, metrics initialization, event processing
- src/lib.rs  — helpers: `decode_signature`, `build_filter`, `compute_shadow_price`, `record_metrics`, `init_metrics` (test/no-op variant)
- tests/integration_metrics.rs — integration test for metrics endpoint
- .github/workflows/ci.yml — CI steps
- Cargo.toml — added dependencies (clap, tiny-keccak, prometheus, tiny_http, tracing, tracing-subscriber, etc.)

Testing & verification
----------------------
Local steps to reproduce:
1. Run unit + integration tests: `cargo test -- --nocapture` ✅
2. Run linter/format checks:
   - `cargo fmt` ✅
   - `cargo clippy --all-targets -- -D warnings` ✅
3. To test metrics manually: run `cargo run -- --rpc-url <wss> --metrics-addr 127.0.0.1:9000` and GET `http://127.0.0.1:9000/metrics`.

Notes / follow-ups
------------------
- Graceful shutdown for the metrics thread is planned (todo: return a shutdown handle and join on `ctrl-c`).
- Add additional useful metrics (error counter, last price gauge) and more integration tests for end-to-end processing.

Merge checklist
---------------
- [ ] All tests pass in CI
- [ ] Clippy & format enforced in CI
- [ ] README updated (basic run instructions included)

Branch
------
Branch: `feature/metrics-ci`

How to push & create PR (if you haven’t already added a remote):

1. Add remote (replace <git-url>):
   ```bash
   git remote add origin <git-url>
   git push -u origin feature/metrics-ci
   ```
2. Create PR with GitHub CLI:
   ```bash
   gh pr create --fill --title "chore: add CLI, logging, metrics, tests & CI"
   ```

