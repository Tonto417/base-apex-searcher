use apex_searcher::backoff;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test]
async fn retry_helper_respects_attempts_and_timing() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);

    let base = Duration::from_millis(5);
    let max = Duration::from_millis(100);

    let start = Instant::now();
    let res = backoff::retry_async_with_backoff(
        || {
            let c = &calls;
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n >= 3 {
                    Ok(())
                } else {
                    Err(())
                }
            }
        },
        base,
        max,
        0,
        Some(10),
    )
    .await;

    let elapsed = start.elapsed();
    assert!(res.is_ok());
    // minimum sleeps: 5 + 10 + 20 = 35ms
    assert!(elapsed >= Duration::from_millis(35));
}

#[tokio::test]
async fn retry_helper_exhausts_max_attempts() {
    let base = Duration::from_millis(5);
    let max = Duration::from_millis(100);

    let start = Instant::now();
    let res: Result<(), ()> = backoff::retry_async_with_backoff(|| async { Err(()) }, base, max, 0, Some(3)).await;
    let elapsed = start.elapsed();

    assert!(res.is_err());
    // should have slept twice: 5 + 10 = 15ms
    assert!(elapsed >= Duration::from_millis(15));
}