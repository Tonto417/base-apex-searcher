use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use std::future::Future;

/// Compute a backoff delay with exponential growth capped by `max` and optional jitter (percentage 0-100).
pub fn backoff_delay(attempt: u32, base: Duration, max: Duration, jitter_pct: u8) -> Duration {
    if attempt == 0 {
        return base.min(max);
    }

    let base_ms = base.as_millis() as u128;
    let max_ms = max.as_millis() as u128;
    let mut exp_ms = base_ms;
    for _ in 1..=attempt.saturating_sub(1) {
        exp_ms = exp_ms.saturating_mul(2);
        if exp_ms >= max_ms {
            exp_ms = max_ms;
            break;
        }
    }

    // jitter +/- jitter_pct%
    let now_nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u128;
    let mut x = now_nanos ^ (attempt as u128).wrapping_mul(0x9E3779B97F4A7C15u128);
    // xorshift
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;

    let jitter_range = if jitter_pct == 0 { 0 } else { (exp_ms * (jitter_pct as u128)) / 100 };
    let rand = if jitter_range == 0 { 0 } else { (x % (2 * jitter_range + 1)) as i128 };
    let min_ms = exp_ms as i128 - jitter_range as i128;
    let jittered_ms = (min_ms + rand) as i128;
    let jittered_ms = jittered_ms.max(1) as u128;

    Duration::from_millis(jittered_ms.min(max_ms) as u64)
}

/// Retry an async operation with the backoff policy. If `max_attempts` is `Some(n)`, it will return
/// the last error after `n` failed attempts. Otherwise it will retry forever.
pub async fn retry_async_with_backoff<F, Fut, T, E>(mut op: F, base: Duration, max: Duration, jitter_pct: u8, max_attempts: Option<u32>) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt: u32 = 1;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if let Some(max) = max_attempts {
                    if attempt >= max {
                        return Err(e);
                    }
                }
                let delay = backoff_delay(attempt, base, max, jitter_pct);
                sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[tokio::test]
    async fn retry_succeeds_after_retries() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = AtomicU32::new(0);
        let base = Duration::from_millis(10);
        let max = Duration::from_millis(100);

        let start = Instant::now();
        let res = retry_async_with_backoff(
            || {
                let c = &calls;
                async move {
                    let n = c.fetch_add(1, Ordering::SeqCst);
                    if n >= 2 {
                        Ok(42)
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
        // We expect at least two backoff sleeps: attempt 1 -> sleep 10ms, attempt 2 -> sleep 20ms => >=30ms
        assert!(elapsed >= Duration::from_millis(30));
    }

    #[tokio::test]
    async fn retry_exhausts_attempts() {
        let base = Duration::from_millis(5);
        let max = Duration::from_millis(50);

        let start = Instant::now();
        let res: Result<(), ()> = retry_async_with_backoff(
            || async { Err(()) },
            base,
            max,
            0,
            Some(3),
        )
        .await;
        let elapsed = start.elapsed();
        assert!(res.is_err());
        // two sleeps should have occurred: 5ms + 10ms = 15ms
        assert!(elapsed >= Duration::from_millis(15));
    }
}