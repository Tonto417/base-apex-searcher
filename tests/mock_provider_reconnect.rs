use apex_searcher::backoff;
use futures_util::{Stream, StreamExt};
use prometheus::IntCounter;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::Instant;

type StreamType = Pin<Box<dyn Stream<Item = Result<f64, ()>> + Send>>;

#[tokio::test]
async fn subscribe_retries_then_processes_stream() {
    let subscribe_calls = AtomicU32::new(0);

    // subscribe_op: fails twice, then returns a stream with three price updates (one decode error in middle)
    let subscribe_op = || {
        let c = &subscribe_calls;
        async move {
            let n = c.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(())
            } else {
                let items = vec![Ok(1.23), Err(()), Ok(2.34)];
                let s: StreamType = Box::pin(futures_util::stream::iter(items));
                Ok(s)
            }
        }
    };

    let base = Duration::from_millis(5);
    let max = Duration::from_millis(100);

    let start = Instant::now();
    // try up to 10 attempts
    let res = backoff::retry_async_with_backoff(subscribe_op, base, max, 0, Some(10)).await;
    let elapsed = start.elapsed();

    assert!(res.is_ok());
    // two failed subscribes -> sleeps of 5 + 10 ms at minimum
    assert!(elapsed >= Duration::from_millis(15));

    let mut stream = res.unwrap();

    // metrics counter to record processed updates
    let counter = IntCounter::new("test_updates_total", "total").unwrap();

    while let Some(item) = stream.next().await {
        match item {
            Ok(price) => {
                // process price (just increment counter)
                counter.inc();
                println!("Processed price {}", price);
            }
            Err(_) => {
                // simulate decode error: continue processing
                println!("Decode error encountered; continuing");
                continue;
            }
        }
    }

    // We expect two successful prices
    assert_eq!(counter.get(), 2);
}

#[tokio::test]
async fn subscribe_exhausts_attempts() {
    // subscribe_op always fails
    let subscribe_op = || async { Err::<StreamType, ()>(()) };

    let base = Duration::from_millis(5);
    let max = Duration::from_millis(100);

    let start = Instant::now();
    let res = backoff::retry_async_with_backoff(subscribe_op, base, max, 0, Some(3)).await;
    let elapsed = start.elapsed();

    assert!(res.is_err());
    // two sleeps should have occurred: 5 + 10 ms = 15 ms
    assert!(elapsed >= Duration::from_millis(15));
}