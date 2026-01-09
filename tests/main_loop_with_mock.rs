use apex_searcher::run_loop_with_subscribe_op;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::Instant;

type MockStream = Pin<Box<dyn futures_util::stream::Stream<Item = Result<u64, ()>> + Send>>;

#[tokio::test]
async fn main_loop_handles_subscribe_failures_then_processes() {
    // Simulate subscribe failing twice, then returning a stream of items (some errors inside)
    let calls = AtomicU32::new(0);

    let subscribe_op = || {
        let c = &calls;
        async move {
            let n = c.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(())
            } else {
                let items = vec![Ok(10u64), Err(()), Ok(20u64)];
                let s: MockStream = Box::pin(futures_util::stream::iter(items));
                Ok(s)
            }
        }
    };

    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counter_clone = counter.clone();
    let process_item = move |v: u64| {
        // simple processor: increment processed counter on success
        let _ = v; // use it to avoid warnings
        counter_clone.fetch_add(1, Ordering::SeqCst);
    };

    let base = Duration::from_millis(5);
    let max = Duration::from_millis(100);

    let start = Instant::now();
    // Run the loop but limit subscribe attempts to 10 to avoid infinite retry
    let run = run_loop_with_subscribe_op(subscribe_op, process_item, base, max, 0, Some(10));

    // Run the loop in a background task and stop it shortly after we expect processing done
    let handle = tokio::spawn(async move {
        // run until it returns (it shouldn't in normal operation), but we expect to at least process the stream once
        let _ = tokio::time::timeout(Duration::from_millis(200), run).await;
    });

    // Wait a bit for retries + processing
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Expect two successful processed items (10 and 20)
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    // Ensure subscribe was called at least 3 times (2 fails + 1 success)
    assert!(calls.load(Ordering::SeqCst) >= 3);

    let _ = handle.await;
}

#[tokio::test]
async fn main_loop_exhausts_subscribe_attempts() {
    // subscribe always fails
    let subscribe_op = || async { Err::<MockStream, ()>(()) };

    let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counter_clone = counter.clone();
    let process_item = move |_v: u64| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    };

    let base = Duration::from_millis(5);
    let max = Duration::from_millis(100);

    let start = Instant::now();
    let res = run_loop_with_subscribe_op(subscribe_op, process_item, base, max, 0, Some(3)).await;
    let elapsed = start.elapsed();

    assert!(res.is_err());
    // two sleeps should have occurred: 5 + 10 ms = 15 ms
    assert!(elapsed >= Duration::from_millis(15));
}
