use apex_searcher::{ProviderLike, run_loop_with_provider_factory};
use futures_util::stream::{self, StreamExt};
use std::pin::Pin;
use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
use std::time::Duration;
use tokio::time::Instant;

type MockStream = Pin<Box<dyn futures_util::stream::Stream<Item = Result<u64, ()>> + Send>>;

#[derive(Clone)]
struct MockProvider {
    items: Vec<Result<u64, ()>>,
}

impl ProviderLike for MockProvider {
    type Item = u64;
    type Error = ();
    type Stream = MockStream;

    fn subscribe_logs(&self) -> Result<Self::Stream, Self::Error> {
        let s: MockStream = Box::pin(stream::iter(self.items.clone()));
        Ok(s)
    }
}

#[tokio::test]
async fn main_loop_with_mock_provider_retries_and_processes() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = attempts.clone();

    // factory: fails twice, then returns a provider with two OK items and one Err in the middle
    let factory = move || {
        let attempts = attempts_clone.clone();
        async move {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(())
            } else {
                Ok(MockProvider { items: vec![Ok(10), Err(()), Ok(20)] })
            }
        }
    };

    let processed = Arc::new(AtomicU32::new(0));
    let proc_clone = processed.clone();
    let processor = move |v: u64| { proc_clone.fetch_add(1, Ordering::SeqCst); let _ = v; };

    let start = Instant::now();
    // run but limit provider obtain attempts to 10
    let run = run_loop_with_provider_factory(factory, processor, Duration::from_millis(5), Duration::from_millis(100), 0, Some(10));
    let handle = tokio::spawn(async move { let _ = tokio::time::timeout(Duration::from_millis(200), run).await; });

    tokio::time::sleep(Duration::from_millis(80)).await;

    // Expect two successful processed items
    assert_eq!(processed.load(Ordering::SeqCst), 2);
    assert!(attempts.load(Ordering::SeqCst) >= 3);

    let _ = handle.await;
}

#[tokio::test]
async fn provider_factory_exhausts_attempts() {
    let factory = || async { Err::<MockProvider, ()>(()) };

    let processor = |_v: u64| {};

    let start = Instant::now();
    let res = run_loop_with_provider_factory(factory, processor, Duration::from_millis(5), Duration::from_millis(100), 0, Some(3)).await;
    let elapsed = start.elapsed();

    assert!(res.is_err());
    assert!(elapsed >= Duration::from_millis(15));
}