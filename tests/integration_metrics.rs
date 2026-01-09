use std::net::TcpListener;
use std::time::Duration;

use apex_searcher::{init_metrics, record_metrics};

#[test]
fn metrics_endpoint_exposes_updates() {
    // pick an ephemeral port
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let addr = format!("127.0.0.1:{}", port).parse().unwrap();

    // start metrics server
    let metrics = init_metrics(Some(addr)).expect("metrics should start");

    // record a single metric
    record_metrics(&metrics, 12.34);

    // give the server a moment to start
    std::thread::sleep(Duration::from_millis(100));

    // fetch the /metrics endpoint
    let url = format!("http://127.0.0.1:{}/metrics", port);
    let body = reqwest::blocking::get(&url).expect("get metrics").text().expect("body");

    assert!(body.contains("apex_price_updates_total"), "metrics endpoint should contain counter");
}
