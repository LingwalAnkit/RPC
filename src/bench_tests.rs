//! Loopback measurement of the typed echo call: sequential latency and throughput
//! on one connection. Ignored by default, so `cargo test` does not run it.
//!
//! Run with:
//!   cargo test --release bench_loopback -- --ignored --nocapture

use std::time::Instant;

use quinn::Endpoint;
use tokio_util::sync::CancellationToken;

use crate::{
    codec::EchoRequest,
    echo_service::{EchoClient, dispatch},
    rpc::{Client, serve},
    tls::DevCA,
};

const WARMUP_CALLS: usize = 500;
const MEASURED_CALLS: usize = 10_000;
const BODY_BYTES: usize = 64;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn bench_loopback() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let ca = DevCA::new()?;
    let server_ep = Endpoint::server(
        ca.server_config(ca.issue("server.rpc")?)?,
        "127.0.0.1:0".parse()?,
    )?;
    let addr = server_ep.local_addr()?;
    let stop = CancellationToken::new();
    let server = tokio::spawn(serve(server_ep, dispatch, stop.clone()));

    let mut client_ep = Endpoint::client("127.0.0.1:0".parse()?)?;
    client_ep.set_default_client_config(ca.client_config(ca.issue("client-1")?)?);
    let client = Client::new(client_ep.connect(addr, "server.rpc")?.await?);
    let echo = EchoClient::new(client);
    let text = "x".repeat(BODY_BYTES);

    for _ in 0..WARMUP_CALLS {
        echo.clone()
            .echo(EchoRequest { text: text.clone() }, None)
            .await?;
    }

    let mut latencies = Vec::with_capacity(MEASURED_CALLS);
    let started = Instant::now();
    for _ in 0..MEASURED_CALLS {
        let call_start = Instant::now();
        let reply = echo
            .clone()
            .echo(EchoRequest { text: text.clone() }, None)
            .await?;
        latencies.push(call_start.elapsed());
        anyhow::ensure!(reply.text == text, "reply did not match the request");
    }
    let total = started.elapsed();

    latencies.sort();
    let pct = |p: f64| latencies[((latencies.len() as f64 * p) as usize).min(latencies.len() - 1)];
    let mean_us =
        latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / latencies.len() as f64 * 1e6;

    println!(
        "BENCH loopback sequential echo: calls={MEASURED_CALLS} body={BODY_BYTES}B \
         calls_per_sec={:.0} mean={:.0}us p50={:.0}us p99={:.0}us",
        MEASURED_CALLS as f64 / total.as_secs_f64(),
        mean_us,
        pct(0.50).as_secs_f64() * 1e6,
        pct(0.99).as_secs_f64() * 1e6,
    );

    stop.cancel();
    server.await?;
    Ok(())
}
