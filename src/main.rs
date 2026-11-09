mod codec;
mod echo_service;
mod frame;
mod rpc;
#[cfg(test)]
mod transport_tests;
mod tls;

use std::{net::SocketAddr, time::Duration};

use codec::EchoRequest;
use echo_service::{EchoClient, EchoResponse, dispatch};
use quinn::Endpoint;
use rpc::{Client, serve};
use tokio_util::sync::CancellationToken;

/// Demo: one typed echo call over QUIC with mutual TLS, on loopback.
/// Exits with an error unless the server echoes the text back unchanged.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let ca = tls::DevCA::new()?;

    let server_ep = Endpoint::server(
        ca.server_config(ca.issue("server.rpc")?)?,
        "127.0.0.1:0".parse()?,
    )?;
    let addr = server_ep.local_addr()?;

    let shutdown = CancellationToken::new();
    let server = tokio::spawn(serve(server_ep, dispatch, shutdown.clone()));

    // Stop the server even if the call fails, so the program always exits cleanly.
    let result = echo_once(&ca, addr).await;
    shutdown.cancel();
    server.await?;

    let reply = result?;
    anyhow::ensure!(
        reply.text == "hi",
        "echo returned {:?}, expected \"hi\"",
        reply.text
    );
    println!("echo over QUIC + mTLS: sent \"hi\", received {:?}: OK", reply.text);
    Ok(())
}

async fn echo_once(ca: &tls::DevCA, addr: SocketAddr) -> anyhow::Result<EchoResponse> {
    let mut client_ep = Endpoint::client("127.0.0.1:0".parse()?)?;
    client_ep.set_default_client_config(ca.client_config(ca.issue("client-1")?)?);
    let client = Client::new(client_ep.connect(addr, "server.rpc")?.await?);

    let reply = EchoClient::new(client)
        .echo(
            EchoRequest {
                text: "hi".into(),
            },
            Some(Duration::from_secs(2)),
        )
        .await?;
    Ok(reply)
}
