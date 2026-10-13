mod frame;
mod rpc;
mod tls;

use std::time::Duration;

use quinn::Endpoint;
use rpc::{Client, echo, serve};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let ca = tls::DevCA::new()?;

    let sep = Endpoint::server(
        ca.server_config(ca.issue("server.rpc")?)?,
        "127.0.0.1:0".parse()?,
    )?;
    let addr = sep.local_addr()?;
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(serve(sep, echo, shutdown.clone()));

    let mut cep = Endpoint::client("0.0.0.0:0".parse()?)?;
    cep.set_default_client_config(ca.client_config(ca.issue("client-1")?)?);
    let client = Client::new(cep.connect(addr, "server.rpc")?.await?);

    println!("{:?}", client.call(b"hi", None).await);
    println!(
        "{:?}",
        client
            .call(b"sleep:2000", Some(Duration::from_millis(100)))
            .await
    );

    shutdown.cancel();
    server.await?;
    Ok(())
}
