mod frame;
mod rpc;
mod tls;

use std::time::Duration;

use quinn::Endpoint; // Endpoint comes from the Quinn library, which implements QUIC in Rust.
use rpc::{Client, echo, serve};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let ca = tls::DevCA::new()?;
    // creates a certificate authority

    let sep = Endpoint::server(
        ca.server_config(ca.issue("server.rpc")?)?,
        "127.0.0.1:0".parse()?,
    )?;
    // creating server endpoint
    // server(server_config , server address)

    let addr = sep.local_addr()?;
    // .local_addr() retrieves its actual local socket address.
    // 127.0.0.1:54321 the address might be this 0 is just a placeholder

    let shutdown = CancellationToken::new();
    // creates a cancellation token
    // think of this a shared signal starts with not cancelled state

    let server = tokio::spawn(serve(sep, echo, shutdown.clone()));
    //Tokio's spawn schedules an asynchronous task to run independently of the current task.

    let mut cep = Endpoint::client("0.0.0.0:0".parse()?)?;
    cep.set_default_client_config(ca.client_config(ca.issue("client-1")?)?);
    //Configure outgoing connections
    let client = Client::new(cep.connect(addr, "server.rpc")?.await?);
    // addr Tells the client where to send network traffic.
    // Identifies the expected server name during TLS authentication.

    println!("{:?}", client.call(b"hi", None).await);
    // b means byte string
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

// ?  at any point of the code if error occur stop the code and throw error
