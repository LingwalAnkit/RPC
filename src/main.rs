mod frame;
mod tls;
use anyhow::{Result, anyhow, ensure};
use bytes::Bytes;
use frame::*;
use futures::{SinkExt, StreamExt};
//StreamExt → gives you .next()
//SinkExt → gives you .send()
// to send and receive frames
use quinn::{Connection, ConnectionError, Endpoint, RecvStream, SendStream};
use tokio_util::codec::{FramedRead, FramedWrite}; // This Gives message/frame interface
// bytes -> codec -> frames and back

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let ca = tls::DevCA::new()?;

    // --- server ---
    let server_cfg = ca.server_config(ca.issue("server.rpc")?)?;
    let server_ep = Endpoint::server(server_cfg, "127.0.0.1:0".parse()?)?;
    let addr = server_ep.local_addr()?;
    tokio::spawn(serve(server_ep)); // run the server in bg

    // --- client ---
    let mut client_ep = Endpoint::client("0.0.0.0:0".parse()?)?;
    client_ep.set_default_client_config(ca.client_config(ca.issue("client-1")?)?);
    let conn = client_ep.connect(addr, "server.rpc")?.await?;

    let resp = call(&conn, 1, b"hello over quic").await?;
    println!("{:?}", resp); // makes an rpc call

    conn.close(0u32.into(), b"done");
    client_ep.wait_idle().await;
    Ok(())
}

// server side
async fn serve(ep: Endpoint) {
    while let Some(incoming) = ep.accept().await {
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    if let Err(e) = handle_conn(conn).await {
                        eprintln!("connection error: {e}");
                    }
                }
                Err(e) => eprintln!("handshake failed: {e}"),
            }
        });
    }
}
async fn handle_conn(conn: Connection) -> Result<()> {
    loop {
        let (send, recv) = match conn.accept_bi().await {
            Ok(s) => s,
            Err(ConnectionError::ApplicationClosed(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        tokio::spawn(async move {
            if let Err(e) = handle_stream(send, recv).await {
                eprintln!("Stream error {}", e);
            }
        });
    }
}

async fn handle_stream(send: SendStream, recv: RecvStream) -> Result<()> {
    let mut rx = FramedRead::new(recv, FrameCodec);
    let mut tx = FramedWrite::new(send, FrameCodec);

    let req = rx
        .next()
        .await
        .ok_or_else(|| anyhow!("stream closed before request"))??;
    ensure!(
        req.kind == Kind::Request,
        "expected Request, got {:?}",
        req.kind
    );

    // Echo handler for now; real dispatch arrives with the IDL in Stage 4.
    tx.send(Frame {
        kind: Kind::Response,
        flags: END_STREAM,
        call_id: req.call_id,
        payload: req.payload,
    })
    .await?;
    tx.close().await?; // finishes the QUIC send stream
    Ok(())
}

async fn call(conn: &Connection, call_id: u64, payload: &[u8]) -> Result<Frame> {
    let (send, recv) = conn.open_bi().await?;

    let mut tx = FramedWrite::new(send, FrameCodec);
    tx.send(Frame {
        kind: Kind::Request,
        flags: END_STREAM,
        call_id,
        payload: Bytes::copy_from_slice(payload),
    })
    // tx.send(frame)
    // |
    // v
    // encode(item, dst)
    // |
    // | item = your Frame
    // | dst  = output byte buffer
    // v
    // Serialized bytes in dst
    // |
    // v
    // QUIC SendStream
    .await?;
    tx.close().await?; // client send request
    // client send bytes framecode encode them
    // server recieve

    let mut rx = FramedRead::new(recv, FrameCodec);
    let resp = rx
        .next()
        .await
        .ok_or_else(|| anyhow!("stream closed before response"))??;
    Ok(resp)

    // server send bytes
    // framed code dencode them
}
