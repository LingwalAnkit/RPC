//! Tests for the transport: calls, deadlines, cancellation, errors, TLS, frames
//! and the typed echo service. Each test runs a real QUIC server and client on
//! loopback with mutual TLS.

use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc, Once,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use quinn::Endpoint;
use tokio::task::JoinHandle;
use tokio_util::{
    codec::{Decoder, Encoder},
    sync::CancellationToken,
};

use crate::{
    codec::{EchoRequest, Message},
    echo_service::{ECHO_METHOD, EchoClient, EchoResponse},
    frame::{Frame, FrameCodec, FrameError, Kind, MAX_PAYLOAD, END_STREAM, VERSION},
    rpc::{Client, Code, RpcError, echo, serve},
    tls::DevCA,
};

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
    });
}

struct TestServer {
    addr: SocketAddr,
    ca: DevCA,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl TestServer {
    /// Stops the server and fails the test if `serve` does not return promptly.
    async fn shutdown(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(15), self.task)
            .await
            .expect("serve() did not stop within 15s")
            .expect("serve task panicked");
    }
}

fn start_server<H, Fut>(handler: H) -> anyhow::Result<TestServer>
where
    H: Fn(Bytes) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, RpcError>> + Send + 'static,
{
    init();
    let ca = DevCA::new()?;
    let ep = Endpoint::server(
        ca.server_config(ca.issue("server.rpc")?)?,
        "127.0.0.1:0".parse()?,
    )?;
    let addr = ep.local_addr()?;
    let stop = CancellationToken::new();
    let task = tokio::spawn(serve(ep, handler, stop.clone()));
    Ok(TestServer {
        addr,
        ca,
        stop,
        task,
    })
}

/// Returns the client endpoint too: dropping it would close the connection.
async fn connect(srv: &TestServer) -> anyhow::Result<(Endpoint, Client)> {
    let mut ep = Endpoint::client("127.0.0.1:0".parse()?)?;
    ep.set_default_client_config(srv.ca.client_config(srv.ca.issue("client-1")?)?);
    let conn = ep.connect(srv.addr, "server.rpc")?.await?;
    Ok((ep, Client::new(conn)))
}

/// Dispatches the typed echo protocol: a u16 method id, then an EchoRequest.
async fn echo_dispatch(mut body: Bytes) -> Result<Bytes, RpcError> {
    if body.len() < 2 {
        return Err(RpcError::new(Code::InvalidArgument, "missing method id"));
    }
    let method = body.get_u16();
    if method != ECHO_METHOD {
        return Err(RpcError::new(Code::NotFound, "unknown method"));
    }
    let req = EchoRequest::decode(&mut body)
        .map_err(|e| RpcError::new(Code::InvalidArgument, e.to_string()))?;
    let mut out = BytesMut::new();
    EchoResponse { text: req.text }.encode(&mut out);
    Ok(out.freeze())
}

// ---------- calls ----------

#[tokio::test]
async fn echo_roundtrip() -> anyhow::Result<()> {
    let srv = start_server(echo)?;
    let (_ep, client) = connect(&srv).await?;
    let out = client.call(b"hello", None).await?;
    assert_eq!(&out[..], b"hello");
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn large_payload_roundtrips() -> anyhow::Result<()> {
    let body: Vec<u8> = (0..(1usize << 20)).map(|i| (i % 251) as u8).collect();
    let srv = start_server(echo)?;
    let (_ep, client) = connect(&srv).await?;
    let out = client.call(&body, None).await?;
    assert_eq!(out.len(), body.len());
    assert!(out[..] == body[..], "payload was corrupted in transit");
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn handler_error_reaches_client_with_code_and_message() -> anyhow::Result<()> {
    let srv = start_server(|_body: Bytes| async {
        Result::<Bytes, RpcError>::Err(RpcError::new(Code::NotFound, "no such key"))
    })?;
    let (_ep, client) = connect(&srv).await?;
    let err = client.call(b"anything", None).await.unwrap_err();
    assert_eq!(err.code, Code::NotFound);
    assert_eq!(err.message, "no such key");
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn client_deadline_is_enforced() -> anyhow::Result<()> {
    let srv = start_server(echo)?;
    let (_ep, client) = connect(&srv).await?;
    let err = client
        .call(b"sleep:2000", Some(Duration::from_millis(100)))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
    srv.shutdown().await;
    Ok(())
}

// ---------- cancellation ----------

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_call_stops_server_handler() -> anyhow::Result<()> {
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = dropped.clone();
    // The handler would run for 30s. The guard records when its future is dropped.
    let srv = start_server(move |_body: Bytes| {
        let guard = DropFlag(flag.clone());
        async move {
            let _guard = guard;
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<Bytes, RpcError>(Bytes::new())
        }
    })?;
    let (_ep, client) = connect(&srv).await?;

    // Cancel by dropping the call future from outside (no deadline on the wire).
    let timed = tokio::time::timeout(Duration::from_millis(200), client.call(b"x", None)).await;
    assert!(timed.is_err(), "the call should have been cancelled");

    for _ in 0..200 {
        if dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        dropped.load(Ordering::SeqCst),
        "server handler kept running after the client cancelled"
    );
    srv.shutdown().await;
    Ok(())
}

// ---------- TLS ----------

#[tokio::test]
async fn client_with_untrusted_certificate_is_rejected() -> anyhow::Result<()> {
    let srv = start_server(echo)?;
    let intruder_ca = DevCA::new()?;
    let mut ep = Endpoint::client("127.0.0.1:0".parse()?)?;
    // Trusts the server's CA, so the server certificate checks out, but presents
    // a certificate issued by a different CA. The server must refuse it.
    ep.set_default_client_config(srv.ca.client_config(intruder_ca.issue("intruder")?)?);

    let outcome: anyhow::Result<Bytes> = async {
        let conn = ep.connect(srv.addr, "server.rpc")?.await?;
        Client::new(conn)
            .call(b"hi", Some(Duration::from_secs(2)))
            .await
            .map_err(anyhow::Error::from)
    }
    .await;

    assert!(outcome.is_err(), "server accepted a client certificate from an untrusted CA");
    srv.shutdown().await;
    Ok(())
}

// ---------- typed echo service ----------

#[tokio::test]
async fn typed_echo_service_roundtrip() -> anyhow::Result<()> {
    let srv = start_server(echo_dispatch)?;
    let (_ep, client) = connect(&srv).await?;
    let resp = EchoClient::new(client)
        .echo(EchoRequest { text: "hello".into() }, None)
        .await?;
    assert_eq!(resp.text, "hello");
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn typed_echo_rejects_unknown_method() -> anyhow::Result<()> {
    let srv = start_server(echo_dispatch)?;
    let (_ep, client) = connect(&srv).await?;
    // A body with method id 99 followed by a valid request body.
    let mut body = BytesMut::new();
    body.put_u16(99);
    EchoRequest { text: "x".into() }.encode(&mut body);
    let err = client.call(&body, None).await.unwrap_err();
    assert_eq!(err.code, Code::NotFound);
    srv.shutdown().await;
    Ok(())
}

// ---------- frame codec ----------

#[test]
fn frame_roundtrip_and_partial_reads() {
    let mut codec = FrameCodec;
    let frame = Frame {
        kind: Kind::Request,
        flags: END_STREAM,
        call_id: 42,
        payload: Bytes::from_static(b"abc"),
    };
    let mut wire = BytesMut::new();
    codec.encode(frame.clone(), &mut wire).unwrap();

    // Only part of the frame has arrived: the decoder must wait, not fail.
    let mut partial = BytesMut::from(&wire[..10]);
    assert!(codec.decode(&mut partial).unwrap().is_none());

    assert_eq!(codec.decode(&mut wire).unwrap(), Some(frame));
}

#[test]
fn frame_rejects_bad_version_kind_and_size() {
    let mut codec = FrameCodec;
    let frame = Frame {
        kind: Kind::Response,
        flags: 0,
        call_id: 7,
        payload: Bytes::from_static(b"ok"),
    };
    let mut wire = BytesMut::new();
    codec.encode(frame, &mut wire).unwrap();

    let mut bad_version = wire.clone();
    bad_version[0] = VERSION + 1;
    assert!(matches!(
        codec.decode(&mut bad_version),
        Err(FrameError::BadVersion(_))
    ));

    let mut bad_kind = wire.clone();
    bad_kind[1] = 9;
    assert!(matches!(codec.decode(&mut bad_kind), Err(FrameError::BadKind(9))));

    let mut too_large = BytesMut::new();
    too_large.put_u8(VERSION);
    too_large.put_u8(Kind::Data as u8);
    too_large.put_u16(0);
    too_large.put_u64(1);
    too_large.put_u32((MAX_PAYLOAD + 1) as u32);
    assert!(matches!(
        codec.decode(&mut too_large),
        Err(FrameError::TooLarge(_))
    ));
}
