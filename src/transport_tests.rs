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

use bytes::{BufMut, Bytes, BytesMut};
use futures::{StreamExt, future::join_all};
use quinn::Endpoint;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::{
    codec::{Decoder, Encoder, FramedRead},
    sync::CancellationToken,
};

use crate::{
    codec::{EchoRequest, Message},
    echo_service::{EchoClient, dispatch as echo_dispatch},
    frame::{END_STREAM, Frame, FrameCodec, FrameError, Kind, MAX_PAYLOAD, VERSION},
    rpc::{Client, Code, RpcError, serve},
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

/// Raw test handler: echoes the body. "sleep:<ms>" sleeps first, for deadline tests.
async fn raw_echo(body: Bytes) -> Result<Bytes, RpcError> {
    if let Some(ms) = body.strip_prefix(b"sleep:") {
        let ms: u64 = std::str::from_utf8(ms)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RpcError::new(Code::InvalidArgument, "bad sleep"))?;
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    Ok(body)
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

// ---------- calls ----------

#[tokio::test]
async fn echo_roundtrip() -> anyhow::Result<()> {
    let srv = start_server(raw_echo)?;
    let (_ep, client) = connect(&srv).await?;
    let out = client.call(b"hello", None).await?;
    assert_eq!(&out[..], b"hello");
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn large_payload_roundtrips() -> anyhow::Result<()> {
    let body: Vec<u8> = (0..(1usize << 20)).map(|i| (i % 251) as u8).collect();
    let srv = start_server(raw_echo)?;
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
    let srv = start_server(raw_echo)?;
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
    let srv = start_server(raw_echo)?;
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

    assert!(
        outcome.is_err(),
        "server accepted a client certificate from an untrusted CA"
    );
    srv.shutdown().await;
    Ok(())
}

// ---------- typed echo service ----------

#[tokio::test]
async fn typed_echo_service_roundtrip() -> anyhow::Result<()> {
    let srv = start_server(echo_dispatch)?;
    let (_ep, client) = connect(&srv).await?;
    let resp = EchoClient::new(client)
        .echo(
            EchoRequest {
                text: "hello".into(),
            },
            None,
        )
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
    assert!(matches!(
        codec.decode(&mut bad_kind),
        Err(FrameError::BadKind(9))
    ));

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

// ---------- edge cases ----------

/// A plain QUIC connection that bypasses the RPC client, so tests can send
/// bytes the client would never produce. Returns the endpoint to keep it alive.
async fn raw_connection(srv: &TestServer) -> anyhow::Result<(Endpoint, quinn::Connection)> {
    let mut ep = Endpoint::client("127.0.0.1:0".parse()?)?;
    ep.set_default_client_config(srv.ca.client_config(srv.ca.issue("client-raw")?)?);
    let conn = ep.connect(srv.addr, "server.rpc")?.await?;
    Ok((ep, conn))
}

#[tokio::test]
async fn client_rejects_a_reply_with_the_wrong_call_id() -> anyhow::Result<()> {
    init();
    let ca = DevCA::new()?;
    let ep = Endpoint::server(
        ca.server_config(ca.issue("server.rpc")?)?,
        "127.0.0.1:0".parse()?,
    )?;
    let addr = ep.local_addr()?;

    // Fake server: answers the first stream with a reply for call id 999.
    let fake = tokio::spawn(async move {
        let conn = ep
            .accept()
            .await
            .expect("no connection")
            .await
            .expect("handshake failed");
        let (mut send, _recv) = conn.accept_bi().await.expect("no stream");
        let mut wire = BytesMut::new();
        let mut codec = FrameCodec;
        codec
            .encode(
                Frame {
                    kind: Kind::Response,
                    flags: END_STREAM,
                    call_id: 999,
                    payload: Bytes::from_static(b"forged"),
                },
                &mut wire,
            )
            .expect("encode failed");
        send.write_all(&wire).await.expect("write failed");
        send.finish().expect("finish failed");
        let _ = send.stopped().await;
    });

    let mut client_ep = Endpoint::client("127.0.0.1:0".parse()?)?;
    client_ep.set_default_client_config(ca.client_config(ca.issue("client-1")?)?);
    let client = Client::new(client_ep.connect(addr, "server.rpc")?.await?);

    let err = client
        .call(b"hello", Some(Duration::from_secs(2)))
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::Internal);
    assert!(
        err.message.contains("call id 999"),
        "unexpected error: {}",
        err.message
    );
    fake.await?;
    Ok(())
}

#[tokio::test]
async fn many_concurrent_calls_on_one_connection() -> anyhow::Result<()> {
    let srv = start_server(raw_echo)?;
    let (_ep, client) = connect(&srv).await?;
    let calls = (0..50).map(|i| {
        let client = client.clone();
        async move {
            let body = format!("message-{i}");
            let out = client.call(body.as_bytes(), None).await?;
            anyhow::ensure!(&out[..] == body.as_bytes(), "call {i} got the wrong reply");
            Ok::<(), anyhow::Error>(())
        }
    });
    for result in join_all(calls).await {
        result?;
    }
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn garbage_streams_do_not_break_the_server() -> anyhow::Result<()> {
    let srv = start_server(raw_echo)?;
    let (_raw_ep, raw) = raw_connection(&srv).await?;

    // Stream 1: bytes that are not a frame at all. The server must answer with
    // an InvalidArgument error rather than stay silent.
    let (mut send, recv) = raw.open_bi().await?;
    send.write_all(&[0xFFu8; 64]).await?;
    send.finish()?;
    let reply = FramedRead::new(recv, FrameCodec)
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("no reply to a malformed request"))??;
    assert_eq!(reply.kind, Kind::Error);
    assert_eq!(
        &reply.payload[..2],
        &(Code::InvalidArgument as u16).to_be_bytes()[..]
    );

    // Stream 2: a valid-looking header with an unknown frame kind (9).
    let (mut send2, _recv2) = raw.open_bi().await?;
    send2
        .write_all(&[VERSION, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0])
        .await?;
    send2.finish()?;

    // The server must still answer a normal client.
    let (_ep, client) = connect(&srv).await?;
    let out = client.call(b"still alive", None).await?;
    assert_eq!(&out[..], b"still alive");
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn panicking_handler_fails_only_that_call() -> anyhow::Result<()> {
    let srv = start_server(|body: Bytes| async move {
        if &body[..] == b"boom" {
            panic!("handler panicked on purpose");
        }
        Ok::<Bytes, RpcError>(body)
    })?;
    let (_ep, client) = connect(&srv).await?;
    assert!(
        client.call(b"boom", None).await.is_err(),
        "the panicking call should fail"
    );
    let out = client.call(b"ok", None).await?;
    assert_eq!(&out[..], b"ok");
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn server_enforces_deadline_from_the_request_frame() -> anyhow::Result<()> {
    let srv = start_server(raw_echo)?;
    let (_raw_ep, raw) = raw_connection(&srv).await?;

    // Request with a 100 ms deadline. The handler would sleep for 2 s.
    let mut payload = BytesMut::new();
    payload.put_u32(100);
    payload.extend_from_slice(b"sleep:2000");
    let mut wire = BytesMut::new();
    let mut codec = FrameCodec;
    codec.encode(
        Frame {
            kind: Kind::Request,
            flags: END_STREAM,
            call_id: 1,
            payload: payload.freeze(),
        },
        &mut wire,
    )?;

    let (mut send, recv) = raw.open_bi().await?;
    send.write_all(&wire).await?;
    send.finish()?;

    let started = std::time::Instant::now();
    let mut frames = FramedRead::new(recv, FrameCodec);
    let reply = tokio::time::timeout(Duration::from_secs(3), frames.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("no reply from server"))??;

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "server waited for the handler instead of enforcing the deadline"
    );
    assert_eq!(reply.kind, Kind::Error);
    assert_eq!(
        &reply.payload[..2],
        &(Code::DeadlineExceeded as u16).to_be_bytes()[..]
    );
    srv.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn shutdown_lets_an_in_flight_call_finish() -> anyhow::Result<()> {
    let started = Arc::new(Notify::new());
    let signal = started.clone();
    let srv = start_server(move |body: Bytes| {
        let signal = signal.clone();
        async move {
            signal.notify_one();
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<Bytes, RpcError>(body)
        }
    })?;
    let (_ep, client) = connect(&srv).await?;
    let call = tokio::spawn(async move { client.call(b"finish-me", None).await });

    // Wait until the handler is running, then shut down while it is still working.
    started.notified().await;
    srv.shutdown().await;

    let reply = call.await??;
    assert_eq!(&reply[..], b"finish-me");
    Ok(())
}

#[tokio::test]
async fn client_gets_an_error_when_server_disappears_mid_call() -> anyhow::Result<()> {
    init();
    let ca = DevCA::new()?;
    let server_ep = Endpoint::server(
        ca.server_config(ca.issue("server.rpc")?)?,
        "127.0.0.1:0".parse()?,
    )?;
    let addr = server_ep.local_addr()?;
    // A second handle to the same endpoint lets the test kill the server abruptly.
    let killer = server_ep.clone();
    let stop = CancellationToken::new();
    let task = tokio::spawn(serve(server_ep, raw_echo, stop.clone()));
    let srv = TestServer {
        addr,
        ca,
        stop,
        task,
    };

    let (_ep, client) = connect(&srv).await?;
    // No client timeout: without the fix this call would hang forever.
    let call = tokio::spawn(async move { client.call(b"sleep:10000", None).await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    killer.close(0u32.into(), b"server gone");
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect("client hung after the server closed")
        .expect("call task panicked");
    assert!(
        result.is_err(),
        "the call should fail when the server is gone"
    );

    srv.task.abort();
    Ok(())
}
