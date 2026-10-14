use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::StreamExt;
use quinn::{Connection, Endpoint, RecvStream, SendStream, VarInt};
use tokio_util::{
    codec::{Encoder, FramedRead},
    sync::CancellationToken,
    task::TaskTracker,
};

use crate::frame::*;

const CANCELLED: VarInt = VarInt::from_u32(1);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

// ---------- errors ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Code {
    Cancelled = 1,
    DeadlineExceeded = 2,
    InvalidArgument = 3,
    NotFound = 4,
    Internal = 5,
    Unavailable = 6,
}

impl Code {
    fn from_u16(v: u16) -> Code {
        match v {
            1 => Code::Cancelled,
            2 => Code::DeadlineExceeded,
            3 => Code::InvalidArgument,
            4 => Code::NotFound,
            6 => Code::Unavailable,
            _ => Code::Internal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct RpcError {
    pub code: Code,
    pub message: String,
}

impl RpcError {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(2 + self.message.len());
        b.put_u16(self.code as u16);
        b.extend_from_slice(self.message.as_bytes());
        b.freeze()
    }
    fn decode(mut p: Bytes) -> Self {
        if p.len() < 2 {
            return Self::new(Code::Internal, "malformed error frame");
        }
        let code = Code::from_u16(p.get_u16());
        Self::new(code, String::from_utf8_lossy(&p).into_owned())
    }
}

fn unavailable(e: impl std::fmt::Display) -> RpcError {
    RpcError::new(Code::Unavailable, e.to_string())
}
fn internal(e: impl std::fmt::Display) -> RpcError {
    RpcError::new(Code::Internal, e.to_string())
}

// ---------- request payload ----------

fn encode_request(deadline_ms: u32, body: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + body.len()); // creates a mutable byte buffer with capacity for the four-byte deadline plus the body. Capacity is not necessarily the same as the buffer's current length.
    // 4 bytes is deadline
    b.put_u32(deadline_ms);
    b.extend_from_slice(body); // append the deadline in the buffer
    b.freeze() // convert the buffer to bytes
}

fn decode_request(mut p: Bytes) -> Result<(u32, Bytes), RpcError> {
    if p.len() < 4 {
        return Err(RpcError::new(Code::InvalidArgument, "request too short"));
    }
    let deadline_ms = p.get_u32();
    Ok((deadline_ms, p))
}

// It reads the first four bytes as the deadline, leaving the remaining bytes as the body.

/// Demo handler: echoes the body. "sleep:<ms>" sleeps first (for testing deadlines).
pub async fn echo(body: Bytes) -> Result<Bytes, RpcError> {
    if let Some(ms) = body.strip_prefix(b"sleep:") {
        let ms: u64 = std::str::from_utf8(ms)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RpcError::new(Code::InvalidArgument, "bad sleep"))?;
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    Ok(body)
}

// ---------- server ----------

pub async fn serve<H, Fut>(ep: Endpoint, handler: H, shutdown: CancellationToken)
where
    H: Fn(Bytes) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, RpcError>> + Send + 'static,
{
    let handler = Arc::new(handler);
    let tracker = TaskTracker::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            incoming = ep.accept() => {
                let Some(incoming) = incoming else { break };
                let (h, t, sd) = (handler.clone(), tracker.clone(), shutdown.clone());
                tracker.spawn(async move {
                    match incoming.await {
                        Ok(conn) => handle_conn(conn, h, t, sd).await,
                        Err(e) => eprintln!("handshake failed: {e}"),
                    }
                });
            }
        }
    }

    // Drain: no new work is accepted; wait (bounded) for in-flight calls.
    tracker.close();
    let _ = tokio::time::timeout(DRAIN_TIMEOUT, tracker.wait()).await;
    ep.close(0u32.into(), b"shutdown");
    ep.wait_idle().await;
}

async fn handle_conn<H, Fut>(
    conn: Connection,
    handler: Arc<H>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
) where
    H: Fn(Bytes) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, RpcError>> + Send + 'static,
{
    loop {
        let (send, recv) = tokio::select! {
            _ = shutdown.cancelled() => return,
            r = conn.accept_bi() => match r {
                Ok(s) => s,
                Err(_) => return, // connection closed or lost
            },
        };
        let h = handler.clone();
        tracker.spawn(async move {
            if let Err(e) = handle_stream(send, recv, h).await {
                eprintln!("stream error: {e}");
            }
        });
    }
}

async fn handle_stream<H, Fut>(
    mut send: SendStream,
    recv: RecvStream,
    handler: Arc<H>,
) -> anyhow::Result<()>
where
    H: Fn(Bytes) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, RpcError>> + Send + 'static,
{
    let mut rx = FramedRead::new(recv, FrameCodec);
    let req = match rx.next().await {
        Some(Ok(f)) if f.kind == Kind::Request => f,
        _ => return Ok(()), // malformed or empty stream: nothing to answer
    };
    let call_id = req.call_id;

    let (deadline_ms, body) = match decode_request(req.payload) {
        Ok(x) => x,
        Err(e) => return respond(&mut send, call_id, Err(e)).await,
    };

    let work = async move {
        let fut = (*handler)(body);
        if deadline_ms == 0 {
            fut.await
        } else {
            tokio::time::timeout(Duration::from_millis(deadline_ms as u64), fut)
                .await
                .unwrap_or_else(|_| Err(RpcError::new(Code::DeadlineExceeded, "deadline exceeded")))
        }
    };

    // If the client cancels, `stopped()` fires and `work` is dropped mid-await.
    let outcome = tokio::select! {
        r = work => Some(r),
        _ = send.stopped() => None,
    };

    match outcome {
        Some(result) => respond(&mut send, call_id, result).await,
        None => Ok(()), // cancelled by peer
    }
}

async fn respond(
    send: &mut SendStream,
    call_id: u64,
    result: Result<Bytes, RpcError>,
) -> anyhow::Result<()> {
    let (kind, payload) = match result {
        Ok(b) => (Kind::Response, b),
        Err(e) => (Kind::Error, e.encode()),
    };
    let mut buf = BytesMut::new();
    FrameCodec.encode(
        Frame {
            kind,
            flags: END_STREAM,
            call_id,
            payload,
        },
        &mut buf,
    )?;
    send.write_all(&buf).await?;
    send.finish()?;
    // finish() only queues the FIN. Wait until the peer ACKed it (or cancelled),
    // otherwise a shutdown right after this could discard the response.
    let _ = send.stopped().await;
    Ok(())
}

// ---------- client ----------

#[derive(Clone)]
pub struct Client {
    conn: Connection,
    next_id: Arc<AtomicU64>,
}

/// Owns both halves of a call's stream. If the call future is dropped before
/// completing (timeout, abort), Drop tells the server to stop working.
struct ActiveCall {
    send: SendStream,
    recv: RecvStream,
    finished: bool,
}

impl Drop for ActiveCall {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.send.reset(CANCELLED);
            let _ = self.recv.stop(CANCELLED);
        }
    }
}

impl Client {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            next_id: Arc::new(AtomicU64::new(1)),
            // this creates a shared atomic counter initialized to 1
            // AtomicU64 stores a 64-bit unsigned integer that can be updated safely across concurrent tasks.
        }
    }

    pub async fn call(&self, body: &[u8], timeout: Option<Duration>) -> Result<Bytes, RpcError> {
        // &[u8] A borrowed slice of the request's bytes.
        // timeout An optional timeout for the call. either contain some time or none
        let call_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // This line retrieves the current counter value and increments the counter atomically.

        let deadline_ms = timeout
            .map(|d| d.as_millis().clamp(1, u32::MAX as u128) as u32)
            //  if timeout is Some(d) then convert duration to integer of millisec
            .unwrap_or(0); // if the timeout was none then it become 0
        // Clamp keep the value between 1 and max u32
        // if option is non return 0

        let fut = self.call_inner(call_id, deadline_ms, body);
        match timeout {
            // On expiry the future is dropped, which triggers ActiveCall::drop.
            Some(d) => tokio::time::timeout(d, fut).await.unwrap_or_else(|_| {
                Err(RpcError::new(Code::DeadlineExceeded, "deadline exceeded"))
            }),
            // if timeout start waiting for the future if future finish in time tokio return Ok(value) if future returns a value else if error deadline exceeded
            // fut -> result<Bytes , RpcError>
            // Bytes is &u[8]
            None => fut.await,
        }
    }

    async fn call_inner(
        &self,
        call_id: u64,
        deadline_ms: u32,
        body: &[u8],
    ) -> Result<Bytes, RpcError> {
        // Waits here if the server's stream limit is exhausted (backpressure).
        let (send, recv) = self.conn.open_bi().await.map_err(unavailable)?;
        // open bi directional quic stream
        // if connection stream limit has been reached map the error with rpc error unavailable

        let mut call = ActiveCall {
            send,
            recv,
            finished: false,
        };

        let req = Frame {
            kind: Kind::Request,
            flags: END_STREAM,
            call_id,
            payload: encode_request(deadline_ms, body), // appending the deadline with the body
        };
        let mut buf = BytesMut::new(); // bytemut is a growable mutable byte buffer
        // buf = []

        FrameCodec.encode(req, &mut buf).map_err(internal)?;
        // encode serialze the req(frame) into bytes
        // If the encoder returns an error, map_err(internal)
        // we are using &mut buf Because encode() needs somewhere to put the encoded bytes, and &mut buf gives it permission to modify the buffer you created.

        call.send.write_all(&buf).await.map_err(unavailable)?;
        //This writes the serialized frame bytes into the QUIC send stream.
        // buf now contain the serealize frame

        call.send.finish().map_err(internal)?;
        // Signals the end of the QUIC send stream.

        let resp = FramedRead::new(&mut call.recv, FrameCodec)
            // call.recv source of incoming bytes
            // framecodec decoder that know how to convert bytes to frame
            // FramedRead bridges that gap: it reads the incoming byte stream and uses FrameCodec to decode complete frames.
            .next()
            .await // wait for the next frame
            .ok_or_else(|| unavailable("stream closed before response"))?
            .map_err(internal)?;
        call.finished = true;

        // switch from sending to receiving
        // Receiving the RPC response

        match resp.kind {
            Kind::Response => Ok(resp.payload),
            Kind::Error => Err(RpcError::decode(resp.payload)),
            other => Err(internal(format!("unexpected frame {other:?}"))),
        }
    }
}
