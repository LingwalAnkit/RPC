use anyhow::{Ok, anyhow};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{StreamExt, future::MaybeDone::Future};
use quinn::{Connection, Endpoint, RecvStream, SendStream, VarInt};
use std::{
    future::Future,
    io::ErrorKind::Other,
    net::Shutdown,
    process::Output,
    result,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::windows::named_pipe::PipeMode::Message, sync::watch::error};
use tokio_util::{
    codec::{Encoder, FramedRead},
    sync::CancellationToken,
    task::TaskTracker,
};

use crate::{frame::*, handle_stream};

const CANCELLED: VarInt = VarInt::from_u32(1);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

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

fn encode_request(deadline_ms: u32, body: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + body.len());
    b.put_u32(deadline_ms);
    b.extend_from_slice(body);
    b.freeze()
}

fn decode_request(mut p: Bytes) -> Result<(u32, Bytes), RpcError> {
    if p.len() < 4 {
        return Err(RpcError::new(Code::InvalidArgument, "Request too short"));
    }
    let deadline_ms = p.get_u32();
    Ok((deadline_ms, p))
}

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
            _  = shutdown.cancelled() => return,
            r = conn.accept_bi() => match r {
                Ok(s) => s,
                Err(_) => return,
            },
        };
        let h = handler.clone();
        tracker.spawn(async move {
            if let Err(e) = handel_stream(send, recv, h).await {
                eprintln!("stream error: {e}");
            }
        });
    }
}

async fn handel_stream<H, Fut>(
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
    result: Result<Bytes, RpcError>,
    call_id: u64,
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
            payload,
            call_id,
        },
        &mut buf,
    )?;
    send.write_all(&buf).await?;
    send.finish()?;
    let _ = send.stopped().await;
    Ok(())
}
