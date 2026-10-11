use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::StreamExt;
use quinn::{Connection, Endpoint, RecvStream, SendStream, VarInt};
use std::{
    future::Future,
    io::ErrorKind::Other,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio_util::{
    codec::{Encoder, FramedRead},
    sync::CancellationToken,
    task::TaskTracker,
};

use crate::frame::*;

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
