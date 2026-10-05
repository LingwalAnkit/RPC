use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub const VERSION: u8 = 1;
pub const HEADER_LENGHT: usize = 16;
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
pub const END_STREAM: u16 = 0b0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Request = 0,
    Response = 1,
    Error = 2,
    Cancel = 3,
    Data = 4,
}

impl TryFrom<u8> for Kind {
    type Error = FrameError;
    fn try_from(b: u8) -> Result<Self, FrameError> {
        Ok(match b {
            0 => Kind::Request,
            1 => Kind::Response,
            2 => Kind::Error,
            3 => Kind::Cancel,
            4 => Kind::Data,
            other => return Err(FrameError::BadKind(other)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: Kind,
    pub flags: u16,
    pub payload: Bytes,
    pub call_id: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported kind: {0}")]
    BadKind(u8),
    #[error("unsupported version: {0}")]
    BadVersion(u8),
    #[error("payload too large: {0} bytes")]
    TooLarge(usize),
}

#[derive(Debug)]
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = FrameError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        if src.len() < HEADER_LENGHT {
            return Ok(None);
        }
        if src[0] != VERSION {
            return Err(FrameError::BadVersion(src[0]));
        }

        let kind = Kind::try_from(src[1])?;
        let flags = u16::from_be_bytes([src[2], src[3]]);
        let call_id = u64::from_be_bytes(src[4..12].try_into().unwrap());
        let len = u32::from_be_bytes(src[12..16].try_into().unwrap()) as usize;

        if len > MAX_PAYLOAD {
            return Err(FrameError::TooLarge(len));
        }
        if src.len() < HEADER_LENGHT + len {
            src.reserve(HEADER_LENGHT + len - src.len());
            return Ok(None);
        }
        src.advance(HEADER_LENGHT);
        let payload = src.split_to(len).freeze();
        Ok(Some(Frame {
            kind,
            flags,
            payload,
            call_id,
        }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = FrameError;

    fn encode(&mut self, f: Frame, dst: &mut BytesMut) -> Result<(), FrameError> {
        if f.payload.len() > MAX_PAYLOAD {
            return Err(FrameError::TooLarge(f.payload.len()));
        }
        dst.reserve(HEADER_LENGHT + f.payload.len());
        dst.put_u8(VERSION);
        dst.put_u8(f.kind as u8);
        dst.put_u16(f.flags);
        dst.put_u64(f.call_id);
        dst.put_u32(f.payload.len() as u32);
        dst.extend_from_slice(&f.payload);
        Ok(())
    }
}
