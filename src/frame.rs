use bytes::{Buf, BufMut, Bytes, BytesMut};
// Bytes → immutable byte data
// BytesMut → mutable byte buffer
// Buf → lets you consume/read bytes
// BufMut → lets you write bytes
use tokio_util::codec::{Decoder, Encoder};

pub const VERSION: u8 = 1; // this for the protocol version
pub const HEADER_LENGHT: usize = 16; // Header is 16 bytes it contains the version, kind, flags, and call_id
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024; // Maximum payload size is 16 MB
pub const END_STREAM: u16 = 0b0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)] // represent this enum as a single u8
pub enum Kind {
    Request = 0,
    Response = 1,
    Error = 2,
    Cancel = 3,
    Data = 4,
}

impl TryFrom<u8> for Kind {
    // given a byte convert it to a Kind enum value
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

    // &mut self is frame codec
    // &mut bytesmut This is the input byte buffer.The decoder reads the encoded frame from it:

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        if src.len() < HEADER_LENGHT {
            return Ok(None);
        }
        // if the bytes are less than the header length, return None
        // if the version is not the expected version, return an error
        if src[0] != VERSION {
            return Err(FrameError::BadVersion(src[0]));
        }

        let kind = Kind::try_from(src[1])?; // try_from converts the byte to a Kind enum value
        let flags = u16::from_be_bytes([src[2], src[3]]);
        let call_id = u64::from_be_bytes(src[4..12].try_into().unwrap()); // 8bytes call id
        let len = u32::from_be_bytes(src[12..16].try_into().unwrap()) as usize;

        if len > MAX_PAYLOAD {
            return Err(FrameError::TooLarge(len));
        }

        // lets say tcp delivered 80 bytes header is 16 and the payload is 100
        // 100 + 16 > 80 so we need to wait for more bytes
        if src.len() < HEADER_LENGHT + len {
            src.reserve(HEADER_LENGHT + len - src.len()); // "We're missing some bytes; prepare space for them."
            return Ok(None); // wait for more bytes
        }
        src.advance(HEADER_LENGHT); // once we have enough bytes, remove the header
        let payload = src.split_to(len).freeze(); // extract the payload
        // freeze() bytemut -> bytes
        Ok(Some(Frame {
            kind,
            flags,
            payload,
            call_id,
        })) // converted to rust object from bytes
    }
}

//teching framecodec how to convert frame into bytes
// frame -> encoder -> bytes -> tcp
impl Encoder<Frame> for FrameCodec {
    type Error = FrameError;

    // &mut self is frame codec
    // &mut bytesmut This is the output byte buffer.The encoder writes the encoded frame into it:
    fn encode(&mut self, f: Frame, dst: &mut BytesMut) -> Result<(), FrameError> {
        if f.payload.len() > MAX_PAYLOAD {
            return Err(FrameError::TooLarge(f.payload.len()));
        }
        dst.reserve(HEADER_LENGHT + f.payload.len()); // reserve space for the header and payload
        dst.put_u8(VERSION);
        dst.put_u8(f.kind as u8);
        dst.put_u16(f.flags);
        dst.put_u64(f.call_id);
        dst.put_u32(f.payload.len() as u32); // write payload length
        dst.extend_from_slice(&f.payload); // write payload
        Ok(())
    }
}
