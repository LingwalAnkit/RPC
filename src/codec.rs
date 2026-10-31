use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const MAX_FIELD_LEN: usize = 1024 * 1024; // reject anything over 1 MiB before allocating

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("truncated: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
    #[error("field length {0} exceeds limit")]
    TooLong(usize),
    #[error("invalid utf-8 in string field")]
    BadUtf8,
}

pub trait Message: Sized {
    fn encode(&self, out: &mut BytesMut);
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError>;
}

// Check before every read. Buf::get_u32 panics on short input, and a panic is not an error.
fn need(input: &Bytes, n: usize) -> Result<(), DecodeError> {
    if input.remaining() < n {
        return Err(DecodeError::Truncated {
            need: n,
            have: input.remaining(),
        });
    }
    Ok(())
}

pub fn put_u32(out: &mut BytesMut, v: u32) {
    out.put_u32(v);
}

pub fn get_u32(input: &mut Bytes) -> Result<u32, DecodeError> {
    need(input, 4)?;
    Ok(input.get_u32())
}

pub fn put_str(out: &mut BytesMut, s: &str) {
    out.put_u32(s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

pub fn get_str(input: &mut Bytes) -> Result<String, DecodeError> {
    let len = get_u32(input)? as usize;
    if len > MAX_FIELD_LEN {
        return Err(DecodeError::TooLong(len));
    }
    need(input, len)?;
    let raw = input.split_to(len);
    String::from_utf8(raw.to_vec()).map_err(|_| DecodeError::BadUtf8)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchoRequest {
    pub text: String,
}

impl Message for EchoRequest {
    fn encode(&self, out: &mut BytesMut) {
        put_str(out, &self.text);
    }
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(Self {
            text: get_str(input)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn echo_request_roundtrip() {
        let msg = EchoRequest {
            text: "hello, wörld".into(),
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf);
        let mut bytes = buf.freeze();
        assert_eq!(EchoRequest::decode(&mut bytes).unwrap(), msg);
        assert!(bytes.is_empty());
    }

    #[test]
    fn truncated_input_is_an_error_not_a_panic() {
        let mut buf = BytesMut::new();
        EchoRequest {
            text: "hello".into(),
        }
        .encode(&mut buf);
        let mut short = buf.freeze().slice(..6); // length prefix ok, body cut off
        assert!(matches!(
            EchoRequest::decode(&mut short),
            Err(DecodeError::Truncated { .. })
        ));
    }

    #[test]
    fn oversized_length_is_rejected_before_reading() {
        let mut buf = BytesMut::new();
        buf.put_u32((MAX_FIELD_LEN + 1) as u32);
        let mut bytes = buf.freeze();
        assert_eq!(
            EchoRequest::decode(&mut bytes),
            Err(DecodeError::TooLong(MAX_FIELD_LEN + 1))
        );
    }
}
