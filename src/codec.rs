use bytes::{Buf, BufMut, Bytes, BytesMut};
// Bytes: a box you can read from but not change. Data arrives in one of these.
// BytesMut: a box you can add to. You build outgoing data in one of these.
// Buf: a set of “read” helpers, like “give me the next 4 bytes as a number.”
// BufMut: a set of “write” helpers, like “append this number as 4 bytes.”

pub const MAX_FIELD_LEN: usize = 1024 * 1024; // reject anything over 1 MiB before allocating
// text field may be bigger than 1 MiB (about one million bytes)

#[derive(Debug, thiserror::Error, PartialEq, Eq)]

// These are instructions to the compiler. Debug lets us print the error. PartialEq and Eq let us compare two errors with ==, which the tests need. thiserror::Error generates the boilerplate that makes this an official error type.

// enum on of these three cases

pub enum DecodeError {
    #[error("truncated: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize }, // data ended early needed n bytes but only got m
    #[error("field length {0} exceeds limit")]
    TooLong(usize), // length field was bigger than our limit.
    #[error("invalid utf-8 in string field")]
    BadUtf8, // the bytes were supposed to be text but aren’t valid text.
}

pub trait Message: Sized {
    // A trait is a promise: “any type that implements me can do these things.”
    // sized means size is known at complie time
    fn encode(&self, out: &mut BytesMut); // take self and a box you can add to
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError>; // Reads bytes and reconstructs the struct.
}

// Check before every read. Buf::get_u32 panics on short input, and a panic is not an error.
fn need(input: &Bytes, n: usize) -> Result<(), DecodeError> {
    if input.remaining() < n {
        // if the bytes are fewer than n show truncate error
        return Err(DecodeError::Truncated {
            need: n,
            have: input.remaining(),
        });
    }
    Ok(())
}

pub fn get_u32(input: &mut Bytes) -> Result<u32, DecodeError> {
    need(input, 4)?; // read the buffer
    Ok(input.get_u32())
}

pub fn put_str(out: &mut BytesMut, s: &str) {
    out.put_u32(s.len() as u32); // calculate the string len and write as 4 bytes
    out.extend_from_slice(s.as_bytes()); // converts the string into a UTF-8 byte representation.
    // extend from slice append the data at the end of the buffer
}

pub fn get_str(input: &mut Bytes) -> Result<String, DecodeError> {
    let len = get_u32(input)? as usize;
    if len > MAX_FIELD_LEN {
        return Err(DecodeError::TooLong(len));
    }
    need(input, len)?;
    let raw = input.split_to(len); // split_to(len) takes the first len bytes from the buffer and removes them from the original buffer's readable portion.
    String::from_utf8(raw.to_vec()).map_err(|_| DecodeError::BadUtf8)
    // raw.to_vec() bytes to vector Vec<u8>
    // String::from_utf8 convert bytes to string
    // If the bytes aren't valid UTF-8, convert the conversion error into your own BadUtf8 error.
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
