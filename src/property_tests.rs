//! Property tests for the frame codec and the echo message codec. Each test runs
//! hundreds of random inputs: round trips must be exact, and decoding arbitrary
//! bytes must never panic.

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    codec::{EchoRequest, Message},
    frame::{Frame, FrameCodec, Kind},
};

fn kind_strategy() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::Request),
        Just(Kind::Response),
        Just(Kind::Error),
        Just(Kind::Cancel),
        Just(Kind::Data),
    ]
}

fn frame_strategy() -> impl Strategy<Value = Frame> {
    (
        kind_strategy(),
        any::<u16>(),
        any::<u64>(),
        prop::collection::vec(any::<u8>(), 0..4096),
    )
        .prop_map(|(kind, flags, call_id, payload)| Frame {
            kind,
            flags,
            call_id,
            payload: Bytes::from(payload),
        })
}

fn encode(frame: Frame) -> BytesMut {
    let mut codec = FrameCodec;
    let mut wire = BytesMut::new();
    codec.encode(frame, &mut wire).expect("encode failed");
    wire
}

proptest! {
    #[test]
    fn frame_roundtrips_exactly(frame in frame_strategy()) {
        let mut codec = FrameCodec;
        let mut wire = encode(frame.clone());
        let decoded = codec.decode(&mut wire).expect("decode failed");
        prop_assert_eq!(decoded, Some(frame));
        prop_assert!(wire.is_empty(), "decoder left {} bytes behind", wire.len());
    }

    #[test]
    fn incomplete_frames_wait_for_more_bytes(frame in frame_strategy(), cut in any::<usize>()) {
        let full = encode(frame);
        // Any strict prefix of a frame is incomplete: the decoder must ask for more.
        let cut = cut % full.len();
        let mut codec = FrameCodec;
        let mut partial = BytesMut::from(&full[..cut]);
        prop_assert!(codec.decode(&mut partial).expect("decode failed").is_none());
    }

    #[test]
    fn decoder_never_panics_on_arbitrary_bytes(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let mut codec = FrameCodec;
        let mut buf = BytesMut::from(&bytes[..]);
        // Each Ok(Some(_)) consumes at least a 16-byte header, so this terminates.
        while let Ok(Some(_)) = codec.decode(&mut buf) {}
    }

    #[test]
    fn echo_request_roundtrips_exactly(text in any::<String>()) {
        let msg = EchoRequest { text };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf);
        let mut bytes = buf.freeze();
        let back = EchoRequest::decode(&mut bytes).expect("decode failed");
        prop_assert_eq!(back, msg);
        prop_assert!(bytes.is_empty());
    }

    #[test]
    fn echo_decode_never_panics_on_arbitrary_bytes(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let mut input = Bytes::from(bytes);
        let _ = EchoRequest::decode(&mut input);
    }
}
