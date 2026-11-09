use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::time::Duration;

use crate::codec::{DecodeError, EchoRequest, Message, get_str, put_str};
use crate::rpc::{Client, Code, RpcError};

pub const ECHO_METHOD: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EchoResponse {
    pub text: String,
}

impl Message for EchoResponse {
    fn encode(&self, out: &mut BytesMut) {
        put_str(out, &self.text);
    }
    fn decode(input: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(Self {
            text: get_str(input)?,
        })
    }
}

fn bad_request(e: DecodeError) -> RpcError {
    RpcError::new(Code::InvalidArgument, e.to_string())
}

/// Server-side dispatcher for the echo protocol: a u16 method id, then an EchoRequest.
pub async fn dispatch(mut body: Bytes) -> Result<Bytes, RpcError> {
    if body.len() < 2 {
        return Err(RpcError::new(Code::InvalidArgument, "missing method id"));
    }
    let method = body.get_u16();
    if method != ECHO_METHOD {
        return Err(RpcError::new(Code::NotFound, "unknown method"));
    }
    let req = EchoRequest::decode(&mut body).map_err(bad_request)?;
    let mut out = BytesMut::new();
    EchoResponse { text: req.text }.encode(&mut out);
    Ok(out.freeze())
}

#[derive(Clone)]
pub struct EchoClient {
    inner: Client,
}

impl EchoClient {
    pub fn new(inner: Client) -> Self {
        Self { inner }
    }
    pub async fn echo(
        self,
        req: EchoRequest,
        timeout: Option<Duration>,
    ) -> Result<EchoResponse, RpcError> {
        let mut body = BytesMut::new();
        body.put_u16(ECHO_METHOD);
        req.encode(&mut body);
        let mut reply = self.inner.call(&body, timeout).await?;
        EchoResponse::decode(&mut reply).map_err(bad_request)
    }
}
