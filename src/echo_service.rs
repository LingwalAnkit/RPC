use bytes::{BufMut, Bytes, BytesMut};
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
