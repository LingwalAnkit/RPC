mod frame;
use frame::*;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:7000").await?;

    tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(sock, FrameCodec);
        while let Some(Ok(mut f)) = framed.next().await {
            f.kind = Kind::Response; // echo back as a response
            framed.send(f).await.unwrap();
        }
    });

    let mut client = Framed::new(TcpStream::connect("127.0.0.1:7000").await?, FrameCodec);
    client
        .send(Frame {
            kind: Kind::Request,
            flags: END_STREAM,
            call_id: 42,
            payload: "hello".into(),
        })
        .await?;
    println!("{:?}", client.next().await.unwrap()?);
    Ok(())
}
