mod frame;
use frame::*;
use futures::{SinkExt, StreamExt};
//StreamExt → gives you .next()
//SinkExt → gives you .send()
// to send and receive frames
use tokio::net::{TcpListener, TcpStream}; // Incomming connection and TCP Stream
use tokio_util::codec::Framed; // This Gives message/frame interface
// bytes -> codec -> frames and back

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:7000").await?;

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap(); // socket is the tcpstream
        let mut framed = Framed::new(socket, FrameCodec);
        while let Some(Ok(mut f)) = framed.next().await {
            f.kind = Kind::Response; // echo back as a response
            framed.send(f).await.unwrap();
        }
    });

    // These both are concurrent and the socket waits for the tcp stream to be established

    let mut client = Framed::new(TcpStream::connect("127.0.0.1:7000").await?, FrameCodec); // Framed<TcpStream, FrameCodec>
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
