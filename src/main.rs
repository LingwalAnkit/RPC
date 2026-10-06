mod frame;
mod tls;
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
        // server task wiats for the client (main task)
        let (socket, _) = listener.accept().await.unwrap(); // socket and client address
        // socket is the tcp stream
        let mut framed = Framed::new(socket, FrameCodec); // combine socket and codec socket understand bytes and framecodec understand both
        while let Some(Ok(mut f)) = framed.next().await {
            // server waits for incoming frames
            f.kind = Kind::Response; // add response to request
            framed.send(f).await.unwrap(); // send response back to client
        } // decode and encode frames with responses
    });

    // These both are concurrent and the socket waits for the tcp stream to be established

    let mut client = Framed::new(TcpStream::connect("127.0.0.1:7000").await?, FrameCodec); // client as get this Framed<TcpStream, FrameCodec>
    client
        .send(Frame {
            kind: Kind::Request,
            flags: END_STREAM,
            call_id: 42,
            payload: "hello".into(),
        })
        .await?;
    // client sends a request
    println!("{:?}", client.next().await.unwrap()?); // waits for a response
    Ok(())
}

// .send calls FrameCodec.encode() to convert the frame to bytes before sending
// .next() calls FrameCodec.decode() to convert the bytes to a frame after receiving

//CLIENT                              SERVER

// .send(Frame) ─────────────────────► .next()
//                                        │
//                                        │ receives Frame
//                                        ▼
//                                     modify Frame
//                                     Request → Response
//                                        │
//                                        ▼
//                                    .send(Frame)
//         ◄──────────────────────────────┘
// .next()
