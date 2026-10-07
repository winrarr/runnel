use std::net::SocketAddr;

use runnel_protocol::Response;
use runnel_protocol::v2::{self, ServerFrame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, tcp::OwnedWriteHalf};

pub async fn exchange_one_request(
    client: TcpStream,
    broker_addr: SocketAddr,
) -> (OwnedWriteHalf, Vec<u8>) {
    let (mut client_reader, mut client_writer) = client.into_split();
    let broker = TcpStream::connect(broker_addr)
        .await
        .expect("proxy should connect to the broker");
    let (mut broker_reader, mut broker_writer) = broker.into_split();

    let mut preface = [0; v2::PREFACE.len()];
    client_reader
        .read_exact(&mut preface)
        .await
        .expect("proxy should read the v2 preface");
    assert_eq!(preface, v2::PREFACE);
    broker_writer
        .write_all(&preface)
        .await
        .expect("proxy should forward the v2 preface");

    let client_hello = read_frame(&mut client_reader, v2::HELLO_MAX_BODY_BYTES).await;
    broker_writer
        .write_all(&client_hello)
        .await
        .expect("proxy should forward the client Hello");
    let server_hello = read_frame(&mut broker_reader, v2::HELLO_MAX_BODY_BYTES).await;
    let v2::ServerHello::Accepted(accepted) =
        v2::decode_server_hello(&server_hello[4..]).expect("broker Hello should decode")
    else {
        panic!("the broker should accept core v2 negotiation");
    };
    client_writer
        .write_all(&server_hello)
        .await
        .expect("proxy should forward the server Hello");

    if accepted.auth_required.unwrap_or(false) {
        let auth = read_frame(&mut client_reader, v2::AUTH_MAX_BODY_BYTES).await;
        broker_writer
            .write_all(&auth)
            .await
            .expect("proxy should forward client authentication");
        let authenticated = read_frame(&mut broker_reader, v2::AUTH_MAX_BODY_BYTES).await;
        client_writer
            .write_all(&authenticated)
            .await
            .expect("proxy should forward the authentication result");
    }

    let request = read_frame(&mut client_reader, accepted.client_to_server_frame_bytes).await;
    broker_writer
        .write_all(&request)
        .await
        .expect("proxy should forward the application request");
    let response = read_frame(&mut broker_reader, accepted.server_to_client_frame_bytes).await;
    (client_writer, response)
}

pub fn decode_application_response(frame: &[u8]) -> Response {
    assert!(frame.len() >= 5);
    let body_bytes = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
    assert_eq!(frame.len(), 4 + body_bytes);
    match v2::decode_server_frame(&frame[4..], v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES).unwrap() {
        ServerFrame::Application(reply) => reply.response,
        ServerFrame::Authenticated | ServerFrame::AuthenticationFailed => {
            panic!("proxy should capture an application response")
        }
    }
}

async fn read_frame<R>(reader: &mut R, maximum_body_bytes: usize) -> Vec<u8>
where
    R: AsyncRead + Unpin,
{
    let mut length_bytes = [0; 4];
    reader
        .read_exact(&mut length_bytes)
        .await
        .expect("proxy should read the v2 frame length");
    let body_bytes = u32::from_be_bytes(length_bytes) as usize;
    assert!(body_bytes > 0 && body_bytes <= maximum_body_bytes);
    let mut frame = Vec::with_capacity(4 + body_bytes);
    frame.extend_from_slice(&length_bytes);
    frame.resize(4 + body_bytes, 0);
    reader
        .read_exact(&mut frame[4..])
        .await
        .expect("proxy should read the complete v2 frame");
    frame
}
