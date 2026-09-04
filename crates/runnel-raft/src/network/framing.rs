use std::io;
use std::mem::size_of;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;
pub(super) const MAX_REUSABLE_FRAME_BUFFER_SIZE: usize = 1024 * 1024;

pub(super) async fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
) -> Result<(), io::Error> {
    let mut frame = Vec::with_capacity(size_of::<u32>());
    frame.extend_from_slice(&[0; size_of::<u32>()]);
    serde_json::to_writer(&mut frame, value).map_err(io::Error::other)?;
    let length = u32::try_from(frame.len() - size_of::<u32>())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "peer RPC is too large"))?;
    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the frame limit",
        ));
    }
    frame[..size_of::<u32>()].copy_from_slice(&length.to_be_bytes());
    stream.write_all(&frame).await
}

pub(super) async fn read_frame<T: DeserializeOwned>(
    stream: &mut TcpStream,
    payload: &mut Vec<u8>,
) -> Result<T, io::Error> {
    let length = stream.read_u32().await?;
    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the frame limit",
        ));
    }
    payload.resize(length as usize, 0);
    stream.read_exact(payload).await?;
    let value = serde_json::from_slice(payload).map_err(io::Error::other)?;
    if payload.capacity() > MAX_REUSABLE_FRAME_BUFFER_SIZE {
        *payload = Vec::new();
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    #[tokio::test]
    async fn writes_big_endian_length_prefix_before_json_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut prefix = [0; size_of::<u32>()];
            stream.read_exact(&mut prefix).await.unwrap();
            let length = u32::from_be_bytes(prefix);
            let mut payload = vec![0; length as usize];
            stream.read_exact(&mut payload).await.unwrap();
            (prefix, payload)
        });

        let mut stream = TcpStream::connect(address).await.unwrap();
        write_frame(&mut stream, &42_u32).await.unwrap();

        let (prefix, payload) = server.await.unwrap();
        assert_eq!(prefix, (payload.len() as u32).to_be_bytes());
        assert_eq!(payload, b"42");
    }

    #[tokio::test]
    async fn rejects_a_frame_above_the_limit_before_reading_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream
                .write_all(&(MAX_FRAME_SIZE + 1).to_be_bytes())
                .await
                .unwrap();
        });

        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut payload = Vec::new();
        let error = read_frame::<u32>(&mut stream, &mut payload)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "peer RPC exceeds the frame limit");
        assert!(payload.is_empty());
        server.await.unwrap();
    }
}
