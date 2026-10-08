use std::io::{self, Write};
use std::mem::size_of;

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) const FRAME_MEMORY_QUANTUM: usize = 1024 * 1024;
pub(super) const MAX_BUFFERED_FRAME_MEMORY: usize = 192 * 1024 * 1024;
const FRAME_ADMISSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// A maximum public consume-batch body may expand by roughly 4/3 when a peer
// encodes arbitrary payload bytes as base64 inside its bounded JSON RPC frame.
// Keep enough headroom for the JSON envelope and per-record metadata.
pub(super) const MAX_FRAME_SIZE: u32 = 96 * 1024 * 1024;
// Bound retained input buffers across all 256 inbound and 256 outbound peer
// connections to 32 MiB per process. Large one-off frames are dropped after
// parsing instead of remaining attached to otherwise idle connections.
pub(super) const MAX_REUSABLE_FRAME_BUFFER_SIZE: usize = 64 * 1024;

/// Parsed frame and its encoded-length admission charge. The caller keeps this
/// value alive while using the deserialized contents so admission covers the
/// processing lifetime. The charge does not estimate Rust heap size after serde.
pub(super) struct BoundedFrame<T> {
    pub(super) value: T,
    memory_permit: Option<OwnedSemaphorePermit>,
}

impl<T> BoundedFrame<T> {
    pub(super) fn into_parts(self) -> (T, Option<OwnedSemaphorePermit>) {
        (self.value, self.memory_permit)
    }

    #[cfg(test)]
    pub(super) fn unbounded(value: T) -> Self {
        Self {
            value,
            memory_permit: None,
        }
    }
}

pub(super) async fn write_frame<T: Serialize>(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &T,
) -> Result<(), io::Error> {
    let mut frame = Vec::with_capacity(size_of::<u32>());
    frame.extend_from_slice(&[0; size_of::<u32>()]);
    let serialization = {
        let mut writer = BoundedFrameWriter {
            frame: &mut frame,
            max_payload_bytes: MAX_FRAME_SIZE as usize,
            exceeded_limit: false,
        };
        let result = serde_json::to_writer(&mut writer, value);
        (result, writer.exceeded_limit)
    };
    if let Err(error) = serialization.0 {
        if serialization.1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer RPC exceeds the frame limit",
            ));
        }
        return Err(io::Error::other(error));
    }
    let length = u32::try_from(frame.len() - size_of::<u32>())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "peer RPC is too large"))?;
    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the frame limit",
        ));
    }
    frame[..size_of::<u32>()].copy_from_slice(&length.to_be_bytes());
    stream.write_all(&frame).await?;
    stream.flush().await
}

pub(super) async fn write_frame_bounded<T: Serialize, S: AsyncWrite + Unpin>(
    stream: &mut S,
    value: &T,
    write_slots: &Arc<Semaphore>,
    memory_budget: &Arc<Semaphore>,
) -> Result<(), io::Error> {
    let _permit =
        tokio::time::timeout(FRAME_ADMISSION_TIMEOUT, write_slots.clone().acquire_owned())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer frame admission timed out"))?
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "peer frame admission is closed")
            })?;
    let memory_units = (MAX_FRAME_SIZE as usize).div_ceil(FRAME_MEMORY_QUANTUM);
    if memory_units > MAX_BUFFERED_FRAME_MEMORY / FRAME_MEMORY_QUANTUM {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the buffered frame memory limit",
        ));
    }
    let _memory_permit = tokio::time::timeout(
        FRAME_ADMISSION_TIMEOUT,
        memory_budget
            .clone()
            .acquire_many_owned(memory_units as u32),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "peer frame memory admission timed out",
        )
    })?
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "peer frame memory admission is closed",
        )
    })?;
    write_frame(stream, value).await
}

/// Write a response whose decoded result and serialized frame already have a
/// caller-owned weighted memory reservation acquired before dispatch.
pub(super) async fn write_frame_response_bounded<T: Serialize, S: AsyncWrite + Unpin>(
    stream: &mut S,
    value: &T,
    write_slots: &Arc<Semaphore>,
) -> Result<(), io::Error> {
    let _permit =
        tokio::time::timeout(FRAME_ADMISSION_TIMEOUT, write_slots.clone().acquire_owned())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer frame admission timed out"))?
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "peer frame admission is closed")
            })?;
    write_frame(stream, value).await
}

struct BoundedFrameWriter<'a> {
    frame: &'a mut Vec<u8>,
    max_payload_bytes: usize,
    exceeded_limit: bool,
}

impl Write for BoundedFrameWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let payload_len = self.frame.len() - size_of::<u32>();
        if bytes.len() > self.max_payload_bytes.saturating_sub(payload_len) {
            self.exceeded_limit = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer RPC exceeds the frame limit",
            ));
        }
        let required_capacity = self.frame.len() + bytes.len();
        if required_capacity > self.frame.capacity() {
            let current_payload_capacity = self.frame.capacity() - size_of::<u32>();
            let target_payload_capacity = current_payload_capacity
                .saturating_mul(2)
                .max(bytes.len())
                .min(self.max_payload_bytes);
            let target_capacity =
                size_of::<u32>() + target_payload_capacity.max(payload_len + bytes.len());
            self.frame
                .try_reserve_exact(target_capacity.saturating_sub(self.frame.len()))
                .map_err(io::Error::other)?;
        }
        self.frame.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub(super) async fn read_frame<T: DeserializeOwned>(
    stream: &mut (impl AsyncRead + Unpin),
    payload: &mut Vec<u8>,
) -> Result<T, io::Error> {
    read_frame_inner(stream, payload, None)
        .await
        .map(|frame| frame.value)
}

pub(super) async fn read_frame_bounded<T: DeserializeOwned, S: AsyncRead + Unpin>(
    stream: &mut S,
    payload: &mut Vec<u8>,
    memory_budget: &Arc<Semaphore>,
) -> Result<BoundedFrame<T>, io::Error> {
    read_frame_inner(stream, payload, Some(memory_budget)).await
}

async fn read_frame_inner<T: DeserializeOwned, S: AsyncRead + Unpin>(
    stream: &mut S,
    payload: &mut Vec<u8>,
    memory_budget: Option<&Arc<Semaphore>>,
) -> Result<BoundedFrame<T>, io::Error> {
    let length = stream.read_u32().await?;
    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the frame limit",
        ));
    }
    let memory_permit = if let Some(memory_budget) = memory_budget {
        // Charge both the encoded input buffer and an approximate decoded
        // object graph while the request or response is being processed.
        let memory_bytes = (length as usize).saturating_mul(2);
        let units = memory_bytes.div_ceil(FRAME_MEMORY_QUANTUM).max(1);
        if units > MAX_BUFFERED_FRAME_MEMORY / FRAME_MEMORY_QUANTUM {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer RPC exceeds the buffered frame memory limit",
            ));
        }
        Some(
            tokio::time::timeout(
                FRAME_ADMISSION_TIMEOUT,
                memory_budget.clone().acquire_many_owned(units as u32),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer frame admission timed out"))?
            .map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "peer frame admission is closed")
            })?,
        )
    } else {
        None
    };
    payload.resize(length as usize, 0);
    stream.read_exact(payload).await?;
    let value = serde_json::from_slice(payload).map_err(io::Error::other)?;
    if payload.capacity() > MAX_REUSABLE_FRAME_BUFFER_SIZE {
        *payload = Vec::new();
    }
    Ok(BoundedFrame {
        value,
        memory_permit,
    })
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
    async fn frame_memory_permit_lives_with_decoded_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            write_frame(&mut stream, &"peer-frame".to_owned())
                .await
                .unwrap();
        });

        let mut stream = TcpStream::connect(address).await.unwrap();
        let budget = Arc::new(Semaphore::new(1));
        let frame = read_frame_bounded::<String, _>(&mut stream, &mut Vec::new(), &budget)
            .await
            .unwrap();

        assert_eq!(frame.value, "peer-frame");
        assert_eq!(budget.available_permits(), 0);
        drop(frame);
        assert_eq!(budget.available_permits(), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_response_writer_uses_pre_reserved_reply_memory() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut payload = Vec::new();
            let response = read_frame::<String>(&mut stream, &mut payload)
                .await
                .unwrap();
            assert_eq!(response, "bounded-response");
        });

        let mut stream = TcpStream::connect(address).await.unwrap();
        let write_slots = Arc::new(Semaphore::new(1));
        write_frame_response_bounded(&mut stream, &"bounded-response", &write_slots)
            .await
            .unwrap();
        assert_eq!(write_slots.available_permits(), 1);
        server.await.unwrap();
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
        assert_eq!(payload.capacity(), 0);
        server.await.unwrap();
    }

    #[test]
    fn bounded_writer_rejects_before_growing_past_its_limit() {
        let mut frame = vec![0; size_of::<u32>()];
        let mut writer = BoundedFrameWriter {
            frame: &mut frame,
            max_payload_bytes: 3,
            exceeded_limit: false,
        };

        assert!(writer.write_all(b"123").is_ok());
        assert_eq!(
            writer.write_all(b"4").unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(writer.exceeded_limit);
        assert_eq!(frame.len(), size_of::<u32>() + 3);
    }
}
