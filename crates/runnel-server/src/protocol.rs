use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use runnel_protocol::Response;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::observability::ServerMetrics;

const MAX_CONFIGURED_REQUEST_BYTES: usize = runnel_protocol::MAX_PUBLISH_BATCH_BYTES;

const CONNECTION_REJECTION_WRITE_TIMEOUT: Duration = Duration::from_millis(10);

#[derive(Clone, Copy)]
pub(crate) struct ProtocolAdmission {
    pub(crate) max_connections: usize,
    pub(crate) max_request_bytes: usize,
    pub(crate) max_in_flight_requests: usize,
    pub(crate) request_timeout: Duration,
}

pub(crate) fn validate_admission_config(
    max_connections: usize,
    max_request_bytes: usize,
    max_in_flight_requests: usize,
    request_timeout_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if max_connections == 0 {
        return Err("--max-connections must be greater than zero".into());
    }
    if max_request_bytes == 0 || max_request_bytes > MAX_CONFIGURED_REQUEST_BYTES {
        return Err(format!(
            "--max-request-bytes must be between 1 and {MAX_CONFIGURED_REQUEST_BYTES}"
        )
        .into());
    }
    if max_in_flight_requests == 0 {
        return Err("--max-in-flight-requests must be greater than zero".into());
    }
    if request_timeout_ms == 0 {
        return Err("--request-timeout-ms must be greater than zero".into());
    }
    Ok(())
}

pub(crate) enum Frame {
    End,
    Complete { bytes: Vec<u8> },
    Unterminated { bytes: Vec<u8> },
    TooLarge { bytes: Vec<u8> },
}

pub(crate) async fn wait_for_request_data<R>(
    reader: &mut BufReader<R>,
    shutdown: &mut watch::Receiver<bool>,
) -> std::io::Result<Option<bool>>
where
    R: AsyncRead + Unpin,
{
    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }
        tokio::select! {
            result = reader.fill_buf() => return Ok(Some(!result?.is_empty())),
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(None);
                }
            }
        }
    }
}

pub(crate) async fn read_frame<R>(
    reader: &mut BufReader<R>,
    max_request_bytes: usize,
) -> std::io::Result<Frame>
where
    R: AsyncRead + Unpin,
{
    let max_frame_bytes = max_request_bytes
        .checked_add(1)
        .ok_or_else(|| std::io::Error::other("maximum request size is too large"))?;
    let mut bytes = Vec::with_capacity(max_frame_bytes.min(8 * 1024));

    loop {
        let buffered = reader.fill_buf().await?;
        if buffered.is_empty() {
            return Ok(if bytes.is_empty() {
                Frame::End
            } else {
                Frame::Unterminated { bytes }
            });
        }

        let newline = buffered.iter().position(|byte| *byte == b'\n');
        let bytes_to_consume = newline.map_or(buffered.len(), |index| index + 1);
        if bytes.len().saturating_add(bytes_to_consume) > max_frame_bytes {
            let remaining = max_frame_bytes - bytes.len();
            bytes.extend_from_slice(&buffered[..remaining]);
            reader.consume(remaining);
            return Ok(Frame::TooLarge { bytes });
        }

        bytes.extend_from_slice(&buffered[..bytes_to_consume]);
        reader.consume(bytes_to_consume);
        if newline.is_some() {
            return Ok(Frame::Complete { bytes });
        }
        if bytes.len() == max_frame_bytes {
            return Ok(Frame::TooLarge { bytes });
        }
    }
}

pub(crate) fn request_line(bytes: &[u8]) -> &[u8] {
    let line = &bytes[..bytes.len() - 1];
    line.strip_suffix(b"\r").unwrap_or(line)
}

pub(crate) fn remaining_timeout(started: Instant, timeout: Duration) -> Duration {
    timeout.saturating_sub(started.elapsed())
}

pub(crate) fn response_write_timeout(
    started: Instant,
    request_timeout: Duration,
    response: &Response,
) -> Duration {
    if matches!(response, Response::Error { .. }) {
        return request_timeout;
    }
    remaining_timeout(started, request_timeout)
}

pub(crate) async fn send_response<W>(
    writer: &mut W,
    response: &Response,
    timeout: Duration,
    metrics: &ServerMetrics,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut encoded = serde_json::to_vec(response).map_err(|error| {
        std::io::Error::other(format!("response serialization failed: {error}"))
    })?;
    encoded.push(b'\n');
    match tokio::time::timeout(timeout, writer.write_all(&encoded)).await {
        Ok(result) => result?,
        Err(_) => {
            metrics
                .response_write_timeouts
                .fetch_add(1, Ordering::Relaxed);
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "response write timed out",
            ));
        }
    }
    metrics
        .response_bytes
        .fetch_add(encoded.len() as u64, Ordering::Relaxed);
    Ok(())
}

pub(crate) async fn reject_connection(stream: TcpStream) {
    // A rejected connection is not assigned a task. try_write keeps the
    // accept loop from being held by a client that will not read the error.
    let response = b"{\"type\":\"error\",\"code\":\"connection_limit\",\"message\":\"maximum client connections reached\"}\n";
    if let Err(error) = stream.try_write(response)
        && error.kind() == std::io::ErrorKind::WouldBlock
    {
        let _ = tokio::time::timeout(CONNECTION_REJECTION_WRITE_TIMEOUT, stream.writable()).await;
        let _ = stream.try_write(response);
    }
}

pub(crate) fn invalid_request_response(message: &str) -> Response {
    Response::Error {
        code: "invalid_request".to_owned(),
        message: message.to_owned(),
    }
}

pub(crate) fn request_size_response(max_request_bytes: usize) -> Response {
    Response::Error {
        code: "request_too_large".to_owned(),
        message: format!(
            "request frame exceeds the configured maximum of {max_request_bytes} bytes"
        ),
    }
}

pub(crate) fn saturated_response() -> Response {
    Response::Error {
        code: "request_saturated".to_owned(),
        message: "maximum in-flight request work is currently active".to_owned(),
    }
}

pub(crate) fn timeout_response() -> Response {
    Response::Error {
        code: "request_timeout".to_owned(),
        message: "request exceeded the configured timeout".to_owned(),
    }
}
