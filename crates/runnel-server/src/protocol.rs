use std::time::{Duration, Instant};

use runnel_protocol::{ProtocolSupport, Response};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::watch;

const MIN_CONFIGURED_FRAME_BYTES: usize = runnel_protocol::v2::MIN_FRAME_BODY_BYTES;
const MAX_CONFIGURED_REQUEST_BYTES: usize = runnel_protocol::v2::MAX_CLIENT_TO_SERVER_FRAME_BYTES;
const MAX_CONFIGURED_RESPONSE_BYTES: usize = runnel_protocol::v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES;

/// Protocol compatibility declared by the server's current listener.
///
/// The listener negotiates this v2 Protobuf protocol range on each connection.
pub(crate) const PROTOCOL_SUPPORT: ProtocolSupport = runnel_protocol::PROTOCOL_SUPPORT;

const _: () = assert!(PROTOCOL_SUPPORT.versions.min <= PROTOCOL_SUPPORT.versions.max);

#[derive(Clone, Copy)]
pub(crate) struct ProtocolAdmission {
    pub(crate) max_connections: usize,
    pub(crate) max_request_bytes: usize,
    pub(crate) max_response_bytes: usize,
    pub(crate) max_in_flight_requests: usize,
    pub(crate) request_timeout: Duration,
}

pub(crate) fn validate_admission_config(
    max_connections: usize,
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_in_flight_requests: usize,
    request_timeout_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if max_connections == 0 {
        return Err("--max-connections must be greater than zero".into());
    }
    if !(MIN_CONFIGURED_FRAME_BYTES..=MAX_CONFIGURED_REQUEST_BYTES).contains(&max_request_bytes) {
        return Err(format!(
            "--max-request-bytes must be between {MIN_CONFIGURED_FRAME_BYTES} and {MAX_CONFIGURED_REQUEST_BYTES}"
        )
        .into());
    }
    if !(MIN_CONFIGURED_FRAME_BYTES..=MAX_CONFIGURED_RESPONSE_BYTES).contains(&max_response_bytes) {
        return Err(format!(
            "--max-response-bytes must be between {MIN_CONFIGURED_FRAME_BYTES} and {MAX_CONFIGURED_RESPONSE_BYTES}"
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

pub(crate) fn remaining_timeout(started: Instant, timeout: Duration) -> Duration {
    timeout.saturating_sub(started.elapsed())
}

pub(crate) fn invalid_request_response(message: &str) -> Response {
    Response::Error {
        code: "invalid_request".to_owned(),
        message: message.to_owned(),
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

#[cfg(test)]
mod tests {
    use super::PROTOCOL_SUPPORT as SERVER_PROTOCOL_SUPPORT;
    use super::{
        MAX_CONFIGURED_REQUEST_BYTES, MAX_CONFIGURED_RESPONSE_BYTES, MIN_CONFIGURED_FRAME_BYTES,
        validate_admission_config,
    };
    use runnel_client::PROTOCOL_SUPPORT as CLIENT_PROTOCOL_SUPPORT;
    use runnel_protocol::{PROTOCOL_SUPPORT as WIRE_PROTOCOL_SUPPORT, PayloadEncoding};

    #[test]
    fn protocol_support_stays_aligned_across_wire_client_and_server() {
        assert_eq!(SERVER_PROTOCOL_SUPPORT, WIRE_PROTOCOL_SUPPORT);
        assert_eq!(CLIENT_PROTOCOL_SUPPORT, WIRE_PROTOCOL_SUPPORT);
        assert_eq!(SERVER_PROTOCOL_SUPPORT.name, "runnel-protobuf");
        assert_eq!(SERVER_PROTOCOL_SUPPORT.versions.min, 2);
        assert_eq!(SERVER_PROTOCOL_SUPPORT.versions.max, 2);
        assert_eq!(
            SERVER_PROTOCOL_SUPPORT.payload_encodings,
            &[PayloadEncoding::Utf8Text, PayloadEncoding::Binary]
        );
    }

    fn validate_frame_limits(request_bytes: usize, response_bytes: usize) -> Result<(), String> {
        validate_admission_config(1, request_bytes, response_bytes, 1, 1)
            .map_err(|error| error.to_string())
    }

    #[test]
    fn configured_frame_limits_accept_protocol_bounds() {
        assert!(
            validate_frame_limits(MIN_CONFIGURED_FRAME_BYTES, MIN_CONFIGURED_FRAME_BYTES).is_ok()
        );
        assert!(
            validate_frame_limits(MAX_CONFIGURED_REQUEST_BYTES, MAX_CONFIGURED_RESPONSE_BYTES)
                .is_ok()
        );
    }

    #[test]
    fn configured_frame_limits_reject_values_outside_protocol_bounds() {
        assert!(
            validate_frame_limits(MIN_CONFIGURED_FRAME_BYTES - 1, MIN_CONFIGURED_FRAME_BYTES)
                .unwrap_err()
                .contains("--max-request-bytes")
        );
        assert!(
            validate_frame_limits(MAX_CONFIGURED_REQUEST_BYTES + 1, MIN_CONFIGURED_FRAME_BYTES)
                .unwrap_err()
                .contains("--max-request-bytes")
        );
        assert!(
            validate_frame_limits(MIN_CONFIGURED_FRAME_BYTES, MIN_CONFIGURED_FRAME_BYTES - 1)
                .unwrap_err()
                .contains("--max-response-bytes")
        );
        assert!(
            validate_frame_limits(
                MIN_CONFIGURED_FRAME_BYTES,
                MAX_CONFIGURED_RESPONSE_BYTES + 1
            )
            .unwrap_err()
            .contains("--max-response-bytes")
        );
    }
}
