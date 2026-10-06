use std::fmt;

use prost::Message;
use thiserror::Error;
use zeroize::Zeroize;

use crate::{BearerToken, Request, Response};

#[allow(dead_code)]
mod wire {
    include!(concat!(env!("OUT_DIR"), "/runnel.v2.rs"));
}

pub const PREFACE: [u8; 8] = *b"RNLN\x01\0\0\0";
pub const HELLO_MAX_BODY_BYTES: usize = 16 * 1024;
pub const AUTH_MAX_BODY_BYTES: usize = 1024;
pub const MIN_FRAME_BODY_BYTES: usize = 1024;
pub const MAX_CLIENT_TO_SERVER_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SERVER_TO_CLIENT_FRAME_BYTES: usize = 65 * 1024 * 1024;
pub const DEFAULT_CLIENT_TO_SERVER_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_SERVER_TO_CLIENT_FRAME_BYTES: usize = 65 * 1024 * 1024;
pub const CURRENT_MAJOR: u32 = 2;
pub const CURRENT_MINOR: u32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRange {
    pub major: u32,
    pub min_minor: u32,
    pub max_minor: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHello {
    pub versions: Vec<VersionRange>,
    pub offered_capabilities: Vec<String>,
    pub required_capabilities: Vec<String>,
    pub max_outbound_frame_bytes: usize,
    pub max_inbound_frame_bytes: usize,
}

impl ClientHello {
    pub fn core_v2() -> Self {
        Self {
            versions: vec![VersionRange {
                major: CURRENT_MAJOR,
                min_minor: CURRENT_MINOR,
                max_minor: CURRENT_MINOR,
            }],
            offered_capabilities: Vec::new(),
            required_capabilities: Vec::new(),
            max_outbound_frame_bytes: DEFAULT_CLIENT_TO_SERVER_FRAME_BYTES,
            max_inbound_frame_bytes: DEFAULT_SERVER_TO_CLIENT_FRAME_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalCode {
    InvalidHello,
    UnsupportedVersion,
    UnsupportedCapability,
    LimitTooSmall,
    LimitTooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloAccepted {
    pub major: u32,
    pub minor: u32,
    pub capabilities: Vec<String>,
    pub max_inbound_frame_bytes: usize,
    pub max_outbound_frame_bytes: usize,
    pub client_to_server_frame_bytes: usize,
    pub server_to_client_frame_bytes: usize,
    /// Presence is required by core v2, independently of its boolean value.
    pub auth_required: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerHello {
    Accepted(HelloAccepted),
    Refused {
        code: RefusalCode,
        diagnostic: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Confirmed,
    Rejected,
    Retryable,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Received,
    Validated,
    ExecutionStarted,
    Durable,
    Completed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ItemMetadata {
    pub outcome: Outcome,
    pub stage: Stage,
}

/// One decoded operation result, retaining its safety metadata separately
/// from the operation-specific detail.
#[derive(Debug, Clone)]
pub struct ApplicationReply {
    pub response: Response,
    /// Completed batches have no aggregate outcome or stage.
    pub outcome: Option<Outcome>,
    pub stage: Option<Stage>,
    /// Per-record or per-receipt metadata for completed batch operations.
    pub items: Vec<ItemMetadata>,
}

impl ApplicationReply {
    pub fn confirmed(response: Response, state_changing: bool) -> Self {
        Self {
            response,
            outcome: Some(Outcome::Confirmed),
            stage: Some(if state_changing {
                Stage::Durable
            } else {
                Stage::Completed
            }),
            items: Vec::new(),
        }
    }

    pub fn failed(response: Response, outcome: Outcome, stage: Stage) -> Self {
        Self {
            response,
            outcome: Some(outcome),
            stage: Some(stage),
            items: Vec::new(),
        }
    }

    pub fn batch(response: Response, items: Vec<ItemMetadata>) -> Self {
        Self {
            response,
            outcome: None,
            stage: None,
            items,
        }
    }
}

pub enum ClientFrame {
    BearerAuth(BearerToken),
    Application(Request),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientFrameKind {
    BearerAuth,
    Application,
    Missing,
}

impl fmt::Debug for ClientFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BearerAuth(_) => formatter.write_str("BearerAuth([REDACTED])"),
            Self::Application(request) => formatter
                .debug_tuple("Application")
                .field(request)
                .finish(),
        }
    }
}

#[derive(Debug)]
pub enum ServerFrame {
    Authenticated,
    AuthenticationFailed,
    Application(ApplicationReply),
}

/// A framed Protobuf body. Secret-bearing frames redact formatting and clear
/// their bytes when dropped.
pub struct EncodedFrame {
    bytes: Vec<u8>,
    contains_secret: bool,
}

impl EncodedFrame {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl fmt::Debug for EncodedFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedFrame")
            .field("bytes", &"[REDACTED]")
            .field("len", &self.bytes.len())
            .finish()
    }
}

impl Drop for EncodedFrame {
    fn drop(&mut self) {
        if self.contains_secret {
            self.bytes.zeroize();
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum V2ProtocolError {
    #[error("invalid v2 protocol frame")]
    InvalidFrame,
    #[error("v2 frame exceeds its configured maximum")]
    FrameTooLarge,
    #[error("v2 frame contains no recognized message")]
    MissingMessage,
    #[error("v2 request contains no recognized operation")]
    MissingOperation,
    #[error("v2 request operation is unsupported")]
    UnsupportedOperation,
    #[error("v2 operation fields are invalid")]
    InvalidOperation,
    #[error("v2 response fields are invalid")]
    InvalidResponse,
    #[error("v2 Hello fields are invalid")]
    InvalidHello,
}

pub fn validate_client_hello(hello: &ClientHello) -> Result<(), RefusalCode> {
    if hello.versions.is_empty() {
        return Err(RefusalCode::InvalidHello);
    }
    let mut majors = std::collections::BTreeSet::new();
    for range in &hello.versions {
        if range.major == 0 || range.min_minor > range.max_minor || !majors.insert(range.major) {
            return Err(RefusalCode::InvalidHello);
        }
    }
    let offered = validate_capability_set(&hello.offered_capabilities)?;
    let required = validate_capability_set(&hello.required_capabilities)?;
    if !required.is_subset(&offered) {
        return Err(RefusalCode::InvalidHello);
    }
    validate_limit(
        hello.max_outbound_frame_bytes,
        MAX_CLIENT_TO_SERVER_FRAME_BYTES,
    )?;
    validate_limit(
        hello.max_inbound_frame_bytes,
        MAX_SERVER_TO_CLIENT_FRAME_BYTES,
    )?;
    Ok(())
}

fn validate_capability_set(values: &[String]) -> Result<std::collections::BTreeSet<&str>, RefusalCode> {
    let mut set = std::collections::BTreeSet::new();
    for value in values {
        let bytes = value.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 64
            || !bytes[0].is_ascii_lowercase()
            || !bytes[1..]
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
            || !set.insert(value.as_str())
        {
            return Err(RefusalCode::InvalidHello);
        }
    }
    Ok(set)
}

fn validate_limit(value: usize, maximum: usize) -> Result<(), RefusalCode> {
    if value < MIN_FRAME_BODY_BYTES {
        return Err(RefusalCode::LimitTooSmall);
    }
    if value > maximum {
        return Err(RefusalCode::LimitTooLarge);
    }
    Ok(())
}

pub fn encode_client_hello(hello: &ClientHello) -> Result<EncodedFrame, V2ProtocolError> {
    let message = wire::HelloFrame {
        message: Some(wire::hello_frame::Message::Client(hello_to_wire(hello)?)),
    };
    encode_message(&message, HELLO_MAX_BODY_BYTES, false)
}

pub fn decode_client_hello(body: &[u8]) -> Result<ClientHello, V2ProtocolError> {
    let frame = decode_message::<wire::HelloFrame>(body, HELLO_MAX_BODY_BYTES)?;
    match frame.message {
        Some(wire::hello_frame::Message::Client(hello)) => hello_from_wire(hello),
        _ => Err(V2ProtocolError::MissingMessage),
    }
}

pub fn encode_server_hello(hello: &ServerHello) -> Result<EncodedFrame, V2ProtocolError> {
    let result = match hello {
        ServerHello::Accepted(accepted) => {
            wire::server_hello::Result::Accepted(wire::HelloAccepted {
                major: accepted.major,
                minor: accepted.minor,
                capabilities: accepted.capabilities.clone(),
                max_inbound_frame_bytes: to_u32(accepted.max_inbound_frame_bytes)?,
                max_outbound_frame_bytes: to_u32(accepted.max_outbound_frame_bytes)?,
                client_to_server_frame_bytes: to_u32(accepted.client_to_server_frame_bytes)?,
                server_to_client_frame_bytes: to_u32(accepted.server_to_client_frame_bytes)?,
                auth_required: accepted.auth_required,
            })
        }
        ServerHello::Refused { code, diagnostic } => {
            wire::server_hello::Result::Refusal(wire::HelloRefusal {
                code: refusal_to_wire(*code),
                diagnostic: diagnostic.clone(),
            })
        }
    };
    let message = wire::HelloFrame {
        message: Some(wire::hello_frame::Message::Server(wire::ServerHello {
            result: Some(result),
        })),
    };
    encode_message(&message, HELLO_MAX_BODY_BYTES, false)
}

pub fn decode_server_hello(body: &[u8]) -> Result<ServerHello, V2ProtocolError> {
    let frame = decode_message::<wire::HelloFrame>(body, HELLO_MAX_BODY_BYTES)?;
    let Some(wire::hello_frame::Message::Server(server)) = frame.message else {
        return Err(V2ProtocolError::MissingMessage);
    };
    match server.result.ok_or(V2ProtocolError::MissingMessage)? {
        wire::server_hello::Result::Accepted(accepted) => Ok(ServerHello::Accepted(
            HelloAccepted {
                major: accepted.major,
                minor: accepted.minor,
                capabilities: accepted.capabilities,
                max_inbound_frame_bytes: accepted.max_inbound_frame_bytes as usize,
                max_outbound_frame_bytes: accepted.max_outbound_frame_bytes as usize,
                client_to_server_frame_bytes: accepted.client_to_server_frame_bytes as usize,
                server_to_client_frame_bytes: accepted.server_to_client_frame_bytes as usize,
                auth_required: accepted.auth_required,
            },
        )),
        wire::server_hello::Result::Refusal(refusal) => Ok(ServerHello::Refused {
            code: refusal_from_wire(refusal.code)?,
            diagnostic: refusal.diagnostic,
        }),
    }
}

pub fn encode_client_frame(frame: &ClientFrame) -> Result<EncodedFrame, V2ProtocolError> {
    let body = match frame {
        ClientFrame::BearerAuth(token) => return encode_bearer_auth(token),
        ClientFrame::Application(request) => {
            wire::client_frame::Body::Application(wire::ApplicationRequest {
                operation: Some(request_to_wire(request)?),
            })
        }
    };
    let frame = wire::ClientFrame { body: Some(body) };
    encode_message(&frame, MAX_CLIENT_TO_SERVER_FRAME_BYTES, false)
}

pub fn encode_bearer_auth(token: &BearerToken) -> Result<EncodedFrame, V2ProtocolError> {
    let mut frame = wire::ClientFrame {
        body: Some(wire::client_frame::Body::BearerAuth(wire::BearerAuth {
            token: token.expose_secret().to_owned(),
        })),
    };
    let encoded = encode_message(&frame, AUTH_MAX_BODY_BYTES, true);
    if let Some(wire::client_frame::Body::BearerAuth(auth)) = frame.body.as_mut() {
        auth.token.zeroize();
    }
    encoded
}

pub fn encode_application_request(request: &Request) -> Result<EncodedFrame, V2ProtocolError> {
    let frame = wire::ClientFrame {
        body: Some(wire::client_frame::Body::Application(
            wire::ApplicationRequest {
                operation: Some(request_to_wire(request)?),
            },
        )),
    };
    encode_message(&frame, MAX_CLIENT_TO_SERVER_FRAME_BYTES, false)
}

pub fn decode_client_frame(body: &[u8], max_bytes: usize) -> Result<ClientFrame, V2ProtocolError> {
    if client_frame_kind(body)? == ClientFrameKind::BearerAuth
        && body.len() > AUTH_MAX_BODY_BYTES
    {
        return Err(V2ProtocolError::FrameTooLarge);
    }
    let frame = decode_message::<wire::ClientFrame>(body, max_bytes)?;
    match frame.body.ok_or(V2ProtocolError::MissingMessage)? {
        wire::client_frame::Body::BearerAuth(auth) => {
            Ok(ClientFrame::BearerAuth(BearerToken::from_wire(auth.token)))
        }
        wire::client_frame::Body::Application(application) => {
            let operation = application.operation.ok_or(V2ProtocolError::MissingOperation)?;
            Ok(ClientFrame::Application(request_from_wire(operation)?))
        }
    }
}

/// Identify the ClientFrame oneof without allocating strings or nested
/// messages. This lets receivers enforce the bearer control-frame cap before
/// Protobuf decoding allocates the token field.
pub fn client_frame_kind(body: &[u8]) -> Result<ClientFrameKind, V2ProtocolError> {
    if body.is_empty() {
        return Err(V2ProtocolError::FrameTooLarge);
    }
    let mut cursor = 0;
    let mut kind = ClientFrameKind::Missing;
    while cursor < body.len() {
        let key = read_varint(body, &mut cursor)?;
        let field_number = key >> 3;
        let wire_type = (key & 0x07) as u8;
        if field_number == 0 || field_number > 0x1fff_ffff {
            return Err(V2ProtocolError::InvalidFrame);
        }
        if wire_type == 2 {
            let length = read_varint(body, &mut cursor)?;
            let end = cursor
                .checked_add(usize::try_from(length).map_err(|_| V2ProtocolError::InvalidFrame)?)
                .filter(|end| *end <= body.len())
                .ok_or(V2ProtocolError::InvalidFrame)?;
            match field_number {
                1 => kind = ClientFrameKind::BearerAuth,
                2 => kind = ClientFrameKind::Application,
                _ => {}
            }
            cursor = end;
            continue;
        }
        match wire_type {
            0 => {
                read_varint(body, &mut cursor)?;
            }
            1 => cursor = cursor.checked_add(8).ok_or(V2ProtocolError::InvalidFrame)?,
            5 => cursor = cursor.checked_add(4).ok_or(V2ProtocolError::InvalidFrame)?,
            _ => return Err(V2ProtocolError::InvalidFrame),
        }
        if cursor > body.len() {
            return Err(V2ProtocolError::InvalidFrame);
        }
    }
    Ok(kind)
}

fn read_varint(body: &[u8], cursor: &mut usize) -> Result<u64, V2ProtocolError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *body.get(*cursor).ok_or(V2ProtocolError::InvalidFrame)?;
        *cursor += 1;
        if shift == 63 && byte > 1 {
            return Err(V2ProtocolError::InvalidFrame);
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(V2ProtocolError::InvalidFrame)
}

pub fn encode_server_frame(frame: &ServerFrame) -> Result<EncodedFrame, V2ProtocolError> {
    let body = match frame {
        ServerFrame::Authenticated => {
            wire::server_frame::Body::Authenticated(wire::Authenticated {})
        }
        ServerFrame::AuthenticationFailed => {
            wire::server_frame::Body::AuthenticationFailed(wire::AuthenticationFailed {})
        }
        ServerFrame::Application(reply) => wire::server_frame::Body::Application(reply_to_wire(reply)?),
    };
    encode_message(
        &wire::ServerFrame { body: Some(body) },
        MAX_SERVER_TO_CLIENT_FRAME_BYTES,
        false,
    )
}

pub fn decode_server_frame(body: &[u8], max_bytes: usize) -> Result<ServerFrame, V2ProtocolError> {
    let frame = decode_message::<wire::ServerFrame>(body, max_bytes)?;
    match frame.body.ok_or(V2ProtocolError::MissingMessage)? {
        wire::server_frame::Body::Authenticated(_) => Ok(ServerFrame::Authenticated),
        wire::server_frame::Body::AuthenticationFailed(_) => {
            Ok(ServerFrame::AuthenticationFailed)
        }
        wire::server_frame::Body::Application(reply) => {
            Ok(ServerFrame::Application(reply_from_wire(reply)?))
        }
    }
}

pub fn encode_delimited<M: Message>(message: &M, max_bytes: usize) -> Result<EncodedFrame, V2ProtocolError> {
    encode_message(message, max_bytes, false)
}

pub fn decode_body<M: Message + Default>(body: &[u8], max_bytes: usize) -> Result<M, V2ProtocolError> {
    decode_message(body, max_bytes)
}

fn encode_message<M: Message>(message: &M, max_bytes: usize, contains_secret: bool) -> Result<EncodedFrame, V2ProtocolError> {
    let body_len = message.encoded_len();
    if body_len == 0 || body_len > max_bytes || body_len > u32::MAX as usize {
        return Err(V2ProtocolError::FrameTooLarge);
    }
    let mut bytes = Vec::with_capacity(4 + body_len);
    bytes.extend_from_slice(&(body_len as u32).to_be_bytes());
    if message.encode(&mut bytes).is_err() {
        if contains_secret {
            bytes.zeroize();
        }
        return Err(V2ProtocolError::InvalidFrame);
    }
    Ok(EncodedFrame {
        bytes,
        contains_secret,
    })
}

fn decode_message<M: Message + Default>(body: &[u8], max_bytes: usize) -> Result<M, V2ProtocolError> {
    if body.is_empty() || body.len() > max_bytes || body.len() > u32::MAX as usize {
        return Err(V2ProtocolError::FrameTooLarge);
    }
    M::decode(body).map_err(|_| V2ProtocolError::InvalidFrame)
}

fn hello_to_wire(hello: &ClientHello) -> Result<wire::ClientHello, V2ProtocolError> {
    Ok(wire::ClientHello {
        versions: hello
            .versions
            .iter()
            .map(|range| {
                Ok(wire::VersionRange {
                    major: range.major,
                    min_minor: range.min_minor,
                    max_minor: range.max_minor,
                })
            })
            .collect::<Result<_, V2ProtocolError>>()?,
        offered_capabilities: hello.offered_capabilities.clone(),
        required_capabilities: hello.required_capabilities.clone(),
        max_outbound_frame_bytes: to_u32(hello.max_outbound_frame_bytes)?,
        max_inbound_frame_bytes: to_u32(hello.max_inbound_frame_bytes)?,
    })
}

fn hello_from_wire(hello: wire::ClientHello) -> Result<ClientHello, V2ProtocolError> {
    Ok(ClientHello {
        versions: hello
            .versions
            .into_iter()
            .map(|range| VersionRange {
                major: range.major,
                min_minor: range.min_minor,
                max_minor: range.max_minor,
            })
            .collect(),
        offered_capabilities: hello.offered_capabilities,
        required_capabilities: hello.required_capabilities,
        max_outbound_frame_bytes: hello.max_outbound_frame_bytes as usize,
        max_inbound_frame_bytes: hello.max_inbound_frame_bytes as usize,
    })
}

fn to_u32(value: usize) -> Result<u32, V2ProtocolError> {
    u32::try_from(value).map_err(|_| V2ProtocolError::InvalidHello)
}

fn refusal_to_wire(code: RefusalCode) -> i32 {
    match code {
        RefusalCode::InvalidHello => wire::RefusalCode::InvalidHello as i32,
        RefusalCode::UnsupportedVersion => wire::RefusalCode::UnsupportedVersion as i32,
        RefusalCode::UnsupportedCapability => wire::RefusalCode::UnsupportedCapability as i32,
        RefusalCode::LimitTooSmall => wire::RefusalCode::LimitTooSmall as i32,
        RefusalCode::LimitTooLarge => wire::RefusalCode::LimitTooLarge as i32,
    }
}

fn refusal_from_wire(value: i32) -> Result<RefusalCode, V2ProtocolError> {
    match wire::RefusalCode::try_from(value) {
        Ok(wire::RefusalCode::InvalidHello) => Ok(RefusalCode::InvalidHello),
        Ok(wire::RefusalCode::UnsupportedVersion) => Ok(RefusalCode::UnsupportedVersion),
        Ok(wire::RefusalCode::UnsupportedCapability) => Ok(RefusalCode::UnsupportedCapability),
        Ok(wire::RefusalCode::LimitTooSmall) => Ok(RefusalCode::LimitTooSmall),
        Ok(wire::RefusalCode::LimitTooLarge) => Ok(RefusalCode::LimitTooLarge),
        _ => Err(V2ProtocolError::InvalidHello),
    }
}

fn request_to_wire(request: &Request) -> Result<wire::application_request::Operation, V2ProtocolError> {
    use wire::application_request::Operation;
    Ok(match request {
        Request::CreateStream { stream } => Operation::CreateStream(wire::CreateStreamRequest {
            stream: stream.clone(),
        }),
        Request::Publish {
            stream,
            key,
            payload,
            request_id,
        } => Operation::Publish(wire::PublishRequest {
            stream: stream.clone(),
            key: key.clone(),
            payload: payload.as_bytes().to_vec(),
            request_id: request_id.clone(),
        }),
        Request::PublishBytes {
            stream,
            key,
            payload,
            request_id,
        } => Operation::Publish(wire::PublishRequest {
            stream: stream.clone(),
            key: key.clone(),
            payload: payload.as_bytes().to_vec(),
            request_id: request_id.clone(),
        }),
        Request::PublishBatch { stream, records } => {
            Operation::PublishBatch(wire::PublishBatchRequest {
                stream: stream.clone(),
                records: records
                    .iter()
                    .map(|record| wire::PublishBatchRecord {
                        key: record.key.clone(),
                        payload: record.payload.as_bytes().to_vec(),
                        request_id: record.request_id.clone(),
                    })
                    .collect(),
            })
        }
        Request::Poll { stream, consumer } => Operation::Poll(wire::PollRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
        }),
        Request::PollBatch {
            stream,
            consumer,
            max_records,
            max_bytes,
            max_wait_ms,
        } => Operation::PollBatch(wire::PollBatchRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            max_records: to_u32(*max_records)?,
            max_bytes: to_u32(*max_bytes)?,
            max_wait_ms: *max_wait_ms,
        }),
        Request::Replay {
            stream,
            consumer,
            offset,
        } => Operation::Replay(wire::ReplayRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            offset: *offset,
        }),
        Request::PollGroup {
            stream,
            consumer,
            member,
        } => Operation::PollGroup(wire::PollGroupRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            member: member.clone(),
        }),
        Request::PollGroupBatch {
            stream,
            consumer,
            member,
            max_records,
            max_bytes,
            max_wait_ms,
        } => Operation::PollGroupBatch(wire::PollGroupBatchRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            member: member.clone(),
            max_records: to_u32(*max_records)?,
            max_bytes: to_u32(*max_bytes)?,
            max_wait_ms: *max_wait_ms,
        }),
        Request::ConfigureConsumer {
            stream,
            consumer,
            ack_timeout_ms,
            max_delivery_attempts,
        } => Operation::ConfigureConsumer(wire::ConfigureConsumerRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            ack_timeout_ms: *ack_timeout_ms,
            max_delivery_attempts: *max_delivery_attempts,
        }),
        Request::InspectConsumer { stream, consumer } => {
            Operation::InspectConsumer(wire::InspectConsumerRequest {
                stream: stream.clone(),
                consumer: consumer.clone(),
            })
        }
        Request::Ack {
            stream,
            consumer,
            offset,
        } => Operation::Ack(wire::AckRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            offset: *offset,
        }),
        Request::AckBatch {
            stream,
            consumer,
            receipts,
        } => Operation::AckBatch(wire::AckBatchRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            receipts: receipts
                .iter()
                .map(|receipt| wire::DeliveryReceipt {
                    offset: receipt.offset,
                    delivery_token: receipt.delivery_token.clone(),
                })
                .collect(),
        }),
        Request::AckGroup {
            stream,
            consumer,
            member,
            offset,
            delivery_token,
        } => Operation::AckGroup(wire::AckGroupRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            member: member.clone(),
            offset: *offset,
            delivery_token: delivery_token.clone(),
        }),
        Request::AckGroupBatch {
            stream,
            consumer,
            member,
            receipts,
        } => Operation::AckGroupBatch(wire::AckGroupBatchRequest {
            stream: stream.clone(),
            consumer: consumer.clone(),
            member: member.clone(),
            receipts: receipts
                .iter()
                .map(|receipt| wire::DeliveryReceipt {
                    offset: receipt.offset,
                    delivery_token: receipt.delivery_token.clone(),
                })
                .collect(),
        }),
        Request::Health => Operation::Health(wire::HealthRequest {}),
    })
}

fn request_from_wire(operation: wire::application_request::Operation) -> Result<Request, V2ProtocolError> {
    use wire::application_request::Operation;
    Ok(match operation {
        Operation::CreateStream(request) => Request::CreateStream {
            stream: request.stream,
        },
        Operation::Publish(request) => Request::PublishBytes {
            stream: request.stream,
            key: request.key,
            payload: crate::BinaryPayload::new(request.payload),
            request_id: request.request_id,
        },
        Operation::PublishBatch(request) => Request::PublishBatch {
            stream: request.stream,
            records: request
                .records
                .into_iter()
                .map(|record| crate::PublishBatchRecord {
                    key: record.key,
                    payload: crate::BinaryPayload::new(record.payload),
                    request_id: record.request_id,
                })
                .collect(),
        },
        Operation::Poll(request) => Request::Poll {
            stream: request.stream,
            consumer: request.consumer,
        },
        Operation::PollBatch(request) => Request::PollBatch {
            stream: request.stream,
            consumer: request.consumer,
            max_records: request.max_records as usize,
            max_bytes: request.max_bytes as usize,
            max_wait_ms: request.max_wait_ms,
        },
        Operation::Replay(request) => Request::Replay {
            stream: request.stream,
            consumer: request.consumer,
            offset: request.offset,
        },
        Operation::PollGroup(request) => Request::PollGroup {
            stream: request.stream,
            consumer: request.consumer,
            member: request.member,
        },
        Operation::PollGroupBatch(request) => Request::PollGroupBatch {
            stream: request.stream,
            consumer: request.consumer,
            member: request.member,
            max_records: request.max_records as usize,
            max_bytes: request.max_bytes as usize,
            max_wait_ms: request.max_wait_ms,
        },
        Operation::ConfigureConsumer(request) => Request::ConfigureConsumer {
            stream: request.stream,
            consumer: request.consumer,
            ack_timeout_ms: request.ack_timeout_ms,
            max_delivery_attempts: request.max_delivery_attempts,
        },
        Operation::InspectConsumer(request) => Request::InspectConsumer {
            stream: request.stream,
            consumer: request.consumer,
        },
        Operation::Ack(request) => Request::Ack {
            stream: request.stream,
            consumer: request.consumer,
            offset: request.offset,
        },
        Operation::AckBatch(request) => Request::AckBatch {
            stream: request.stream,
            consumer: request.consumer,
            receipts: request
                .receipts
                .into_iter()
                .map(|receipt| crate::BatchDeliveryReceipt {
                    offset: receipt.offset,
                    delivery_token: receipt.delivery_token,
                })
                .collect(),
        },
        Operation::AckGroup(request) => Request::AckGroup {
            stream: request.stream,
            consumer: request.consumer,
            member: request.member,
            offset: request.offset,
            delivery_token: request.delivery_token,
        },
        Operation::AckGroupBatch(request) => Request::AckGroupBatch {
            stream: request.stream,
            consumer: request.consumer,
            member: request.member,
            receipts: request
                .receipts
                .into_iter()
                .map(|receipt| crate::BatchDeliveryReceipt {
                    offset: receipt.offset,
                    delivery_token: receipt.delivery_token,
                })
                .collect(),
        },
        Operation::Health(_) => Request::Health,
    })
}

fn reply_to_wire(reply: &ApplicationReply) -> Result<wire::ApplicationResponse, V2ProtocolError> {
    use wire::application_response::Result as ResultBody;
    let (result, batched) = match &reply.response {
        Response::StreamCreated { stream, created } => (
            ResultBody::StreamCreated(wire::StreamCreatedResult {
                stream: stream.clone(),
                created: *created,
            }),
            false,
        ),
        Response::Published { stream, offset } => (
            ResultBody::Published(wire::PublishedResult {
                stream: stream.clone(),
                offset: *offset,
            }),
            false,
        ),
        Response::PublishBatch { stream, outcomes } => {
            if reply.outcome.is_some() || reply.stage.is_some() || reply.items.len() != outcomes.len() {
                return Err(V2ProtocolError::InvalidResponse);
            }
            let items = outcomes
                .iter()
                .zip(&reply.items)
                .map(|(item, metadata)| match item {
                    crate::PublishBatchRecordResponse::Published { offset } => {
                        if metadata.outcome != Outcome::Confirmed {
                            return Err(V2ProtocolError::InvalidResponse);
                        }
                        Ok(wire::PublishItemResult {
                            outcome: outcome_to_wire(metadata.outcome),
                            stage: stage_to_wire(metadata.stage),
                            offset: Some(*offset),
                            code: None,
                            diagnostic: None,
                        })
                    }
                    crate::PublishBatchRecordResponse::Error { code, message } => {
                        if metadata.outcome == Outcome::Confirmed {
                            return Err(V2ProtocolError::InvalidResponse);
                        }
                        Ok(wire::PublishItemResult {
                            outcome: outcome_to_wire(metadata.outcome),
                            stage: stage_to_wire(metadata.stage),
                            offset: None,
                            code: Some(code.clone()),
                            diagnostic: Some(message.clone()),
                        })
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            (
                ResultBody::PublishBatch(wire::PublishBatchResult {
                    stream: stream.clone(),
                    items,
                }),
                true,
            )
        }
        Response::PollBatch { stream, consumer, messages } => {
            if reply.outcome.is_some() || reply.stage.is_some() || reply.items.len() != messages.len() {
                return Err(V2ProtocolError::InvalidResponse);
            }
            let items = messages
                .iter()
                .zip(&reply.items)
                .map(|(message, metadata)| {
                    if metadata.outcome != Outcome::Confirmed {
                        return Err(V2ProtocolError::InvalidResponse);
                    }
                    Ok(wire::MessageItemResult {
                        outcome: outcome_to_wire(metadata.outcome),
                        stage: stage_to_wire(metadata.stage),
                        message: Some(batch_message_to_wire(message)),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            (
                ResultBody::PollBatch(wire::PollBatchResult {
                    stream: stream.clone(),
                    consumer: consumer.clone(),
                    items,
                }),
                true,
            )
        }
        Response::Message { stream, consumer, member, offset, key, payload, published_at_ms, delivery_token, delivery_attempt } => (
            ResultBody::Message(wire::MessageResult {
                stream: stream.clone(), consumer: consumer.clone(), member: member.clone(),
                offset: *offset, key: key.clone(), payload: payload.as_bytes().to_vec(),
                published_at_ms: *published_at_ms, delivery_token: delivery_token.clone(),
                delivery_attempt: *delivery_attempt,
            }), false,
        ),
        Response::MessageBytes { stream, consumer, member, offset, key, payload, published_at_ms, delivery_token, delivery_attempt } => (
            ResultBody::Message(wire::MessageResult {
                stream: stream.clone(), consumer: consumer.clone(), member: member.clone(),
                offset: *offset, key: key.clone(), payload: payload.as_bytes().to_vec(),
                published_at_ms: *published_at_ms, delivery_token: delivery_token.clone(),
                delivery_attempt: *delivery_attempt,
            }), false,
        ),
        Response::ReplayMessage { stream, consumer, offset, key, payload, published_at_ms } => (
            ResultBody::Replay(wire::ReplayResult {
                stream: stream.clone(), consumer: consumer.clone(), offset: *offset,
                key: key.clone(), payload: payload.as_bytes().to_vec(), published_at_ms: *published_at_ms,
            }), false,
        ),
        Response::ReplayMessageBytes { stream, consumer, offset, key, payload, published_at_ms } => (
            ResultBody::Replay(wire::ReplayResult {
                stream: stream.clone(), consumer: consumer.clone(), offset: *offset,
                key: key.clone(), payload: payload.as_bytes().to_vec(), published_at_ms: *published_at_ms,
            }), false,
        ),
        Response::Empty { stream, consumer } => (
            ResultBody::Empty(wire::EmptyResult {
                stream: stream.clone(), consumer: consumer.clone(),
            }), false,
        ),
        Response::Acknowledged { stream, consumer, offset, already_acknowledged } => (
            ResultBody::Acknowledged(wire::AcknowledgedResult {
                stream: stream.clone(), consumer: consumer.clone(), offset: *offset,
                already_acknowledged: *already_acknowledged,
            }), false,
        ),
        Response::AckBatch { stream, consumer, outcomes } => {
            if reply.outcome.is_some() || reply.stage.is_some() || reply.items.len() != outcomes.len() {
                return Err(V2ProtocolError::InvalidResponse);
            }
            let items = outcomes
                .iter()
                .zip(&reply.items)
                .map(|(item, metadata)| {
                    let (status, code, diagnostic) = match &item.outcome {
                        crate::AckBatchItemOutcome::Confirmed => (wire::AckStatus::Confirmed, None, None),
                        crate::AckBatchItemOutcome::AlreadyConfirmed => (wire::AckStatus::AlreadyConfirmed, None, None),
                        crate::AckBatchItemOutcome::Rejected => {
                            (wire::AckStatus::Rejected, item.code.clone(), item.message.clone())
                        }
                    };
                    Ok(wire::AckItemResult {
                        offset: item.offset,
                        outcome: outcome_to_wire(metadata.outcome),
                        stage: stage_to_wire(metadata.stage),
                        status: status as i32,
                        code,
                        diagnostic,
                    })
                })
                .collect::<Result<Vec<_>, V2ProtocolError>>()?;
            (
                ResultBody::AckBatch(wire::AckBatchResult {
                    stream: stream.clone(), consumer: consumer.clone(), items,
                }), true,
            )
        }
        Response::ConsumerPolicy { stream, consumer, version, configured, ack_timeout_ms, max_delivery_attempts } => (
            ResultBody::ConsumerPolicy(wire::ConsumerPolicyResult {
                stream: stream.clone(), consumer: consumer.clone(), version: *version,
                configured: *configured, ack_timeout_ms: *ack_timeout_ms,
                max_delivery_attempts: *max_delivery_attempts,
            }), false,
        ),
        Response::Health { status, streams, storage_bytes } => (
            ResultBody::Health(wire::HealthResult {
                status: status.clone(), streams: *streams as u64, storage_bytes: *storage_bytes,
            }), false,
        ),
        Response::Error { code, message } => (
            ResultBody::Error(wire::ErrorResult {
                code: code.clone(), diagnostic: message.clone(),
            }), false,
        ),
    };
    let (outcome, stage) = if batched {
        (None, None)
    } else {
        if !reply.items.is_empty() {
            return Err(V2ProtocolError::InvalidResponse);
        }
        (
            Some(outcome_to_wire(reply.outcome.ok_or(V2ProtocolError::InvalidResponse)?)),
            Some(stage_to_wire(reply.stage.ok_or(V2ProtocolError::InvalidResponse)?)),
        )
    };
    Ok(wire::ApplicationResponse {
        outcome,
        stage,
        result: Some(result),
    })
}

fn reply_from_wire(reply: wire::ApplicationResponse) -> Result<ApplicationReply, V2ProtocolError> {
    use wire::application_response::Result as ResultBody;
    let result = reply.result.ok_or(V2ProtocolError::InvalidResponse)?;
    let is_batch = matches!(result, ResultBody::PublishBatch(_) | ResultBody::PollBatch(_) | ResultBody::AckBatch(_));
    if is_batch && (reply.outcome.is_some() || reply.stage.is_some()) {
        return Err(V2ProtocolError::InvalidResponse);
    }
    let mut items = Vec::new();
    let response = match result {
        ResultBody::StreamCreated(result) => Response::StreamCreated {
            stream: result.stream,
            created: result.created,
        },
        ResultBody::Published(result) => Response::Published {
            stream: result.stream,
            offset: result.offset,
        },
        ResultBody::PublishBatch(result) => {
            let mut outcomes = Vec::with_capacity(result.items.len());
            for item in result.items {
                let metadata = ItemMetadata {
                    outcome: outcome_from_wire(item.outcome)?,
                    stage: stage_from_wire(item.stage)?,
                };
                if let Some(offset) = item.offset {
                    if metadata.outcome != Outcome::Confirmed || item.code.is_some() || item.diagnostic.is_some() {
                        return Err(V2ProtocolError::InvalidResponse);
                    }
                    outcomes.push(crate::PublishBatchRecordResponse::Published { offset });
                } else {
                    let code = item.code.ok_or(V2ProtocolError::InvalidResponse)?;
                    let message = item.diagnostic.ok_or(V2ProtocolError::InvalidResponse)?;
                    if metadata.outcome == Outcome::Confirmed {
                        return Err(V2ProtocolError::InvalidResponse);
                    }
                    outcomes.push(crate::PublishBatchRecordResponse::Error { code, message });
                }
                items.push(metadata);
            }
            Response::PublishBatch { stream: result.stream, outcomes }
        }
        ResultBody::PollBatch(result) => {
            let mut messages = Vec::with_capacity(result.items.len());
            for item in result.items {
                let metadata = ItemMetadata {
                    outcome: outcome_from_wire(item.outcome)?,
                    stage: stage_from_wire(item.stage)?,
                };
                if metadata.outcome != Outcome::Confirmed {
                    return Err(V2ProtocolError::InvalidResponse);
                }
                let message = item.message.ok_or(V2ProtocolError::InvalidResponse)?;
                messages.push(batch_message_from_wire(message)?);
                items.push(metadata);
            }
            Response::PollBatch {
                stream: result.stream,
                consumer: result.consumer,
                messages,
            }
        }
        ResultBody::Message(result) => message_from_wire(result)?,
        ResultBody::Replay(result) => replay_from_wire(result)?,
        ResultBody::Empty(result) => Response::Empty {
            stream: result.stream,
            consumer: result.consumer,
        },
        ResultBody::Acknowledged(result) => Response::Acknowledged {
            stream: result.stream,
            consumer: result.consumer,
            offset: result.offset,
            already_acknowledged: result.already_acknowledged,
        },
        ResultBody::AckBatch(result) => {
            let mut outcomes = Vec::with_capacity(result.items.len());
            for item in result.items {
                let outcome = outcome_from_wire(item.outcome)?;
                let stage = stage_from_wire(item.stage)?;
                let status = match wire::AckStatus::try_from(item.status) {
                    Ok(wire::AckStatus::Confirmed) if outcome == Outcome::Confirmed => crate::AckBatchItemOutcome::Confirmed,
                    Ok(wire::AckStatus::AlreadyConfirmed) if outcome == Outcome::Confirmed => crate::AckBatchItemOutcome::AlreadyConfirmed,
                    Ok(wire::AckStatus::Rejected) if outcome != Outcome::Confirmed => crate::AckBatchItemOutcome::Rejected,
                    _ => return Err(V2ProtocolError::InvalidResponse),
                };
                let has_failure = matches!(status, crate::AckBatchItemOutcome::Rejected);
                if has_failure != (item.code.is_some() && item.diagnostic.is_some()) {
                    return Err(V2ProtocolError::InvalidResponse);
                }
                outcomes.push(crate::AckBatchItemResponse {
                    offset: item.offset,
                    outcome: status,
                    code: item.code,
                    message: item.diagnostic,
                });
                items.push(ItemMetadata { outcome, stage });
            }
            Response::AckBatch { stream: result.stream, consumer: result.consumer, outcomes }
        }
        ResultBody::ConsumerPolicy(result) => Response::ConsumerPolicy {
            stream: result.stream,
            consumer: result.consumer,
            version: result.version,
            configured: result.configured,
            ack_timeout_ms: result.ack_timeout_ms,
            max_delivery_attempts: result.max_delivery_attempts,
        },
        ResultBody::Health(result) => Response::Health {
            status: result.status,
            streams: usize::try_from(result.streams).map_err(|_| V2ProtocolError::InvalidResponse)?,
            storage_bytes: result.storage_bytes,
        },
        ResultBody::Error(result) => Response::Error {
            code: result.code,
            message: result.diagnostic,
        },
    };
    if is_batch {
        Ok(ApplicationReply::batch(response, items))
    } else {
        Ok(ApplicationReply {
            response,
            outcome: Some(outcome_from_wire(reply.outcome.ok_or(V2ProtocolError::InvalidResponse)?)?),
            stage: Some(stage_from_wire(reply.stage.ok_or(V2ProtocolError::InvalidResponse)?)?),
            items,
        })
    }
}

fn message_from_wire(result: wire::MessageResult) -> Result<Response, V2ProtocolError> {
    let wire::MessageResult {
        stream,
        consumer,
        member,
        offset,
        key,
        payload,
        published_at_ms,
        delivery_token,
        delivery_attempt,
    } = result;
    match String::from_utf8(payload) {
        Ok(payload) => Ok(Response::Message {
            stream,
            consumer,
            member,
            offset,
            key,
            payload,
            published_at_ms,
            delivery_token,
            delivery_attempt,
        }),
        Err(error) => Ok(Response::MessageBytes {
            stream,
            consumer,
            member,
            offset,
            key,
            payload: crate::BinaryPayload::new(error.into_bytes()),
            published_at_ms,
            delivery_token,
            delivery_attempt,
        }),
    }
}

fn replay_from_wire(result: wire::ReplayResult) -> Result<Response, V2ProtocolError> {
    if let Ok(payload) = String::from_utf8(result.payload.clone()) {
        Ok(Response::ReplayMessage {
            stream: result.stream,
            consumer: result.consumer,
            offset: result.offset,
            key: result.key,
            payload,
            published_at_ms: result.published_at_ms,
        })
    } else {
        Ok(Response::ReplayMessageBytes {
            stream: result.stream,
            consumer: result.consumer,
            offset: result.offset,
            key: result.key,
            payload: crate::BinaryPayload::new(result.payload),
            published_at_ms: result.published_at_ms,
        })
    }
}

fn batch_message_to_wire(result: &crate::BatchMessageResponse) -> wire::MessageResult {
    match result {
        crate::BatchMessageResponse::Text { stream, consumer, member, offset, key, payload, published_at_ms, delivery_token, delivery_attempt } => wire::MessageResult {
            stream: stream.clone(), consumer: consumer.clone(), member: member.clone(), offset: *offset,
            key: key.clone(), payload: payload.as_bytes().to_vec(), published_at_ms: *published_at_ms,
            delivery_token: delivery_token.clone(), delivery_attempt: *delivery_attempt,
        },
        crate::BatchMessageResponse::Bytes { stream, consumer, member, offset, key, payload, published_at_ms, delivery_token, delivery_attempt } => wire::MessageResult {
            stream: stream.clone(), consumer: consumer.clone(), member: member.clone(), offset: *offset,
            key: key.clone(), payload: payload.as_bytes().to_vec(), published_at_ms: *published_at_ms,
            delivery_token: delivery_token.clone(), delivery_attempt: *delivery_attempt,
        },
    }
}

fn batch_message_from_wire(result: wire::MessageResult) -> Result<crate::BatchMessageResponse, V2ProtocolError> {
    let wire::MessageResult {
        stream, consumer, member, offset, key, payload, published_at_ms, delivery_token, delivery_attempt,
    } = result;
    if let Ok(payload) = String::from_utf8(payload.clone()) {
        Ok(crate::BatchMessageResponse::Text {
            stream, consumer, member, offset, key, payload, published_at_ms, delivery_token, delivery_attempt,
        })
    } else {
        Ok(crate::BatchMessageResponse::Bytes {
            stream, consumer, member, offset, key,
            payload: crate::BinaryPayload::new(payload), published_at_ms, delivery_token, delivery_attempt,
        })
    }
}

fn outcome_to_wire(outcome: Outcome) -> i32 {
    match outcome {
        Outcome::Confirmed => wire::Outcome::Confirmed as i32,
        Outcome::Rejected => wire::Outcome::Rejected as i32,
        Outcome::Retryable => wire::Outcome::Retryable as i32,
        Outcome::Unknown => wire::Outcome::Unknown as i32,
    }
}

fn outcome_from_wire(outcome: i32) -> Result<Outcome, V2ProtocolError> {
    match wire::Outcome::try_from(outcome) {
        Ok(wire::Outcome::Confirmed) => Ok(Outcome::Confirmed),
        Ok(wire::Outcome::Rejected) => Ok(Outcome::Rejected),
        Ok(wire::Outcome::Retryable) => Ok(Outcome::Retryable),
        Ok(wire::Outcome::Unknown) => Ok(Outcome::Unknown),
        _ => Err(V2ProtocolError::InvalidResponse),
    }
}

fn stage_to_wire(stage: Stage) -> i32 {
    match stage {
        Stage::Received => wire::Stage::Received as i32,
        Stage::Validated => wire::Stage::Validated as i32,
        Stage::ExecutionStarted => wire::Stage::ExecutionStarted as i32,
        Stage::Durable => wire::Stage::Durable as i32,
        Stage::Completed => wire::Stage::Completed as i32,
        Stage::Unknown => wire::Stage::Unknown as i32,
    }
}

fn stage_from_wire(stage: i32) -> Result<Stage, V2ProtocolError> {
    match wire::Stage::try_from(stage) {
        Ok(wire::Stage::Received) => Ok(Stage::Received),
        Ok(wire::Stage::Validated) => Ok(Stage::Validated),
        Ok(wire::Stage::ExecutionStarted) => Ok(Stage::ExecutionStarted),
        Ok(wire::Stage::Durable) => Ok(Stage::Durable),
        Ok(wire::Stage::Completed) => Ok(Stage::Completed),
        Ok(wire::Stage::Unknown) => Ok(Stage::Unknown),
        _ => Err(V2ProtocolError::InvalidResponse),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchMessageResponse, BinaryPayload, PublishBatchRecord};
    use prost::Message as _;
    use runnel_engine::{Message, poll_batch_response_len, poll_message_response_upper_bound};

    #[test]
    fn core_hello_uses_the_fixed_preface_and_delimited_protobuf() {
        let hello = ClientHello {
            versions: vec![VersionRange {
                major: 2,
                min_minor: 0,
                max_minor: 0,
            }],
            offered_capabilities: Vec::new(),
            required_capabilities: Vec::new(),
            max_outbound_frame_bytes: 1024,
            max_inbound_frame_bytes: 1024,
        };
        let encoded = encode_client_hello(&hello).unwrap();
        assert_eq!(&PREFACE, b"RNLN\x01\0\0\0");
        assert_eq!(
            encoded.as_bytes(),
            &[0, 0, 0, 12, 10, 10, 10, 2, 8, 2, 32, 128, 8, 40, 128, 8]
        );
        assert_eq!(decode_client_hello(&encoded.as_bytes()[4..]).unwrap(), hello);
    }

    #[test]
    fn hello_validation_rejects_duplicate_ranges_and_capability_sets() {
        let mut hello = ClientHello::core_v2();
        assert_eq!(validate_client_hello(&hello), Ok(()));
        hello.versions.push(hello.versions[0].clone());
        assert_eq!(validate_client_hello(&hello), Err(RefusalCode::InvalidHello));

        let mut hello = ClientHello::core_v2();
        hello.offered_capabilities = vec!["consume_batch".to_owned(), "consume_batch".to_owned()];
        assert_eq!(validate_client_hello(&hello), Err(RefusalCode::InvalidHello));

        let mut hello = ClientHello::core_v2();
        hello.required_capabilities.push("future".to_owned());
        assert_eq!(validate_client_hello(&hello), Err(RefusalCode::InvalidHello));
    }

    #[test]
    fn authentication_frame_redacts_and_round_trips_secret() {
        let token_text = "A".repeat(43);
        let token = BearerToken::parse(token_text.clone()).unwrap();
        let encoded = encode_client_frame(&ClientFrame::BearerAuth(token)).unwrap();
        assert!(!format!("{encoded:?}").contains(&token_text));
        assert!(encoded.len() <= AUTH_MAX_BODY_BYTES + 4);
        let ClientFrame::BearerAuth(decoded) =
            decode_client_frame(&encoded.as_bytes()[4..], AUTH_MAX_BODY_BYTES).unwrap()
        else {
            panic!("auth frame should decode as authentication control");
        };
        assert_eq!(decoded.expose_secret(), token_text);
        assert!(!format!("{decoded:?}").contains(&token_text));
    }

    #[test]
    fn oversized_bearer_control_frame_is_rejected_before_token_decode() {
        let frame = wire::ClientFrame {
            body: Some(wire::client_frame::Body::BearerAuth(wire::BearerAuth {
                token: "A".repeat(AUTH_MAX_BODY_BYTES + 10),
            })),
        };
        let mut body = Vec::new();
        frame.encode(&mut body).unwrap();
        assert_eq!(client_frame_kind(&body), Ok(ClientFrameKind::BearerAuth));
        assert!(matches!(
            decode_client_frame(&body, MAX_CLIENT_TO_SERVER_FRAME_BYTES),
            Err(V2ProtocolError::FrameTooLarge)
        ));
    }

    #[test]
    fn binary_application_payloads_are_unmodified_in_v2_frames() {
        let request = Request::PublishBytes {
            stream: "events".to_owned(),
            key: Some("orders".to_owned()),
            payload: BinaryPayload::new(vec![0, 1, 255, b'\n']),
            request_id: Some("stable-id".to_owned()),
        };
        let encoded = encode_client_frame(&ClientFrame::Application(request)).unwrap();
        let ClientFrame::Application(Request::PublishBytes {
            payload,
            request_id,
            ..
        }) = decode_client_frame(
            &encoded.as_bytes()[4..],
            MAX_CLIENT_TO_SERVER_FRAME_BYTES,
        )
        .unwrap()
        else {
            panic!("v2 publish should retain its opaque byte field");
        };
        assert_eq!(payload.as_bytes(), [0, 1, 255, b'\n']);
        assert_eq!(request_id.as_deref(), Some("stable-id"));
    }

    #[test]
    fn application_reply_carries_outcome_and_stage() {
        let reply = ApplicationReply::confirmed(
            Response::Published {
                stream: "events".to_owned(),
                offset: 9,
            },
            true,
        );
        let encoded = encode_server_frame(&ServerFrame::Application(reply)).unwrap();
        let ServerFrame::Application(decoded) = decode_server_frame(
            &encoded.as_bytes()[4..],
            MAX_SERVER_TO_CLIENT_FRAME_BYTES,
        )
        .unwrap()
        else {
            panic!("application result should decode");
        };
        assert_eq!(decoded.outcome, Some(Outcome::Confirmed));
        assert_eq!(decoded.stage, Some(Stage::Durable));
        assert!(matches!(decoded.response, Response::Published { offset: 9, .. }));
    }

    #[test]
    fn batch_replies_have_item_metadata_and_no_aggregate_metadata() {
        let reply = ApplicationReply::batch(
            Response::PublishBatch {
                stream: "events".to_owned(),
                outcomes: vec![
                    crate::PublishBatchRecordResponse::Published { offset: 3 },
                    crate::PublishBatchRecordResponse::Error {
                        code: "request_id_content_conflict".to_owned(),
                        message: "content conflict".to_owned(),
                    },
                ],
            },
            vec![
                ItemMetadata {
                    outcome: Outcome::Confirmed,
                    stage: Stage::Durable,
                },
                ItemMetadata {
                    outcome: Outcome::Rejected,
                    stage: Stage::ExecutionStarted,
                },
            ],
        );
        let encoded = encode_server_frame(&ServerFrame::Application(reply)).unwrap();
        let ServerFrame::Application(decoded) = decode_server_frame(
            &encoded.as_bytes()[4..],
            MAX_SERVER_TO_CLIENT_FRAME_BYTES,
        )
        .unwrap()
        else {
            panic!("batch result should decode");
        };
        assert_eq!(decoded.outcome, None);
        assert_eq!(decoded.stage, None);
        assert_eq!(decoded.items.len(), 2);
        assert_eq!(decoded.items[0].outcome, Outcome::Confirmed);
        assert_eq!(decoded.items[1].stage, Stage::ExecutionStarted);
    }

    #[test]
    fn oversized_and_empty_frames_fail_before_protobuf_decode() {
        assert_eq!(
            decode_body::<wire::ClientHello>(&[], HELLO_MAX_BODY_BYTES),
            Err(V2ProtocolError::FrameTooLarge)
        );
        assert_eq!(
            decode_body::<wire::ClientHello>(
                &vec![0; HELLO_MAX_BODY_BYTES + 1],
                HELLO_MAX_BODY_BYTES
            ),
            Err(V2ProtocolError::FrameTooLarge)
        );
        assert!(matches!(
            decode_client_frame(&vec![0; AUTH_MAX_BODY_BYTES + 1], MAX_CLIENT_TO_SERVER_FRAME_BYTES),
            Err(V2ProtocolError::InvalidFrame)
        ));
    }

    #[test]
    fn publish_batch_wire_conversion_preserves_each_record() {
        let request = Request::PublishBatch {
            stream: "events".to_owned(),
            records: vec![PublishBatchRecord {
                key: Some("key".to_owned()),
                payload: BinaryPayload::new(vec![0, 1]),
                request_id: None,
            }],
        };
        let encoded = encode_client_frame(&ClientFrame::Application(request)).unwrap();
        let ClientFrame::Application(Request::PublishBatch { records, .. }) = decode_client_frame(
            &encoded.as_bytes()[4..],
            MAX_CLIENT_TO_SERVER_FRAME_BYTES,
        )
        .unwrap()
        else {
            panic!("batch operation should decode");
        };
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.as_deref(), Some("key"));
        assert_eq!(records[0].payload.as_bytes(), [0, 1]);
    }

    fn binary_message() -> Message {
        Message {
            stream: "events".to_owned(),
            offset: 23,
            key: Some("orders".to_owned()),
            payload: vec![0, 0xff, 0x80, b'/', b'\n', 0xc3],
            published_at_ms: 1_725_000_123_456,
            delivery_token: Some("opaque-delivery-token".to_owned()),
            delivery_attempt: Some(7),
        }
    }

    fn scalar_binary_reply(message: &Message) -> ApplicationReply {
        ApplicationReply::confirmed(
            Response::MessageBytes {
                stream: message.stream.clone(),
                consumer: "worker".to_owned(),
                member: Some("member-a".to_owned()),
                offset: message.offset,
                key: message.key.clone(),
                payload: BinaryPayload::new(message.payload.clone()),
                published_at_ms: message.published_at_ms,
                delivery_token: message.delivery_token.clone(),
                delivery_attempt: message.delivery_attempt,
            },
            true,
        )
    }

    fn batch_binary_reply(message: &Message) -> ApplicationReply {
        ApplicationReply::batch(
            Response::PollBatch {
                stream: message.stream.clone(),
                consumer: "worker".to_owned(),
                messages: vec![BatchMessageResponse::Bytes {
                    stream: message.stream.clone(),
                    consumer: "worker".to_owned(),
                    member: Some("member-a".to_owned()),
                    offset: message.offset,
                    key: message.key.clone(),
                    payload: BinaryPayload::new(message.payload.clone()),
                    published_at_ms: message.published_at_ms,
                    delivery_token: message.delivery_token.clone(),
                    delivery_attempt: message.delivery_attempt,
                }],
            },
            vec![ItemMetadata {
                outcome: Outcome::Confirmed,
                stage: Stage::Durable,
            }],
        )
    }

    fn encoded_application_body(reply: ApplicationReply) -> Vec<u8> {
        let frame = encode_server_frame(&ServerFrame::Application(reply)).unwrap();
        frame.as_bytes()[4..].to_vec()
    }

    #[test]
    fn scalar_and_batch_binary_poll_responses_round_trip_exact_bytes() {
        let message = binary_message();

        let scalar = encoded_application_body(scalar_binary_reply(&message));
        let ServerFrame::Application(scalar) =
            decode_server_frame(&scalar, MAX_SERVER_TO_CLIENT_FRAME_BYTES).unwrap()
        else {
            panic!("scalar poll result should decode");
        };
        let Response::MessageBytes { payload, .. } = scalar.response else {
            panic!("invalid UTF-8 payload must remain an opaque binary payload");
        };
        assert_eq!(payload.as_bytes(), message.payload);

        let batch = encoded_application_body(batch_binary_reply(&message));
        let ServerFrame::Application(batch) =
            decode_server_frame(&batch, MAX_SERVER_TO_CLIENT_FRAME_BYTES).unwrap()
        else {
            panic!("batch poll result should decode");
        };
        let Response::PollBatch { messages, .. } = batch.response else {
            panic!("batch poll result should retain its operation shape");
        };
        let [BatchMessageResponse::Bytes { payload, .. }] = messages.as_slice() else {
            panic!("invalid UTF-8 batch payload must remain opaque bytes");
        };
        assert_eq!(payload.as_bytes(), message.payload);
    }

    #[test]
    fn scalar_and_batch_response_bounds_cover_generated_protobuf_frames() {
        let message = binary_message();
        let scalar = encoded_application_body(scalar_binary_reply(&message));
        let scalar_bound = poll_message_response_upper_bound(
            &message,
            "worker",
            Some("member-a"),
            true,
        );
        assert!(scalar_bound >= scalar.len(), "{scalar_bound} < {}", scalar.len());

        let batch = encoded_application_body(batch_binary_reply(&message));
        let batch_bound = poll_batch_response_len(
            &message.stream,
            "worker",
            Some("member-a"),
            std::slice::from_ref(&message),
        );
        assert!(batch_bound >= batch.len(), "{batch_bound} < {}", batch.len());
    }
}
