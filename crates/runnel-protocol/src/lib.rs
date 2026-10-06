use serde::{Deserialize, Serialize};

pub mod v2;
mod security;
pub use security::{BearerToken, SecurityRole, TokenFormatError};

/// Name of the v2 Protobuf application protocol.
pub const PROTOCOL_NAME: &str = "runnel-protobuf";
/// Application protocol major declared by this implementation.
pub const PROTOCOL_VERSION: u16 = 2;
/// Lowest protocol version supported by this crate.
pub const MIN_SUPPORTED_PROTOCOL_VERSION: u16 = PROTOCOL_VERSION;
/// Highest protocol version supported by this crate.
pub const MAX_SUPPORTED_PROTOCOL_VERSION: u16 = PROTOCOL_VERSION;

/// A closed version range supported by one protocol implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolVersionRange {
    /// Lowest supported version, inclusive.
    pub min: u16,
    /// Highest supported version, inclusive.
    pub max: u16,
}

impl ProtocolVersionRange {
    /// Return whether a version is in this range.
    pub const fn contains(self, version: u16) -> bool {
        version >= self.min && version <= self.max
    }
}

/// Payload helpers exposed by the Rust API. Every v2 wire payload is an opaque
/// Protobuf byte field; `Utf8Text` is only a convenience input/output view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadEncoding {
    /// UTF-8 text supplied through a text convenience API.
    Utf8Text,
    /// Exact application bytes supplied through a binary convenience API.
    Binary,
}

/// Version and payload compatibility declared by this protocol implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolSupport {
    /// Stable name for the protocol family.
    pub name: &'static str,
    /// Supported protocol versions, inclusive.
    pub versions: ProtocolVersionRange,
    /// Payload representations accepted and emitted by this protocol.
    pub payload_encodings: &'static [PayloadEncoding],
}

impl ProtocolSupport {
    /// Return whether a protocol version is supported.
    pub const fn supports_version(self, version: u16) -> bool {
        self.versions.contains(version)
    }

    /// Return whether a payload representation is supported.
    pub fn supports_payload_encoding(self, encoding: PayloadEncoding) -> bool {
        self.payload_encodings.contains(&encoding)
    }
}

/// The compatibility declaration shared by the broker and reusable client.
pub const PROTOCOL_SUPPORT: ProtocolSupport = ProtocolSupport {
    name: PROTOCOL_NAME,
    versions: ProtocolVersionRange {
        min: MIN_SUPPORTED_PROTOCOL_VERSION,
        max: MAX_SUPPORTED_PROTOCOL_VERSION,
    },
    payload_encodings: &[PayloadEncoding::Utf8Text, PayloadEncoding::Binary],
};

/// Maximum number of records accepted in one publish-batch request.
pub const MAX_PUBLISH_BATCH_RECORDS: usize = 1024;
/// Maximum encoded request size supported by the protocol's publish-batch path.
pub const MAX_PUBLISH_BATCH_BYTES: usize = 64 * 1024 * 1024;
/// Maximum server-to-client Protobuf frame body size.
pub const MAX_RESPONSE_BYTES: usize = v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES;
/// Maximum number of records accepted by one consume-batch request.
pub const MAX_CONSUME_BATCH_RECORDS: usize = 1024;

/// Opaque application bytes carried directly in a v2 Protobuf `bytes` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BinaryPayload(Vec<u8>);

impl BinaryPayload {
    /// Construct a binary payload from its application bytes.
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }

    /// Decode a base64 command-line input into application bytes.
    ///
    /// This is an input convenience only; v2 transports the decoded bytes in
    /// its Protobuf `bytes` field.
    pub fn from_base64(value: &str) -> Result<Self, base64::DecodeError> {
        use base64::Engine as _;

        base64::engine::general_purpose::STANDARD
            .decode(value)
            .map(Self)
    }

    /// Return the application bytes without changing them.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume the payload and return its application bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Return the Rust API view used by this payload.
    pub const fn encoding(&self) -> PayloadEncoding {
        PayloadEncoding::Binary
    }
}

/// One record in a binary-safe publish batch.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishBatchRecord {
    pub key: Option<String>,
    pub payload: BinaryPayload,
    #[serde(default)]
    pub request_id: Option<String>,
}

impl PublishBatchRecord {
    /// Return the wire representation used by this record's payload.
    pub const fn payload_encoding(&self) -> PayloadEncoding {
        PayloadEncoding::Binary
    }
}

/// The broker's result for one publish-batch record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PublishBatchRecordResponse {
    Published { offset: u64 },
    Error { code: String, message: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    CreateStream {
        stream: String,
    },
    Publish {
        stream: String,
        key: Option<String>,
        payload: String,
        #[serde(default)]
        request_id: Option<String>,
    },
    /// Publish arbitrary application bytes.
    PublishBytes {
        stream: String,
        key: Option<String>,
        payload: BinaryPayload,
        #[serde(default)]
        request_id: Option<String>,
    },
    PublishBatch {
        stream: String,
        records: Vec<PublishBatchRecord>,
    },
    Poll {
        stream: String,
        consumer: String,
    },
    PollBatch {
        stream: String,
        consumer: String,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    },
    /// Read one retained record at an inclusive logical offset without
    /// changing the consumer's ordinary progress.
    Replay {
        stream: String,
        consumer: String,
        offset: u64,
    },
    PollGroup {
        stream: String,
        consumer: String,
        member: String,
    },
    PollGroupBatch {
        stream: String,
        consumer: String,
        member: String,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    },
    ConfigureConsumer {
        stream: String,
        consumer: String,
        ack_timeout_ms: u64,
        #[serde(default)]
        max_delivery_attempts: Option<u32>,
    },
    InspectConsumer {
        stream: String,
        consumer: String,
    },
    Ack {
        stream: String,
        consumer: String,
        offset: u64,
    },
    AckBatch {
        stream: String,
        consumer: String,
        receipts: Vec<BatchDeliveryReceipt>,
    },
    AckGroup {
        stream: String,
        consumer: String,
        member: String,
        offset: u64,
        delivery_token: String,
    },
    AckGroupBatch {
        stream: String,
        consumer: String,
        member: String,
        receipts: Vec<BatchDeliveryReceipt>,
    },
    Health,
}

impl Request {
    /// Return the payload representation used by this request, if it carries a payload.
    pub const fn payload_encoding(&self) -> Option<PayloadEncoding> {
        match self {
            Self::Publish { .. } => Some(PayloadEncoding::Utf8Text),
            Self::PublishBytes { .. } | Self::PublishBatch { .. } => Some(PayloadEncoding::Binary),
            Self::CreateStream { .. }
            | Self::Poll { .. }
            | Self::PollBatch { .. }
            | Self::Replay { .. }
            | Self::PollGroup { .. }
            | Self::PollGroupBatch { .. }
            | Self::ConfigureConsumer { .. }
            | Self::InspectConsumer { .. }
            | Self::Ack { .. }
            | Self::AckBatch { .. }
            | Self::AckGroup { .. }
            | Self::AckGroupBatch { .. }
            | Self::Health => None,
        }
    }
}

/// One opaque token/offset pair in an acknowledgement-batch request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchDeliveryReceipt {
    pub offset: u64,
    pub delivery_token: String,
}

/// One decoded item in a consume-batch response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BatchMessageResponse {
    Text {
        stream: String,
        consumer: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<String>,
        offset: u64,
        key: Option<String>,
        payload: String,
        published_at_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_attempt: Option<u32>,
    },
    Bytes {
        stream: String,
        consumer: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<String>,
        offset: u64,
        key: Option<String>,
        payload: BinaryPayload,
        published_at_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_attempt: Option<u32>,
    },
}

/// One independently evaluated acknowledgement receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckBatchItemResponse {
    pub offset: u64,
    pub outcome: AckBatchItemOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Provisional status names for an individual receipt result.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckBatchItemOutcome {
    Confirmed,
    AlreadyConfirmed,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    StreamCreated {
        stream: String,
        created: bool,
    },
    Published {
        stream: String,
        offset: u64,
    },
    PublishBatch {
        stream: String,
        outcomes: Vec<PublishBatchRecordResponse>,
    },
    PollBatch {
        stream: String,
        consumer: String,
        messages: Vec<BatchMessageResponse>,
    },
    Message {
        stream: String,
        consumer: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<String>,
        offset: u64,
        key: Option<String>,
        payload: String,
        published_at_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_attempt: Option<u32>,
    },
    /// A message whose application bytes are not valid UTF-8.
    MessageBytes {
        stream: String,
        consumer: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<String>,
        offset: u64,
        key: Option<String>,
        payload: BinaryPayload,
        published_at_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_attempt: Option<u32>,
    },
    ReplayMessage {
        stream: String,
        consumer: String,
        offset: u64,
        key: Option<String>,
        payload: String,
        published_at_ms: u64,
    },
    /// A replay record whose application bytes are not valid UTF-8.
    ReplayMessageBytes {
        stream: String,
        consumer: String,
        offset: u64,
        key: Option<String>,
        payload: BinaryPayload,
        published_at_ms: u64,
    },
    Empty {
        stream: String,
        consumer: String,
    },
    Acknowledged {
        stream: String,
        consumer: String,
        offset: u64,
        already_acknowledged: bool,
    },
    AckBatch {
        stream: String,
        consumer: String,
        outcomes: Vec<AckBatchItemResponse>,
    },
    ConsumerPolicy {
        stream: String,
        consumer: String,
        version: u64,
        configured: bool,
        ack_timeout_ms: u64,
        max_delivery_attempts: Option<u32>,
    },
    Health {
        status: String,
        streams: usize,
        storage_bytes: u64,
    },
    Error {
        code: String,
        message: String,
    },
}

impl Response {
    /// Return the payload representation used by this response, if it carries a payload.
    pub const fn payload_encoding(&self) -> Option<PayloadEncoding> {
        match self {
            Self::Message { .. } | Self::ReplayMessage { .. } => Some(PayloadEncoding::Utf8Text),
            Self::MessageBytes { .. } | Self::ReplayMessageBytes { .. } => {
                Some(PayloadEncoding::Binary)
            }
            Self::StreamCreated { .. }
            | Self::Published { .. }
            | Self::PublishBatch { .. }
            | Self::PollBatch { .. }
            | Self::Empty { .. }
            | Self::Acknowledged { .. }
            | Self::AckBatch { .. }
            | Self::ConsumerPolicy { .. }
            | Self::Health { .. }
            | Self::Error { .. } => None,
        }
    }
}

#[cfg(test)]
mod payload_tests {
    use super::{BinaryPayload, PayloadEncoding};

    #[test]
    fn binary_payload_is_raw_v2_bytes() {
        let value = [0, 1, 255, b'\n'];
        let payload = BinaryPayload::new(value);
        assert_eq!(payload.as_bytes(), value);
        assert_eq!(payload.encoding(), PayloadEncoding::Binary);
    }
}
