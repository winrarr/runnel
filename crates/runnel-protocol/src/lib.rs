use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Name of the current provisional application protocol.
pub const PROTOCOL_NAME: &str = "runnel-json-lines";
/// Version of the current provisional JSON-lines protocol.
pub const PROTOCOL_VERSION: u16 = 1;
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

/// Payload representation supported by the provisional wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadEncoding {
    /// UTF-8 text in the legacy `payload` field.
    Utf8Text,
    /// Exact application bytes in the padded `payload_base64` field.
    Base64,
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
    payload_encodings: &[PayloadEncoding::Utf8Text, PayloadEncoding::Base64],
};

/// Maximum number of records accepted in one publish-batch request.
pub const MAX_PUBLISH_BATCH_RECORDS: usize = 1024;
/// Maximum encoded request size supported by the protocol's publish-batch path.
pub const MAX_PUBLISH_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Opaque bytes represented as standard padded base64 on the provisional wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryPayload(Vec<u8>);

impl BinaryPayload {
    /// Construct a binary payload from its application bytes.
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }

    /// Decode a standard padded base64 value into a binary payload.
    pub fn from_base64(value: &str) -> Result<Self, base64::DecodeError> {
        STANDARD.decode(value).map(Self)
    }

    /// Return the application bytes without changing them.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume the payload and return its application bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Return the wire representation used by this payload.
    pub const fn encoding(&self) -> PayloadEncoding {
        PayloadEncoding::Base64
    }
}

impl Serialize for BinaryPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(&self.0))
    }
}

impl<'de> Deserialize<'de> for BinaryPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        STANDARD.decode(value).map(Self).map_err(D::Error::custom)
    }
}

/// One record in a binary-safe publish batch.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishBatchRecord {
    pub key: Option<String>,
    pub payload_base64: BinaryPayload,
    #[serde(default)]
    pub request_id: Option<String>,
}

impl PublishBatchRecord {
    /// Return the wire representation used by this record's payload.
    pub const fn payload_encoding(&self) -> PayloadEncoding {
        PayloadEncoding::Base64
    }
}

/// The broker's result for one publish-batch record.
#[derive(Debug, Serialize, Deserialize)]
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
    /// Publish arbitrary bytes as the explicit `payload_base64` representation.
    PublishBytes {
        stream: String,
        key: Option<String>,
        payload_base64: BinaryPayload,
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
    Ack {
        stream: String,
        consumer: String,
        offset: u64,
    },
    AckGroup {
        stream: String,
        consumer: String,
        member: String,
        offset: u64,
        delivery_token: String,
    },
    Health,
}

impl Request {
    /// Return the payload representation used by this request, if it carries a payload.
    pub const fn payload_encoding(&self) -> Option<PayloadEncoding> {
        match self {
            Self::Publish { .. } => Some(PayloadEncoding::Utf8Text),
            Self::PublishBytes { .. } | Self::PublishBatch { .. } => Some(PayloadEncoding::Base64),
            Self::CreateStream { .. }
            | Self::Poll { .. }
            | Self::Replay { .. }
            | Self::PollGroup { .. }
            | Self::Ack { .. }
            | Self::AckGroup { .. }
            | Self::Health => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
    /// A message whose bytes are not representable by the legacy UTF-8 payload field.
    MessageBytes {
        stream: String,
        consumer: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<String>,
        offset: u64,
        key: Option<String>,
        payload_base64: BinaryPayload,
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
    /// A replay record whose bytes are not representable by the legacy UTF-8 payload field.
    ReplayMessageBytes {
        stream: String,
        consumer: String,
        offset: u64,
        key: Option<String>,
        payload_base64: BinaryPayload,
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
                Some(PayloadEncoding::Base64)
            }
            Self::StreamCreated { .. }
            | Self::Published { .. }
            | Self::PublishBatch { .. }
            | Self::Empty { .. }
            | Self::Acknowledged { .. }
            | Self::Health { .. }
            | Self::Error { .. } => None,
        }
    }
}
