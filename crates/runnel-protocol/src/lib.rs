use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
    Poll {
        stream: String,
        consumer: String,
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
