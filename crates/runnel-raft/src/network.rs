use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use crate::TypeConfig;
use runnel_engine::{
    AckBatchResult, AckResult, ConsumerPolicy, DeliveryReceipt, Message, Offset, PollResult,
    ReplayMessage,
};

mod framing;
mod inbound;
mod outbound;

pub(crate) use inbound::serve;
pub(crate) use outbound::{PeerTransport, TcpNetwork, ensure_data_group, forward};

#[derive(Debug, Serialize, Deserialize)]
enum PeerRequest {
    AppendEntries {
        group_id: String,
        request: AppendEntriesRequest<TypeConfig>,
    },
    InstallSnapshot {
        group_id: String,
        request: InstallSnapshotRequest<TypeConfig>,
    },
    Vote {
        group_id: String,
        request: VoteRequest<u64>,
    },
    Forward(ForwardedOperation),
    EnsureDataGroup {
        stream: String,
        stream_id: String,
        group_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum PeerResponse {
    AppendEntries(AppendEntriesResponse<u64>),
    InstallSnapshot(InstallSnapshotResponse<u64>),
    Vote(VoteResponse<u64>),
    Forward(ForwardedResponse),
    Ready,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ForwardedOperation {
    CreateStream {
        stream: String,
    },
    Publish {
        stream: String,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
        published_at_ms: u64,
    },
    Poll {
        stream: String,
        consumer: String,
        #[serde(default)]
        max_response_bytes: Option<usize>,
    },
    Replay {
        stream: String,
        consumer: String,
        offset: Offset,
    },
    Ack {
        stream: String,
        consumer: String,
        offset: Offset,
    },
    ConfigureConsumer {
        stream: String,
        consumer: String,
        ack_timeout_ms: u64,
        max_delivery_attempts: Option<u32>,
    },
    InspectConsumer {
        stream: String,
        consumer: String,
    },
    PollGroup {
        stream: String,
        consumer: String,
        member: String,
        #[serde(default)]
        max_response_bytes: Option<usize>,
    },
    PollGroupBatch {
        stream: String,
        consumer: String,
        member: String,
        #[serde(default)]
        response_member: Option<String>,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    },
    AckGroup {
        stream: String,
        consumer: String,
        member: String,
        offset: Offset,
        delivery_token: String,
    },
    AckGroupBatch {
        stream: String,
        consumer: String,
        member: String,
        receipts: Vec<DeliveryReceipt>,
    },
    InitializeDataStream {
        stream: String,
        stream_id: String,
        group_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ForwardedResponse {
    CreateStream(Result<bool, ForwardError>),
    Publish(Result<Offset, ForwardError>),
    Poll(Result<PollResult, ForwardError>),
    Replay(Result<ReplayMessage, ForwardError>),
    Ack(Result<AckResult, ForwardError>),
    ConsumerPolicy(Result<ConsumerPolicy, ForwardError>),
    PollGroup(Result<PollResult, ForwardError>),
    PollGroupBatch(Result<Vec<ForwardedBatchMessage>, ForwardError>),
    AckGroup(Result<AckResult, ForwardError>),
    AckGroupBatch(Result<AckBatchResult, ForwardError>),
    InitializeDataStream(Result<bool, ForwardError>),
}

/// Compact message representation used only in Raft peer forwarding.
///
/// `Message` derives JSON serialization for byte vectors as integer arrays,
/// which can be several times larger than the public response. This shape
/// preserves UTF-8 payloads as strings and encodes other bytes as base64 so a
/// response admitted by the public batch byte bound remains forwardable.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ForwardedBatchMessage {
    stream: String,
    offset: Offset,
    key: Option<String>,
    #[serde(with = "forwarded_payload")]
    payload: Vec<u8>,
    published_at_ms: u64,
    #[serde(default)]
    delivery_token: Option<String>,
    #[serde(default)]
    delivery_attempt: Option<u32>,
}

#[derive(Serialize)]
struct ForwardedBatchMessageRef<'a> {
    stream: &'a str,
    offset: Offset,
    key: Option<&'a str>,
    #[serde(with = "forwarded_payload")]
    payload: &'a [u8],
    published_at_ms: u64,
    delivery_token: Option<&'a str>,
    delivery_attempt: Option<u32>,
}

impl<'a> From<&'a Message> for ForwardedBatchMessageRef<'a> {
    fn from(message: &'a Message) -> Self {
        Self {
            stream: &message.stream,
            offset: message.offset,
            key: message.key.as_deref(),
            payload: &message.payload,
            published_at_ms: message.published_at_ms,
            delivery_token: message.delivery_token.as_deref(),
            delivery_attempt: message.delivery_attempt,
        }
    }
}

#[derive(Serialize)]
enum PeerResponseRef<'a> {
    Forward(ForwardedResponseRef<'a>),
}

#[derive(Serialize)]
enum ForwardedResponseRef<'a> {
    PollGroupBatch(Result<Vec<ForwardedBatchMessageRef<'a>>, String>),
}

/// Count the exact JSON frame payload for a successful batch-poll response.
/// The writer counts bytes without retaining a second full frame buffer.
pub(crate) fn forwarded_batch_response_frame_len(
    messages: &[Message],
) -> Result<usize, serde_json::Error> {
    let response = PeerResponseRef::Forward(ForwardedResponseRef::PollGroupBatch(Ok(messages
        .iter()
        .map(ForwardedBatchMessageRef::from)
        .collect())));
    let mut counter = CountingWriter::default();
    serde_json::to_writer(&mut counter, &response)?;
    Ok(counter.bytes_written)
}

pub(crate) fn ensure_forwarded_batch_response_fits(
    messages: &[Message],
) -> Result<(), runnel_engine::BrokerError> {
    let frame_len = forwarded_batch_response_frame_len(messages).map_err(|error| {
        runnel_engine::BrokerError::Cluster(format!(
            "could not size forwarded consume-batch response: {error}"
        ))
    })?;
    if frame_len > framing::MAX_FRAME_SIZE as usize {
        return Err(runnel_engine::BrokerError::Cluster(format!(
            "forwarded consume-batch response is {frame_len} bytes, above the {} byte peer frame limit",
            framing::MAX_FRAME_SIZE
        )));
    }
    Ok(())
}

#[derive(Default)]
struct CountingWriter {
    bytes_written: usize,
}

impl std::io::Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes_written = self
            .bytes_written
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("serialized frame length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl From<Message> for ForwardedBatchMessage {
    fn from(message: Message) -> Self {
        Self {
            stream: message.stream,
            offset: message.offset,
            key: message.key,
            payload: message.payload,
            published_at_ms: message.published_at_ms,
            delivery_token: message.delivery_token,
            delivery_attempt: message.delivery_attempt,
        }
    }
}

impl From<ForwardedBatchMessage> for Message {
    fn from(message: ForwardedBatchMessage) -> Self {
        Self {
            stream: message.stream,
            offset: message.offset,
            key: message.key,
            payload: message.payload,
            published_at_ms: message.published_at_ms,
            delivery_token: message.delivery_token,
            delivery_attempt: message.delivery_attempt,
        }
    }
}

mod forwarded_payload {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::de::Error as _;
    use serde::{Deserialize, Serialize, Serializer};

    #[derive(Serialize)]
    #[serde(untagged)]
    enum PayloadRef<'a> {
        Utf8(&'a str),
        Binary { base64: String },
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Payload {
        Utf8(String),
        Binary { base64: String },
    }

    pub(super) fn serialize<S>(payload: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = match std::str::from_utf8(payload) {
            Ok(text) => PayloadRef::Utf8(text),
            Err(_) => PayloadRef::Binary {
                base64: STANDARD.encode(payload),
            },
        };
        encoded.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Payload::deserialize(deserializer)? {
            Payload::Utf8(text) => Ok(text.into_bytes()),
            Payload::Binary { base64 } => STANDARD.decode(base64).map_err(D::Error::custom),
        }
    }
}

#[cfg(test)]
mod forwarded_batch_tests {
    use super::*;
    use runnel_engine::{MAX_CONSUME_BATCH_RESPONSE_BYTES, poll_batch_response_len};

    #[test]
    fn compact_forwarded_batch_preserves_binary_and_receipt_fields() {
        let messages = vec![
            Message {
                stream: "events".to_owned(),
                offset: 8,
                key: Some("first".to_owned()),
                payload: "snowman ☃".as_bytes().to_vec(),
                published_at_ms: 123,
                delivery_token: Some("receipt-a".to_owned()),
                delivery_attempt: Some(4),
            },
            Message {
                stream: "events".to_owned(),
                offset: 9,
                key: None,
                payload: vec![0, 0xff, b'\n'],
                published_at_ms: 124,
                delivery_token: Some("receipt-b".to_owned()),
                delivery_attempt: Some(5),
            },
        ];
        let forwarded = messages
            .iter()
            .cloned()
            .map(ForwardedBatchMessage::from)
            .collect::<Vec<_>>();
        let decoded = serde_json::from_slice::<PeerResponse>(
            &serde_json::to_vec(&PeerResponse::Forward(ForwardedResponse::PollGroupBatch(
                Ok(forwarded),
            )))
            .unwrap(),
        )
        .unwrap();
        let PeerResponse::Forward(ForwardedResponse::PollGroupBatch(Ok(decoded))) = decoded else {
            panic!("expected forwarded batch poll response");
        };
        let decoded = decoded.into_iter().map(Message::from).collect::<Vec<_>>();
        assert_eq!(decoded, messages);

        let exact_frame_len = forwarded_batch_response_frame_len(&messages).unwrap();
        let actual_frame = serde_json::to_vec(&PeerResponse::Forward(
            ForwardedResponse::PollGroupBatch(Ok(messages
                .iter()
                .cloned()
                .map(ForwardedBatchMessage::from)
                .collect())),
        ))
        .unwrap();
        assert_eq!(exact_frame_len, actual_frame.len());
    }

    #[test]
    fn largest_public_batch_shape_fits_the_bounded_peer_frame() {
        let stream = "s".repeat(128);
        let consumer = "c".repeat(128);
        let member = "m".repeat(128);
        let mut messages = (0..1024)
            .map(|index| Message {
                stream: stream.clone(),
                offset: u64::MAX - index,
                key: Some("k".repeat(128)),
                payload: if index % 2 == 0 {
                    vec![b'x'; 32 * 1024]
                } else {
                    vec![0xff; 32 * 1024]
                },
                published_at_ms: u64::MAX,
                delivery_token: Some("f".repeat(64)),
                delivery_attempt: Some(u32::MAX),
            })
            .collect::<Vec<_>>();

        let current_public_len =
            poll_batch_response_len(&stream, &consumer, Some(&member), &messages);
        assert!(current_public_len < MAX_CONSUME_BATCH_RESPONSE_BYTES);
        let extra_text_bytes = MAX_CONSUME_BATCH_RESPONSE_BYTES - current_public_len;
        let text_message_count = messages.len() / 2;
        let bytes_per_message = extra_text_bytes / text_message_count;
        let remainder = extra_text_bytes % text_message_count;
        for message in messages.iter_mut().step_by(2) {
            message
                .payload
                .resize(message.payload.len() + bytes_per_message, b'x');
        }
        let first_text_payload_len = messages[0].payload.len();
        messages[0]
            .payload
            .resize(first_text_payload_len + remainder, b'x');

        let public_len = poll_batch_response_len(&stream, &consumer, Some(&member), &messages);
        let peer_len = forwarded_batch_response_frame_len(&messages).unwrap();
        assert_eq!(public_len, MAX_CONSUME_BATCH_RESPONSE_BYTES);
        assert!(peer_len <= public_len + 1024 * 1024);
        assert!(peer_len <= framing::MAX_FRAME_SIZE as usize);
        ensure_forwarded_batch_response_fits(&messages).unwrap();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ForwardError {
    NotLeader {
        leader_id: Option<u64>,
    },
    AckNotInFlight {
        consumer: String,
        offset: Offset,
    },
    StaleDelivery {
        consumer: String,
        offset: Offset,
    },
    InvalidBatchRequest(String),
    ConsumeBatchRecordTooLarge {
        max_bytes: usize,
    },
    ResponseTooLarge {
        max_bytes: usize,
    },
    RequestIdContentConflict,
    HistoryUnavailable {
        stream: String,
        requested_offset: Offset,
        earliest_offset: Offset,
        next_offset: Offset,
    },
    Message(String),
}
