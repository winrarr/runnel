use std::future::Future;
use std::io;
use std::pin::Pin;
#[cfg(feature = "instrumentation")]
use std::time::Instant;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Measures one internal stage when the optional instrumentation feature is enabled.
///
/// The default implementation is an empty type and its constructor is always inlined, so
/// release builds do not retain timing calls unless instrumentation is explicitly enabled.
#[cfg(feature = "instrumentation")]
pub struct StageTimer {
    stage: &'static str,
    started: Instant,
}

#[cfg(not(feature = "instrumentation"))]
pub struct StageTimer;

impl StageTimer {
    #[inline(always)]
    pub fn new(stage: &'static str) -> Self {
        #[cfg(feature = "instrumentation")]
        {
            Self {
                stage,
                started: Instant::now(),
            }
        }

        #[cfg(not(feature = "instrumentation"))]
        {
            let _ = stage;
            Self
        }
    }
}

#[cfg(feature = "instrumentation")]
impl Drop for StageTimer {
    fn drop(&mut self) {
        tracing::trace!(
            target: "runnel::timing",
            stage = self.stage,
            elapsed_us = self.started.elapsed().as_micros() as u64,
            "stage complete"
        );
    }
}

pub type Offset = u64;

/// Maximum acknowledgement timeout accepted by a consumer-scoped policy.
///
/// The bound keeps an accidentally large policy from becoming an unbounded
/// lease in either engine while leaving the broker-wide legacy setting
/// unchanged for existing deployments.
pub const MAX_CONSUMER_POLICY_ACK_TIMEOUT_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// Maximum number of records accepted by one publish-batch engine operation.
pub const MAX_PUBLISH_BATCH_RECORDS: usize = 1024;

/// Maximum number of records returned by one consume-batch operation.
pub const MAX_CONSUME_BATCH_RECORDS: usize = 1024;

/// Hard line-size ceiling for a consume-batch response, including its newline.
pub const MAX_CONSUME_BATCH_RESPONSE_BYTES: usize = 65 * 1024 * 1024;

/// The caller-visible bounds for one consume-batch poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeBatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_wait_ms: u64,
}

/// One opaque receipt used to acknowledge a delivered record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryReceipt {
    pub offset: Offset,
    pub delivery_token: String,
}

/// Per-receipt result of a consume-batch acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckBatchItem {
    pub offset: Offset,
    pub outcome: AckBatchOutcome,
}

/// Result for one receipt in an acknowledgement vector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AckBatchOutcome {
    Confirmed,
    AlreadyConfirmed,
    Rejected { reason: AckBatchRejection },
}

/// Why an individual receipt was rejected while other receipts were evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckBatchRejection {
    NotInFlight,
    StaleDelivery,
}

/// Ordered per-receipt outcomes from one acknowledgement-vector operation.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AckBatchResult {
    pub outcomes: Vec<AckBatchItem>,
}

/// Validate public shape and resource bounds for a consume-batch poll.
pub fn validate_consume_batch_limits(limits: ConsumeBatchLimits) -> Result<(), BrokerError> {
    if !(1..=MAX_CONSUME_BATCH_RECORDS).contains(&limits.max_records) {
        return Err(BrokerError::InvalidBatchRequest(format!(
            "max_records must be between 1 and {MAX_CONSUME_BATCH_RECORDS}"
        )));
    }
    if limits.max_bytes == 0 || limits.max_bytes > MAX_CONSUME_BATCH_RESPONSE_BYTES {
        return Err(BrokerError::InvalidBatchRequest(format!(
            "max_bytes must be between 1 and {MAX_CONSUME_BATCH_RESPONSE_BYTES}"
        )));
    }
    Ok(())
}

/// Validate a receipt vector before the engine reads or mutates consumer state.
pub fn validate_ack_batch_receipts(receipts: &[DeliveryReceipt]) -> Result<(), BrokerError> {
    if receipts.is_empty() || receipts.len() > MAX_CONSUME_BATCH_RECORDS {
        return Err(BrokerError::InvalidBatchRequest(format!(
            "acknowledgement receipt count must be between 1 and {MAX_CONSUME_BATCH_RECORDS}"
        )));
    }
    let mut offsets = std::collections::HashSet::with_capacity(receipts.len());
    for receipt in receipts {
        if !offsets.insert(receipt.offset) {
            return Err(BrokerError::InvalidBatchRequest(format!(
                "duplicate acknowledgement offset {}",
                receipt.offset
            )));
        }
    }
    Ok(())
}

/// Measure the exact encoded JSON-lines size of the provisional `poll_batch`
/// response shape without allocating base64 copies of record payloads.
///
/// The matching wire response stores valid UTF-8 payloads as JSON text and
/// other payloads as padded standard base64. Metadata and the terminating
/// newline are included in the returned byte count.
pub fn poll_batch_response_len(
    stream: &str,
    consumer: &str,
    member: Option<&str>,
    messages: &[Message],
) -> usize {
    let mut sizer = PollBatchResponseSizer::new(stream, consumer, member);
    for message in messages {
        sizer.push(message);
    }
    sizer.encoded_len()
}

/// Incrementally measures an encoded poll-batch response while candidates are
/// selected. This avoids repeatedly encoding or rescanning accumulated payloads.
pub struct PollBatchResponseSizer {
    consumer: String,
    member: Option<String>,
    encoded_len: usize,
    messages: usize,
}

impl PollBatchResponseSizer {
    pub fn new(stream: &str, consumer: &str, member: Option<&str>) -> Self {
        let encoded_len = "{\"type\":\"poll_batch\",\"stream\":".len()
            + json_string_len(stream)
            + ",\"consumer\":".len()
            + json_string_len(consumer)
            + ",\"messages\":[".len()
            + "]}\n".len();
        Self {
            consumer: consumer.to_owned(),
            member: member.map(str::to_owned),
            encoded_len,
            messages: 0,
        }
    }

    pub fn projected_len(&self, message: &Message) -> usize {
        self.encoded_len
            .saturating_add(if self.messages == 0 { 0 } else { 1 })
            .saturating_add(poll_batch_message_len(
                message,
                &self.consumer,
                self.member.as_deref(),
            ))
    }

    pub fn push(&mut self, message: &Message) -> usize {
        self.encoded_len = self.projected_len(message);
        self.messages = self.messages.saturating_add(1);
        self.encoded_len
    }

    pub fn encoded_len(&self) -> usize {
        self.encoded_len
    }
}

fn poll_batch_message_len(message: &Message, consumer: &str, member: Option<&str>) -> usize {
    let mut length = "{\"stream\":".len() + json_string_len(&message.stream);
    length += ",\"consumer\":".len() + json_string_len(consumer);
    if let Some(member) = member {
        length += ",\"member\":".len() + json_string_len(member);
    }
    length += ",\"offset\":".len() + message.offset.to_string().len();
    length += ",\"key\":".len() + message.key.as_deref().map_or("null".len(), json_string_len);
    if let Ok(payload) = std::str::from_utf8(&message.payload) {
        length += ",\"payload\":".len() + json_string_len(payload);
    } else {
        let encoded_len = message.payload.len().saturating_add(2) / 3 * 4;
        length += ",\"payload_base64\":".len() + encoded_len + 2;
    }
    length += ",\"published_at_ms\":".len() + message.published_at_ms.to_string().len();
    if let Some(delivery_token) = message.delivery_token.as_deref() {
        length += ",\"delivery_token\":".len() + json_string_len(delivery_token);
    }
    if let Some(delivery_attempt) = message.delivery_attempt {
        length += ",\"delivery_attempt\":".len() + delivery_attempt.to_string().len();
    }
    length + 1
}

fn json_string_len(value: &str) -> usize {
    value.bytes().fold(2, |length, byte| {
        length.saturating_add(match byte {
            b'"' | b'\\' | b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' => 2,
            0x00..=0x1f => 6,
            _ => 1,
        })
    })
}

/// One opaque record in a publish batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecord {
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub request_id: Option<String>,
}

/// Durable retry settings associated with one named consumer.
///
/// Version zero with `configured = false` represents the broker-wide legacy
/// fallback. Configured policies start at version one and advance only when
/// their values change. The version is persisted with the consumer so a
/// delivery can retain the policy that selected its attempt budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerPolicy {
    pub version: u64,
    pub configured: bool,
    pub ack_timeout_ms: u64,
    pub max_delivery_attempts: Option<u32>,
}

impl ConsumerPolicy {
    /// Construct the legacy broker-wide fallback view.
    pub const fn legacy(ack_timeout_ms: u64, max_delivery_attempts: Option<u32>) -> Self {
        Self {
            version: 0,
            configured: false,
            ack_timeout_ms,
            max_delivery_attempts,
        }
    }

    /// Construct a configured policy with the supplied durable version.
    pub const fn configured(
        version: u64,
        ack_timeout_ms: u64,
        max_delivery_attempts: Option<u32>,
    ) -> Self {
        Self {
            version,
            configured: true,
            ack_timeout_ms,
            max_delivery_attempts,
        }
    }
}

/// Validate values supplied by a consumer-policy configuration request.
pub fn validate_consumer_policy(
    ack_timeout_ms: u64,
    max_delivery_attempts: Option<u32>,
) -> Result<(), BrokerError> {
    if ack_timeout_ms > MAX_CONSUMER_POLICY_ACK_TIMEOUT_MS {
        return Err(BrokerError::Configuration(format!(
            "consumer acknowledgement timeout must not exceed {MAX_CONSUMER_POLICY_ACK_TIMEOUT_MS} milliseconds"
        )));
    }
    if max_delivery_attempts == Some(0) {
        return Err(BrokerError::Configuration(
            "max delivery attempts must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

/// The result for one record in a publish batch.
pub type PublishRecordOutcome = Result<Offset, BrokerError>;

pub type EngineFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, BrokerError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub stream: String,
    pub offset: Offset,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub published_at_ms: u64,
    #[serde(default)]
    pub delivery_token: Option<String>,
    #[serde(default)]
    pub delivery_attempt: Option<u32>,
}

/// A message returned by an explicit replay read.
///
/// Replay is read-only in the first protocol slice: it does not create an
/// in-flight delivery, increment delivery attempts, or change consumer
/// progress. A replay result therefore cannot be acknowledged as an ordinary
/// delivery through the typed engine model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayMessage {
    pub stream: String,
    pub offset: Offset,
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub published_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PollResult {
    Message(Message),
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckResult {
    Acknowledged,
    AlreadyAcknowledged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub streams: usize,
    pub storage_bytes: u64,
    pub in_flight_deliveries: u64,
    pub redeliveries: u64,
    pub dead_letters: u64,
}

/// Stable semantic category for a broker failure.
///
/// The category deliberately does not expose the representation of the
/// backend that produced the error. Callers that need a human-readable cause
/// should retain the [`BrokerError`] itself and its [`std::error::Error`]
/// source; callers deciding whether an operation may be retried should use
/// [`BrokerError::outcome`] instead of matching backend-specific variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerErrorKind {
    /// The request or its arguments are invalid.
    InvalidRequest,
    /// The requested stream or other durable resource does not exist.
    ResourceNotFound,
    /// The resource exists but is not currently available for serving.
    ResourceNotReady,
    /// The delivery state rejects this acknowledgement or delivery attempt.
    DeliveryRejected,
    /// A retained public request ID was reused with different message content.
    RequestIdContentConflict,
    /// The requested retained history is not available.
    HistoryUnavailable,
    /// Durable message data could not be decoded or validated.
    CorruptData,
    /// A durable storage operation failed.
    Storage,
    /// Consumer-state persistence or another internal state operation failed.
    State,
    /// The broker configuration is invalid.
    Configuration,
    /// The request needs routing or leadership that is not currently present.
    Routing,
    /// A distributed-engine failure occurred without a more specific
    /// semantic category.
    Cluster,
    /// An unexpected in-process failure occurred.
    Internal,
}

/// What an engine can safely say about a failed operation.
///
/// `Confirmed` is represented by a successful `Result`; this enum only covers
/// failures. `Unknown` is intentionally conservative: the engine could not
/// prove that the operation was not applied, so a caller must resolve the
/// intent before replaying it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerErrorOutcome {
    /// The operation was definitely not applied and the request or state is
    /// not valid for an unchanged retry.
    Rejected,
    /// The operation was definitely not applied, but a later attempt may
    /// succeed.
    Retryable,
    /// The operation may have crossed a durable boundary; do not blindly
    /// replay it.
    Unknown,
}

/// The local consumer-journal boundary where persistence failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerStatePersistStage {
    /// The event was not appended, so retrying the unchanged operation is safe.
    BeforeAppend,
    /// The append may contain a partial event and must be reconciled first.
    Append,
    /// The complete event was written but its durability sync failed.
    Sync,
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("invalid {kind} name '{name}'; use 1-128 ASCII letters, digits, '.', '_', or '-'")]
    InvalidName { kind: &'static str, name: String },
    #[error("stream '{0}' does not exist")]
    StreamNotFound(String),
    #[error("stream '{0}' is not ready")]
    StreamNotReady(String),
    #[error("consumer '{consumer}' has no in-flight message for offset {offset}")]
    AckNotInFlight { consumer: String, offset: Offset },
    #[error("consumer '{consumer}' has a stale delivery for offset {offset}")]
    StaleDelivery { consumer: String, offset: Offset },
    #[error("consumer '{consumer}' must acknowledge offset {expected} before offset {received}")]
    OutOfOrderAck {
        consumer: String,
        expected: Offset,
        received: Offset,
    },
    #[error(
        "replay offset {requested_offset} is unavailable for stream '{stream}'; available offsets are [{earliest_offset}, {next_offset})"
    )]
    HistoryUnavailable {
        stream: String,
        requested_offset: Offset,
        earliest_offset: Offset,
        next_offset: Offset,
    },
    #[error("record log is malformed at offset {0}")]
    CorruptRecord(Offset),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("consumer state error: {0}")]
    State(#[from] serde_json::Error),
    #[error("consumer state persistence failed at {stage:?}: {message}")]
    ConsumerStatePersistence {
        stage: ConsumerStatePersistStage,
        message: String,
    },
    #[error("request ID was already used for different message content")]
    RequestIdContentConflict,
    #[error("broker lock is poisoned")]
    LockPoisoned,
    #[error("invalid broker configuration: {0}")]
    Configuration(String),
    #[error("invalid consume-batch request: {0}")]
    InvalidBatchRequest(String),
    #[error(
        "the first eligible message cannot fit in the {max_bytes}-byte consume-batch response bound"
    )]
    ConsumeBatchRecordTooLarge { max_bytes: usize },
    #[error("request must be sent to the elected leader {leader_id:?}")]
    NotLeader { leader_id: Option<u64> },
    #[error("cluster error: {0}")]
    Cluster(String),
}

impl BrokerError {
    /// Return the stable semantic category for this failure.
    ///
    /// This is the engine-facing boundary. The concrete variants remain
    /// available so operators can inspect diagnostic details, but callers do
    /// not need to know whether a storage error came from an `io::Error`, a
    /// JSON state file, a lock, or a distributed transport.
    pub fn kind(&self) -> BrokerErrorKind {
        match self {
            Self::InvalidName { .. } => BrokerErrorKind::InvalidRequest,
            Self::StreamNotFound(_) => BrokerErrorKind::ResourceNotFound,
            Self::StreamNotReady(_) => BrokerErrorKind::ResourceNotReady,
            Self::AckNotInFlight { .. }
            | Self::StaleDelivery { .. }
            | Self::OutOfOrderAck { .. } => BrokerErrorKind::DeliveryRejected,
            Self::RequestIdContentConflict => BrokerErrorKind::RequestIdContentConflict,
            Self::HistoryUnavailable { .. } => BrokerErrorKind::HistoryUnavailable,
            Self::CorruptRecord(_) => BrokerErrorKind::CorruptData,
            Self::Io(_) => BrokerErrorKind::Storage,
            Self::State(_) => BrokerErrorKind::State,
            Self::ConsumerStatePersistence { .. } => BrokerErrorKind::State,
            Self::LockPoisoned => BrokerErrorKind::Internal,
            Self::Configuration(_) => BrokerErrorKind::Configuration,
            Self::InvalidBatchRequest(_) | Self::ConsumeBatchRecordTooLarge { .. } => {
                BrokerErrorKind::InvalidRequest
            }
            Self::NotLeader { .. } => BrokerErrorKind::Routing,
            Self::Cluster(_) => BrokerErrorKind::Cluster,
        }
    }

    /// Return the safe retry boundary for this failure.
    ///
    /// The result is independent of storage representation and cluster
    /// topology. In particular, generic storage, state, and cluster failures
    /// remain `Unknown` because an engine cannot prove that a mutation did not
    /// commit merely from the backend error it received.
    pub fn outcome(&self) -> BrokerErrorOutcome {
        if matches!(
            self,
            Self::ConsumerStatePersistence {
                stage: ConsumerStatePersistStage::BeforeAppend,
                ..
            }
        ) {
            return BrokerErrorOutcome::Retryable;
        }
        match self.kind() {
            BrokerErrorKind::ResourceNotReady | BrokerErrorKind::Routing => {
                BrokerErrorOutcome::Retryable
            }
            BrokerErrorKind::CorruptData
            | BrokerErrorKind::Storage
            | BrokerErrorKind::State
            | BrokerErrorKind::Internal
            | BrokerErrorKind::Cluster => BrokerErrorOutcome::Unknown,
            BrokerErrorKind::InvalidRequest
            | BrokerErrorKind::ResourceNotFound
            | BrokerErrorKind::DeliveryRejected
            | BrokerErrorKind::RequestIdContentConflict
            | BrokerErrorKind::HistoryUnavailable
            | BrokerErrorKind::Configuration => BrokerErrorOutcome::Rejected,
        }
    }
}

pub trait Engine: Send + Sync {
    fn create_stream<'a>(&'a self, stream: &'a str) -> EngineFuture<'a, bool>;

    fn publish<'a>(
        &'a self,
        stream: &'a str,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
    ) -> EngineFuture<'a, Offset>;

    /// Publish records independently in input order.
    ///
    /// The default implementation preserves the no-atomicity contract by
    /// using the single-record operation for each record. Engines may
    /// override it to amortize storage work while retaining the same
    /// per-record outcomes.
    fn publish_batch<'a>(
        &'a self,
        stream: &'a str,
        records: Vec<PublishRecord>,
    ) -> EngineFuture<'a, Vec<PublishRecordOutcome>> {
        if records.len() > MAX_PUBLISH_BATCH_RECORDS {
            return Box::pin(async {
                Err(BrokerError::Configuration(format!(
                    "publish batch contains more than {MAX_PUBLISH_BATCH_RECORDS} records"
                )))
            });
        }
        Box::pin(async move {
            let mut outcomes = Vec::with_capacity(records.len());
            for record in records {
                outcomes.push(
                    self.publish(stream, record.key, record.payload, record.request_id)
                        .await,
                );
            }
            Ok(outcomes)
        })
    }

    fn poll<'a>(&'a self, stream: &'a str, consumer: &'a str) -> EngineFuture<'a, PollResult>;

    /// Return one ordered, bounded set for an ordinary consumer. The ordinary
    /// consumer name is also the member identity for receipts.
    fn poll_batch<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        limits: ConsumeBatchLimits,
    ) -> EngineFuture<'a, Vec<Message>> {
        let _ = (stream, consumer, limits);
        Box::pin(async {
            Err(BrokerError::Cluster(
                "consume batches are not supported by this engine".to_owned(),
            ))
        })
    }

    /// Return one ordered, bounded set for a shared-consumer member.
    fn poll_group_batch<'a>(
        &'a self,
        _stream: &'a str,
        _consumer: &'a str,
        _member: &'a str,
        _limits: ConsumeBatchLimits,
    ) -> EngineFuture<'a, Vec<Message>> {
        Box::pin(async {
            Err(BrokerError::Cluster(
                "shared consume batches are not supported by this engine".to_owned(),
            ))
        })
    }

    /// Configure durable retry settings for one named consumer.
    ///
    /// Engines that do not support consumer configuration retain the shared
    /// broker contract by returning a semantic unsupported-operation error.
    fn configure_consumer<'a>(
        &'a self,
        _stream: &'a str,
        _consumer: &'a str,
        _ack_timeout_ms: u64,
        _max_delivery_attempts: Option<u32>,
    ) -> EngineFuture<'a, ConsumerPolicy> {
        Box::pin(async {
            Err(BrokerError::Cluster(
                "consumer retry policy is not supported by this engine".to_owned(),
            ))
        })
    }

    /// Inspect durable retry settings for one named consumer.
    fn inspect_consumer<'a>(
        &'a self,
        _stream: &'a str,
        _consumer: &'a str,
    ) -> EngineFuture<'a, ConsumerPolicy> {
        Box::pin(async {
            Err(BrokerError::Cluster(
                "consumer retry policy is not supported by this engine".to_owned(),
            ))
        })
    }

    /// Read one retained record at an inclusive logical offset without
    /// changing the consumer checkpoint or delivery state.
    fn replay<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, ReplayMessage>;

    fn poll_group<'a>(
        &'a self,
        _stream: &'a str,
        _consumer: &'a str,
        _member: &'a str,
    ) -> EngineFuture<'a, PollResult> {
        Box::pin(async {
            Err(BrokerError::Cluster(
                "shared consumer delivery is not supported by this engine".to_owned(),
            ))
        })
    }

    /// Acknowledge one ordinary scalar delivery by offset.
    ///
    /// A delivery assigned by `poll_batch` is receipt-fenced and cannot be
    /// acknowledged through this offset-only operation; use `ack_batch` for
    /// that active set.
    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult>;

    /// Acknowledge an ordinary consumer's receipts independently in one
    /// durable acknowledgement-subset transition. Each receipt is required
    /// for an entry assigned by `poll_batch`; scalar offset-only ack cannot
    /// bypass the batch delivery fence.
    fn ack_batch<'a>(
        &'a self,
        _stream: &'a str,
        _consumer: &'a str,
        _receipts: Vec<DeliveryReceipt>,
    ) -> EngineFuture<'a, AckBatchResult> {
        Box::pin(async {
            Err(BrokerError::Cluster(
                "consume-batch acknowledgements are not supported by this engine".to_owned(),
            ))
        })
    }

    fn ack_group<'a>(
        &'a self,
        _stream: &'a str,
        _consumer: &'a str,
        _member: &'a str,
        _offset: Offset,
        _delivery_token: &'a str,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async {
            Err(BrokerError::Cluster(
                "shared consumer acknowledgements are not supported by this engine".to_owned(),
            ))
        })
    }

    /// Acknowledge a shared member's receipts independently in one durable
    /// acknowledgement-subset transition. Each receipt is required for an
    /// entry assigned by `poll_group_batch`; scalar offset-only ack cannot
    /// bypass the batch delivery fence.
    fn ack_group_batch<'a>(
        &'a self,
        _stream: &'a str,
        _consumer: &'a str,
        _member: &'a str,
        _receipts: Vec<DeliveryReceipt>,
    ) -> EngineFuture<'a, AckBatchResult> {
        Box::pin(async {
            Err(BrokerError::Cluster(
                "shared consume-batch acknowledgements are not supported by this engine".to_owned(),
            ))
        })
    }

    fn health<'a>(&'a self) -> EngineFuture<'a, HealthSnapshot>;
}

#[cfg(test)]
mod tests {
    use super::{
        BrokerError, BrokerErrorKind, BrokerErrorOutcome, MAX_CONSUMER_POLICY_ACK_TIMEOUT_MS,
        validate_consumer_policy,
    };
    use std::io;

    #[test]
    fn classifies_failures_without_exposing_backend_details() {
        let state_error = serde_json::from_str::<String>("not-json").unwrap_err();
        let cases = [
            (
                BrokerError::InvalidName {
                    kind: "stream",
                    name: "bad/name".to_owned(),
                },
                BrokerErrorKind::InvalidRequest,
                BrokerErrorOutcome::Rejected,
            ),
            (
                BrokerError::StreamNotFound("events".to_owned()),
                BrokerErrorKind::ResourceNotFound,
                BrokerErrorOutcome::Rejected,
            ),
            (
                BrokerError::StreamNotReady("events".to_owned()),
                BrokerErrorKind::ResourceNotReady,
                BrokerErrorOutcome::Retryable,
            ),
            (
                BrokerError::AckNotInFlight {
                    consumer: "worker".to_owned(),
                    offset: 0,
                },
                BrokerErrorKind::DeliveryRejected,
                BrokerErrorOutcome::Rejected,
            ),
            (
                BrokerError::StaleDelivery {
                    consumer: "worker".to_owned(),
                    offset: 0,
                },
                BrokerErrorKind::DeliveryRejected,
                BrokerErrorOutcome::Rejected,
            ),
            (
                BrokerError::OutOfOrderAck {
                    consumer: "worker".to_owned(),
                    expected: 0,
                    received: 1,
                },
                BrokerErrorKind::DeliveryRejected,
                BrokerErrorOutcome::Rejected,
            ),
            (
                BrokerError::RequestIdContentConflict,
                BrokerErrorKind::RequestIdContentConflict,
                BrokerErrorOutcome::Rejected,
            ),
            (
                BrokerError::HistoryUnavailable {
                    stream: "events".to_owned(),
                    requested_offset: 10,
                    earliest_offset: 0,
                    next_offset: 1,
                },
                BrokerErrorKind::HistoryUnavailable,
                BrokerErrorOutcome::Rejected,
            ),
            (
                BrokerError::CorruptRecord(0),
                BrokerErrorKind::CorruptData,
                BrokerErrorOutcome::Unknown,
            ),
            (
                BrokerError::Io(io::Error::other("disk failure")),
                BrokerErrorKind::Storage,
                BrokerErrorOutcome::Unknown,
            ),
            (
                BrokerError::State(state_error),
                BrokerErrorKind::State,
                BrokerErrorOutcome::Unknown,
            ),
            (
                BrokerError::ConsumerStatePersistence {
                    stage: super::ConsumerStatePersistStage::BeforeAppend,
                    message: "injected".to_owned(),
                },
                BrokerErrorKind::State,
                BrokerErrorOutcome::Retryable,
            ),
            (
                BrokerError::ConsumerStatePersistence {
                    stage: super::ConsumerStatePersistStage::Append,
                    message: "injected".to_owned(),
                },
                BrokerErrorKind::State,
                BrokerErrorOutcome::Unknown,
            ),
            (
                BrokerError::ConsumerStatePersistence {
                    stage: super::ConsumerStatePersistStage::Sync,
                    message: "injected".to_owned(),
                },
                BrokerErrorKind::State,
                BrokerErrorOutcome::Unknown,
            ),
            (
                BrokerError::LockPoisoned,
                BrokerErrorKind::Internal,
                BrokerErrorOutcome::Unknown,
            ),
            (
                BrokerError::Configuration("bad setting".to_owned()),
                BrokerErrorKind::Configuration,
                BrokerErrorOutcome::Rejected,
            ),
            (
                BrokerError::NotLeader { leader_id: Some(2) },
                BrokerErrorKind::Routing,
                BrokerErrorOutcome::Retryable,
            ),
            (
                BrokerError::Cluster("quorum lost".to_owned()),
                BrokerErrorKind::Cluster,
                BrokerErrorOutcome::Unknown,
            ),
        ];

        for (error, expected_kind, expected_outcome) in cases {
            assert_eq!(error.kind(), expected_kind, "{error}");
            assert_eq!(error.outcome(), expected_outcome, "{error}");
        }
    }

    #[test]
    fn validates_consumer_policy_bounds() {
        assert!(validate_consumer_policy(0, Some(1)).is_ok());
        assert!(validate_consumer_policy(MAX_CONSUMER_POLICY_ACK_TIMEOUT_MS, None).is_ok());
        assert!(validate_consumer_policy(0, Some(0)).is_err());
        assert!(validate_consumer_policy(MAX_CONSUMER_POLICY_ACK_TIMEOUT_MS + 1, None).is_err());
    }

    #[test]
    fn retains_diagnostic_sources_for_backend_failures() {
        let io_error = BrokerError::Io(io::Error::other("disk failure"));
        assert!(std::error::Error::source(&io_error).is_some());

        let state_error = serde_json::from_str::<String>("not-json").unwrap_err();
        let state_error = BrokerError::State(state_error);
        assert!(std::error::Error::source(&state_error).is_some());
    }
}
