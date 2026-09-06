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

/// Maximum number of records accepted by one publish-batch engine operation.
pub const MAX_PUBLISH_BATCH_RECORDS: usize = 1024;

/// One opaque record in a publish batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecord {
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub request_id: Option<String>,
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
    #[error("broker lock is poisoned")]
    LockPoisoned,
    #[error("invalid broker configuration: {0}")]
    Configuration(String),
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
            Self::HistoryUnavailable { .. } => BrokerErrorKind::HistoryUnavailable,
            Self::CorruptRecord(_) => BrokerErrorKind::CorruptData,
            Self::Io(_) => BrokerErrorKind::Storage,
            Self::State(_) => BrokerErrorKind::State,
            Self::LockPoisoned => BrokerErrorKind::Internal,
            Self::Configuration(_) => BrokerErrorKind::Configuration,
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

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult>;

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

    fn health<'a>(&'a self) -> EngineFuture<'a, HealthSnapshot>;
}

#[cfg(test)]
mod tests {
    use super::{BrokerError, BrokerErrorKind, BrokerErrorOutcome};
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
    fn retains_diagnostic_sources_for_backend_failures() {
        let io_error = BrokerError::Io(io::Error::other("disk failure"));
        assert!(std::error::Error::source(&io_error).is_some());

        let state_error = serde_json::from_str::<String>("not-json").unwrap_err();
        let state_error = BrokerError::State(state_error);
        assert!(std::error::Error::source(&state_error).is_some());
    }
}
