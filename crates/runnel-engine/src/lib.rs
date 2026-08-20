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
    pub redeliveries: u64,
    pub dead_letters: u64,
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

pub trait Engine: Send + Sync {
    fn create_stream<'a>(&'a self, stream: &'a str) -> EngineFuture<'a, bool>;

    fn publish<'a>(
        &'a self,
        stream: &'a str,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
    ) -> EngineFuture<'a, Offset>;

    fn poll<'a>(&'a self, stream: &'a str, consumer: &'a str) -> EngineFuture<'a, PollResult>;

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
