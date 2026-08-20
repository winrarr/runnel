use std::future::Future;
use std::io;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Offset = u64;

pub type EngineFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, BrokerError>> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
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

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult>;

    fn health<'a>(&'a self) -> EngineFuture<'a, HealthSnapshot>;
}
