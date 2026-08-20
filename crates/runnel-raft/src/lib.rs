#![allow(clippy::result_large_err)]

//! OpenRaft integration behind Runnel's topology-free engine contract.
//!
//! The persistent engine uses versioned local files and a framed TCP peer
//! protocol. It is an early clustered backend: membership is static, each
//! stream has a data group, and public requests are routed to the elected
//! leader through the internal peer protocol.

use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openraft::error::{ClientWriteError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig,
    SnapshotMeta, SnapshotPolicy, StorageError, StorageIOError, StoredMembership,
};
use runnel_engine::{AckResult, BrokerError, Engine, EngineFuture, Message, Offset, PollResult};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

mod log_store;
mod network;

pub type NodeId = u64;
pub const METADATA_GROUP_ID: &str = "metadata";

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
        NodeId = NodeId,
        Node = BasicNode,
);

pub type Raft = openraft::Raft<TypeConfig>;

pub async fn serve_peer(
    listener: tokio::net::TcpListener,
    manager: Arc<GroupManager>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    network::serve(listener, manager, shutdown).await
}

const FORMAT_VERSION: u32 = 2;
// These defaults keep the consensus log bounded while the snapshot format is
// still intentionally simple. The snapshot cadence must be revisited with
// retained-data benchmarks before this backend is used for large streams.
const SNAPSHOT_LOG_THRESHOLD: u64 = 32;
const REPLICATION_LAG_THRESHOLD: u64 = 64;
const SNAPSHOT_LOGS_TO_KEEP: u64 = 4;
// Keep individual peer snapshot RPCs bounded; interrupted transfers restart
// from the beginning in the current in-memory receiver.
const SNAPSHOT_CHUNK_SIZE: u64 = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    CreateStream {
        stream: String,
        #[serde(default)]
        stream_id: Option<String>,
        #[serde(default)]
        group_id: Option<String>,
    },
    InitializeDataStream {
        stream: String,
        stream_id: String,
        group_id: String,
    },
    ActivateStream {
        stream: String,
    },
    Publish {
        stream: String,
        key: Option<String>,
        payload: Vec<u8>,
        published_at_ms: u64,
        #[serde(default)]
        request_id: Option<String>,
    },
    Ack {
        stream: String,
        consumer: String,
        offset: Offset,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommandResponse {
    StreamCreated { created: bool },
    DataStreamInitialized { initialized: bool },
    StreamActivated { activated: bool },
    Published { offset: Offset },
    Acknowledged,
    AlreadyAcknowledged,
    OutOfOrderAck { expected: Offset, received: Offset },
    StreamNotFound,
    Noop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMessage {
    key: Option<String>,
    payload: Vec<u8>,
    published_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum StreamLifecycle {
    Creating,
    #[default]
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamMetadata {
    pub stream_id: String,
    pub group_id: String,
    pub lifecycle: StreamLifecycle,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StreamState {
    stream_id: String,
    group_id: String,
    lifecycle: StreamLifecycle,
    messages: Vec<StoredMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
enum GroupKind {
    #[default]
    Combined,
    Metadata,
    Data {
        stream: String,
        stream_id: String,
        group_id: String,
    },
}

impl StreamState {
    fn active(stream_id: String, group_id: String) -> Self {
        Self {
            stream_id,
            group_id,
            lifecycle: StreamLifecycle::Active,
            messages: Vec::new(),
        }
    }

    fn metadata(&self, stream: &str) -> StreamMetadata {
        let (stream_id, group_id) = stream_identity(stream);
        StreamMetadata {
            stream_id: if self.stream_id.is_empty() {
                stream_id
            } else {
                self.stream_id.clone()
            },
            group_id: if self.group_id.is_empty() {
                group_id
            } else {
                self.group_id.clone()
            },
            lifecycle: self.lifecycle.clone(),
        }
    }

    fn is_active(&self) -> bool {
        self.lifecycle == StreamLifecycle::Active
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SnapshotState {
    streams: BTreeMap<String, StreamState>,
    consumers: BTreeMap<(String, String), Offset>,
    dedup: BTreeMap<String, BTreeMap<String, Offset>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    state: SnapshotState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedState {
    version: u32,
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    streams: BTreeMap<String, PersistedStreamData>,
    consumers: Vec<PersistedConsumer>,
    #[serde(default)]
    dedup: BTreeMap<String, BTreeMap<String, Offset>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSnapshotState {
    #[serde(default = "legacy_format_version")]
    version: u32,
    streams: BTreeMap<String, PersistedStreamData>,
    consumers: Vec<PersistedConsumer>,
    #[serde(default)]
    dedup: BTreeMap<String, BTreeMap<String, Offset>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum PersistedStreamData {
    Legacy(Vec<StoredMessage>),
    Current(PersistedStream),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedStream {
    #[serde(default)]
    stream_id: String,
    #[serde(default)]
    group_id: String,
    #[serde(default)]
    lifecycle: Option<StreamLifecycle>,
    messages: Vec<StoredMessage>,
}

impl PersistedStreamData {
    fn current(state: &StreamState) -> Self {
        Self::Current(PersistedStream {
            stream_id: state.stream_id.clone(),
            group_id: state.group_id.clone(),
            lifecycle: Some(state.lifecycle.clone()),
            messages: state.messages.clone(),
        })
    }

    fn into_state(self, stream: &str) -> StreamState {
        match self {
            Self::Legacy(messages) => {
                let (stream_id, group_id) = stream_identity(stream);
                StreamState {
                    stream_id,
                    group_id,
                    lifecycle: StreamLifecycle::Active,
                    messages,
                }
            }
            Self::Current(stream_state) => StreamState {
                stream_id: stream_state.stream_id,
                group_id: stream_state.group_id,
                lifecycle: stream_state.lifecycle.unwrap_or_default(),
                messages: stream_state.messages,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedConsumer {
    stream: String,
    consumer: String,
    offset: Offset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

#[derive(Debug, Default)]
struct SnapshotMetrics {
    builds_started: AtomicU64,
    builds_completed: AtomicU64,
    build_failures: AtomicU64,
    installs_started: AtomicU64,
    installs_completed: AtomicU64,
    install_failures: AtomicU64,
    install_bytes: AtomicU64,
    installs_in_progress: AtomicU64,
    transfer_chunks: AtomicU64,
    transfer_final_chunks: AtomicU64,
    transfer_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotMetricsSnapshot {
    pub builds_started: u64,
    pub builds_completed: u64,
    pub build_failures: u64,
    pub installs_started: u64,
    pub installs_completed: u64,
    pub install_failures: u64,
    pub install_bytes: u64,
    pub installs_in_progress: u64,
    pub transfer_chunks: u64,
    pub transfer_final_chunks: u64,
    pub transfer_bytes: u64,
}

impl SnapshotMetrics {
    fn snapshot(&self) -> SnapshotMetricsSnapshot {
        SnapshotMetricsSnapshot {
            builds_started: self.builds_started.load(Ordering::Relaxed),
            builds_completed: self.builds_completed.load(Ordering::Relaxed),
            build_failures: self.build_failures.load(Ordering::Relaxed),
            installs_started: self.installs_started.load(Ordering::Relaxed),
            installs_completed: self.installs_completed.load(Ordering::Relaxed),
            install_failures: self.install_failures.load(Ordering::Relaxed),
            install_bytes: self.install_bytes.load(Ordering::Relaxed),
            installs_in_progress: self.installs_in_progress.load(Ordering::Relaxed),
            transfer_chunks: self.transfer_chunks.load(Ordering::Relaxed),
            transfer_final_chunks: self.transfer_final_chunks.load(Ordering::Relaxed),
            transfer_bytes: self.transfer_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
struct StateMachineStore {
    state: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    path: Option<PathBuf>,
    kind: GroupKind,
    metrics: Arc<SnapshotMetrics>,
}

impl StateMachineStore {
    fn open(path: impl AsRef<Path>, kind: GroupKind) -> Result<Self, BrokerError> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;
        let state_path = path.join("state-machine.json");
        let state = if state_path.exists() {
            let bytes = fs::read(&state_path)?;
            let persisted: PersistedState = serde_json::from_slice(&bytes)?;
            if !matches!(persisted.version, 1 | FORMAT_VERSION) {
                return Err(BrokerError::Cluster(format!(
                    "unsupported state-machine format version {}",
                    persisted.version
                )));
            }
            StateMachineData {
                last_applied_log: persisted.last_applied_log,
                last_membership: persisted.last_membership,
                state: SnapshotState {
                    streams: persisted
                        .streams
                        .into_iter()
                        .map(|(stream, persisted)| {
                            let state = persisted.into_state(&stream);
                            (stream, state)
                        })
                        .collect(),
                    consumers: persisted
                        .consumers
                        .into_iter()
                        .map(|consumer| ((consumer.stream, consumer.consumer), consumer.offset))
                        .collect(),
                    dedup: persisted.dedup,
                },
            }
        } else {
            StateMachineData::default()
        };
        let snapshot_path = path.join("snapshot.json");
        let current_snapshot = if snapshot_path.exists() {
            let bytes = fs::read(&snapshot_path)?;
            let snapshot: StoredSnapshot = serde_json::from_slice(&bytes)?;
            validate_snapshot_data(&snapshot.data).map_err(|error| {
                BrokerError::Cluster(format!(
                    "invalid persisted snapshot '{}': {error}",
                    snapshot_path.display()
                ))
            })?;
            Some(snapshot)
        } else {
            None
        };
        Ok(Self {
            state: RwLock::new(state),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(current_snapshot),
            path: Some(path),
            kind,
            metrics: Arc::new(SnapshotMetrics::default()),
        })
    }

    fn persist_state(&self, state: &StateMachineData) -> Result<(), StorageError<NodeId>> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let persisted = PersistedState {
            version: FORMAT_VERSION,
            last_applied_log: state.last_applied_log,
            last_membership: state.last_membership.clone(),
            streams: state
                .state
                .streams
                .iter()
                .map(|(stream, state)| (stream.clone(), PersistedStreamData::current(state)))
                .collect(),
            consumers: state
                .state
                .consumers
                .iter()
                .map(|((stream, consumer), offset)| PersistedConsumer {
                    stream: stream.clone(),
                    consumer: consumer.clone(),
                    offset: *offset,
                })
                .collect(),
            dedup: state.state.dedup.clone(),
        };
        let bytes = serde_json::to_vec(&persisted)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        atomic_write(&path.join("state-machine.json"), &bytes)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        Ok(())
    }

    async fn persist_snapshot(
        &self,
        snapshot: &StoredSnapshot,
    ) -> Result<(), StorageError<NodeId>> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(snapshot)
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
        atomic_write(&path.join("snapshot.json"), &bytes)
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
        Ok(())
    }

    async fn poll(&self, stream: &str, consumer: &str) -> Result<PollResult, BrokerError> {
        let state = self.state.read().await;
        let Some(stream_state) = state.state.streams.get(stream) else {
            return Err(BrokerError::StreamNotFound(stream.to_owned()));
        };
        if !stream_state.is_active() {
            return Err(BrokerError::Cluster(format!(
                "stream '{stream}' is not active"
            )));
        }
        let offset = state
            .state
            .consumers
            .get(&(stream.to_owned(), consumer.to_owned()))
            .copied()
            .unwrap_or_default();
        let Some(message) = stream_state.messages.get(offset as usize) else {
            return Ok(PollResult::Empty);
        };
        Ok(PollResult::Message(Message {
            stream: stream.to_owned(),
            offset,
            key: message.key.clone(),
            payload: message.payload.clone(),
            published_at_ms: message.published_at_ms,
        }))
    }

    async fn metadata(&self, stream: &str) -> Result<StreamMetadata, BrokerError> {
        let state = self.state.read().await;
        state
            .state
            .streams
            .get(stream)
            .map(|stream_state| stream_state.metadata(stream))
            .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))
    }

    async fn metadata_by_group_id(&self, group_id: &str) -> Option<(String, StreamMetadata)> {
        let state = self.state.read().await;
        state
            .state
            .streams
            .iter()
            .find(|(_, stream_state)| stream_state.group_id == group_id)
            .map(|(stream, stream_state)| (stream.clone(), stream_state.metadata(stream)))
    }

    async fn health(&self) -> runnel_engine::HealthSnapshot {
        let state = self.state.read().await;
        let streams = state.state.streams.len();
        let storage_bytes = state
            .state
            .streams
            .values()
            .flat_map(|stream| stream.messages.iter())
            .map(|message| {
                message.payload.len() as u64
                    + message.key.as_ref().map_or(0, |key| key.len() as u64)
            })
            .sum();
        runnel_engine::HealthSnapshot {
            streams,
            storage_bytes,
        }
    }

    fn snapshot_metrics(&self) -> SnapshotMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn record_snapshot_chunk(&self, bytes: u64, final_chunk: bool) {
        self.metrics.transfer_chunks.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .transfer_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        if final_chunk {
            self.metrics
                .transfer_final_chunks
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        self.metrics.builds_started.fetch_add(1, Ordering::Relaxed);
        let result = async {
            let (snapshot_state, last_applied_log, last_membership) = {
                let state = self.state.read().await;
                (
                    persisted_snapshot_state(&state.state),
                    state.last_applied_log,
                    state.last_membership.clone(),
                )
            };
            let data = serde_json::to_vec(&snapshot_state)
                .map_err(|error| StorageIOError::read_state_machine(&error))?;
            let meta = SnapshotMeta {
                last_log_id: last_applied_log,
                last_membership,
                snapshot_id: format!(
                    "snapshot-{}",
                    self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1
                ),
            };
            let stored_snapshot = StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            };
            self.persist_snapshot(&stored_snapshot).await?;
            *self.current_snapshot.write().await = Some(stored_snapshot);
            Ok(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            })
        }
        .await;
        if result.is_ok() {
            self.metrics
                .builds_completed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.build_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state = self.state.read().await;
        Ok((state.last_applied_log, state.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut state = self.state.write().await;
        let mut responses = Vec::new();
        for entry in entries {
            state.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => responses.push(CommandResponse::Noop),
                EntryPayload::Membership(membership) => {
                    state.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(CommandResponse::Noop);
                }
                EntryPayload::Normal(command) => {
                    responses.push(apply_command(&mut state.state, command, &self.kind))
                }
            }
        }
        if !responses.is_empty() {
            self.persist_state(&state)?;
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        self.metrics
            .installs_started
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .installs_in_progress
            .fetch_add(1, Ordering::Relaxed);
        let result = async {
            let data = snapshot.into_inner();
            let data_len = data.len() as u64;
            let persisted_snapshot =
                validate_snapshot_data(&data).map_err(|error| StorageError::IO {
                    source: StorageIOError::read_snapshot(Some(meta.signature()), &error),
                })?;
            let snapshot_state = snapshot_state_from_persisted(persisted_snapshot);
            let mut state = self.state.write().await;
            state.last_applied_log = meta.last_log_id;
            state.last_membership = meta.last_membership.clone();
            state.state = snapshot_state;
            self.persist_state(&state)?;
            drop(state);
            let stored_snapshot = StoredSnapshot {
                meta: meta.clone(),
                data,
            };
            *self.current_snapshot.write().await = Some(stored_snapshot.clone());
            self.persist_snapshot(&stored_snapshot).await?;
            Ok(data_len)
        }
        .await;
        self.metrics
            .installs_in_progress
            .fetch_sub(1, Ordering::Relaxed);
        if let Ok(data_len) = result {
            self.metrics
                .installs_completed
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .install_bytes
                .fetch_add(data_len, Ordering::Relaxed);
            Ok(())
        } else {
            self.metrics
                .install_failures
                .fetch_add(1, Ordering::Relaxed);
            result.map(|_| ())
        }
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let snapshot = self.current_snapshot.read().await.clone();
        Ok(snapshot.map(|snapshot| Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(Cursor::new(snapshot.data)),
        }))
    }
}

fn persisted_snapshot_state(state: &SnapshotState) -> PersistedSnapshotState {
    PersistedSnapshotState {
        version: FORMAT_VERSION,
        streams: state
            .streams
            .iter()
            .map(|(stream, state)| (stream.clone(), PersistedStreamData::current(state)))
            .collect(),
        consumers: state
            .consumers
            .iter()
            .map(|((stream, consumer), offset)| PersistedConsumer {
                stream: stream.clone(),
                consumer: consumer.clone(),
                offset: *offset,
            })
            .collect(),
        dedup: state.dedup.clone(),
    }
}

fn validate_snapshot_data(data: &[u8]) -> Result<PersistedSnapshotState, std::io::Error> {
    let persisted: PersistedSnapshotState = serde_json::from_slice(data)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if !matches!(persisted.version, 1 | FORMAT_VERSION) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported snapshot format version {}", persisted.version),
        ));
    }
    Ok(persisted)
}

fn snapshot_state_from_persisted(persisted: PersistedSnapshotState) -> SnapshotState {
    SnapshotState {
        streams: persisted
            .streams
            .into_iter()
            .map(|(stream, persisted)| {
                let state = persisted.into_state(&stream);
                (stream, state)
            })
            .collect(),
        consumers: persisted
            .consumers
            .into_iter()
            .map(|consumer| ((consumer.stream, consumer.consumer), consumer.offset))
            .collect(),
        dedup: persisted.dedup,
    }
}

fn apply_command(state: &mut SnapshotState, command: Command, kind: &GroupKind) -> CommandResponse {
    match command {
        Command::CreateStream {
            stream,
            stream_id,
            group_id,
        } => {
            if matches!(kind, GroupKind::Data { .. }) {
                return CommandResponse::Noop;
            }
            let (derived_stream_id, derived_group_id) = stream_identity(&stream);
            let lifecycle = if matches!(kind, GroupKind::Metadata) {
                StreamLifecycle::Creating
            } else {
                StreamLifecycle::Active
            };
            let created = if let std::collections::btree_map::Entry::Vacant(entry) =
                state.streams.entry(stream)
            {
                entry.insert(StreamState {
                    stream_id: stream_id.unwrap_or(derived_stream_id),
                    group_id: group_id.unwrap_or(derived_group_id),
                    lifecycle,
                    messages: Vec::new(),
                });
                true
            } else {
                false
            };
            CommandResponse::StreamCreated { created }
        }
        Command::InitializeDataStream {
            stream,
            stream_id,
            group_id,
        } => {
            let GroupKind::Data {
                stream: expected_stream,
                stream_id: expected_stream_id,
                group_id: expected_group_id,
            } = kind
            else {
                return CommandResponse::Noop;
            };
            if &stream != expected_stream
                || &stream_id != expected_stream_id
                || &group_id != expected_group_id
            {
                return CommandResponse::Noop;
            }
            let initialized = if let std::collections::btree_map::Entry::Vacant(entry) =
                state.streams.entry(stream)
            {
                entry.insert(StreamState::active(stream_id, group_id));
                true
            } else {
                false
            };
            CommandResponse::DataStreamInitialized { initialized }
        }
        Command::ActivateStream { stream } => {
            if matches!(kind, GroupKind::Data { .. }) {
                return CommandResponse::Noop;
            }
            let Some(stream_state) = state.streams.get_mut(&stream) else {
                return CommandResponse::StreamActivated { activated: false };
            };
            let activated = stream_state.lifecycle != StreamLifecycle::Active;
            stream_state.lifecycle = StreamLifecycle::Active;
            CommandResponse::StreamActivated { activated }
        }
        Command::Publish {
            stream,
            key,
            payload,
            published_at_ms,
            request_id,
        } => {
            if matches!(kind, GroupKind::Metadata) {
                return CommandResponse::StreamNotFound;
            }
            if let Some(request_id) = request_id.as_ref()
                && let Some(offset) = state
                    .dedup
                    .get(&stream)
                    .and_then(|requests| requests.get(request_id))
            {
                return CommandResponse::Published { offset: *offset };
            }
            let (stream_id, group_id) = stream_identity(&stream);
            let stream_state = state
                .streams
                .entry(stream.clone())
                .or_insert_with(|| StreamState::active(stream_id, group_id));
            if !stream_state.is_active() {
                return CommandResponse::StreamNotFound;
            }
            let offset = stream_state.messages.len() as Offset;
            stream_state.messages.push(StoredMessage {
                key,
                payload,
                published_at_ms,
            });
            if let Some(request_id) = request_id {
                state
                    .dedup
                    .entry(stream)
                    .or_default()
                    .insert(request_id, offset);
            }
            CommandResponse::Published { offset }
        }
        Command::Ack {
            stream,
            consumer,
            offset,
        } => {
            if matches!(kind, GroupKind::Metadata) {
                return CommandResponse::StreamNotFound;
            }
            let Some(stream_state) = state.streams.get(&stream) else {
                return CommandResponse::StreamNotFound;
            };
            if !stream_state.is_active() {
                return CommandResponse::StreamNotFound;
            }
            let key = (stream, consumer);
            let expected = state.consumers.get(&key).copied().unwrap_or_default();
            if offset < expected {
                return CommandResponse::AlreadyAcknowledged;
            }
            if offset > expected {
                return CommandResponse::OutOfOrderAck {
                    expected,
                    received: offset,
                };
            }
            state.consumers.insert(key, offset + 1);
            CommandResponse::Acknowledged
        }
    }
}

fn stream_identity(stream: &str) -> (String, String) {
    (format!("stream/{stream}"), format!("group/{stream}/data"))
}

fn legacy_format_version() -> u32 {
    1
}

#[derive(Clone)]
struct UnreachableNetwork;

impl RaftNetworkFactory<TypeConfig> for UnreachableNetwork {
    type Network = UnreachableConnection;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        UnreachableConnection
    }
}

struct UnreachableConnection;

fn unreachable<T>() -> Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
    Err(RPCError::Unreachable(Unreachable::new(
        &std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "in-process network is not configured",
        ),
    )))
}

fn unreachable_snapshot<T>()
-> Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>>
{
    Err(RPCError::Unreachable(Unreachable::new(
        &std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "in-process network is not configured",
        ),
    )))
}

impl RaftNetwork<TypeConfig> for UnreachableConnection {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        unreachable()
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        unreachable_snapshot()
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        unreachable()
    }
}

#[derive(Clone)]
struct InMemoryNetwork {
    peers: Arc<RwLock<BTreeMap<NodeId, Raft>>>,
}

impl RaftNetworkFactory<TypeConfig> for InMemoryNetwork {
    type Network = InMemoryConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        InMemoryConnection {
            target,
            peers: Arc::clone(&self.peers),
        }
    }
}

struct InMemoryConnection {
    target: NodeId,
    peers: Arc<RwLock<BTreeMap<NodeId, Raft>>>,
}

impl InMemoryConnection {
    async fn target(&self) -> Result<Raft, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.peers
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!("node {} is not registered", self.target),
                )))
            })
    }
}

impl RaftNetwork<TypeConfig> for InMemoryConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let target = self.target().await?;
        target
            .append_entries(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let target = self
            .peers
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!("node {} is not registered", self.target),
                )))
            })?;
        target
            .install_snapshot(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let target = self.target().await?;
        target
            .vote(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }
}

pub struct InMemoryCluster {
    nodes: BTreeMap<NodeId, Arc<RaftGroup>>,
}

impl InMemoryCluster {
    pub async fn new<I>(node_ids: I) -> Result<Self, BrokerError>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let ids = node_ids.into_iter().collect::<Vec<_>>();
        if ids.is_empty() {
            return Err(BrokerError::Cluster(
                "cluster must contain at least one node".to_owned(),
            ));
        }

        let peers = Arc::new(RwLock::new(BTreeMap::new()));
        let mut nodes = BTreeMap::new();
        for node_id in ids.iter().copied() {
            let network = InMemoryNetwork {
                peers: Arc::clone(&peers),
            };
            let (raft, state_machine) =
                build_raft(node_id, "runnel-in-memory-cluster".to_owned(), network).await?;
            peers.write().await.insert(node_id, raft.clone());
            nodes.insert(
                node_id,
                Arc::new(RaftGroup {
                    node_id,
                    raft,
                    state_machine,
                }),
            );
        }

        let members = nodes
            .keys()
            .copied()
            .map(|node_id| (node_id, BasicNode::new(format!("in-memory-{node_id}"))))
            .collect::<BTreeMap<_, _>>();
        let first = nodes.values().next().expect("non-empty cluster").clone();
        first
            .raft
            .initialize(members)
            .await
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        first
            .raft
            .wait(Some(Duration::from_secs(2)))
            .current_leader(
                *nodes.keys().next().expect("non-empty cluster"),
                "cluster election",
            )
            .await
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;

        Ok(Self { nodes })
    }

    pub fn node(&self, node_id: NodeId) -> Option<Arc<RaftGroup>> {
        self.nodes.get(&node_id).cloned()
    }

    pub async fn leader(&self) -> Result<Arc<RaftGroup>, BrokerError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            for node in self.nodes.values() {
                if let Some(leader_id) = node.raft.current_leader().await
                    && self.nodes.contains_key(&leader_id)
                {
                    return Ok(self
                        .nodes
                        .get(&leader_id)
                        .expect("leader is registered")
                        .clone());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BrokerError::Cluster("cluster has no leader".to_owned()));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub struct RaftGroup {
    node_id: NodeId,
    raft: Raft,
    state_machine: Arc<StateMachineStore>,
}

fn map_client_write_error(
    error: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
) -> BrokerError {
    if let Some(forward) = error.forward_to_leader::<BasicNode>() {
        return BrokerError::NotLeader {
            leader_id: forward.leader_id,
        };
    }
    BrokerError::Cluster(error.to_string())
}

async fn build_raft<N>(
    node_id: NodeId,
    cluster_name: String,
    network: N,
) -> Result<(Raft, Arc<StateMachineStore>), BrokerError>
where
    N: RaftNetworkFactory<TypeConfig>,
{
    build_raft_with_storage(
        node_id,
        cluster_name,
        network,
        log_store::LogStore::default(),
        Arc::new(StateMachineStore::default()),
    )
    .await
}

async fn build_raft_with_storage<N>(
    node_id: NodeId,
    cluster_name: String,
    network: N,
    log_store: log_store::LogStore<TypeConfig>,
    state_machine: Arc<StateMachineStore>,
) -> Result<(Raft, Arc<StateMachineStore>), BrokerError>
where
    N: RaftNetworkFactory<TypeConfig>,
{
    let config = Config {
        cluster_name,
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        replication_lag_threshold: REPLICATION_LAG_THRESHOLD,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(SNAPSHOT_LOG_THRESHOLD),
        max_in_snapshot_log_to_keep: SNAPSHOT_LOGS_TO_KEEP,
        snapshot_max_chunk_size: SNAPSHOT_CHUNK_SIZE,
        ..Default::default()
    }
    .validate()
    .map_err(|error| BrokerError::Cluster(error.to_string()))?;
    let raft = Raft::new(
        node_id,
        Arc::new(config),
        network,
        log_store,
        Arc::clone(&state_machine),
    )
    .await
    .map_err(|error| BrokerError::Cluster(error.to_string()))?;
    Ok((raft, state_machine))
}

impl RaftGroup {
    pub async fn new_single(node_id: NodeId) -> Result<Self, BrokerError> {
        let (raft, state_machine) = build_raft(
            node_id,
            format!("runnel-single-{node_id}"),
            UnreachableNetwork,
        )
        .await?;
        raft.initialize(BTreeMap::from([(node_id, BasicNode::new("in-process"))]))
            .await
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(node_id, "single-node election")
            .await
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        Ok(Self {
            node_id,
            raft,
            state_machine,
        })
    }

    pub async fn create_stream(&self, stream: String) -> Result<bool, BrokerError> {
        let (stream_id, group_id) = stream_identity(&stream);
        let response = self
            .raft
            .client_write(Command::CreateStream {
                stream,
                stream_id: Some(stream_id),
                group_id: Some(group_id),
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::StreamCreated { created } => Ok(created),
            other => Err(BrokerError::Cluster(format!(
                "unexpected stream response: {other:?}"
            ))),
        }
    }

    async fn initialize_data_stream(
        &self,
        stream: String,
        stream_id: String,
        group_id: String,
    ) -> Result<bool, BrokerError> {
        let response = self
            .raft
            .client_write(Command::InitializeDataStream {
                stream,
                stream_id,
                group_id,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::DataStreamInitialized { initialized } => Ok(initialized),
            other => Err(BrokerError::Cluster(format!(
                "unexpected data-stream response: {other:?}"
            ))),
        }
    }

    async fn activate_stream(&self, stream: String) -> Result<bool, BrokerError> {
        let response = self
            .raft
            .client_write(Command::ActivateStream { stream })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::StreamActivated { activated } => Ok(activated),
            other => Err(BrokerError::Cluster(format!(
                "unexpected stream-activation response: {other:?}"
            ))),
        }
    }

    pub async fn publish(
        &self,
        stream: String,
        key: Option<String>,
        payload: Vec<u8>,
        published_at_ms: u64,
        request_id: Option<String>,
    ) -> Result<Offset, BrokerError> {
        let stream_name = stream.clone();
        let response = self
            .raft
            .client_write(Command::Publish {
                stream,
                key,
                payload,
                published_at_ms,
                request_id,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::Published { offset } => Ok(offset),
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected publish response: {other:?}"
            ))),
        }
    }

    pub async fn poll(&self, stream: &str, consumer: &str) -> Result<PollResult, BrokerError> {
        self.state_machine.poll(stream, consumer).await
    }

    pub async fn stream_metadata(&self, stream: &str) -> Result<StreamMetadata, BrokerError> {
        self.state_machine.metadata(stream).await
    }

    pub async fn ack(
        &self,
        stream: String,
        consumer: String,
        offset: Offset,
    ) -> Result<AckResult, BrokerError> {
        let stream_name = stream.clone();
        let consumer_name = consumer.clone();
        let response = self
            .raft
            .client_write(Command::Ack {
                stream,
                consumer,
                offset,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::Acknowledged => Ok(AckResult::Acknowledged),
            CommandResponse::AlreadyAcknowledged => Ok(AckResult::AlreadyAcknowledged),
            CommandResponse::OutOfOrderAck { expected, received } => {
                Err(BrokerError::OutOfOrderAck {
                    consumer: consumer_name,
                    expected,
                    received,
                })
            }
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected ack response: {other:?}"
            ))),
        }
    }

    pub async fn health(&self) -> runnel_engine::HealthSnapshot {
        self.state_machine.health().await
    }

    pub fn raft(&self) -> Raft {
        self.raft.clone()
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) fn record_snapshot_chunk(&self, bytes: u64, final_chunk: bool) {
        self.state_machine.record_snapshot_chunk(bytes, final_chunk);
    }

    pub async fn trigger_snapshot(&self) -> Result<(), BrokerError> {
        self.raft.trigger().snapshot().await.map_err(fatal_error)
    }

    async fn is_initialized(&self) -> Result<bool, BrokerError> {
        self.raft.is_initialized().await.map_err(fatal_error)
    }
}

pub struct SingleNodeEngine {
    group: Arc<RaftGroup>,
}

impl SingleNodeEngine {
    pub async fn new(node_id: NodeId) -> Result<Self, BrokerError> {
        Ok(Self {
            group: Arc::new(RaftGroup::new_single(node_id).await?),
        })
    }
}

impl Engine for SingleNodeEngine {
    fn create_stream<'a>(&'a self, stream: &'a str) -> EngineFuture<'a, bool> {
        Box::pin(async move { self.group.create_stream(stream.to_owned()).await })
    }

    fn publish<'a>(
        &'a self,
        stream: &'a str,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
    ) -> EngineFuture<'a, Offset> {
        Box::pin(async move {
            self.group
                .publish(stream.to_owned(), key, payload, now_ms(), request_id)
                .await
        })
    }

    fn poll<'a>(&'a self, stream: &'a str, consumer: &'a str) -> EngineFuture<'a, PollResult> {
        Box::pin(async move { self.group.poll(stream, consumer).await })
    }

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move {
            self.group
                .ack(stream.to_owned(), consumer.to_owned(), offset)
                .await
        })
    }

    fn health<'a>(&'a self) -> EngineFuture<'a, runnel_engine::HealthSnapshot> {
        Box::pin(async move { Ok(self.group.health().await) })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataGroupManifest {
    stream: String,
    stream_id: String,
    group_id: String,
}

pub struct GroupManager {
    node_id: NodeId,
    cluster_name: String,
    data_dir: PathBuf,
    peers: BTreeMap<NodeId, String>,
    groups: RwLock<BTreeMap<String, Arc<RaftGroup>>>,
    creation_lock: Mutex<()>,
}

impl GroupManager {
    async fn open(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
    ) -> Result<Arc<Self>, BrokerError> {
        let manager = Arc::new(Self {
            node_id,
            cluster_name,
            data_dir: data_dir.as_ref().to_path_buf(),
            peers,
            groups: RwLock::new(BTreeMap::new()),
            creation_lock: Mutex::new(()),
        });
        let metadata = manager
            .open_group(
                METADATA_GROUP_ID,
                GroupKind::Metadata,
                manager.group_directory(METADATA_GROUP_ID),
            )
            .await?;
        manager
            .groups
            .write()
            .await
            .insert(METADATA_GROUP_ID.to_owned(), metadata);
        manager.restore_data_groups().await?;
        Ok(manager)
    }

    async fn open_group(
        &self,
        group_id: &str,
        kind: GroupKind,
        directory: PathBuf,
    ) -> Result<Arc<RaftGroup>, BrokerError> {
        fs::create_dir_all(&directory)?;
        let network = network::TcpNetwork::new(self.peers.clone(), group_id);
        let log_store = log_store::LogStore::open(directory.join("raft-log.json"))
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        let state_machine = Arc::new(StateMachineStore::open(
            directory.join("state-machine"),
            kind,
        )?);
        let cluster_name = format!("{}/{}", self.cluster_name, group_id);
        let (raft, state_machine) = build_raft_with_storage(
            self.node_id,
            cluster_name,
            network,
            log_store,
            state_machine,
        )
        .await?;
        Ok(Arc::new(RaftGroup {
            node_id: self.node_id,
            raft,
            state_machine,
        }))
    }

    async fn restore_data_groups(&self) -> Result<(), BrokerError> {
        let directory = self.data_dir.join("groups").join("data");
        if !directory.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let manifest_path = entry.path().join("group.json");
            if !manifest_path.exists() {
                continue;
            }
            let manifest: DataGroupManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
            self.ensure_data_group_local(
                &manifest.stream,
                &StreamMetadata {
                    stream_id: manifest.stream_id,
                    group_id: manifest.group_id,
                    lifecycle: StreamLifecycle::Active,
                },
            )
            .await?;
        }
        Ok(())
    }

    fn group_directory(&self, group_id: &str) -> PathBuf {
        self.data_dir.join("groups").join(group_id)
    }

    fn data_group_directory(&self, stream: &str) -> PathBuf {
        self.data_dir
            .join("groups")
            .join("data")
            .join(path_component(stream))
    }

    pub(crate) async fn group(&self, group_id: &str) -> Option<Arc<RaftGroup>> {
        self.groups.read().await.get(group_id).cloned()
    }

    pub(crate) async fn ensure_group_for_id(
        &self,
        group_id: &str,
    ) -> Result<Option<Arc<RaftGroup>>, BrokerError> {
        if let Some(group) = self.group(group_id).await {
            return Ok(Some(group));
        }
        if group_id == METADATA_GROUP_ID {
            return Ok(None);
        }
        let metadata_group = self.metadata_group().await;
        let Some((stream, metadata)) = metadata_group
            .state_machine
            .metadata_by_group_id(group_id)
            .await
        else {
            return Ok(None);
        };
        self.ensure_data_group_local(&stream, &metadata)
            .await
            .map(Some)
    }

    async fn metadata_group(&self) -> Arc<RaftGroup> {
        self.groups
            .read()
            .await
            .get(METADATA_GROUP_ID)
            .expect("metadata group must be opened before serving requests")
            .clone()
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) async fn ensure_data_group_local(
        &self,
        stream: &str,
        metadata: &StreamMetadata,
    ) -> Result<Arc<RaftGroup>, BrokerError> {
        if let Some(group) = self.group(&metadata.group_id).await {
            return Ok(group);
        }
        let _guard = self.creation_lock.lock().await;
        if let Some(group) = self.group(&metadata.group_id).await {
            return Ok(group);
        }
        let directory = self.data_group_directory(stream);
        fs::create_dir_all(&directory)?;
        let manifest = DataGroupManifest {
            stream: stream.to_owned(),
            stream_id: metadata.stream_id.clone(),
            group_id: metadata.group_id.clone(),
        };
        let bytes = serde_json::to_vec(&manifest)?;
        atomic_write(&directory.join("group.json"), &bytes)?;
        let group = self
            .open_group(
                &metadata.group_id,
                GroupKind::Data {
                    stream: stream.to_owned(),
                    stream_id: metadata.stream_id.clone(),
                    group_id: metadata.group_id.clone(),
                },
                directory,
            )
            .await?;
        self.groups
            .write()
            .await
            .insert(metadata.group_id.clone(), group.clone());
        Ok(group)
    }

    pub(crate) async fn data_group_for_stream(
        &self,
        stream: &str,
    ) -> Result<Arc<RaftGroup>, BrokerError> {
        let metadata_group = self.metadata_group().await;
        let metadata = metadata_group.stream_metadata(stream).await?;
        if metadata.lifecycle != StreamLifecycle::Active {
            return Err(BrokerError::StreamNotReady(stream.to_owned()));
        }
        self.ensure_data_group_local(stream, &metadata).await
    }

    pub(crate) async fn create_stream_local(&self, stream: String) -> Result<bool, BrokerError> {
        let metadata_group = self.metadata_group().await;
        let created = metadata_group.create_stream(stream.clone()).await?;
        let metadata = metadata_group.stream_metadata(&stream).await?;
        self.ensure_data_group_local(&stream, &metadata).await?;
        if metadata.lifecycle == StreamLifecycle::Creating {
            self.reconcile_stream(&stream, &metadata).await?;
        }
        Ok(created)
    }

    async fn reconcile_stream(
        &self,
        stream: &str,
        metadata: &StreamMetadata,
    ) -> Result<(), BrokerError> {
        let metadata_group = self.metadata_group().await;
        let leader_id = metadata_group
            .raft()
            .current_leader()
            .await
            .ok_or_else(|| BrokerError::Cluster("metadata group has no leader".to_owned()))?;
        if leader_id != self.node_id {
            return Err(BrokerError::NotLeader {
                leader_id: Some(leader_id),
            });
        }

        let data_group = self.ensure_data_group_local(stream, metadata).await?;
        for (node_id, address) in &self.peers {
            if *node_id == self.node_id {
                continue;
            }
            network::ensure_data_group(
                address,
                stream.to_owned(),
                metadata.stream_id.clone(),
                metadata.group_id.clone(),
                Duration::from_secs(2),
            )
            .await
            .map_err(|error| {
                BrokerError::Cluster(format!(
                    "could not prepare data group on node {node_id}: {error}"
                ))
            })?;
        }

        if !data_group.is_initialized().await? {
            let members = self
                .peers
                .iter()
                .map(|(id, address)| (*id, BasicNode::new(address)))
                .collect::<BTreeMap<_, _>>();
            data_group
                .raft()
                .initialize(members)
                .await
                .map_err(|error| BrokerError::Cluster(error.to_string()))?;
            data_group
                .raft()
                .wait(Some(Duration::from_secs(2)))
                .current_leader(self.node_id, "data-group election")
                .await
                .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        }

        if data_group.stream_metadata(stream).await.is_err() {
            self.initialize_data_stream(stream, metadata, data_group)
                .await?;
        }
        if metadata.lifecycle == StreamLifecycle::Creating {
            metadata_group.activate_stream(stream.to_owned()).await?;
        }
        Ok(())
    }

    async fn initialize_data_stream(
        &self,
        stream: &str,
        metadata: &StreamMetadata,
        data_group: Arc<RaftGroup>,
    ) -> Result<(), BrokerError> {
        match data_group
            .initialize_data_stream(
                stream.to_owned(),
                metadata.stream_id.clone(),
                metadata.group_id.clone(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(BrokerError::NotLeader { leader_id }) => {
                let target = leader_id
                    .or(data_group.raft().current_leader().await)
                    .ok_or_else(|| BrokerError::Cluster("data group has no leader".to_owned()))?;
                let address = self.peers.get(&target).ok_or_else(|| {
                    BrokerError::Cluster(format!("data leader node {target} has no address"))
                })?;
                let response = network::forward(
                    address,
                    network::ForwardedOperation::InitializeDataStream {
                        stream: stream.to_owned(),
                        stream_id: metadata.stream_id.clone(),
                        group_id: metadata.group_id.clone(),
                    },
                    Duration::from_secs(2),
                )
                .await
                .map_err(|error| BrokerError::Cluster(error.to_string()))?;
                match response {
                    network::ForwardedResponse::InitializeDataStream(result) => {
                        result.map(|_| ()).map_err(forward_error_to_broker)
                    }
                    _ => Err(BrokerError::Cluster(
                        "data leader returned the wrong initialization response".to_owned(),
                    )),
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn initialize_data_stream_local(
        &self,
        stream: String,
        stream_id: String,
        group_id: String,
    ) -> Result<bool, BrokerError> {
        let group = self
            .ensure_data_group_local(
                &stream,
                &StreamMetadata {
                    stream_id: stream_id.clone(),
                    group_id: group_id.clone(),
                    lifecycle: StreamLifecycle::Creating,
                },
            )
            .await?;
        group
            .initialize_data_stream(stream, stream_id, group_id)
            .await
    }

    pub(crate) async fn publish_local(
        &self,
        stream: String,
        key: Option<String>,
        payload: Vec<u8>,
        published_at_ms: u64,
        request_id: Option<String>,
    ) -> Result<Offset, BrokerError> {
        self.data_group_for_stream(&stream)
            .await?
            .publish(stream, key, payload, published_at_ms, request_id)
            .await
    }

    pub(crate) async fn poll_local(
        &self,
        stream: &str,
        consumer: &str,
    ) -> Result<PollResult, BrokerError> {
        self.data_group_for_stream(stream)
            .await?
            .poll(stream, consumer)
            .await
    }

    pub(crate) async fn ack_local(
        &self,
        stream: String,
        consumer: String,
        offset: Offset,
    ) -> Result<AckResult, BrokerError> {
        self.data_group_for_stream(&stream)
            .await?
            .ack(stream, consumer, offset)
            .await
    }

    async fn health(&self) -> runnel_engine::HealthSnapshot {
        let metadata_health = self.metadata_group().await.health().await;
        let groups = self
            .groups
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut storage_bytes = 0;
        for group in groups {
            storage_bytes += group.health().await.storage_bytes;
        }
        runnel_engine::HealthSnapshot {
            streams: metadata_health.streams,
            storage_bytes,
        }
    }

    pub async fn snapshot_metrics(&self) -> SnapshotMetricsSnapshot {
        let groups = self
            .groups
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        groups
            .iter()
            .map(|group| group.state_machine.snapshot_metrics())
            .fold(SnapshotMetricsSnapshot::default(), add_snapshot_metrics)
    }
}

fn add_snapshot_metrics(
    left: SnapshotMetricsSnapshot,
    right: SnapshotMetricsSnapshot,
) -> SnapshotMetricsSnapshot {
    SnapshotMetricsSnapshot {
        builds_started: left.builds_started + right.builds_started,
        builds_completed: left.builds_completed + right.builds_completed,
        build_failures: left.build_failures + right.build_failures,
        installs_started: left.installs_started + right.installs_started,
        installs_completed: left.installs_completed + right.installs_completed,
        install_failures: left.install_failures + right.install_failures,
        install_bytes: left.install_bytes + right.install_bytes,
        installs_in_progress: left.installs_in_progress + right.installs_in_progress,
        transfer_chunks: left.transfer_chunks + right.transfer_chunks,
        transfer_final_chunks: left.transfer_final_chunks + right.transfer_final_chunks,
        transfer_bytes: left.transfer_bytes + right.transfer_bytes,
    }
}

fn path_component(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub struct PersistentEngine {
    manager: Arc<GroupManager>,
    node_id: NodeId,
    peers: BTreeMap<NodeId, String>,
}

impl PersistentEngine {
    pub async fn open(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
        bootstrap: bool,
    ) -> Result<Self, BrokerError> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir)?;
        if data_dir.join("raft-log.json").exists() || data_dir.join("state-machine").exists() {
            return Err(BrokerError::Cluster(
                "legacy single-group storage detected; migrate the data directory before starting this clustered layout"
                    .to_owned(),
            ));
        }
        let manager = GroupManager::open(node_id, cluster_name, data_dir, peers.clone()).await?;
        let metadata_group = manager.metadata_group().await;

        if bootstrap && !metadata_group.is_initialized().await? {
            let members = peers
                .keys()
                .copied()
                .map(|id| {
                    let address = peers.get(&id).expect("peer map key must have an address");
                    (id, BasicNode::new(address))
                })
                .collect::<BTreeMap<_, _>>();
            metadata_group
                .raft()
                .initialize(members)
                .await
                .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        }

        Ok(Self {
            manager,
            node_id,
            peers,
        })
    }

    pub fn raft(&self) -> Raft {
        self.manager
            .groups
            .try_read()
            .expect("metadata group lock should not be held")
            .get(METADATA_GROUP_ID)
            .expect("metadata group must be opened")
            .raft()
    }

    pub fn group(&self) -> Arc<RaftGroup> {
        self.manager
            .groups
            .try_read()
            .expect("metadata group lock should not be held")
            .get(METADATA_GROUP_ID)
            .expect("metadata group must be opened")
            .clone()
    }

    pub fn manager(&self) -> Arc<GroupManager> {
        Arc::clone(&self.manager)
    }

    async fn operation_leader(
        &self,
        operation: &network::ForwardedOperation,
    ) -> Result<Option<NodeId>, BrokerError> {
        match operation {
            network::ForwardedOperation::CreateStream { .. } => Ok(self
                .manager
                .metadata_group()
                .await
                .raft()
                .current_leader()
                .await),
            network::ForwardedOperation::Publish { stream, .. }
            | network::ForwardedOperation::Poll { stream, .. }
            | network::ForwardedOperation::Ack { stream, .. }
            | network::ForwardedOperation::InitializeDataStream { stream, .. } => Ok(self
                .manager
                .data_group_for_stream(stream)
                .await?
                .raft()
                .current_leader()
                .await),
        }
    }

    async fn forward_operation(
        &self,
        operation: network::ForwardedOperation,
        mut leader_id: Option<NodeId>,
    ) -> Result<network::ForwardedResponse, BrokerError> {
        for _ in 0..3 {
            let target = leader_id
                .or(self.operation_leader(&operation).await?)
                .ok_or_else(|| BrokerError::Cluster("cluster has no elected leader".to_owned()))?;
            let address = self.peers.get(&target).ok_or_else(|| {
                BrokerError::Cluster(format!("leader node {target} has no configured address"))
            })?;
            let response = network::forward(address, operation.clone(), Duration::from_secs(2))
                .await
                .map_err(|error| {
                    BrokerError::Cluster(format!("leader forwarding failed: {error}"))
                })?;
            let next_leader = match &response {
                network::ForwardedResponse::CreateStream(Err(
                    network::ForwardError::NotLeader { leader_id },
                ))
                | network::ForwardedResponse::Publish(Err(network::ForwardError::NotLeader {
                    leader_id,
                }))
                | network::ForwardedResponse::Poll(Err(network::ForwardError::NotLeader {
                    leader_id,
                }))
                | network::ForwardedResponse::Ack(Err(network::ForwardError::NotLeader {
                    leader_id,
                })) => Some(*leader_id),
                _ => None,
            };
            if let Some(next_leader) = next_leader {
                leader_id = next_leader;
                continue;
            }
            return Ok(response);
        }
        Err(BrokerError::Cluster(
            "leader changed repeatedly during forwarding".to_owned(),
        ))
    }

    async fn forward_create_stream(
        &self,
        stream: String,
        leader_id: Option<NodeId>,
    ) -> Result<bool, BrokerError> {
        match self
            .forward_operation(
                network::ForwardedOperation::CreateStream { stream },
                leader_id,
            )
            .await?
        {
            network::ForwardedResponse::CreateStream(result) => {
                result.map_err(forward_error_to_broker)
            }
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong create-stream response".to_owned(),
            )),
        }
    }

    async fn forward_publish(
        &self,
        operation: network::ForwardedOperation,
        leader_id: Option<NodeId>,
    ) -> Result<Offset, BrokerError> {
        match self.forward_operation(operation, leader_id).await? {
            network::ForwardedResponse::Publish(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong publish response".to_owned(),
            )),
        }
    }

    async fn forward_poll(
        &self,
        stream: String,
        consumer: String,
        leader_id: Option<NodeId>,
    ) -> Result<PollResult, BrokerError> {
        match self
            .forward_operation(
                network::ForwardedOperation::Poll { stream, consumer },
                leader_id,
            )
            .await?
        {
            network::ForwardedResponse::Poll(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong poll response".to_owned(),
            )),
        }
    }

    async fn forward_ack(
        &self,
        operation: network::ForwardedOperation,
        leader_id: Option<NodeId>,
    ) -> Result<AckResult, BrokerError> {
        match self.forward_operation(operation, leader_id).await? {
            network::ForwardedResponse::Ack(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong acknowledgement response".to_owned(),
            )),
        }
    }
}

impl Engine for PersistentEngine {
    fn create_stream<'a>(&'a self, stream: &'a str) -> EngineFuture<'a, bool> {
        Box::pin(async move {
            let stream_name = stream.to_owned();
            match self.manager.create_stream_local(stream_name.clone()).await {
                Ok(created) => Ok(created),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forward_create_stream(stream_name, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn publish<'a>(
        &'a self,
        stream: &'a str,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
    ) -> EngineFuture<'a, Offset> {
        Box::pin(async move {
            let stream_name = stream.to_owned();
            let published_at_ms = now_ms();
            let operation = network::ForwardedOperation::Publish {
                stream: stream_name.clone(),
                key: key.clone(),
                payload: payload.clone(),
                request_id: request_id.clone(),
                published_at_ms,
            };
            match self
                .manager
                .publish_local(stream_name, key, payload, published_at_ms, request_id)
                .await
            {
                Ok(offset) => Ok(offset),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forward_publish(operation, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn poll<'a>(&'a self, stream: &'a str, consumer: &'a str) -> EngineFuture<'a, PollResult> {
        Box::pin(async move {
            let data_group = self.manager.data_group_for_stream(stream).await?;
            let leader_id =
                data_group.raft().current_leader().await.ok_or_else(|| {
                    BrokerError::Cluster("cluster has no elected leader".to_owned())
                })?;
            if leader_id != self.node_id {
                return self
                    .forward_poll(stream.to_owned(), consumer.to_owned(), Some(leader_id))
                    .await;
            }
            self.manager.poll_local(stream, consumer).await
        })
    }

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move {
            let operation = network::ForwardedOperation::Ack {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                offset,
            };
            match self
                .manager
                .ack_local(stream.to_owned(), consumer.to_owned(), offset)
                .await
            {
                Ok(result) => Ok(result),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forward_ack(operation, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn health<'a>(&'a self) -> EngineFuture<'a, runnel_engine::HealthSnapshot> {
        Box::pin(async move {
            if !self.manager.metadata_group().await.is_initialized().await? {
                return Err(BrokerError::Cluster(
                    "cluster is not initialized".to_owned(),
                ));
            }
            if self
                .manager
                .metadata_group()
                .await
                .raft()
                .current_leader()
                .await
                .is_none()
            {
                return Err(BrokerError::Cluster(
                    "cluster has no elected leader".to_owned(),
                ));
            }
            Ok(self.manager.health().await)
        })
    }
}

fn forward_error_to_broker(error: network::ForwardError) -> BrokerError {
    match error {
        network::ForwardError::NotLeader { leader_id } => BrokerError::NotLeader { leader_id },
        network::ForwardError::Message(message) => BrokerError::Cluster(message),
    }
}

fn fatal_error(error: openraft::error::Fatal<NodeId>) -> BrokerError {
    BrokerError::Cluster(error.to_string())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = fs::File::create(&temp)?;
    use std::io::Write;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    let directory = fs::File::open(parent)?;
    directory.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_node_raft_commits_and_applies_messages() {
        let engine = SingleNodeEngine::new(1).await.unwrap();
        assert!(engine.create_stream("events").await.unwrap());
        assert_eq!(
            engine
                .publish("events", None, b"hello".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, payload, .. }) if payload == b"hello"
        ));
        assert_eq!(
            engine.ack("events", "worker", 0).await.unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Empty
        );
    }

    #[tokio::test]
    async fn three_node_cluster_replicates_a_committed_message() {
        let cluster = InMemoryCluster::new([1, 2, 3]).await.unwrap();
        let leader = cluster.leader().await.unwrap();
        assert!(leader.create_stream("events".to_owned()).await.unwrap());
        assert_eq!(
            leader
                .publish("events".to_owned(), None, b"hello".to_vec(), now_ms(), None)
                .await
                .unwrap(),
            0
        );

        for node_id in [1, 2, 3] {
            let node = cluster.node(node_id).unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                if matches!(
                    node.poll("events", "worker").await.unwrap(),
                    PollResult::Message(Message { offset: 0, .. })
                ) {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("node {node_id} did not apply the committed message");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    #[tokio::test]
    async fn persistent_engine_recovers_committed_state_after_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        assert!(engine.create_stream("events").await.unwrap());
        assert!(!engine.create_stream("events").await.unwrap());
        let metadata = engine.group().stream_metadata("events").await.unwrap();
        assert_eq!(metadata.stream_id, "stream/events");
        assert_eq!(metadata.group_id, "group/events/data");
        assert_eq!(metadata.lifecycle, StreamLifecycle::Active);
        assert_eq!(
            engine
                .publish("events", None, b"durable".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        drop(engine);

        let reopened = PersistentEngine::open(
            1,
            "runnel-persistent-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        let reopened_metadata = reopened.group().stream_metadata("events").await.unwrap();
        assert_eq!(reopened_metadata, metadata);
        assert!(matches!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, payload, .. }) if payload == b"durable"
        ));
    }

    #[tokio::test]
    async fn snapshots_bound_consensus_history_and_recover_state() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-snapshot-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        assert!(engine.create_stream("events").await.unwrap());
        let data_group = engine
            .manager
            .data_group_for_stream("events")
            .await
            .unwrap();
        for index in 0..40 {
            engine
                .publish(
                    "events",
                    None,
                    format!("message-{index}").into_bytes(),
                    None,
                )
                .await
                .unwrap();
        }
        data_group.trigger_snapshot().await.unwrap();
        let snapshot_metrics = data_group.state_machine.snapshot_metrics();
        assert!(snapshot_metrics.builds_started >= 1);
        assert!(snapshot_metrics.builds_completed >= 1);

        let snapshot_path = directory
            .path()
            .join("groups/data")
            .join(path_component("events"))
            .join("state-machine/snapshot.json");
        assert!(snapshot_path.exists());
        drop(engine);

        let reopened = PersistentEngine::open(
            1,
            "runnel-snapshot-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, payload, .. })
                if payload == b"message-0"
        ));
    }

    #[test]
    fn invalid_persisted_snapshot_is_rejected_before_startup() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "invalid".to_owned(),
            },
            data: b"not-a-snapshot".to_vec(),
        };
        fs::write(
            state_directory.join("snapshot.json"),
            serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("invalid persisted snapshot"));
    }

    #[tokio::test]
    async fn rejected_snapshot_install_preserves_existing_state() {
        let store = Arc::new(StateMachineStore::default());
        {
            let mut state = store.state.write().await;
            state.state.streams.insert(
                "events".to_owned(),
                StreamState {
                    stream_id: "stream/events".to_owned(),
                    group_id: "group/events/data".to_owned(),
                    lifecycle: StreamLifecycle::Active,
                    messages: vec![StoredMessage {
                        key: None,
                        payload: b"durable".to_vec(),
                        published_at_ms: 1,
                    }],
                },
            );
        }
        let before = store.poll("events", "worker").await.unwrap();
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "truncated".to_owned(),
        };
        let mut state_machine = store.clone();
        let result = state_machine
            .install_snapshot(&meta, Box::new(Cursor::new(b"truncated-snapshot".to_vec())))
            .await;
        assert!(result.is_err());
        assert_eq!(store.poll("events", "worker").await.unwrap(), before);
        let metrics = store.snapshot_metrics();
        assert_eq!(metrics.installs_started, 1);
        assert_eq!(metrics.install_failures, 1);
        assert_eq!(metrics.installs_in_progress, 0);
    }

    #[tokio::test]
    async fn legacy_stream_state_migrates_to_stable_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let legacy = serde_json::json!({
            "version": 1,
            "last_applied_log": null,
            "last_membership": serde_json::to_value(
                StoredMembership::<NodeId, BasicNode>::default()
            )
            .unwrap(),
            "streams": {"events": []},
            "consumers": []
        });
        fs::write(
            state_directory.join("state-machine.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();

        let store = StateMachineStore::open(&state_directory, GroupKind::Metadata).unwrap();
        assert_eq!(
            store.metadata("events").await.unwrap(),
            StreamMetadata {
                stream_id: "stream/events".to_owned(),
                group_id: "group/events/data".to_owned(),
                lifecycle: StreamLifecycle::Active,
            }
        );
    }

    #[test]
    fn stream_creation_has_explicit_metadata_and_data_states() {
        let mut metadata_state = SnapshotState::default();
        assert_eq!(
            apply_command(
                &mut metadata_state,
                Command::CreateStream {
                    stream: "events".to_owned(),
                    stream_id: Some("stream/events".to_owned()),
                    group_id: Some("group/events/data".to_owned()),
                },
                &GroupKind::Metadata,
            ),
            CommandResponse::StreamCreated { created: true }
        );
        assert_eq!(
            metadata_state.streams["events"].lifecycle,
            StreamLifecycle::Creating
        );
        assert_eq!(
            apply_command(
                &mut metadata_state,
                Command::ActivateStream {
                    stream: "events".to_owned(),
                },
                &GroupKind::Metadata,
            ),
            CommandResponse::StreamActivated { activated: true }
        );
        assert_eq!(
            metadata_state.streams["events"].lifecycle,
            StreamLifecycle::Active
        );

        let mut data_state = SnapshotState::default();
        assert_eq!(
            apply_command(
                &mut data_state,
                Command::InitializeDataStream {
                    stream: "events".to_owned(),
                    stream_id: "stream/events".to_owned(),
                    group_id: "group/events/data".to_owned(),
                },
                &GroupKind::Data {
                    stream: "events".to_owned(),
                    stream_id: "stream/events".to_owned(),
                    group_id: "group/events/data".to_owned(),
                },
            ),
            CommandResponse::DataStreamInitialized { initialized: true }
        );
        assert_eq!(
            data_state.streams["events"].lifecycle,
            StreamLifecycle::Active
        );
    }

    #[tokio::test]
    async fn persistent_streams_use_independent_data_groups() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-multi-stream-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();

        assert!(engine.create_stream("events").await.unwrap());
        assert!(engine.create_stream("jobs").await.unwrap());
        let events = engine.group().stream_metadata("events").await.unwrap();
        let jobs = engine.group().stream_metadata("jobs").await.unwrap();
        assert_ne!(events.group_id, jobs.group_id);
        assert!(engine.manager.group(&events.group_id).await.is_some());
        assert!(engine.manager.group(&jobs.group_id).await.is_some());

        assert_eq!(
            engine
                .publish("events", None, b"event".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            engine
                .publish("jobs", None, b"job".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { payload, .. }) if payload == b"event"
        ));
        assert!(matches!(
            engine.poll("jobs", "worker").await.unwrap(),
            PollResult::Message(Message { payload, .. }) if payload == b"job"
        ));
    }

    #[tokio::test]
    async fn stream_creation_resumes_after_restart_from_creating_state() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-lifecycle-recovery-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        assert!(
            engine
                .group()
                .create_stream("events".to_owned())
                .await
                .unwrap()
        );
        assert_eq!(
            engine
                .group()
                .stream_metadata("events")
                .await
                .unwrap()
                .lifecycle,
            StreamLifecycle::Creating
        );
        drop(engine);

        let reopened = PersistentEngine::open(
            1,
            "runnel-lifecycle-recovery-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        assert!(!reopened.create_stream("events").await.unwrap());
        assert_eq!(
            reopened
                .group()
                .stream_metadata("events")
                .await
                .unwrap()
                .lifecycle,
            StreamLifecycle::Active
        );
        assert_eq!(
            reopened
                .publish("events", None, b"recovered".to_vec(), None)
                .await
                .unwrap(),
            0
        );
    }
}
