#![allow(clippy::result_large_err)]

//! OpenRaft integration behind Runnel's topology-free engine contract.
//!
//! The persistent engine uses versioned local files and a framed TCP peer
//! protocol. It is an early clustered backend: membership is static, each
//! stream has a data group, and public requests are routed to the elected
//! leader through the internal peer protocol.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Cursor, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
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
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{
    AckResult, BrokerError, Engine, EngineFuture, Message, Offset, PollResult, ReplayMessage,
};
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
const DEFAULT_RAFT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const FORWARD_ATTEMPTS: usize = 3;
const FORWARD_TIMEOUT: Duration = Duration::from_secs(2);
const STORAGE_METADATA_FORMAT_VERSION: u32 = 1;
const STORAGE_METADATA_FILE: &str = "storage.json";
const LEGACY_SINGLE_GROUP_PATHS: &[&str] = &[
    "raft-log.json",
    "state-machine",
    "state-machine.json",
    "snapshot.json",
    "state-machine.log",
];
const STATE_MACHINE_JOURNAL_FORMAT_VERSION: u32 = 1;
const STATE_MACHINE_JOURNAL_FILE: &str = "state-machine.log";
const MAX_STATE_MACHINE_JOURNAL_RECORD_SIZE: u32 = 64 * 1024 * 1024;
const DEAD_LETTER_SUFFIX: &str = ".dead-letter";
const DEAD_LETTER_HASH_PREFIX: &str = "runnel.dead-letter.";

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
    Replay {
        stream: String,
        consumer: String,
        offset: Offset,
    },
    PollGroup {
        stream: String,
        consumer: String,
        member: String,
        now_ms: u64,
        lease_deadline_ms: u64,
        #[serde(default)]
        max_delivery_attempts: Option<u32>,
    },
    AckGroup {
        stream: String,
        consumer: String,
        member: String,
        offset: Offset,
        delivery_token: String,
        now_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommandResponse {
    StreamCreated {
        created: bool,
    },
    DataStreamInitialized {
        initialized: bool,
    },
    StreamActivated {
        activated: bool,
    },
    Published {
        offset: Offset,
    },
    Acknowledged,
    AlreadyAcknowledged,
    OutOfOrderAck {
        expected: Offset,
        received: Offset,
    },
    GroupPoll {
        result: PollResult,
    },
    Replay {
        result: ReplayMessage,
    },
    HistoryUnavailable {
        requested_offset: Offset,
        earliest_offset: Offset,
        next_offset: Offset,
    },
    GroupAcknowledged,
    GroupAlreadyAcknowledged,
    GroupAckNotInFlight {
        consumer: String,
        offset: Offset,
    },
    GroupStaleDelivery {
        consumer: String,
        offset: Offset,
    },
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct GroupDelivery {
    member: String,
    key: Option<String>,
    delivery_attempt: u32,
    delivery_token: String,
    deadline_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct GroupConsumerState {
    committed_offset: Offset,
    #[serde(default)]
    acknowledged_offsets: BTreeSet<Offset>,
    #[serde(default)]
    delivery_attempts: BTreeMap<Offset, u32>,
    #[serde(default)]
    in_flight: BTreeMap<Offset, GroupDelivery>,
}

struct GroupPollRequest {
    stream: String,
    consumer: String,
    member: String,
    now_ms: u64,
    lease_deadline_ms: u64,
    max_delivery_attempts: Option<u32>,
}

struct GroupAckRequest {
    stream: String,
    consumer: String,
    member: String,
    offset: Offset,
    delivery_token: String,
    now_ms: u64,
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
    #[serde(default)]
    group_consumers: BTreeMap<(String, String), GroupConsumerState>,
    // The lease evaluator is a replicated, persisted floor of command
    // observations. It prevents a backward wall-clock step from moving
    // expiry backwards after recovery or leader changes.
    #[serde(default)]
    lease_clock_ms: u64,
    dedup: BTreeMap<String, BTreeMap<String, Offset>>,
    #[serde(default)]
    redeliveries: u64,
    #[serde(default)]
    dead_letters: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    state: SnapshotState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedStorageMetadata {
    version: u32,
    cluster_name: String,
    node_id: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedState {
    version: u32,
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    streams: BTreeMap<String, PersistedStreamData>,
    consumers: Vec<PersistedConsumer>,
    #[serde(default)]
    group_consumers: Vec<PersistedGroupConsumer>,
    #[serde(default)]
    lease_clock_ms: u64,
    #[serde(default)]
    dedup: BTreeMap<String, BTreeMap<String, Offset>>,
    #[serde(default)]
    redeliveries: u64,
    #[serde(default)]
    dead_letters: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedSnapshotState {
    #[serde(default = "legacy_format_version")]
    version: u32,
    streams: BTreeMap<String, PersistedStreamData>,
    consumers: Vec<PersistedConsumer>,
    #[serde(default)]
    group_consumers: Vec<PersistedGroupConsumer>,
    #[serde(default)]
    lease_clock_ms: u64,
    #[serde(default)]
    dedup: BTreeMap<String, BTreeMap<String, Offset>>,
    #[serde(default)]
    redeliveries: u64,
    #[serde(default)]
    dead_letters: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum PersistedStreamData {
    Legacy(Vec<StoredMessage>),
    Current(PersistedStream),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Serialize)]
struct PersistedStreamRef<'a> {
    stream_id: &'a str,
    group_id: &'a str,
    lifecycle: Option<&'a StreamLifecycle>,
    messages: &'a [StoredMessage],
}

impl<'a> PersistedStreamRef<'a> {
    fn current(state: &'a StreamState) -> Self {
        Self {
            stream_id: &state.stream_id,
            group_id: &state.group_id,
            lifecycle: Some(&state.lifecycle),
            messages: &state.messages,
        }
    }
}

#[derive(Serialize)]
struct PersistedConsumerRef<'a> {
    stream: &'a str,
    consumer: &'a str,
    offset: Offset,
}

#[derive(Serialize)]
struct PersistedGroupConsumerRef<'a> {
    stream: &'a str,
    consumer: &'a str,
    state: &'a GroupConsumerState,
}

#[derive(Serialize)]
struct PersistedStateBodyRef<'a> {
    streams: BTreeMap<&'a str, PersistedStreamRef<'a>>,
    consumers: Vec<PersistedConsumerRef<'a>>,
    group_consumers: Vec<PersistedGroupConsumerRef<'a>>,
    lease_clock_ms: u64,
    dedup: &'a BTreeMap<String, BTreeMap<String, Offset>>,
    redeliveries: u64,
    dead_letters: u64,
}

impl<'a> PersistedStateBodyRef<'a> {
    fn new(state: &'a SnapshotState) -> Self {
        Self {
            streams: state
                .streams
                .iter()
                .map(|(stream, state)| (stream.as_str(), PersistedStreamRef::current(state)))
                .collect(),
            consumers: state
                .consumers
                .iter()
                .map(|((stream, consumer), offset)| PersistedConsumerRef {
                    stream,
                    consumer,
                    offset: *offset,
                })
                .collect(),
            group_consumers: state
                .group_consumers
                .iter()
                .map(|((stream, consumer), state)| PersistedGroupConsumerRef {
                    stream,
                    consumer,
                    state,
                })
                .collect(),
            lease_clock_ms: state.lease_clock_ms,
            dedup: &state.dedup,
            redeliveries: state.redeliveries,
            dead_letters: state.dead_letters,
        }
    }
}

#[derive(Serialize)]
struct PersistedSnapshotStateRef<'a> {
    version: u32,
    #[serde(flatten)]
    body: PersistedStateBodyRef<'a>,
}

impl<'a> PersistedSnapshotStateRef<'a> {
    fn new(state: &'a SnapshotState) -> Self {
        Self {
            version: FORMAT_VERSION,
            body: PersistedStateBodyRef::new(state),
        }
    }
}

#[derive(Serialize)]
struct PersistedStateRef<'a> {
    version: u32,
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: &'a StoredMembership<NodeId, BasicNode>,
    #[serde(flatten)]
    body: PersistedStateBodyRef<'a>,
}

impl<'a> PersistedStateRef<'a> {
    fn new(state: &'a StateMachineData) -> Self {
        Self {
            version: FORMAT_VERSION,
            last_applied_log: state.last_applied_log,
            last_membership: &state.last_membership,
            body: PersistedStateBodyRef::new(&state.state),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedConsumer {
    stream: String,
    consumer: String,
    offset: Offset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedGroupConsumer {
    stream: String,
    consumer: String,
    state: GroupConsumerState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateMachineJournalEntry {
    version: u32,
    log_id: LogId<NodeId>,
    payload: EntryPayload<TypeConfig>,
}

#[derive(Serialize)]
struct StateMachineJournalEntryRef<'a> {
    version: u32,
    log_id: LogId<NodeId>,
    payload: &'a EntryPayload<TypeConfig>,
}

impl<'a> StateMachineJournalEntryRef<'a> {
    fn from_entry(entry: &'a Entry<TypeConfig>) -> Self {
        Self {
            version: STATE_MACHINE_JOURNAL_FORMAT_VERSION,
            log_id: entry.log_id,
            payload: &entry.payload,
        }
    }
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
    journal: Option<StdMutex<fs::File>>,
    kind: GroupKind,
    metrics: Arc<SnapshotMetrics>,
}

fn read_persisted_state(path: &Path) -> Result<Option<PersistedState>, BrokerError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|error| {
        BrokerError::Cluster(format!(
            "could not read persisted state-machine '{}': {error}",
            path.display()
        ))
    })?;
    let persisted: PersistedState = serde_json::from_slice(&bytes).map_err(|error| {
        BrokerError::Cluster(format!(
            "invalid persisted state-machine '{}': {error}",
            path.display()
        ))
    })?;
    if !matches!(persisted.version, 1 | FORMAT_VERSION) {
        return Err(BrokerError::Cluster(format!(
            "unsupported state-machine format version {} in '{}' (checkpoint; supported versions: 1 and {})",
            persisted.version,
            path.display(),
            FORMAT_VERSION
        )));
    }
    Ok(Some(persisted))
}

fn state_machine_data_from_persisted(persisted: PersistedState) -> StateMachineData {
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
            group_consumers: persisted
                .group_consumers
                .into_iter()
                .map(|consumer| ((consumer.stream, consumer.consumer), consumer.state))
                .collect(),
            lease_clock_ms: persisted.lease_clock_ms,
            dedup: persisted.dedup,
            redeliveries: persisted.redeliveries,
            dead_letters: persisted.dead_letters,
        },
    }
}

fn read_persisted_snapshot(path: &Path) -> Result<Option<StoredSnapshot>, BrokerError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|error| {
        BrokerError::Cluster(format!(
            "could not read persisted snapshot '{}': {error}",
            path.display()
        ))
    })?;
    serde_json::from_slice(&bytes).map(Some).map_err(|error| {
        BrokerError::Cluster(format!(
            "invalid persisted snapshot '{}': {error}",
            path.display()
        ))
    })
}

impl StateMachineStore {
    fn open(path: impl AsRef<Path>, kind: GroupKind) -> Result<Self, BrokerError> {
        let path = path.as_ref().to_path_buf();
        let state_path = path.join("state-machine.json");
        let mut state = read_persisted_state(&state_path)?
            .map(state_machine_data_from_persisted)
            .unwrap_or_default();
        let snapshot_path = path.join("snapshot.json");
        let (current_snapshot, snapshot_state) =
            if let Some(snapshot) = read_persisted_snapshot(&snapshot_path)? {
                let persisted = validate_snapshot_data(&snapshot.data).map_err(|error| {
                    BrokerError::Cluster(format!(
                        "invalid persisted snapshot '{}': {error}",
                        snapshot_path.display()
                    ))
                })?;
                (
                    Some(snapshot),
                    Some(snapshot_state_from_persisted(persisted)),
                )
            } else {
                (None, None)
            };
        if let (Some(snapshot), Some(snapshot_state)) = (&current_snapshot, snapshot_state)
            && is_optional_log_after(snapshot.meta.last_log_id, state.last_applied_log)
        {
            state = StateMachineData {
                last_applied_log: snapshot.meta.last_log_id,
                last_membership: snapshot.meta.last_membership.clone(),
                state: snapshot_state,
            };
        }
        let journal_path = path.join(STATE_MACHINE_JOURNAL_FILE);
        let journal_entries = read_state_machine_journal(&journal_path)?;
        replay_state_machine_journal(&mut state, journal_entries, &kind)?;
        fs::create_dir_all(&path)?;
        let journal = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&journal_path)?;
        Ok(Self {
            state: RwLock::new(state),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(current_snapshot),
            path: Some(path),
            journal: Some(StdMutex::new(journal)),
            kind,
            metrics: Arc::new(SnapshotMetrics::default()),
        })
    }

    fn persist_journal(&self, entries: &[Entry<TypeConfig>]) -> Result<(), StorageError<NodeId>> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.state_persist");
        let Some(journal) = &self.journal else {
            return Ok(());
        };
        let mut journal = journal.lock().map_err(|_| {
            StorageIOError::write_state_machine(&std::io::Error::other(
                "state-machine journal lock was poisoned",
            ))
        })?;
        for entry in entries {
            let journal_entry = StateMachineJournalEntryRef::from_entry(entry);
            append_state_machine_journal_entry(&mut journal, &journal_entry)
                .map_err(|error| StorageIOError::write_state_machine(&error))?;
        }
        journal
            .sync_data()
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        Ok(())
    }

    fn persist_checkpoint(&self, state: &StateMachineData) -> Result<(), StorageError<NodeId>> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.state_checkpoint");
        let Some(path) = &self.path else {
            return Ok(());
        };
        // Encode borrowed views so checkpointing does not clone every retained message before
        // serde_json performs the same traversal. The caller holds the state lock while this
        // function runs, so the checkpoint remains a coherent image of the applied state.
        let persisted = PersistedStateRef::new(state);
        let bytes = serde_json::to_vec(&persisted)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        atomic_write(&path.join("state-machine.json"), &bytes)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        Ok(())
    }

    fn compact_journal(
        &self,
        last_applied_log: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let Some(journal) = &self.journal else {
            return Ok(());
        };
        let mut journal = journal.lock().map_err(|_| {
            StorageIOError::write_state_machine(&std::io::Error::other(
                "state-machine journal lock was poisoned",
            ))
        })?;
        let journal_path = path.join(STATE_MACHINE_JOURNAL_FILE);
        let retained = read_state_machine_journal(&journal_path)
            .map_err(|error| {
                StorageIOError::write_state_machine(&std::io::Error::other(error.to_string()))
            })?
            .into_iter()
            .filter(|entry| last_applied_log.is_none_or(|last| is_log_after(entry.log_id, last)))
            .collect::<Vec<_>>();
        let temporary_path = journal_path.with_extension(format!("tmp-{}", std::process::id()));
        let mut temporary = fs::File::create(&temporary_path)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        for entry in &retained {
            append_state_machine_journal_entry(&mut temporary, entry)
                .map_err(|error| StorageIOError::write_state_machine(&error))?;
        }
        temporary
            .sync_all()
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        fs::rename(&temporary_path, &journal_path)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        let parent = journal_path.parent().unwrap_or_else(|| Path::new("."));
        let directory =
            fs::File::open(parent).map_err(|error| StorageIOError::write_state_machine(&error))?;
        directory
            .sync_all()
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        *journal = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&journal_path)
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

    #[cfg(test)]
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
            delivery_token: None,
            delivery_attempt: None,
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

    async fn dead_letter_source(
        &self,
        dead_letter_stream: &str,
    ) -> Option<(String, StreamMetadata)> {
        let state = self.state.read().await;
        state
            .state
            .streams
            .iter()
            .find(|(stream, _)| dead_letter_stream_name(stream) == dead_letter_stream)
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
        let in_flight_deliveries = state
            .state
            .group_consumers
            .values()
            .map(|consumer| consumer.in_flight.len() as u64)
            .sum();
        runnel_engine::HealthSnapshot {
            streams,
            storage_bytes,
            in_flight_deliveries,
            redeliveries: state.state.redeliveries,
            dead_letters: state.state.dead_letters,
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

fn read_state_machine_journal(path: &Path) -> Result<Vec<StateMachineJournalEntry>, BrokerError> {
    let (entries, truncated_at) = parse_state_machine_journal(path)?;
    let Some(truncated_at) = truncated_at else {
        return Ok(entries);
    };
    let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
    file.set_len(truncated_at as u64)?;
    file.sync_data()?;
    Ok(entries)
}

fn validate_state_machine_journal(path: &Path) -> Result<(), BrokerError> {
    parse_state_machine_journal(path).map(|_| ())
}

fn parse_state_machine_journal(
    path: &Path,
) -> Result<(Vec<StateMachineJournalEntry>, Option<usize>), BrokerError> {
    if !path.exists() {
        return Ok((Vec::new(), None));
    }
    let bytes = fs::read(path).map_err(|error| {
        BrokerError::Cluster(format!(
            "could not read state-machine journal '{}': {error}",
            path.display()
        ))
    })?;
    let mut cursor = 0usize;
    let mut truncated_at = None;
    let mut entries = Vec::new();
    while cursor < bytes.len() {
        let record_start = cursor;
        if bytes.len() - cursor < size_of::<u32>() {
            truncated_at = Some(record_start);
            break;
        }
        let record_len = u32::from_le_bytes(
            bytes[cursor..cursor + size_of::<u32>()]
                .try_into()
                .expect("journal length has a fixed size"),
        );
        cursor += size_of::<u32>();
        if record_len > MAX_STATE_MACHINE_JOURNAL_RECORD_SIZE {
            return Err(BrokerError::Cluster(format!(
                "state-machine journal record is too large: {record_len} bytes"
            )));
        }
        let record_len = record_len as usize;
        if bytes.len() - cursor < record_len {
            truncated_at = Some(record_start);
            break;
        }
        let entry: StateMachineJournalEntry =
            serde_json::from_slice(&bytes[cursor..cursor + record_len]).map_err(|error| {
                BrokerError::Cluster(format!(
                    "invalid state-machine journal record in '{}': {error}",
                    path.display()
                ))
            })?;
        if entry.version != STATE_MACHINE_JOURNAL_FORMAT_VERSION {
            return Err(BrokerError::Cluster(format!(
                "unsupported state-machine journal format version {} in '{}' (supported version {})",
                entry.version,
                path.display(),
                STATE_MACHINE_JOURNAL_FORMAT_VERSION
            )));
        }
        entries.push(entry);
        cursor += record_len;
    }
    Ok((entries, truncated_at))
}

fn validate_state_machine_storage(path: &Path) -> Result<(), BrokerError> {
    if !path.exists() {
        return Ok(());
    }
    if !path.is_dir() {
        return Err(BrokerError::Cluster(format!(
            "invalid state-machine storage '{}': expected a directory",
            path.display()
        )));
    }
    let _ = read_persisted_state(&path.join("state-machine.json"))?;
    if let Some(snapshot) = read_persisted_snapshot(&path.join("snapshot.json"))? {
        validate_snapshot_data(&snapshot.data).map_err(|error| {
            BrokerError::Cluster(format!(
                "invalid persisted snapshot '{}': {error}",
                path.join("snapshot.json").display()
            ))
        })?;
    }
    validate_state_machine_journal(&path.join(STATE_MACHINE_JOURNAL_FILE))
}

fn append_state_machine_journal_entry<T: Serialize>(
    file: &mut fs::File,
    entry: &T,
) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(entry).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_STATE_MACHINE_JOURNAL_RECORD_SIZE as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "state-machine journal record exceeds configured size",
        ));
    }
    let length = u32::try_from(bytes.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "state-machine journal record exceeds u32 length",
        )
    })?;
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&bytes)
}

fn replay_state_machine_journal(
    state: &mut StateMachineData,
    entries: impl IntoIterator<Item = StateMachineJournalEntry>,
    kind: &GroupKind,
) -> Result<(), BrokerError> {
    for entry in entries {
        if state
            .last_applied_log
            .is_some_and(|last| !is_log_after(entry.log_id, last))
        {
            continue;
        }
        state.last_applied_log = Some(entry.log_id);
        match entry.payload {
            EntryPayload::Blank => {}
            EntryPayload::Membership(membership) => {
                state.last_membership = StoredMembership::new(Some(entry.log_id), membership);
            }
            EntryPayload::Normal(command) => {
                apply_command(&mut state.state, command, kind, entry.log_id);
            }
        }
    }
    Ok(())
}

fn is_log_after(candidate: LogId<NodeId>, current: LogId<NodeId>) -> bool {
    (candidate.leader_id.term, candidate.index) > (current.leader_id.term, current.index)
}

fn is_optional_log_after(candidate: Option<LogId<NodeId>>, current: Option<LogId<NodeId>>) -> bool {
    match (candidate, current) {
        (Some(candidate), Some(current)) => is_log_after(candidate, current),
        (Some(_), None) => true,
        _ => false,
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        self.metrics.builds_started.fetch_add(1, Ordering::Relaxed);
        let result = async {
            let (data, last_applied_log, last_membership) = {
                let state = self.state.read().await;
                // Keep the read guard through encoding so the snapshot is coherent without
                // materializing a second copy of every retained message.
                let snapshot_state = PersistedSnapshotStateRef::new(&state.state);
                let data = serde_json::to_vec(&snapshot_state)
                    .map_err(|error| StorageIOError::read_state_machine(&error))?;
                (data, state.last_applied_log, state.last_membership.clone())
            };
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
            self.compact_journal(last_applied_log)?;
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
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.state_machine_apply");
        let entries = entries.into_iter().collect::<Vec<_>>();
        let mut state = self.state.write().await;
        // The journal is the durable write-ahead record for the materialized state. Serializing
        // borrowed entries avoids cloning each retained payload before it is moved into state.
        if !entries.is_empty() {
            self.persist_journal(&entries)?;
        }
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            state.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => responses.push(CommandResponse::Noop),
                EntryPayload::Membership(membership) => {
                    state.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(CommandResponse::Noop);
                }
                EntryPayload::Normal(command) => responses.push(apply_command(
                    &mut state.state,
                    command,
                    &self.kind,
                    entry.log_id,
                )),
            }
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
            self.persist_checkpoint(&state)?;
            let last_applied_log = state.last_applied_log;
            drop(state);
            self.compact_journal(last_applied_log)?;
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

fn validate_snapshot_data(data: &[u8]) -> Result<PersistedSnapshotState, std::io::Error> {
    let persisted: PersistedSnapshotState = serde_json::from_slice(data)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if !matches!(persisted.version, 1 | FORMAT_VERSION) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unsupported snapshot format version {} (supported versions: 1 and {})",
                persisted.version, FORMAT_VERSION
            ),
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
        group_consumers: persisted
            .group_consumers
            .into_iter()
            .map(|consumer| ((consumer.stream, consumer.consumer), consumer.state))
            .collect(),
        lease_clock_ms: persisted.lease_clock_ms,
        dedup: persisted.dedup,
        redeliveries: persisted.redeliveries,
        dead_letters: persisted.dead_letters,
    }
}

fn apply_command(
    state: &mut SnapshotState,
    command: Command,
    kind: &GroupKind,
    log_id: LogId<NodeId>,
) -> CommandResponse {
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
        Command::Replay {
            stream,
            consumer: _,
            offset,
        } => apply_replay(state, &stream, offset, kind),
        Command::PollGroup {
            stream,
            consumer,
            member,
            now_ms,
            lease_deadline_ms,
            max_delivery_attempts,
        } => apply_group_poll(
            state,
            GroupPollRequest {
                stream,
                consumer,
                member,
                now_ms,
                lease_deadline_ms,
                max_delivery_attempts,
            },
            log_id,
            kind,
        ),
        Command::AckGroup {
            stream,
            consumer,
            member,
            offset,
            delivery_token,
            now_ms,
        } => apply_group_ack(
            state,
            GroupAckRequest {
                stream,
                consumer,
                member,
                offset,
                delivery_token,
                now_ms,
            },
            kind,
        ),
    }
}

fn apply_replay(
    state: &SnapshotState,
    stream: &str,
    offset: Offset,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    let Some(stream_state) = state.streams.get(stream) else {
        return CommandResponse::StreamNotFound;
    };
    if !stream_state.is_active() {
        return CommandResponse::StreamNotFound;
    }

    let next_offset = stream_state.messages.len() as Offset;
    let Some(message) = stream_state.messages.get(offset as usize) else {
        return CommandResponse::HistoryUnavailable {
            requested_offset: offset,
            earliest_offset: 0,
            next_offset,
        };
    };
    CommandResponse::Replay {
        result: ReplayMessage {
            stream: stream.to_owned(),
            offset,
            key: message.key.clone(),
            payload: message.payload.clone(),
            published_at_ms: message.published_at_ms,
        },
    }
}

fn apply_group_poll(
    state: &mut SnapshotState,
    request: GroupPollRequest,
    log_id: LogId<NodeId>,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    let GroupPollRequest {
        stream,
        consumer,
        member,
        now_ms,
        lease_deadline_ms,
        max_delivery_attempts,
    } = request;
    if !state
        .streams
        .get(&stream)
        .is_some_and(StreamState::is_active)
    {
        return CommandResponse::StreamNotFound;
    }
    let now_ms = observe_lease_clock(state, now_ms);

    let consumer_key = (stream.clone(), consumer.clone());
    if !state.group_consumers.contains_key(&consumer_key) {
        let committed_offset = state
            .consumers
            .get(&consumer_key)
            .copied()
            .unwrap_or_default();
        state.group_consumers.insert(
            consumer_key.clone(),
            GroupConsumerState {
                committed_offset,
                ..GroupConsumerState::default()
            },
        );
    }
    let existing = {
        let consumer_state = state
            .group_consumers
            .entry(consumer_key.clone())
            .or_default();
        let expired = consumer_state
            .in_flight
            .iter()
            .filter_map(|(offset, delivery)| {
                lease_expired(delivery.deadline_ms, now_ms).then_some(*offset)
            })
            .collect::<Vec<_>>();
        for offset in expired {
            consumer_state.in_flight.remove(&offset);
        }
        consumer_state
            .in_flight
            .iter()
            .find(|(_, delivery)| delivery.member == member)
            .map(|(offset, delivery)| (*offset, delivery.clone()))
    };

    if let Some((offset, delivery)) = existing {
        let messages = &state
            .streams
            .get(&stream)
            .expect("stream was checked above")
            .messages;
        return group_poll_message(&stream, offset, messages, &delivery);
    }

    loop {
        let candidate = {
            let consumer_state = state
                .group_consumers
                .get(&consumer_key)
                .expect("group consumer state was initialized above");
            let stream_state = state
                .streams
                .get(&stream)
                .expect("stream was checked above");
            stream_state
                .messages
                .iter()
                .enumerate()
                .map(|(offset, message)| (offset as Offset, message))
                .filter(|(offset, _)| *offset >= consumer_state.committed_offset)
                .find(|(offset, message)| {
                    if consumer_state.acknowledged_offsets.contains(offset)
                        || consumer_state.in_flight.contains_key(offset)
                    {
                        return false;
                    }
                    message.key.as_ref().is_none_or(|key| {
                        !consumer_state
                            .in_flight
                            .values()
                            .any(|delivery| delivery.key.as_ref() == Some(key))
                    })
                })
                .map(|(offset, message)| (offset, message.key.clone()))
        };
        let Some((offset, key)) = candidate else {
            return CommandResponse::GroupPoll {
                result: PollResult::Empty,
            };
        };

        let attempts = state
            .group_consumers
            .get(&consumer_key)
            .expect("group consumer state was initialized above")
            .delivery_attempts
            .get(&offset)
            .copied()
            .unwrap_or_default();
        if max_delivery_attempts.is_some_and(|max| attempts >= max)
            && !is_dead_letter_stream(&stream)
        {
            let original = state
                .streams
                .get(&stream)
                .and_then(|stream_state| stream_state.messages.get(offset as usize))
                .cloned()
                .expect("candidate offset must refer to a stored message");
            let dead_letter_stream = dead_letter_stream_name(&stream);
            let (stream_id, group_id) = stream_identity(&dead_letter_stream);
            state
                .streams
                .entry(dead_letter_stream)
                .or_insert_with(|| StreamState::active(stream_id, group_id))
                .messages
                .push(original);
            acknowledge_group_offset(
                state
                    .group_consumers
                    .get_mut(&consumer_key)
                    .expect("group consumer state was initialized above"),
                offset,
            );
            state.dead_letters = state.dead_letters.saturating_add(1);
            continue;
        }

        let (delivery_attempt, delivery) = {
            let consumer_state = state
                .group_consumers
                .get_mut(&consumer_key)
                .expect("group consumer state was initialized above");
            let delivery_attempt = consumer_state
                .delivery_attempts
                .entry(offset)
                .and_modify(|attempt| *attempt = attempt.saturating_add(1))
                .or_insert(1);
            let delivery = GroupDelivery {
                member: member.clone(),
                key,
                delivery_attempt: *delivery_attempt,
                delivery_token: format!("raft-{log_id}"),
                deadline_ms: lease_deadline_ms,
            };
            consumer_state.in_flight.insert(offset, delivery.clone());
            (*delivery_attempt, delivery)
        };
        if delivery_attempt > 1 {
            state.redeliveries = state.redeliveries.saturating_add(1);
        }
        let messages = &state
            .streams
            .get(&stream)
            .expect("stream was checked above")
            .messages;
        return group_poll_message(&stream, offset, messages, &delivery);
    }
}

fn group_poll_message(
    stream: &str,
    offset: Offset,
    messages: &[StoredMessage],
    delivery: &GroupDelivery,
) -> CommandResponse {
    let Some(message) = messages.get(offset as usize) else {
        return CommandResponse::StreamNotFound;
    };
    CommandResponse::GroupPoll {
        result: PollResult::Message(Message {
            stream: stream.to_owned(),
            offset,
            key: message.key.clone(),
            payload: message.payload.clone(),
            published_at_ms: message.published_at_ms,
            delivery_token: Some(delivery.delivery_token.clone()),
            delivery_attempt: Some(delivery.delivery_attempt),
        }),
    }
}

fn apply_group_ack(
    state: &mut SnapshotState,
    request: GroupAckRequest,
    kind: &GroupKind,
) -> CommandResponse {
    if matches!(kind, GroupKind::Metadata) {
        return CommandResponse::StreamNotFound;
    }
    let GroupAckRequest {
        stream,
        consumer,
        member,
        offset,
        delivery_token,
        now_ms,
    } = request;
    let Some(stream_state) = state.streams.get(&stream) else {
        return CommandResponse::StreamNotFound;
    };
    if !stream_state.is_active() {
        return CommandResponse::StreamNotFound;
    }

    let now_ms = observe_lease_clock(state, now_ms);
    let consumer_key = (stream, consumer.clone());
    let consumer_state = state.group_consumers.entry(consumer_key).or_default();
    let expired = consumer_state
        .in_flight
        .iter()
        .filter_map(|(offset, delivery)| {
            lease_expired(delivery.deadline_ms, now_ms).then_some(*offset)
        })
        .collect::<Vec<_>>();
    for expired_offset in expired {
        consumer_state.in_flight.remove(&expired_offset);
    }

    if offset < consumer_state.committed_offset
        || consumer_state.acknowledged_offsets.contains(&offset)
    {
        return CommandResponse::GroupAlreadyAcknowledged;
    }
    let Some(delivery) = consumer_state.in_flight.get(&offset) else {
        if delivery_token.is_empty()
            && member == consumer
            && offset == consumer_state.committed_offset
        {
            acknowledge_group_offset(consumer_state, offset);
            return CommandResponse::GroupAcknowledged;
        }
        return if consumer_state.delivery_attempts.contains_key(&offset) {
            CommandResponse::GroupStaleDelivery { consumer, offset }
        } else {
            CommandResponse::GroupAckNotInFlight { consumer, offset }
        };
    };
    if delivery.member != member
        || (!delivery_token.is_empty() && delivery.delivery_token != delivery_token)
    {
        return CommandResponse::GroupStaleDelivery { consumer, offset };
    }

    acknowledge_group_offset(consumer_state, offset);
    CommandResponse::GroupAcknowledged
}

fn acknowledge_group_offset(consumer_state: &mut GroupConsumerState, offset: Offset) {
    consumer_state.in_flight.remove(&offset);
    consumer_state.delivery_attempts.remove(&offset);
    if offset == consumer_state.committed_offset {
        consumer_state.committed_offset = consumer_state.committed_offset.saturating_add(1);
        while consumer_state
            .acknowledged_offsets
            .remove(&consumer_state.committed_offset)
        {
            consumer_state.committed_offset = consumer_state.committed_offset.saturating_add(1);
        }
    } else {
        consumer_state.acknowledged_offsets.insert(offset);
    }
}

fn observe_lease_clock(state: &mut SnapshotState, observed_ms: u64) -> u64 {
    state.lease_clock_ms = state.lease_clock_ms.max(observed_ms);
    state.lease_clock_ms
}

fn lease_expired(deadline_ms: u64, now_ms: u64) -> bool {
    deadline_ms <= now_ms
}

fn stream_identity(stream: &str) -> (String, String) {
    (format!("stream/{stream}"), format!("group/{stream}/data"))
}

fn dead_letter_stream_name(stream: &str) -> String {
    let name = format!("{stream}{DEAD_LETTER_SUFFIX}");
    if name.len() <= 128 {
        return name;
    }
    let hash = stream.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    format!("{DEAD_LETTER_HASH_PREFIX}{hash:016x}")
}

fn is_dead_letter_stream(stream: &str) -> bool {
    stream.ends_with(DEAD_LETTER_SUFFIX) || stream.starts_with(DEAD_LETTER_HASH_PREFIX)
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
                    ack_timeout: DEFAULT_RAFT_ACK_TIMEOUT,
                    max_delivery_attempts: None,
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
    ack_timeout: Duration,
    max_delivery_attempts: Option<u32>,
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
            ack_timeout: DEFAULT_RAFT_ACK_TIMEOUT,
            max_delivery_attempts: None,
        })
    }

    pub async fn create_stream(&self, stream: String) -> Result<bool, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.create_stream");
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
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.publish_quorum");
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
        self.poll_group(stream.to_owned(), consumer.to_owned(), consumer.to_owned())
            .await
    }

    pub async fn replay(
        &self,
        stream: String,
        consumer: String,
        offset: Offset,
    ) -> Result<ReplayMessage, BrokerError> {
        validate_name("stream", &stream)?;
        validate_name("consumer", &consumer)?;
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.replay_quorum");
        let stream_name = stream.clone();
        let response = self
            .raft
            .client_write(Command::Replay {
                stream,
                consumer,
                offset,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::Replay { result } => Ok(result),
            CommandResponse::HistoryUnavailable {
                requested_offset,
                earliest_offset,
                next_offset,
            } => Err(BrokerError::HistoryUnavailable {
                stream: stream_name,
                requested_offset,
                earliest_offset,
                next_offset,
            }),
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected replay response: {other:?}"
            ))),
        }
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
        self.ack_group(stream, consumer.clone(), consumer, offset, String::new())
            .await
    }

    pub async fn poll_group(
        &self,
        stream: String,
        consumer: String,
        member: String,
    ) -> Result<PollResult, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.poll_quorum");
        let now_ms = now_ms();
        let lease_deadline_ms = now_ms.saturating_add(duration_ms(self.ack_timeout));
        let stream_name = stream.clone();
        let response = self
            .raft
            .client_write(Command::PollGroup {
                stream,
                consumer,
                member,
                now_ms,
                lease_deadline_ms,
                max_delivery_attempts: self.max_delivery_attempts,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::GroupPoll { result } => Ok(result),
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected grouped poll response: {other:?}"
            ))),
        }
    }

    pub async fn ack_group(
        &self,
        stream: String,
        consumer: String,
        member: String,
        offset: Offset,
        delivery_token: String,
    ) -> Result<AckResult, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.ack_quorum");
        let stream_name = stream.clone();
        let consumer_name = consumer.clone();
        let response = self
            .raft
            .client_write(Command::AckGroup {
                stream,
                consumer,
                member,
                offset,
                delivery_token,
                now_ms: now_ms(),
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::GroupAcknowledged => Ok(AckResult::Acknowledged),
            CommandResponse::GroupAlreadyAcknowledged => Ok(AckResult::AlreadyAcknowledged),
            CommandResponse::GroupAckNotInFlight { consumer, offset } => {
                Err(BrokerError::AckNotInFlight { consumer, offset })
            }
            CommandResponse::GroupStaleDelivery { consumer, offset } => {
                Err(BrokerError::StaleDelivery { consumer, offset })
            }
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected grouped acknowledgement response for consumer '{consumer_name}': {other:?}"
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

    fn replay<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, ReplayMessage> {
        Box::pin(async move {
            self.group
                .replay(stream.to_owned(), consumer.to_owned(), offset)
                .await
        })
    }

    fn poll_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
    ) -> EngineFuture<'a, PollResult> {
        Box::pin(async move {
            self.group
                .poll_group(stream.to_owned(), consumer.to_owned(), member.to_owned())
                .await
        })
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

    fn ack_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        offset: Offset,
        delivery_token: &'a str,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move {
            self.group
                .ack_group(
                    stream.to_owned(),
                    consumer.to_owned(),
                    member.to_owned(),
                    offset,
                    delivery_token.to_owned(),
                )
                .await
        })
    }

    fn health<'a>(&'a self) -> EngineFuture<'a, runnel_engine::HealthSnapshot> {
        Box::pin(async move { Ok(self.group.health().await) })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    ack_timeout: Duration,
    max_delivery_attempts: Option<u32>,
    groups: RwLock<BTreeMap<String, Arc<RaftGroup>>>,
    creation_lock: Mutex<()>,
    peer_transport: Arc<network::PeerTransport>,
}

impl GroupManager {
    async fn open(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
        ack_timeout: Duration,
        max_delivery_attempts: Option<u32>,
    ) -> Result<Arc<Self>, BrokerError> {
        let manager = Arc::new(Self {
            node_id,
            cluster_name,
            data_dir: data_dir.as_ref().to_path_buf(),
            peers,
            ack_timeout,
            max_delivery_attempts,
            groups: RwLock::new(BTreeMap::new()),
            creation_lock: Mutex::new(()),
            peer_transport: network::PeerTransport::new(),
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
        let network = network::TcpNetwork::with_transport(
            self.peers.clone(),
            group_id,
            Arc::clone(&self.peer_transport),
        );
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
            ack_timeout: self.ack_timeout,
            max_delivery_attempts: self.max_delivery_attempts,
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
                return Err(BrokerError::Cluster(format!(
                    "invalid data-group entry '{}': expected a directory",
                    entry.path().display()
                )));
            }
            let manifest_path = entry.path().join("group.json");
            if !manifest_path.exists() {
                return Err(BrokerError::Cluster(format!(
                    "missing data-group manifest '{}'",
                    manifest_path.display()
                )));
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
        let manifest_path = directory.join("group.json");
        if directory.exists() && !directory.is_dir() {
            return Err(BrokerError::Cluster(format!(
                "invalid data-group storage '{}': expected a directory",
                directory.display()
            )));
        }
        if directory.exists() && !manifest_path.exists() {
            let has_entries = fs::read_dir(&directory)?.next().transpose()?.is_some();
            if has_entries {
                return Err(BrokerError::Cluster(format!(
                    "missing data-group manifest '{}'",
                    manifest_path.display()
                )));
            }
        }
        let manifest = DataGroupManifest {
            stream: stream.to_owned(),
            stream_id: metadata.stream_id.clone(),
            group_id: metadata.group_id.clone(),
        };
        if !manifest_path.exists() {
            let bytes = serde_json::to_vec(&manifest)?;
            atomic_write(&manifest_path, &bytes)?;
        }
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
        let (group_stream, metadata) = match metadata_group.stream_metadata(stream).await {
            Ok(metadata) => (stream.to_owned(), metadata),
            Err(BrokerError::StreamNotFound(_)) => metadata_group
                .state_machine
                .dead_letter_source(stream)
                .await
                .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?,
            Err(error) => return Err(error),
        };
        if metadata.lifecycle != StreamLifecycle::Active {
            return Err(BrokerError::StreamNotReady(stream.to_owned()));
        }
        self.ensure_data_group_local(&group_stream, &metadata).await
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
                &self.peer_transport,
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
                    &self.peer_transport,
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
            .poll_group(stream.to_owned(), consumer.to_owned(), consumer.to_owned())
            .await
    }

    pub(crate) async fn replay_local(
        &self,
        stream: &str,
        consumer: &str,
        offset: Offset,
    ) -> Result<ReplayMessage, BrokerError> {
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        self.data_group_for_stream(stream)
            .await?
            .replay(stream.to_owned(), consumer.to_owned(), offset)
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
            .ack_group(stream, consumer.clone(), consumer, offset, String::new())
            .await
    }

    pub(crate) async fn poll_group_local(
        &self,
        stream: &str,
        consumer: &str,
        member: &str,
    ) -> Result<PollResult, BrokerError> {
        self.data_group_for_stream(stream)
            .await?
            .poll_group(stream.to_owned(), consumer.to_owned(), member.to_owned())
            .await
    }

    pub(crate) async fn ack_group_local(
        &self,
        stream: String,
        consumer: String,
        member: String,
        offset: Offset,
        delivery_token: String,
    ) -> Result<AckResult, BrokerError> {
        self.data_group_for_stream(&stream)
            .await?
            .ack_group(stream, consumer, member, offset, delivery_token)
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
        let mut in_flight_deliveries = 0;
        let mut redeliveries = metadata_health.redeliveries;
        let mut dead_letters = metadata_health.dead_letters;
        for group in groups {
            let health = group.health().await;
            storage_bytes += health.storage_bytes;
            in_flight_deliveries += health.in_flight_deliveries;
            redeliveries += health.redeliveries;
            dead_letters += health.dead_letters;
        }
        runnel_engine::HealthSnapshot {
            streams: metadata_health.streams,
            storage_bytes,
            in_flight_deliveries,
            redeliveries,
            dead_letters,
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

fn validate_name(kind: &'static str, name: &str) -> Result<(), BrokerError> {
    let valid_length = (1..=128).contains(&name.len());
    let valid_characters = name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid_length && valid_characters {
        return Ok(());
    }
    Err(BrokerError::InvalidName {
        kind,
        name: name.to_owned(),
    })
}

fn validate_persisted_group_storage(directory: &Path) -> Result<(), BrokerError> {
    if !directory.exists() {
        return Ok(());
    }
    if !directory.is_dir() {
        return Err(BrokerError::Cluster(format!(
            "invalid clustered group storage '{}': expected a directory",
            directory.display()
        )));
    }
    log_store::LogStore::<TypeConfig>::validate(directory.join("raft-log.json"))
        .map_err(|error| BrokerError::Cluster(error.to_string()))?;
    validate_state_machine_storage(&directory.join("state-machine"))
}

fn validate_persisted_cluster_storage(data_dir: &Path) -> Result<(), BrokerError> {
    let groups_directory = data_dir.join("groups");
    if !groups_directory.exists() {
        return Ok(());
    }
    if !groups_directory.is_dir() {
        return Err(BrokerError::Cluster(format!(
            "invalid clustered storage '{}': expected a groups directory",
            groups_directory.display()
        )));
    }
    validate_persisted_group_storage(&groups_directory.join(METADATA_GROUP_ID))?;

    let data_groups_directory = groups_directory.join("data");
    if !data_groups_directory.exists() {
        return Ok(());
    }
    if !data_groups_directory.is_dir() {
        return Err(BrokerError::Cluster(format!(
            "invalid clustered storage '{}': expected a data-groups directory",
            data_groups_directory.display()
        )));
    }
    for entry in fs::read_dir(&data_groups_directory)? {
        let entry = entry?;
        let entry_path = entry.path();
        if !entry.file_type()?.is_dir() {
            return Err(BrokerError::Cluster(format!(
                "invalid data-group entry '{}': expected a directory",
                entry_path.display()
            )));
        }
        let manifest_path = entry_path.join("group.json");
        if !manifest_path.exists() {
            return Err(BrokerError::Cluster(format!(
                "missing data-group manifest '{}'",
                manifest_path.display()
            )));
        }
        let manifest: DataGroupManifest = serde_json::from_slice(&fs::read(&manifest_path)?)
            .map_err(|error| {
                BrokerError::Cluster(format!(
                    "invalid data-group manifest '{}': {error}",
                    manifest_path.display()
                ))
            })?;
        let expected_directory = data_groups_directory.join(path_component(&manifest.stream));
        if entry_path != expected_directory {
            return Err(BrokerError::Cluster(format!(
                "data-group manifest '{}' does not match its directory '{}'",
                manifest_path.display(),
                entry_path.display()
            )));
        }
        let (expected_stream_id, expected_group_id) = stream_identity(&manifest.stream);
        if manifest.stream_id != expected_stream_id || manifest.group_id != expected_group_id {
            return Err(BrokerError::Cluster(format!(
                "data-group manifest '{}' has incompatible stream/group identity",
                manifest_path.display()
            )));
        }
        validate_persisted_group_storage(&entry_path)?;
    }
    Ok(())
}

fn ensure_storage_metadata(
    data_dir: &Path,
    cluster_name: &str,
    node_id: NodeId,
) -> Result<(), BrokerError> {
    let metadata_path = data_dir.join(STORAGE_METADATA_FILE);
    if metadata_path.exists() {
        let bytes = fs::read(&metadata_path).map_err(|error| {
            BrokerError::Cluster(format!(
                "could not read persisted storage metadata '{}': {error}",
                metadata_path.display()
            ))
        })?;
        let metadata: PersistedStorageMetadata =
            serde_json::from_slice(&bytes).map_err(|error| {
                BrokerError::Cluster(format!(
                    "invalid persisted storage metadata '{}': {error}",
                    metadata_path.display()
                ))
            })?;
        if metadata.version != STORAGE_METADATA_FORMAT_VERSION {
            return Err(BrokerError::Cluster(format!(
                "unsupported storage metadata format version {} in '{}' (supported version {})",
                metadata.version,
                metadata_path.display(),
                STORAGE_METADATA_FORMAT_VERSION
            )));
        }
        if metadata.cluster_name != cluster_name {
            return Err(BrokerError::Cluster(format!(
                "cluster identity mismatch: storage belongs to cluster '{}', configured cluster is '{}'",
                metadata.cluster_name, cluster_name
            )));
        }
        if metadata.node_id != node_id {
            return Err(BrokerError::Cluster(format!(
                "node identity mismatch: storage belongs to node {}, configured node is {}",
                metadata.node_id, node_id
            )));
        }
        return Ok(());
    }

    if data_dir.join("groups").exists() {
        return Err(BrokerError::Cluster(
            "persisted clustered storage is missing storage metadata; refusing to open it because its cluster identity cannot be verified"
                .to_owned(),
        ));
    }

    let metadata = PersistedStorageMetadata {
        version: STORAGE_METADATA_FORMAT_VERSION,
        cluster_name: cluster_name.to_owned(),
        node_id,
    };
    let bytes = serde_json::to_vec(&metadata).map_err(|error| {
        BrokerError::Cluster(format!(
            "could not encode storage metadata '{}': {error}",
            metadata_path.display()
        ))
    })?;
    atomic_write(&metadata_path, &bytes).map_err(|error| {
        BrokerError::Cluster(format!(
            "could not persist storage metadata '{}': {error}",
            metadata_path.display()
        ))
    })?;
    Ok(())
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
        Self::open_with_ack_timeout(
            node_id,
            cluster_name,
            data_dir,
            peers,
            bootstrap,
            DEFAULT_RAFT_ACK_TIMEOUT,
        )
        .await
    }

    pub async fn open_with_ack_timeout(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
        bootstrap: bool,
        ack_timeout: Duration,
    ) -> Result<Self, BrokerError> {
        Self::open_with_config(
            node_id,
            cluster_name,
            data_dir,
            peers,
            bootstrap,
            ack_timeout,
            None,
        )
        .await
    }

    pub async fn open_with_config(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
        bootstrap: bool,
        ack_timeout: Duration,
        max_delivery_attempts: Option<u32>,
    ) -> Result<Self, BrokerError> {
        if max_delivery_attempts == Some(0) {
            return Err(BrokerError::Configuration(
                "max delivery attempts must be greater than zero".to_owned(),
            ));
        }
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir)?;
        let legacy_paths = LEGACY_SINGLE_GROUP_PATHS
            .iter()
            .map(|relative| data_dir.join(relative))
            .filter(|path| path.exists())
            .collect::<Vec<_>>();
        if !legacy_paths.is_empty() {
            return Err(BrokerError::Cluster(format!(
                "legacy single-group storage detected at {}; migrate the data directory before starting this clustered layout",
                legacy_paths
                    .iter()
                    .map(|path| format!("'{}'", path.display()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        ensure_storage_metadata(data_dir, &cluster_name, node_id)?;
        validate_persisted_cluster_storage(data_dir)?;
        let manager = GroupManager::open(
            node_id,
            cluster_name,
            data_dir,
            peers.clone(),
            ack_timeout,
            max_delivery_attempts,
        )
        .await?;
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
            | network::ForwardedOperation::Replay { stream, .. }
            | network::ForwardedOperation::Ack { stream, .. }
            | network::ForwardedOperation::PollGroup { stream, .. }
            | network::ForwardedOperation::AckGroup { stream, .. }
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
        let mut last_error = None;
        for _ in 0..FORWARD_ATTEMPTS {
            let preferred_leader = if let Some(leader_id) = leader_id.take() {
                Some(leader_id)
            } else {
                self.operation_leader(&operation).await?
            };
            let mut candidates = self
                .peers
                .keys()
                .copied()
                .filter(|target| *target != self.node_id)
                .collect::<Vec<_>>();
            if let Some(preferred_leader) =
                preferred_leader.filter(|target| *target != self.node_id)
            {
                candidates.retain(|target| *target != preferred_leader);
                candidates.insert(0, preferred_leader);
            }

            for target in candidates {
                let Some(address) = self.peers.get(&target) else {
                    last_error = Some(format!("leader node {target} has no configured address"));
                    continue;
                };
                let response = match network::forward(
                    &self.manager.peer_transport,
                    address,
                    operation.clone(),
                    FORWARD_TIMEOUT,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        last_error = Some(format!("leader forwarding failed: {error}"));
                        continue;
                    }
                };
                if let Some(next_leader) = forwarded_leader(&response) {
                    if let Some(next_leader) = next_leader {
                        leader_id = Some(next_leader);
                    } else {
                        last_error = Some("peer has no elected leader".to_owned());
                    }
                    continue;
                }
                return Ok(response);
            }
        }
        Err(BrokerError::Cluster(last_error.unwrap_or_else(|| {
            "cluster has no elected leader".to_owned()
        })))
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

    async fn forward_replay(
        &self,
        operation: network::ForwardedOperation,
        leader_id: Option<NodeId>,
    ) -> Result<ReplayMessage, BrokerError> {
        match self.forward_operation(operation, leader_id).await? {
            network::ForwardedResponse::Replay(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong replay response".to_owned(),
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

    async fn forward_poll_group(
        &self,
        stream: String,
        consumer: String,
        member: String,
        leader_id: Option<NodeId>,
    ) -> Result<PollResult, BrokerError> {
        match self
            .forward_operation(
                network::ForwardedOperation::PollGroup {
                    stream,
                    consumer,
                    member,
                },
                leader_id,
            )
            .await?
        {
            network::ForwardedResponse::PollGroup(result) => {
                result.map_err(forward_error_to_broker)
            }
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong grouped poll response".to_owned(),
            )),
        }
    }

    async fn forward_ack_group(
        &self,
        operation: network::ForwardedOperation,
        leader_id: Option<NodeId>,
    ) -> Result<AckResult, BrokerError> {
        match self.forward_operation(operation, leader_id).await? {
            network::ForwardedResponse::AckGroup(result) => result.map_err(forward_error_to_broker),
            _ => Err(BrokerError::Cluster(
                "leader returned the wrong grouped acknowledgement response".to_owned(),
            )),
        }
    }
}

fn forwarded_leader(response: &network::ForwardedResponse) -> Option<Option<NodeId>> {
    match response {
        network::ForwardedResponse::CreateStream(Err(network::ForwardError::NotLeader {
            leader_id,
        }))
        | network::ForwardedResponse::Publish(Err(network::ForwardError::NotLeader {
            leader_id,
        }))
        | network::ForwardedResponse::Poll(Err(network::ForwardError::NotLeader { leader_id }))
        | network::ForwardedResponse::Replay(Err(network::ForwardError::NotLeader { leader_id }))
        | network::ForwardedResponse::Ack(Err(network::ForwardError::NotLeader { leader_id }))
        | network::ForwardedResponse::PollGroup(Err(network::ForwardError::NotLeader {
            leader_id,
        }))
        | network::ForwardedResponse::AckGroup(Err(network::ForwardError::NotLeader {
            leader_id,
        })) => Some(*leader_id),
        _ => None,
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
            let Some(leader_id) = data_group.raft().current_leader().await else {
                return Err(BrokerError::NotLeader { leader_id: None });
            };
            if leader_id != self.node_id {
                return self
                    .forward_poll(stream.to_owned(), consumer.to_owned(), Some(leader_id))
                    .await;
            }
            self.manager.poll_local(stream, consumer).await
        })
    }

    fn replay<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, ReplayMessage> {
        Box::pin(async move {
            validate_name("stream", stream)?;
            validate_name("consumer", consumer)?;
            let operation = network::ForwardedOperation::Replay {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                offset,
            };
            match self.manager.replay_local(stream, consumer, offset).await {
                Ok(message) => Ok(message),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forward_replay(operation, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn poll_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
    ) -> EngineFuture<'a, PollResult> {
        Box::pin(async move {
            let data_group = self.manager.data_group_for_stream(stream).await?;
            let Some(leader_id) = data_group.raft().current_leader().await else {
                return Err(BrokerError::NotLeader { leader_id: None });
            };
            if leader_id != self.node_id {
                return self
                    .forward_poll_group(
                        stream.to_owned(),
                        consumer.to_owned(),
                        member.to_owned(),
                        Some(leader_id),
                    )
                    .await;
            }
            self.manager
                .poll_group_local(stream, consumer, member)
                .await
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

    fn ack_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        offset: Offset,
        delivery_token: &'a str,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move {
            let operation = network::ForwardedOperation::AckGroup {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                member: member.to_owned(),
                offset,
                delivery_token: delivery_token.to_owned(),
            };
            match self
                .manager
                .ack_group_local(
                    stream.to_owned(),
                    consumer.to_owned(),
                    member.to_owned(),
                    offset,
                    delivery_token.to_owned(),
                )
                .await
            {
                Ok(result) => Ok(result),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forward_ack_group(operation, leader_id).await
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
        network::ForwardError::AckNotInFlight { consumer, offset } => {
            BrokerError::AckNotInFlight { consumer, offset }
        }
        network::ForwardError::StaleDelivery { consumer, offset } => {
            BrokerError::StaleDelivery { consumer, offset }
        }
        network::ForwardError::HistoryUnavailable {
            stream,
            requested_offset,
            earliest_offset,
            next_offset,
        } => BrokerError::HistoryUnavailable {
            stream,
            requested_offset,
            earliest_offset,
            next_offset,
        },
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

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
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

    fn grouped_test_state() -> SnapshotState {
        let mut state = SnapshotState::default();
        state.streams.insert(
            "events".to_owned(),
            StreamState::active("stream/events".to_owned(), "group/events/data".to_owned()),
        );
        state
            .streams
            .get_mut("events")
            .unwrap()
            .messages
            .push(StoredMessage {
                key: None,
                payload: b"lease".to_vec(),
                published_at_ms: 1,
            });
        state.streams.insert(
            "clock".to_owned(),
            StreamState::active("stream/clock".to_owned(), "group/clock/data".to_owned()),
        );
        state
    }

    fn data_group_kind(stream: &str) -> GroupKind {
        GroupKind::Data {
            stream: stream.to_owned(),
            stream_id: format!("stream/{stream}"),
            group_id: format!("group/{stream}/data"),
        }
    }

    fn poll_group_for_test(
        state: &mut SnapshotState,
        stream: &str,
        consumer: &str,
        member: &str,
        now_ms: u64,
        lease_deadline_ms: u64,
        log_index: u64,
    ) -> PollResult {
        poll_group_with_log_id_for_test(
            state,
            stream,
            consumer,
            member,
            now_ms,
            lease_deadline_ms,
            LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: log_index,
            },
        )
    }

    fn poll_group_with_log_id_for_test(
        state: &mut SnapshotState,
        stream: &str,
        consumer: &str,
        member: &str,
        now_ms: u64,
        lease_deadline_ms: u64,
        log_id: LogId<NodeId>,
    ) -> PollResult {
        match apply_command(
            state,
            Command::PollGroup {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                member: member.to_owned(),
                now_ms,
                lease_deadline_ms,
                max_delivery_attempts: None,
            },
            &data_group_kind(stream),
            log_id,
        ) {
            CommandResponse::GroupPoll { result } => result,
            response => panic!("unexpected grouped poll response: {response:?}"),
        }
    }

    fn round_trip_snapshot_state_for_test(state: &SnapshotState) -> SnapshotState {
        let recovered = serde_json::from_slice::<PersistedSnapshotState>(
            &serde_json::to_vec(&PersistedSnapshotStateRef::new(state)).unwrap(),
        )
        .unwrap();
        snapshot_state_from_persisted(recovered)
    }

    fn delivery_token_for_test(
        state: &mut SnapshotState,
        member: &str,
        now_ms: u64,
        lease_deadline_ms: u64,
        log_index: u64,
    ) -> String {
        match poll_group_for_test(
            state,
            "events",
            "workers",
            member,
            now_ms,
            lease_deadline_ms,
            log_index,
        ) {
            PollResult::Message(message) => message.delivery_token.unwrap(),
            PollResult::Empty => panic!("expected grouped delivery"),
        }
    }

    fn ack_group_for_test(
        state: &mut SnapshotState,
        stream: &str,
        consumer: &str,
        member: &str,
        offset: Offset,
        delivery_token: &str,
        now_ms: u64,
    ) -> CommandResponse {
        apply_command(
            state,
            Command::AckGroup {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                member: member.to_owned(),
                offset,
                delivery_token: delivery_token.to_owned(),
                now_ms,
            },
            &data_group_kind(stream),
            LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 100 + now_ms,
            },
        )
    }

    #[test]
    fn grouped_lease_deadline_boundaries_are_future_past_and_equal() {
        assert!(!lease_expired(200, 199));
        assert!(lease_expired(200, 200));
        assert!(lease_expired(200, 201));

        let mut future_state = grouped_test_state();
        let first_token = delivery_token_for_test(&mut future_state, "member-a", 100, 200, 0);
        let same_delivery = poll_group_for_test(
            &mut future_state,
            "events",
            "workers",
            "member-a",
            199,
            299,
            1,
        );
        assert!(matches!(
            same_delivery,
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                delivery_token: Some(token),
                ..
            }) if token == first_token
        ));

        let mut equal_state = grouped_test_state();
        let first_token = delivery_token_for_test(&mut equal_state, "member-a", 100, 200, 0);
        let redelivery = poll_group_for_test(
            &mut equal_state,
            "events",
            "workers",
            "member-b",
            200,
            300,
            1,
        );
        assert!(matches!(
            redelivery,
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                delivery_token: Some(token),
                ..
            }) if token != first_token
        ));

        let mut past_state = grouped_test_state();
        delivery_token_for_test(&mut past_state, "member-a", 100, 200, 0);
        assert!(matches!(
            poll_group_for_test(
                &mut past_state,
                "events",
                "workers",
                "member-b",
                201,
                301,
                1,
            ),
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
    }

    #[test]
    fn grouped_lease_forward_jump_and_fixed_leader_offset_expire_early() {
        let mut forward_jump_state = grouped_test_state();
        let old_token = delivery_token_for_test(&mut forward_jump_state, "member-a", 100, 200, 0);
        let redelivery = poll_group_for_test(
            &mut forward_jump_state,
            "events",
            "workers",
            "member-b",
            10_000,
            10_100,
            1,
        );
        let new_token = match redelivery {
            PollResult::Message(message) => {
                assert_eq!(message.delivery_attempt, Some(2));
                message.delivery_token.unwrap()
            }
            PollResult::Empty => panic!("expected redelivery after a forward clock jump"),
        };
        assert_ne!(new_token, old_token);
        assert_eq!(forward_jump_state.lease_clock_ms, 10_000);

        // A successor whose clock is fixed 2 seconds ahead can expire a
        // delivery even when its own new deadline is still in the future.
        let mut offset_state = grouped_test_state();
        let old_token = match poll_group_with_log_id_for_test(
            &mut offset_state,
            "events",
            "workers",
            "member-a",
            1_000,
            2_000,
            LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 0,
            },
        ) {
            PollResult::Message(message) => message.delivery_token.unwrap(),
            PollResult::Empty => panic!("expected initial delivery"),
        };
        let redelivery = poll_group_with_log_id_for_test(
            &mut offset_state,
            "events",
            "workers",
            "member-b",
            3_000,
            4_000,
            LogId {
                leader_id: openraft::CommittedLeaderId::new(2, 2),
                index: 0,
            },
        );
        assert!(!lease_expired(4_000, 3_000));
        match redelivery {
            PollResult::Message(message) => {
                assert_eq!(message.delivery_attempt, Some(2));
                assert_ne!(message.delivery_token.as_deref(), Some(old_token.as_str()));
            }
            PollResult::Empty => panic!("expected redelivery after a fixed leader offset"),
        }
        assert_eq!(offset_state.lease_clock_ms, 3_000);
    }

    #[test]
    fn grouped_lease_clock_floor_survives_snapshot_recovery_and_backward_time() {
        let mut state = grouped_test_state();
        poll_group_for_test(&mut state, "events", "workers", "member-a", 100, 200, 0);
        assert_eq!(state.lease_clock_ms, 100);
        assert_eq!(
            poll_group_for_test(&mut state, "clock", "workers", "member-a", 200, 300, 1),
            PollResult::Empty
        );
        assert_eq!(state.lease_clock_ms, 200);

        let mut recovered = round_trip_snapshot_state_for_test(&state);
        assert_eq!(recovered.lease_clock_ms, 200);

        assert!(matches!(
            poll_group_for_test(&mut recovered, "events", "workers", "member-b", 150, 250, 2,),
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        assert_eq!(recovered.lease_clock_ms, 200);
    }

    #[test]
    fn grouped_lease_preserves_early_expiry_for_deadline_behind_clock_floor() {
        let mut state = grouped_test_state();
        delivery_token_for_test(&mut state, "member-a", 100, 200, 0);

        // A committed observation from another command advances the floor.
        assert_eq!(
            poll_group_for_test(&mut state, "clock", "workers", "member-a", 300, 400, 1),
            PollResult::Empty
        );
        assert_eq!(state.lease_clock_ms, 300);

        // A successor with a regressed clock can submit a deadline behind the
        // floor. Keep that absolute deadline unchanged so it expires on the
        // next command instead of silently extending the delivery.
        let regressed_token = delivery_token_for_test(&mut state, "member-b", 150, 250, 2);
        let delivery = state
            .group_consumers
            .get(&("events".to_owned(), "workers".to_owned()))
            .and_then(|consumer| consumer.in_flight.get(&0))
            .expect("redelivery must be in flight");
        assert_eq!(delivery.deadline_ms, 250);
        assert!(lease_expired(delivery.deadline_ms, state.lease_clock_ms));

        let next_delivery =
            poll_group_for_test(&mut state, "events", "workers", "member-c", 150, 350, 3);
        match next_delivery {
            PollResult::Message(message) => {
                assert_eq!(message.delivery_attempt, Some(3));
                assert_ne!(
                    message.delivery_token.as_deref(),
                    Some(regressed_token.as_str())
                );
            }
            PollResult::Empty => panic!("expected the behind-floor deadline to expire"),
        }
    }

    #[test]
    fn grouped_lease_has_no_lazy_expiry_without_a_committed_command() {
        let mut state = grouped_test_state();
        let token = delivery_token_for_test(&mut state, "member-a", 100, 200, 0);

        // This state machine has no timer callback. With no elected leader,
        // no PollGroup/AckGroup command can be committed, so the delivery
        // remains in flight until a later command supplies an observation.
        let recovered = round_trip_snapshot_state_for_test(&state);
        let delivery = recovered
            .group_consumers
            .get(&("events".to_owned(), "workers".to_owned()))
            .and_then(|consumer| consumer.in_flight.get(&0))
            .expect("snapshot recovery must retain the in-flight delivery");
        assert_eq!(delivery.delivery_token, token);
        assert_eq!(recovered.lease_clock_ms, 100);

        let mut recovered = recovered;
        assert!(matches!(
            poll_group_for_test(&mut recovered, "events", "workers", "member-a", 50, 150, 1),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                delivery_token: Some(ref current_token),
                ..
            }) if current_token == &token
        ));
        assert_eq!(recovered.lease_clock_ms, 100);
    }

    #[test]
    fn grouped_ack_preserves_backward_clock_safety_and_fences_expired_tokens() {
        let mut state = grouped_test_state();
        let old_token = delivery_token_for_test(&mut state, "member-a", 100, 200, 0);

        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-a", 0, &old_token, 50,
            ),
            CommandResponse::GroupAcknowledged
        );

        let mut state = grouped_test_state();
        let old_token = delivery_token_for_test(&mut state, "member-a", 100, 200, 0);
        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-a", 0, &old_token, 200,
            ),
            CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }
        );
        let new_token = delivery_token_for_test(&mut state, "member-b", 150, 250, 2);
        assert_ne!(old_token, new_token);
        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-a", 0, &old_token, 150,
            ),
            CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }
        );
        assert_eq!(
            ack_group_for_test(
                &mut state, "events", "workers", "member-b", 0, &new_token, 150,
            ),
            CommandResponse::GroupAcknowledged
        );
    }

    #[tokio::test]
    async fn grouped_lease_survives_journal_restart_and_leader_change() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let store =
            Arc::new(StateMachineStore::open(&state_directory, data_group_kind("events")).unwrap());
        let mut state_machine = store.clone();
        let first_responses = state_machine
            .apply([
                Entry {
                    log_id: LogId {
                        leader_id: openraft::CommittedLeaderId::new(1, 1),
                        index: 0,
                    },
                    payload: EntryPayload::Normal(Command::Publish {
                        stream: "events".to_owned(),
                        key: None,
                        payload: b"lease".to_vec(),
                        published_at_ms: 1,
                        request_id: None,
                    }),
                },
                Entry {
                    log_id: LogId {
                        leader_id: openraft::CommittedLeaderId::new(1, 1),
                        index: 1,
                    },
                    payload: EntryPayload::Normal(Command::PollGroup {
                        stream: "events".to_owned(),
                        consumer: "workers".to_owned(),
                        member: "member-a".to_owned(),
                        now_ms: 100,
                        lease_deadline_ms: 200,
                        max_delivery_attempts: None,
                    }),
                },
            ])
            .await
            .unwrap();
        let old_token = match &first_responses[1] {
            CommandResponse::GroupPoll {
                result: PollResult::Message(message),
            } => message.delivery_token.clone().unwrap(),
            response => panic!("unexpected initial poll response: {response:?}"),
        };
        drop(state_machine);
        drop(store);

        let reopened =
            StateMachineStore::open(&state_directory, data_group_kind("events")).unwrap();
        {
            let state = reopened.state.read().await;
            assert_eq!(state.state.lease_clock_ms, 100);
            assert_eq!(
                state
                    .state
                    .group_consumers
                    .get(&("events".to_owned(), "workers".to_owned()))
                    .and_then(|consumer| consumer.in_flight.get(&0))
                    .map(|delivery| delivery.delivery_token.as_str()),
                Some(old_token.as_str())
            );
        }

        let reopened = Arc::new(reopened);
        let mut state_machine = reopened.clone();
        let redelivery_responses = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(2, 2),
                    index: 0,
                },
                payload: EntryPayload::Normal(Command::PollGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-b".to_owned(),
                    now_ms: 200,
                    lease_deadline_ms: 300,
                    max_delivery_attempts: None,
                }),
            }))
            .await
            .unwrap();
        let new_token = match &redelivery_responses[0] {
            CommandResponse::GroupPoll {
                result: PollResult::Message(message),
            } => {
                assert_eq!(message.delivery_attempt, Some(2));
                message.delivery_token.clone().unwrap()
            }
            response => panic!("unexpected redelivery response: {response:?}"),
        };
        assert_ne!(new_token, old_token);

        let stale_response = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(2, 2),
                    index: 1,
                },
                payload: EntryPayload::Normal(Command::AckGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-a".to_owned(),
                    offset: 0,
                    delivery_token: old_token,
                    now_ms: 200,
                }),
            }))
            .await
            .unwrap();
        assert_eq!(
            stale_response,
            vec![CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }]
        );

        let acknowledged_response = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(2, 2),
                    index: 2,
                },
                payload: EntryPayload::Normal(Command::AckGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-b".to_owned(),
                    offset: 0,
                    delivery_token: new_token,
                    now_ms: 200,
                }),
            }))
            .await
            .unwrap();
        assert_eq!(
            acknowledged_response,
            vec![CommandResponse::GroupAcknowledged]
        );
    }

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
    async fn single_node_raft_implements_shared_delivery_contract() {
        let engine = SingleNodeEngine::new(1).await.unwrap();
        runnel_test_support::assert_publish_batch_contract(&engine).await;
        runnel_test_support::assert_shared_delivery_contract(&engine).await;
    }

    #[tokio::test]
    async fn single_node_raft_implements_replay_contract() {
        let engine = SingleNodeEngine::new(1).await.unwrap();
        runnel_test_support::assert_replay_contract(&engine).await;
    }

    #[tokio::test]
    async fn persistent_raft_implements_shared_delivery_contract() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-group-contract-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        runnel_test_support::assert_publish_batch_contract(&engine).await;
        runnel_test_support::assert_shared_delivery_contract(&engine).await;
        runnel_test_support::assert_independent_consumers_contract(&engine).await;
        runnel_test_support::assert_key_ordering_contract(&engine).await;
    }

    #[tokio::test]
    async fn persistent_raft_implements_replay_contract() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-replay-contract-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();

        runnel_test_support::assert_replay_contract(&engine).await;
    }

    #[tokio::test]
    async fn persistent_raft_replay_and_ordinary_progress_survive_restart_independently() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-replay-restart-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        engine.create_stream("events").await.unwrap();
        engine
            .publish("events", None, b"first".to_vec(), None)
            .await
            .unwrap();
        engine
            .publish("events", None, b"second".to_vec(), None)
            .await
            .unwrap();

        assert_eq!(
            engine.replay("events", "worker", 1).await.unwrap().payload,
            b"second"
        );
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));
        assert_eq!(
            engine.ack("events", "worker", 0).await.unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            engine.replay("events", "worker", 0).await.unwrap().payload,
            b"first"
        );
        drop(engine);

        let reopened = PersistentEngine::open(
            1,
            "runnel-persistent-replay-restart-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            reopened
                .replay("events", "worker", 1)
                .await
                .unwrap()
                .payload,
            b"second"
        );
        assert!(matches!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 1, .. })
        ));
    }

    #[tokio::test]
    async fn persistent_raft_legacy_consumers_use_clustered_retry_policy() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open_with_config(
            1,
            "runnel-legacy-retry-contract-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
            Duration::from_millis(100),
            Some(2),
        )
        .await
        .unwrap();
        engine.create_stream("events").await.unwrap();
        engine
            .publish(
                "events",
                Some("poison".to_owned()),
                b"dead-letter-me".to_vec(),
                None,
            )
            .await
            .unwrap();

        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(1),
                ..
            })
        ));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(2),
                ..
            })
        ));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Empty
        );
        assert!(matches!(
            engine.poll("events.dead-letter", "inspector").await.unwrap(),
            PollResult::Message(Message {
                offset: 0,
                payload,
                delivery_attempt: Some(1),
                ..
            }) if payload == b"dead-letter-me"
        ));
        assert_eq!(
            engine
                .ack("events.dead-letter", "inspector", 0)
                .await
                .unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(engine.health().await.unwrap().dead_letters, 1);

        drop(engine);
        let reopened = PersistentEngine::open_with_config(
            1,
            "runnel-legacy-retry-contract-test".to_owned(),
            directory.path(),
            peers,
            true,
            Duration::from_millis(100),
            Some(2),
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Empty
        );
        assert_eq!(
            reopened
                .poll("events.dead-letter", "inspector")
                .await
                .unwrap(),
            PollResult::Empty
        );
    }

    #[tokio::test]
    async fn persistent_raft_fences_expired_group_delivery() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let ack_timeout = Duration::from_secs(2);
        let engine = PersistentEngine::open_with_ack_timeout(
            1,
            "runnel-group-expiry-test".to_owned(),
            directory.path(),
            peers,
            true,
            ack_timeout,
        )
        .await
        .unwrap();
        runnel_test_support::assert_expired_delivery_is_fenced(
            &engine,
            ack_timeout + Duration::from_millis(100),
        )
        .await;
    }

    #[tokio::test]
    async fn persistent_raft_recovers_group_delivery_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let ack_timeout = Duration::from_secs(2);
        let engine = PersistentEngine::open_with_ack_timeout(
            1,
            "runnel-group-restart-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
            ack_timeout,
        )
        .await
        .unwrap();
        engine.create_stream("jobs").await.unwrap();
        engine
            .publish("jobs", None, b"recover".to_vec(), None)
            .await
            .unwrap();
        let (old_token, old_attempt) = match engine
            .poll_group("jobs", "workers", "member-a")
            .await
            .unwrap()
        {
            PollResult::Message(message) => (
                message.delivery_token.unwrap(),
                message.delivery_attempt.unwrap(),
            ),
            PollResult::Empty => panic!("expected grouped delivery"),
        };
        assert_eq!(old_attempt, 1);
        drop(engine);

        tokio::time::sleep(ack_timeout + Duration::from_millis(100)).await;
        let reopened = PersistentEngine::open_with_ack_timeout(
            1,
            "runnel-group-restart-test".to_owned(),
            directory.path(),
            peers,
            true,
            ack_timeout,
        )
        .await
        .unwrap();
        let (new_token, new_attempt) = match reopened
            .poll_group("jobs", "workers", "member-b")
            .await
            .unwrap()
        {
            PollResult::Message(message) => (
                message.delivery_token.unwrap(),
                message.delivery_attempt.unwrap(),
            ),
            PollResult::Empty => panic!("expected redelivery after restart"),
        };
        assert_eq!(new_attempt, 2);
        assert_ne!(new_token, old_token);
        assert!(matches!(
            reopened
                .ack_group("jobs", "workers", "member-a", 0, &old_token)
                .await,
            Err(BrokerError::StaleDelivery { .. })
        ));
        assert_eq!(
            reopened
                .ack_group("jobs", "workers", "member-b", 0, &new_token)
                .await
                .unwrap(),
            AckResult::Acknowledged
        );
    }

    #[tokio::test]
    async fn persistent_raft_dead_letters_after_the_configured_attempt_limit() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let ack_timeout = Duration::from_secs(2);
        let engine = PersistentEngine::open_with_config(
            1,
            "runnel-group-dead-letter-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
            ack_timeout,
            Some(2),
        )
        .await
        .unwrap();
        engine.create_stream("events").await.unwrap();
        engine
            .publish(
                "events",
                Some("poison".to_owned()),
                b"do-not-process".to_vec(),
                None,
            )
            .await
            .unwrap();

        let first = engine
            .poll_group("events", "workers", "member-a")
            .await
            .unwrap();
        assert!(matches!(
            first,
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        tokio::time::sleep(ack_timeout + Duration::from_millis(100)).await;
        let second = engine
            .poll_group("events", "workers", "member-b")
            .await
            .unwrap();
        assert!(matches!(
            second,
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        tokio::time::sleep(ack_timeout + Duration::from_millis(100)).await;
        assert_eq!(
            engine
                .poll_group("events", "workers", "member-c")
                .await
                .unwrap(),
            PollResult::Empty
        );

        let dead_letter = engine
            .poll_group("events.dead-letter", "inspector", "member-a")
            .await
            .unwrap();
        let dead_letter_token = match dead_letter {
            PollResult::Message(message) => {
                assert_eq!(message.payload, b"do-not-process");
                assert_eq!(message.key.as_deref(), Some("poison"));
                assert_eq!(message.delivery_attempt, Some(1));
                message.delivery_token.unwrap()
            }
            PollResult::Empty => panic!("expected dead-letter message"),
        };
        assert_eq!(
            engine
                .ack_group(
                    "events.dead-letter",
                    "inspector",
                    "member-a",
                    0,
                    &dead_letter_token
                )
                .await
                .unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(engine.health().await.unwrap().dead_letters, 1);
        drop(engine);

        let reopened = PersistentEngine::open_with_config(
            1,
            "runnel-group-dead-letter-test".to_owned(),
            directory.path(),
            peers,
            true,
            ack_timeout,
            Some(2),
        )
        .await
        .unwrap();
        assert_eq!(
            reopened
                .poll_group("events", "workers", "member-d")
                .await
                .unwrap(),
            PollResult::Empty
        );
        assert_eq!(
            reopened
                .poll_group("events.dead-letter", "inspector", "member-b")
                .await
                .unwrap(),
            PollResult::Empty
        );
        assert_eq!(reopened.health().await.unwrap().dead_letters, 1);
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
                    node.state_machine.poll("events", "worker").await.unwrap(),
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
    async fn health_reports_in_flight_deliveries_until_group_acknowledged() {
        let cluster = InMemoryCluster::new([1]).await.unwrap();
        let leader = cluster.leader().await.unwrap();
        assert!(leader.create_stream("events".to_owned()).await.unwrap());
        leader
            .publish(
                "events".to_owned(),
                None,
                b"payload".to_vec(),
                now_ms(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(leader.health().await.in_flight_deliveries, 0);
        let message = match leader
            .poll_group(
                "events".to_owned(),
                "workers".to_owned(),
                "member-a".to_owned(),
            )
            .await
            .unwrap()
        {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected a message"),
        };
        assert_eq!(leader.health().await.in_flight_deliveries, 1);

        leader
            .ack_group(
                "events".to_owned(),
                "workers".to_owned(),
                "member-a".to_owned(),
                message.offset,
                message.delivery_token.expect("group delivery token"),
            )
            .await
            .unwrap();
        assert_eq!(leader.health().await.in_flight_deliveries, 0);
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
    async fn persisted_storage_rejects_cluster_identity_mismatch_without_rewriting_data() {
        let directory = tempfile::tempdir().unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);
        let engine = PersistentEngine::open(
            1,
            "runnel-persistent-identity-test".to_owned(),
            directory.path(),
            peers.clone(),
            true,
        )
        .await
        .unwrap();
        assert!(engine.create_stream("events").await.unwrap());
        assert_eq!(
            engine
                .publish("events", None, b"acknowledged".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        drop(engine);

        let metadata_path = directory.path().join(STORAGE_METADATA_FILE);
        let metadata_before = fs::read(&metadata_path).unwrap();
        let error = match PersistentEngine::open(
            1,
            "another-cluster".to_owned(),
            directory.path(),
            peers.clone(),
            false,
        )
        .await
        {
            Ok(_) => panic!("opening storage under another cluster identity must fail"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("cluster identity mismatch"));
        assert_eq!(fs::read(&metadata_path).unwrap(), metadata_before);

        let reopened = PersistentEngine::open(
            1,
            "runnel-persistent-identity-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        .unwrap();
        assert!(matches!(
            reopened.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message { offset: 0, payload, .. })
                if payload == b"acknowledged"
        ));
    }

    #[tokio::test]
    async fn state_machine_journal_replays_and_discards_a_partial_tail() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let store =
            Arc::new(StateMachineStore::open(&state_directory, GroupKind::Metadata).unwrap());
        let mut state_machine = store.clone();
        state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 0,
                },
                payload: EntryPayload::Normal(Command::CreateStream {
                    stream: "events".to_owned(),
                    stream_id: Some("stream/events".to_owned()),
                    group_id: Some("group/events/data".to_owned()),
                }),
            }))
            .await
            .unwrap();
        drop(state_machine);
        drop(store);

        let journal_path = state_directory.join(STATE_MACHINE_JOURNAL_FILE);
        let valid_journal_length = fs::metadata(&journal_path).unwrap().len();
        let mut journal = fs::OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .unwrap();
        journal.write_all(&[0x01, 0x02, 0x03]).unwrap();
        drop(journal);

        let reopened = StateMachineStore::open(&state_directory, GroupKind::Metadata).unwrap();
        assert_eq!(
            reopened.metadata("events").await.unwrap(),
            StreamMetadata {
                stream_id: "stream/events".to_owned(),
                group_id: "group/events/data".to_owned(),
                lifecycle: StreamLifecycle::Creating,
            }
        );
        assert_eq!(read_state_machine_journal(&journal_path).unwrap().len(), 1);
        assert_eq!(
            fs::metadata(&journal_path).unwrap().len(),
            valid_journal_length
        );
    }

    #[tokio::test]
    async fn state_machine_journal_replays_a_retained_batch_after_restart() {
        const RETAINED_MESSAGES: u64 = 256;
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let kind = data_group_kind("events");
        let store = Arc::new(StateMachineStore::open(&state_directory, kind.clone()).unwrap());
        let entries = (0..RETAINED_MESSAGES).map(|index| Entry {
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Normal(Command::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: format!("message-{index}").into_bytes(),
                published_at_ms: index,
                request_id: None,
            }),
        });
        let mut state_machine = store.clone();
        let responses = state_machine.apply(entries).await.unwrap();
        assert_eq!(responses.len(), RETAINED_MESSAGES as usize);
        drop(state_machine);
        drop(store);

        let reopened = StateMachineStore::open(&state_directory, kind).unwrap();
        let state = reopened.state.read().await;
        let messages = &state.state.streams.get("events").unwrap().messages;
        assert_eq!(messages.len(), RETAINED_MESSAGES as usize);
        assert!(messages.iter().enumerate().all(|(index, message)| {
            message.payload == format!("message-{index}").into_bytes()
        }));
    }

    #[test]
    fn invalid_persisted_state_machine_is_rejected_with_file_context() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        fs::write(
            state_directory.join("state-machine.json"),
            b"not-a-state-machine",
        )
        .unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("invalid persisted state-machine"));
        assert!(error.contains("state-machine.json"));
    }

    #[tokio::test]
    async fn legacy_cluster_layout_is_rejected_without_creating_new_layout() {
        let directory = tempfile::tempdir().unwrap();
        let legacy_log = directory.path().join("raft-log.json");
        fs::write(&legacy_log, b"legacy acknowledged data").unwrap();
        let legacy_state = directory.path().join("state-machine");
        fs::create_dir_all(&legacy_state).unwrap();
        let legacy_state_marker = legacy_state.join("state-machine.json");
        fs::write(&legacy_state_marker, b"legacy state-machine data").unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-legacy-layout-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("legacy clustered storage must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("legacy single-group storage detected"));
        assert_eq!(fs::read(&legacy_log).unwrap(), b"legacy acknowledged data");
        assert_eq!(
            fs::read(&legacy_state_marker).unwrap(),
            b"legacy state-machine data"
        );
        assert!(!directory.path().join(STORAGE_METADATA_FILE).exists());
        assert!(!directory.path().join("groups").exists());
    }

    #[tokio::test]
    async fn legacy_root_checkpoint_is_rejected_without_creating_new_layout() {
        let directory = tempfile::tempdir().unwrap();
        let legacy_checkpoint = directory.path().join("state-machine.json");
        fs::write(&legacy_checkpoint, b"legacy state-machine data").unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-legacy-root-checkpoint-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("legacy root checkpoint must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("legacy single-group storage detected"));
        assert!(error.contains(legacy_checkpoint.to_str().unwrap()));
        assert_eq!(
            fs::read(&legacy_checkpoint).unwrap(),
            b"legacy state-machine data"
        );
        assert!(!directory.path().join(STORAGE_METADATA_FILE).exists());
        assert!(!directory.path().join("groups").exists());
    }

    #[tokio::test]
    async fn unsupported_storage_metadata_version_is_rejected_before_opening_groups() {
        let directory = tempfile::tempdir().unwrap();
        let metadata_path = directory.path().join(STORAGE_METADATA_FILE);
        fs::write(
            &metadata_path,
            serde_json::to_vec(&serde_json::json!({
                "version": STORAGE_METADATA_FORMAT_VERSION + 1,
                "cluster_name": "runnel-version-test",
                "node_id": 1,
            }))
            .unwrap(),
        )
        .unwrap();
        let metadata_before = fs::read(&metadata_path).unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-version-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("unsupported storage metadata must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("unsupported storage metadata format version"));
        assert_eq!(fs::read(&metadata_path).unwrap(), metadata_before);
        assert!(!directory.path().join("groups").exists());
    }

    #[tokio::test]
    async fn unsupported_data_group_log_is_rejected_before_opening_new_groups() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join(STORAGE_METADATA_FILE),
            serde_json::to_vec(&PersistedStorageMetadata {
                version: STORAGE_METADATA_FORMAT_VERSION,
                cluster_name: "runnel-data-group-version-test".to_owned(),
                node_id: 1,
            })
            .unwrap(),
        )
        .unwrap();
        let data_group_directory = directory
            .path()
            .join("groups/data")
            .join(path_component("events"));
        fs::create_dir_all(&data_group_directory).unwrap();
        let manifest_path = data_group_directory.join("group.json");
        let manifest = DataGroupManifest {
            stream: "events".to_owned(),
            stream_id: "stream/events".to_owned(),
            group_id: "group/events/data".to_owned(),
        };
        let manifest_before = serde_json::to_vec(&manifest).unwrap();
        fs::write(&manifest_path, &manifest_before).unwrap();
        let log_path = data_group_directory.join("raft-log.json");
        let log_before = serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "last_purged_log_id": null,
            "log": {},
            "committed": null,
            "vote": null,
        }))
        .unwrap();
        fs::write(&log_path, &log_before).unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-data-group-version-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("unsupported data-group log must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("unsupported log format version"));
        assert!(error.contains(log_path.to_str().unwrap()));
        assert_eq!(fs::read(&manifest_path).unwrap(), manifest_before);
        assert_eq!(fs::read(&log_path).unwrap(), log_before);
        assert!(!directory.path().join("groups/metadata").exists());
    }

    #[test]
    fn unsupported_state_machine_checkpoint_is_rejected_without_creating_journal() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let state_path = state_directory.join("state-machine.json");
        let state_before = serde_json::to_vec(&serde_json::json!({
            "version": FORMAT_VERSION + 1,
            "last_applied_log": null,
            "last_membership": serde_json::to_value(
                StoredMembership::<NodeId, BasicNode>::default()
            )
            .unwrap(),
            "streams": {},
            "consumers": [],
        }))
        .unwrap();
        fs::write(&state_path, &state_before).unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported state-machine format version"));
        assert!(error.contains(state_path.to_str().unwrap()));
        assert_eq!(fs::read(&state_path).unwrap(), state_before);
        assert!(!state_directory.join(STATE_MACHINE_JOURNAL_FILE).exists());
    }

    #[test]
    fn unsupported_state_machine_journal_is_rejected_without_truncating() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let journal_path = state_directory.join(STATE_MACHINE_JOURNAL_FILE);
        let record = serde_json::to_vec(&StateMachineJournalEntry {
            version: STATE_MACHINE_JOURNAL_FORMAT_VERSION + 1,
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 0,
            },
            payload: EntryPayload::Blank,
        })
        .unwrap();
        let mut journal_before = (record.len() as u32).to_le_bytes().to_vec();
        journal_before.extend_from_slice(&record);
        fs::write(&journal_path, &journal_before).unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported state-machine journal format version"));
        assert!(error.contains(journal_path.to_str().unwrap()));
        assert_eq!(fs::read(&journal_path).unwrap(), journal_before);
        assert!(!state_directory.join("state-machine.json").exists());
    }

    #[test]
    fn unsupported_snapshot_version_is_rejected_without_creating_journal() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let snapshot_path = state_directory.join("snapshot.json");
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "unsupported".to_owned(),
            },
            data: serde_json::to_vec(&serde_json::json!({
                "version": FORMAT_VERSION + 1,
                "streams": {},
                "consumers": [],
            }))
            .unwrap(),
        };
        let snapshot_before = serde_json::to_vec(&snapshot).unwrap();
        fs::write(&snapshot_path, &snapshot_before).unwrap();

        let error = StateMachineStore::open(&state_directory, GroupKind::Metadata)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported snapshot format version"));
        assert!(error.contains(snapshot_path.to_str().unwrap()));
        assert_eq!(fs::read(&snapshot_path).unwrap(), snapshot_before);
        assert!(!state_directory.join(STATE_MACHINE_JOURNAL_FILE).exists());
    }

    #[tokio::test]
    async fn unmarked_clustered_layout_is_rejected_without_guessing_identity() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("groups").join("acknowledged.data");
        fs::create_dir_all(marker.parent().unwrap()).unwrap();
        fs::write(&marker, b"acknowledged data").unwrap();
        let peers = BTreeMap::from([(1, "127.0.0.1:0".to_owned())]);

        let error = match PersistentEngine::open(
            1,
            "runnel-unmarked-layout-test".to_owned(),
            directory.path(),
            peers,
            true,
        )
        .await
        {
            Ok(_) => panic!("unmarked clustered storage must be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("missing storage metadata"));
        assert!(!directory.path().join(STORAGE_METADATA_FILE).exists());
        assert_eq!(fs::read(marker).unwrap(), b"acknowledged data");
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
        let journal_path = directory
            .path()
            .join("groups/data")
            .join(path_component("events"))
            .join("state-machine/state-machine.log");
        assert!(journal_path.exists());

        let snapshot_path = directory
            .path()
            .join("groups/data")
            .join(path_component("events"))
            .join("state-machine/snapshot.json");
        assert!(snapshot_path.exists());
        let snapshot: StoredSnapshot =
            serde_json::from_slice(&fs::read(&snapshot_path).unwrap()).unwrap();
        let remaining_journal = read_state_machine_journal(&journal_path).unwrap();
        assert!(remaining_journal.iter().all(|entry| {
            snapshot
                .meta
                .last_log_id
                .is_none_or(|last| is_log_after(entry.log_id, last))
        }));
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

    #[tokio::test]
    async fn retained_history_survives_snapshot_install_and_reopen() {
        const RETAINED_MESSAGES: u64 = 256;
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("source-state-machine");
        let kind = GroupKind::Data {
            stream: "events".to_owned(),
            stream_id: "stream/events".to_owned(),
            group_id: "group/events/data".to_owned(),
        };
        let source = Arc::new(StateMachineStore::open(&state_directory, kind.clone()).unwrap());
        let entries = (0..RETAINED_MESSAGES).map(|index| Entry {
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Normal(Command::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: format!("message-{index}").into_bytes(),
                published_at_ms: index,
                request_id: None,
            }),
        });
        let mut source_machine = source.clone();
        let responses = source_machine.apply(entries).await.unwrap();
        assert_eq!(responses.len(), RETAINED_MESSAGES as usize);

        let mut snapshot_builder = source.clone();
        let snapshot = snapshot_builder.build_snapshot().await.unwrap();
        let snapshot_state =
            validate_snapshot_data(&snapshot.snapshot.clone().into_inner()).unwrap();
        let snapshot_messages = match snapshot_state.streams.get("events").unwrap() {
            PersistedStreamData::Current(stream) => &stream.messages,
            PersistedStreamData::Legacy(_) => panic!("new snapshots must use current streams"),
        };
        assert_eq!(snapshot_messages.len(), RETAINED_MESSAGES as usize);
        assert_eq!(
            snapshot_messages.last().unwrap().payload,
            format!("message-{}", RETAINED_MESSAGES - 1).into_bytes()
        );

        let installed_directory = directory.path().join("installed-state-machine");
        let installed =
            Arc::new(StateMachineStore::open(&installed_directory, kind.clone()).unwrap());
        let snapshot_meta = snapshot.meta.clone();
        let snapshot_data = snapshot.snapshot;
        let mut installed_machine = installed.clone();
        installed_machine
            .install_snapshot(&snapshot_meta, snapshot_data)
            .await
            .unwrap();
        drop(installed_machine);
        drop(installed);

        let reopened_source = StateMachineStore::open(&state_directory, kind.clone()).unwrap();
        let reopened_installed = StateMachineStore::open(&installed_directory, kind).unwrap();
        for store in [&reopened_source, &reopened_installed] {
            let state = store.state.read().await;
            let messages = &state.state.streams.get("events").unwrap().messages;
            assert_eq!(messages.len(), RETAINED_MESSAGES as usize);
            assert_eq!(messages.first().unwrap().payload, b"message-0");
            assert_eq!(
                messages.last().unwrap().payload,
                format!("message-{}", RETAINED_MESSAGES - 1).into_bytes()
            );
        }
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
    async fn legacy_state_machine_format_recovers_metadata_messages_and_progress() {
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
            "streams": {"events": [
                {"key": null, "payload": [115, 107, 105, 112], "published_at_ms": 1},
                {"key": "key", "payload": [114, 101, 99, 111, 118, 101, 114], "published_at_ms": 2}
            ]},
            "consumers": [{"stream": "events", "consumer": "worker", "offset": 1}]
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
        assert!(matches!(
            store.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message {
                offset: 1,
                key: Some(key),
                payload,
                ..
            }) if key == "key" && payload == b"recover"
        ));
    }

    #[tokio::test]
    async fn legacy_snapshot_format_recovers_metadata_messages_and_progress() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let snapshot_path = state_directory.join("snapshot.json");
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: Some(LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 1,
                }),
                last_membership: StoredMembership::default(),
                snapshot_id: "legacy".to_owned(),
            },
            data: serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "streams": {"events": [
                    {"key": null, "payload": [115, 107, 105, 112], "published_at_ms": 1},
                    {"key": "key", "payload": [114, 101, 99, 111, 118, 101, 114], "published_at_ms": 2}
                ]},
                "consumers": [{"stream": "events", "consumer": "worker", "offset": 1}]
            }))
            .unwrap(),
        };
        let snapshot_before = serde_json::to_vec(&snapshot).unwrap();
        fs::write(&snapshot_path, &snapshot_before).unwrap();

        let store = StateMachineStore::open(&state_directory, GroupKind::Metadata).unwrap();
        assert_eq!(fs::read(&snapshot_path).unwrap(), snapshot_before);
        assert_eq!(
            store.metadata("events").await.unwrap(),
            StreamMetadata {
                stream_id: "stream/events".to_owned(),
                group_id: "group/events/data".to_owned(),
                lifecycle: StreamLifecycle::Active,
            }
        );
        assert!(matches!(
            store.poll("events", "worker").await.unwrap(),
            PollResult::Message(Message {
                offset: 1,
                key: Some(key),
                payload,
                ..
            }) if key == "key" && payload == b"recover"
        ));
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
                LogId::default(),
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
                LogId::default(),
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
                LogId::default(),
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
