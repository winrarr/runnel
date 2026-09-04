use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{BrokerError, Offset};
#[cfg(test)]
use runnel_engine::{Message, PollResult};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::delivery::{GroupConsumerState, dead_letter_stream_name};
use super::state_machine::{
    CommandResponse, GroupKind, SnapshotState, StateMachineData, StoredMessage, StreamLifecycle,
    StreamMetadata, StreamState, apply_command, stream_identity,
};
use super::state_machine_journal::{
    FILE as STATE_MACHINE_JOURNAL_FILE, JournalEntryRef as StateMachineJournalEntryRef,
    append as append_state_machine_journal_entry, is_log_after, read as read_state_machine_journal,
    replay as replay_state_machine_journal, validate as validate_state_machine_journal,
};
use super::{FORMAT_VERSION, TypeConfig, atomic_write};
use crate::NodeId;

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
pub(super) struct PersistedSnapshotState {
    #[serde(default = "legacy_format_version")]
    pub(super) version: u32,
    pub(super) streams: BTreeMap<String, PersistedStreamData>,
    pub(super) consumers: Vec<PersistedConsumer>,
    #[serde(default)]
    pub(super) group_consumers: Vec<PersistedGroupConsumer>,
    #[serde(default)]
    pub(super) lease_clock_ms: u64,
    #[serde(default)]
    pub(super) dedup: BTreeMap<String, BTreeMap<String, Offset>>,
    #[serde(default)]
    pub(super) redeliveries: u64,
    #[serde(default)]
    pub(super) dead_letters: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum PersistedStreamData {
    Legacy(Vec<StoredMessage>),
    Current(PersistedStream),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedStream {
    #[serde(default)]
    stream_id: String,
    #[serde(default)]
    group_id: String,
    #[serde(default)]
    lifecycle: Option<StreamLifecycle>,
    pub(super) messages: Vec<StoredMessage>,
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
pub(super) struct PersistedSnapshotStateRef<'a> {
    version: u32,
    #[serde(flatten)]
    body: PersistedStateBodyRef<'a>,
}

impl<'a> PersistedSnapshotStateRef<'a> {
    pub(super) fn new(state: &'a SnapshotState) -> Self {
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
pub(super) struct PersistedConsumer {
    stream: String,
    consumer: String,
    offset: Offset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedGroupConsumer {
    stream: String,
    consumer: String,
    state: GroupConsumerState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredSnapshot {
    pub(super) meta: SnapshotMeta<NodeId, BasicNode>,
    pub(super) data: Vec<u8>,
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
pub(super) struct StateMachineStore {
    pub(super) state: RwLock<StateMachineData>,
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
    pub(super) fn open(path: impl AsRef<Path>, kind: GroupKind) -> Result<Self, BrokerError> {
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
    pub(super) async fn poll(
        &self,
        stream: &str,
        consumer: &str,
    ) -> Result<PollResult, BrokerError> {
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

    pub(super) async fn metadata(&self, stream: &str) -> Result<StreamMetadata, BrokerError> {
        let state = self.state.read().await;
        state
            .state
            .streams
            .get(stream)
            .map(|stream_state| stream_state.metadata(stream))
            .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))
    }

    pub(super) async fn metadata_by_group_id(
        &self,
        group_id: &str,
    ) -> Option<(String, StreamMetadata)> {
        let state = self.state.read().await;
        state
            .state
            .streams
            .iter()
            .find(|(_, stream_state)| stream_state.group_id == group_id)
            .map(|(stream, stream_state)| (stream.clone(), stream_state.metadata(stream)))
    }

    pub(super) async fn dead_letter_source(
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

    pub(super) async fn health(&self) -> runnel_engine::HealthSnapshot {
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

    pub(super) fn snapshot_metrics(&self) -> SnapshotMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub(super) fn record_snapshot_chunk(&self, bytes: u64, final_chunk: bool) {
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

pub(super) fn validate_state_machine_storage(path: &Path) -> Result<(), BrokerError> {
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

pub(super) fn validate_snapshot_data(
    data: &[u8],
) -> Result<PersistedSnapshotState, std::io::Error> {
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

pub(super) fn snapshot_state_from_persisted(persisted: PersistedSnapshotState) -> SnapshotState {
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

fn legacy_format_version() -> u32 {
    1
}
