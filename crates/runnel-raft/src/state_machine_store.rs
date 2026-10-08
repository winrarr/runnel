use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{BrokerError, ConsumerPolicy, Offset};
#[cfg(test)]
use runnel_engine::{Message, PollResult};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock, futures::Notified};

use super::delivery::{
    GroupBatchPollRequest, GroupConsumerState, dead_letter_stream_name, group_policy_for_offset,
    preview_group_batch,
};
use super::state_machine::{
    CommandResponse, GroupKind, SnapshotState, StateMachineData, StoredMessage, StreamLifecycle,
    StreamMetadata, StreamState, apply_command,
};
use super::state_machine_journal::{
    FILE as STATE_MACHINE_JOURNAL_FILE, JournalEntryRef as StateMachineJournalEntryRef,
    append as append_state_machine_journal_entry, is_log_after, read as read_state_machine_journal,
    replay as replay_state_machine_journal, validate as validate_state_machine_journal,
};
use super::{
    FORMAT_VERSION, PersistenceWriteOperation, PersistenceWriteRole, TypeConfig,
    atomic_write_with_role, persistence_write,
};
use crate::NodeId;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedState {
    version: u32,
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    streams: BTreeMap<String, PersistedStream>,
    consumers: Vec<PersistedConsumer>,
    group_consumers: Vec<PersistedGroupConsumer>,
    lease_clock_ms: u64,
    dedup: BTreeMap<String, BTreeMap<String, Offset>>,
    redeliveries: u64,
    dead_letters: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedSnapshotState {
    pub(super) version: u32,
    pub(super) streams: BTreeMap<String, PersistedStream>,
    pub(super) consumers: Vec<PersistedConsumer>,
    pub(super) group_consumers: Vec<PersistedGroupConsumer>,
    pub(super) lease_clock_ms: u64,
    pub(super) dedup: BTreeMap<String, BTreeMap<String, Offset>>,
    pub(super) redeliveries: u64,
    pub(super) dead_letters: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedStream {
    stream_id: String,
    group_id: String,
    lifecycle: StreamLifecycle,
    pub(super) messages: Vec<StoredMessage>,
}

impl PersistedStream {
    fn into_state(self) -> StreamState {
        StreamState {
            stream_id: self.stream_id,
            group_id: self.group_id,
            lifecycle: self.lifecycle,
            messages: self.messages,
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
    builds_in_progress: AtomicU64,
    build_duration_nanos_sum: AtomicU64,
    build_duration_count: AtomicU64,
    build_duration_nanos_max: AtomicU64,
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
    pub builds_in_progress: u64,
    pub build_duration_nanos_sum: u64,
    pub build_duration_count: u64,
    pub build_duration_nanos_max: u64,
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
            builds_in_progress: self.builds_in_progress.load(Ordering::Relaxed),
            build_duration_nanos_sum: self.build_duration_nanos_sum.load(Ordering::Relaxed),
            build_duration_count: self.build_duration_count.load(Ordering::Relaxed),
            build_duration_nanos_max: self.build_duration_nanos_max.load(Ordering::Relaxed),
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

struct SnapshotBuildAttempt<'a> {
    metrics: &'a SnapshotMetrics,
    started_at: Instant,
    in_progress: bool,
}

impl<'a> SnapshotBuildAttempt<'a> {
    fn start(metrics: &'a SnapshotMetrics) -> Self {
        metrics.builds_started.fetch_add(1, Ordering::Relaxed);
        metrics.builds_in_progress.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            started_at: Instant::now(),
            in_progress: true,
        }
    }

    fn finish(mut self) {
        let elapsed_nanos = u64::try_from(self.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.metrics
            .build_duration_nanos_sum
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(elapsed_nanos))
            })
            .expect("saturating snapshot duration update always succeeds");
        self.metrics
            .build_duration_count
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .build_duration_nanos_max
            .fetch_max(elapsed_nanos, Ordering::Relaxed);
        self.metrics
            .builds_in_progress
            .fetch_sub(1, Ordering::Relaxed);
        self.in_progress = false;
    }
}

impl Drop for SnapshotBuildAttempt<'_> {
    fn drop(&mut self) {
        if self.in_progress {
            self.metrics
                .builds_in_progress
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct StateMachineStore {
    pub(super) state: RwLock<StateMachineData>,
    changes: Notify,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    #[cfg(test)]
    fail_next_checkpoint_persist: AtomicBool,
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
    if persisted.version != FORMAT_VERSION {
        return Err(BrokerError::Cluster(format!(
            "unsupported state-machine format version {} in '{}' (checkpoint; supported version {})",
            persisted.version,
            path.display(),
            FORMAT_VERSION
        )));
    }
    validate_group_consumer_entries(persisted.group_consumers.iter().map(|consumer| {
        (
            consumer.stream.as_str(),
            consumer.consumer.as_str(),
            &consumer.state,
        )
    }))
    .map_err(|error| {
        BrokerError::Cluster(format!(
            "invalid persisted state-machine '{}': {error}",
            path.display()
        ))
    })?;
    Ok(Some(persisted))
}

fn validate_group_consumer_entries<'a>(
    entries: impl IntoIterator<Item = (&'a str, &'a str, &'a GroupConsumerState)>,
) -> Result<(), String> {
    for (stream, consumer, state) in entries {
        state
            .validate_pinned_policies()
            .map_err(|error| format!("consumer '{stream}/{consumer}': {error}"))?;
    }
    Ok(())
}

fn state_machine_data_from_persisted(persisted: PersistedState) -> StateMachineData {
    StateMachineData {
        last_applied_log: persisted.last_applied_log,
        last_membership: persisted.last_membership,
        state: SnapshotState {
            streams: persisted
                .streams
                .into_iter()
                .map(|(stream, persisted)| (stream, persisted.into_state()))
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
        validate_group_consumer_entries(
            state
                .state
                .group_consumers
                .iter()
                .map(|((stream, consumer), state)| (stream.as_str(), consumer.as_str(), state)),
        )
        .map_err(|error| {
            BrokerError::Cluster(format!(
                "invalid recovered state-machine '{}': {error}",
                path.display()
            ))
        })?;
        fs::create_dir_all(&path)?;
        let journal = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&journal_path)?;
        Ok(Self {
            state: RwLock::new(state),
            changes: Notify::new(),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(current_snapshot),
            #[cfg(test)]
            fail_next_checkpoint_persist: AtomicBool::new(false),
            path: Some(path),
            journal: Some(StdMutex::new(journal)),
            kind,
            metrics: Arc::new(SnapshotMetrics::default()),
        })
    }

    pub(super) fn changed(&self) -> Notified<'_> {
        self.changes.notified()
    }

    pub(super) async fn preview_group_batch(
        &self,
        request: GroupBatchPollRequest,
    ) -> Result<(CommandResponse, Option<u64>, bool), BrokerError> {
        let state = self.state.read().await;
        let effective_now_ms = state.state.lease_clock_ms.max(request.now_ms);
        let consumer = state
            .state
            .group_consumers
            .get(&(request.stream.clone(), request.consumer.clone()));
        let next_lease_expiry_ms = consumer.and_then(|consumer| {
            consumer
                .in_flight
                .values()
                .map(|delivery| delivery.deadline_ms)
                .filter(|deadline| *deadline > effective_now_ms)
                .min()
        });
        let next_retry_ms = consumer.and_then(|consumer| {
            consumer
                .retry_not_before
                .values()
                .map(|schedule| schedule.retry_not_before_ms)
                .filter(|deadline| *deadline > effective_now_ms)
                .min()
        });
        let next_expiry_ms = next_lease_expiry_ms.into_iter().chain(next_retry_ms).min();
        let legacy_policy = ConsumerPolicy::legacy(
            request.legacy_ack_timeout_ms.unwrap_or_default(),
            request.max_delivery_attempts,
        );
        let needs_retry_schedule = consumer.is_some_and(|consumer| {
            consumer.in_flight.iter().any(|(offset, delivery)| {
                if delivery.deadline_ms > effective_now_ms
                    || consumer.retry_not_before.contains_key(offset)
                {
                    return false;
                }
                let attempts = consumer
                    .delivery_attempts
                    .get(offset)
                    .copied()
                    .unwrap_or_default();
                let policy = group_policy_for_offset(
                    consumer,
                    *offset,
                    attempts,
                    &legacy_policy,
                    request.policy_version,
                );
                policy.retry_delay_ms > 0
                    && !policy
                        .max_delivery_attempts
                        .is_some_and(|maximum| attempts >= maximum)
            })
        });
        let response = preview_group_batch(&state.state, request, &self.kind);
        Ok((response, next_expiry_ms, needs_retry_schedule))
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
            append_state_machine_journal_entry(
                &mut journal,
                &journal_entry,
                PersistenceWriteRole::StateMachineJournalAppend,
            )
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        }
        persistence_write::measure_io(
            PersistenceWriteRole::StateMachineJournalAppend,
            PersistenceWriteOperation::SyncData,
            0,
            || journal.sync_data(),
        )
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
        let bytes = persistence_write::serialize_json(
            PersistenceWriteRole::StateMachineCheckpoint,
            PersistenceWriteOperation::Serialize,
            &persisted,
        )
        .map_err(|error| StorageIOError::write_state_machine(&error))?;
        #[cfg(test)]
        if self
            .fail_next_checkpoint_persist
            .swap(false, Ordering::Relaxed)
        {
            let error = std::io::Error::other("injected checkpoint persistence failure");
            return Err(StorageError::IO {
                source: StorageIOError::write_state_machine(&error),
            });
        }
        atomic_write_with_role(
            &path.join("state-machine.json"),
            &bytes,
            PersistenceWriteRole::StateMachineCheckpoint,
        )
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
            append_state_machine_journal_entry(
                &mut temporary,
                entry,
                PersistenceWriteRole::StateMachineJournalCompaction,
            )
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        }
        persistence_write::measure_io(
            PersistenceWriteRole::StateMachineJournalCompaction,
            PersistenceWriteOperation::SyncAll,
            0,
            || temporary.sync_all(),
        )
        .map_err(|error| StorageIOError::write_state_machine(&error))?;
        persistence_write::measure_io(
            PersistenceWriteRole::StateMachineJournalCompaction,
            PersistenceWriteOperation::Rename,
            0,
            || fs::rename(&temporary_path, &journal_path),
        )
        .map_err(|error| StorageIOError::write_state_machine(&error))?;
        let parent = journal_path.parent().unwrap_or_else(|| Path::new("."));
        let directory =
            fs::File::open(parent).map_err(|error| StorageIOError::write_state_machine(&error))?;
        persistence_write::measure_io(
            PersistenceWriteRole::StateMachineJournalCompaction,
            PersistenceWriteOperation::DirectorySync,
            0,
            || directory.sync_all(),
        )
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
        let bytes = persistence_write::serialize_json(
            PersistenceWriteRole::StateMachineSnapshot,
            PersistenceWriteOperation::Serialize,
            snapshot,
        )
        .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
        atomic_write_with_role(
            &path.join("snapshot.json"),
            &bytes,
            PersistenceWriteRole::StateMachineSnapshot,
        )
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

    pub(super) async fn consumer_policy(
        &self,
        stream: &str,
        consumer: &str,
        legacy: ConsumerPolicy,
    ) -> Result<ConsumerPolicy, BrokerError> {
        let state = self.state.read().await;
        if !state
            .state
            .streams
            .get(stream)
            .is_some_and(StreamState::is_active)
        {
            return Err(BrokerError::StreamNotFound(stream.to_owned()));
        }
        Ok(state
            .state
            .group_consumers
            .get(&(stream.to_owned(), consumer.to_owned()))
            .and_then(|state| state.policy.clone())
            .unwrap_or(legacy))
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
        let build_attempt = SnapshotBuildAttempt::start(&self.metrics);
        let result = async {
            let (data, last_applied_log, last_membership) = {
                let state = self.state.read().await;
                // Keep the read guard through encoding so the snapshot is coherent without
                // materializing a second copy of every retained message.
                let snapshot_state = PersistedSnapshotStateRef::new(&state.state);
                let data = persistence_write::serialize_json(
                    PersistenceWriteRole::StateMachineSnapshot,
                    PersistenceWriteOperation::SnapshotStateSerialize,
                    &snapshot_state,
                )
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
        build_attempt.finish();
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
        let changed = !entries.is_empty();
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
        drop(state);
        if changed {
            self.changes.notify_waiters();
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
            let next_state = StateMachineData {
                last_applied_log: meta.last_log_id,
                last_membership: meta.last_membership.clone(),
                state: snapshot_state,
            };
            let stored_snapshot = StoredSnapshot {
                meta: meta.clone(),
                data,
            };

            // Keep the in-memory state and snapshot cache unchanged until all durable writes
            // succeed. The owned snapshot can then move into the cache without cloning its data.
            self.persist_snapshot(&stored_snapshot).await?;
            self.persist_checkpoint(&next_state)?;
            self.compact_journal(next_state.last_applied_log)?;
            *state = next_state;
            drop(state);
            *self.current_snapshot.write().await = Some(stored_snapshot);
            self.changes.notify_waiters();
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
    if persisted.version != FORMAT_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unsupported snapshot format version {} (supported version {})",
                persisted.version, FORMAT_VERSION
            ),
        ));
    }
    validate_group_consumer_entries(persisted.group_consumers.iter().map(|consumer| {
        (
            consumer.stream.as_str(),
            consumer.consumer.as_str(),
            &consumer.state,
        )
    }))
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(persisted)
}

pub(super) fn snapshot_state_from_persisted(persisted: PersistedSnapshotState) -> SnapshotState {
    SnapshotState {
        streams: persisted
            .streams
            .into_iter()
            .map(|(stream, persisted)| (stream, persisted.into_state()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftStateMachine;

    #[tokio::test]
    async fn snapshot_build_metrics_record_success_and_clear_progress_gauge() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(StateMachineStore::open(directory.path(), GroupKind::Metadata).unwrap());
        let mut builder = store.clone();

        builder.build_snapshot().await.unwrap();

        let metrics = store.snapshot_metrics();
        assert_eq!(metrics.builds_started, 1);
        assert_eq!(metrics.builds_completed, 1);
        assert_eq!(metrics.build_failures, 0);
        assert_eq!(metrics.builds_in_progress, 0);
        assert_eq!(metrics.build_duration_count, 1);
        assert!(metrics.build_duration_nanos_sum > 0);
        assert_eq!(
            metrics.build_duration_nanos_max,
            metrics.build_duration_nanos_sum
        );
    }

    #[tokio::test]
    async fn failed_snapshot_build_records_duration_and_clears_progress_gauge() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            Arc::new(StateMachineStore::open(directory.path(), GroupKind::Metadata).unwrap());
        fs::create_dir(directory.path().join("snapshot.json")).unwrap();
        let mut builder = store.clone();

        assert!(builder.build_snapshot().await.is_err());

        let metrics = store.snapshot_metrics();
        assert_eq!(metrics.builds_started, 1);
        assert_eq!(metrics.builds_completed, 0);
        assert_eq!(metrics.build_failures, 1);
        assert_eq!(metrics.builds_in_progress, 0);
        assert_eq!(metrics.build_duration_count, 1);
        assert!(metrics.build_duration_nanos_sum > 0);
        assert_eq!(
            metrics.build_duration_nanos_max,
            metrics.build_duration_nanos_sum
        );
    }

    fn snapshot_meta(index: u64, snapshot_id: &str) -> SnapshotMeta<NodeId, BasicNode> {
        SnapshotMeta {
            last_log_id: Some(LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index,
            }),
            last_membership: StoredMembership::default(),
            snapshot_id: snapshot_id.to_owned(),
        }
    }

    fn snapshot_data(payload: &[u8]) -> Vec<u8> {
        let mut state = SnapshotState::default();
        state.streams.insert(
            "events".to_owned(),
            StreamState {
                stream_id: "stream/events".to_owned(),
                group_id: "group/events/data".to_owned(),
                lifecycle: StreamLifecycle::Active,
                messages: vec![StoredMessage {
                    key: Some("key".to_owned()),
                    payload: payload.to_vec(),
                    published_at_ms: 1,
                }],
            },
        );
        serde_json::to_vec(&PersistedSnapshotStateRef::new(&state)).unwrap()
    }

    #[test]
    fn attempted_offset_without_pinned_policy_fails_closed_without_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let mut state = SnapshotState::default();
        let mut consumer_state = GroupConsumerState::default();
        consumer_state.delivery_attempts.insert(0, 1);
        state
            .group_consumers
            .insert(("events".to_owned(), "workers".to_owned()), consumer_state);
        let data = serde_json::to_vec(&PersistedSnapshotStateRef::new(&state)).unwrap();
        let snapshot_path = state_directory.join("snapshot.json");
        let snapshot = serde_json::to_vec(&StoredSnapshot {
            meta: snapshot_meta(1, "missing-pinned-policy"),
            data,
        })
        .unwrap();
        fs::write(&snapshot_path, &snapshot).unwrap();

        let Err(error) = StateMachineStore::open(&state_directory, GroupKind::Combined) else {
            panic!("attempt without a pinned policy must be rejected");
        };

        assert!(error.to_string().contains("attempts and pinned policies"));
        assert_eq!(fs::read(snapshot_path).unwrap(), snapshot);
        assert!(!state_directory.join("state-machine.json").exists());
        assert!(!state_directory.join(STATE_MACHINE_JOURNAL_FILE).exists());
    }

    #[tokio::test]
    async fn failed_snapshot_persistence_keeps_previous_state_and_recovers_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let store =
            Arc::new(StateMachineStore::open(&state_directory, GroupKind::Combined).unwrap());

        let first_meta = snapshot_meta(1, "first");
        let first_data = snapshot_data(b"first");
        let mut state_machine = store.clone();
        state_machine
            .install_snapshot(&first_meta, Box::new(Cursor::new(first_data.clone())))
            .await
            .unwrap();

        let snapshot_path = state_directory.join("snapshot.json");
        fs::remove_file(&snapshot_path).unwrap();
        fs::create_dir(&snapshot_path).unwrap();

        let second_meta = snapshot_meta(2, "second");
        let second_data = snapshot_data(b"second");
        let error = state_machine
            .install_snapshot(&second_meta, Box::new(Cursor::new(second_data)))
            .await;
        assert!(error.is_err());

        {
            let state = store.state.read().await;
            let message = &state.state.streams["events"].messages[0];
            assert_eq!(message.payload, b"first");
        }
        let current = state_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("failed install must not publish a new current snapshot");
        assert_eq!(current.meta, first_meta);
        assert_eq!(current.snapshot.into_inner(), first_data);

        drop(state_machine);
        drop(store);
        fs::remove_dir(&snapshot_path).unwrap();

        let reopened = StateMachineStore::open(&state_directory, GroupKind::Combined).unwrap();
        let state = reopened.state.read().await;
        let message = &state.state.streams["events"].messages[0];
        assert_eq!(message.payload, b"first");
    }

    #[tokio::test]
    async fn retry_deadline_survives_cluster_snapshot_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let store =
            Arc::new(StateMachineStore::open(&state_directory, GroupKind::Combined).unwrap());
        let mut state_machine = store.clone();
        state_machine
            .install_snapshot(
                &snapshot_meta(1, "retry-delay-seed"),
                Box::new(Cursor::new(snapshot_data(b"retry-me"))),
            )
            .await
            .unwrap();

        let configured = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 2,
                },
                payload: EntryPayload::Normal(crate::Command::ConfigureConsumer {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    ack_timeout_ms: 0,
                    max_delivery_attempts: None,
                    retry_delay_ms: 100,
                }),
            }))
            .await
            .unwrap();
        assert!(matches!(
            configured.as_slice(),
            [CommandResponse::ConsumerPolicy { policy }]
                if policy.version == 1 && policy.retry_delay_ms == 100
        ));

        let first = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 3,
                },
                payload: EntryPayload::Normal(crate::Command::PollGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-a".to_owned(),
                    response_member: None,
                    max_response_bytes: runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
                    now_ms: 100,
                    lease_deadline_ms: 100,
                    max_delivery_attempts: None,
                    legacy_ack_timeout_ms: Some(0),
                    policy_version: Some(1),
                }),
            }))
            .await
            .unwrap();
        let token = match &first[0] {
            CommandResponse::GroupPoll {
                result: PollResult::Message(message),
            } => message.delivery_token.clone().unwrap(),
            response => panic!("unexpected first delivery: {response:?}"),
        };
        let stale_ack = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 4,
                },
                payload: EntryPayload::Normal(crate::Command::AckGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-a".to_owned(),
                    offset: 0,
                    delivery_token: token,
                    now_ms: 100,
                }),
            }))
            .await
            .unwrap();
        assert!(matches!(
            stale_ack.as_slice(),
            [CommandResponse::GroupStaleDelivery { offset: 0, .. }]
        ));

        let deadline = store.state.read().await.state.group_consumers
            [&("events".to_owned(), "workers".to_owned())]
            .retry_not_before[&0]
            .retry_not_before_ms;
        assert_eq!(deadline, 200);

        let mut snapshot_builder = store.clone();
        snapshot_builder.build_snapshot().await.unwrap();
        drop(snapshot_builder);
        drop(state_machine);
        drop(store);

        let recovered =
            Arc::new(StateMachineStore::open(&state_directory, GroupKind::Combined).unwrap());
        let mut recovered_machine = recovered.clone();
        let recovered_deadline = recovered.state.read().await.state.group_consumers
            [&("events".to_owned(), "workers".to_owned())]
            .retry_not_before[&0]
            .retry_not_before_ms;
        assert_eq!(recovered_deadline, deadline);

        for (index, now_ms, expected_attempt) in [(5, 199, None), (6, 200, Some(2))] {
            let responses = recovered_machine
                .apply(std::iter::once(Entry {
                    log_id: LogId {
                        leader_id: openraft::CommittedLeaderId::new(1, 1),
                        index,
                    },
                    payload: EntryPayload::Normal(crate::Command::PollGroup {
                        stream: "events".to_owned(),
                        consumer: "workers".to_owned(),
                        member: format!("member-{index}"),
                        response_member: None,
                        max_response_bytes: runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
                        now_ms,
                        lease_deadline_ms: now_ms,
                        max_delivery_attempts: None,
                        legacy_ack_timeout_ms: Some(0),
                        policy_version: Some(1),
                    }),
                }))
                .await
                .unwrap();
            match (&responses[0], expected_attempt) {
                (
                    CommandResponse::GroupPoll {
                        result: PollResult::Empty,
                    },
                    None,
                ) => {}
                (
                    CommandResponse::GroupPoll {
                        result: PollResult::Message(message),
                    },
                    Some(expected),
                ) => assert_eq!(message.delivery_attempt, Some(expected)),
                (response, expected) => {
                    panic!(
                        "unexpected response before/at retry deadline {expected:?}: {response:?}"
                    )
                }
            }
        }
    }

    #[tokio::test]
    async fn checkpoint_failure_after_snapshot_persist_recovers_new_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let store =
            Arc::new(StateMachineStore::open(&state_directory, GroupKind::Combined).unwrap());
        let mut state_machine = store.clone();

        let first_meta = snapshot_meta(1, "first");
        let first_data = snapshot_data(b"first");
        state_machine
            .install_snapshot(&first_meta, Box::new(Cursor::new(first_data.clone())))
            .await
            .unwrap();
        let checkpoint_path = state_directory.join("state-machine.json");
        let checkpoint_before = fs::read(&checkpoint_path).unwrap();

        let second_meta = snapshot_meta(2, "second");
        let second_data = snapshot_data(b"second");
        store
            .fail_next_checkpoint_persist
            .store(true, Ordering::Relaxed);
        assert!(
            state_machine
                .install_snapshot(&second_meta, Box::new(Cursor::new(second_data.clone())))
                .await
                .is_err()
        );

        let snapshot_path = state_directory.join("snapshot.json");
        let persisted_snapshot: StoredSnapshot =
            serde_json::from_slice(&fs::read(snapshot_path).unwrap()).unwrap();
        assert_eq!(persisted_snapshot.meta, second_meta);
        assert_eq!(persisted_snapshot.data, second_data);
        assert_eq!(fs::read(checkpoint_path).unwrap(), checkpoint_before);
        {
            let state = store.state.read().await;
            assert_eq!(state.state.streams["events"].messages[0].payload, b"first");
        }
        let current = state_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("failed install must not publish a new current snapshot");
        assert_eq!(current.meta, first_meta);
        assert_eq!(current.snapshot.into_inner(), first_data);

        drop(state_machine);
        drop(store);

        let reopened =
            Arc::new(StateMachineStore::open(&state_directory, GroupKind::Combined).unwrap());
        let state = reopened.state.read().await;
        assert_eq!(state.state.streams["events"].messages[0].payload, b"second");
        drop(state);
        let mut reopened_state_machine = reopened.clone();
        let recovered_snapshot = reopened_state_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("reopen must select the complete newer persisted snapshot");
        assert_eq!(recovered_snapshot.meta, second_meta);
        assert_eq!(recovered_snapshot.snapshot.into_inner(), second_data);
    }
    #[tokio::test]
    async fn current_snapshot_preserves_lease_floor_and_applied_commands_advance_it() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        let kind = GroupKind::Data {
            stream: "events".to_owned(),
            stream_id: "stream/events".to_owned(),
            group_id: "group/events/data".to_owned(),
        };
        let store = Arc::new(StateMachineStore::open(&state_directory, kind.clone()).unwrap());
        let snapshot_value = serde_json::json!({
            "version": FORMAT_VERSION,
            "streams": {
                "events": {
                    "stream_id": "stream/events",
                    "group_id": "group/events/data",
                    "lifecycle": "Active",
                    "messages": [{
                        "key": null,
                        "payload": [108, 101, 103, 97, 99, 121],
                        "published_at_ms": 1
                    }]
                }
            },
            "consumers": [],
            "group_consumers": [],
            "lease_clock_ms": 0,
            "dedup": {},
            "redeliveries": 0,
            "dead_letters": 0
        });
        assert_eq!(snapshot_value["version"], FORMAT_VERSION);
        assert_eq!(snapshot_value["lease_clock_ms"], 0);
        let snapshot_data = serde_json::to_vec(&snapshot_value).unwrap();
        let snapshot_meta = snapshot_meta(1, "current-lease-floor");

        let mut state_machine = store.clone();
        state_machine
            .install_snapshot(&snapshot_meta, Box::new(Cursor::new(snapshot_data.clone())))
            .await
            .unwrap();

        assert_eq!(store.state.read().await.state.lease_clock_ms, 0);

        let poll_responses = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 2,
                },
                payload: EntryPayload::Normal(crate::Command::PollGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-a".to_owned(),
                    response_member: Some("member-a".to_owned()),
                    max_response_bytes: runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
                    now_ms: 125,
                    lease_deadline_ms: 250,
                    max_delivery_attempts: None,
                    legacy_ack_timeout_ms: None,
                    policy_version: None,
                }),
            }))
            .await
            .unwrap();
        let delivery_token = match &poll_responses[0] {
            CommandResponse::GroupPoll {
                result: PollResult::Message(message),
            } => message
                .delivery_token
                .clone()
                .expect("poll should create a lease"),
            response => panic!("unexpected poll response: {response:?}"),
        };
        assert_eq!(store.state.read().await.state.lease_clock_ms, 125);

        let stale_ack = state_machine
            .apply(std::iter::once(Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1),
                    index: 3,
                },
                payload: EntryPayload::Normal(crate::Command::AckGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-b".to_owned(),
                    offset: 0,
                    delivery_token,
                    now_ms: 175,
                }),
            }))
            .await
            .unwrap();
        assert_eq!(
            stale_ack,
            vec![CommandResponse::GroupStaleDelivery {
                consumer: "workers".to_owned(),
                offset: 0,
            }]
        );
        assert_eq!(store.state.read().await.state.lease_clock_ms, 175);

        drop(state_machine);
        drop(store);
        let recovered = StateMachineStore::open(&state_directory, kind).unwrap();
        let state = recovered.state.read().await;
        assert_eq!(state.state.lease_clock_ms, 175);
        let delivery = state.state.group_consumers[&("events".to_owned(), "workers".to_owned())]
            .in_flight
            .get(&0)
            .expect("stale member acknowledgement must leave the live lease intact");
        assert_eq!(delivery.member, "member-a");
        assert_eq!(delivery.deadline_ms, 250);
    }

    #[tokio::test]
    async fn current_checkpoint_preserves_lease_floor_and_group_poll_survives_replay() {
        let directory = tempfile::tempdir().unwrap();
        let state_directory = directory.path().join("state-machine");
        fs::create_dir_all(&state_directory).unwrap();
        let kind = GroupKind::Data {
            stream: "events".to_owned(),
            stream_id: "stream/events".to_owned(),
            group_id: "group/events/data".to_owned(),
        };
        let checkpoint_log_id = LogId {
            leader_id: openraft::CommittedLeaderId::new(1, 1),
            index: 1,
        };
        let checkpoint = serde_json::json!({
            "version": FORMAT_VERSION,
            "last_applied_log": serde_json::to_value(checkpoint_log_id).unwrap(),
            "last_membership": serde_json::to_value(
                StoredMembership::<NodeId, BasicNode>::default()
            )
            .unwrap(),
            "streams": {
                "events": {
                    "stream_id": "stream/events",
                    "group_id": "group/events/data",
                    "lifecycle": "Active",
                    "messages": [{
                        "key": null,
                        "payload": [108, 101, 103, 97, 99, 121],
                        "published_at_ms": 1
                    }]
                }
            },
            "consumers": [],
            "group_consumers": [],
            "lease_clock_ms": 0,
            "dedup": {},
            "redeliveries": 0,
            "dead_letters": 0
        });
        assert_eq!(checkpoint["version"], FORMAT_VERSION);
        assert_eq!(checkpoint["lease_clock_ms"], 0);
        fs::write(
            state_directory.join("state-machine.json"),
            serde_json::to_vec(&checkpoint).unwrap(),
        )
        .unwrap();

        let store = Arc::new(StateMachineStore::open(&state_directory, kind.clone()).unwrap());
        {
            let state = store.state.read().await;
            assert_eq!(state.last_applied_log, Some(checkpoint_log_id));
            assert_eq!(state.state.lease_clock_ms, 0);
            assert_eq!(state.state.streams["events"].messages[0].payload, b"legacy");
        }

        let log_id = LogId {
            leader_id: openraft::CommittedLeaderId::new(1, 1),
            index: 2,
        };
        let mut state_machine = store.clone();
        let responses = state_machine
            .apply(std::iter::once(Entry {
                log_id,
                payload: EntryPayload::Normal(crate::Command::PollGroup {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: "member-a".to_owned(),
                    response_member: Some("member-a".to_owned()),
                    max_response_bytes: runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
                    now_ms: 125,
                    lease_deadline_ms: 250,
                    max_delivery_attempts: None,
                    legacy_ack_timeout_ms: None,
                    policy_version: None,
                }),
            }))
            .await
            .unwrap();
        let delivery_token = match &responses[0] {
            CommandResponse::GroupPoll {
                result: PollResult::Message(message),
            } => message
                .delivery_token
                .clone()
                .expect("poll should create a lease"),
            response => panic!("unexpected poll response: {response:?}"),
        };
        {
            let state = store.state.read().await;
            assert_eq!(state.state.lease_clock_ms, 125);
            assert_eq!(state.last_applied_log, Some(log_id));
        }

        drop(state_machine);
        drop(store);
        let recovered = StateMachineStore::open(&state_directory, kind).unwrap();
        let state = recovered.state.read().await;
        assert_eq!(state.state.lease_clock_ms, 125);
        assert_eq!(state.last_applied_log, Some(log_id));
        let delivery = state.state.group_consumers[&("events".to_owned(), "workers".to_owned())]
            .in_flight
            .get(&0)
            .expect("journal replay must retain the grouped poll lease");
        assert_eq!(delivery.member, "member-a");
        assert_eq!(delivery.deadline_ms, 250);
        assert_eq!(delivery.delivery_token, delivery_token);
    }
}
