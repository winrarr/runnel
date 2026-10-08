use std::collections::BTreeMap;
use std::fmt::Debug;
#[cfg(test)]
use std::fs;
use std::ops::RangeBounds;
use std::path::Path;
#[cfg(all(test, feature = "persistence-write-counters"))]
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{LogId, LogState, RaftLogId, RaftTypeConfig, StorageError, Vote};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::persistence_write::serialize_json;
use crate::raft_log_segments::{self, ControlState, SegmentStore};
use crate::{NodeId, PersistenceWriteOperation, PersistenceWriteRole};
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;

#[derive(Clone, Debug, Default)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

#[derive(Debug)]
struct LogStoreInner<C: RaftTypeConfig> {
    last_purged_log_id: Option<LogId<C::NodeId>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogId<C::NodeId>>,
    vote: Option<Vote<C::NodeId>>,
    persistence: Persistence<C::Entry>,
    poisoned: bool,
}

#[derive(Debug)]
enum Persistence<E> {
    Memory,
    Segmented(SegmentStore<E>),
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
            persistence: Persistence::Memory,
            poisoned: false,
        }
    }
}

impl<C: RaftTypeConfig<NodeId = NodeId>> LogStore<C>
where
    C::Entry: Clone + Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<NodeId>> {
        let path = path.as_ref().to_path_buf();
        let inner = if path.exists() {
            let version = raft_log_segments::read_version(&path)
                .map_err(|error| storage_error(openraft::ErrorVerb::Read, error))?;
            if version != raft_log_segments::FORMAT_VERSION {
                return Err(unsupported_log_version(&path, version));
            }
            let (store, log, control) = SegmentStore::open(&path)
                .map_err(|error| storage_error(openraft::ErrorVerb::Read, error))?;
            LogStoreInner {
                last_purged_log_id: control.last_purged_log_id,
                log,
                committed: control.committed,
                vote: control.vote,
                persistence: Persistence::Segmented(store),
                poisoned: false,
            }
        } else {
            let family = raft_log_segments::family_directory(&path);
            if family.exists() {
                return Err(StorageError::from_io_error(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Read,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "unselected Raft-log segment family '{}' without marker '{}'",
                            family.display(),
                            path.display()
                        ),
                    ),
                ));
            }
            let (store, log, control) = SegmentStore::initialize(&path)
                .map_err(|error| storage_error(openraft::ErrorVerb::Write, error))?;
            LogStoreInner {
                last_purged_log_id: control.last_purged_log_id,
                log,
                committed: control.committed,
                vote: control.vote,
                persistence: Persistence::Segmented(store),
                poisoned: false,
            }
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub(crate) fn validate(path: impl AsRef<Path>) -> Result<(), StorageError<NodeId>> {
        let path = path.as_ref();
        if !path.exists() {
            let family = raft_log_segments::family_directory(path);
            if family.exists() {
                return Err(StorageError::from_io_error(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Read,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unselected Raft-log segment family '{}'", family.display()),
                    ),
                ));
            }
            return Ok(());
        }
        let version = raft_log_segments::read_version(path)
            .map_err(|error| storage_error(openraft::ErrorVerb::Read, error))?;
        if version != raft_log_segments::FORMAT_VERSION {
            return Err(unsupported_log_version(path, version));
        }
        raft_log_segments::SegmentStore::<C::Entry>::validate(path)
            .map_err(|error| storage_error(openraft::ErrorVerb::Read, error))
    }
}

fn storage_error(verb: openraft::ErrorVerb, error: std::io::Error) -> StorageError<NodeId> {
    StorageError::from_io_error(openraft::ErrorSubject::Logs, verb, error)
}

fn ensure_healthy<C: RaftTypeConfig>(inner: &LogStoreInner<C>) -> Result<(), StorageError<NodeId>> {
    if inner.poisoned {
        return Err(storage_error(
            openraft::ErrorVerb::Write,
            std::io::Error::other(
                "Raft log store is unavailable after an ambiguous persistence failure; reopen it to recover",
            ),
        ));
    }
    Ok(())
}

fn control_state<C: RaftTypeConfig<NodeId = NodeId>>(inner: &LogStoreInner<C>) -> ControlState
where
    C::Entry: Clone + Serialize + DeserializeOwned,
{
    match &inner.persistence {
        Persistence::Segmented(store) => ControlState {
            last_purged_log_id: inner.last_purged_log_id,
            committed: inner.committed,
            vote: inner.vote,
            ..store.control()
        },
        Persistence::Memory => ControlState {
            last_purged_log_id: inner.last_purged_log_id,
            committed: inner.committed,
            vote: inner.vote,
            ..ControlState::default()
        },
    }
}

fn unsupported_log_version(path: &Path, version: u32) -> StorageError<NodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Logs,
        openraft::ErrorVerb::Read,
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unsupported Raft-log format version {version} in '{}' (supported version {})",
                path.display(),
                raft_log_segments::FORMAT_VERSION
            ),
        ),
    )
}

impl<C: RaftTypeConfig<NodeId = NodeId>> RaftLogReader<C> for LogStore<C>
where
    C::Entry: Clone + Serialize + DeserializeOwned,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        ensure_healthy(&inner)?;
        Ok(inner
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl<C: RaftTypeConfig<NodeId = NodeId>> RaftLogStorage<C> for LogStore<C>
where
    C::Entry: Clone + Serialize + DeserializeOwned,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        ensure_healthy(&inner)?;
        let last_log_id = inner
            .log
            .values()
            .next_back()
            .map(|entry| *entry.get_log_id())
            .or_else(|| inner.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        ensure_healthy(&inner)?;
        let mut state = control_state(&inner);
        state.vote = Some(*vote);
        match &mut inner.persistence {
            Persistence::Segmented(store) => {
                if let Err(error) = store.persist_control(state) {
                    inner.poisoned = true;
                    return Err(storage_error(openraft::ErrorVerb::Write, error));
                }
                inner.vote = Some(*vote);
                Ok(())
            }
            Persistence::Memory => {
                inner.vote = Some(*vote);
                Ok(())
            }
        }
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        ensure_healthy(&inner)?;
        Ok(inner.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        ensure_healthy(&inner)?;
        let mut state = control_state(&inner);
        state.committed = committed;
        match &mut inner.persistence {
            Persistence::Segmented(store) => {
                if let Err(error) = store.persist_control(state) {
                    inner.poisoned = true;
                    return Err(storage_error(openraft::ErrorVerb::Write, error));
                }
                inner.committed = committed;
                Ok(())
            }
            Persistence::Memory => {
                inner.committed = committed;
                Ok(())
            }
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        ensure_healthy(&inner)?;
        Ok(inner.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + Send,
        I::IntoIter: Send,
    {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.log_append");
        let mut inner = self.inner.lock().await;
        if let Err(error) = ensure_healthy(&inner) {
            callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
            return Err(error);
        }
        let mut batch = Vec::new();
        let mut expected_index = match inner
            .log
            .keys()
            .next_back()
            .copied()
            .or_else(|| inner.last_purged_log_id.map(|log_id| log_id.index))
        {
            Some(index) => index.checked_add(1),
            None => Some(0),
        };
        for entry in entries {
            let Some(expected) = expected_index else {
                let error = storage_error(
                    openraft::ErrorVerb::Write,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Raft-log index overflow",
                    ),
                );
                callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
                return Err(error);
            };
            let index = entry.get_log_id().index;
            if index != expected {
                let error = storage_error(
                    openraft::ErrorVerb::Write,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "non-contiguous Raft-log append: expected index {expected}, found {index}"
                        ),
                    ),
                );
                callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
                return Err(error);
            }
            let encoded = if matches!(inner.persistence, Persistence::Segmented(_)) {
                match serialize_json(
                    PersistenceWriteRole::RaftLogSegmentAppend,
                    PersistenceWriteOperation::Serialize,
                    &entry,
                ) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        let error =
                            storage_error(openraft::ErrorVerb::Write, std::io::Error::other(error));
                        callback.log_io_completed(Err(std::io::Error::other(error.to_string())));
                        return Err(error);
                    }
                }
            } else {
                Vec::new()
            };
            batch.push((entry, index, encoded));
            expected_index = index.checked_add(1);
        }

        let result = match &mut inner.persistence {
            Persistence::Segmented(store) => {
                let entries = batch
                    .iter()
                    .map(|(_, index, bytes)| (*index, bytes.as_slice()))
                    .collect::<Vec<_>>();
                match store.append(store.control().generation, &entries) {
                    Ok(()) => {
                        for (entry, index, _) in batch {
                            inner.log.insert(index, entry);
                        }
                        Ok(())
                    }
                    Err(error) => {
                        inner.poisoned = true;
                        Err(storage_error(openraft::ErrorVerb::Write, error))
                    }
                }
            }
            Persistence::Memory => {
                for (entry, index, _) in batch {
                    inner.log.insert(index, entry);
                }
                Ok(())
            }
        };
        callback.log_io_completed(
            result
                .as_ref()
                .map(|_| ())
                .map_err(|error| std::io::Error::other(error.to_string())),
        );
        result
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        ensure_healthy(&inner)?;
        match &mut inner.persistence {
            Persistence::Segmented(store) => {
                let generation = store.control().generation.checked_add(1).ok_or_else(|| {
                    storage_error(
                        openraft::ErrorVerb::Write,
                        std::io::Error::other("Raft-log generation exhausted"),
                    )
                })?;
                if let Err(error) = store.truncate(log_id.index, generation) {
                    inner.poisoned = true;
                    return Err(storage_error(openraft::ErrorVerb::Write, error));
                }
                inner.log.retain(|index, _| *index < log_id.index);
                Ok(())
            }
            Persistence::Memory => {
                inner.log.retain(|index, _| *index < log_id.index);
                Ok(())
            }
        }
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        ensure_healthy(&inner)?;
        match &mut inner.persistence {
            Persistence::Segmented(store) => {
                if let Err(error) = store.purge(Some(log_id)) {
                    inner.poisoned = true;
                    return Err(storage_error(openraft::ErrorVerb::Write, error));
                }
                inner.last_purged_log_id = Some(log_id);
                inner.log.retain(|index, _| *index > log_id.index);
                Ok(())
            }
            Persistence::Memory => {
                inner.last_purged_log_id = Some(log_id);
                inner.log.retain(|index, _| *index > log_id.index);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftLogStorageExt;
    use openraft::{CommittedLeaderId, Entry, EntryPayload};
    use std::io::Write;
    #[cfg(feature = "persistence-write-counters")]
    use std::time::Instant;

    fn blank_entry(index: u64, term: u64) -> Entry<crate::TypeConfig> {
        Entry {
            log_id: LogId {
                leader_id: CommittedLeaderId::new(term, 1),
                index,
            },
            payload: EntryPayload::Blank,
        }
    }

    fn publish_entry(index: u64, payload_bytes: usize) -> Entry<crate::TypeConfig> {
        Entry {
            log_id: LogId {
                leader_id: CommittedLeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Normal(crate::Command::Publish {
                stream: "td026-log-persistence".to_owned(),
                key: None,
                payload: vec![b'x'; payload_bytes],
                published_at_ms: 1,
                request_id: None,
            }),
        }
    }

    #[cfg(feature = "persistence-write-counters")]
    fn benchmark_dimension(name: &str, default: &[usize]) -> Vec<usize> {
        let Ok(value) = std::env::var(name) else {
            return default.to_vec();
        };
        let values = value
            .split(',')
            .map(|item| {
                item.parse::<usize>()
                    .expect("benchmark dimension must be an integer")
            })
            .collect::<Vec<_>>();
        assert!(!values.is_empty(), "benchmark dimension cannot be empty");
        values
    }

    #[cfg(feature = "persistence-write-counters")]
    fn benchmark_count(name: &str, default: usize) -> usize {
        std::env::var(name)
            .map(|value| value.parse().expect("benchmark count must be an integer"))
            .unwrap_or(default)
    }

    #[cfg(feature = "persistence-write-counters")]
    fn segment_bytes(directory: &Path) -> u64 {
        fs::read_dir(directory)
            .unwrap()
            .map(|item| item.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "rlog")
            })
            .map(|path| fs::metadata(path).unwrap().len())
            .sum()
    }

    #[tokio::test]
    #[cfg(feature = "persistence-write-counters")]
    #[ignore = "manual TD-026 persistence evidence; requires an explicit ext4 output directory"]
    async fn measure_retained_log_append_persistence_cost() {
        let output_dir = std::env::var_os("RUNNEL_TD026_OUTPUT_DIR")
            .map(PathBuf::from)
            .expect(
                "set RUNNEL_TD026_OUTPUT_DIR to a run-scoped directory on persistent local storage",
            );
        fs::create_dir_all(&output_dir).unwrap();

        let retained_lengths =
            benchmark_dimension("RUNNEL_TD026_RETAINED", &[0, 4, 32, 36, 256, 1024, 4096]);
        let batch_sizes = benchmark_dimension("RUNNEL_TD026_BATCHES", &[1, 8, 32]);
        let payload_sizes = benchmark_dimension("RUNNEL_TD026_PAYLOADS", &[100, 1024]);
        let warmups = benchmark_count("RUNNEL_TD026_WARMUPS", 2);
        let samples = benchmark_count("RUNNEL_TD026_SAMPLES", 20);
        assert!(batch_sizes.iter().all(|size| *size > 0));
        assert!(payload_sizes.iter().all(|size| *size > 0));
        assert!(samples > 0);

        let result_path = output_dir.join("samples.csv");
        let mut results = fs::File::create(&result_path).unwrap();
        writeln!(
            results,
            "operation,retained_log_entries,payload_bytes,batch_entries,sample,serialized_bytes,write_bytes_offered,write_bytes_completed,file_bytes_added,control_file_bytes,elapsed_ns"
        )
        .unwrap();

        for retained in retained_lengths {
            for payload_bytes in &payload_sizes {
                for batch_size in &batch_sizes {
                    let scenario_dir = output_dir.join(format!(
                        "retained-{retained}-payload-{payload_bytes}-batch-{batch_size}"
                    ));
                    for sample in 0..(warmups + samples) {
                        if scenario_dir.exists() {
                            fs::remove_dir_all(&scenario_dir).unwrap();
                        }
                        fs::create_dir_all(&scenario_dir).unwrap();
                        let log_path = scenario_dir.join("raft-log.json");
                        let family = raft_log_segments::family_directory(&log_path);
                        let mut store = LogStore::<crate::TypeConfig>::open(&log_path).unwrap();
                        let seed = (0..retained)
                            .map(|index| publish_entry(index as u64, *payload_bytes))
                            .collect::<Vec<_>>();
                        if !seed.is_empty() {
                            store.blocking_append(seed).await.unwrap();
                            let last = store.get_log_state().await.unwrap().last_log_id.unwrap();
                            store.save_committed(Some(last)).await.unwrap();
                        }
                        let entries = (0..*batch_size)
                            .map(|offset| {
                                publish_entry(retained as u64 + offset as u64, *payload_bytes)
                            })
                            .collect::<Vec<_>>();

                        let before_segment_bytes = segment_bytes(&family);
                        let before_append_metrics = crate::persistence_write_metrics_snapshot();
                        let started = Instant::now();
                        store.blocking_append(entries).await.unwrap();
                        let append_elapsed_ns = started.elapsed().as_nanos();
                        let after_append_metrics = crate::persistence_write_metrics_snapshot();
                        let after_segment_bytes = segment_bytes(&family);
                        let last_log_id = store.get_log_state().await.unwrap().last_log_id.unwrap();
                        assert_eq!(last_log_id.index, retained as u64 + *batch_size as u64 - 1);

                        let append_role = PersistenceWriteRole::RaftLogSegmentAppend;
                        let serialize = PersistenceWriteOperation::Serialize;
                        let write_all = PersistenceWriteOperation::WriteAll;
                        let append_serialized = after_append_metrics
                            .counter(append_role, serialize)
                            .serialization_output_bytes
                            - before_append_metrics
                                .counter(append_role, serialize)
                                .serialization_output_bytes;
                        let append_write_before =
                            before_append_metrics.counter(append_role, write_all);
                        let append_write_after =
                            after_append_metrics.counter(append_role, write_all);
                        let append_write_offered = append_write_after.write_bytes_offered
                            - append_write_before.write_bytes_offered;
                        let append_write_completed = append_write_after.write_bytes_completed
                            - append_write_before.write_bytes_completed;
                        assert_eq!(append_write_offered, append_write_completed);

                        let before_control_metrics = crate::persistence_write_metrics_snapshot();
                        let started = Instant::now();
                        store.save_committed(Some(last_log_id)).await.unwrap();
                        let committed_elapsed_ns = started.elapsed().as_nanos();
                        let after_control_metrics = crate::persistence_write_metrics_snapshot();
                        let control_bytes =
                            fs::metadata(family.join("control.json")).unwrap().len();
                        assert_eq!(store.read_committed().await.unwrap(), Some(last_log_id));
                        let control_role = PersistenceWriteRole::RaftLogControlState;
                        let control_serialized = after_control_metrics
                            .counter(control_role, serialize)
                            .serialization_output_bytes
                            - before_control_metrics
                                .counter(control_role, serialize)
                                .serialization_output_bytes;
                        let control_write_before =
                            before_control_metrics.counter(control_role, write_all);
                        let control_write_after =
                            after_control_metrics.counter(control_role, write_all);
                        let control_write_offered = control_write_after.write_bytes_offered
                            - control_write_before.write_bytes_offered;
                        let control_write_completed = control_write_after.write_bytes_completed
                            - control_write_before.write_bytes_completed;
                        assert_eq!(control_write_offered, control_write_completed);

                        if sample >= warmups {
                            writeln!(
                                results,
                                "append,{retained},{payload_bytes},{batch_size},{},{append_serialized},{append_write_offered},{append_write_completed},{},,{append_elapsed_ns}",
                                sample - warmups,
                                after_segment_bytes - before_segment_bytes
                            )
                            .unwrap();
                            writeln!(
                                results,
                                "save_committed,{retained},{payload_bytes},{batch_size},{},{control_serialized},{control_write_offered},{control_write_completed},,{control_bytes},{committed_elapsed_ns}",
                                sample - warmups,
                            )
                            .unwrap();
                        }
                    }
                    fs::remove_dir_all(scenario_dir).unwrap();
                }
            }
        }

        results.sync_all().unwrap();
        println!("TD-026 raw samples: {}", result_path.display());
    }

    #[tokio::test]
    async fn new_segmented_raft_log_recovers_entries_control_state_and_callback() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        let entries = (0..4)
            .map(|index| blank_entry(index, 2))
            .collect::<Vec<_>>();
        store.blocking_append(entries.clone()).await.unwrap();
        let committed = *entries.last().unwrap().get_log_id();
        store.save_vote(&Vote::new(3, 1)).await.unwrap();
        store.save_committed(Some(committed)).await.unwrap();
        drop(store);

        let mut recovered = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        assert_eq!(
            recovered.get_log_state().await.unwrap().last_log_id,
            Some(committed)
        );
        assert_eq!(recovered.read_vote().await.unwrap(), Some(Vote::new(3, 1)));
        assert_eq!(recovered.read_committed().await.unwrap(), Some(committed));
        let mut reader = recovered.get_log_reader().await;
        let recovered_entries = reader.try_get_log_entries(0..=3).await.unwrap();
        assert_eq!(
            recovered_entries
                .iter()
                .map(|entry| entry.get_log_id().index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }

    #[tokio::test]
    async fn truncation_boundary_recovers_before_suffix_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        store
            .blocking_append(
                (0..6)
                    .map(|index| blank_entry(index, 1))
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        let boundary = blank_entry(3, 1).get_log_id().to_owned();
        {
            let mut inner = store.inner.lock().await;
            let Persistence::Segmented(segments) = &mut inner.persistence else {
                panic!("opened store must use segmented persistence");
            };
            let old = segments.control();
            segments
                .persist_control(ControlState {
                    generation: old.generation + 1,
                    truncation_pending_from: Some(boundary.index),
                    ..old
                })
                .unwrap();
        }
        drop(store);

        let mut recovered = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        assert_eq!(
            recovered
                .get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap()
                .index,
            2
        );
        recovered
            .blocking_append(vec![blank_entry(3, 2), blank_entry(4, 2)])
            .await
            .unwrap();
        drop(recovered);

        let mut reopened = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        let mut reader = reopened.get_log_reader().await;
        let entries = reader.try_get_log_entries(0..=4).await.unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[3].get_log_id().leader_id.term, 2);
        assert_eq!(entries[4].get_log_id().leader_id.term, 2);
    }

    #[tokio::test]
    async fn purge_floor_recovers_and_reclaims_complete_segments() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        store
            .blocking_append(
                (0..4)
                    .map(|index| publish_entry(index, 600 * 1024))
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        let floor = publish_entry(1, 0).get_log_id().to_owned();
        store.purge(floor).await.unwrap();
        let family = raft_log_segments::family_directory(&path);
        let segments_before_reopen = fs::read_dir(&family)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|item| {
                item.path()
                    .extension()
                    .is_some_and(|extension| extension == "rlog")
            })
            .count();
        assert_eq!(segments_before_reopen, 2);
        drop(store);

        let mut recovered = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        let state = recovered.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(floor));
        assert_eq!(state.last_log_id.unwrap().index, 3);
        let mut reader = recovered.get_log_reader().await;
        let entries = reader.try_get_log_entries(0..=3).await.unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.get_log_id().index)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[tokio::test]
    async fn interrupted_final_batch_is_discarded_on_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        store
            .blocking_append(vec![blank_entry(0, 1)])
            .await
            .unwrap();
        let family = raft_log_segments::family_directory(&path);
        let segment = fs::read_dir(&family)
            .unwrap()
            .map(|item| item.unwrap().path())
            .find(|candidate| {
                candidate
                    .extension()
                    .is_some_and(|extension| extension == "rlog")
            })
            .unwrap();
        let original_length = fs::metadata(&segment).unwrap().len();
        let mut file = fs::OpenOptions::new().append(true).open(&segment).unwrap();
        file.write_all(b"BAT").unwrap();
        file.sync_all().unwrap();
        drop(store);

        let mut recovered = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        assert_eq!(
            recovered
                .get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap()
                .index,
            0
        );
        assert_eq!(fs::metadata(segment).unwrap().len(), original_length);
    }

    #[test]
    fn valid_batches_with_a_log_index_gap_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        drop(store);

        let entry_zero = serde_json::to_vec(&blank_entry(0, 1)).unwrap();
        let entry_two = serde_json::to_vec(&blank_entry(2, 1)).unwrap();
        let first = raft_log_segments::encode_frame(0, 0, &[(0, entry_zero.as_slice())]).unwrap();
        let second = raft_log_segments::encode_frame(0, 2, &[(2, entry_two.as_slice())]).unwrap();
        let mut segment = raft_log_segments::segment_header(0);
        segment.extend_from_slice(&first);
        segment.extend_from_slice(&second);
        let family = raft_log_segments::family_directory(&path);
        fs::write(family.join("segment-00000000000000000000.rlog"), segment).unwrap();

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected contiguous Raft-log entry at index Some(1), found 2"));
    }

    #[tokio::test]
    async fn completed_batch_checksum_corruption_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        store
            .blocking_append(vec![blank_entry(0, 1)])
            .await
            .unwrap();
        let family = raft_log_segments::family_directory(&path);
        let segment = fs::read_dir(&family)
            .unwrap()
            .map(|item| item.unwrap().path())
            .find(|candidate| {
                candidate
                    .extension()
                    .is_some_and(|extension| extension == "rlog")
            })
            .unwrap();
        let mut bytes = fs::read(&segment).unwrap();
        bytes[16 + raft_log_segments::BATCH_HEADER_LEN + 1] ^= 0x01;
        fs::write(&segment, bytes).unwrap();
        drop(store);

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("batch checksum mismatch"));
    }

    #[tokio::test]
    async fn completed_batch_with_corrupt_completion_marker_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        store
            .blocking_append(vec![blank_entry(0, 1)])
            .await
            .unwrap();
        let family = raft_log_segments::family_directory(&path);
        let segment = fs::read_dir(&family)
            .unwrap()
            .map(|item| item.unwrap().path())
            .find(|candidate| {
                candidate
                    .extension()
                    .is_some_and(|extension| extension == "rlog")
            })
            .unwrap();
        let mut bytes = fs::read(&segment).unwrap();
        *bytes.last_mut().unwrap() ^= 0x01;
        fs::write(&segment, bytes).unwrap();
        drop(store);

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("invalid Raft-log batch completion marker"));
    }

    #[test]
    fn prior_raft_log_versions_are_rejected_without_rewriting_or_creating_segments() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        for version in [1, 2] {
            let mut old_entry =
                serde_json::to_value(blank_entry(0, 1)).expect("serialize old log entry shape");
            old_entry["payload"] = match version {
                1 => serde_json::json!({
                    "Normal": {
                        "ConfigureConsumer": {
                            "stream": "events",
                            "consumer": "worker",
                            "ack_timeout_ms": 30_000,
                            "max_delivery_attempts": null,
                        }
                    }
                }),
                2 => serde_json::json!({
                    "Normal": {
                        "Publish": {
                            "stream": "events",
                            "key": null,
                            // Version 2 serialized Vec<u8> as JSON integer arrays.
                            "payload": [0, 255],
                            "published_at_ms": 1,
                            "request_id": null,
                        }
                    }
                }),
                _ => unreachable!("test only uses known prior versions"),
            };
            let old_bytes = serde_json::to_vec(&serde_json::json!({
                "version": version,
                "last_purged_log_id": null,
                "log": {"0": old_entry},
                "committed": null,
                "vote": null,
            }))
            .unwrap();
            fs::write(&path, &old_bytes).unwrap();

            let expected = format!("unsupported Raft-log format version {version}");
            let validation_error = LogStore::<crate::TypeConfig>::validate(&path)
                .unwrap_err()
                .to_string();
            assert!(validation_error.contains(&expected));
            let error = LogStore::<crate::TypeConfig>::open(&path)
                .unwrap_err()
                .to_string();
            assert!(error.contains(&expected));
            assert_eq!(fs::read(&path).unwrap(), old_bytes);
            assert!(!raft_log_segments::family_directory(&path).exists());
        }
    }

    #[test]
    fn control_state_checksum_corruption_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        drop(store);
        let control_path = raft_log_segments::family_directory(&path).join("control.json");
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(&control_path).unwrap()).unwrap();
        let checksum = record["checksum"].as_u64().unwrap();
        record["checksum"] = serde_json::json!(checksum ^ 1);
        fs::write(&control_path, serde_json::to_vec(&record).unwrap()).unwrap();

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("control checksum mismatch"));
    }

    #[test]
    fn unselected_segment_family_is_rejected_instead_of_treated_as_empty() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let family = raft_log_segments::family_directory(&path);
        fs::create_dir(&family).unwrap();
        fs::write(family.join("control.json"), b"{}").unwrap();

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unselected Raft-log segment family"));
    }

    #[test]
    fn interrupted_segment_temporary_is_cleaned_but_unknown_artifacts_are_preserved() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        drop(store);
        let family = raft_log_segments::family_directory(&path);
        let temporary = family.join("segment-00000000000000000000.tmp-123");
        fs::write(&temporary, b"unpublished segment").unwrap();
        let unknown = family.join("unrecognized.tmp-123");
        fs::write(&unknown, b"unknown artifact").unwrap();

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unexpected Raft-log artifact"));
        assert!(!temporary.exists());
        assert_eq!(fs::read(unknown).unwrap(), b"unknown artifact");
    }

    #[cfg(feature = "persistence-write-counters")]
    #[tokio::test]
    async fn segment_append_and_control_state_use_separate_counter_roles() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        let before = crate::persistence_write_metrics_snapshot();
        store
            .blocking_append(vec![publish_entry(0, 128)])
            .await
            .unwrap();
        store
            .blocking_append(vec![publish_entry(1, 64)])
            .await
            .unwrap();
        let committed = store.get_log_state().await.unwrap().last_log_id;
        store.save_committed(committed).await.unwrap();
        let after = crate::persistence_write_metrics_snapshot();

        let append = after.counter(
            PersistenceWriteRole::RaftLogSegmentAppend,
            PersistenceWriteOperation::WriteAll,
        );
        let control = after.counter(
            PersistenceWriteRole::RaftLogControlState,
            PersistenceWriteOperation::WriteAll,
        );
        let append_before = before.counter(
            PersistenceWriteRole::RaftLogSegmentAppend,
            PersistenceWriteOperation::WriteAll,
        );
        let control_before = before.counter(
            PersistenceWriteRole::RaftLogControlState,
            PersistenceWriteOperation::WriteAll,
        );
        assert!(append.write_bytes_completed > append_before.write_bytes_completed);
        assert!(control.write_bytes_completed > control_before.write_bytes_completed);
        assert!(
            after
                .counter(
                    PersistenceWriteRole::RaftLogSegmentAppend,
                    PersistenceWriteOperation::SyncData,
                )
                .successes
                > before
                    .counter(
                        PersistenceWriteRole::RaftLogSegmentAppend,
                        PersistenceWriteOperation::SyncData,
                    )
                    .successes
        );

        let append_serialized = after.counter(
            PersistenceWriteRole::RaftLogSegmentAppend,
            PersistenceWriteOperation::Serialize,
        );
        let append_serialized_before = before.counter(
            PersistenceWriteRole::RaftLogSegmentAppend,
            PersistenceWriteOperation::Serialize,
        );
        let control_serialized = after.counter(
            PersistenceWriteRole::RaftLogControlState,
            PersistenceWriteOperation::Serialize,
        );
        let control_serialized_before = before.counter(
            PersistenceWriteRole::RaftLogControlState,
            PersistenceWriteOperation::Serialize,
        );
        assert!(
            append_serialized.serialization_output_bytes
                > append_serialized_before.serialization_output_bytes
        );
        assert!(
            control_serialized.serialization_output_bytes
                > control_serialized_before.serialization_output_bytes
        );

        for operation in [
            PersistenceWriteOperation::SyncAll,
            PersistenceWriteOperation::Rename,
            PersistenceWriteOperation::DirectorySync,
        ] {
            assert!(
                after
                    .counter(PersistenceWriteRole::RaftLogSegmentAppend, operation)
                    .successes
                    > before
                        .counter(PersistenceWriteRole::RaftLogSegmentAppend, operation)
                        .successes
            );
            assert!(
                after
                    .counter(PersistenceWriteRole::RaftLogControlState, operation)
                    .successes
                    > before
                        .counter(PersistenceWriteRole::RaftLogControlState, operation)
                        .successes
            );
        }
    }

    #[cfg(feature = "persistence-write-counters")]
    #[tokio::test]
    async fn appended_segment_and_control_file_growth_do_not_scale_with_retained_entries() {
        let mut append_file_deltas = Vec::new();
        let mut control_results = Vec::new();
        for retained in [0_u64, 4] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("raft-log.json");
            let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
            let seeds = (0..retained)
                .map(|index| blank_entry(index, 1))
                .collect::<Vec<_>>();
            if !seeds.is_empty() {
                store.blocking_append(seeds).await.unwrap();
            }
            let family = raft_log_segments::family_directory(&path);

            let before_segment_bytes = segment_bytes(&family);
            store
                .blocking_append(vec![publish_entry(retained, 128)])
                .await
                .unwrap();
            let file_bytes = segment_bytes(&family) - before_segment_bytes;
            append_file_deltas.push(file_bytes);

            let last_log_id = store.get_log_state().await.unwrap().last_log_id.unwrap();
            store.save_committed(Some(last_log_id)).await.unwrap();
            control_results.push(fs::metadata(family.join("control.json")).unwrap().len());
        }

        // Empty storage adds the fixed segment header; bounded decimal fields may vary by a digit.
        assert!(append_file_deltas[0].abs_diff(append_file_deltas[1]) <= 16);
        assert!(control_results[0].abs_diff(control_results[1]) <= 10);
    }
}
