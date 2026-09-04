use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{LogId, LogState, RaftLogId, RaftTypeConfig, StorageError, Vote};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::NodeId;
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;

const FORMAT_VERSION: u32 = 1;

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
    path: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(bound(serialize = "E: Serialize", deserialize = "E: DeserializeOwned"))]
struct PersistedLog<E> {
    version: u32,
    last_purged_log_id: Option<LogId<NodeId>>,
    log: BTreeMap<u64, E>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

#[derive(Serialize)]
#[serde(bound(serialize = "E: Serialize"))]
struct PersistedLogRef<'a, E> {
    version: u32,
    last_purged_log_id: Option<LogId<NodeId>>,
    log: &'a BTreeMap<u64, E>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
            path: None,
        }
    }
}

impl<C: RaftTypeConfig<NodeId = NodeId>> LogStore<C>
where
    C::Entry: Clone + Serialize + DeserializeOwned,
{
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError<NodeId>> {
        let path = path.as_ref().to_path_buf();
        let inner = if let Some(persisted) = Self::read_persisted(&path)? {
            LogStoreInner {
                last_purged_log_id: persisted.last_purged_log_id,
                log: persisted.log,
                committed: persisted.committed,
                vote: persisted.vote,
                path: Some(path),
            }
        } else {
            LogStoreInner {
                path: Some(path),
                ..Default::default()
            }
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub(crate) fn validate(path: impl AsRef<Path>) -> Result<(), StorageError<NodeId>> {
        Self::read_persisted(path.as_ref()).map(|_| ())
    }

    fn read_persisted(path: &Path) -> Result<Option<PersistedLog<C::Entry>>, StorageError<NodeId>> {
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).map_err(|error| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Logs,
                openraft::ErrorVerb::Read,
                std::io::Error::new(
                    error.kind(),
                    format!("could not read Raft log '{}': {error}", path.display()),
                ),
            )
        })?;
        let persisted: PersistedLog<C::Entry> =
            serde_json::from_slice(&bytes).map_err(|error| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Read,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid Raft log '{}': {error}", path.display()),
                    ),
                )
            })?;
        if persisted.version != FORMAT_VERSION {
            return Err(StorageError::from_io_error(
                openraft::ErrorSubject::Logs,
                openraft::ErrorVerb::Read,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "unsupported log format version {} in '{}' (Raft log; supported version {})",
                        persisted.version,
                        path.display(),
                        FORMAT_VERSION
                    ),
                ),
            ));
        }

        let mut expected_index = persisted
            .last_purged_log_id
            .map(|log_id| log_id.index.checked_add(1))
            .unwrap_or(Some(0));
        for (index, entry) in &persisted.log {
            let Some(next_index) = expected_index else {
                return Err(invalid_log(
                    path,
                    format!("log entry at index {index} follows the maximum possible log index"),
                ));
            };
            if *index != next_index {
                return Err(invalid_log(
                    path,
                    format!("expected contiguous log entry at index {next_index}, found {index}"),
                ));
            }
            let entry_index = entry.get_log_id().index;
            if entry_index != *index {
                return Err(invalid_log(
                    path,
                    format!(
                        "log entry map index {index} does not match entry log index {entry_index}"
                    ),
                ));
            }
            expected_index = index.checked_add(1);
        }

        let last_log_index = persisted
            .log
            .keys()
            .next_back()
            .copied()
            .or_else(|| persisted.last_purged_log_id.map(|log_id| log_id.index));
        if let Some(committed) = persisted.committed {
            let Some(last_log_index) = last_log_index else {
                return Err(invalid_log(
                    path,
                    format!(
                        "committed log index {} is present but the persisted log is empty",
                        committed.index
                    ),
                ));
            };
            if committed.index > last_log_index {
                return Err(invalid_log(
                    path,
                    format!(
                        "committed log index {} is beyond the last persisted log index {last_log_index}",
                        committed.index
                    ),
                ));
            }
            if let Some(entry) = persisted.log.get(&committed.index)
                && *entry.get_log_id() != committed
            {
                return Err(invalid_log(
                    path,
                    format!(
                        "committed log id {committed:?} does not match the retained entry at index {} ({:?})",
                        committed.index,
                        entry.get_log_id()
                    ),
                ));
            }
        }
        Ok(Some(persisted))
    }

    fn persist(inner: &LogStoreInner<C>) -> Result<(), StorageError<NodeId>> {
        let Some(path) = &inner.path else {
            return Ok(());
        };

        let persisted = PersistedLogRef {
            version: FORMAT_VERSION,
            last_purged_log_id: inner.last_purged_log_id,
            log: &inner.log,
            committed: inner.committed,
            vote: inner.vote,
        };
        let bytes = serde_json::to_vec(&persisted).map_err(|error| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Logs,
                openraft::ErrorVerb::Write,
                std::io::Error::other(error),
            )
        })?;
        atomic_write(path, &bytes).map_err(|error| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Logs,
                openraft::ErrorVerb::Write,
                error,
            )
        })
    }
}

fn invalid_log(path: &Path, reason: String) -> StorageError<NodeId> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Logs,
        openraft::ErrorVerb::Read,
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid Raft log '{}': {reason}", path.display()),
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
        inner.vote = Some(*vote);
        Self::persist(&inner)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.committed = committed;
        Self::persist(&inner)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.committed)
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
        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }
        let result = Self::persist(&inner);
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
        let keys = inner
            .log
            .range(log_id.index..)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in keys {
            inner.log.remove(&key);
        }
        Self::persist(&inner)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.last_purged_log_id = Some(log_id);
        let keys = inner
            .log
            .range(..=log_id.index)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in keys {
            inner.log.remove(&key);
        }
        Self::persist(&inner)
    }
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
    use openraft::{CommittedLeaderId, Entry, EntryPayload};

    fn blank_entry(index: u64, term: u64) -> Entry<crate::TypeConfig> {
        Entry {
            log_id: LogId {
                leader_id: CommittedLeaderId::new(term, 1),
                index,
            },
            payload: EntryPayload::Blank,
        }
    }

    fn write_persisted_log(
        path: &Path,
        last_purged_log_id: Option<LogId<NodeId>>,
        log: BTreeMap<u64, Entry<crate::TypeConfig>>,
        committed: Option<LogId<NodeId>>,
    ) -> Vec<u8> {
        let bytes = serde_json::to_vec(&PersistedLog {
            version: FORMAT_VERSION,
            last_purged_log_id,
            log,
            committed,
            vote: None,
        })
        .unwrap();
        fs::write(path, &bytes).unwrap();
        bytes
    }

    #[tokio::test]
    async fn supported_raft_log_format_recovers_without_rewriting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "version": FORMAT_VERSION,
            "last_purged_log_id": null,
            "log": {},
            "committed": null,
            "vote": null,
        }))
        .unwrap();
        fs::write(&path, &bytes).unwrap();

        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, None);
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn unsupported_raft_log_format_is_rejected_without_rewriting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let bytes = serde_json::to_vec(&serde_json::json!({
            "version": FORMAT_VERSION + 1,
            "last_purged_log_id": null,
            "log": {},
            "committed": null,
            "vote": null,
        }))
        .unwrap();
        fs::write(&path, &bytes).unwrap();

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unsupported log format version"));
        assert!(error.contains(path.to_str().unwrap()));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn mismatched_raft_log_entry_index_is_rejected_without_rewriting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let bytes =
            write_persisted_log(&path, None, BTreeMap::from([(0, blank_entry(1, 1))]), None);

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("log entry map index 0 does not match entry log index 1"));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn non_contiguous_raft_log_is_rejected_without_rewriting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let bytes = write_persisted_log(
            &path,
            None,
            BTreeMap::from([(0, blank_entry(0, 1)), (2, blank_entry(2, 1))]),
            None,
        );

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected contiguous log entry at index 1, found 2"));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn committed_raft_log_beyond_retained_entries_is_rejected_without_rewriting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let bytes = write_persisted_log(
            &path,
            None,
            BTreeMap::from([(0, blank_entry(0, 1))]),
            Some(LogId {
                leader_id: CommittedLeaderId::new(1, 1),
                index: 1,
            }),
        );

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("committed log index 1 is beyond the last persisted log index 0"));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn committed_raft_log_id_mismatch_is_rejected_without_rewriting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let bytes = write_persisted_log(
            &path,
            None,
            BTreeMap::from([(0, blank_entry(0, 1))]),
            Some(LogId {
                leader_id: CommittedLeaderId::new(2, 1),
                index: 0,
            }),
        );

        let error = LogStore::<crate::TypeConfig>::open(&path)
            .unwrap_err()
            .to_string();
        assert!(error.contains("does not match the retained entry at index 0"));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[tokio::test]
    async fn valid_purged_raft_log_recovers_without_rewriting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("raft-log.json");
        let last_purged_log_id = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 4,
        });
        let committed = Some(LogId {
            leader_id: CommittedLeaderId::new(2, 1),
            index: 6,
        });
        let bytes = write_persisted_log(
            &path,
            last_purged_log_id,
            BTreeMap::from([(5, blank_entry(5, 2)), (6, blank_entry(6, 2))]),
            committed,
        );

        let mut store = LogStore::<crate::TypeConfig>::open(&path).unwrap();
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, committed);
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}
