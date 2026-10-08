use std::fs;
use std::mem::size_of;
use std::path::Path;

use openraft::{EntryPayload, LogId};
use runnel_engine::BrokerError;
use serde::{Deserialize, Serialize};

use super::state_machine::{GroupKind, StateMachineData, apply_command};
use super::{
    NodeId, PersistenceWriteOperation, PersistenceWriteRole, TypeConfig, persistence_write,
};

pub(super) const FORMAT_VERSION: u32 = 3;
pub(super) const FILE: &str = "state-machine.log";
pub(super) const MAX_RECORD_SIZE: u32 = 96 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JournalEntry {
    pub(super) version: u32,
    pub(super) log_id: LogId<NodeId>,
    pub(super) payload: EntryPayload<TypeConfig>,
}

#[derive(Deserialize)]
struct JournalRecordVersion {
    version: u32,
}

#[derive(Serialize)]
pub(super) struct JournalEntryRef<'a> {
    version: u32,
    log_id: LogId<NodeId>,
    payload: &'a EntryPayload<TypeConfig>,
}

impl<'a> JournalEntryRef<'a> {
    pub(super) fn from_entry(entry: &'a openraft::Entry<TypeConfig>) -> Self {
        Self {
            version: FORMAT_VERSION,
            log_id: entry.log_id,
            payload: &entry.payload,
        }
    }
}

pub(super) fn read(path: &Path) -> Result<Vec<JournalEntry>, BrokerError> {
    let (entries, truncated_at) = parse(path)?;
    let Some(truncated_at) = truncated_at else {
        return Ok(entries);
    };
    let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
    file.set_len(truncated_at as u64)?;
    file.sync_data()?;
    Ok(entries)
}

pub(super) fn validate(path: &Path) -> Result<(), BrokerError> {
    parse(path).map(|_| ())
}

fn parse(path: &Path) -> Result<(Vec<JournalEntry>, Option<usize>), BrokerError> {
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
        if record_len > MAX_RECORD_SIZE {
            return Err(BrokerError::Cluster(format!(
                "state-machine journal record is too large: {record_len} bytes"
            )));
        }
        let record_len = record_len as usize;
        if bytes.len() - cursor < record_len {
            truncated_at = Some(record_start);
            break;
        }
        let record_bytes = &bytes[cursor..cursor + record_len];
        let version = serde_json::from_slice::<JournalRecordVersion>(record_bytes)
            .map_err(|error| {
                BrokerError::Cluster(format!(
                    "invalid state-machine journal record in '{}': {error}",
                    path.display()
                ))
            })?
            .version;
        if version != FORMAT_VERSION {
            return Err(BrokerError::Cluster(format!(
                "unsupported state-machine journal format version {} in '{}' (supported version {})",
                version,
                path.display(),
                FORMAT_VERSION
            )));
        }
        let entry: JournalEntry = serde_json::from_slice(record_bytes).map_err(|error| {
            BrokerError::Cluster(format!(
                "invalid state-machine journal record in '{}': {error}",
                path.display()
            ))
        })?;
        entries.push(entry);
        cursor += record_len;
    }
    Ok((entries, truncated_at))
}

pub(super) fn append<T: Serialize>(
    file: &mut fs::File,
    entry: &T,
    role: PersistenceWriteRole,
) -> std::io::Result<()> {
    let bytes =
        persistence_write::serialize_json(role, PersistenceWriteOperation::Serialize, entry)
            .map_err(std::io::Error::other)?;
    if bytes.len() > MAX_RECORD_SIZE as usize {
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
    persistence_write::write_all(file, role, &length.to_le_bytes())?;
    persistence_write::write_all(file, role, &bytes)
}

pub(super) fn replay(
    state: &mut StateMachineData,
    entries: impl IntoIterator<Item = JournalEntry>,
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
                state.last_membership =
                    openraft::StoredMembership::new(Some(entry.log_id), membership);
            }
            EntryPayload::Normal(command) => {
                apply_command(&mut state.state, command, kind, entry.log_id);
            }
        }
    }
    Ok(())
}

pub(super) fn is_log_after(candidate: LogId<NodeId>, current: LogId<NodeId>) -> bool {
    (candidate.leader_id.term, candidate.index) > (current.leader_id.term, current.index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn maximum_publish_payload_fits_peer_frame_and_journal_record_bounds() {
        let public_frame_limit: usize = 64 * 1024 * 1024;
        let maximum_base64_payload = public_frame_limit.div_ceil(3) * 4;
        let encoded_command_bound = maximum_base64_payload + 1024 * 1024;

        assert!(encoded_command_bound <= 96 * 1024 * 1024);
        assert!(encoded_command_bound <= MAX_RECORD_SIZE as usize);
    }

    #[test]
    fn read_recovers_valid_prefix_and_discards_partial_tail() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(FILE);
        let entry = JournalEntry {
            version: FORMAT_VERSION,
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1),
                index: 7,
            },
            payload: EntryPayload::Blank,
        };
        let mut file = fs::File::create(&path).unwrap();
        append(
            &mut file,
            &entry,
            PersistenceWriteRole::StateMachineJournalAppend,
        )
        .unwrap();
        file.sync_all().unwrap();
        let valid_length = file.metadata().unwrap().len();
        file.write_all(&[0x01, 0x02, 0x03]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let entries = read(&path).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, entry.log_id);
        assert!(matches!(&entries[0].payload, EntryPayload::Blank));
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_length);
    }
}
