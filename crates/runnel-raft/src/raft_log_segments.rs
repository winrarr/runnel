use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use openraft::{LogId, RaftLogId, Vote};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::persistence_write::{serialize_json, write_all};
use crate::{NodeId, PersistenceWriteOperation, PersistenceWriteRole, atomic_write_with_role};

pub(super) const FORMAT_VERSION: u32 = 2;
const SEGMENT_TARGET_BYTES: usize = 1024 * 1024;
const SEGMENT_MAGIC: &[u8; 4] = b"RSG2";
const BATCH_MAGIC: &[u8; 4] = b"BAT2";
const BATCH_END: &[u8; 4] = b"END2";
const SEGMENT_HEADER_LEN: usize = 16;
pub(super) const BATCH_HEADER_LEN: usize = 36;
const BATCH_TRAILER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ControlState {
    pub(super) generation: u64,
    pub(super) truncation_pending_from: Option<u64>,
    pub(super) last_purged_log_id: Option<LogId<NodeId>>,
    pub(super) committed: Option<LogId<NodeId>>,
    pub(super) vote: Option<Vote<NodeId>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlRecord {
    state: ControlState,
    checksum: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionMarker {
    version: u32,
    artifact: String,
}

#[derive(Debug, Clone)]
struct Tail {
    path: PathBuf,
    length: u64,
}

#[derive(Debug)]
struct EntryRecord<E> {
    generation: u64,
    entry: E,
}

#[derive(Debug)]
struct ReadSegment<E> {
    path: PathBuf,
    start_index: u64,
    records: Vec<EntryRecord<E>>,
    record_count: usize,
    valid_length: u64,
    incomplete_tail: bool,
}

pub(super) struct SegmentStore<E> {
    marker_path: PathBuf,
    directory: PathBuf,
    control: ControlState,
    tail: Option<Tail>,
    _entry: PhantomData<fn() -> E>,
}

impl<E> std::fmt::Debug for SegmentStore<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentStore")
            .field("marker_path", &self.marker_path)
            .field("directory", &self.directory)
            .field("control", &self.control)
            .field("tail", &self.tail)
            .finish()
    }
}

impl<E> SegmentStore<E>
where
    E: RaftLogId<NodeId> + Clone + Serialize + DeserializeOwned,
{
    pub(super) fn initialize(
        marker_path: &Path,
    ) -> io::Result<(Self, BTreeMap<u64, E>, ControlState)> {
        let directory = family_directory(marker_path);
        if directory.exists() {
            return Err(invalid_data(format!(
                "unselected Raft-log segment family '{}'",
                directory.display()
            )));
        }

        let parent = marker_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        fs::create_dir(&directory)?;
        sync_directory(parent, PersistenceWriteRole::RaftLogControlState)?;

        let control = ControlState::default();
        let mut store = Self {
            marker_path: marker_path.to_path_buf(),
            directory,
            control,
            tail: None,
            _entry: PhantomData,
        };
        store.persist_control(control)?;

        let marker = VersionMarker {
            version: FORMAT_VERSION,
            artifact: "runnel-raft-log-segments".to_owned(),
        };
        let bytes = serialize_json(
            PersistenceWriteRole::RaftLogControlState,
            PersistenceWriteOperation::Serialize,
            &marker,
        )
        .map_err(invalid_input)?;
        atomic_write_with_role(
            &store.marker_path,
            &bytes,
            PersistenceWriteRole::RaftLogControlState,
        )?;
        Ok((store, BTreeMap::new(), control))
    }

    pub(super) fn open(marker_path: &Path) -> io::Result<(Self, BTreeMap<u64, E>, ControlState)> {
        let marker: VersionMarker = read_json(marker_path)?;
        if marker.version != FORMAT_VERSION || marker.artifact != "runnel-raft-log-segments" {
            return Err(invalid_data(format!(
                "unsupported Raft-log segment marker in '{}'",
                marker_path.display()
            )));
        }

        let directory = family_directory(marker_path);
        if !directory.is_dir() {
            return Err(invalid_data(format!(
                "missing selected Raft-log segment family '{}'",
                directory.display()
            )));
        }
        let control = read_control(&directory)?;
        let mut store = Self {
            marker_path: marker_path.to_path_buf(),
            directory,
            control,
            tail: None,
            _entry: PhantomData,
        };

        store.remove_temporary_files()?;
        if let Some(boundary) = control.truncation_pending_from {
            store.finish_truncation(boundary, control.generation)?;
            let mut complete = control;
            complete.truncation_pending_from = None;
            store.persist_control(complete)?;
        }

        let log = store.finish_purge(store.control)?;
        let recovered_control = store.control;
        Ok((store, log, recovered_control))
    }

    pub(super) fn validate(marker_path: &Path) -> io::Result<()> {
        let marker: VersionMarker = read_json(marker_path)?;
        if marker.version != FORMAT_VERSION || marker.artifact != "runnel-raft-log-segments" {
            return Err(invalid_data(format!(
                "unsupported Raft-log segment marker in '{}'",
                marker_path.display()
            )));
        }
        let directory = family_directory(marker_path);
        if !directory.is_dir() {
            return Err(invalid_data(format!(
                "missing selected Raft-log segment family '{}'",
                directory.display()
            )));
        }
        let control = read_control(&directory)?;
        let store = Self {
            marker_path: marker_path.to_path_buf(),
            directory,
            control,
            tail: None,
            _entry: PhantomData,
        };
        let _ = store.scan_readonly(control)?;
        Ok(())
    }

    pub(super) fn control(&self) -> ControlState {
        self.control
    }

    pub(super) fn persist_control(&mut self, state: ControlState) -> io::Result<()> {
        let state_bytes = serialize_json(
            PersistenceWriteRole::RaftLogControlState,
            PersistenceWriteOperation::Serialize,
            &state,
        )
        .map_err(invalid_input)?;
        let record = ControlRecord {
            state,
            checksum: crc32(&state_bytes),
        };
        let bytes = serialize_json(
            PersistenceWriteRole::RaftLogControlState,
            PersistenceWriteOperation::Serialize,
            &record,
        )
        .map_err(invalid_input)?;
        atomic_write_with_role(
            &self.directory.join("control.json"),
            &bytes,
            PersistenceWriteRole::RaftLogControlState,
        )?;
        self.control = state;
        Ok(())
    }

    pub(super) fn append(&mut self, generation: u64, entries: &[(u64, &[u8])]) -> io::Result<()> {
        let mut cursor = 0;
        let mut created_segment = false;
        while cursor < entries.len() {
            let mut end = cursor;
            let mut payload_size = 0_usize;
            while end < entries.len() {
                let candidate_size = 4_usize
                    .checked_add(entries[end].1.len())
                    .ok_or_else(|| invalid_data("Raft-log segment size overflow"))?;
                let next_payload_size = payload_size.saturating_add(candidate_size);
                if end > cursor && frame_size(next_payload_size) > SEGMENT_TARGET_BYTES {
                    break;
                }
                payload_size = next_payload_size;
                end += 1;
                if frame_size(payload_size) > SEGMENT_TARGET_BYTES {
                    break;
                }
            }

            let batch = &entries[cursor..end];
            let batch_start = batch[0].0;
            let frame = encode_frame(generation, batch_start, batch)?;
            let use_tail = self.tail.as_ref().is_some_and(|tail| {
                tail.length
                    .checked_add(frame.len() as u64)
                    .is_some_and(|length| length <= SEGMENT_TARGET_BYTES as u64)
            });

            if use_tail {
                let tail = self.tail.as_mut().expect("checked above");
                let mut file = OpenOptions::new().append(true).open(&tail.path)?;
                write_all(
                    &mut file,
                    PersistenceWriteRole::RaftLogSegmentAppend,
                    &frame,
                )?;
                crate::persistence_write::measure_io(
                    PersistenceWriteRole::RaftLogSegmentAppend,
                    PersistenceWriteOperation::SyncData,
                    0,
                    || file.sync_data(),
                )?;
                tail.length = tail.length.saturating_add(frame.len() as u64);
            } else {
                let path = self.directory.join(segment_file_name(batch_start));
                if path.exists() {
                    return Err(invalid_data(format!(
                        "Raft-log segment already exists at index {batch_start}"
                    )));
                }
                let temp_path = path.with_extension(format!("tmp-{}", std::process::id()));
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temp_path)?;
                let header = segment_header(batch_start);
                write_all(
                    &mut file,
                    PersistenceWriteRole::RaftLogSegmentAppend,
                    &header,
                )?;
                write_all(
                    &mut file,
                    PersistenceWriteRole::RaftLogSegmentAppend,
                    &frame,
                )?;
                crate::persistence_write::measure_io(
                    PersistenceWriteRole::RaftLogSegmentAppend,
                    PersistenceWriteOperation::SyncAll,
                    0,
                    || file.sync_all(),
                )?;
                crate::persistence_write::measure_io(
                    PersistenceWriteRole::RaftLogSegmentAppend,
                    PersistenceWriteOperation::Rename,
                    0,
                    || fs::rename(&temp_path, &path),
                )?;
                self.tail = Some(Tail {
                    path,
                    length: (header.len() + frame.len()) as u64,
                });
                created_segment = true;
            }
            cursor = end;
        }

        if created_segment {
            sync_directory(&self.directory, PersistenceWriteRole::RaftLogSegmentAppend)?;
        }
        Ok(())
    }

    pub(super) fn truncate(&mut self, boundary: u64, generation: u64) -> io::Result<()> {
        let state = ControlState {
            generation,
            truncation_pending_from: Some(boundary),
            ..self.control
        };
        self.persist_control(state)?;
        self.finish_truncation(boundary, generation)?;
        self.persist_control(ControlState {
            truncation_pending_from: None,
            ..state
        })?;
        let (_, tail) = self.scan_and_recover(self.control)?;
        self.tail = tail;
        Ok(())
    }

    pub(super) fn purge(&mut self, floor: Option<LogId<NodeId>>) -> io::Result<()> {
        let state = ControlState {
            last_purged_log_id: floor,
            ..self.control
        };
        self.persist_control(state)?;
        let _ = self.finish_purge(state)?;
        Ok(())
    }

    fn finish_purge(&mut self, state: ControlState) -> io::Result<BTreeMap<u64, E>> {
        let segments = read_segments::<E>(&self.directory, state)?;
        let mut retained = Vec::with_capacity(segments.len());
        let mut changed = false;
        for segment in segments {
            let last_index = segment
                .records
                .last()
                .map(|record| record.entry.get_log_id().index)
                .unwrap_or(segment.start_index.saturating_sub(1));
            let purged = state
                .last_purged_log_id
                .is_some_and(|log_id| last_index <= log_id.index);
            if purged {
                fs::remove_file(&segment.path)?;
                changed = true;
            } else {
                retained.push(segment);
            }
        }
        if changed {
            sync_directory(&self.directory, PersistenceWriteRole::RaftLogSegmentAppend)?;
        }

        let (log, tail) = self.recover_segments(retained, state)?;
        self.tail = tail;
        Ok(log)
    }

    fn remove_temporary_files(&self) -> io::Result<()> {
        let mut changed = false;
        for item in fs::read_dir(&self.directory)? {
            let item = item?;
            let name = item.file_name();
            if is_temporary_artifact(&name.to_string_lossy()) && item.file_type()?.is_file() {
                fs::remove_file(item.path())?;
                changed = true;
            }
        }
        if changed {
            sync_directory(&self.directory, PersistenceWriteRole::RaftLogSegmentAppend)?;
        }
        Ok(())
    }

    fn finish_truncation(&mut self, boundary: u64, generation: u64) -> io::Result<()> {
        let mut changed = false;
        for item in fs::read_dir(&self.directory)? {
            let item = item?;
            let path = item.path();
            if !path
                .extension()
                .is_some_and(|extension| extension == "rlog")
            {
                continue;
            }
            let name = path
                .file_stem()
                .and_then(|name| name.to_str())
                .ok_or_else(|| invalid_data("invalid Raft-log segment filename"))?;
            let start_index = name
                .strip_prefix("segment-")
                .and_then(|index| index.parse::<u64>().ok())
                .ok_or_else(|| {
                    invalid_data(format!("invalid Raft-log segment filename '{name}'"))
                })?;
            if start_index >= boundary {
                fs::remove_file(path)?;
                changed = true;
            }
        }
        if changed {
            sync_directory(&self.directory, PersistenceWriteRole::RaftLogSegmentAppend)?;
        }

        for segment in read_segments::<E>(&self.directory, self.control)? {
            let record_count = segment.record_count;
            let retained = segment
                .records
                .into_iter()
                .filter(|record| {
                    record.entry.get_log_id().index < boundary && record.generation < generation
                })
                .collect::<Vec<_>>();
            if retained.len() == record_count {
                continue;
            }
            if retained.is_empty() {
                fs::remove_file(segment.path)?;
            } else {
                rewrite_segment(&segment.path, segment.start_index, &retained)?;
            }
            changed = true;
        }
        if changed {
            sync_directory(&self.directory, PersistenceWriteRole::RaftLogSegmentAppend)?;
        }
        Ok(())
    }

    fn scan_and_recover(
        &self,
        state: ControlState,
    ) -> io::Result<(BTreeMap<u64, E>, Option<Tail>)> {
        let segments = read_segments::<E>(&self.directory, state)?;
        self.recover_segments(segments, state)
    }

    fn recover_segments(
        &self,
        mut segments: Vec<ReadSegment<E>>,
        state: ControlState,
    ) -> io::Result<(BTreeMap<u64, E>, Option<Tail>)> {
        if segments
            .iter()
            .take(segments.len().saturating_sub(1))
            .any(|segment| segment.incomplete_tail)
        {
            return Err(invalid_data(
                "incomplete Raft-log batch occurs before the final segment",
            ));
        }
        if let Some(last) = segments.last_mut()
            && last.incomplete_tail
        {
            let file = OpenOptions::new().write(true).open(&last.path)?;
            file.set_len(last.valid_length)?;
            file.sync_all()?;
            last.incomplete_tail = false;
        }
        build_log(segments, state)
    }

    fn scan_readonly(&self, state: ControlState) -> io::Result<BTreeMap<u64, E>> {
        let segments = read_segments::<E>(&self.directory, state)?;
        if segments
            .iter()
            .take(segments.len().saturating_sub(1))
            .any(|segment| segment.incomplete_tail)
        {
            return Err(invalid_data(
                "incomplete Raft-log batch occurs before the final segment",
            ));
        }
        let (log, _) = build_log(segments, state)?;
        Ok(log)
    }
}

pub(super) fn family_directory(marker_path: &Path) -> PathBuf {
    marker_path.with_extension("segments")
}

pub(super) fn read_version(marker_path: &Path) -> io::Result<u32> {
    let bytes = fs::read(marker_path)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(invalid_input)?;
    value
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
        .ok_or_else(|| invalid_data("Raft-log marker has no valid version"))
}

fn read_control(directory: &Path) -> io::Result<ControlState> {
    let record: ControlRecord = read_json(&directory.join("control.json"))?;
    let state_bytes = serde_json::to_vec(&record.state).map_err(invalid_input)?;
    if crc32(&state_bytes) != record.checksum {
        return Err(invalid_data("Raft-log control checksum mismatch"));
    }
    Ok(record.state)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> io::Result<T> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid_data(format!(
            "Raft-log artifact '{}' is not a regular file",
            path.display()
        )));
    }
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(invalid_input)
}

pub(super) fn segment_header(start_index: u64) -> Vec<u8> {
    let mut header = Vec::with_capacity(SEGMENT_HEADER_LEN);
    header.extend_from_slice(SEGMENT_MAGIC);
    header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    header.extend_from_slice(&start_index.to_le_bytes());
    header
}

pub(super) fn encode_frame(
    start_generation: u64,
    start_index: u64,
    entries: &[(u64, &[u8])],
) -> io::Result<Vec<u8>> {
    let capacity = entries
        .iter()
        .fold(0_usize, |size, (_, bytes)| {
            size.saturating_add(4_usize.saturating_add(bytes.len()))
        })
        .saturating_add(BATCH_HEADER_LEN + BATCH_TRAILER_LEN);
    let mut frame = Vec::with_capacity(capacity);
    frame.extend_from_slice(BATCH_MAGIC);
    frame.extend_from_slice(&start_generation.to_le_bytes());
    frame.extend_from_slice(&start_index.to_le_bytes());
    let count = u32::try_from(entries.len())
        .map_err(|_| invalid_data("Raft-log segment batch has too many entries"))?;
    frame.extend_from_slice(&count.to_le_bytes());
    let payload_length_offset = frame.len();
    frame.extend_from_slice(&0_u64.to_le_bytes());
    let header_checksum_offset = frame.len();
    frame.extend_from_slice(&0_u32.to_le_bytes());
    for (expected_index, (index, bytes)) in entries.iter().enumerate() {
        let expected_index = start_index
            .checked_add(expected_index as u64)
            .ok_or_else(|| invalid_data("Raft-log index overflow"))?;
        if *index != expected_index {
            return Err(invalid_data(format!(
                "non-contiguous Raft-log append: expected index {expected_index}, found {index}"
            )));
        }
        let length = u32::try_from(bytes.len())
            .map_err(|_| invalid_data("Raft-log entry exceeds the frame length field"))?;
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(bytes);
    }
    let payload_length = u64::try_from(frame.len() - BATCH_HEADER_LEN)
        .map_err(|_| invalid_data("Raft-log batch payload length overflow"))?;
    frame[payload_length_offset..payload_length_offset + 8]
        .copy_from_slice(&payload_length.to_le_bytes());
    let header_checksum = crc32(&frame[..header_checksum_offset]);
    frame[header_checksum_offset..header_checksum_offset + 4]
        .copy_from_slice(&header_checksum.to_le_bytes());
    let checksum = crc32(&frame);
    frame.extend_from_slice(&checksum.to_le_bytes());
    frame.extend_from_slice(BATCH_END);
    Ok(frame)
}

fn parse_segment<E>(
    path: &Path,
    expected_start: u64,
    state: ControlState,
) -> io::Result<ReadSegment<E>>
where
    E: RaftLogId<NodeId> + DeserializeOwned,
{
    let mut bytes = Vec::new();
    fs::File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < SEGMENT_HEADER_LEN {
        return Err(invalid_data(format!(
            "truncated Raft-log segment header in '{}'",
            path.display()
        )));
    }
    if &bytes[..4] != SEGMENT_MAGIC {
        return Err(invalid_data(format!(
            "invalid Raft-log segment magic in '{}'",
            path.display()
        )));
    }
    let version = read_u32(&bytes, 4)?;
    if version != FORMAT_VERSION {
        return Err(invalid_data(format!(
            "unsupported Raft-log segment version {version} in '{}'",
            path.display()
        )));
    }
    let start_index = read_u64(&bytes, 8)?;
    if start_index != expected_start {
        return Err(invalid_data(format!(
            "Raft-log segment filename index {expected_start} does not match header index {start_index}"
        )));
    }

    let mut records = Vec::new();
    let mut record_count = 0;
    let mut offset = SEGMENT_HEADER_LEN;
    let mut incomplete_tail = false;
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < BATCH_HEADER_LEN {
            incomplete_tail = true;
            break;
        }
        if &bytes[offset..offset + 4] != BATCH_MAGIC {
            return Err(invalid_data(format!(
                "invalid Raft-log batch marker in '{}' at byte {offset}",
                path.display()
            )));
        }
        let generation = read_u64(&bytes, offset + 4)?;
        let batch_start = read_u64(&bytes, offset + 12)?;
        let count = read_u32(&bytes, offset + 20)? as usize;
        let payload_length = usize::try_from(read_u64(&bytes, offset + 24)?)
            .map_err(|_| invalid_data("Raft-log batch payload is too large"))?;
        let header_checksum = read_u32(&bytes, offset + 32)?;
        if crc32(&bytes[offset..offset + 32]) != header_checksum {
            return Err(invalid_data(format!(
                "Raft-log batch header checksum mismatch in '{}' at byte {offset}",
                path.display()
            )));
        }
        let frame_length = BATCH_HEADER_LEN
            .checked_add(payload_length)
            .and_then(|length| length.checked_add(BATCH_TRAILER_LEN))
            .ok_or_else(|| invalid_data("Raft-log batch length overflow"))?;
        if frame_length > remaining {
            incomplete_tail = true;
            break;
        }
        let checksum_position = offset + BATCH_HEADER_LEN + payload_length;
        let checksum = read_u32(&bytes, checksum_position)?;
        if &bytes[checksum_position + 4..checksum_position + 8] != BATCH_END {
            return Err(invalid_data(format!(
                "invalid Raft-log batch completion marker in '{}' at byte {offset}",
                path.display()
            )));
        }
        if crc32(&bytes[offset..checksum_position]) != checksum {
            return Err(invalid_data(format!(
                "Raft-log batch checksum mismatch in '{}' at byte {offset}",
                path.display()
            )));
        }
        if count == 0 {
            return Err(invalid_data("Raft-log batch cannot be empty"));
        }
        let mut payload_offset = offset + BATCH_HEADER_LEN;
        for ordinal in 0..count {
            let payload_end = offset + BATCH_HEADER_LEN + payload_length;
            if payload_offset + 4 > payload_end {
                return Err(invalid_data(
                    "truncated Raft-log entry frame in completed batch",
                ));
            }
            let entry_length = read_u32(&bytes, payload_offset)? as usize;
            payload_offset += 4;
            let entry_end = payload_offset
                .checked_add(entry_length)
                .ok_or_else(|| invalid_data("Raft-log entry frame length overflow"))?;
            if entry_end > payload_end {
                return Err(invalid_data("truncated Raft-log entry in completed batch"));
            }
            let entry: E =
                serde_json::from_slice(&bytes[payload_offset..entry_end]).map_err(invalid_input)?;
            let expected_index = batch_start
                .checked_add(ordinal as u64)
                .ok_or_else(|| invalid_data("Raft-log entry index overflow"))?;
            if entry.get_log_id().index != expected_index {
                return Err(invalid_data(format!(
                    "Raft-log entry index {} does not match batch index {expected_index}",
                    entry.get_log_id().index
                )));
            }
            let retain_record = match state.truncation_pending_from {
                None => true,
                Some(boundary) if expected_index >= boundary => false,
                Some(_) if generation < state.generation => true,
                Some(_) => {
                    return Err(invalid_data(format!(
                        "Raft-log entry at index {expected_index} belongs to the pending truncation generation"
                    )));
                }
            };
            if retain_record {
                records.push(EntryRecord { generation, entry });
            }
            record_count += 1;
            payload_offset = entry_end;
        }
        if payload_offset != offset + BATCH_HEADER_LEN + payload_length {
            return Err(invalid_data(
                "Raft-log batch has trailing entry payload bytes",
            ));
        }
        offset += frame_length;
    }

    if record_count == 0 {
        return Err(invalid_data(format!(
            "Raft-log segment '{}' contains no complete batch",
            path.display()
        )));
    }

    Ok(ReadSegment {
        path: path.to_path_buf(),
        start_index,
        records,
        record_count,
        valid_length: offset as u64,
        incomplete_tail,
    })
}

fn read_segments<E>(directory: &Path, state: ControlState) -> io::Result<Vec<ReadSegment<E>>>
where
    E: RaftLogId<NodeId> + DeserializeOwned,
{
    let mut paths = Vec::new();
    for item in fs::read_dir(directory)? {
        let item = item?;
        let path = item.path();
        let name = item.file_name();
        let name = name.to_string_lossy();
        let file_type = item.file_type()?;
        if name == "control.json" && file_type.is_file() {
            continue;
        }
        if is_temporary_artifact(&name) && file_type.is_file() {
            continue;
        }
        if !name.starts_with("segment-") || !name.ends_with(".rlog") || !file_type.is_file() {
            return Err(invalid_data(format!(
                "unexpected Raft-log artifact '{}'",
                path.display()
            )));
        }
        paths.push(path);
    }
    paths.sort();
    let mut segments = Vec::with_capacity(paths.len());
    for path in paths {
        let name = path
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid_data("invalid Raft-log segment filename"))?;
        let start_index = name
            .strip_prefix("segment-")
            .and_then(|index| index.parse::<u64>().ok())
            .ok_or_else(|| invalid_data(format!("invalid Raft-log segment filename '{name}'")))?;
        segments.push(parse_segment(&path, start_index, state)?);
    }
    Ok(segments)
}

fn build_log<E>(
    segments: Vec<ReadSegment<E>>,
    state: ControlState,
) -> io::Result<(BTreeMap<u64, E>, Option<Tail>)>
where
    E: RaftLogId<NodeId>,
{
    let mut log = BTreeMap::new();
    let mut tail = None;
    for segment in segments {
        for record in segment.records {
            let index = record.entry.get_log_id().index;
            if state
                .last_purged_log_id
                .is_some_and(|purged| index <= purged.index)
            {
                continue;
            }
            if log.insert(index, record.entry).is_some() {
                return Err(invalid_data(format!(
                    "duplicate Raft-log entry at index {index}"
                )));
            }
        }
        tail = Some(Tail {
            path: segment.path,
            length: segment.valid_length,
        });
        if segment.incomplete_tail {
            // The caller decides whether to repair the final segment before using its tail.
        }
    }

    let mut expected = match state.last_purged_log_id {
        Some(log_id) => log_id.index.checked_add(1),
        None => Some(0),
    };
    for (index, entry) in &log {
        if Some(*index) != expected {
            return Err(invalid_data(format!(
                "expected contiguous Raft-log entry at index {:?}, found {index}",
                expected
            )));
        }
        if entry.get_log_id().index != *index {
            return Err(invalid_data(format!(
                "Raft-log map index {index} does not match its entry"
            )));
        }
        expected = index.checked_add(1);
    }

    if let Some(committed) = state.committed {
        let last_index = log
            .keys()
            .next_back()
            .copied()
            .or_else(|| state.last_purged_log_id.map(|log_id| log_id.index));
        let Some(last_index) = last_index else {
            return Err(invalid_data(format!(
                "committed log index {} is present but the persisted log is empty",
                committed.index
            )));
        };
        if committed.index > last_index {
            return Err(invalid_data(format!(
                "committed log index {} is beyond last persisted log index {last_index}",
                committed.index
            )));
        }
        if let Some(entry) = log.get(&committed.index)
            && *entry.get_log_id() != committed
        {
            return Err(invalid_data(format!(
                "committed Raft-log id {committed:?} does not match entry at index {}",
                committed.index
            )));
        }
    }

    Ok((log, tail))
}

fn rewrite_segment<E>(path: &Path, start_index: u64, records: &[EntryRecord<E>]) -> io::Result<()>
where
    E: RaftLogId<NodeId> + Serialize,
{
    let mut bytes = segment_header(start_index);
    let mut cursor = 0;
    while cursor < records.len() {
        let generation = records[cursor].generation;
        let mut batch = Vec::new();
        while cursor < records.len() && records[cursor].generation == generation {
            let encoded = serialize_json(
                PersistenceWriteRole::RaftLogSegmentAppend,
                PersistenceWriteOperation::Serialize,
                &records[cursor].entry,
            )
            .map_err(invalid_input)?;
            batch.push((records[cursor].entry.get_log_id().index, encoded));
            cursor += 1;
        }
        let entries = batch
            .iter()
            .map(|(index, encoded)| (*index, encoded.as_slice()))
            .collect::<Vec<_>>();
        bytes.extend_from_slice(&encode_frame(generation, batch[0].0, &entries)?);
    }
    let temp_path = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;
    write_all(
        &mut file,
        PersistenceWriteRole::RaftLogSegmentAppend,
        &bytes,
    )?;
    crate::persistence_write::measure_io(
        PersistenceWriteRole::RaftLogSegmentAppend,
        PersistenceWriteOperation::SyncAll,
        0,
        || file.sync_all(),
    )?;
    crate::persistence_write::measure_io(
        PersistenceWriteRole::RaftLogSegmentAppend,
        PersistenceWriteOperation::Rename,
        0,
        || fs::rename(temp_path, path),
    )?;
    Ok(())
}

fn segment_file_name(start_index: u64) -> String {
    format!("segment-{start_index:020}.rlog")
}

fn is_temporary_artifact(name: &str) -> bool {
    let process_id = if let Some(process_id) = name.strip_prefix("control.tmp-") {
        process_id
    } else if let Some(segment) = name.strip_prefix("segment-") {
        let Some((index, process_id)) = segment.split_once(".tmp-") else {
            return false;
        };
        if index.len() != 20
            || !index.bytes().all(|byte| byte.is_ascii_digit())
            || index.parse::<u64>().is_err()
        {
            return false;
        }
        process_id
    } else {
        return false;
    };
    process_id.bytes().all(|byte| byte.is_ascii_digit()) && process_id.parse::<u32>().is_ok()
}

fn frame_size(payload_size: usize) -> usize {
    BATCH_HEADER_LEN
        .saturating_add(payload_size)
        .saturating_add(BATCH_TRAILER_LEN)
}

fn sync_directory(path: &Path, role: PersistenceWriteRole) -> io::Result<()> {
    crate::persistence_write::measure_io(role, PersistenceWriteOperation::DirectorySync, 0, || {
        fs::File::open(path)?.sync_all()
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> io::Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_data("truncated Raft-log integer"))?;
    Ok(u32::from_le_bytes(
        value.try_into().expect("slice length checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> io::Result<u64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid_data("truncated Raft-log integer"))?;
    Ok(u64::from_le_bytes(
        value.try_into().expect("slice length checked"),
    ))
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0_u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

fn invalid_input(error: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
