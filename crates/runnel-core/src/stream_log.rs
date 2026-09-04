use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use runnel_engine::{
    BrokerError, Message, Offset, PublishRecord, PublishRecordOutcome, ReplayMessage,
};

use super::DurableFormat;

pub(super) const LEGACY_MAGIC: &[u8; 4] = b"RNL1";
pub(super) const VERSIONED_MAGIC: &[u8; 4] = b"RNL2";
pub(super) const REQUEST_ID_MAGIC: &[u8; 4] = b"RNL3";
pub(super) const LEGACY_HEADER_LEN: usize = 28;
pub(super) const VERSIONED_HEADER_LEN: usize = 44;
pub(super) const REQUEST_ID_HEADER_LEN: usize = 48;
pub(super) const VERSIONED_FORMAT_VERSION: u8 = 1;
pub(super) const REQUEST_ID_FORMAT_VERSION: u8 = 1;
const VERSIONED_ENCODING_BYTES: u8 = 0;
const VERSIONED_COMPRESSION_NONE: u8 = 0;
pub(super) const VERSIONED_MAX_KEY_LEN: u32 = 128;
pub(super) const VERSIONED_MAX_BODY_LEN: u32 = 64 * 1024 * 1024;
pub(super) const REQUEST_ID_MAX_LEN: u32 = 1024;
pub(super) const REQUEST_ID_MAX_KEY_LEN: u32 = 128;
pub(super) const REQUEST_ID_MAX_BODY_LEN: u32 = 64 * 1024 * 1024;
pub(super) const MAX_IN_MEMORY_RECORDS: usize = 1024;
const SPARSE_INDEX_STRIDE: Offset = 64;
const MAX_SPARSE_INDEX_ENTRIES: usize = 1024;

#[derive(Debug, Clone, Copy)]
struct RequestAwareLimits {
    max_key_len: u32,
    max_body_len: u32,
}

fn request_aware_limits(durable_format: DurableFormat) -> RequestAwareLimits {
    match durable_format {
        // Keep the legacy no-request-id writer and reader unchanged. Request-aware frames have
        // their own bounded limits so malformed headers cannot force unbounded allocations.
        DurableFormat::Rnl1 => RequestAwareLimits {
            max_key_len: REQUEST_ID_MAX_KEY_LEN,
            max_body_len: REQUEST_ID_MAX_BODY_LEN,
        },
        DurableFormat::VersionedV1 => RequestAwareLimits {
            max_key_len: VERSIONED_MAX_KEY_LEN,
            max_body_len: VERSIONED_MAX_BODY_LEN,
        },
    }
}

pub(super) struct StreamLog {
    file: File,
    durable_format: DurableFormat,
    // The durable log retains the complete history; this tail cache keeps normal delivery
    // bounded while older replay requests use the bounded sparse index as a scan starting point.
    records: VecDeque<RecordIndex>,
    sparse_index: VecDeque<LogCheckpoint>,
    request_ids: HashMap<String, Offset>,
    next_offset: Offset,
}

impl StreamLog {
    pub(super) fn create(path: &Path, durable_format: DurableFormat) -> Result<Self, BrokerError> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file,
            durable_format,
            records: VecDeque::with_capacity(MAX_IN_MEMORY_RECORDS),
            sparse_index: VecDeque::with_capacity(MAX_SPARSE_INDEX_ENTRIES),
            request_ids: HashMap::new(),
            next_offset: 0,
        })
    }

    pub(super) fn open(path: &Path, durable_format: DurableFormat) -> Result<Self, BrokerError> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let file_len = file.metadata()?.len();
        let mut records = VecDeque::with_capacity(MAX_IN_MEMORY_RECORDS);
        let mut sparse_index = VecDeque::with_capacity(MAX_SPARSE_INDEX_ENTRIES);
        let mut request_ids = HashMap::new();
        let mut cursor = 0;
        let mut next_offset = 0;
        while let Some(parsed) = read_record(&mut file, cursor, file_len, durable_format)? {
            remember_checkpoint(&mut sparse_index, parsed.index.offset, cursor);
            cursor = parsed.next_cursor;
            next_offset = parsed.index.offset.saturating_add(1);
            if let Some(request_id) = parsed.index.request_id.as_ref() {
                request_ids
                    .entry(request_id.clone())
                    .or_insert(parsed.index.offset);
            }
            remember_record(&mut records, parsed.index);
        }

        if cursor != file_len {
            file.set_len(cursor)?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            durable_format,
            records,
            sparse_index,
            request_ids,
            next_offset,
        })
    }

    pub(super) fn request_offset(&self, request_id: &str) -> Option<Offset> {
        self.request_ids.get(request_id).copied()
    }

    #[cfg(test)]
    pub(super) fn request_id_count(&self) -> usize {
        self.request_ids.len()
    }

    pub(super) fn storage_bytes(&self) -> Result<u64, BrokerError> {
        Ok(self.file.metadata()?.len())
    }

    #[cfg(test)]
    pub(super) fn in_memory_record_count(&self) -> usize {
        self.records.len()
    }

    #[cfg(test)]
    pub(super) fn first_in_memory_offset(&self) -> Option<Offset> {
        self.records.front().map(|record| record.offset)
    }

    #[cfg(test)]
    pub(super) fn next_offset(&self) -> Offset {
        self.next_offset
    }

    #[cfg(test)]
    pub(super) fn sparse_index_len(&self) -> usize {
        self.sparse_index.len()
    }

    #[cfg(test)]
    pub(super) fn first_sparse_offset(&self) -> Option<Offset> {
        self.sparse_index
            .front()
            .map(|checkpoint| checkpoint.offset)
    }

    #[cfg(test)]
    pub(super) fn last_sparse_offset(&self) -> Option<Offset> {
        self.sparse_index.back().map(|checkpoint| checkpoint.offset)
    }

    pub(super) fn append(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
    ) -> Result<Offset, BrokerError> {
        self.append_with_sync(key, payload, true)
    }

    fn append_with_sync(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        sync: bool,
    ) -> Result<Offset, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = runnel_engine::StageTimer::new("core.storage_append");
        if self.durable_format == DurableFormat::VersionedV1 {
            return self.append_versioned_with_sync(key, payload, sync);
        }

        let key_bytes = key.as_deref().unwrap_or_default().as_bytes();
        let key_len = u32::try_from(key_bytes.len()).map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message key exceeds u32 length",
            ))
        })?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message payload exceeds u32 length",
            ))
        })?;
        let offset = self.next_offset;
        let published_at_ms = now_ms();

        let mut header = Vec::with_capacity(LEGACY_HEADER_LEN);
        header.extend_from_slice(LEGACY_MAGIC);
        header.extend_from_slice(&offset.to_le_bytes());
        header.extend_from_slice(&published_at_ms.to_le_bytes());
        header.extend_from_slice(&key_len.to_le_bytes());
        header.extend_from_slice(&payload_len.to_le_bytes());

        self.file.write_all(&header)?;
        self.file.write_all(key_bytes)?;
        self.file.write_all(&payload)?;
        if sync {
            self.file.sync_data()?;
        }

        let payload_offset = self.file.stream_position()? - payload.len() as u64;
        let record_cursor = payload_offset - key_bytes.len() as u64 - LEGACY_HEADER_LEN as u64;
        remember_checkpoint(&mut self.sparse_index, offset, record_cursor);
        remember_record(
            &mut self.records,
            RecordIndex {
                offset,
                payload_offset,
                payload_len,
                key,
                request_id: None,
                published_at_ms,
            },
        );
        self.next_offset = offset.saturating_add(1);
        Ok(offset)
    }

    fn append_versioned_with_sync(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        sync: bool,
    ) -> Result<Offset, BrokerError> {
        let key_bytes = key.as_deref().unwrap_or_default().as_bytes();
        let key_len = u32::try_from(key_bytes.len()).map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message key exceeds u32 length",
            ))
        })?;
        if key_len > VERSIONED_MAX_KEY_LEN {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message key exceeds versioned storage limit",
            )));
        }
        let body_len = u32::try_from(payload.len()).map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message payload exceeds u32 length",
            ))
        })?;
        if body_len > VERSIONED_MAX_BODY_LEN {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message payload exceeds versioned storage limit",
            )));
        }

        let offset = self.next_offset;
        let published_at_ms = now_ms();
        let mut header = [0; VERSIONED_HEADER_LEN];
        header[..4].copy_from_slice(VERSIONED_MAGIC);
        header[4] = VERSIONED_FORMAT_VERSION;
        header[6..8].copy_from_slice(&(VERSIONED_HEADER_LEN as u16).to_le_bytes());
        header[8..12].copy_from_slice(&body_len.to_le_bytes());
        header[12..16].copy_from_slice(&body_len.to_le_bytes());
        header[16..24].copy_from_slice(&offset.to_le_bytes());
        header[24..32].copy_from_slice(&published_at_ms.to_le_bytes());
        header[32..36].copy_from_slice(&key_len.to_le_bytes());
        header[36] = VERSIONED_ENCODING_BYTES;
        header[37] = VERSIONED_COMPRESSION_NONE;
        let checksum = versioned_checksum(&header, key_bytes, &payload);
        header[40..44].copy_from_slice(&checksum.to_le_bytes());

        self.file.write_all(&header)?;
        self.file.write_all(key_bytes)?;
        self.file.write_all(&payload)?;
        if sync {
            self.file.sync_data()?;
        }

        let payload_offset = self.file.stream_position()? - payload.len() as u64;
        let record_cursor = payload_offset - key_bytes.len() as u64 - VERSIONED_HEADER_LEN as u64;
        remember_checkpoint(&mut self.sparse_index, offset, record_cursor);
        remember_record(
            &mut self.records,
            RecordIndex {
                offset,
                payload_offset,
                payload_len: body_len,
                key,
                request_id: None,
                published_at_ms,
            },
        );
        self.next_offset = offset.saturating_add(1);
        Ok(offset)
    }

    pub(super) fn append_with_request_id(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: String,
    ) -> Result<Offset, BrokerError> {
        self.append_with_request_id_sync(key, payload, request_id, true)
    }

    pub(super) fn append_with_move_id(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        move_id: String,
    ) -> Result<Offset, BrokerError> {
        if let Some(offset) = self.request_ids.get(&move_id).copied() {
            let existing = self.find_record(offset)?;
            if existing.key.as_ref() != key.as_ref() || self.read_payload(&existing)? != payload {
                return Err(invalid_record_data(
                    "dead-letter move identity has different key or payload",
                ));
            }
            return Ok(offset);
        }

        // Move identities are internal request-aware records. Unlike public request IDs, their
        // key and payload are part of the identity invariant and are checked on every retry.
        self.append_with_request_id(key, payload, move_id)
    }

    fn append_with_request_id_sync(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: String,
        sync: bool,
    ) -> Result<Offset, BrokerError> {
        let limits = request_aware_limits(self.durable_format);
        let key_bytes = key.as_deref().unwrap_or_default().as_bytes();
        let key_len = u32::try_from(key_bytes.len()).map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message key exceeds u32 length",
            ))
        })?;
        if key_len > limits.max_key_len {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message key exceeds request-aware storage limit",
            )));
        }
        let request_id_bytes = request_id.as_bytes();
        let request_id_len = u32::try_from(request_id_bytes.len()).map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request ID exceeds u32 length",
            ))
        })?;
        if request_id_len > REQUEST_ID_MAX_LEN {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "request ID exceeds request-aware storage limit",
            )));
        }
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message payload exceeds u32 length",
            ))
        })?;
        if payload_len > limits.max_body_len {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message payload exceeds request-aware storage limit",
            )));
        }
        let offset = self.next_offset;
        let published_at_ms = now_ms();
        let mut header = [0; REQUEST_ID_HEADER_LEN];
        header[..4].copy_from_slice(REQUEST_ID_MAGIC);
        header[4] = REQUEST_ID_FORMAT_VERSION;
        header[6..8].copy_from_slice(&(REQUEST_ID_HEADER_LEN as u16).to_le_bytes());
        header[8..12].copy_from_slice(&payload_len.to_le_bytes());
        header[12..16].copy_from_slice(&payload_len.to_le_bytes());
        header[16..24].copy_from_slice(&offset.to_le_bytes());
        header[24..32].copy_from_slice(&published_at_ms.to_le_bytes());
        header[32..36].copy_from_slice(&key_len.to_le_bytes());
        header[36..40].copy_from_slice(&request_id_len.to_le_bytes());
        let checksum = request_id_checksum(&header, key_bytes, request_id_bytes, &payload);
        header[44..48].copy_from_slice(&checksum.to_le_bytes());

        self.file.write_all(&header)?;
        self.file.write_all(key_bytes)?;
        self.file.write_all(request_id_bytes)?;
        self.file.write_all(&payload)?;
        if sync {
            self.file.sync_data()?;
        }

        let payload_offset = self.file.stream_position()? - payload.len() as u64;
        let record_cursor = payload_offset
            - request_id_bytes.len() as u64
            - key_bytes.len() as u64
            - REQUEST_ID_HEADER_LEN as u64;
        remember_checkpoint(&mut self.sparse_index, offset, record_cursor);
        remember_record(
            &mut self.records,
            RecordIndex {
                offset,
                payload_offset,
                payload_len,
                key,
                request_id: Some(request_id.clone()),
                published_at_ms,
            },
        );
        self.request_ids.insert(request_id, offset);
        self.next_offset = offset.saturating_add(1);
        Ok(offset)
    }

    pub(super) fn append_batch(
        &mut self,
        records: Vec<PublishRecord>,
    ) -> Result<Vec<PublishRecordOutcome>, BrokerError> {
        let mut outcomes = Vec::with_capacity(records.len());
        let mut appended = false;

        for PublishRecord {
            key,
            payload,
            request_id,
        } in records
        {
            if let Some(request_id) = request_id.as_ref()
                && let Some(offset) = self.request_ids.get(request_id)
            {
                outcomes.push(Ok(*offset));
                continue;
            }

            let outcome = match request_id {
                Some(request_id) => {
                    self.append_with_request_id_sync(key, payload, request_id, false)
                }
                None => self.append_with_sync(key, payload, false),
            };
            match outcome {
                Ok(offset) => {
                    appended = true;
                    outcomes.push(Ok(offset));
                }
                Err(error) if is_invalid_input(&error) => outcomes.push(Err(error)),
                Err(error) => return Err(error),
            }
        }

        if appended {
            self.file.sync_data()?;
        }
        Ok(outcomes)
    }

    pub(super) fn read_message(
        &mut self,
        stream: &str,
        offset: Offset,
    ) -> Result<Message, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = runnel_engine::StageTimer::new("core.storage_read");
        let index = self.find_record(offset)?;
        let payload = self.read_payload(&index)?;
        Ok(Message {
            stream: stream.to_owned(),
            offset: index.offset,
            key: index.key.clone(),
            payload,
            published_at_ms: index.published_at_ms,
            delivery_token: None,
            delivery_attempt: None,
        })
    }

    pub(super) fn read_replay_message(
        &mut self,
        stream: &str,
        offset: Offset,
    ) -> Result<ReplayMessage, BrokerError> {
        if offset >= self.next_offset {
            return Err(BrokerError::HistoryUnavailable {
                stream: stream.to_owned(),
                requested_offset: offset,
                earliest_offset: 0,
                next_offset: self.next_offset,
            });
        }

        let index = self.find_record(offset)?;
        let payload = self.read_payload(&index)?;
        Ok(ReplayMessage {
            stream: stream.to_owned(),
            offset: index.offset,
            key: index.key,
            payload,
            published_at_ms: index.published_at_ms,
        })
    }

    fn read_payload(&mut self, record: &RecordIndex) -> Result<Vec<u8>, BrokerError> {
        let mut payload = vec![0; record.payload_len as usize];
        self.file.seek(SeekFrom::Start(record.payload_offset))?;
        self.file.read_exact(&mut payload)?;
        Ok(payload)
    }

    pub(super) fn find_candidate(
        &mut self,
        committed_offset: Offset,
        acknowledged_offsets: &BTreeSet<Offset>,
        in_flight: Option<(&HashSet<Offset>, &HashSet<String>)>,
    ) -> Result<Option<RecordIndex>, BrokerError> {
        let Some(first_indexed_offset) = self.records.front().map(|record| record.offset) else {
            return Ok(None);
        };
        if committed_offset >= first_indexed_offset {
            let start = self.tail_start_index(committed_offset);
            return Ok(self
                .records
                .iter()
                .skip(start)
                .find(|record| record_is_candidate(record, acknowledged_offsets, in_flight))
                .cloned());
        }

        // A consumer that has fallen behind the bounded tail index still has the same replay
        // rights. Start at the nearest sparse checkpoint so cold replay does not always scan
        // from byte zero.
        let file_len = self.file.metadata()?.len();
        let mut cursor = self.scan_start(committed_offset);
        while let Some(parsed) = read_record(&mut self.file, cursor, file_len, self.durable_format)?
        {
            cursor = parsed.next_cursor;
            if parsed.index.offset < committed_offset {
                continue;
            }
            if record_is_candidate(&parsed.index, acknowledged_offsets, in_flight) {
                return Ok(Some(parsed.index));
            }
        }
        Ok(None)
    }

    fn tail_start_index(&self, offset: Offset) -> usize {
        let mut low = 0;
        let mut high = self.records.len();
        while low < high {
            let middle = low + (high - low) / 2;
            if self.records[middle].offset < offset {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }

    pub(super) fn find_record(&mut self, offset: Offset) -> Result<RecordIndex, BrokerError> {
        if let Some(record) = self.records.iter().find(|record| record.offset == offset) {
            return Ok(record.clone());
        }

        let file_len = self.file.metadata()?.len();
        let mut cursor = self.scan_start(offset);
        while let Some(parsed) = read_record(&mut self.file, cursor, file_len, self.durable_format)?
        {
            cursor = parsed.next_cursor;
            if parsed.index.offset == offset {
                return Ok(parsed.index);
            }
        }
        Err(BrokerError::CorruptRecord(offset))
    }

    pub(super) fn scan_start(&self, offset: Offset) -> u64 {
        self.sparse_index
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.offset <= offset)
            .map_or(0, |checkpoint| checkpoint.cursor)
    }
}

#[derive(Debug, Clone)]
pub(super) struct RecordIndex {
    pub(super) offset: Offset,
    payload_offset: u64,
    payload_len: u32,
    key: Option<String>,
    request_id: Option<String>,
    published_at_ms: u64,
}

impl RecordIndex {
    pub(super) fn into_key(self) -> Option<String> {
        self.key
    }
}

#[derive(Debug, Clone, Copy)]
struct LogCheckpoint {
    offset: Offset,
    cursor: u64,
}

struct ParsedRecord {
    index: RecordIndex,
    next_cursor: u64,
}

fn read_record(
    file: &mut File,
    cursor: u64,
    file_len: u64,
    durable_format: DurableFormat,
) -> Result<Option<ParsedRecord>, BrokerError> {
    if file_len.saturating_sub(cursor) < 4 {
        return Ok(None);
    }

    file.seek(SeekFrom::Start(cursor))?;
    let mut magic = [0; 4];
    file.read_exact(&mut magic)?;
    if &magic == VERSIONED_MAGIC {
        return read_versioned_record(file, cursor, file_len);
    }
    if &magic == REQUEST_ID_MAGIC {
        return read_request_id_record(file, cursor, file_len, durable_format);
    }
    if &magic != LEGACY_MAGIC {
        return Err(invalid_record_data("unsupported record magic"));
    }
    read_legacy_record(file, cursor, file_len, magic)
}

fn read_legacy_record(
    file: &mut File,
    cursor: u64,
    file_len: u64,
    magic: [u8; 4],
) -> Result<Option<ParsedRecord>, BrokerError> {
    if file_len.saturating_sub(cursor) < LEGACY_HEADER_LEN as u64 {
        return Ok(None);
    }

    let mut header = [0; LEGACY_HEADER_LEN];
    header[..4].copy_from_slice(&magic);
    file.read_exact(&mut header[4..])?;

    let offset = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let published_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let key_len = u32::from_le_bytes(header[20..24].try_into().unwrap());
    let payload_len = u32::from_le_bytes(header[24..28].try_into().unwrap());
    let record_len = (LEGACY_HEADER_LEN as u64)
        .checked_add(u64::from(key_len))
        .and_then(|length| length.checked_add(u64::from(payload_len)))
        .ok_or_else(|| {
            BrokerError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "record length overflows u64",
            ))
        })?;
    if file_len.saturating_sub(cursor) < record_len {
        return Ok(None);
    }

    let mut key_bytes = vec![0; key_len as usize];
    file.read_exact(&mut key_bytes)?;
    let key = if key_bytes.is_empty() {
        None
    } else {
        let Ok(key) = std::str::from_utf8(&key_bytes) else {
            return Err(invalid_record_data("legacy record key is not UTF-8"));
        };
        Some(key.to_owned())
    };
    let payload_offset = cursor + LEGACY_HEADER_LEN as u64 + u64::from(key_len);
    file.seek(SeekFrom::Start(payload_offset + u64::from(payload_len)))?;
    Ok(Some(ParsedRecord {
        index: RecordIndex {
            offset,
            payload_offset,
            payload_len,
            key,
            request_id: None,
            published_at_ms,
        },
        next_cursor: cursor + record_len,
    }))
}

fn read_versioned_record(
    file: &mut File,
    cursor: u64,
    file_len: u64,
) -> Result<Option<ParsedRecord>, BrokerError> {
    if file_len.saturating_sub(cursor) < VERSIONED_HEADER_LEN as u64 {
        return Ok(None);
    }

    file.seek(SeekFrom::Start(cursor))?;
    let mut header = [0; VERSIONED_HEADER_LEN];
    file.read_exact(&mut header)?;
    if header[4] != VERSIONED_FORMAT_VERSION {
        return Err(invalid_record_data("unsupported versioned record version"));
    }
    if header[5] != 0 {
        return Err(invalid_record_data("unsupported versioned record flags"));
    }
    let header_len = u16::from_le_bytes(header[6..8].try_into().unwrap()) as usize;
    if header_len != VERSIONED_HEADER_LEN {
        return Err(invalid_record_data(
            "invalid versioned record header length",
        ));
    }
    let stored_len = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let logical_len = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let key_len = u32::from_le_bytes(header[32..36].try_into().unwrap());
    if stored_len > VERSIONED_MAX_BODY_LEN || logical_len > VERSIONED_MAX_BODY_LEN {
        return Err(invalid_record_data(
            "versioned record exceeds storage limit",
        ));
    }
    if logical_len != stored_len {
        return Err(invalid_record_data(
            "compressed versioned records are not supported",
        ));
    }
    if key_len > VERSIONED_MAX_KEY_LEN {
        return Err(invalid_record_data(
            "versioned record key exceeds storage limit",
        ));
    }
    if header[36] != VERSIONED_ENCODING_BYTES {
        return Err(invalid_record_data("unsupported versioned record encoding"));
    }
    if header[37] != VERSIONED_COMPRESSION_NONE {
        return Err(invalid_record_data(
            "unsupported versioned record compression",
        ));
    }
    if u16::from_le_bytes(header[38..40].try_into().unwrap()) != 0 {
        return Err(invalid_record_data(
            "unsupported versioned record header fields",
        ));
    }

    let record_len = (VERSIONED_HEADER_LEN as u64)
        .checked_add(u64::from(key_len))
        .and_then(|length| length.checked_add(u64::from(stored_len)))
        .ok_or_else(|| invalid_record_data("versioned record length overflows u64"))?;
    if file_len.saturating_sub(cursor) < record_len {
        return Ok(None);
    }

    let mut key_bytes = vec![0; key_len as usize];
    file.read_exact(&mut key_bytes)?;
    let key = if key_bytes.is_empty() {
        None
    } else {
        let key = std::str::from_utf8(&key_bytes)
            .map_err(|_| invalid_record_data("versioned record key is not UTF-8"))?;
        Some(key.to_owned())
    };

    let mut checksum_header = header;
    let expected_checksum = u32::from_le_bytes(header[40..44].try_into().unwrap());
    checksum_header[40..44].fill(0);
    let mut checksum = crc32c_update(!0, &checksum_header);
    checksum = crc32c_update(checksum, &key_bytes);
    let mut remaining = u64::from(stored_len);
    let mut buffer = [0; 8192];
    while remaining > 0 {
        let read_len = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..read_len])?;
        checksum = crc32c_update(checksum, &buffer[..read_len]);
        remaining -= read_len as u64;
    }
    if crc32c_finalize(checksum) != expected_checksum {
        return Err(invalid_record_data("versioned record checksum mismatch"));
    }

    let payload_offset = cursor + VERSIONED_HEADER_LEN as u64 + u64::from(key_len);
    Ok(Some(ParsedRecord {
        index: RecordIndex {
            offset: u64::from_le_bytes(header[16..24].try_into().unwrap()),
            payload_offset,
            payload_len: stored_len,
            key,
            request_id: None,
            published_at_ms: u64::from_le_bytes(header[24..32].try_into().unwrap()),
        },
        next_cursor: cursor + record_len,
    }))
}

fn read_request_id_record(
    file: &mut File,
    cursor: u64,
    file_len: u64,
    durable_format: DurableFormat,
) -> Result<Option<ParsedRecord>, BrokerError> {
    if file_len.saturating_sub(cursor) < REQUEST_ID_HEADER_LEN as u64 {
        return Ok(None);
    }

    file.seek(SeekFrom::Start(cursor))?;
    let mut header = [0; REQUEST_ID_HEADER_LEN];
    file.read_exact(&mut header)?;
    if header[4] != REQUEST_ID_FORMAT_VERSION {
        return Err(invalid_record_data(
            "unsupported request-aware record version",
        ));
    }
    if header[5] != 0 {
        return Err(invalid_record_data(
            "unsupported request-aware record flags",
        ));
    }
    let header_len = u16::from_le_bytes(header[6..8].try_into().unwrap()) as usize;
    if header_len != REQUEST_ID_HEADER_LEN {
        return Err(invalid_record_data(
            "invalid request-aware record header length",
        ));
    }
    let stored_len = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let logical_len = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let key_len = u32::from_le_bytes(header[32..36].try_into().unwrap());
    let request_id_len = u32::from_le_bytes(header[36..40].try_into().unwrap());
    let limits = request_aware_limits(durable_format);
    if key_len > limits.max_key_len {
        return Err(invalid_record_data(
            "request-aware record key exceeds storage limit",
        ));
    }
    if request_id_len > REQUEST_ID_MAX_LEN {
        return Err(invalid_record_data(
            "request-aware record ID exceeds storage limit",
        ));
    }
    if stored_len > limits.max_body_len || logical_len > limits.max_body_len {
        return Err(invalid_record_data(
            "request-aware record exceeds storage limit",
        ));
    }
    if logical_len != stored_len {
        return Err(invalid_record_data(
            "compressed request-aware records are not supported",
        ));
    }
    if header[40..44] != [0; 4] {
        return Err(invalid_record_data(
            "unsupported request-aware record header fields",
        ));
    }

    let record_len = (REQUEST_ID_HEADER_LEN as u64)
        .checked_add(u64::from(key_len))
        .and_then(|length| length.checked_add(u64::from(request_id_len)))
        .and_then(|length| length.checked_add(u64::from(stored_len)))
        .ok_or_else(|| invalid_record_data("request-aware record length overflows u64"))?;
    if file_len.saturating_sub(cursor) < record_len {
        return Ok(None);
    }

    let mut key_bytes = vec![0; key_len as usize];
    file.read_exact(&mut key_bytes)?;
    let key = if key_bytes.is_empty() {
        None
    } else {
        let key = std::str::from_utf8(&key_bytes)
            .map_err(|_| invalid_record_data("request-aware record key is not UTF-8"))?;
        Some(key.to_owned())
    };

    let mut request_id_bytes = vec![0; request_id_len as usize];
    file.read_exact(&mut request_id_bytes)?;
    let request_id = std::str::from_utf8(&request_id_bytes)
        .map_err(|_| invalid_record_data("request-aware record ID is not UTF-8"))?
        .to_owned();

    let expected_checksum = u32::from_le_bytes(header[44..48].try_into().unwrap());
    let mut checksum_header = header;
    checksum_header[44..48].fill(0);
    let mut checksum = crc32c_update(!0, &checksum_header);
    checksum = crc32c_update(checksum, &key_bytes);
    checksum = crc32c_update(checksum, &request_id_bytes);
    let mut remaining = u64::from(stored_len);
    let mut buffer = [0; 8192];
    while remaining > 0 {
        let read_len = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..read_len])?;
        checksum = crc32c_update(checksum, &buffer[..read_len]);
        remaining -= read_len as u64;
    }
    if crc32c_finalize(checksum) != expected_checksum {
        return Err(invalid_record_data(
            "request-aware record checksum mismatch",
        ));
    }

    let payload_offset =
        cursor + REQUEST_ID_HEADER_LEN as u64 + u64::from(key_len) + u64::from(request_id_len);
    Ok(Some(ParsedRecord {
        index: RecordIndex {
            offset: u64::from_le_bytes(header[16..24].try_into().unwrap()),
            payload_offset,
            payload_len: stored_len,
            key,
            request_id: Some(request_id),
            published_at_ms: u64::from_le_bytes(header[24..32].try_into().unwrap()),
        },
        next_cursor: cursor + record_len,
    }))
}

fn invalid_record_data(message: &'static str) -> BrokerError {
    BrokerError::Io(io::Error::new(io::ErrorKind::InvalidData, message))
}

const CRC32C_TABLE: [u32; 256] = crc32c_table();

const fn crc32c_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0;
    while index < table.len() {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 1 {
                (value >> 1) ^ 0x82f6_3b78
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

fn crc32c_update(mut checksum: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        let table_index = ((checksum ^ u32::from(byte)) & 0xff) as usize;
        checksum = (checksum >> 8) ^ CRC32C_TABLE[table_index];
    }
    checksum
}

fn crc32c_finalize(checksum: u32) -> u32 {
    !checksum
}

fn versioned_checksum(header: &[u8; VERSIONED_HEADER_LEN], key: &[u8], body: &[u8]) -> u32 {
    let mut checksum_header = *header;
    checksum_header[40..44].fill(0);
    let checksum = crc32c_update(!0, &checksum_header);
    let checksum = crc32c_update(checksum, key);
    crc32c_finalize(crc32c_update(checksum, body))
}

fn request_id_checksum(
    header: &[u8; REQUEST_ID_HEADER_LEN],
    key: &[u8],
    request_id: &[u8],
    body: &[u8],
) -> u32 {
    let mut checksum_header = *header;
    checksum_header[44..48].fill(0);
    let mut checksum = crc32c_update(!0, &checksum_header);
    checksum = crc32c_update(checksum, key);
    checksum = crc32c_update(checksum, request_id);
    crc32c_finalize(crc32c_update(checksum, body))
}

fn remember_record(records: &mut VecDeque<RecordIndex>, record: RecordIndex) {
    if records.len() == MAX_IN_MEMORY_RECORDS {
        records.pop_front();
    }
    records.push_back(record);
}

fn remember_checkpoint(checkpoints: &mut VecDeque<LogCheckpoint>, offset: Offset, cursor: u64) {
    if !offset.is_multiple_of(SPARSE_INDEX_STRIDE) {
        return;
    }
    if checkpoints.len() == MAX_SPARSE_INDEX_ENTRIES {
        checkpoints.pop_front();
    }
    checkpoints.push_back(LogCheckpoint { offset, cursor });
}

fn record_is_candidate(
    record: &RecordIndex,
    acknowledged_offsets: &BTreeSet<Offset>,
    in_flight: Option<(&HashSet<Offset>, &HashSet<String>)>,
) -> bool {
    if acknowledged_offsets.contains(&record.offset)
        || in_flight.is_some_and(|(offsets, _)| offsets.contains(&record.offset))
    {
        return false;
    }
    record
        .key
        .as_ref()
        .is_none_or(|key| in_flight.is_none_or(|(_, keys)| !keys.contains(key)))
}

fn is_invalid_input(error: &BrokerError) -> bool {
    matches!(
        error,
        BrokerError::Io(io_error) if io_error.kind() == io::ErrorKind::InvalidInput
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_candidate_lookup_lower_bounds_the_committed_offset() {
        let directory = tempfile::tempdir().unwrap();
        let mut log =
            StreamLog::create(&directory.path().join("events.log"), DurableFormat::Rnl1).unwrap();
        for offset in 0..8 {
            assert_eq!(log.append(None, vec![offset as u8]).unwrap(), offset);
        }

        assert_eq!(log.tail_start_index(0), 0);
        assert_eq!(log.tail_start_index(3), 3);
        assert_eq!(log.tail_start_index(7), 7);
        assert_eq!(log.tail_start_index(8), 8);

        let acknowledged_offsets = BTreeSet::from([3]);
        let candidate = log
            .find_candidate(3, &acknowledged_offsets, None)
            .unwrap()
            .unwrap();
        assert_eq!(candidate.offset, 4);
    }

    #[test]
    fn sparse_lookup_index_keeps_only_a_bounded_recent_window() {
        let mut checkpoints = VecDeque::new();
        for index in 0..=MAX_SPARSE_INDEX_ENTRIES {
            let offset = index as Offset * SPARSE_INDEX_STRIDE;
            remember_checkpoint(&mut checkpoints, offset, offset * 10);
        }

        assert_eq!(checkpoints.len(), MAX_SPARSE_INDEX_ENTRIES);
        assert_eq!(checkpoints.front().unwrap().offset, SPARSE_INDEX_STRIDE);
        assert_eq!(
            checkpoints.back().unwrap().offset,
            MAX_SPARSE_INDEX_ENTRIES as Offset * SPARSE_INDEX_STRIDE
        );
        assert_eq!(
            checkpoints.back().unwrap().cursor,
            MAX_SPARSE_INDEX_ENTRIES as Offset * SPARSE_INDEX_STRIDE * 10
        );
    }
}
