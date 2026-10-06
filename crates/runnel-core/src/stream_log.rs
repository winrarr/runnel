use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use runnel_engine::{
    BrokerError, Message, Offset, PublishRecord, PublishRecordOutcome, ReplayMessage,
};

pub(super) const RECORD_MAGIC: &[u8; 4] = b"RNL3";
pub(super) const RECORD_HEADER_LEN: usize = 48;
pub(super) const RECORD_FORMAT_VERSION: u8 = 2;
const NO_IDENTITY_FLAG: u8 = 2;
pub(super) const MAX_KEY_LEN: u32 = 128;
pub(super) const MAX_BODY_LEN: u32 = 64 * 1024 * 1024;
pub(super) const MAX_REQUEST_ID_LEN: u32 = 1024;
pub(super) const MAX_IN_MEMORY_RECORDS: usize = 1024;
const SPARSE_INDEX_STRIDE: Offset = 64;
const MAX_SPARSE_INDEX_ENTRIES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestIdentityKind {
    Public,
    DeadLetterMove,
}

impl RequestIdentityKind {
    fn flag(self) -> u8 {
        match self {
            Self::Public => 0,
            Self::DeadLetterMove => 1,
        }
    }
}

#[derive(Debug, Default)]
struct RequestIdentityIndex {
    public: HashMap<String, Offset>,
    dead_letter_moves: HashMap<String, Offset>,
}

impl RequestIdentityIndex {
    fn get(&self, identity: &str, kind: RequestIdentityKind) -> Option<Offset> {
        match kind {
            RequestIdentityKind::Public => self.public.get(identity).copied(),
            RequestIdentityKind::DeadLetterMove => self.dead_letter_moves.get(identity).copied(),
        }
    }

    fn remember(&mut self, identity: String, kind: RequestIdentityKind, offset: Offset) {
        let identities = match kind {
            RequestIdentityKind::Public => &mut self.public,
            RequestIdentityKind::DeadLetterMove => &mut self.dead_letter_moves,
        };
        identities.entry(identity).or_insert(offset);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.public.len() + self.dead_letter_moves.len()
    }
}

#[derive(Debug, Clone)]
enum RecordIdentity {
    None,
    Public(String),
    DeadLetterMove(String),
}

impl RecordIdentity {
    fn flag(&self) -> u8 {
        match self {
            Self::None => NO_IDENTITY_FLAG,
            Self::Public(_) => RequestIdentityKind::Public.flag(),
            Self::DeadLetterMove(_) => RequestIdentityKind::DeadLetterMove.flag(),
        }
    }

    fn request_id(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Public(request_id) | Self::DeadLetterMove(request_id) => Some(request_id),
        }
    }

    fn kind(&self) -> Option<RequestIdentityKind> {
        match self {
            Self::None => None,
            Self::Public(_) => Some(RequestIdentityKind::Public),
            Self::DeadLetterMove(_) => Some(RequestIdentityKind::DeadLetterMove),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(super) enum DeadLetterMoveWriteFailure {
    PartialFrame,
    CompleteFrameBeforeSync,
}

fn record_lengths(key_len: usize, payload_len: usize) -> Result<(u32, u32), BrokerError> {
    let key_len = u32::try_from(key_len).map_err(|_| {
        BrokerError::InvalidRecord(
            "message key length exceeds u32 storage representation".to_owned(),
        )
    })?;
    if key_len > MAX_KEY_LEN {
        return Err(BrokerError::InvalidRecord(format!(
            "message key is {key_len} bytes; stream record limit is {MAX_KEY_LEN} bytes"
        )));
    }

    let payload_len = u32::try_from(payload_len).map_err(|_| {
        BrokerError::InvalidRecord(
            "message payload length exceeds u32 storage representation".to_owned(),
        )
    })?;
    if payload_len > MAX_BODY_LEN {
        return Err(BrokerError::InvalidRecord(format!(
            "message payload is {payload_len} bytes; stream record limit is {MAX_BODY_LEN} bytes"
        )));
    }

    Ok((key_len, payload_len))
}

pub(super) struct StreamLog {
    file: File,
    // The durable log retains the complete history; this tail cache keeps normal delivery
    // bounded while older replay requests use the bounded sparse index as a scan starting point.
    records: VecDeque<RecordIndex>,
    sparse_index: SparseIndex,
    // Keep values at the existing one-offset size per retained identity. The second map adds
    // per-stream and capacity-slack costs; only a cross-namespace collision duplicates a key.
    request_ids: RequestIdentityIndex,
    next_offset: Offset,
    #[cfg(test)]
    dead_letter_move_write_failure: Option<DeadLetterMoveWriteFailure>,
}

impl StreamLog {
    pub(super) fn create(path: &Path) -> Result<Self, BrokerError> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file,
            records: VecDeque::with_capacity(MAX_IN_MEMORY_RECORDS),
            sparse_index: SparseIndex::new(),
            request_ids: RequestIdentityIndex::default(),
            next_offset: 0,
            #[cfg(test)]
            dead_letter_move_write_failure: None,
        })
    }

    pub(super) fn inspect(path: &Path) -> Result<(Self, Option<u64>), BrokerError> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let file_len = file.metadata()?.len();
        let mut records = VecDeque::with_capacity(MAX_IN_MEMORY_RECORDS);
        let mut sparse_index = SparseIndex::new();
        let mut request_ids = RequestIdentityIndex::default();
        let mut cursor = 0;
        let mut next_offset = 0;
        file.seek(SeekFrom::Start(cursor))?;
        while let Some(parsed) = read_next_record(&mut file, cursor, file_len)? {
            if parsed.index.offset != next_offset {
                return Err(invalid_record_data("record offsets are not contiguous"));
            }
            sparse_index.remember(parsed.index.offset, cursor);
            cursor = parsed.next_cursor;
            next_offset = next_offset
                .checked_add(1)
                .ok_or_else(|| invalid_record_data("record offset exceeds u64 range"))?;
            if let (Some(request_id), Some(identity_kind)) = (
                parsed.index.request_id.as_ref(),
                parsed.index.request_identity_kind,
            ) {
                request_ids.remember(request_id.clone(), identity_kind, parsed.index.offset);
            }
            remember_record(&mut records, parsed.index);
        }

        let incomplete_tail = (cursor != file_len).then_some(cursor);
        Ok((
            Self {
                file,
                records,
                sparse_index,
                request_ids,
                next_offset,
                #[cfg(test)]
                dead_letter_move_write_failure: None,
            },
            incomplete_tail,
        ))
    }

    pub(super) fn finish_recovery(
        &mut self,
        incomplete_tail: Option<u64>,
    ) -> Result<(), BrokerError> {
        if let Some(cursor) = incomplete_tail {
            self.file.set_len(cursor)?;
        }
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    pub(super) fn request_offset(&self, request_id: &str) -> Option<Offset> {
        self.identity_offset(request_id, RequestIdentityKind::Public)
    }

    pub(super) fn request_offset_for_content(
        &mut self,
        request_id: &str,
        key: Option<&str>,
        payload: &[u8],
    ) -> Result<Option<Offset>, BrokerError> {
        let Some(offset) = self.request_offset(request_id) else {
            return Ok(None);
        };
        let existing = self.find_record(offset)?;
        let existing_key = existing.key.as_deref().unwrap_or_default().as_bytes();
        let incoming_key = key.unwrap_or_default().as_bytes();
        if existing_key != incoming_key || self.read_payload(&existing)? != payload {
            return Err(BrokerError::RequestIdContentConflict);
        }
        Ok(Some(offset))
    }

    pub(super) fn dead_letter_move_offset(&self, move_id: &str) -> Option<Offset> {
        self.identity_offset(move_id, RequestIdentityKind::DeadLetterMove)
    }

    fn identity_offset(&self, identity: &str, kind: RequestIdentityKind) -> Option<Offset> {
        self.request_ids.get(identity, kind)
    }

    #[cfg(test)]
    pub(super) fn fail_next_dead_letter_move_write(&mut self, failure: DeadLetterMoveWriteFailure) {
        self.dead_letter_move_write_failure = Some(failure);
    }

    #[cfg(test)]
    pub(super) fn request_identity_count(&self) -> usize {
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
        self.sparse_index.first_offset()
    }

    #[cfg(test)]
    pub(super) fn last_sparse_offset(&self) -> Option<Offset> {
        self.sparse_index.last_offset()
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
        self.append_record_with_sync(key, payload, RecordIdentity::None, sync)
    }

    pub(super) fn append_with_request_id(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: String,
    ) -> Result<Offset, BrokerError> {
        self.append_record_with_sync(key, payload, RecordIdentity::Public(request_id), true)
    }

    pub(super) fn append_with_move_id(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        move_id: String,
    ) -> Result<Offset, BrokerError> {
        if let Some(offset) = self.dead_letter_move_offset(&move_id) {
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
        self.append_record_with_sync(key, payload, RecordIdentity::DeadLetterMove(move_id), true)
    }

    fn append_record_with_sync(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        identity: RecordIdentity,
        sync: bool,
    ) -> Result<Offset, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = runnel_engine::StageTimer::new("core.storage_append");
        let key_bytes = key.as_deref().unwrap_or_default().as_bytes();
        let (key_len, payload_len) = record_lengths(key_bytes.len(), payload.len())?;
        let request_id_bytes = identity.request_id().unwrap_or_default().as_bytes();
        let request_id_len = u32::try_from(request_id_bytes.len()).map_err(|_| {
            BrokerError::InvalidRecord(
                "request ID length exceeds u32 storage representation".to_owned(),
            )
        })?;
        if request_id_len > MAX_REQUEST_ID_LEN {
            return Err(BrokerError::InvalidRecord(format!(
                "request ID is {request_id_len} bytes; stream record limit is {MAX_REQUEST_ID_LEN} bytes"
            )));
        }
        let offset = self.next_offset;
        let published_at_ms = now_ms();
        let mut header = [0; RECORD_HEADER_LEN];
        header[..4].copy_from_slice(RECORD_MAGIC);
        header[4] = RECORD_FORMAT_VERSION;
        header[5] = identity.flag();
        header[6..8].copy_from_slice(&(RECORD_HEADER_LEN as u16).to_le_bytes());
        header[8..12].copy_from_slice(&payload_len.to_le_bytes());
        header[12..16].copy_from_slice(&payload_len.to_le_bytes());
        header[16..24].copy_from_slice(&offset.to_le_bytes());
        header[24..32].copy_from_slice(&published_at_ms.to_le_bytes());
        header[32..36].copy_from_slice(&key_len.to_le_bytes());
        header[36..40].copy_from_slice(&request_id_len.to_le_bytes());
        let checksum = record_checksum(&header, key_bytes, request_id_bytes, &payload);
        header[44..48].copy_from_slice(&checksum.to_le_bytes());

        #[cfg(test)]
        let move_write_failure = if identity.kind() == Some(RequestIdentityKind::DeadLetterMove) {
            self.dead_letter_move_write_failure.take()
        } else {
            None
        };
        #[cfg(test)]
        if matches!(
            move_write_failure,
            Some(DeadLetterMoveWriteFailure::PartialFrame)
        ) {
            self.file.write_all(&header[..RECORD_HEADER_LEN / 2])?;
            return Err(injected_dead_letter_write_failure());
        }

        self.file.write_all(&header)?;
        self.file.write_all(key_bytes)?;
        self.file.write_all(request_id_bytes)?;
        self.file.write_all(&payload)?;
        #[cfg(test)]
        if matches!(
            move_write_failure,
            Some(DeadLetterMoveWriteFailure::CompleteFrameBeforeSync)
        ) {
            return Err(injected_dead_letter_write_failure());
        }
        if sync {
            self.file.sync_data()?;
        }

        let payload_offset = self.file.stream_position()? - payload.len() as u64;
        let record_cursor = payload_offset
            - request_id_bytes.len() as u64
            - key_bytes.len() as u64
            - RECORD_HEADER_LEN as u64;
        self.sparse_index.remember(offset, record_cursor);
        remember_record(
            &mut self.records,
            RecordIndex {
                offset,
                payload_offset,
                payload_len,
                key,
                request_id: identity.request_id().map(str::to_owned),
                request_identity_kind: identity.kind(),
                published_at_ms,
            },
        );
        if let (Some(request_id), Some(identity_kind)) = (identity.request_id(), identity.kind()) {
            self.request_ids
                .remember(request_id.to_owned(), identity_kind, offset);
        }
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
            if let Some(request_id) = request_id.as_ref() {
                match self.request_offset_for_content(request_id, key.as_deref(), &payload) {
                    Ok(Some(offset)) => {
                        outcomes.push(Ok(offset));
                        continue;
                    }
                    Ok(None) => {}
                    Err(error @ BrokerError::RequestIdContentConflict) => {
                        outcomes.push(Err(error));
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }

            let outcome = match request_id {
                Some(request_id) => self.append_record_with_sync(
                    key,
                    payload,
                    RecordIdentity::Public(request_id),
                    false,
                ),
                None => self.append_with_sync(key, payload, false),
            };
            match outcome {
                Ok(offset) => {
                    appended = true;
                    outcomes.push(Ok(offset));
                }
                Err(error) if is_per_record_rejection(&error) => outcomes.push(Err(error)),
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
            let mut index = self.tail_start_index(committed_offset);
            while let Some(record) = self.records.get(index) {
                if record_is_candidate(record, acknowledged_offsets, in_flight) {
                    return Ok(Some(record.clone()));
                }
                index += 1;
            }
            return Ok(None);
        }

        // A consumer that has fallen behind the bounded tail index still has the same replay
        // rights. Start at the nearest sparse checkpoint so cold replay does not always scan
        // from byte zero.
        let file_len = self.file.metadata()?.len();
        let mut cursor = self.scan_start(committed_offset);
        self.file.seek(SeekFrom::Start(cursor))?;
        while let Some(parsed) = read_next_record(&mut self.file, cursor, file_len)? {
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
        // Appends and recovery preserve offset order, and tail eviction only removes the front.
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
        self.file.seek(SeekFrom::Start(cursor))?;
        while let Some(parsed) = read_next_record(&mut self.file, cursor, file_len)? {
            cursor = parsed.next_cursor;
            if parsed.index.offset == offset {
                return Ok(parsed.index);
            }
        }
        Err(BrokerError::CorruptRecord(offset))
    }

    pub(super) fn scan_start(&self, offset: Offset) -> u64 {
        self.sparse_index.start_for(offset)
    }
}

#[derive(Debug, Clone)]
pub(super) struct RecordIndex {
    pub(super) offset: Offset,
    payload_offset: u64,
    payload_len: u32,
    key: Option<String>,
    request_id: Option<String>,
    request_identity_kind: Option<RequestIdentityKind>,
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

#[derive(Debug, Default)]
struct SparseIndex {
    // Checkpoints stay in offset order and retain only a bounded recent window. An old replay can
    // still scan from byte zero after its checkpoint is evicted, while normal delivery never
    // retains one location per durable record.
    checkpoints: VecDeque<LogCheckpoint>,
}

impl SparseIndex {
    fn new() -> Self {
        Self {
            checkpoints: VecDeque::with_capacity(MAX_SPARSE_INDEX_ENTRIES),
        }
    }

    fn remember(&mut self, offset: Offset, cursor: u64) {
        if !offset.is_multiple_of(SPARSE_INDEX_STRIDE) {
            return;
        }

        debug_assert!(
            self.checkpoints
                .back()
                .is_none_or(|previous| { previous.offset < offset && previous.cursor < cursor })
        );
        if self.checkpoints.len() == MAX_SPARSE_INDEX_ENTRIES {
            self.checkpoints.pop_front();
        }
        self.checkpoints.push_back(LogCheckpoint { offset, cursor });
    }

    fn start_for(&self, offset: Offset) -> u64 {
        self.checkpoints
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.offset <= offset)
            .map_or(0, |checkpoint| checkpoint.cursor)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.checkpoints.len()
    }

    #[cfg(test)]
    fn first_offset(&self) -> Option<Offset> {
        self.checkpoints.front().map(|checkpoint| checkpoint.offset)
    }

    #[cfg(test)]
    fn last_offset(&self) -> Option<Offset> {
        self.checkpoints.back().map(|checkpoint| checkpoint.offset)
    }
}

struct ParsedRecord {
    index: RecordIndex,
    next_cursor: u64,
}

// The caller seeks to `cursor` once before starting a scan. The parser consumes exactly one
// complete record and leaves the file positioned at `next_cursor`.
fn read_next_record(
    file: &mut File,
    cursor: u64,
    file_len: u64,
) -> Result<Option<ParsedRecord>, BrokerError> {
    if file_len.saturating_sub(cursor) < 4 {
        return Ok(None);
    }

    let mut magic = [0; 4];
    file.read_exact(&mut magic)?;
    if &magic == b"RNL1" {
        return Err(invalid_record_data(
            "unsupported RNL1 stream format; this broker requires RNL3",
        ));
    }
    if &magic == b"RNL2" {
        return Err(invalid_record_data(
            "unsupported RNL2 stream format; this broker requires RNL3",
        ));
    }
    if &magic != RECORD_MAGIC {
        return Err(invalid_record_data("unsupported stream record magic"));
    }
    read_record(file, cursor, file_len, magic)
}

fn read_record(
    file: &mut File,
    cursor: u64,
    file_len: u64,
    magic: [u8; 4],
) -> Result<Option<ParsedRecord>, BrokerError> {
    if file_len.saturating_sub(cursor) < RECORD_HEADER_LEN as u64 {
        return Ok(None);
    }

    let mut header = [0; RECORD_HEADER_LEN];
    header[..4].copy_from_slice(&magic);
    file.read_exact(&mut header[4..])?;
    if header[4] != RECORD_FORMAT_VERSION {
        return Err(invalid_record_data("unsupported RNL3 record version"));
    }
    let identity_kind = match header[5] {
        0 => Some(RequestIdentityKind::Public),
        1 => Some(RequestIdentityKind::DeadLetterMove),
        NO_IDENTITY_FLAG => None,
        _ => return Err(invalid_record_data("unsupported RNL3 record identity flag")),
    };
    let header_len = u16::from_le_bytes(header[6..8].try_into().unwrap()) as usize;
    if header_len != RECORD_HEADER_LEN {
        return Err(invalid_record_data("invalid RNL3 record header length"));
    }
    let stored_len = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let logical_len = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let key_len = u32::from_le_bytes(header[32..36].try_into().unwrap());
    let request_id_len = u32::from_le_bytes(header[36..40].try_into().unwrap());
    if key_len > MAX_KEY_LEN {
        return Err(invalid_record_data("RNL3 record key exceeds storage limit"));
    }
    if request_id_len > MAX_REQUEST_ID_LEN {
        return Err(invalid_record_data("RNL3 record ID exceeds storage limit"));
    }
    if stored_len > MAX_BODY_LEN || logical_len > MAX_BODY_LEN {
        return Err(invalid_record_data("RNL3 record exceeds storage limit"));
    }
    if identity_kind.is_none() && request_id_len != 0 {
        return Err(invalid_record_data(
            "RNL3 record without identity contains ID bytes",
        ));
    }
    if logical_len != stored_len {
        return Err(invalid_record_data(
            "compressed RNL3 records are not supported",
        ));
    }
    if header[40..44] != [0; 4] {
        return Err(invalid_record_data("unsupported RNL3 record header fields"));
    }

    let record_len = (RECORD_HEADER_LEN as u64)
        .checked_add(u64::from(key_len))
        .and_then(|length| length.checked_add(u64::from(request_id_len)))
        .and_then(|length| length.checked_add(u64::from(stored_len)))
        .ok_or_else(|| invalid_record_data("RNL3 record length overflows u64"))?;
    if file_len.saturating_sub(cursor) < record_len {
        return Ok(None);
    }

    let mut key_bytes = vec![0; key_len as usize];
    file.read_exact(&mut key_bytes)?;
    let key = if key_bytes.is_empty() {
        None
    } else {
        let key = std::str::from_utf8(&key_bytes)
            .map_err(|_| invalid_record_data("RNL3 record key is not UTF-8"))?;
        Some(key.to_owned())
    };

    let mut request_id_bytes = vec![0; request_id_len as usize];
    file.read_exact(&mut request_id_bytes)?;
    let request_id = identity_kind
        .map(|_| {
            std::str::from_utf8(&request_id_bytes)
                .map(str::to_owned)
                .map_err(|_| invalid_record_data("RNL3 record ID is not UTF-8"))
        })
        .transpose()?;

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
        return Err(invalid_record_data("RNL3 record checksum mismatch"));
    }

    let payload_offset =
        cursor + RECORD_HEADER_LEN as u64 + u64::from(key_len) + u64::from(request_id_len);
    Ok(Some(ParsedRecord {
        index: RecordIndex {
            offset: u64::from_le_bytes(header[16..24].try_into().unwrap()),
            payload_offset,
            payload_len: stored_len,
            key,
            request_id,
            request_identity_kind: identity_kind,
            published_at_ms: u64::from_le_bytes(header[24..32].try_into().unwrap()),
        },
        next_cursor: cursor + record_len,
    }))
}

fn invalid_record_data(message: &'static str) -> BrokerError {
    BrokerError::Io(io::Error::new(io::ErrorKind::InvalidData, message))
}

#[cfg(test)]
fn injected_dead_letter_write_failure() -> BrokerError {
    BrokerError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "injected dead-letter target write failure",
    ))
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

pub(super) fn record_checksum(
    header: &[u8; RECORD_HEADER_LEN],
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

fn is_per_record_rejection(error: &BrokerError) -> bool {
    match error {
        BrokerError::InvalidRecord(_) => true,
        BrokerError::Io(io_error) => io_error.kind() == io::ErrorKind::InvalidInput,
        _ => false,
    }
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
    fn record_write_limits_reject_oversized_fields_without_mutating_the_log() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.log");
        let mut log = StreamLog::create(&path).unwrap();
        assert_eq!(
            log.append_with_sync(Some("k".repeat(MAX_KEY_LEN as usize)), vec![1], false)
                .unwrap(),
            0
        );
        let bytes_after_valid_boundary_record = std::fs::read(&path).unwrap();
        assert_eq!(log.next_offset(), 1);
        assert_eq!(log.in_memory_record_count(), 1);
        assert_eq!(log.sparse_index_len(), 1);

        let key_error =
            log.append_with_sync(Some("k".repeat(MAX_KEY_LEN as usize + 1)), vec![2], false);
        assert!(matches!(key_error, Err(BrokerError::InvalidRecord(_))));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes_after_valid_boundary_record
        );
        assert_eq!(log.next_offset(), 1);
        assert_eq!(log.in_memory_record_count(), 1);
        assert_eq!(log.sparse_index_len(), 1);

        let payload_error = log.append_with_sync(None, vec![2; MAX_BODY_LEN as usize + 1], false);
        assert!(matches!(payload_error, Err(BrokerError::InvalidRecord(_))));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes_after_valid_boundary_record
        );
        assert_eq!(log.next_offset(), 1);
        assert_eq!(log.in_memory_record_count(), 1);
        assert_eq!(log.sparse_index_len(), 1);
        assert_eq!(
            log.append_with_sync(None, vec![3], false).unwrap(),
            1,
            "a rejected append must not consume a logical offset"
        );
    }

    #[test]
    fn record_writer_accepts_payload_at_selected_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.log");
        let mut log = StreamLog::create(&path).unwrap();
        assert_eq!(
            log.append_with_sync(None, vec![7; MAX_BODY_LEN as usize], false)
                .unwrap(),
            0
        );
        assert_eq!(log.next_offset(), 1);
        assert_eq!(log.in_memory_record_count(), 1);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            RECORD_HEADER_LEN as u64 + u64::from(MAX_BODY_LEN)
        );
    }

    #[test]
    fn recovery_refuses_old_frame_magics_without_mutating_the_file() {
        let directory = tempfile::tempdir().unwrap();
        for (magic, message) in [
            (
                b"RNL1" as &[u8],
                "unsupported RNL1 stream format; this broker requires RNL3",
            ),
            (
                b"RNL2" as &[u8],
                "unsupported RNL2 stream format; this broker requires RNL3",
            ),
        ] {
            let path = directory
                .path()
                .join(format!("{}.log", std::str::from_utf8(magic).unwrap()));
            let bytes = [magic, b"old stream bytes"].concat();
            std::fs::write(&path, &bytes).unwrap();

            let error = match StreamLog::inspect(&path) {
                Err(error) => error,
                Ok(_) => panic!("expected the old stream format to be refused"),
            };
            assert!(matches!(
                error,
                BrokerError::Io(error)
                    if error.kind() == io::ErrorKind::InvalidData && error.to_string() == message
            ));
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn sparse_lookup_index_keeps_only_a_bounded_recent_window() {
        let mut index = SparseIndex::new();
        for checkpoint in 0..=MAX_SPARSE_INDEX_ENTRIES {
            let offset = checkpoint as Offset * SPARSE_INDEX_STRIDE;
            index.remember(offset, offset * 10);
        }

        assert_eq!(index.len(), MAX_SPARSE_INDEX_ENTRIES);
        assert_eq!(index.first_offset().unwrap(), SPARSE_INDEX_STRIDE);
        assert_eq!(index.start_for(SPARSE_INDEX_STRIDE - 1), 0);
        assert_eq!(
            index.start_for(SPARSE_INDEX_STRIDE),
            SPARSE_INDEX_STRIDE * 10
        );
        assert_eq!(
            index.last_offset().unwrap(),
            MAX_SPARSE_INDEX_ENTRIES as Offset * SPARSE_INDEX_STRIDE
        );
        assert_eq!(
            index.start_for(Offset::MAX),
            MAX_SPARSE_INDEX_ENTRIES as Offset * SPARSE_INDEX_STRIDE * 10
        );
    }

    #[test]
    fn tail_candidate_lookup_starts_at_committed_offset_after_cache_wrap() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.log");
        let mut log = StreamLog::create(&path).unwrap();
        for offset in 0..(MAX_IN_MEMORY_RECORDS as Offset + 5) {
            assert_eq!(
                log.append_with_sync(Some(format!("key-{offset}")), vec![0], false)
                    .unwrap(),
                offset
            );
        }

        // Offsets below the bounded cache still use the sparse-checkpoint disk scan.
        let no_acknowledgements = BTreeSet::new();
        assert_eq!(
            log.find_candidate(0, &no_acknowledgements, None)
                .unwrap()
                .unwrap()
                .offset,
            0
        );

        // The circular tail cache remains ordered by offset after evicting its oldest records.
        assert_eq!(log.tail_start_index(5), 0);
        assert_eq!(log.tail_start_index(6), 1);
        assert_eq!(log.tail_start_index(1028), MAX_IN_MEMORY_RECORDS - 1);
        assert_eq!(log.tail_start_index(1029), MAX_IN_MEMORY_RECORDS);

        let acknowledged_offsets = BTreeSet::from([1000]);
        let in_flight_offsets = HashSet::from([1001]);
        let in_flight_keys = HashSet::from(["key-1002".to_owned()]);
        let candidate = log
            .find_candidate(
                1000,
                &acknowledged_offsets,
                Some((&in_flight_offsets, &in_flight_keys)),
            )
            .unwrap()
            .unwrap();
        assert_eq!(candidate.offset, 1003);

        assert!(
            log.find_candidate(1029, &no_acknowledgements, None)
                .unwrap()
                .is_none()
        );
    }
}
