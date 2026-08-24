use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{Engine, EngineFuture};
use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 4] = b"RNL1";
const HEADER_LEN: usize = 28;
const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const DEAD_LETTER_SUFFIX: &str = ".dead-letter";

pub use runnel_engine::{AckResult, BrokerError, HealthSnapshot, Message, Offset, PollResult};

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub ack_timeout: Duration,
    pub max_delivery_attempts: Option<u32>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            ack_timeout: DEFAULT_ACK_TIMEOUT,
            max_delivery_attempts: None,
        }
    }
}

#[derive(Clone)]
pub struct Broker {
    inner: Arc<Mutex<BrokerState>>,
}

struct BrokerState {
    root: PathBuf,
    streams: HashMap<String, StreamLog>,
    // Entries are retained only while a consumer has active deliveries; the file remains the
    // durable source of truth across restart and cache eviction.
    consumer_states: HashMap<ConsumerKey, ConsumerState>,
    in_flight: HashMap<DeliveryKey, InFlight>,
    ack_timeout: Duration,
    max_delivery_attempts: Option<u32>,
    redeliveries: u64,
    dead_letters: u64,
    delivery_epoch: u128,
    next_delivery_id: u64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ConsumerKey {
    stream: String,
    consumer: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DeliveryKey {
    consumer: ConsumerKey,
    offset: Offset,
}

#[derive(Debug, Clone)]
struct InFlight {
    member: String,
    offset: Offset,
    key: Option<String>,
    delivery_attempt: u32,
    delivery_token: String,
    deadline: Instant,
}

struct StreamLog {
    file: File,
    records: Vec<RecordIndex>,
}

#[derive(Debug, Clone)]
struct RecordIndex {
    offset: Offset,
    payload_offset: u64,
    payload_len: u32,
    key: Option<String>,
    published_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsumerState {
    stream: String,
    consumer: String,
    committed_offset: Offset,
    #[serde(default)]
    acknowledged_offsets: BTreeSet<Offset>,
    #[serde(default)]
    delivery_attempts: BTreeMap<Offset, u32>,
}

impl ConsumerState {
    fn acknowledge(&mut self, offset: Offset) {
        self.delivery_attempts.remove(&offset);
        if offset == self.committed_offset {
            self.committed_offset += 1;
            while self.acknowledged_offsets.remove(&self.committed_offset) {
                self.delivery_attempts.remove(&self.committed_offset);
                self.committed_offset += 1;
            }
        } else {
            self.acknowledged_offsets.insert(offset);
        }
    }
}

impl Broker {
    pub fn open(root: impl AsRef<Path>, config: BrokerConfig) -> Result<Self, BrokerError> {
        if config.max_delivery_attempts == Some(0) {
            return Err(BrokerError::Configuration(
                "max delivery attempts must be greater than zero".to_owned(),
            ));
        }
        let root = root.as_ref().to_path_buf();
        let streams_dir = root.join("streams");
        let consumers_dir = root.join("consumers");
        fs::create_dir_all(&streams_dir)?;
        fs::create_dir_all(&consumers_dir)?;

        let mut streams = HashMap::new();
        for entry in fs::read_dir(&streams_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("log") {
                continue;
            }

            let Some(name) = path.file_stem().and_then(|name| name.to_str()) else {
                continue;
            };
            validate_name("stream", name)?;
            let log = StreamLog::open(&path)?;
            streams.insert(name.to_owned(), log);
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(BrokerState {
                root,
                streams,
                consumer_states: HashMap::new(),
                in_flight: HashMap::new(),
                ack_timeout: config.ack_timeout,
                max_delivery_attempts: config.max_delivery_attempts,
                redeliveries: 0,
                dead_letters: 0,
                delivery_epoch: delivery_epoch(),
                next_delivery_id: 0,
            })),
        })
    }

    pub fn create_stream(&self, stream: &str) -> Result<bool, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.create_stream");
        validate_name("stream", stream)?;
        let mut state = self.lock()?;
        if state.streams.contains_key(stream) {
            return Ok(false);
        }

        let path = stream_path(&state.root, stream);
        let log = StreamLog::create(&path)?;
        state.streams.insert(stream.to_owned(), log);
        Ok(true)
    }

    pub fn publish(
        &self,
        stream: &str,
        key: Option<String>,
        payload: Vec<u8>,
    ) -> Result<Offset, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.publish");
        validate_name("stream", stream)?;
        let mut state = self.lock()?;
        if !state.streams.contains_key(stream) {
            let path = stream_path(&state.root, stream);
            let log = StreamLog::create(&path)?;
            state.streams.insert(stream.to_owned(), log);
        }

        let stream_log = state
            .streams
            .get_mut(stream)
            .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?;
        stream_log.append(key, payload)
    }

    pub fn poll(&self, stream: &str, consumer: &str) -> Result<PollResult, BrokerError> {
        self.poll_group(stream, consumer, consumer)
    }

    pub fn poll_group(
        &self,
        stream: &str,
        consumer: &str,
        member: &str,
    ) -> Result<PollResult, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.poll");
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        validate_name("member", member)?;
        let mut state = self.lock()?;
        let consumer_key = ConsumerKey {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
        };
        let root = state.root.clone();
        let now = Instant::now();
        state
            .in_flight
            .retain(|_, in_flight| in_flight.deadline > now);
        if !state.consumer_states.is_empty() {
            let active_consumers: HashSet<_> = state
                .in_flight
                .keys()
                .map(|delivery_key| delivery_key.consumer.clone())
                .collect();
            state
                .consumer_states
                .retain(|consumer_key, _| active_consumers.contains(consumer_key));
        }

        let existing = state
            .in_flight
            .iter()
            .find(|(delivery_key, in_flight)| {
                delivery_key.consumer == consumer_key && in_flight.member == member
            })
            .map(|(delivery_key, in_flight)| (delivery_key.clone(), in_flight.clone()));
        if let Some(in_flight) = existing {
            let stream_log = state
                .streams
                .get_mut(stream)
                .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?;
            let mut message = stream_log.read_message(stream, in_flight.1.offset)?;
            message.delivery_token = Some(in_flight.1.delivery_token);
            message.delivery_attempt = Some(in_flight.1.delivery_attempt);
            return Ok(PollResult::Message(message));
        }

        let mut consumer_state = load_consumer_state_for_request(&mut state, &consumer_key)?;
        loop {
            let candidate = state
                .streams
                .get(stream)
                .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?
                .records
                .iter()
                .filter(|record| record.offset >= consumer_state.committed_offset)
                .find(|record| {
                    if consumer_state.acknowledged_offsets.contains(&record.offset) {
                        return false;
                    }
                    if state.in_flight.keys().any(|delivery_key| {
                        delivery_key.consumer == consumer_key
                            && delivery_key.offset == record.offset
                    }) {
                        return false;
                    }
                    record.key.as_ref().is_none_or(|key| {
                        !state.in_flight.iter().any(|(delivery_key, in_flight)| {
                            delivery_key.consumer == consumer_key
                                && in_flight.key.as_ref() == Some(key)
                        })
                    })
                })
                .cloned();
            let Some(candidate) = candidate else {
                return Ok(PollResult::Empty);
            };

            let attempts = consumer_state
                .delivery_attempts
                .get(&candidate.offset)
                .copied()
                .unwrap_or(0);
            if state
                .max_delivery_attempts
                .is_some_and(|max_attempts| attempts >= max_attempts)
                && !stream.ends_with(DEAD_LETTER_SUFFIX)
            {
                dead_letter_record(&mut state, stream, &candidate)?;
                consumer_state.acknowledge(candidate.offset);
                persist_consumer_state(&root, &consumer_state)?;
                state
                    .consumer_states
                    .insert(consumer_key.clone(), consumer_state.clone());
                state.dead_letters += 1;
                continue;
            }

            let delivery_attempt = attempts.saturating_add(1);
            let stream_log = state
                .streams
                .get_mut(stream)
                .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?;
            let mut message = stream_log.read_message(stream, candidate.offset)?;
            let mut next_state = consumer_state;
            next_state
                .delivery_attempts
                .insert(candidate.offset, delivery_attempt);
            persist_consumer_state(&root, &next_state)?;
            state
                .consumer_states
                .insert(consumer_key.clone(), next_state);
            let delivery_token = next_delivery_token(&mut state);
            message.delivery_token = Some(delivery_token.clone());
            message.delivery_attempt = Some(delivery_attempt);
            if delivery_attempt > 1 {
                state.redeliveries += 1;
            }
            let ack_timeout = state.ack_timeout;
            state.in_flight.insert(
                DeliveryKey {
                    consumer: consumer_key,
                    offset: candidate.offset,
                },
                InFlight {
                    member: member.to_owned(),
                    offset: candidate.offset,
                    key: candidate.key,
                    delivery_attempt,
                    delivery_token,
                    deadline: Instant::now() + ack_timeout,
                },
            );
            return Ok(PollResult::Message(message));
        }
    }

    pub fn ack(
        &self,
        stream: &str,
        consumer: &str,
        offset: Offset,
    ) -> Result<AckResult, BrokerError> {
        self.ack_group(stream, consumer, consumer, offset, "")
    }

    pub fn ack_group(
        &self,
        stream: &str,
        consumer: &str,
        member: &str,
        offset: Offset,
        delivery_token: &str,
    ) -> Result<AckResult, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.ack");
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        validate_name("member", member)?;
        let mut state = self.lock()?;
        if !state.streams.contains_key(stream) {
            return Err(BrokerError::StreamNotFound(stream.to_owned()));
        }

        let root = state.root.clone();
        let consumer_key = ConsumerKey {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
        };
        let consumer_state = load_consumer_state_for_request(&mut state, &consumer_key)?;
        if offset < consumer_state.committed_offset {
            return Ok(AckResult::AlreadyAcknowledged);
        }
        if consumer_state.acknowledged_offsets.contains(&offset) {
            return Ok(AckResult::AlreadyAcknowledged);
        }
        let delivery_key = DeliveryKey {
            consumer: consumer_key.clone(),
            offset,
        };
        let Some(in_flight) = state.in_flight.get(&delivery_key) else {
            if !delivery_token.is_empty() {
                return Err(BrokerError::StaleDelivery {
                    consumer: consumer.to_owned(),
                    offset,
                });
            }
            return Err(BrokerError::AckNotInFlight {
                consumer: consumer.to_owned(),
                offset,
            });
        };
        if in_flight.member != member
            || (!delivery_token.is_empty() && in_flight.delivery_token != delivery_token)
        {
            return Err(BrokerError::StaleDelivery {
                consumer: consumer.to_owned(),
                offset,
            });
        }

        let mut next_state = consumer_state;
        next_state.acknowledge(offset);
        let next_state = ConsumerState {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
            ..next_state
        };
        persist_consumer_state(&root, &next_state)?;
        state
            .consumer_states
            .insert(consumer_key.clone(), next_state);
        state.in_flight.remove(&delivery_key);
        if !state
            .in_flight
            .keys()
            .any(|remaining| remaining.consumer == consumer_key)
        {
            state.consumer_states.remove(&consumer_key);
        }
        Ok(AckResult::Acknowledged)
    }

    pub fn health(&self) -> Result<HealthSnapshot, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.health");
        let state = self.lock()?;
        let mut storage_bytes = 0;
        for stream in state.streams.values() {
            storage_bytes += stream.file.metadata()?.len();
        }
        Ok(HealthSnapshot {
            streams: state.streams.len(),
            storage_bytes,
            redeliveries: state.redeliveries,
            dead_letters: state.dead_letters,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, BrokerState>, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.lock_wait");
        self.inner.lock().map_err(|_| BrokerError::LockPoisoned)
    }
}

impl Engine for Broker {
    fn create_stream<'a>(&'a self, stream: &'a str) -> EngineFuture<'a, bool> {
        Box::pin(async move { Broker::create_stream(self, stream) })
    }

    fn publish<'a>(
        &'a self,
        stream: &'a str,
        key: Option<String>,
        payload: Vec<u8>,
        _request_id: Option<String>,
    ) -> EngineFuture<'a, Offset> {
        Box::pin(async move { Broker::publish(self, stream, key, payload) })
    }

    fn poll<'a>(&'a self, stream: &'a str, consumer: &'a str) -> EngineFuture<'a, PollResult> {
        Box::pin(async move { Broker::poll(self, stream, consumer) })
    }

    fn poll_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
    ) -> EngineFuture<'a, PollResult> {
        Box::pin(async move { Broker::poll_group(self, stream, consumer, member) })
    }

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move { Broker::ack(self, stream, consumer, offset) })
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
            Broker::ack_group(self, stream, consumer, member, offset, delivery_token)
        })
    }

    fn health<'a>(&'a self) -> EngineFuture<'a, HealthSnapshot> {
        Box::pin(async move { Broker::health(self) })
    }
}

impl StreamLog {
    fn create(path: &Path) -> Result<Self, BrokerError> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file,
            records: Vec::new(),
        })
    }

    fn open(path: &Path) -> Result<Self, BrokerError> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        let mut records = Vec::new();
        let mut cursor = 0usize;
        while bytes.len().saturating_sub(cursor) >= HEADER_LEN {
            let header = &bytes[cursor..cursor + HEADER_LEN];
            if &header[..4] != MAGIC {
                break;
            }

            let offset = u64::from_le_bytes(header[4..12].try_into().unwrap());
            let published_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
            let key_len = u32::from_le_bytes(header[20..24].try_into().unwrap()) as usize;
            let payload_len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
            let Some(record_len) = HEADER_LEN
                .checked_add(key_len)
                .and_then(|length| length.checked_add(payload_len))
            else {
                break;
            };
            if bytes.len().saturating_sub(cursor) < record_len {
                break;
            }

            let key_start = cursor + HEADER_LEN;
            let payload_start = key_start + key_len;
            let key = if key_len == 0 {
                None
            } else {
                match std::str::from_utf8(&bytes[key_start..payload_start]) {
                    Ok(key) => Some(key.to_owned()),
                    Err(_) => break,
                }
            };
            records.push(RecordIndex {
                offset,
                payload_offset: payload_start as u64,
                payload_len: payload_len as u32,
                key,
                published_at_ms,
            });
            cursor += record_len;
        }

        if cursor != bytes.len() {
            file.set_len(cursor as u64)?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self { file, records })
    }

    fn append(&mut self, key: Option<String>, payload: Vec<u8>) -> Result<Offset, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.storage_append");
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
        let offset = self.records.last().map_or(0, |record| record.offset + 1);
        let published_at_ms = now_ms();

        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&offset.to_le_bytes());
        header.extend_from_slice(&published_at_ms.to_le_bytes());
        header.extend_from_slice(&key_len.to_le_bytes());
        header.extend_from_slice(&payload_len.to_le_bytes());

        self.file.write_all(&header)?;
        self.file.write_all(key_bytes)?;
        self.file.write_all(&payload)?;
        self.file.sync_data()?;

        let payload_offset = self.file.stream_position()? - payload.len() as u64;
        self.records.push(RecordIndex {
            offset,
            payload_offset,
            payload_len,
            key,
            published_at_ms,
        });
        Ok(offset)
    }

    fn read_message(&mut self, stream: &str, offset: Offset) -> Result<Message, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.storage_read");
        let Some(index) = self.records.iter().find(|record| record.offset == offset) else {
            return Err(BrokerError::CorruptRecord(offset));
        };
        let mut payload = vec![0; index.payload_len as usize];
        self.file.seek(SeekFrom::Start(index.payload_offset))?;
        self.file.read_exact(&mut payload)?;
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
}

fn dead_letter_record(
    state: &mut BrokerState,
    stream: &str,
    record: &RecordIndex,
) -> Result<(), BrokerError> {
    let dead_letter_stream = dead_letter_stream_name(stream)?;
    let message = state
        .streams
        .get_mut(stream)
        .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?
        .read_message(stream, record.offset)?;

    if !state.streams.contains_key(&dead_letter_stream) {
        let path = stream_path(&state.root, &dead_letter_stream);
        let log = StreamLog::create(&path)?;
        state.streams.insert(dead_letter_stream.clone(), log);
    }
    state
        .streams
        .get_mut(&dead_letter_stream)
        .ok_or_else(|| BrokerError::StreamNotFound(dead_letter_stream.clone()))?
        .append(message.key, message.payload)?;
    Ok(())
}

fn stream_path(root: &Path, stream: &str) -> PathBuf {
    root.join("streams").join(format!("{stream}.log"))
}

fn dead_letter_stream_name(stream: &str) -> Result<String, BrokerError> {
    let name = format!("{stream}{DEAD_LETTER_SUFFIX}");
    if name.len() <= 128 {
        validate_name("dead-letter stream", &name)?;
        return Ok(name);
    }

    let hash = stream.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    let fallback = format!("runnel.dead-letter.{hash:016x}");
    validate_name("dead-letter stream", &fallback)?;
    Ok(fallback)
}

fn consumer_state_path(root: &Path, stream: &str, consumer: &str) -> PathBuf {
    root.join("consumers")
        .join(stream)
        .join(format!("{consumer}.json"))
}

fn load_consumer_state(
    root: &Path,
    stream: &str,
    consumer: &str,
) -> Result<ConsumerState, BrokerError> {
    let path = consumer_state_path(root, stream, consumer);
    if !path.exists() {
        return Ok(ConsumerState {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
            committed_offset: 0,
            acknowledged_offsets: BTreeSet::new(),
            delivery_attempts: BTreeMap::new(),
        });
    }
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn load_consumer_state_for_request(
    broker_state: &mut BrokerState,
    consumer_key: &ConsumerKey,
) -> Result<ConsumerState, BrokerError> {
    if let Some(cached) = broker_state.consumer_states.get(consumer_key) {
        return Ok(cached.clone());
    }

    load_consumer_state(
        &broker_state.root,
        &consumer_key.stream,
        &consumer_key.consumer,
    )
}

fn persist_consumer_state(root: &Path, state: &ConsumerState) -> Result<(), BrokerError> {
    #[cfg(feature = "instrumentation")]
    let _stage_timer = StageTimer::new("core.consumer_state_persist");
    let path = consumer_state_path(root, &state.stream, &state.consumer);
    let parent = path.parent().ok_or_else(|| {
        BrokerError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "consumer state has no parent",
        ))
    })?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("json.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temporary)?;
    serde_json::to_writer(&mut file, state)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn delivery_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn next_delivery_token(state: &mut BrokerState) -> String {
    state.next_delivery_id = state.next_delivery_id.wrapping_add(1);
    format!("{:x}-{:x}", state.delivery_epoch, state.next_delivery_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn independent_consumers_each_receive_the_stream() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();

        assert!(broker.create_stream("events").unwrap());
        assert!(!broker.create_stream("events").unwrap());
        assert_eq!(
            broker.publish("events", None, b"hello".to_vec()).unwrap(),
            0
        );

        let first = broker.poll("events", "worker-a").unwrap();
        assert!(matches!(
            first,
            PollResult::Message(Message { offset: 0, .. })
        ));
        assert_eq!(
            broker.ack("events", "worker-a", 0).unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            broker.ack("events", "worker-a", 0).unwrap(),
            AckResult::AlreadyAcknowledged
        );
        assert_eq!(
            broker.poll("events", "worker-a").unwrap(),
            PollResult::Empty
        );

        let second = broker.poll("events", "worker-b").unwrap();
        assert!(matches!(
            second,
            PollResult::Message(Message { offset: 0, .. })
        ));
    }

    #[test]
    fn grouped_consumers_share_records_and_allow_out_of_order_acknowledgements() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        for payload in [
            b"first".as_slice(),
            b"second".as_slice(),
            b"third".as_slice(),
        ] {
            broker.publish("events", None, payload.to_vec()).unwrap();
        }

        let (first_offset, first_token) = delivery(broker.poll_group("events", "workers", "a"));
        let (second_offset, second_token) = delivery(broker.poll_group("events", "workers", "b"));
        assert_eq!((first_offset, second_offset), (0, 1));

        assert_eq!(
            broker
                .ack_group("events", "workers", "b", second_offset, &second_token)
                .unwrap(),
            AckResult::Acknowledged
        );
        let (third_offset, third_token) = delivery(broker.poll_group("events", "workers", "b"));
        assert_eq!(third_offset, 2);

        assert_eq!(
            broker
                .ack_group("events", "workers", "a", first_offset, &first_token)
                .unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            broker
                .ack_group("events", "workers", "b", third_offset, &third_token)
                .unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            broker.poll_group("events", "workers", "a").unwrap(),
            PollResult::Empty
        );
    }

    #[test]
    fn grouped_consumers_preserve_order_for_each_key() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker
            .publish("events", Some("customer-a".to_owned()), b"a1".to_vec())
            .unwrap();
        broker
            .publish("events", Some("customer-a".to_owned()), b"a2".to_vec())
            .unwrap();
        broker
            .publish("events", Some("customer-b".to_owned()), b"b1".to_vec())
            .unwrap();

        let (first_offset, first_token) = delivery(broker.poll_group("events", "workers", "a"));
        assert_eq!(first_offset, 0);
        let (other_offset, other_token) = delivery(broker.poll_group("events", "workers", "b"));
        assert_eq!(other_offset, 2);
        assert!(matches!(
            broker.poll_group("events", "workers", "a").unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));

        broker
            .ack_group("events", "workers", "a", first_offset, &first_token)
            .unwrap();
        broker
            .ack_group("events", "workers", "b", other_offset, &other_token)
            .unwrap();
        assert!(matches!(
            broker.poll_group("events", "workers", "b").unwrap(),
            PollResult::Message(Message { offset: 1, .. })
        ));
    }

    #[test]
    fn expired_group_delivery_rejects_stale_acknowledgement() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout: Duration::from_millis(100),
                max_delivery_attempts: None,
            },
        )
        .unwrap();
        broker.publish("events", None, b"payload".to_vec()).unwrap();
        let (offset, old_token) = delivery(broker.poll_group("events", "workers", "a"));
        std::thread::sleep(Duration::from_millis(250));
        let (_, new_token) = delivery(broker.poll_group("events", "workers", "b"));

        assert!(matches!(
            broker.ack_group("events", "workers", "a", offset, &old_token),
            Err(BrokerError::StaleDelivery { .. })
        ));
        assert_eq!(
            broker
                .ack_group("events", "workers", "b", offset, &new_token)
                .unwrap(),
            AckResult::Acknowledged
        );
    }

    #[test]
    fn delivery_attempts_are_durable_and_dead_letter_after_limit() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::from_millis(100),
            max_delivery_attempts: Some(2),
        };
        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        broker
            .publish("events", Some("order-1".to_owned()), b"poison".to_vec())
            .unwrap();

        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        std::thread::sleep(Duration::from_millis(250));
        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        std::thread::sleep(Duration::from_millis(250));

        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        assert_eq!(
            broker.health().unwrap(),
            HealthSnapshot {
                streams: 2,
                storage_bytes: broker.health().unwrap().storage_bytes,
                redeliveries: 1,
                dead_letters: 1,
            }
        );

        let dead_letter = broker.poll("events.dead-letter", "inspector").unwrap();
        assert!(matches!(
            dead_letter,
            PollResult::Message(Message {
                key: Some(key),
                payload,
                delivery_attempt: Some(1),
                ..
            }) if key == "order-1" && payload == b"poison"
        ));
        std::thread::sleep(Duration::from_millis(250));
        assert!(matches!(
            broker.poll("events.dead-letter", "inspector").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        assert_eq!(broker.health().unwrap().streams, 2);
        assert_eq!(
            broker.ack("events.dead-letter", "inspector", 0).unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            broker.poll("events.dead-letter", "inspector").unwrap(),
            PollResult::Empty
        );

        drop(broker);
        let reopened = Broker::open(directory.path(), config).unwrap();
        assert_eq!(
            reopened.poll("events", "worker").unwrap(),
            PollResult::Empty
        );
        assert_eq!(
            reopened.poll("events.dead-letter", "inspector").unwrap(),
            PollResult::Empty
        );
    }

    fn delivery(result: Result<PollResult, BrokerError>) -> (Offset, String) {
        match result.unwrap() {
            PollResult::Message(message) => (
                message.offset,
                message
                    .delivery_token
                    .expect("group deliveries should have a token"),
            ),
            PollResult::Empty => panic!("expected a message"),
        }
    }

    #[test]
    fn unacknowledged_message_is_delivered_after_restart() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            broker
                .publish("events", Some("order-1".to_owned()), b"payload".to_vec())
                .unwrap();
            assert!(matches!(
                broker.poll("events", "worker").unwrap(),
                PollResult::Message(Message { offset: 0, .. })
            ));
        }

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let result = broker.poll("events", "worker").unwrap();
        assert!(matches!(
            result,
            PollResult::Message(Message { offset: 0, .. })
        ));
    }

    #[test]
    fn acknowledged_group_progress_and_retry_state_survive_restart() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::from_secs(60),
            max_delivery_attempts: None,
        };
        {
            let broker = Broker::open(directory.path(), config.clone()).unwrap();
            for payload in [
                b"first".as_slice(),
                b"second".as_slice(),
                b"third".as_slice(),
            ] {
                broker.publish("events", None, payload.to_vec()).unwrap();
            }

            let first = delivery(broker.poll_group("events", "workers", "member-a"));
            let second = delivery(broker.poll_group("events", "workers", "member-b"));
            assert_eq!((first.0, second.0), (0, 1));
            assert_eq!(
                broker
                    .ack_group("events", "workers", "member-b", second.0, &second.1)
                    .unwrap(),
                AckResult::Acknowledged
            );
        }

        let broker = Broker::open(directory.path(), config).unwrap();
        let redelivered = match broker
            .poll_group("events", "workers", "replacement")
            .unwrap()
        {
            PollResult::Message(message) => {
                assert_eq!(message.offset, 0);
                assert_eq!(message.delivery_attempt, Some(2));
                message.delivery_token.unwrap()
            }
            PollResult::Empty => panic!("expected the unacknowledged message to be redelivered"),
        };
        assert_eq!(
            broker
                .ack_group("events", "workers", "replacement", 0, &redelivered)
                .unwrap(),
            AckResult::Acknowledged
        );
        assert!(matches!(
            broker
                .poll_group("events", "workers", "replacement")
                .unwrap(),
            PollResult::Message(Message { offset: 2, .. })
        ));
    }

    #[test]
    fn expired_in_flight_message_is_redelivered() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout: Duration::from_millis(100),
                max_delivery_attempts: None,
            },
        )
        .unwrap();
        broker.publish("events", None, b"payload".to_vec()).unwrap();
        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));
        std::thread::sleep(Duration::from_millis(250));
        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));
    }

    #[test]
    fn incomplete_trailing_frame_is_discarded_on_recovery() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            broker
                .publish("events", None, b"complete".to_vec())
                .unwrap();
        }
        let path = directory.path().join("streams/events.log");
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(b"RNL1partial").unwrap();
        file.sync_all().unwrap();

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let result = broker.poll("events", "worker").unwrap();
        assert!(matches!(
            result,
            PollResult::Message(Message {
                offset: 0,
                payload,
                ..
            }) if payload == b"complete"
        ));
    }
}
