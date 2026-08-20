use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use runnel_engine::{Engine, EngineFuture};
use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 4] = b"RNL1";
const HEADER_LEN: usize = 28;
const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);

pub use runnel_engine::{AckResult, BrokerError, HealthSnapshot, Message, Offset, PollResult};

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub ack_timeout: Duration,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            ack_timeout: DEFAULT_ACK_TIMEOUT,
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
    in_flight: HashMap<ConsumerKey, InFlight>,
    ack_timeout: Duration,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ConsumerKey {
    stream: String,
    consumer: String,
}

#[derive(Debug, Clone)]
struct InFlight {
    offset: Offset,
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

#[derive(Debug, Serialize, Deserialize)]
struct ConsumerState {
    stream: String,
    consumer: String,
    committed_offset: Offset,
}

impl Broker {
    pub fn open(root: impl AsRef<Path>, config: BrokerConfig) -> Result<Self, BrokerError> {
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
                in_flight: HashMap::new(),
                ack_timeout: config.ack_timeout,
            })),
        })
    }

    pub fn create_stream(&self, stream: &str) -> Result<bool, BrokerError> {
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
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        let mut state = self.lock()?;
        let key = ConsumerKey {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
        };
        let root = state.root.clone();

        let existing = state.in_flight.get(&key).cloned();
        if let Some(in_flight) = existing {
            if Instant::now() < in_flight.deadline {
                let stream_log = state
                    .streams
                    .get_mut(stream)
                    .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?;
                return Ok(PollResult::Message(
                    stream_log.read_message(stream, in_flight.offset)?,
                ));
            }
            state.in_flight.remove(&key);
        }

        let consumer_state = load_consumer_state(&root, stream, consumer)?;
        let stream_log = state
            .streams
            .get_mut(stream)
            .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?;
        if consumer_state.committed_offset as usize >= stream_log.records.len() {
            return Ok(PollResult::Empty);
        }

        let offset = consumer_state.committed_offset;
        let message = stream_log.read_message(stream, offset)?;
        let ack_timeout = state.ack_timeout;
        state.in_flight.insert(
            key,
            InFlight {
                offset,
                deadline: Instant::now() + ack_timeout,
            },
        );
        Ok(PollResult::Message(message))
    }

    pub fn ack(
        &self,
        stream: &str,
        consumer: &str,
        offset: Offset,
    ) -> Result<AckResult, BrokerError> {
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        let mut state = self.lock()?;
        if !state.streams.contains_key(stream) {
            return Err(BrokerError::StreamNotFound(stream.to_owned()));
        }

        let root = state.root.clone();
        let consumer_state = load_consumer_state(&root, stream, consumer)?;
        if offset < consumer_state.committed_offset {
            return Ok(AckResult::AlreadyAcknowledged);
        }
        if offset > consumer_state.committed_offset {
            return Err(BrokerError::OutOfOrderAck {
                consumer: consumer.to_owned(),
                expected: consumer_state.committed_offset,
                received: offset,
            });
        }

        let key = ConsumerKey {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
        };
        let Some(in_flight) = state.in_flight.get(&key) else {
            return Err(BrokerError::AckNotInFlight {
                consumer: consumer.to_owned(),
                offset,
            });
        };
        if in_flight.offset != offset {
            return Err(BrokerError::AckNotInFlight {
                consumer: consumer.to_owned(),
                offset,
            });
        }

        let next_state = ConsumerState {
            stream: stream.to_owned(),
            consumer: consumer.to_owned(),
            committed_offset: offset + 1,
        };
        persist_consumer_state(&root, &next_state)?;
        state.in_flight.remove(&key);
        Ok(AckResult::Acknowledged)
    }

    pub fn health(&self) -> Result<HealthSnapshot, BrokerError> {
        let state = self.lock()?;
        let mut storage_bytes = 0;
        for stream in state.streams.values() {
            storage_bytes += stream.file.metadata()?.len();
        }
        Ok(HealthSnapshot {
            streams: state.streams.len(),
            storage_bytes,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, BrokerState>, BrokerError> {
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

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move { Broker::ack(self, stream, consumer, offset) })
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
        })
    }
}

fn stream_path(root: &Path, stream: &str) -> PathBuf {
    root.join("streams").join(format!("{stream}.log"))
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
        });
    }
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn persist_consumer_state(root: &Path, state: &ConsumerState) -> Result<(), BrokerError> {
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
    fn expired_in_flight_message_is_redelivered() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout: Duration::from_millis(10),
            },
        )
        .unwrap();
        broker.publish("events", None, b"payload".to_vec()).unwrap();
        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));
        std::thread::sleep(Duration::from_millis(20));
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
