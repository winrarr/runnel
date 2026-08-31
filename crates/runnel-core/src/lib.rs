use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{
    Engine, EngineFuture, MAX_PUBLISH_BATCH_RECORDS, PublishRecord, PublishRecordOutcome,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

const LEGACY_MAGIC: &[u8; 4] = b"RNL1";
const VERSIONED_MAGIC: &[u8; 4] = b"RNL2";
const REQUEST_ID_MAGIC: &[u8; 4] = b"RNL3";
const LEGACY_HEADER_LEN: usize = 28;
const VERSIONED_HEADER_LEN: usize = 44;
const REQUEST_ID_HEADER_LEN: usize = 48;
const VERSIONED_FORMAT_VERSION: u8 = 1;
const REQUEST_ID_FORMAT_VERSION: u8 = 1;
const VERSIONED_ENCODING_BYTES: u8 = 0;
const VERSIONED_COMPRESSION_NONE: u8 = 0;
const VERSIONED_MAX_KEY_LEN: u32 = 128;
const VERSIONED_MAX_BODY_LEN: u32 = 64 * 1024 * 1024;
const REQUEST_ID_MAX_LEN: u32 = 1024;
const REQUEST_ID_MAX_KEY_LEN: u32 = 128;
const REQUEST_ID_MAX_BODY_LEN: u32 = 64 * 1024 * 1024;
const MAX_IN_MEMORY_RECORDS: usize = 1024;
const MAX_CACHED_CONSUMER_STATES: usize = 1024;
const SPARSE_INDEX_STRIDE: Offset = 64;
const MAX_SPARSE_INDEX_ENTRIES: usize = 1024;
const DEFAULT_STORAGE_EXECUTOR_CAPACITY: usize = 32;
const DEFAULT_STORAGE_QUEUE_CAPACITY: usize = 32;
const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const DEAD_LETTER_SUFFIX: &str = ".dead-letter";

pub use runnel_engine::{AckResult, BrokerError, HealthSnapshot, Message, Offset, PollResult};

/// Selects the durable record format used for new appends.
///
/// Readers accept both the legacy `RNL1` format and versioned `RNL2` frames.
/// The versioned format is deliberately opt-in until its compatibility policy
/// is accepted for normal broker deployments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableFormat {
    Rnl1,
    VersionedV1,
}

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
    inner: Arc<BrokerState>,
}

struct BrokerState {
    root: PathBuf,
    durable_format: DurableFormat,
    streams: RwLock<HashMap<String, Arc<Mutex<StreamState>>>>,
    ack_timeout: Duration,
    max_delivery_attempts: Option<u32>,
    redeliveries: AtomicU64,
    dead_letters: AtomicU64,
    delivery_epoch: u128,
    next_delivery_id: AtomicU64,
    storage_executor: Arc<StorageExecutor>,
}

struct StorageExecutor {
    execution_permits: Arc<Semaphore>,
    admission_permits: Arc<Semaphore>,
    // Weak entries keep transient lane state from growing with one-off requests for unknown
    // streams. Active and queued operations hold the strong lane reference until they finish.
    stream_lanes: Mutex<HashMap<String, Weak<Semaphore>>>,
}

impl StorageExecutor {
    fn new() -> Self {
        Self::with_capacities(
            DEFAULT_STORAGE_EXECUTOR_CAPACITY,
            DEFAULT_STORAGE_QUEUE_CAPACITY,
        )
    }

    fn with_capacities(execution_capacity: usize, queue_capacity: usize) -> Self {
        Self {
            execution_permits: Arc::new(Semaphore::new(execution_capacity)),
            admission_permits: Arc::new(Semaphore::new(
                execution_capacity.saturating_add(queue_capacity),
            )),
            stream_lanes: Mutex::new(HashMap::new()),
        }
    }

    fn dispatch<T, F>(self: Arc<Self>, operation: F) -> EngineFuture<'static, T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, BrokerError> + Send + 'static,
    {
        self.dispatch_with_lane(None, operation)
    }

    fn dispatch_stream<T, F>(
        self: Arc<Self>,
        stream: String,
        operation: F,
    ) -> EngineFuture<'static, T>
    where
        T: Send + 'static,
        F: FnOnce(&str) -> Result<T, BrokerError> + Send + 'static,
    {
        if let Err(error) = validate_name("stream", &stream) {
            return Box::pin(async move { Err(error) });
        }
        let lane = match self.stream_lane(&stream) {
            Ok(lane) => lane,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        self.dispatch_with_lane(Some(lane), move || operation(&stream))
    }

    fn dispatch_with_lane<T, F>(
        self: Arc<Self>,
        stream_lane: Option<Arc<Semaphore>>,
        operation: F,
    ) -> EngineFuture<'static, T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, BrokerError> + Send + 'static,
    {
        Box::pin(async move {
            #[cfg(feature = "instrumentation")]
            let _stage_timer = StageTimer::new("core.storage_dispatch");

            // Admission is reserved before waiting for an execution permit, so the number of
            // active and queued blocking operations is bounded. Waiting for execution remains
            // asynchronous; only the synchronous operation runs on Tokio's blocking pool.
            let admission_permit = Arc::clone(&self.admission_permits)
                .try_acquire_owned()
                .map_err(|_| {
                    BrokerError::Io(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "storage execution queue is full",
                    ))
                })?;
            let stream_permit = match stream_lane {
                Some(stream_lane) => Some(stream_lane.acquire_owned().await.map_err(|_| {
                    BrokerError::Io(io::Error::other("storage stream lane was closed"))
                })?),
                None => None,
            };
            let execution_permit = Arc::clone(&self.execution_permits)
                .acquire_owned()
                .await
                .map_err(|_| {
                    BrokerError::Io(io::Error::other("storage execution capacity was closed"))
                })?;
            tokio::task::spawn_blocking(move || {
                let _admission_permit = admission_permit;
                let _stream_permit = stream_permit;
                let _execution_permit = execution_permit;
                operation()
            })
            .await
            .map_err(|error| {
                let message = if error.is_panic() {
                    "storage execution task panicked"
                } else {
                    "storage execution task was cancelled"
                };
                BrokerError::Io(io::Error::other(message))
            })?
        })
    }

    fn stream_lane(&self, stream: &str) -> Result<Arc<Semaphore>, BrokerError> {
        let mut stream_lanes = self
            .stream_lanes
            .lock()
            .map_err(|_| BrokerError::LockPoisoned)?;
        stream_lanes.retain(|_, lane| lane.upgrade().is_some());
        if let Some(lane) = stream_lanes.get(stream).and_then(Weak::upgrade) {
            return Ok(lane);
        }

        let lane = Arc::new(Semaphore::new(1));
        stream_lanes.insert(stream.to_owned(), Arc::downgrade(&lane));
        Ok(lane)
    }
}

struct StreamState {
    log: StreamLog,
    // Consumer state and in-flight deliveries are owned by the stream so independent streams
    // do not contend on a process-wide broker lock. The bounded cache is only a best-effort
    // fast path; the durable checkpoint remains the source of truth across restart and eviction.
    consumer_states: HashMap<String, ConsumerState>,
    in_flight: HashMap<DeliveryKey, InFlight>,
    // This index mirrors only active deliveries and is removed when a consumer has no deliveries.
    // It avoids rebuilding per-consumer offset/key sets and scanning members on every poll.
    in_flight_by_consumer: HashMap<String, ConsumerInFlightIndex>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DeliveryKey {
    consumer: String,
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

#[derive(Default)]
struct ConsumerInFlightIndex {
    offsets: HashSet<Offset>,
    keys: HashSet<String>,
    members: HashMap<String, Offset>,
}

struct StreamLog {
    file: File,
    durable_format: DurableFormat,
    // The durable log retains the complete history; this tail cache keeps normal delivery
    // bounded while older replay requests use the bounded sparse index as a scan starting point.
    records: VecDeque<RecordIndex>,
    sparse_index: VecDeque<LogCheckpoint>,
    request_ids: HashMap<String, Offset>,
    next_offset: Offset,
}

impl StreamState {
    fn new(log: StreamLog) -> Self {
        Self {
            log,
            consumer_states: HashMap::new(),
            in_flight: HashMap::new(),
            in_flight_by_consumer: HashMap::new(),
        }
    }

    fn expire_in_flight(&mut self, now: Instant) {
        let expired = self
            .in_flight
            .iter()
            .filter(|(_, in_flight)| in_flight.deadline <= now)
            .map(|(delivery_key, _)| delivery_key.clone())
            .collect::<Vec<_>>();
        for delivery_key in expired {
            self.remove_in_flight(&delivery_key);
        }
    }

    fn insert_in_flight(&mut self, delivery_key: DeliveryKey, in_flight: InFlight) {
        let consumer = delivery_key.consumer.clone();
        let offset = delivery_key.offset;
        let member = in_flight.member.clone();
        let key = in_flight.key.clone();
        debug_assert!(!self.in_flight.contains_key(&delivery_key));
        self.in_flight.insert(delivery_key, in_flight);

        let index = self.in_flight_by_consumer.entry(consumer).or_default();
        index.offsets.insert(offset);
        if let Some(key) = key {
            index.keys.insert(key);
        }
        index.members.insert(member, offset);
    }

    fn remove_in_flight(&mut self, delivery_key: &DeliveryKey) -> Option<InFlight> {
        let in_flight = self.in_flight.remove(delivery_key)?;
        let mut remove_consumer_index = false;
        if let Some(index) = self.in_flight_by_consumer.get_mut(&delivery_key.consumer) {
            index.offsets.remove(&delivery_key.offset);
            if let Some(key) = in_flight.key.as_ref() {
                index.keys.remove(key);
            }
            if index
                .members
                .get(&in_flight.member)
                .is_some_and(|offset| *offset == delivery_key.offset)
            {
                index.members.remove(&in_flight.member);
            }
            remove_consumer_index = index.offsets.is_empty();
        }
        if remove_consumer_index {
            self.in_flight_by_consumer.remove(&delivery_key.consumer);
        }
        Some(in_flight)
    }

    fn find_candidate(
        &mut self,
        consumer: &str,
        committed_offset: Offset,
        acknowledged_offsets: &BTreeSet<Offset>,
    ) -> Result<Option<RecordIndex>, BrokerError> {
        let StreamState {
            log,
            in_flight_by_consumer,
            ..
        } = self;
        log.find_candidate(
            committed_offset,
            acknowledged_offsets,
            in_flight_by_consumer.get(consumer),
        )
    }
}

#[derive(Debug, Clone)]
struct RecordIndex {
    offset: Offset,
    payload_offset: u64,
    payload_len: u32,
    key: Option<String>,
    request_id: Option<String>,
    published_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct LogCheckpoint {
    offset: Offset,
    cursor: u64,
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
        Self::open_with_format(root, config, DurableFormat::Rnl1)
    }

    pub fn open_with_format(
        root: impl AsRef<Path>,
        config: BrokerConfig,
        durable_format: DurableFormat,
    ) -> Result<Self, BrokerError> {
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
            let log = StreamLog::open(&path, durable_format)?;
            streams.insert(name.to_owned(), Arc::new(Mutex::new(StreamState::new(log))));
        }

        Ok(Self {
            inner: Arc::new(BrokerState {
                root,
                durable_format,
                streams: RwLock::new(streams),
                ack_timeout: config.ack_timeout,
                max_delivery_attempts: config.max_delivery_attempts,
                redeliveries: AtomicU64::new(0),
                dead_letters: AtomicU64::new(0),
                delivery_epoch: delivery_epoch(),
                next_delivery_id: AtomicU64::new(0),
                storage_executor: Arc::new(StorageExecutor::new()),
            }),
        })
    }

    pub fn create_stream(&self, stream: &str) -> Result<bool, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.create_stream");
        validate_name("stream", stream)?;
        let mut streams = self
            .inner
            .streams
            .write()
            .map_err(|_| BrokerError::LockPoisoned)?;
        if streams.contains_key(stream) {
            return Ok(false);
        }

        let path = stream_path(&self.inner.root, stream);
        let log = StreamLog::create(&path, self.inner.durable_format)?;
        streams.insert(
            stream.to_owned(),
            Arc::new(Mutex::new(StreamState::new(log))),
        );
        Ok(true)
    }

    pub fn publish(
        &self,
        stream: &str,
        key: Option<String>,
        payload: Vec<u8>,
    ) -> Result<Offset, BrokerError> {
        self.publish_with_request_id(stream, key, payload, None)
    }

    fn publish_with_request_id(
        &self,
        stream: &str,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
    ) -> Result<Offset, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.publish");
        validate_name("stream", stream)?;
        let stream_state = self.get_or_create_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        if let Some(request_id) = request_id.as_ref()
            && let Some(offset) = stream_state.log.request_ids.get(request_id)
        {
            // As with the clustered engine, a repeated identity resolves to its original
            // offset; payload and key mismatches are intentionally ignored for compatibility.
            return Ok(*offset);
        }
        match request_id {
            Some(request_id) => stream_state
                .log
                .append_with_request_id(key, payload, request_id),
            None => stream_state.log.append(key, payload),
        }
    }

    pub fn publish_batch(
        &self,
        stream: &str,
        records: Vec<PublishRecord>,
    ) -> Result<Vec<PublishRecordOutcome>, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.publish_batch");
        if records.len() > MAX_PUBLISH_BATCH_RECORDS {
            return Err(BrokerError::Configuration(format!(
                "publish batch contains more than {MAX_PUBLISH_BATCH_RECORDS} records"
            )));
        }
        validate_name("stream", stream)?;
        let stream_state = self.get_or_create_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        stream_state.log.append_batch(records)
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
        let stream_state = self.get_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        let root = self.inner.root.clone();
        let now = Instant::now();
        stream_state.expire_in_flight(now);

        let existing_offset = stream_state
            .in_flight_by_consumer
            .get(consumer)
            .and_then(|index| index.members.get(member))
            .copied();
        if let Some(offset) = existing_offset {
            let delivery_key = DeliveryKey {
                consumer: consumer.to_owned(),
                offset,
            };
            let Some(in_flight) = stream_state.in_flight.get(&delivery_key).cloned() else {
                return Err(BrokerError::CorruptRecord(offset));
            };
            let mut message = stream_state.log.read_message(stream, in_flight.offset)?;
            message.delivery_token = Some(in_flight.delivery_token);
            message.delivery_attempt = Some(in_flight.delivery_attempt);
            return Ok(PollResult::Message(message));
        }

        let mut consumer_state =
            load_consumer_state_for_request(&mut stream_state, &root, stream, consumer)?;
        loop {
            let candidate = stream_state.find_candidate(
                consumer,
                consumer_state.committed_offset,
                &consumer_state.acknowledged_offsets,
            )?;
            let Some(candidate) = candidate else {
                return Ok(PollResult::Empty);
            };

            let attempts = consumer_state
                .delivery_attempts
                .get(&candidate.offset)
                .copied()
                .unwrap_or(0);
            if self
                .inner
                .max_delivery_attempts
                .is_some_and(|max_attempts| attempts >= max_attempts)
                && !stream.ends_with(DEAD_LETTER_SUFFIX)
            {
                self.dead_letter_record(&mut stream_state, stream, &candidate)?;
                consumer_state.acknowledge(candidate.offset);
                persist_consumer_state(&root, &consumer_state)?;
                cache_consumer_state(
                    &mut stream_state,
                    consumer.to_owned(),
                    consumer_state.clone(),
                );
                self.inner.dead_letters.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let delivery_attempt = attempts.saturating_add(1);
            let mut message = stream_state.log.read_message(stream, candidate.offset)?;
            let mut next_state = consumer_state;
            next_state
                .delivery_attempts
                .insert(candidate.offset, delivery_attempt);
            persist_consumer_state(&root, &next_state)?;
            let delivery_token = self.next_delivery_token();
            message.delivery_token = Some(delivery_token.clone());
            message.delivery_attempt = Some(delivery_attempt);
            if delivery_attempt > 1 {
                self.inner.redeliveries.fetch_add(1, Ordering::Relaxed);
            }
            let ack_timeout = self.inner.ack_timeout;
            stream_state.insert_in_flight(
                DeliveryKey {
                    consumer: consumer.to_owned(),
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
            cache_consumer_state(&mut stream_state, consumer.to_owned(), next_state);
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
        let stream_state = self.get_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        let root = self.inner.root.clone();
        let consumer_state =
            load_consumer_state_for_request(&mut stream_state, &root, stream, consumer)?;
        if offset < consumer_state.committed_offset {
            return Ok(AckResult::AlreadyAcknowledged);
        }
        if consumer_state.acknowledged_offsets.contains(&offset) {
            return Ok(AckResult::AlreadyAcknowledged);
        }
        let delivery_key = DeliveryKey {
            consumer: consumer.to_owned(),
            offset,
        };
        let Some(in_flight) = stream_state.in_flight.get(&delivery_key) else {
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
        stream_state.remove_in_flight(&delivery_key);
        cache_consumer_state(&mut stream_state, consumer.to_owned(), next_state);
        Ok(AckResult::Acknowledged)
    }

    pub fn health(&self) -> Result<HealthSnapshot, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.health");
        let streams = self
            .inner
            .streams
            .read()
            .map_err(|_| BrokerError::LockPoisoned)?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut storage_bytes = 0;
        for stream in &streams {
            let stream = self.lock_stream(stream)?;
            storage_bytes += stream.log.file.metadata()?.len();
        }
        Ok(HealthSnapshot {
            streams: streams.len(),
            storage_bytes,
            redeliveries: self.inner.redeliveries.load(Ordering::Relaxed),
            dead_letters: self.inner.dead_letters.load(Ordering::Relaxed),
        })
    }

    fn get_stream(&self, stream: &str) -> Result<Arc<Mutex<StreamState>>, BrokerError> {
        let streams = self
            .inner
            .streams
            .read()
            .map_err(|_| BrokerError::LockPoisoned)?;
        streams
            .get(stream)
            .cloned()
            .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))
    }

    fn get_or_create_stream(&self, stream: &str) -> Result<Arc<Mutex<StreamState>>, BrokerError> {
        {
            let streams = self
                .inner
                .streams
                .read()
                .map_err(|_| BrokerError::LockPoisoned)?;
            if let Some(stream_state) = streams.get(stream) {
                return Ok(Arc::clone(stream_state));
            }
        }

        let mut streams = self
            .inner
            .streams
            .write()
            .map_err(|_| BrokerError::LockPoisoned)?;
        if let Some(stream_state) = streams.get(stream) {
            return Ok(Arc::clone(stream_state));
        }
        let path = stream_path(&self.inner.root, stream);
        let log = StreamLog::create(&path, self.inner.durable_format)?;
        let stream_state = Arc::new(Mutex::new(StreamState::new(log)));
        streams.insert(stream.to_owned(), Arc::clone(&stream_state));
        Ok(stream_state)
    }

    fn lock_stream<'a>(
        &self,
        stream: &'a Arc<Mutex<StreamState>>,
    ) -> Result<std::sync::MutexGuard<'a, StreamState>, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.stream_lock_wait");
        stream.lock().map_err(|_| BrokerError::LockPoisoned)
    }

    fn dead_letter_record(
        &self,
        source: &mut StreamState,
        stream: &str,
        record: &RecordIndex,
    ) -> Result<(), BrokerError> {
        let dead_letter_stream = dead_letter_stream_name(stream)?;
        let message = source.log.read_message(stream, record.offset)?;
        let target = self.get_or_create_stream(&dead_letter_stream)?;
        let mut target = self.lock_stream(&target)?;
        target.log.append(message.key, message.payload)?;
        Ok(())
    }

    fn next_delivery_token(&self) -> String {
        let next_id = self
            .inner
            .next_delivery_id
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        format!("{:x}-{:x}", self.inner.delivery_epoch, next_id)
    }
}

impl Engine for Broker {
    fn create_stream<'a>(&'a self, stream: &'a str) -> EngineFuture<'a, bool> {
        let broker = self.clone();
        let stream = stream.to_owned();
        Arc::clone(&self.inner.storage_executor)
            .dispatch_stream(stream, move |stream| broker.create_stream(stream))
    }

    fn publish<'a>(
        &'a self,
        stream: &'a str,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
    ) -> EngineFuture<'a, Offset> {
        let broker = self.clone();
        let stream = stream.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.publish_with_request_id(stream, key, payload, request_id)
        })
    }

    fn publish_batch<'a>(
        &'a self,
        stream: &'a str,
        records: Vec<PublishRecord>,
    ) -> EngineFuture<'a, Vec<PublishRecordOutcome>> {
        let broker = self.clone();
        let stream = stream.to_owned();
        Arc::clone(&self.inner.storage_executor)
            .dispatch_stream(stream, move |stream| broker.publish_batch(stream, records))
    }

    fn poll<'a>(&'a self, stream: &'a str, consumer: &'a str) -> EngineFuture<'a, PollResult> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        Arc::clone(&self.inner.storage_executor)
            .dispatch_stream(stream, move |stream| broker.poll(stream, &consumer))
    }

    fn poll_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
    ) -> EngineFuture<'a, PollResult> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        let member = member.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.poll_group(stream, &consumer, &member)
        })
    }

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        Arc::clone(&self.inner.storage_executor)
            .dispatch_stream(stream, move |stream| broker.ack(stream, &consumer, offset))
    }

    fn ack_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        offset: Offset,
        delivery_token: &'a str,
    ) -> EngineFuture<'a, AckResult> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        let member = member.to_owned();
        let delivery_token = delivery_token.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.ack_group(stream, &consumer, &member, offset, &delivery_token)
        })
    }

    fn health<'a>(&'a self) -> EngineFuture<'a, HealthSnapshot> {
        let broker = self.clone();
        Arc::clone(&self.inner.storage_executor).dispatch(move || broker.health())
    }
}

impl StreamLog {
    fn create(path: &Path, durable_format: DurableFormat) -> Result<Self, BrokerError> {
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

    fn open(path: &Path, durable_format: DurableFormat) -> Result<Self, BrokerError> {
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

    fn append(&mut self, key: Option<String>, payload: Vec<u8>) -> Result<Offset, BrokerError> {
        self.append_with_sync(key, payload, true)
    }

    fn append_with_sync(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        sync: bool,
    ) -> Result<Offset, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.storage_append");
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

    fn append_with_request_id(
        &mut self,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: String,
    ) -> Result<Offset, BrokerError> {
        self.append_with_request_id_sync(key, payload, request_id, true)
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

    fn append_batch(
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

    fn read_message(&mut self, stream: &str, offset: Offset) -> Result<Message, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.storage_read");
        let index = self.find_record(offset)?;
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

    fn find_candidate(
        &mut self,
        committed_offset: Offset,
        acknowledged_offsets: &BTreeSet<Offset>,
        in_flight: Option<&ConsumerInFlightIndex>,
    ) -> Result<Option<RecordIndex>, BrokerError> {
        let Some(first_indexed_offset) = self.records.front().map(|record| record.offset) else {
            return Ok(None);
        };
        if committed_offset >= first_indexed_offset {
            return Ok(self
                .records
                .iter()
                .filter(|record| record.offset >= committed_offset)
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

    fn find_record(&mut self, offset: Offset) -> Result<RecordIndex, BrokerError> {
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

    fn scan_start(&self, offset: Offset) -> u64 {
        self.sparse_index
            .iter()
            .rev()
            .find(|checkpoint| checkpoint.offset <= offset)
            .map_or(0, |checkpoint| checkpoint.cursor)
    }
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
            return Ok(None);
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
    if !crc32c_finalize(checksum).eq(&expected_checksum) {
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
    in_flight: Option<&ConsumerInFlightIndex>,
) -> bool {
    if acknowledged_offsets.contains(&record.offset)
        || in_flight.is_some_and(|index| index.offsets.contains(&record.offset))
    {
        return false;
    }
    record
        .key
        .as_ref()
        .is_none_or(|key| in_flight.is_none_or(|index| !index.keys.contains(key)))
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
    stream_state: &mut StreamState,
    root: &Path,
    stream: &str,
    consumer: &str,
) -> Result<ConsumerState, BrokerError> {
    if let Some(cached) = stream_state.consumer_states.get(consumer) {
        return Ok(cached.clone());
    }

    load_consumer_state(root, stream, consumer)
}

fn cache_consumer_state(stream_state: &mut StreamState, consumer: String, state: ConsumerState) {
    stream_state.consumer_states.insert(consumer, state);
    while stream_state.consumer_states.len() > MAX_CACHED_CONSUMER_STATES {
        let evicted = {
            let active_consumers: HashSet<&str> = stream_state
                .in_flight
                .keys()
                .map(|delivery_key| delivery_key.consumer.as_str())
                .collect();
            stream_state
                .consumer_states
                .keys()
                .find(|consumer| !active_consumers.contains((*consumer).as_str()))
                .cloned()
                .or_else(|| stream_state.consumer_states.keys().next().cloned())
        };
        let Some(evicted) = evicted else {
            break;
        };
        stream_state.consumer_states.remove(&evicted);
    }
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

fn delivery_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::{Arc, Barrier};
    use std::thread;
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
    fn acknowledged_consumer_state_cache_is_bounded() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.publish("events", None, b"payload".to_vec()).unwrap();

        for index in 0..=MAX_CACHED_CONSUMER_STATES {
            let consumer = format!("consumer-{index}");
            assert!(matches!(
                broker.poll("events", &consumer).unwrap(),
                PollResult::Message(Message { offset: 0, .. })
            ));
            assert_eq!(
                broker.ack("events", &consumer, 0).unwrap(),
                AckResult::Acknowledged
            );
        }

        let stream_state = broker.get_stream("events").unwrap();
        let stream_state = broker.lock_stream(&stream_state).unwrap();
        assert_eq!(
            stream_state.consumer_states.len(),
            MAX_CACHED_CONSUMER_STATES
        );
        let evicted_consumer = (0..=MAX_CACHED_CONSUMER_STATES)
            .map(|index| format!("consumer-{index}"))
            .find(|consumer| !stream_state.consumer_states.contains_key(consumer))
            .expect("one consumer should have been evicted");
        drop(stream_state);
        assert_eq!(
            broker.poll("events", &evicted_consumer).unwrap(),
            PollResult::Empty
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_runs_off_the_async_runtime_thread() {
        let runtime_thread = thread::current().id();
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let storage_thread = executor
            .dispatch(|| Ok(thread::current().id()))
            .await
            .unwrap();

        assert_ne!(storage_thread, runtime_thread);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_does_not_block_unrelated_async_work() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let storage_started = Arc::clone(&started);
        let storage = tokio::spawn(Arc::clone(&executor).dispatch(move || {
            storage_started.notify_one();
            release_receiver
                .recv()
                .expect("storage task should be released by the test");
            Ok(())
        }));

        started.notified().await;
        let unrelated = tokio::spawn(async { 42_u8 });
        assert_eq!(unrelated.await.unwrap(), 42);

        release_sender.send(()).unwrap();
        storage.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_waits_when_capacity_is_exhausted() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let storage_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch(move || {
            storage_started.notify_one();
            release_receiver
                .recv()
                .expect("storage task should be released by the test");
            Ok(())
        }));

        started.notified().await;
        let second = tokio::spawn(executor.dispatch(|| Ok(())));
        tokio::task::yield_now().await;
        assert!(!second.is_finished());

        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_rejects_work_beyond_the_bounded_queue() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let storage_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch(move || {
            storage_started.notify_one();
            release_receiver
                .recv()
                .expect("storage task should be released by the test");
            Ok(())
        }));

        started.notified().await;
        let second = tokio::spawn(Arc::clone(&executor).dispatch(|| Ok(())));
        while executor.admission_permits.available_permits() != 0 {
            tokio::task::yield_now().await;
        }

        let rejected = executor.dispatch(|| Ok(())).await;
        assert!(matches!(
            rejected,
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));

        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_cancellation_drops_queued_work() {
        let executor = Arc::new(StorageExecutor::with_capacities(1, 1));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let storage_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch(move || {
            storage_started.notify_one();
            release_receiver
                .recv()
                .expect("storage task should be released by the test");
            Ok(())
        }));

        started.notified().await;
        let executed = Arc::new(AtomicBool::new(false));
        let queued_executed = Arc::clone(&executed);
        let second = tokio::spawn(executor.clone().dispatch(move || {
            queued_executed.store(true, AtomicOrdering::Release);
            Ok(())
        }));
        while executor.admission_permits.available_permits() != 0 {
            tokio::task::yield_now().await;
        }

        second.abort();
        assert!(second.await.unwrap_err().is_cancelled());
        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();

        assert!(!executed.load(AtomicOrdering::Acquire));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn storage_dispatch_lanes_preserve_stream_order_without_starving_other_streams() {
        let executor = Arc::new(StorageExecutor::with_capacities(2, 2));
        let started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let order = Arc::new(Mutex::new(Vec::new()));
        let first_order = Arc::clone(&order);
        let first_started = Arc::clone(&started);
        let first = tokio::spawn(Arc::clone(&executor).dispatch_stream(
            "events".to_owned(),
            move |_| {
                first_order.lock().unwrap().push(1_u8);
                first_started.notify_one();
                release_receiver
                    .recv()
                    .expect("storage task should be released by the test");
                Ok(())
            },
        ));

        started.notified().await;
        let second_order = Arc::clone(&order);
        let second = tokio::spawn(Arc::clone(&executor).dispatch_stream(
            "events".to_owned(),
            move |_| {
                second_order.lock().unwrap().push(2_u8);
                Ok(())
            },
        ));
        while executor.admission_permits.available_permits() != 2 {
            tokio::task::yield_now().await;
        }

        assert_eq!(executor.execution_permits.available_permits(), 1);
        assert!(!second.is_finished());
        assert_eq!(
            executor
                .clone()
                .dispatch_stream("jobs".to_owned(), |_| Ok(42_u8))
                .await
                .unwrap(),
            42
        );

        release_sender.send(()).unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(*order.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_engine_storage_stall_does_not_block_unrelated_work() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.create_stream("events").unwrap();

        // Holding the stream's synchronous storage lock creates the same blocking point that a
        // slow filesystem operation would expose to the local engine. The Engine adapter must
        // wait for it on a blocking worker, leaving this current-thread runtime responsive.
        let stream = broker.get_stream("events").unwrap();
        let storage_started = Arc::new(tokio::sync::Notify::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let blocked_stream = Arc::clone(&stream);
        let storage_started_thread = Arc::clone(&storage_started);
        let blocker = thread::spawn(move || {
            let _stream_guard = blocked_stream.lock().unwrap();
            storage_started_thread.notify_one();
            release_receiver
                .recv()
                .expect("storage lock should be released by the test");
        });
        storage_started.notified().await;
        let executor = Arc::clone(&broker.inner.storage_executor);
        let publish = tokio::spawn({
            let broker = broker.clone();
            async move { Engine::publish(&broker, "events", None, b"payload".to_vec(), None).await }
        });
        while executor.execution_permits.available_permits() == DEFAULT_STORAGE_EXECUTOR_CAPACITY {
            tokio::task::yield_now().await;
        }

        let unrelated = tokio::spawn(async { 42_u8 });
        assert_eq!(unrelated.await.unwrap(), 42);
        release_sender.send(()).unwrap();

        assert_eq!(publish.await.unwrap().unwrap(), 0);
        blocker.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_engine_preserves_order_and_ack_recovery_under_concurrent_publishes() {
        const MESSAGE_COUNT: usize = 16;

        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let mut publishes = Vec::with_capacity(MESSAGE_COUNT);
        for message in 0..MESSAGE_COUNT {
            let broker = broker.clone();
            publishes.push(tokio::spawn(async move {
                let payload = format!("payload-{message}").into_bytes();
                let offset = Engine::publish(&broker, "events", None, payload.clone(), None)
                    .await
                    .unwrap();
                (offset, payload)
            }));
        }

        let mut published = Vec::with_capacity(MESSAGE_COUNT);
        for publish in publishes {
            published.push(publish.await.unwrap());
        }
        published.sort_unstable_by_key(|(offset, _)| *offset);
        for (expected_offset, (offset, _)) in published.iter().enumerate() {
            assert_eq!(*offset, expected_offset as Offset);
        }

        let first = match Engine::poll(&broker, "events", "worker").await.unwrap() {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected the first message"),
        };
        assert_eq!(first.offset, 0);
        assert_eq!(first.payload, published[0].1);
        drop(broker);

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let redelivered = match Engine::poll(&broker, "events", "worker").await.unwrap() {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected the unacknowledged message after restart"),
        };
        assert_eq!(redelivered.offset, 0);
        assert_eq!(redelivered.payload, published[0].1);
        assert_eq!(redelivered.delivery_attempt, Some(2));
        assert_eq!(
            Engine::ack(&broker, "events", "worker", redelivered.offset)
                .await
                .unwrap(),
            AckResult::Acknowledged
        );

        for (expected_offset, (_, payload)) in published.iter().enumerate().skip(1) {
            let message = match Engine::poll(&broker, "events", "worker").await.unwrap() {
                PollResult::Message(message) => message,
                PollResult::Empty => panic!("expected message at offset {expected_offset}"),
            };
            assert_eq!(message.offset, expected_offset as Offset);
            assert_eq!(message.payload, *payload);
            assert_eq!(
                Engine::ack(&broker, "events", "worker", message.offset)
                    .await
                    .unwrap(),
                AckResult::Acknowledged
            );
        }
        assert_eq!(
            Engine::poll(&broker, "events", "worker").await.unwrap(),
            PollResult::Empty
        );
    }

    #[tokio::test]
    async fn repeated_request_id_returns_original_offset_without_appending() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();

        let first = Engine::publish(
            &broker,
            "events",
            Some("original-key".to_owned()),
            b"original".to_vec(),
            Some("request-1".to_owned()),
        )
        .await
        .unwrap();
        let retry = Engine::publish(
            &broker,
            "events",
            Some("retry-key".to_owned()),
            b"retry-payload".to_vec(),
            Some("request-1".to_owned()),
        )
        .await
        .unwrap();

        assert_eq!((first, retry), (0, 0));
        assert!(matches!(
            broker.poll("events", "reader").unwrap(),
            PollResult::Message(Message {
                offset: 0,
                key: Some(key),
                payload,
                ..
            }) if key == "original-key" && payload == b"original"
        ));
        assert_eq!(
            broker.ack("events", "reader", 0).unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(broker.poll("events", "reader").unwrap(), PollResult::Empty);
    }

    #[tokio::test]
    async fn request_ids_are_scoped_per_stream() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();

        let events_offset = Engine::publish(
            &broker,
            "events",
            None,
            b"events".to_vec(),
            Some("same-request".to_owned()),
        )
        .await
        .unwrap();
        let audit_offset = Engine::publish(
            &broker,
            "audit",
            None,
            b"audit".to_vec(),
            Some("same-request".to_owned()),
        )
        .await
        .unwrap();

        assert_eq!((events_offset, audit_offset), (0, 0));
        assert_eq!(
            Engine::publish(
                &broker,
                "events",
                None,
                b"events-retry".to_vec(),
                Some("same-request".to_owned()),
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            Engine::publish(
                &broker,
                "audit",
                None,
                b"audit-retry".to_vec(),
                Some("same-request".to_owned()),
            )
            .await
            .unwrap(),
            0
        );
        assert!(matches!(
            broker.poll("events", "reader").unwrap(),
            PollResult::Message(Message { payload, .. }) if payload == b"events"
        ));
        assert!(matches!(
            broker.poll("audit", "reader").unwrap(),
            PollResult::Message(Message { payload, .. }) if payload == b"audit"
        ));
    }

    #[tokio::test]
    async fn request_id_deduplication_survives_restart() {
        let directory = tempdir().unwrap();
        let first = {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            Engine::publish(
                &broker,
                "events",
                Some("original-key".to_owned()),
                b"original".to_vec(),
                Some("request-1".to_owned()),
            )
            .await
            .unwrap()
        };
        assert_eq!(first, 0);

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(
            Engine::publish(
                &broker,
                "events",
                Some("retry-key".to_owned()),
                b"retry-payload".to_vec(),
                Some("request-1".to_owned()),
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            Engine::publish(&broker, "events", None, b"ordinary".to_vec(), None)
                .await
                .unwrap(),
            1
        );
        assert!(matches!(
            broker.poll("events", "reader").unwrap(),
            PollResult::Message(Message {
                offset: 0,
                payload,
                ..
            }) if payload == b"original"
        ));
        assert_eq!(
            broker.ack("events", "reader", 0).unwrap(),
            AckResult::Acknowledged
        );
        assert!(matches!(
            broker.poll("events", "reader").unwrap(),
            PollResult::Message(Message {
                offset: 1,
                payload,
                ..
            }) if payload == b"ordinary"
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_duplicate_requests_append_once() {
        const CALL_COUNT: usize = 32;

        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let mut calls = Vec::with_capacity(CALL_COUNT);
        for call in 0..CALL_COUNT {
            let broker = broker.clone();
            calls.push(tokio::spawn(async move {
                Engine::publish(
                    &broker,
                    "events",
                    Some(format!("key-{call}")),
                    format!("payload-{call}").into_bytes(),
                    Some("request-1".to_owned()),
                )
                .await
                .unwrap()
            }));
        }

        for call in calls {
            assert_eq!(call.await.unwrap(), 0);
        }
        assert!(matches!(
            broker.poll("events", "reader").unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));
        assert_eq!(
            broker.ack("events", "reader", 0).unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(broker.poll("events", "reader").unwrap(), PollResult::Empty);
    }

    #[tokio::test]
    async fn publishes_without_request_id_are_not_deduplicated() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();

        let first = Engine::publish(&broker, "events", None, b"same".to_vec(), None)
            .await
            .unwrap();
        let second = Engine::publish(&broker, "events", None, b"same".to_vec(), None)
            .await
            .unwrap();

        assert_eq!((first, second), (0, 1));
    }

    #[test]
    fn independent_streams_publish_concurrently_and_recover() {
        const STREAM_COUNT: usize = 4;
        const MESSAGES_PER_STREAM: usize = 32;

        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let start = Arc::new(Barrier::new(STREAM_COUNT));

        thread::scope(|scope| {
            for stream_index in 0..STREAM_COUNT {
                let broker = broker.clone();
                let start = Arc::clone(&start);
                scope.spawn(move || {
                    let stream = format!("stream-{stream_index}");
                    start.wait();
                    for message_index in 0..MESSAGES_PER_STREAM {
                        let offset = broker
                            .publish(
                                &stream,
                                Some(format!("key-{stream_index}")),
                                format!("payload-{message_index}").into_bytes(),
                            )
                            .unwrap();
                        assert_eq!(offset, message_index as Offset);
                    }
                });
            }
        });

        drop(broker);
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(broker.health().unwrap().streams, STREAM_COUNT);
        for stream_index in 0..STREAM_COUNT {
            let stream = format!("stream-{stream_index}");
            for message_index in 0..MESSAGES_PER_STREAM {
                let message = match broker.poll(&stream, "replayer").unwrap() {
                    PollResult::Message(message) => message,
                    PollResult::Empty => panic!("expected message {message_index} in {stream}"),
                };
                assert_eq!(message.offset, message_index as Offset);
                assert_eq!(
                    message.payload,
                    format!("payload-{message_index}").as_bytes()
                );
                assert_eq!(
                    broker.ack(&stream, "replayer", message.offset).unwrap(),
                    AckResult::Acknowledged
                );
            }
            assert_eq!(broker.poll(&stream, "replayer").unwrap(), PollResult::Empty);
        }
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
    fn grouped_dispatch_index_releases_keys_after_ack_and_expiry() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout: Duration::from_millis(100),
                max_delivery_attempts: None,
            },
        )
        .unwrap();
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
        let (other_offset, _other_token) = delivery(broker.poll_group("events", "workers", "b"));
        assert_eq!((first_offset, other_offset), (0, 2));

        assert_eq!(
            broker
                .ack_group("events", "workers", "a", first_offset, &first_token)
                .unwrap(),
            AckResult::Acknowledged
        );
        let (next_offset, next_token) = delivery(broker.poll_group("events", "workers", "c"));
        assert_eq!(next_offset, 1);
        assert_eq!(
            broker
                .ack_group("events", "workers", "c", next_offset, &next_token)
                .unwrap(),
            AckResult::Acknowledged
        );

        std::thread::sleep(Duration::from_millis(250));
        let (redelivered_offset, redelivered_token) =
            delivery(broker.poll_group("events", "workers", "replacement"));
        assert_eq!(redelivered_offset, other_offset);
        assert_eq!(
            broker
                .ack_group(
                    "events",
                    "workers",
                    "replacement",
                    redelivered_offset,
                    &redelivered_token,
                )
                .unwrap(),
            AckResult::Acknowledged
        );

        let stream_state = broker.get_stream("events").unwrap();
        let stream_state = broker.lock_stream(&stream_state).unwrap();
        assert!(stream_state.in_flight.is_empty());
        assert!(stream_state.in_flight_by_consumer.is_empty());
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
    fn retained_history_replays_beyond_the_bounded_index_after_restart() {
        let directory = tempdir().unwrap();
        let retained_message_count = MAX_IN_MEMORY_RECORDS as u64 + 8;
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            for offset in 0..retained_message_count {
                assert_eq!(
                    broker
                        .publish("events", None, format!("payload-{offset}").into_bytes())
                        .unwrap(),
                    offset
                );
            }

            let streams = broker.inner.streams.read().unwrap();
            let stream = streams.get("events").unwrap().clone();
            drop(streams);
            let stream = stream.lock().unwrap();
            let log = &stream.log;
            assert_eq!(log.records.len(), MAX_IN_MEMORY_RECORDS);
            assert_eq!(log.records.front().unwrap().offset, 8);
            assert_eq!(log.next_offset, retained_message_count);
        }

        let path = directory.path().join("streams/events.log");
        let complete_len = fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"RNL1partial").unwrap();
        file.sync_all().unwrap();

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), complete_len);
        let streams = broker.inner.streams.read().unwrap();
        let stream = streams.get("events").unwrap().clone();
        drop(streams);
        let stream = stream.lock().unwrap();
        assert_eq!(stream.log.sparse_index.len(), 17);
        assert_eq!(stream.log.sparse_index.front().unwrap().offset, 0);
        assert_eq!(stream.log.sparse_index.back().unwrap().offset, 1024);
        assert!(stream.log.scan_start(512) > 0);
        drop(stream);

        for offset in 0..retained_message_count {
            let message = match broker.poll("events", "replayer").unwrap() {
                PollResult::Message(message) => message,
                PollResult::Empty => panic!("expected retained offset {offset}"),
            };
            assert_eq!(message.offset, offset);
            assert_eq!(message.payload, format!("payload-{offset}").as_bytes());
            assert_eq!(
                broker.ack("events", "replayer", offset).unwrap(),
                AckResult::Acknowledged
            );
        }
        assert_eq!(
            broker.poll("events", "replayer").unwrap(),
            PollResult::Empty
        );
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

    #[tokio::test]
    async fn incomplete_request_id_frame_is_discarded_on_recovery() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            assert_eq!(
                Engine::publish(
                    &broker,
                    "events",
                    None,
                    b"complete".to_vec(),
                    Some("request-1".to_owned()),
                )
                .await
                .unwrap(),
                0
            );
        }

        let path = directory.path().join("streams/events.log");
        let complete_len = fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(REQUEST_ID_MAGIC).unwrap();
        file.sync_all().unwrap();

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), complete_len);
        assert_eq!(
            Engine::publish(
                &broker,
                "events",
                None,
                b"retry".to_vec(),
                Some("request-1".to_owned()),
            )
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn request_id_writer_rejects_oversized_fields_for_versioned_storage() {
        let directory = tempdir().unwrap();
        let broker = Broker::open_with_format(
            directory.path(),
            BrokerConfig::default(),
            DurableFormat::VersionedV1,
        )
        .unwrap();

        let key_error = Engine::publish(
            &broker,
            "events",
            Some("k".repeat(VERSIONED_MAX_KEY_LEN as usize + 1)),
            b"payload".to_vec(),
            Some("request-1".to_owned()),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(key_error, BrokerError::Io(error) if error.kind() == io::ErrorKind::InvalidInput)
        );

        let request_id_error = Engine::publish(
            &broker,
            "events",
            None,
            b"payload".to_vec(),
            Some("r".repeat(REQUEST_ID_MAX_LEN as usize + 1)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            request_id_error,
            BrokerError::Io(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }

    #[test]
    fn request_id_recovery_rejects_oversized_lengths_before_allocation() {
        for (key_len, request_id_len, body_len) in [
            (REQUEST_ID_MAX_KEY_LEN + 1, 0, 0),
            (0, REQUEST_ID_MAX_LEN + 1, 0),
            (0, 0, REQUEST_ID_MAX_BODY_LEN + 1),
        ] {
            let directory = tempdir().unwrap();
            {
                let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
                broker.create_stream("events").unwrap();
            }
            let path = directory.path().join("streams/events.log");
            let mut header = [0; REQUEST_ID_HEADER_LEN];
            header[..4].copy_from_slice(REQUEST_ID_MAGIC);
            header[4] = REQUEST_ID_FORMAT_VERSION;
            header[6..8].copy_from_slice(&(REQUEST_ID_HEADER_LEN as u16).to_le_bytes());
            header[8..12].copy_from_slice(&body_len.to_le_bytes());
            header[12..16].copy_from_slice(&body_len.to_le_bytes());
            header[32..36].copy_from_slice(&key_len.to_le_bytes());
            header[36..40].copy_from_slice(&request_id_len.to_le_bytes());
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&header).unwrap();
            file.sync_all().unwrap();

            let result = Broker::open(directory.path(), BrokerConfig::default());
            assert!(matches!(
                result,
                Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
            ));
        }
    }

    #[test]
    fn versioned_frames_round_trip_and_recover_after_restart() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open_with_format(
                directory.path(),
                BrokerConfig::default(),
                DurableFormat::VersionedV1,
            )
            .unwrap();
            assert_eq!(
                broker
                    .publish("events", Some("order-1".to_owned()), b"payload".to_vec())
                    .unwrap(),
                0
            );
        }

        let path = directory.path().join("streams/events.log");
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..4], VERSIONED_MAGIC);
        assert_eq!(
            u16::from_le_bytes(bytes[6..8].try_into().unwrap()) as usize,
            VERSIONED_HEADER_LEN
        );
        assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().unwrap()), 7);

        let broker = Broker::open_with_format(
            directory.path(),
            BrokerConfig::default(),
            DurableFormat::VersionedV1,
        )
        .unwrap();
        let message = match broker.poll("events", "reader").unwrap() {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected versioned message"),
        };
        assert_eq!(message.offset, 0);
        assert_eq!(message.key.as_deref(), Some("order-1"));
        assert_eq!(message.payload, b"payload");
        assert_eq!(
            broker.ack("events", "reader", 0).unwrap(),
            AckResult::Acknowledged
        );
    }

    #[test]
    fn versioned_reader_replays_mixed_legacy_and_versioned_frames() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            broker.publish("events", None, b"legacy".to_vec()).unwrap();
        }
        {
            let broker = Broker::open_with_format(
                directory.path(),
                BrokerConfig::default(),
                DurableFormat::VersionedV1,
            )
            .unwrap();
            broker
                .publish("events", None, b"versioned".to_vec())
                .unwrap();
        }

        let broker = Broker::open_with_format(
            directory.path(),
            BrokerConfig::default(),
            DurableFormat::VersionedV1,
        )
        .unwrap();
        for (offset, payload) in [(0, b"legacy".as_slice()), (1, b"versioned".as_slice())] {
            let message = match broker.poll("events", "reader").unwrap() {
                PollResult::Message(message) => message,
                PollResult::Empty => panic!("expected retained offset {offset}"),
            };
            assert_eq!(message.offset, offset);
            assert_eq!(message.payload, payload);
            assert_eq!(
                broker.ack("events", "reader", offset).unwrap(),
                AckResult::Acknowledged
            );
        }
        assert_eq!(broker.poll("events", "reader").unwrap(), PollResult::Empty);
    }

    #[test]
    fn versioned_checksum_corruption_fails_recovery() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open_with_format(
                directory.path(),
                BrokerConfig::default(),
                DurableFormat::VersionedV1,
            )
            .unwrap();
            broker.publish("events", None, b"payload".to_vec()).unwrap();
        }
        let path = directory.path().join("streams/events.log");
        let mut bytes = fs::read(&path).unwrap();
        bytes[VERSIONED_HEADER_LEN] ^= 1;
        fs::write(&path, bytes).unwrap();

        let result = Broker::open_with_format(
            directory.path(),
            BrokerConfig::default(),
            DurableFormat::VersionedV1,
        );
        assert!(matches!(
            result,
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn incomplete_versioned_frame_is_discarded_on_recovery() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open_with_format(
                directory.path(),
                BrokerConfig::default(),
                DurableFormat::VersionedV1,
            )
            .unwrap();
            broker
                .publish("events", None, b"complete".to_vec())
                .unwrap();
        }
        let path = directory.path().join("streams/events.log");
        let complete_len = fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(VERSIONED_MAGIC).unwrap();
        file.write_all(&[VERSIONED_FORMAT_VERSION]).unwrap();
        file.sync_all().unwrap();

        let broker = Broker::open_with_format(
            directory.path(),
            BrokerConfig::default(),
            DurableFormat::VersionedV1,
        )
        .unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), complete_len);
        assert!(matches!(
            broker.poll("events", "reader").unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));
    }

    #[test]
    fn versioned_storage_rejects_oversized_records_before_allocation() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open_with_format(
                directory.path(),
                BrokerConfig::default(),
                DurableFormat::VersionedV1,
            )
            .unwrap();
            broker.create_stream("events").unwrap();
        }
        let path = directory.path().join("streams/events.log");
        let mut header = [0; VERSIONED_HEADER_LEN];
        header[..4].copy_from_slice(VERSIONED_MAGIC);
        header[4] = VERSIONED_FORMAT_VERSION;
        header[6..8].copy_from_slice(&(VERSIONED_HEADER_LEN as u16).to_le_bytes());
        header[8..12].copy_from_slice(&(VERSIONED_MAX_BODY_LEN + 1).to_le_bytes());
        header[12..16].copy_from_slice(&(VERSIONED_MAX_BODY_LEN + 1).to_le_bytes());
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&header).unwrap();
        file.sync_all().unwrap();

        let result = Broker::open_with_format(
            directory.path(),
            BrokerConfig::default(),
            DurableFormat::VersionedV1,
        );
        assert!(matches!(
            result,
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn versioned_writer_rejects_oversized_payloads_and_keys() {
        let directory = tempdir().unwrap();
        let broker = Broker::open_with_format(
            directory.path(),
            BrokerConfig::default(),
            DurableFormat::VersionedV1,
        )
        .unwrap();

        let key_error = broker
            .publish(
                "events",
                Some("k".repeat(VERSIONED_MAX_KEY_LEN as usize + 1)),
                b"payload".to_vec(),
            )
            .unwrap_err();
        assert!(
            matches!(key_error, BrokerError::Io(error) if error.kind() == io::ErrorKind::InvalidInput)
        );

        let payload_error = broker
            .publish("events", None, vec![0; VERSIONED_MAX_BODY_LEN as usize + 1])
            .unwrap_err();
        assert!(
            matches!(payload_error, BrokerError::Io(error) if error.kind() == io::ErrorKind::InvalidInput)
        );
    }
}
