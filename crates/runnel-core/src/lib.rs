use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::sync::atomic::AtomicBool;

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{
    Engine, EngineFuture, MAX_PUBLISH_BATCH_RECORDS, PublishRecord, PublishRecordOutcome,
};

mod consumer_state;
mod storage;
mod stream_log;

#[cfg(test)]
use consumer_state::MAX_CONSUMER_STATE_JOURNAL_BYTES;
use consumer_state::{
    ConsumerState, ConsumerStateEvent, load_consumer_state, persist_consumer_event,
};
#[cfg(test)]
use std::fs::OpenOptions;
#[cfg(test)]
use std::io::Write;
use storage::StorageExecutor;
#[cfg(test)]
use stream_log::{
    LEGACY_HEADER_LEN, LEGACY_MAGIC, MAX_IN_MEMORY_RECORDS, REQUEST_ID_FORMAT_VERSION,
    REQUEST_ID_HEADER_LEN, REQUEST_ID_MAGIC, REQUEST_ID_MAX_BODY_LEN, REQUEST_ID_MAX_KEY_LEN,
    VERSIONED_FORMAT_VERSION, VERSIONED_HEADER_LEN, VERSIONED_MAGIC, VERSIONED_MAX_BODY_LEN,
    VERSIONED_MAX_KEY_LEN,
};
use stream_log::{REQUEST_ID_MAX_LEN, RecordIndex, StreamLog};
const MAX_CACHED_CONSUMER_STATES: usize = 1024;
const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const DEAD_LETTER_SUFFIX: &str = ".dead-letter";

pub use runnel_engine::{
    AckResult, BrokerError, HealthSnapshot, Message, Offset, PollResult, ReplayMessage,
};

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
    #[cfg(test)]
    fail_next_dead_letter_ack_persist: AtomicBool,
}

struct StreamState {
    log: StreamLog,
    // Consumer state and in-flight deliveries are owned by the stream so independent streams
    // do not contend on a process-wide broker lock. The bounded cache is only a best-effort
    // fast path; the durable checkpoint remains the source of truth across restart and eviction.
    consumer_states: HashMap<String, ConsumerState>,
    in_flight: HashMap<DeliveryKey, InFlight>,
    // This index keeps expiry checks proportional to deliveries that are due instead of scanning
    // every outstanding delivery on each poll. Entries are removed when a delivery is acked.
    in_flight_deadlines: BTreeMap<Instant, Vec<DeliveryKey>>,
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

impl StreamState {
    fn new(log: StreamLog) -> Self {
        Self {
            log,
            consumer_states: HashMap::new(),
            in_flight: HashMap::new(),
            in_flight_deadlines: BTreeMap::new(),
            in_flight_by_consumer: HashMap::new(),
        }
    }

    fn expire_in_flight(&mut self, now: Instant) {
        while let Some((&deadline, _)) = self.in_flight_deadlines.first_key_value() {
            if deadline > now {
                break;
            }

            let Some((_, delivery_keys)) = self.in_flight_deadlines.pop_first() else {
                break;
            };
            for delivery_key in delivery_keys {
                if self
                    .in_flight
                    .get(&delivery_key)
                    .is_some_and(|in_flight| in_flight.deadline <= now)
                {
                    self.remove_in_flight(&delivery_key);
                }
            }
        }
    }

    fn insert_in_flight(&mut self, delivery_key: DeliveryKey, in_flight: InFlight) {
        let consumer = delivery_key.consumer.clone();
        let offset = delivery_key.offset;
        let member = in_flight.member.clone();
        let key = in_flight.key.clone();
        debug_assert!(!self.in_flight.contains_key(&delivery_key));
        let deadline = in_flight.deadline;
        self.in_flight.insert(delivery_key.clone(), in_flight);
        self.in_flight_deadlines
            .entry(deadline)
            .or_default()
            .push(delivery_key);

        let index = self.in_flight_by_consumer.entry(consumer).or_default();
        index.offsets.insert(offset);
        if let Some(key) = key {
            index.keys.insert(key);
        }
        index.members.insert(member, offset);
    }

    fn remove_in_flight(&mut self, delivery_key: &DeliveryKey) -> Option<InFlight> {
        let in_flight = self.in_flight.remove(delivery_key)?;
        let mut remove_deadline_index = false;
        if let Some(deliveries) = self.in_flight_deadlines.get_mut(&in_flight.deadline) {
            if let Some(index) = deliveries.iter().position(|key| key == delivery_key) {
                deliveries.swap_remove(index);
            }
            remove_deadline_index = deliveries.is_empty();
        }
        if remove_deadline_index {
            self.in_flight_deadlines.remove(&in_flight.deadline);
        }
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
        let in_flight = in_flight_by_consumer
            .get(consumer)
            .map(|index| (&index.offsets, &index.keys));
        log.find_candidate(committed_offset, acknowledged_offsets, in_flight)
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
                #[cfg(test)]
                fail_next_dead_letter_ack_persist: AtomicBool::new(false),
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
            && let Some(offset) = stream_state.log.request_offset(request_id)
        {
            // As with the clustered engine, a repeated identity resolves to its original
            // offset; payload and key mismatches are intentionally ignored for compatibility.
            return Ok(offset);
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

    /// Read one retained record without creating delivery state or changing
    /// the ordinary consumer checkpoint.
    pub fn replay(
        &self,
        stream: &str,
        consumer: &str,
        offset: Offset,
    ) -> Result<ReplayMessage, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.replay");
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        let stream_state = self.get_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        stream_state.log.read_replay_message(stream, offset)
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
                self.dead_letter_record(&mut stream_state, stream, consumer, &candidate)?;
                self.persist_dead_letter_ack(
                    &root,
                    stream,
                    consumer,
                    &consumer_state,
                    candidate.offset,
                )?;
                consumer_state.acknowledge(candidate.offset);
                cache_consumer_state(
                    &mut stream_state,
                    consumer.to_owned(),
                    consumer_state.clone(),
                );
                self.inner.dead_letters.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let candidate_offset = candidate.offset;
            let delivery_attempt = attempts.saturating_add(1);
            let mut message = stream_state.log.read_message(stream, candidate.offset)?;
            persist_consumer_event(
                &root,
                stream,
                consumer,
                &consumer_state,
                ConsumerStateEvent::DeliveryAttempt {
                    offset: candidate.offset,
                    attempt: delivery_attempt,
                },
            )?;
            consumer_state
                .delivery_attempts
                .insert(candidate.offset, delivery_attempt);
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
                    offset: candidate_offset,
                    key: candidate.into_key(),
                    delivery_attempt,
                    delivery_token,
                    deadline: Instant::now() + ack_timeout,
                },
            );
            cache_consumer_state(&mut stream_state, consumer.to_owned(), consumer_state);
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
        let mut consumer_state =
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

        persist_consumer_event(
            &root,
            stream,
            consumer,
            &consumer_state,
            ConsumerStateEvent::Acknowledge { offset },
        )?;
        consumer_state.acknowledge(offset);
        consumer_state.stream = stream.to_owned();
        consumer_state.consumer = consumer.to_owned();
        stream_state.remove_in_flight(&delivery_key);
        cache_consumer_state(&mut stream_state, consumer.to_owned(), consumer_state);
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
        let mut in_flight_deliveries = 0;
        for stream in &streams {
            let stream = self.lock_stream(stream)?;
            storage_bytes += stream.log.storage_bytes()?;
            in_flight_deliveries += stream.in_flight.len() as u64;
        }
        Ok(HealthSnapshot {
            streams: streams.len(),
            storage_bytes,
            in_flight_deliveries,
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
        consumer: &str,
        record: &RecordIndex,
    ) -> Result<(), BrokerError> {
        let dead_letter_stream = dead_letter_stream_name(stream)?;
        let move_id = dead_letter_move_id(stream, consumer, record.offset)?;
        let message = source.log.read_message(stream, record.offset)?;
        let target = self.get_or_create_stream(&dead_letter_stream)?;
        let mut target = self.lock_stream(&target)?;
        target
            .log
            .append_with_move_id(message.key, message.payload, move_id)?;
        Ok(())
    }

    fn persist_dead_letter_ack(
        &self,
        root: &Path,
        stream: &str,
        consumer: &str,
        current_state: &ConsumerState,
        offset: Offset,
    ) -> Result<(), BrokerError> {
        #[cfg(test)]
        if self
            .inner
            .fail_next_dead_letter_ack_persist
            .swap(false, Ordering::AcqRel)
        {
            return Err(BrokerError::Io(io::Error::new(
                io::ErrorKind::Interrupted,
                "injected dead-letter acknowledgement persistence failure",
            )));
        }

        persist_consumer_event(
            root,
            stream,
            consumer,
            current_state,
            ConsumerStateEvent::Acknowledge { offset },
        )
    }

    #[cfg(test)]
    fn fail_next_dead_letter_ack_persist(&self) {
        self.inner
            .fail_next_dead_letter_ack_persist
            .store(true, Ordering::Release);
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

    fn replay<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, ReplayMessage> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.replay(stream, &consumer, offset)
        })
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

fn dead_letter_move_id(
    source_stream: &str,
    source_consumer: &str,
    source_offset: Offset,
) -> Result<String, BrokerError> {
    // Length-prefix the validated names so the internal identity remains unambiguous without
    // becoming a path or changing the public request-ID contract.
    let move_id = format!(
        "runnel-dlq/v1/{}:{source_stream}/{}:{source_consumer}/{source_offset}",
        source_stream.len(),
        source_consumer.len(),
    );
    if move_id.len() <= REQUEST_ID_MAX_LEN as usize {
        return Ok(move_id);
    }
    Err(BrokerError::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        "dead-letter move identity exceeds storage limit",
    )))
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

fn delivery_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
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
        while !executor.execution_permit_is_consumed() {
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
    fn health_reports_in_flight_deliveries_until_acknowledged() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.publish("events", None, b"payload".to_vec()).unwrap();

        assert_eq!(broker.health().unwrap().in_flight_deliveries, 0);
        let message = match broker.poll("events", "worker").unwrap() {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected a message"),
        };
        assert_eq!(broker.health().unwrap().in_flight_deliveries, 1);

        assert_eq!(
            broker.ack("events", "worker", message.offset).unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(broker.health().unwrap().in_flight_deliveries, 0);
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
        assert!(stream_state.in_flight_deadlines.is_empty());
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
                in_flight_deliveries: 0,
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

    #[test]
    fn dead_letter_move_identity_is_stable_scoped_and_bounded() {
        let first = dead_letter_move_id("events", "worker", 7).unwrap();
        assert_eq!(first, dead_letter_move_id("events", "worker", 7).unwrap());
        assert_ne!(first, dead_letter_move_id("events", "other", 7).unwrap());
        assert_ne!(first, dead_letter_move_id("audit", "worker", 7).unwrap());
        assert_ne!(first, dead_letter_move_id("events", "worker", 8).unwrap());
        assert!(first.len() <= REQUEST_ID_MAX_LEN as usize);
    }

    #[test]
    fn dead_letter_retry_reuses_move_identity_without_appending() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout: Duration::ZERO,
                max_delivery_attempts: Some(1),
            },
        )
        .unwrap();
        broker
            .publish("events", Some("order-1".to_owned()), b"poison".to_vec())
            .unwrap();
        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));

        let source = broker.get_stream("events").unwrap();
        let mut source = broker.lock_stream(&source).unwrap();
        let candidate = source.log.find_record(0).unwrap();
        broker
            .dead_letter_record(&mut source, "events", "worker", &candidate)
            .unwrap();
        broker
            .dead_letter_record(&mut source, "events", "worker", &candidate)
            .unwrap();
        drop(source);

        let target = broker.get_stream("events.dead-letter").unwrap();
        let target = broker.lock_stream(&target).unwrap();
        assert_eq!(target.log.next_offset(), 1);
        assert_eq!(target.log.request_id_count(), 1);
    }

    #[test]
    fn dead_letter_move_reconciles_target_after_restart() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: Some(1),
        };
        {
            let broker = Broker::open(directory.path(), config.clone()).unwrap();
            broker
                .publish("events", Some("order-1".to_owned()), b"poison".to_vec())
                .unwrap();
            assert!(matches!(
                broker.poll("events", "worker").unwrap(),
                PollResult::Message(Message { offset: 0, .. })
            ));

            let source = broker.get_stream("events").unwrap();
            let mut source = broker.lock_stream(&source).unwrap();
            let candidate = source.log.find_record(0).unwrap();
            broker
                .dead_letter_record(&mut source, "events", "worker", &candidate)
                .unwrap();
        }

        let broker = Broker::open(directory.path(), config).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        let dead_letter = match broker.poll("events.dead-letter", "inspector").unwrap() {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected the reconciled dead-letter record"),
        };
        assert_eq!(dead_letter.offset, 0);
        assert_eq!(dead_letter.key.as_deref(), Some("order-1"));
        assert_eq!(dead_letter.payload, b"poison");
        assert_eq!(
            broker
                .ack("events.dead-letter", "inspector", dead_letter.offset)
                .unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(
            broker.poll("events.dead-letter", "inspector").unwrap(),
            PollResult::Empty
        );
    }

    #[test]
    fn dead_letter_move_reconciles_after_source_ack_persistence_failure_and_restart() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: Some(1),
        };
        let move_id = dead_letter_move_id("events", "worker", 0).unwrap();

        {
            let broker = Broker::open(directory.path(), config.clone()).unwrap();
            broker
                .publish("events", Some("order-1".to_owned()), b"poison".to_vec())
                .unwrap();
            assert!(matches!(
                broker.poll("events", "worker").unwrap(),
                PollResult::Message(Message {
                    offset: 0,
                    delivery_attempt: Some(1),
                    ..
                })
            ));

            broker.fail_next_dead_letter_ack_persist();
            let error = broker.poll("events", "worker").unwrap_err();
            assert!(matches!(
                error,
                BrokerError::Io(error) if error.kind() == io::ErrorKind::Interrupted
            ));

            let source_state = load_consumer_state(directory.path(), "events", "worker").unwrap();
            assert_eq!(source_state.committed_offset, 0);
            assert_eq!(source_state.delivery_attempts.get(&0), Some(&1));

            let target = broker.get_stream("events.dead-letter").unwrap();
            let target = broker.lock_stream(&target).unwrap();
            assert_eq!(target.log.next_offset(), 1);
            assert_eq!(target.log.request_offset(&move_id), Some(0));
        }

        {
            let broker = Broker::open(directory.path(), config.clone()).unwrap();
            assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);

            let source_state = load_consumer_state(directory.path(), "events", "worker").unwrap();
            assert_eq!(source_state.committed_offset, 1);
            assert!(source_state.delivery_attempts.is_empty());

            let target = broker.get_stream("events.dead-letter").unwrap();
            let target = broker.lock_stream(&target).unwrap();
            assert_eq!(target.log.next_offset(), 1);
            assert_eq!(target.log.request_offset(&move_id), Some(0));
        }

        let broker = Broker::open(directory.path(), config).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        let target = broker.get_stream("events.dead-letter").unwrap();
        let target = broker.lock_stream(&target).unwrap();
        assert_eq!(target.log.next_offset(), 1);
        assert_eq!(target.log.request_offset(&move_id), Some(0));
    }

    #[test]
    fn dead_letter_move_content_mismatch_is_storage_error_without_acknowledgement() {
        for (key, payload) in [
            (Some("wrong-key".to_owned()), b"poison".to_vec()),
            (Some("order-1".to_owned()), b"wrong-payload".to_vec()),
        ] {
            let directory = tempdir().unwrap();
            let config = BrokerConfig {
                ack_timeout: Duration::ZERO,
                max_delivery_attempts: Some(1),
            };
            let broker = Broker::open(directory.path(), config.clone()).unwrap();
            broker
                .publish("events", Some("order-1".to_owned()), b"poison".to_vec())
                .unwrap();
            assert!(matches!(
                broker.poll("events", "worker").unwrap(),
                PollResult::Message(Message { offset: 0, .. })
            ));

            let move_id = dead_letter_move_id("events", "worker", 0).unwrap();
            broker
                .publish_with_request_id("events.dead-letter", key, payload, Some(move_id))
                .unwrap();
            let error = broker.poll("events", "worker").unwrap_err();
            assert!(matches!(
                error,
                BrokerError::Io(error) if error.kind() == io::ErrorKind::InvalidData
            ));

            drop(broker);
            let broker = Broker::open(directory.path(), config).unwrap();
            assert!(matches!(
                broker.poll("events", "worker"),
                Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
            ));
        }
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
    fn consumer_delivery_journal_recovers_committed_events_and_discards_partial_tail() {
        let directory = tempdir().unwrap();
        let journal_path = directory.path().join("consumers/events/worker.json.tmp");
        let first_journal_len;
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            broker
                .publish("events", Some("order-1".to_owned()), b"payload".to_vec())
                .unwrap();
            assert!(matches!(
                broker.poll("events", "worker").unwrap(),
                PollResult::Message(Message {
                    offset: 0,
                    delivery_attempt: Some(1),
                    ..
                })
            ));
            first_journal_len = fs::metadata(&journal_path).unwrap().len();
            assert!(first_journal_len > 0);

            let mut journal = OpenOptions::new().append(true).open(&journal_path).unwrap();
            journal.write_all(b"{\"delivery\"").unwrap();
            journal.sync_all().unwrap();
        }

        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            assert!(matches!(
                broker.poll("events", "worker").unwrap(),
                PollResult::Message(Message {
                    offset: 0,
                    delivery_attempt: Some(2),
                    ..
                })
            ));
            assert_eq!(
                fs::metadata(&journal_path).unwrap().len(),
                first_journal_len * 2
            );
            assert_eq!(
                broker.ack("events", "worker", 0).unwrap(),
                AckResult::Acknowledged
            );
        }

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
    }

    #[test]
    fn consumer_delivery_journal_stays_within_its_checkpoint_bound() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout: Duration::ZERO,
                max_delivery_attempts: None,
            },
        )
        .unwrap();
        broker.publish("events", None, b"payload".to_vec()).unwrap();

        for expected_attempt in 1..=2_000 {
            let message = match broker.poll("events", "worker").unwrap() {
                PollResult::Message(message) => message,
                PollResult::Empty => panic!("expected delivery attempt {expected_attempt}"),
            };
            assert_eq!(message.delivery_attempt, Some(expected_attempt));
        }

        let journal_path = directory.path().join("consumers/events/worker.json.tmp");
        assert!(fs::metadata(journal_path).unwrap().len() <= MAX_CONSUMER_STATE_JOURNAL_BYTES);

        drop(broker);
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout: Duration::ZERO,
                max_delivery_attempts: None,
            },
        )
        .unwrap();
        let message = match broker.poll("events", "worker").unwrap() {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected delivery after journal compaction recovery"),
        };
        assert_eq!(message.delivery_attempt, Some(2_001));
    }

    #[test]
    fn oversized_consumer_delivery_journal_is_rejected_on_recovery() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.publish("events", None, b"payload".to_vec()).unwrap();
        drop(broker);

        let journal_path = directory.path().join("consumers/events/worker.json.tmp");
        fs::create_dir_all(journal_path.parent().unwrap()).unwrap();
        fs::write(
            &journal_path,
            vec![b'x'; MAX_CONSUMER_STATE_JOURNAL_BYTES as usize + 1],
        )
        .unwrap();

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert!(matches!(
            broker.poll("events", "worker"),
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
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
            assert_eq!(log.in_memory_record_count(), MAX_IN_MEMORY_RECORDS);
            assert_eq!(log.first_in_memory_offset(), Some(8));
            assert_eq!(log.next_offset(), retained_message_count);
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
        assert_eq!(stream.log.sparse_index_len(), 17);
        assert_eq!(stream.log.first_sparse_offset(), Some(0));
        assert_eq!(stream.log.last_sparse_offset(), Some(1024));
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

    #[test]
    fn complete_legacy_record_with_malformed_key_fails_closed_on_recovery() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            broker
                .publish("events", Some("complete".to_owned()), b"payload".to_vec())
                .unwrap();
        }

        let path = directory.path().join("streams/events.log");
        let valid_len = fs::metadata(&path).unwrap().len();
        let key = [0xff];
        let payload = b"malformed-key-record";
        let mut header = [0; LEGACY_HEADER_LEN];
        header[..4].copy_from_slice(LEGACY_MAGIC);
        header[4..12].copy_from_slice(&1_u64.to_le_bytes());
        header[20..24].copy_from_slice(&(key.len() as u32).to_le_bytes());
        header[24..28].copy_from_slice(&(payload.len() as u32).to_le_bytes());

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&header).unwrap();
        file.write_all(&key).unwrap();
        file.write_all(payload).unwrap();
        file.sync_all().unwrap();
        let complete_len =
            valid_len + LEGACY_HEADER_LEN as u64 + key.len() as u64 + payload.len() as u64;

        let error = match Broker::open(directory.path(), BrokerConfig::default()) {
            Err(BrokerError::Io(error)) => error,
            Err(error) => panic!("expected malformed legacy key to fail recovery, got {error}"),
            Ok(_) => panic!("expected malformed legacy key to fail recovery"),
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "legacy record key is not UTF-8");
        assert_eq!(fs::metadata(&path).unwrap().len(), complete_len);
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
