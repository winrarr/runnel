use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{
    BrokerError, MAX_PUBLISH_BATCH_RECORDS, Offset, PollResult, PublishRecord,
    PublishRecordOutcome, ReplayMessage,
};
#[cfg(test)]
use std::io;
#[cfg(test)]
use std::sync::atomic::AtomicBool;

use super::consumer_state::{ConsumerState, ConsumerStateEvent, persist_consumer_event};
use super::delivery_state::{DeliveryState, DeliveryTokenGenerator, InFlight};
use super::storage::StorageExecutor;
use super::stream_log::{RecordIndex, StreamLog};
use super::{
    AckResult, BrokerConfig, DurableFormat, HealthSnapshot, dead_letter_move_id,
    dead_letter_stream_name, is_dead_letter_stream, stream_path, validate_name,
};

#[derive(Clone)]
pub struct Broker {
    pub(super) inner: Arc<BrokerState>,
}

pub(super) struct BrokerState {
    pub(super) root: PathBuf,
    pub(super) durable_format: DurableFormat,
    pub(super) streams: RwLock<HashMap<String, Arc<Mutex<StreamState>>>>,
    pub(super) ack_timeout: Duration,
    pub(super) max_delivery_attempts: Option<u32>,
    pub(super) redeliveries: AtomicU64,
    pub(super) dead_letters: AtomicU64,
    pub(super) delivery_tokens: DeliveryTokenGenerator,
    pub(super) storage_executor: Arc<StorageExecutor>,
    #[cfg(test)]
    pub(super) fail_next_dead_letter_ack_persist: AtomicBool,
}

pub(super) struct StreamState {
    pub(super) log: StreamLog,
    pub(super) delivery: DeliveryState,
}

impl StreamState {
    fn new(log: StreamLog) -> Self {
        Self {
            log,
            delivery: DeliveryState::new(),
        }
    }

    fn find_candidate(
        &mut self,
        consumer: &str,
        committed_offset: Offset,
        acknowledged_offsets: &BTreeSet<Offset>,
    ) -> Result<Option<RecordIndex>, BrokerError> {
        let in_flight = self.delivery.in_flight_filter(consumer);
        self.log
            .find_candidate(committed_offset, acknowledged_offsets, in_flight)
    }
}

impl BrokerState {
    fn open(
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
            root,
            durable_format,
            streams: RwLock::new(streams),
            ack_timeout: config.ack_timeout,
            max_delivery_attempts: config.max_delivery_attempts,
            redeliveries: AtomicU64::new(0),
            dead_letters: AtomicU64::new(0),
            delivery_tokens: DeliveryTokenGenerator::new(),
            storage_executor: Arc::new(StorageExecutor::new()),
            #[cfg(test)]
            fail_next_dead_letter_ack_persist: AtomicBool::new(false),
        })
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
        Ok(Self {
            inner: Arc::new(BrokerState::open(root, config, durable_format)?),
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

    pub(super) fn publish_with_request_id(
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
        stream_state.delivery.expire(now);

        if let Some(in_flight) = stream_state.delivery.member_delivery(consumer, member)? {
            let mut message = stream_state.log.read_message(stream, in_flight.offset())?;
            message.delivery_token = Some(in_flight.delivery_token().to_owned());
            message.delivery_attempt = Some(in_flight.delivery_attempt());
            return Ok(PollResult::Message(message));
        }

        let mut consumer_state = stream_state
            .delivery
            .load_consumer_state_for_request(&root, stream, consumer)?;
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
                && !is_dead_letter_stream(stream)
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
                stream_state
                    .delivery
                    .cache_consumer_state(consumer.to_owned(), consumer_state.clone());
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
            let delivery_token = self.inner.delivery_tokens.next();
            message.delivery_token = Some(delivery_token.clone());
            message.delivery_attempt = Some(delivery_attempt);
            if delivery_attempt > 1 {
                self.inner.redeliveries.fetch_add(1, Ordering::Relaxed);
            }
            let ack_timeout = self.inner.ack_timeout;
            stream_state.delivery.insert(
                consumer,
                InFlight::new(
                    member,
                    candidate_offset,
                    candidate.into_key(),
                    delivery_attempt,
                    delivery_token,
                    Instant::now() + ack_timeout,
                ),
            );
            stream_state
                .delivery
                .cache_consumer_state(consumer.to_owned(), consumer_state);
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
        let mut consumer_state = stream_state
            .delivery
            .load_consumer_state_for_request(&root, stream, consumer)?;
        if offset < consumer_state.committed_offset {
            return Ok(AckResult::AlreadyAcknowledged);
        }
        if consumer_state.acknowledged_offsets.contains(&offset) {
            return Ok(AckResult::AlreadyAcknowledged);
        }
        let Some(in_flight) = stream_state.delivery.get_in_flight(consumer, offset) else {
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
        if in_flight.member() != member
            || (!delivery_token.is_empty() && in_flight.delivery_token() != delivery_token)
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
        stream_state.delivery.remove(consumer, offset);
        stream_state
            .delivery
            .cache_consumer_state(consumer.to_owned(), consumer_state);
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
            in_flight_deliveries += stream.delivery.in_flight_count() as u64;
        }
        Ok(HealthSnapshot {
            streams: streams.len(),
            storage_bytes,
            in_flight_deliveries,
            redeliveries: self.inner.redeliveries.load(Ordering::Relaxed),
            dead_letters: self.inner.dead_letters.load(Ordering::Relaxed),
        })
    }

    pub(super) fn get_stream(&self, stream: &str) -> Result<Arc<Mutex<StreamState>>, BrokerError> {
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

    pub(super) fn lock_stream<'a>(
        &self,
        stream: &'a Arc<Mutex<StreamState>>,
    ) -> Result<std::sync::MutexGuard<'a, StreamState>, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("core.stream_lock_wait");
        stream.lock().map_err(|_| BrokerError::LockPoisoned)
    }

    pub(super) fn dead_letter_record(
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
    pub(super) fn fail_next_dead_letter_ack_persist(&self) {
        self.inner
            .fail_next_dead_letter_ack_persist
            .store(true, Ordering::Release);
    }
}
