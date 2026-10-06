use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{
    AckBatchItem, AckBatchOutcome, AckBatchRejection, AckBatchResult, BrokerError,
    ConsumeBatchLimits, ConsumerPolicy, DeliveryReceipt, MAX_PUBLISH_BATCH_RECORDS, Message,
    Offset, PollBatchResponseSizer, PollResult, PublishRecord, PublishRecordOutcome, ReplayMessage,
    poll_batch_response_len, validate_ack_batch_receipts, validate_consume_batch_limits,
    validate_consumer_policy,
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
    AckResult, BrokerConfig, DEAD_LETTER_HASH_PREFIX, DEAD_LETTER_SUFFIX, DurableFormat,
    HealthSnapshot, dead_letter_move_id, dead_letter_stream_name, stream_path, validate_name,
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
    #[cfg(test)]
    pub(super) fail_next_dead_letter_ack_sync: AtomicBool,
    #[cfg(test)]
    pub(super) fail_next_consumer_batch_event_sync: AtomicBool,
    #[cfg(test)]
    pub(super) fail_next_consumer_batch_event_before_append: AtomicBool,
    #[cfg(test)]
    pub(super) fail_next_consumer_batch_event_partial_append: AtomicBool,
}

pub(super) struct StreamState {
    pub(super) log: StreamLog,
    pub(super) delivery: DeliveryState,
    pub(super) availability: Arc<Notify>,
}

impl StreamState {
    fn new(log: StreamLog) -> Self {
        Self {
            log,
            delivery: DeliveryState::new(),
            availability: Arc::new(Notify::new()),
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

    fn find_candidate_excluding(
        &mut self,
        consumer: &str,
        committed_offset: Offset,
        acknowledged_offsets: &BTreeSet<Offset>,
        extra_offsets: &HashSet<Offset>,
        extra_keys: &HashSet<String>,
    ) -> Result<Option<RecordIndex>, BrokerError> {
        let Some((current_offsets, current_keys)) = self.delivery.in_flight_filter(consumer) else {
            if extra_offsets.is_empty() && extra_keys.is_empty() {
                return self
                    .log
                    .find_candidate(committed_offset, acknowledged_offsets, None);
            }
            return self.log.find_candidate(
                committed_offset,
                acknowledged_offsets,
                Some((extra_offsets, extra_keys)),
            );
        };
        if extra_offsets.is_empty() && extra_keys.is_empty() {
            return self.log.find_candidate(
                committed_offset,
                acknowledged_offsets,
                Some((current_offsets, current_keys)),
            );
        }
        let mut offsets = current_offsets.clone();
        offsets.extend(extra_offsets.iter().copied());
        let mut keys = current_keys.clone();
        keys.extend(extra_keys.iter().cloned());
        self.log.find_candidate(
            committed_offset,
            acknowledged_offsets,
            Some((&offsets, &keys)),
        )
    }
}

enum PollBatchStep {
    Complete(Vec<Message>),
    Waiting { next_expiry: Option<Instant> },
}

struct PollBatchStepRequest<'a> {
    stream: &'a str,
    consumer: &'a str,
    member: &'a str,
    response_member: Option<&'a str>,
    limits: ConsumeBatchLimits,
    deadline: Option<Instant>,
    token_seed: &'a str,
}

struct PendingDelivery {
    message: Message,
    key: Option<String>,
    attempt: u32,
    policy: ConsumerPolicy,
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
            #[cfg(test)]
            fail_next_dead_letter_ack_sync: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_consumer_batch_event_sync: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_consumer_batch_event_before_append: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_consumer_batch_event_partial_append: AtomicBool::new(false),
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
        let offset = match request_id {
            Some(request_id) => stream_state
                .log
                .append_with_request_id(key, payload, request_id),
            None => stream_state.log.append(key, payload),
        }?;
        stream_state.availability.notify_waiters();
        Ok(offset)
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
        let has_records = !records.is_empty();
        let outcomes = stream_state.log.append_batch(records)?;
        if has_records {
            stream_state.availability.notify_waiters();
        }
        Ok(outcomes)
    }

    pub fn poll(&self, stream: &str, consumer: &str) -> Result<PollResult, BrokerError> {
        self.poll_group(stream, consumer, consumer)
    }

    pub fn configure_consumer(
        &self,
        stream: &str,
        consumer: &str,
        ack_timeout_ms: u64,
        max_delivery_attempts: Option<u32>,
    ) -> Result<ConsumerPolicy, BrokerError> {
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        validate_consumer_policy(ack_timeout_ms, max_delivery_attempts)?;
        let stream_state = self.get_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        let root = self.inner.root.clone();
        let mut consumer_state = stream_state
            .delivery
            .load_consumer_state_for_request(&root, stream, consumer)?;
        if let Some(current) = consumer_state.policy.as_ref()
            && current.ack_timeout_ms == ack_timeout_ms
            && current.max_delivery_attempts == max_delivery_attempts
        {
            return Ok(current.clone());
        }
        let version = consumer_state
            .policy
            .as_ref()
            .map(|policy| policy.version.saturating_add(1))
            .unwrap_or(1);
        if version == 0 {
            return Err(BrokerError::Configuration(
                "consumer policy version exhausted".to_owned(),
            ));
        }
        let policy = ConsumerPolicy::configured(version, ack_timeout_ms, max_delivery_attempts);
        if let Err(error) = persist_consumer_event(
            &root,
            stream,
            consumer,
            &consumer_state,
            ConsumerStateEvent::PolicyConfigured {
                policy: policy.clone(),
            },
        ) {
            stream_state
                .delivery
                .mark_consumer_needs_reconcile(consumer);
            return Err(error);
        }
        consumer_state.policy = Some(policy.clone());
        consumer_state.stream = stream.to_owned();
        consumer_state.consumer = consumer.to_owned();
        stream_state
            .delivery
            .cache_consumer_state(consumer.to_owned(), consumer_state);
        stream_state.availability.notify_waiters();
        Ok(policy)
    }

    pub fn inspect_consumer(
        &self,
        stream: &str,
        consumer: &str,
    ) -> Result<ConsumerPolicy, BrokerError> {
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        let stream_state = self.get_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        let root = self.inner.root.clone();
        let consumer_state = stream_state
            .delivery
            .load_consumer_state_for_request(&root, stream, consumer)?;
        Ok(consumer_state.policy.clone().unwrap_or_else(|| {
            ConsumerPolicy::legacy(
                self.inner.ack_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                self.inner.max_delivery_attempts,
            )
        }))
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
        if stream_state.delivery.expire(now) {
            stream_state.availability.notify_waiters();
        }

        if let Some(in_flight) = stream_state.delivery.member_delivery(consumer, member)? {
            let mut message = stream_state.log.read_message(stream, in_flight.offset())?;
            message.delivery_token = Some(in_flight.delivery_token().to_owned());
            message.delivery_attempt = Some(in_flight.delivery_attempt());
            return Ok(PollResult::Message(message));
        }

        let mut consumer_state = stream_state
            .delivery
            .load_consumer_state_for_request(&root, stream, consumer)?;
        let legacy_policy = ConsumerPolicy::legacy(
            self.inner.ack_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            self.inner.max_delivery_attempts,
        );
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
            let policy = consumer_state.policy_for_offset(candidate.offset, &legacy_policy);
            if policy
                .max_delivery_attempts
                .is_some_and(|max_attempts| attempts >= max_attempts)
                && !self.is_dead_letter_stream(stream)?
            {
                self.dead_letter_record(&mut stream_state, stream, consumer, &candidate)?;
                if let Err(error) = self.persist_dead_letter_ack(
                    &root,
                    stream,
                    consumer,
                    &consumer_state,
                    candidate.offset,
                ) {
                    stream_state
                        .delivery
                        .mark_consumer_needs_reconcile(consumer);
                    return Err(error);
                }
                consumer_state.acknowledge(candidate.offset);
                stream_state
                    .delivery
                    .cache_consumer_state(consumer.to_owned(), consumer_state.clone());
                stream_state.availability.notify_waiters();
                self.inner.dead_letters.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let candidate_offset = candidate.offset;
            let delivery_attempt = attempts.saturating_add(1);
            let mut message = stream_state.log.read_message(stream, candidate.offset)?;
            if let Err(error) = persist_consumer_event(
                &root,
                stream,
                consumer,
                &consumer_state,
                ConsumerStateEvent::DeliveryAttempt {
                    offset: candidate.offset,
                    attempt: delivery_attempt,
                    policy: Some(policy.clone()),
                },
            ) {
                stream_state
                    .delivery
                    .mark_consumer_needs_reconcile(consumer);
                return Err(error);
            }
            consumer_state
                .delivery_attempts
                .insert(candidate.offset, delivery_attempt);
            consumer_state
                .delivery_policies
                .entry(candidate.offset)
                .or_insert_with(|| policy.clone());
            let delivery_token = self.inner.delivery_tokens.next();
            message.delivery_token = Some(delivery_token.clone());
            message.delivery_attempt = Some(delivery_attempt);
            if delivery_attempt > 1 {
                self.inner.redeliveries.fetch_add(1, Ordering::Relaxed);
            }
            let ack_timeout = Duration::from_millis(policy.ack_timeout_ms);
            stream_state.delivery.insert(
                consumer,
                InFlight::new(
                    member,
                    candidate_offset,
                    candidate.into_key(),
                    delivery_attempt,
                    delivery_token,
                    false,
                    Instant::now() + ack_timeout,
                ),
            );
            stream_state
                .delivery
                .cache_consumer_state(consumer.to_owned(), consumer_state);
            stream_state.availability.notify_waiters();
            return Ok(PollResult::Message(message));
        }
    }

    pub(super) async fn poll_batch_wait(
        &self,
        stream: String,
        consumer: String,
        member: String,
        response_member: Option<String>,
        limits: ConsumeBatchLimits,
    ) -> Result<Vec<Message>, BrokerError> {
        validate_name("stream", &stream)?;
        validate_name("consumer", &consumer)?;
        validate_name("member", &member)?;
        validate_consume_batch_limits(limits)?;
        if poll_batch_response_len(&stream, &consumer, response_member.as_deref(), &[])
            > limits.max_bytes
        {
            return Err(BrokerError::InvalidBatchRequest(
                "max_bytes is too small for an empty consume-batch response".to_owned(),
            ));
        }
        let stream_state = self.get_stream(&stream)?;
        let availability = self.lock_stream(&stream_state)?.availability.clone();
        let start = Instant::now();
        let deadline = Some(
            start
                .checked_add(Duration::from_millis(limits.max_wait_ms))
                .ok_or_else(|| {
                    BrokerError::InvalidBatchRequest("max_wait_ms is too large".to_owned())
                })?,
        );
        let token_seed = self.inner.delivery_tokens.next();

        loop {
            let notified = availability.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let broker = self.clone();
            let operation_stream = stream.clone();
            let operation_consumer = consumer.clone();
            let operation_member = member.clone();
            let operation_response_member = response_member.clone();
            let operation_token_seed = token_seed.clone();
            let step = Arc::clone(&self.inner.storage_executor)
                .dispatch_stream(operation_stream, move |stream| {
                    broker.poll_batch_step(PollBatchStepRequest {
                        stream,
                        consumer: &operation_consumer,
                        member: &operation_member,
                        response_member: operation_response_member.as_deref(),
                        limits,
                        deadline,
                        token_seed: &operation_token_seed,
                    })
                })
                .await?;

            match step {
                PollBatchStep::Complete(messages) => return Ok(messages),
                PollBatchStep::Waiting { next_expiry } => {
                    let wake_at = match (deadline, next_expiry) {
                        (Some(deadline), Some(expiry)) => Some(deadline.min(expiry)),
                        (Some(deadline), None) => Some(deadline),
                        (None, expiry) => expiry,
                    };
                    if let Some(wake_at) = wake_at {
                        tokio::select! {
                            _ = &mut notified => {}
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)) => {}
                        }
                    } else {
                        notified.await;
                    }
                }
            }
        }
    }

    fn poll_batch_step(
        &self,
        request: PollBatchStepRequest<'_>,
    ) -> Result<PollBatchStep, BrokerError> {
        let PollBatchStepRequest {
            stream,
            consumer,
            member,
            response_member,
            limits,
            deadline,
            token_seed,
        } = request;
        let stream_state = self.get_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        let now = Instant::now();
        if stream_state.delivery.expire(now) {
            stream_state.availability.notify_waiters();
        }
        let root = self.inner.root.clone();
        let mut consumer_state = stream_state
            .delivery
            .load_consumer_state_for_request(&root, stream, consumer)?;

        let existing = stream_state.delivery.member_deliveries(consumer, member)?;
        if !existing.is_empty() {
            if existing.len() > limits.max_records {
                return Err(BrokerError::InvalidBatchRequest(
                    "the active delivery set exceeds max_records; retry with the original or a larger limit"
                        .to_owned(),
                ));
            }
            let mut messages = Vec::with_capacity(existing.len());
            let mut size = PollBatchResponseSizer::new(stream, consumer, response_member);
            for delivery in existing {
                let mut message = stream_state.log.read_message(stream, delivery.offset())?;
                message.delivery_token = Some(delivery.delivery_token().to_owned());
                message.delivery_attempt = Some(delivery.delivery_attempt());
                if size.push(&message) > limits.max_bytes {
                    return Err(BrokerError::InvalidBatchRequest(
                        "the active delivery set exceeds max_bytes; retry with the original or a larger limit"
                            .to_owned(),
                    ));
                }
                messages.push(message);
            }
            return Ok(PollBatchStep::Complete(messages));
        }

        let legacy_policy = ConsumerPolicy::legacy(
            self.inner.ack_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            self.inner.max_delivery_attempts,
        );
        let mut extra_offsets = HashSet::with_capacity(limits.max_records);
        let mut extra_keys = HashSet::with_capacity(limits.max_records);
        let mut pending = Vec::with_capacity(limits.max_records);
        let mut sizer = PollBatchResponseSizer::new(stream, consumer, response_member);
        let is_waiting = limits.max_wait_ms > 0 && deadline.is_none_or(|deadline| now < deadline);
        let mut stopped_at_byte_limit = false;

        while pending.len() < limits.max_records && sizer.encoded_len() < limits.max_bytes {
            let candidate = stream_state.find_candidate_excluding(
                consumer,
                consumer_state.committed_offset,
                &consumer_state.acknowledged_offsets,
                &extra_offsets,
                &extra_keys,
            )?;
            let Some(candidate) = candidate else {
                break;
            };

            let attempts = consumer_state
                .delivery_attempts
                .get(&candidate.offset)
                .copied()
                .unwrap_or_default();
            let policy = consumer_state.policy_for_offset(candidate.offset, &legacy_policy);
            if policy
                .max_delivery_attempts
                .is_some_and(|maximum| attempts >= maximum)
                && !self.is_dead_letter_stream(stream)?
            {
                self.dead_letter_record(&mut stream_state, stream, consumer, &candidate)?;
                if let Err(error) = self.persist_dead_letter_ack(
                    &root,
                    stream,
                    consumer,
                    &consumer_state,
                    candidate.offset,
                ) {
                    stream_state
                        .delivery
                        .mark_consumer_needs_reconcile(consumer);
                    return Err(error);
                }
                consumer_state.acknowledge(candidate.offset);
                stream_state
                    .delivery
                    .cache_consumer_state(consumer.to_owned(), consumer_state.clone());
                stream_state.availability.notify_waiters();
                self.inner.dead_letters.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let delivery_attempt = attempts.saturating_add(1);
            let mut message = stream_state.log.read_message(stream, candidate.offset)?;
            let delivery_token = format!("{token_seed}-{:x}", candidate.offset);
            message.delivery_token = Some(delivery_token);
            message.delivery_attempt = Some(delivery_attempt);
            let projected_size = sizer.projected_len(&message);
            if projected_size > limits.max_bytes {
                if pending.is_empty() {
                    return Err(BrokerError::ConsumeBatchRecordTooLarge {
                        max_bytes: limits.max_bytes,
                    });
                }
                stopped_at_byte_limit = true;
                break;
            }

            sizer.push(&message);
            extra_offsets.insert(candidate.offset);
            if let Some(key) = message.key.as_ref() {
                extra_keys.insert(key.clone());
            }
            pending.push(PendingDelivery {
                message,
                key: candidate.into_key(),
                attempt: delivery_attempt,
                policy,
            });
        }

        let now = Instant::now();
        let deadline_reached = deadline.is_some_and(|deadline| now >= deadline);
        let record_limit_reached = pending.len() == limits.max_records;
        let byte_limit_reached = stopped_at_byte_limit || sizer.encoded_len() >= limits.max_bytes;
        if is_waiting && !deadline_reached && !record_limit_reached && !byte_limit_reached {
            return Ok(PollBatchStep::Waiting {
                next_expiry: stream_state.delivery.next_deadline_for_consumer(consumer),
            });
        }
        if pending.is_empty() {
            return Ok(PollBatchStep::Complete(Vec::new()));
        }

        let assignments = pending
            .iter()
            .map(|delivery| super::consumer_state::DeliveryAttempt {
                offset: delivery.message.offset,
                attempt: delivery.attempt,
                policy: delivery.policy.clone(),
            })
            .collect::<Vec<_>>();
        if let Err(error) = self.persist_consumer_batch_event(
            &root,
            stream,
            consumer,
            &consumer_state,
            ConsumerStateEvent::DeliveryAttempts {
                attempts: assignments,
            },
        ) {
            stream_state
                .delivery
                .mark_consumer_needs_reconcile(consumer);
            return Err(error);
        }

        for delivery in &pending {
            consumer_state
                .delivery_attempts
                .insert(delivery.message.offset, delivery.attempt);
            consumer_state
                .delivery_policies
                .entry(delivery.message.offset)
                .or_insert_with(|| delivery.policy.clone());
            let timeout = Duration::from_millis(delivery.policy.ack_timeout_ms);
            stream_state.delivery.insert(
                consumer,
                InFlight::new(
                    member,
                    delivery.message.offset,
                    delivery.key.clone(),
                    delivery.attempt,
                    delivery
                        .message
                        .delivery_token
                        .as_ref()
                        .expect("batch delivery token was assigned above")
                        .clone(),
                    true,
                    Instant::now() + timeout,
                ),
            );
            if delivery.attempt > 1 {
                self.inner.redeliveries.fetch_add(1, Ordering::Relaxed);
            }
        }
        consumer_state.stream = stream.to_owned();
        consumer_state.consumer = consumer.to_owned();
        stream_state
            .delivery
            .cache_consumer_state(consumer.to_owned(), consumer_state);
        stream_state.availability.notify_waiters();
        Ok(PollBatchStep::Complete(
            pending
                .into_iter()
                .map(|delivery| delivery.message)
                .collect(),
        ))
    }

    pub fn ack_batch(
        &self,
        stream: &str,
        consumer: &str,
        member: &str,
        receipts: Vec<DeliveryReceipt>,
    ) -> Result<AckBatchResult, BrokerError> {
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        validate_name("member", member)?;
        validate_ack_batch_receipts(&receipts)?;
        let stream_state = self.get_stream(stream)?;
        let mut stream_state = self.lock_stream(&stream_state)?;
        let root = self.inner.root.clone();
        let mut consumer_state = stream_state
            .delivery
            .load_consumer_state_for_request(&root, stream, consumer)?;
        if stream_state.delivery.expire(Instant::now()) {
            stream_state.availability.notify_waiters();
        }

        let mut valid_offsets = Vec::new();
        let mut outcomes = Vec::with_capacity(receipts.len());
        for receipt in receipts {
            let outcome = if receipt.offset < consumer_state.committed_offset
                || consumer_state
                    .acknowledged_offsets
                    .contains(&receipt.offset)
            {
                AckBatchOutcome::AlreadyConfirmed
            } else if receipt.delivery_token.is_empty() {
                AckBatchOutcome::Rejected {
                    reason: AckBatchRejection::StaleDelivery,
                }
            } else if let Some(in_flight) = stream_state
                .delivery
                .get_in_flight(consumer, receipt.offset)
            {
                if in_flight.member() != member
                    || in_flight.delivery_token() != receipt.delivery_token
                {
                    AckBatchOutcome::Rejected {
                        reason: AckBatchRejection::StaleDelivery,
                    }
                } else {
                    valid_offsets.push(receipt.offset);
                    AckBatchOutcome::Confirmed
                }
            } else if consumer_state
                .delivery_attempts
                .contains_key(&receipt.offset)
            {
                AckBatchOutcome::Rejected {
                    reason: AckBatchRejection::StaleDelivery,
                }
            } else {
                AckBatchOutcome::Rejected {
                    reason: AckBatchRejection::NotInFlight,
                }
            };
            outcomes.push(AckBatchItem {
                offset: receipt.offset,
                outcome,
            });
        }

        if valid_offsets.is_empty() {
            return Ok(AckBatchResult { outcomes });
        }
        if let Err(error) = self.persist_consumer_batch_event(
            &root,
            stream,
            consumer,
            &consumer_state,
            ConsumerStateEvent::AcknowledgeBatch {
                offsets: valid_offsets.clone(),
            },
        ) {
            stream_state
                .delivery
                .mark_consumer_needs_reconcile(consumer);
            return Err(error);
        }

        for offset in valid_offsets {
            consumer_state.acknowledge(offset);
            stream_state.delivery.remove(consumer, offset);
        }
        consumer_state.stream = stream.to_owned();
        consumer_state.consumer = consumer.to_owned();
        stream_state
            .delivery
            .cache_consumer_state(consumer.to_owned(), consumer_state);
        stream_state.availability.notify_waiters();
        Ok(AckBatchResult { outcomes })
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
        // An acknowledgement observes lease expiry even before reassignment.
        if stream_state.delivery.expire(Instant::now()) {
            stream_state.availability.notify_waiters();
        }
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
            || ((in_flight.requires_receipt() || !delivery_token.is_empty())
                && in_flight.delivery_token() != delivery_token)
        {
            return Err(BrokerError::StaleDelivery {
                consumer: consumer.to_owned(),
                offset,
            });
        }

        if let Err(error) = persist_consumer_event(
            &root,
            stream,
            consumer,
            &consumer_state,
            ConsumerStateEvent::Acknowledge { offset },
        ) {
            stream_state
                .delivery
                .mark_consumer_needs_reconcile(consumer);
            return Err(error);
        }
        consumer_state.acknowledge(offset);
        consumer_state.stream = stream.to_owned();
        consumer_state.consumer = consumer.to_owned();
        stream_state.delivery.remove(consumer, offset);
        stream_state
            .delivery
            .cache_consumer_state(consumer.to_owned(), consumer_state);
        stream_state.availability.notify_waiters();
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

    fn is_dead_letter_stream(&self, stream: &str) -> Result<bool, BrokerError> {
        if stream.ends_with(DEAD_LETTER_SUFFIX) {
            return Ok(true);
        }
        if !stream.starts_with(DEAD_LETTER_HASH_PREFIX) {
            return Ok(false);
        }

        let streams = self
            .inner
            .streams
            .read()
            .map_err(|_| BrokerError::LockPoisoned)?;
        Ok(streams.keys().any(|source| {
            dead_letter_stream_name(source)
                .map(|target| target == stream)
                .unwrap_or(false)
        }))
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

        #[cfg(test)]
        if self
            .inner
            .fail_next_dead_letter_ack_sync
            .swap(false, Ordering::AcqRel)
        {
            return super::consumer_state::persist_consumer_event_with_sync_failure(
                root,
                stream,
                consumer,
                current_state,
                ConsumerStateEvent::Acknowledge { offset },
            );
        }

        persist_consumer_event(
            root,
            stream,
            consumer,
            current_state,
            ConsumerStateEvent::Acknowledge { offset },
        )
    }

    fn persist_consumer_batch_event(
        &self,
        root: &Path,
        stream: &str,
        consumer: &str,
        current_state: &ConsumerState,
        event: ConsumerStateEvent,
    ) -> Result<(), BrokerError> {
        #[cfg(test)]
        if self
            .inner
            .fail_next_consumer_batch_event_before_append
            .swap(false, Ordering::AcqRel)
        {
            return Err(BrokerError::ConsumerStatePersistence {
                stage: runnel_engine::ConsumerStatePersistStage::BeforeAppend,
                message: "injected consumer state failure before append".to_owned(),
            });
        }

        #[cfg(test)]
        if self
            .inner
            .fail_next_consumer_batch_event_partial_append
            .swap(false, Ordering::AcqRel)
        {
            return super::consumer_state::persist_consumer_event_with_partial_append_failure(
                root, stream, consumer, event,
            );
        }

        #[cfg(test)]
        if self
            .inner
            .fail_next_consumer_batch_event_sync
            .swap(false, Ordering::AcqRel)
        {
            return super::consumer_state::persist_consumer_event_with_sync_failure(
                root,
                stream,
                consumer,
                current_state,
                event,
            );
        }

        persist_consumer_event(root, stream, consumer, current_state, event)
    }

    #[cfg(test)]
    pub(super) fn fail_next_dead_letter_ack_persist(&self) {
        self.inner
            .fail_next_dead_letter_ack_persist
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn fail_next_dead_letter_ack_sync(&self) {
        self.inner
            .fail_next_dead_letter_ack_sync
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn fail_next_consumer_batch_event_sync(&self) {
        self.inner
            .fail_next_consumer_batch_event_sync
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn fail_next_consumer_batch_event_before_append(&self) {
        self.inner
            .fail_next_consumer_batch_event_before_append
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn fail_next_consumer_batch_event_partial_append(&self) {
        self.inner
            .fail_next_consumer_batch_event_partial_append
            .store(true, Ordering::Release);
    }
}
