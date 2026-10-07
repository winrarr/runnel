use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use runnel_engine::{
    AckBatchResult, ConsumeBatchLimits, DeliveryReceipt, Engine, EngineFuture, PublishRecord,
    PublishRecordOutcome,
};
#[cfg(test)]
use std::fs;

mod broker;
mod consumer_state;
mod delivery_state;
mod storage;
mod stream_log;

pub use broker::Broker;

#[cfg(test)]
use consumer_state::MAX_CONSUMER_STATE_JOURNAL_BYTES;
#[cfg(test)]
use consumer_state::load_consumer_state;
#[cfg(test)]
use delivery_state::MAX_CACHED_CONSUMER_STATES;
#[cfg(test)]
use std::fs::OpenOptions;
#[cfg(test)]
use std::io::Write;
use stream_log::MAX_REQUEST_ID_LEN;
#[cfg(test)]
use stream_log::{
    MAX_BODY_LEN, MAX_IN_MEMORY_RECORDS, MAX_KEY_LEN, RECORD_FORMAT_VERSION, RECORD_HEADER_LEN,
    RECORD_MAGIC,
};
const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const DEAD_LETTER_SUFFIX: &str = ".dead-letter";
const DEAD_LETTER_HASH_PREFIX: &str = "runnel.dead-letter.";

pub use runnel_engine::{
    AckResult, BrokerError, ConsumerPolicy, HealthSnapshot, Message, Offset, PollResult,
    ReplayMessage,
};

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

// Broker orchestration and its private state live in `broker`.

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

    fn poll_with_response_limit<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        max_response_bytes: usize,
    ) -> EngineFuture<'a, PollResult> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.poll_with_response_limit(stream, &consumer, max_response_bytes)
        })
    }

    fn poll_batch<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        limits: ConsumeBatchLimits,
    ) -> EngineFuture<'a, Vec<Message>> {
        let broker = self.clone();
        Box::pin(async move {
            broker
                .poll_batch_wait(
                    stream.to_owned(),
                    consumer.to_owned(),
                    consumer.to_owned(),
                    None,
                    limits,
                )
                .await
        })
    }

    fn configure_consumer<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        ack_timeout_ms: u64,
        max_delivery_attempts: Option<u32>,
        retry_delay_ms: u64,
    ) -> EngineFuture<'a, ConsumerPolicy> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.configure_consumer(
                stream,
                &consumer,
                ack_timeout_ms,
                max_delivery_attempts,
                retry_delay_ms,
            )
        })
    }

    fn inspect_consumer<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
    ) -> EngineFuture<'a, ConsumerPolicy> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.inspect_consumer(stream, &consumer)
        })
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

    fn poll_group_with_response_limit<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        max_response_bytes: usize,
    ) -> EngineFuture<'a, PollResult> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        let member = member.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.poll_group_with_response_limit(stream, &consumer, &member, max_response_bytes)
        })
    }

    fn poll_group_batch<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        limits: ConsumeBatchLimits,
    ) -> EngineFuture<'a, Vec<Message>> {
        let broker = self.clone();
        Box::pin(async move {
            broker
                .poll_batch_wait(
                    stream.to_owned(),
                    consumer.to_owned(),
                    member.to_owned(),
                    Some(member.to_owned()),
                    limits,
                )
                .await
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

    fn ack_batch<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        receipts: Vec<DeliveryReceipt>,
    ) -> EngineFuture<'a, AckBatchResult> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.ack_batch(stream, &consumer, &consumer, receipts)
        })
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

    fn ack_group_batch<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        receipts: Vec<DeliveryReceipt>,
    ) -> EngineFuture<'a, AckBatchResult> {
        let broker = self.clone();
        let stream = stream.to_owned();
        let consumer = consumer.to_owned();
        let member = member.to_owned();
        Arc::clone(&self.inner.storage_executor).dispatch_stream(stream, move |stream| {
            broker.ack_batch(stream, &consumer, &member, receipts)
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
    let fallback = format!("{DEAD_LETTER_HASH_PREFIX}{hash:016x}");
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
    if move_id.len() <= MAX_REQUEST_ID_LEN as usize {
        return Ok(move_id);
    }
    Err(BrokerError::Io(io::Error::new(
        io::ErrorKind::InvalidInput,
        "dead-letter move identity exceeds storage limit",
    )))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;

    #[tokio::test]
    async fn local_engine_implements_consume_batch_contract() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        runnel_test_support::assert_consume_batch_contract(&broker).await;
    }

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
            stream_state.delivery.consumer_state_cache_len(),
            MAX_CACHED_CONSUMER_STATES
        );
        let evicted_consumer = (0..=MAX_CACHED_CONSUMER_STATES)
            .map(|index| format!("consumer-{index}"))
            .find(|consumer| !stream_state.delivery.has_cached_consumer(consumer))
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
            Some("original-key".to_owned()),
            b"original".to_vec(),
            Some("request-1".to_owned()),
        )
        .await
        .unwrap();

        assert_eq!((first, retry), (0, 0));
        let key_conflict = Engine::publish(
            &broker,
            "events",
            Some("retry-key".to_owned()),
            b"original".to_vec(),
            Some("request-1".to_owned()),
        )
        .await
        .unwrap_err();
        assert_eq!(
            key_conflict.kind(),
            runnel_engine::BrokerErrorKind::RequestIdContentConflict
        );
        assert_eq!(
            key_conflict.outcome(),
            runnel_engine::BrokerErrorOutcome::Rejected
        );
        assert!(matches!(
            Engine::publish(
                &broker,
                "events",
                Some("original-key".to_owned()),
                b"retry-payload".to_vec(),
                Some("request-1".to_owned()),
            )
            .await,
            Err(BrokerError::RequestIdContentConflict)
        ));
        assert_eq!(
            Engine::publish(&broker, "events", None, b"next".to_vec(), None)
                .await
                .unwrap(),
            1
        );
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
        assert!(matches!(
            broker.poll("events", "reader").unwrap(),
            PollResult::Message(Message { offset: 1, payload, .. }) if payload == b"next"
        ));
        assert_eq!(
            broker.ack("events", "reader", 1).unwrap(),
            AckResult::Acknowledged
        );
        assert_eq!(broker.poll("events", "reader").unwrap(), PollResult::Empty);
    }

    #[tokio::test]
    async fn broker_implements_publish_request_id_content_contract() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        runnel_test_support::assert_publish_request_id_contract(&broker).await;
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
                b"events".to_vec(),
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
                b"audit".to_vec(),
                Some("same-request".to_owned()),
            )
            .await
            .unwrap(),
            0
        );
        for (stream, payload) in [
            ("events", b"events-retry".as_slice()),
            ("audit", b"audit-retry"),
        ] {
            assert!(matches!(
                Engine::publish(
                    &broker,
                    stream,
                    None,
                    payload.to_vec(),
                    Some("same-request".to_owned()),
                )
                .await,
                Err(BrokerError::RequestIdContentConflict)
            ));
        }
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
                Some("original-key".to_owned()),
                b"original".to_vec(),
                Some("request-1".to_owned()),
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            Engine::publish(
                &broker,
                "events",
                Some("retry-key".to_owned()),
                b"retry-payload".to_vec(),
                Some("request-1".to_owned()),
            )
            .await
            .unwrap_err()
            .kind(),
            runnel_engine::BrokerErrorKind::RequestIdContentConflict
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
    async fn concurrent_mismatched_request_ids_accept_one_content() {
        const CALL_COUNT: usize = 32;

        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let mut calls = Vec::with_capacity(CALL_COUNT);
        for call in 0..CALL_COUNT {
            let broker = broker.clone();
            calls.push(tokio::spawn(async move {
                let result = Engine::publish(
                    &broker,
                    "events",
                    Some(format!("key-{call}")),
                    format!("payload-{call}").into_bytes(),
                    Some("request-1".to_owned()),
                )
                .await;
                (call, result)
            }));
        }

        let mut accepted_call = None;
        let mut conflicts = 0;
        for call in calls {
            let (call, result) = call.await.unwrap();
            match result {
                Ok(0) => {
                    assert!(accepted_call.replace(call).is_none());
                }
                Err(error) => {
                    assert_eq!(
                        error.kind(),
                        runnel_engine::BrokerErrorKind::RequestIdContentConflict
                    );
                    conflicts += 1;
                }
                result => panic!("unexpected concurrent publish result: {result:?}"),
            }
        }
        assert!(accepted_call.is_some());
        assert_eq!(conflicts, CALL_COUNT - 1);
        assert!(matches!(
            broker.poll("events", "reader").unwrap(),
            PollResult::Message(Message { offset: 0, key: Some(key), payload, .. })
                if key == format!("key-{}", accepted_call.unwrap())
                    && payload == format!("payload-{}", accepted_call.unwrap()).as_bytes()
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
        let ack_timeout = Duration::from_secs(2);
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout,
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

        std::thread::sleep(ack_timeout + Duration::from_millis(100));
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
        assert!(stream_state.delivery.is_empty());
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
    fn oversized_scalar_poll_does_not_persist_a_delivery_attempt() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.publish("events", None, vec![0xff; 2_048]).unwrap();

        assert!(matches!(
            broker.poll_with_response_limit("events", "worker", 1_024),
            Err(BrokerError::ResponseTooLarge { max_bytes: 1_024 })
        ));
        let state = load_consumer_state(directory.path(), "events", "worker").unwrap();
        assert_eq!(state.committed_offset, 0);
        assert!(state.delivery_attempts.is_empty());
        assert_eq!(broker.health().unwrap().in_flight_deliveries, 0);

        let delivered = broker
            .poll_with_response_limit("events", "worker", 4_096)
            .unwrap();
        assert!(matches!(
            delivered,
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(1),
                ..
            })
        ));
    }

    #[test]
    fn oversized_poll_does_not_commit_expiry_retry_or_member_changes() {
        use crate::delivery_state::InFlight;
        use std::time::Instant;

        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.create_stream("events").unwrap();
        broker
            .configure_consumer("events", "workers", 60_000, None, 60_000)
            .unwrap();
        broker
            .publish("events", None, b"expired-small".to_vec())
            .unwrap();
        broker.publish("events", None, vec![0xff; 2_048]).unwrap();

        let expired = match broker
            .poll_group("events", "workers", "expired-member")
            .unwrap()
        {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected the first delivery"),
        };
        let active = match broker
            .poll_group("events", "workers", "target-member")
            .unwrap()
        {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected the second delivery"),
        };

        let stream_state = broker.inner.streams.read().unwrap()["events"].clone();
        let mut stream_state = stream_state.lock().unwrap();
        stream_state.delivery.remove("workers", expired.offset);
        stream_state.delivery.insert(
            "workers",
            InFlight::new(
                "expired-member",
                expired.offset,
                expired.key,
                expired.delivery_attempt.unwrap(),
                expired.delivery_token.unwrap(),
                false,
                Instant::now() - Duration::from_millis(1),
            ),
        );
        assert_eq!(
            stream_state
                .delivery
                .member_delivery("workers", "target-member")
                .unwrap()
                .unwrap()
                .offset(),
            active.offset
        );
        drop(stream_state);

        assert!(matches!(
            broker.poll_group_with_response_limit("events", "workers", "target-member", 1_024),
            Err(BrokerError::ResponseTooLarge { max_bytes: 1_024 })
        ));

        let state = load_consumer_state(directory.path(), "events", "workers").unwrap();
        assert_eq!(state.delivery_attempts.get(&expired.offset), Some(&1));
        assert!(state.retry_not_before.is_empty());
        assert_eq!(state.delivery_attempts.get(&active.offset), Some(&1));
        let stream_state = broker.inner.streams.read().unwrap()["events"].clone();
        let stream_state = stream_state.lock().unwrap();
        assert!(
            stream_state
                .delivery
                .member_delivery("workers", "expired-member")
                .unwrap()
                .is_some()
        );
        assert!(
            stream_state
                .delivery
                .member_delivery("workers", "target-member")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn oversized_poll_does_not_commit_staged_dead_letter_transitions() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(
            directory.path(),
            BrokerConfig {
                ack_timeout: Duration::ZERO,
                max_delivery_attempts: Some(1),
            },
        )
        .unwrap();
        broker.publish("events", None, b"poison".to_vec()).unwrap();
        broker.publish("events", None, vec![0xff; 2_048]).unwrap();
        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message { offset: 0, .. })
        ));

        assert!(matches!(
            broker.poll_with_response_limit("events", "worker", 1_024),
            Err(BrokerError::ResponseTooLarge { max_bytes: 1_024 })
        ));
        let source = load_consumer_state(directory.path(), "events", "worker").unwrap();
        assert_eq!(source.committed_offset, 0);
        assert_eq!(source.delivery_attempts.get(&0), Some(&1));
        assert_eq!(broker.health().unwrap().streams, 1);
        assert_eq!(broker.health().unwrap().dead_letters, 0);

        assert!(matches!(
            broker.poll_with_response_limit("events", "worker", 4_096),
            Ok(PollResult::Message(Message {
                offset: 1,
                delivery_attempt: Some(1),
                ..
            }))
        ));
        let source = load_consumer_state(directory.path(), "events", "worker").unwrap();
        assert_eq!(source.committed_offset, 1);
        assert_eq!(source.delivery_attempts.get(&1), Some(&1));
        assert_eq!(broker.health().unwrap().dead_letters, 1);
    }

    #[test]
    fn over_cap_poll_limit_is_rejected_before_creating_a_delivery() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.create_stream("events").unwrap();
        broker.publish("events", None, b"bounded".to_vec()).unwrap();

        assert!(matches!(
            broker.poll_group_with_response_limit(
                "events",
                "workers",
                "member-a",
                runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES + 1,
            ),
            Err(BrokerError::InvalidBatchRequest(_))
        ));

        let PollResult::Message(message) = broker
            .poll_group_with_response_limit(
                "events",
                "workers",
                "member-a",
                runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
            )
            .unwrap()
        else {
            panic!("expected first delivery after rejected over-cap poll");
        };
        assert_eq!(message.offset, 0);
        assert_eq!(message.delivery_attempt, Some(1));
    }

    #[test]
    fn consumer_policy_is_isolated_durable_and_pinned_per_delivery() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.create_stream("events").unwrap();
        let policy = broker
            .configure_consumer("events", "worker-a", 0, Some(2), 0)
            .unwrap();
        assert_eq!(policy.version, 1);
        assert!(policy.configured);
        assert_eq!(
            broker.inspect_consumer("events", "worker-a").unwrap(),
            policy
        );
        assert!(
            !broker
                .inspect_consumer("events", "worker-b")
                .unwrap()
                .configured
        );
        broker.publish("events", None, b"poison".to_vec()).unwrap();

        let first = broker.poll("events", "worker-a").unwrap();
        assert!(matches!(
            first,
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        // Updating a policy does not change the attempt budget of an in-flight record.
        broker
            .configure_consumer("events", "worker-a", 0, Some(1), 0)
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let second = broker.poll("events", "worker-a").unwrap();
        assert!(matches!(
            second,
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            broker.poll("events", "worker-a").unwrap(),
            PollResult::Empty
        );

        // The other consumer keeps its own independent policy and checkpoint.
        assert!(matches!(
            broker.poll("events", "worker-b").unwrap(),
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(1),
                ..
            })
        ));

        drop(broker);
        let reopened = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let reopened_policy = reopened.inspect_consumer("events", "worker-a").unwrap();
        assert_eq!(reopened_policy.version, 2);
        assert_eq!(reopened_policy.max_delivery_attempts, Some(1));
    }

    #[test]
    fn pending_delivery_keeps_pinned_attempt_limit_after_reopen() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.create_stream("events").unwrap();
        let original_policy = broker
            .configure_consumer("events", "worker", 0, Some(2), 0)
            .unwrap();
        assert_eq!(original_policy.version, 1);
        broker.publish("events", None, b"poison".to_vec()).unwrap();

        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(1),
                ..
            })
        ));
        let updated_policy = broker
            .configure_consumer("events", "worker", 0, Some(1), 0)
            .unwrap();
        assert_eq!(updated_policy.version, 2);
        assert_eq!(updated_policy.max_delivery_attempts, Some(1));
        drop(broker);

        let reopened = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(
            reopened.inspect_consumer("events", "worker").unwrap(),
            updated_policy
        );
        assert!(matches!(
            reopened.poll("events", "worker").unwrap(),
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(2),
                payload,
                ..
            }) if payload == b"poison"
        ));
        assert_eq!(
            reopened.poll("events", "worker").unwrap(),
            PollResult::Empty
        );
        assert!(matches!(
            reopened.poll("events.dead-letter", "inspector").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                payload,
                ..
            }) if payload == b"poison"
        ));
    }

    #[test]
    fn maximum_length_dead_letter_target_does_not_recurse() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: Some(1),
        };
        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        let source = "s".repeat(128);
        broker
            .publish(&source, Some("order-1".to_owned()), b"poison".to_vec())
            .unwrap();

        assert!(matches!(
            broker.poll(&source, "worker").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        assert_eq!(broker.poll(&source, "worker").unwrap(), PollResult::Empty);

        let dead_letter = dead_letter_stream_name(&source).unwrap();
        assert!(dead_letter.starts_with(DEAD_LETTER_HASH_PREFIX));
        assert!(matches!(
            broker.poll(&dead_letter, "inspector").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));

        drop(broker);
        let broker = Broker::open(directory.path(), config).unwrap();
        assert!(matches!(
            broker.poll(&dead_letter, "inspector").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(2),
                ..
            })
        ));
        assert_eq!(broker.health().unwrap().streams, 2);

        let nested_dead_letter = format!("{dead_letter}{DEAD_LETTER_SUFFIX}");
        assert!(matches!(
            broker.poll(&nested_dead_letter, "inspector"),
            Err(BrokerError::StreamNotFound(stream)) if stream == nested_dead_letter
        ));
    }

    #[test]
    fn user_stream_with_dead_letter_hash_prefix_still_dead_letters() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: Some(1),
        };
        let broker = Broker::open(directory.path(), config).unwrap();
        let source = "runnel.dead-letter.user";
        broker
            .publish(source, Some("order-1".to_owned()), b"poison".to_vec())
            .unwrap();

        assert!(matches!(
            broker.poll(source, "worker").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        assert_eq!(broker.poll(source, "worker").unwrap(), PollResult::Empty);

        let dead_letter = dead_letter_stream_name(source).unwrap();
        assert!(matches!(
            broker.poll(&dead_letter, "inspector").unwrap(),
            PollResult::Message(Message {
                delivery_attempt: Some(1),
                ..
            })
        ));
        assert_eq!(broker.health().unwrap().dead_letters, 1);
    }

    #[test]
    fn dead_letter_move_identity_is_stable_scoped_and_bounded() {
        let first = dead_letter_move_id("events", "worker", 7).unwrap();
        assert_eq!(first, dead_letter_move_id("events", "worker", 7).unwrap());
        assert_ne!(first, dead_letter_move_id("events", "other", 7).unwrap());
        assert_ne!(first, dead_letter_move_id("audit", "worker", 7).unwrap());
        assert_ne!(first, dead_letter_move_id("events", "worker", 8).unwrap());
        assert!(first.len() <= MAX_REQUEST_ID_LEN as usize);
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
        assert_eq!(target.log.request_identity_count(), 1);
    }

    #[test]
    fn dead_letter_move_reconciles_target_after_restart() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::from_secs(60),
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
            assert_eq!(target.log.dead_letter_move_offset(&move_id), Some(0));
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
            assert_eq!(target.log.dead_letter_move_offset(&move_id), Some(0));
        }

        let broker = Broker::open(directory.path(), config).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        let target = broker.get_stream("events.dead-letter").unwrap();
        let target = broker.lock_stream(&target).unwrap();
        assert_eq!(target.log.next_offset(), 1);
        assert_eq!(target.log.dead_letter_move_offset(&move_id), Some(0));
    }

    #[test]
    fn dead_letter_move_recovers_after_partial_target_write_and_restart() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: Some(1),
        };
        let move_id = dead_letter_move_id("events", "worker", 0).unwrap();
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
        fail_next_dead_letter_target_write(
            &broker,
            stream_log::DeadLetterMoveWriteFailure::PartialFrame,
        );

        assert!(matches!(
            broker.poll("events", "worker"),
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::Interrupted
        ));
        let source_state = load_consumer_state(directory.path(), "events", "worker").unwrap();
        assert_eq!(source_state.committed_offset, 0);
        assert_eq!(source_state.delivery_attempts.get(&0), Some(&1));
        drop(broker);

        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        assert_dead_letter_move(&broker, &move_id);
        drop(broker);

        let broker = Broker::open(directory.path(), config).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        assert_dead_letter_move(&broker, &move_id);
    }

    #[test]
    fn dead_letter_move_reconciles_complete_target_write_reported_as_failure() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: Some(1),
        };
        let move_id = dead_letter_move_id("events", "worker", 0).unwrap();
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
        fail_next_dead_letter_target_write(
            &broker,
            stream_log::DeadLetterMoveWriteFailure::CompleteFrameBeforeSync,
        );

        assert!(matches!(
            broker.poll("events", "worker"),
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::Interrupted
        ));
        let source_state = load_consumer_state(directory.path(), "events", "worker").unwrap();
        assert_eq!(source_state.committed_offset, 0);
        assert_eq!(source_state.delivery_attempts.get(&0), Some(&1));
        drop(broker);

        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        assert_dead_letter_move(&broker, &move_id);
        drop(broker);

        let broker = Broker::open(directory.path(), config).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        assert_dead_letter_move(&broker, &move_id);
    }

    #[test]
    fn dead_letter_move_retries_after_source_event_sync_failure_and_restart() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: Some(1),
        };
        let move_id = dead_letter_move_id("events", "worker", 0).unwrap();
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
        broker.fail_next_dead_letter_ack_sync();

        assert!(matches!(
            broker.poll("events", "worker"),
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::Interrupted
        ));
        let target = broker.get_stream("events.dead-letter").unwrap();
        let target = broker.lock_stream(&target).unwrap();
        assert_eq!(target.log.next_offset(), 1);
        drop(target);
        drop(broker);

        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        let source_state = load_consumer_state(directory.path(), "events", "worker").unwrap();
        assert_eq!(source_state.committed_offset, 1);
        assert_dead_letter_move(&broker, &move_id);
        drop(broker);

        let broker = Broker::open(directory.path(), config).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        assert_dead_letter_move(&broker, &move_id);
    }

    fn fail_next_dead_letter_target_write(
        broker: &Broker,
        failure: stream_log::DeadLetterMoveWriteFailure,
    ) {
        broker.create_stream("events.dead-letter").unwrap();
        let target = broker.get_stream("events.dead-letter").unwrap();
        broker
            .lock_stream(&target)
            .unwrap()
            .log
            .fail_next_dead_letter_move_write(failure);
    }

    fn assert_dead_letter_move(broker: &Broker, move_id: &str) {
        assert_dead_letter_move_at(broker, move_id, 0, 1);
    }

    fn assert_dead_letter_move_at(
        broker: &Broker,
        move_id: &str,
        offset: Offset,
        target_len: Offset,
    ) {
        let target = broker.get_stream("events.dead-letter").unwrap();
        let mut target = broker.lock_stream(&target).unwrap();
        assert_eq!(target.log.next_offset(), target_len);
        assert_eq!(target.log.dead_letter_move_offset(move_id), Some(offset));
        let message = target
            .log
            .read_message("events.dead-letter", offset)
            .unwrap();
        assert_eq!(message.key.as_deref(), Some("order-1"));
        assert_eq!(message.payload, b"poison");
    }

    #[test]
    fn dead_letter_move_identity_rejects_different_content_after_restart() {
        let directory = tempdir().unwrap();
        let config = BrokerConfig::default();
        let move_id = dead_letter_move_id("events", "worker", 0).unwrap();

        {
            let broker = Broker::open(directory.path(), config.clone()).unwrap();
            broker.create_stream("events.dead-letter").unwrap();
            let target = broker.get_stream("events.dead-letter").unwrap();
            let mut target = broker.lock_stream(&target).unwrap();
            assert_eq!(
                target
                    .log
                    .append_with_move_id(
                        Some("order-1".to_owned()),
                        b"poison".to_vec(),
                        move_id.clone(),
                    )
                    .unwrap(),
                0
            );
        }

        let broker = Broker::open(directory.path(), config).unwrap();
        let target = broker.get_stream("events.dead-letter").unwrap();
        let mut target = broker.lock_stream(&target).unwrap();
        assert_eq!(
            target
                .log
                .append_with_move_id(
                    Some("order-1".to_owned()),
                    b"poison".to_vec(),
                    move_id.clone(),
                )
                .unwrap(),
            0
        );
        assert!(matches!(
            target.log.append_with_move_id(
                Some("wrong-key".to_owned()),
                b"different".to_vec(),
                move_id,
            ),
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(target.log.next_offset(), 1);
    }

    #[test]
    fn public_move_id_collision_with_different_content_does_not_block_source_progress() {
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
                .publish_with_request_id("events.dead-letter", key, payload.clone(), Some(move_id))
                .unwrap();
            assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
            let source_state = load_consumer_state(directory.path(), "events", "worker").unwrap();
            assert_eq!(source_state.committed_offset, 1);
            assert_dead_letter_move_at(
                &broker,
                &dead_letter_move_id("events", "worker", 0).unwrap(),
                1,
                2,
            );

            drop(broker);
            let broker = Broker::open(directory.path(), config).unwrap();
            assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
            assert_eq!(
                broker
                    .replay("events.dead-letter", "inspector", 0)
                    .unwrap()
                    .payload,
                payload
            );
            assert_dead_letter_move_at(
                &broker,
                &dead_letter_move_id("events", "worker", 0).unwrap(),
                1,
                2,
            );
        }
    }

    #[test]
    fn same_content_public_move_id_does_not_impersonate_move_across_restart() {
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
            assert_eq!(
                broker
                    .publish_with_request_id(
                        "events.dead-letter",
                        Some("order-1".to_owned()),
                        b"poison".to_vec(),
                        Some(move_id.clone()),
                    )
                    .unwrap(),
                0
            );

            broker.fail_next_dead_letter_ack_persist();
            assert!(matches!(
                broker.poll("events", "worker"),
                Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::Interrupted
            ));
            let source_state = load_consumer_state(directory.path(), "events", "worker").unwrap();
            assert_eq!(source_state.committed_offset, 0);
            assert_eq!(source_state.delivery_attempts.get(&0), Some(&1));
            assert_dead_letter_move_at(&broker, &move_id, 1, 2);
            let public = broker.replay("events.dead-letter", "inspector", 0).unwrap();
            assert_eq!(public.key.as_deref(), Some("order-1"));
            assert_eq!(public.payload, b"poison");
        }

        {
            let broker = Broker::open(directory.path(), config.clone()).unwrap();
            assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
            let source_state = load_consumer_state(directory.path(), "events", "worker").unwrap();
            assert_eq!(source_state.committed_offset, 1);
            assert!(source_state.delivery_attempts.is_empty());
            assert_dead_letter_move_at(&broker, &move_id, 1, 2);
            assert!(matches!(
                broker.publish_with_request_id(
                    "events.dead-letter",
                    Some("changed-key".to_owned()),
                    b"changed payload".to_vec(),
                    Some(move_id.clone()),
                ),
                Err(BrokerError::RequestIdContentConflict)
            ));
        }

        let broker = Broker::open(directory.path(), config).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        assert_dead_letter_move_at(&broker, &move_id, 1, 2);
        let public = broker.replay("events.dead-letter", "inspector", 0).unwrap();
        assert_eq!(public.payload, b"poison");
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
    fn acknowledgement_journal_open_failure_preserves_progress_across_restart() {
        let directory = tempdir().unwrap();
        let journal_path = directory.path().join("consumers/events/worker.json.tmp");
        let saved_journal_path = directory.path().join("consumers/events/worker.json.saved");
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

        // Replacing the journal with a directory makes opening it for append fail
        // deterministically, after the delivery attempt itself is durable.
        fs::rename(&journal_path, &saved_journal_path).unwrap();
        fs::create_dir(&journal_path).unwrap();
        let acknowledgement = broker.ack("events", "worker", 0);
        fs::remove_dir(&journal_path).unwrap();
        fs::rename(&saved_journal_path, &journal_path).unwrap();

        assert!(matches!(
            acknowledgement,
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::IsADirectory
        ));
        assert!(matches!(
            broker.poll("events", "worker").unwrap(),
            PollResult::Message(Message {
                offset: 0,
                delivery_attempt: Some(1),
                ..
            })
        ));
        drop(broker);

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
            broker.ack("events", "worker", 0).unwrap(),
            AckResult::Acknowledged
        );
        drop(broker);

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
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
    fn unsupported_consumer_checkpoint_fails_closed_without_mutation() {
        let directory = tempdir().unwrap();
        let checkpoint_path = directory.path().join("consumers/events/worker.json");
        fs::create_dir_all(checkpoint_path.parent().unwrap()).unwrap();
        let unsupported = br#"{"stream":"events","consumer":"worker","committed_offset":0,"acknowledged_offsets":[],"delivery_attempts":{},"policy":null,"delivery_policies":{}}"#;
        fs::write(&checkpoint_path, unsupported).unwrap();

        assert!(load_consumer_state(directory.path(), "events", "worker").is_err());
        assert_eq!(fs::read(&checkpoint_path).unwrap(), unsupported);
        assert!(!checkpoint_path.with_extension("json.tmp").exists());
    }

    #[test]
    fn unsupported_consumer_journal_checkpoint_fails_closed_without_mutation() {
        let directory = tempdir().unwrap();
        let journal_path = directory.path().join("consumers/events/worker.json.tmp");
        fs::create_dir_all(journal_path.parent().unwrap()).unwrap();
        let unsupported = br#"{"stream":"events","consumer":"worker","committed_offset":0,"acknowledged_offsets":[],"delivery_attempts":{},"policy":null,"delivery_policies":{}}"#;
        fs::write(&journal_path, unsupported).unwrap();

        assert!(load_consumer_state(directory.path(), "events", "worker").is_err());
        assert_eq!(fs::read(&journal_path).unwrap(), unsupported);
        assert!(!journal_path.with_extension("checkpoint.tmp").exists());
    }

    #[test]
    fn consumer_attempt_without_pinned_policy_fails_closed_without_mutation() {
        let directory = tempdir().unwrap();
        let checkpoint_path = directory.path().join("consumers/events/worker.json");
        fs::create_dir_all(checkpoint_path.parent().unwrap()).unwrap();
        let unsupported = br#"{"stream":"events","consumer":"worker","committed_offset":0,"acknowledged_offsets":[],"delivery_attempts":{"0":1},"policy":null,"delivery_policies":{},"retry_not_before":{}}"#;
        fs::write(&checkpoint_path, unsupported).unwrap();

        assert!(load_consumer_state(directory.path(), "events", "worker").is_err());
        assert_eq!(fs::read(&checkpoint_path).unwrap(), unsupported);
        assert!(!checkpoint_path.with_extension("json.tmp").exists());
    }

    #[tokio::test]
    async fn uncertain_batch_journal_sync_recovers_assignment_and_acknowledgement() {
        let directory = tempdir().unwrap();
        let limits = ConsumeBatchLimits {
            max_records: 2,
            max_bytes: 64 * 1024,
            max_wait_ms: 0,
        };
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            Engine::publish(&broker, "events", None, b"first".to_vec(), None)
                .await
                .unwrap();
            Engine::publish(&broker, "events", None, b"second".to_vec(), None)
                .await
                .unwrap();
            broker.fail_next_consumer_batch_event_sync();
            let error = Engine::poll_batch(&broker, "events", "worker", limits)
                .await
                .expect_err("a failed sync must leave the assignment outcome unknown");
            assert_eq!(error.outcome(), runnel_engine::BrokerErrorOutcome::Unknown);
        }

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let recovered = Engine::poll_batch(&broker, "events", "worker", limits)
            .await
            .unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|message| (message.offset, message.delivery_attempt))
                .collect::<Vec<_>>(),
            [(0, Some(2)), (1, Some(2))]
        );
        let receipts = recovered
            .iter()
            .map(|message| DeliveryReceipt {
                offset: message.offset,
                delivery_token: message.delivery_token.clone().unwrap(),
            })
            .collect::<Vec<_>>();
        broker.fail_next_consumer_batch_event_sync();
        let error = Engine::ack_batch(&broker, "events", "worker", receipts.clone())
            .await
            .expect_err("a failed acknowledgement sync must remain unknown");
        assert_eq!(error.outcome(), runnel_engine::BrokerErrorOutcome::Unknown);
        drop(broker);

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let retried = Engine::ack_batch(&broker, "events", "worker", receipts)
            .await
            .unwrap();
        assert!(retried.outcomes.iter().all(|item| matches!(
            item.outcome,
            runnel_engine::AckBatchOutcome::AlreadyConfirmed
        )));
    }

    #[tokio::test]
    async fn batch_journal_preappend_failure_is_retryable_without_advancing_attempt() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        Engine::publish(&broker, "events", None, b"work".to_vec(), None)
            .await
            .unwrap();
        broker.fail_next_consumer_batch_event_before_append();

        let error = Engine::poll_batch(
            &broker,
            "events",
            "worker",
            ConsumeBatchLimits {
                max_records: 1,
                max_bytes: 64 * 1024,
                max_wait_ms: 0,
            },
        )
        .await
        .expect_err("an injected pre-append failure should be reported");
        assert_eq!(
            error.outcome(),
            runnel_engine::BrokerErrorOutcome::Retryable
        );

        let retry = Engine::poll_batch(
            &broker,
            "events",
            "worker",
            ConsumeBatchLimits {
                max_records: 1,
                max_bytes: 64 * 1024,
                max_wait_ms: 0,
            },
        )
        .await
        .unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].delivery_attempt, Some(1));
    }

    #[tokio::test]
    async fn partial_batch_journal_appends_truncate_and_redeliver_after_restart() {
        let directory = tempdir().unwrap();
        let limits = ConsumeBatchLimits {
            max_records: 1,
            max_bytes: 64 * 1024,
            max_wait_ms: 0,
        };
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            Engine::publish(&broker, "events", None, b"work".to_vec(), None)
                .await
                .unwrap();
            broker.fail_next_consumer_batch_event_partial_append();
            let error = Engine::poll_batch(&broker, "events", "worker", limits)
                .await
                .expect_err("a partial assignment event append has an unknown outcome");
            assert_eq!(error.outcome(), runnel_engine::BrokerErrorOutcome::Unknown);
        }

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let first = Engine::poll_batch(&broker, "events", "worker", limits)
            .await
            .unwrap();
        assert_eq!(first[0].delivery_attempt, Some(1));
        broker.fail_next_consumer_batch_event_partial_append();
        let error = Engine::ack_batch(
            &broker,
            "events",
            "worker",
            vec![DeliveryReceipt {
                offset: first[0].offset,
                delivery_token: first[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .expect_err("a partial acknowledgement event append is unknown");
        assert_eq!(error.outcome(), runnel_engine::BrokerErrorOutcome::Unknown);
        drop(broker);

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let redelivered = Engine::poll_batch(&broker, "events", "worker", limits)
            .await
            .unwrap();
        assert_eq!(redelivered.len(), 1);
        assert_eq!(redelivered[0].offset, first[0].offset);
        assert_eq!(redelivered[0].delivery_attempt, Some(2));
        let acked = Engine::ack_batch(
            &broker,
            "events",
            "worker",
            vec![DeliveryReceipt {
                offset: redelivered[0].offset,
                delivery_token: redelivered[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(
            acked.outcomes[0].outcome,
            runnel_engine::AckBatchOutcome::Confirmed
        );
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

        for expected_attempt in 1..=12_000 {
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
        assert_eq!(message.delivery_attempt, Some(12_001));
    }

    #[tokio::test]
    async fn maximum_consume_batch_fits_the_bounded_consumer_journal() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        for _ in 0..runnel_engine::MAX_CONSUME_BATCH_RECORDS {
            broker.publish("events", None, b"work".to_vec()).unwrap();
        }
        let limits = ConsumeBatchLimits {
            max_records: runnel_engine::MAX_CONSUME_BATCH_RECORDS,
            max_bytes: runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
            max_wait_ms: 0,
        };
        let first = Engine::poll_batch(&broker, "events", "worker", limits)
            .await
            .unwrap();
        assert_eq!(first.len(), runnel_engine::MAX_CONSUME_BATCH_RECORDS);
        let journal_path = directory.path().join("consumers/events/worker.json.tmp");
        assert!(fs::metadata(&journal_path).unwrap().len() <= MAX_CONSUMER_STATE_JOURNAL_BYTES);
        drop(broker);

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let recovered = Engine::poll_batch(&broker, "events", "worker", limits)
            .await
            .unwrap();
        assert_eq!(recovered.len(), runnel_engine::MAX_CONSUME_BATCH_RECORDS);
        assert!(
            recovered
                .iter()
                .all(|message| message.delivery_attempt == Some(2))
        );
        assert!(fs::metadata(&journal_path).unwrap().len() <= MAX_CONSUMER_STATE_JOURNAL_BYTES);

        let acknowledged = Engine::ack_batch(
            &broker,
            "events",
            "worker",
            recovered
                .iter()
                .map(|message| DeliveryReceipt {
                    offset: message.offset,
                    delivery_token: message.delivery_token.clone().unwrap(),
                })
                .collect(),
        )
        .await
        .unwrap();
        assert_eq!(
            acknowledged.outcomes.len(),
            runnel_engine::MAX_CONSUME_BATCH_RECORDS
        );
        assert!(
            acknowledged
                .outcomes
                .iter()
                .all(|item| matches!(item.outcome, runnel_engine::AckBatchOutcome::Confirmed))
        );
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
        file.write_all(b"RNL3partial").unwrap();
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
        file.write_all(b"RNL3partial").unwrap();
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
    fn unsupported_old_stream_formats_fail_before_any_tail_is_truncated() {
        let mut rnl3_v1_frame = vec![0; RECORD_HEADER_LEN];
        rnl3_v1_frame[..4].copy_from_slice(RECORD_MAGIC);
        rnl3_v1_frame[4] = 1;
        rnl3_v1_frame[6..8].copy_from_slice(&(RECORD_HEADER_LEN as u16).to_le_bytes());

        for (old_bytes, expected_error) in [
            (
                b"RNL1 old stream data".to_vec(),
                "unsupported RNL1 stream format; this broker requires RNL3",
            ),
            (
                b"RNL2 old stream data".to_vec(),
                "unsupported RNL2 stream format; this broker requires RNL3",
            ),
            (rnl3_v1_frame, "unsupported RNL3 record version"),
        ] {
            let directory = tempdir().unwrap();
            {
                let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
                broker
                    .publish("events", None, b"complete".to_vec())
                    .unwrap();
            }

            let current_path = directory.path().join("streams/events.log");
            let mut current = OpenOptions::new().append(true).open(&current_path).unwrap();
            current.write_all(&RECORD_MAGIC[..3]).unwrap();
            current.sync_all().unwrap();
            let current_bytes = fs::read(&current_path).unwrap();

            let old_path = directory.path().join("streams/old.log");
            fs::write(&old_path, &old_bytes).unwrap();

            let error = match Broker::open(directory.path(), BrokerConfig::default()) {
                Err(BrokerError::Io(error)) => error,
                Err(error) => panic!("expected unsupported old stream artifact, got {error}"),
                Ok(_) => panic!("expected unsupported old stream artifact to fail startup"),
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(error.to_string(), expected_error);
            assert_eq!(fs::read(&old_path).unwrap(), old_bytes);
            assert_eq!(fs::read(&current_path).unwrap(), current_bytes);
        }
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
        file.write_all(RECORD_MAGIC).unwrap();
        file.sync_all().unwrap();

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), complete_len);
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

    #[test]
    fn current_record_writer_round_trips_ordinary_and_request_id_records() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            assert_eq!(
                broker
                    .publish("events", None, b"ordinary".to_vec())
                    .unwrap(),
                0
            );
            assert_eq!(
                broker
                    .publish_with_request_id(
                        "events",
                        Some("order-1".to_owned()),
                        b"request-aware".to_vec(),
                        Some("request-1".to_owned()),
                    )
                    .unwrap(),
                1
            );
        }

        let path = directory.path().join("streams/events.log");
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..4], RECORD_MAGIC);
        assert_eq!(bytes[4], RECORD_FORMAT_VERSION);
        assert_eq!(bytes[5], 2, "ordinary publishes have no request identity");
        assert_eq!(u32::from_le_bytes(bytes[36..40].try_into().unwrap()), 0);
        let second_record = RECORD_HEADER_LEN + b"ordinary".len();
        assert_eq!(&bytes[second_record..second_record + 4], RECORD_MAGIC);
        assert_eq!(bytes[second_record + 4], RECORD_FORMAT_VERSION);
        assert_eq!(bytes[second_record + 5], 0);
        assert_eq!(
            u32::from_le_bytes(
                bytes[second_record + 36..second_record + 40]
                    .try_into()
                    .unwrap()
            ),
            "request-1".len() as u32
        );

        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert_eq!(
            broker.replay("events", "inspector", 0).unwrap().payload,
            b"ordinary"
        );
        assert_eq!(
            broker.replay("events", "inspector", 1).unwrap().payload,
            b"request-aware"
        );
        assert_eq!(
            broker
                .publish_with_request_id(
                    "events",
                    Some("order-1".to_owned()),
                    b"request-aware".to_vec(),
                    Some("request-1".to_owned()),
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn record_reader_fails_closed_on_unknown_versions_and_identity_flags() {
        for (version, flags, expected_error) in [
            (1, 0, "unsupported RNL3 record version"),
            (3, 0, "unsupported RNL3 record version"),
            (
                RECORD_FORMAT_VERSION,
                3,
                "unsupported RNL3 record identity flag",
            ),
        ] {
            let directory = tempdir().unwrap();
            {
                let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
                broker.create_stream("events").unwrap();
            }

            let path = directory.path().join("streams/events.log");
            let mut header = [0; RECORD_HEADER_LEN];
            header[..4].copy_from_slice(RECORD_MAGIC);
            header[4] = version;
            header[5] = flags;
            header[6..8].copy_from_slice(&(RECORD_HEADER_LEN as u16).to_le_bytes());
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&header).unwrap();
            file.sync_all().unwrap();

            let error = match Broker::open(directory.path(), BrokerConfig::default()) {
                Err(BrokerError::Io(error)) => error,
                Err(error) => panic!("expected invalid stream record, got {error}"),
                Ok(_) => panic!("expected invalid stream record to fail recovery"),
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(error.to_string(), expected_error);
        }
    }

    #[test]
    fn record_identity_kind_is_covered_by_frame_checksum() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            broker
                .publish_with_request_id(
                    "events",
                    None,
                    b"payload".to_vec(),
                    Some("public-id".to_owned()),
                )
                .unwrap();
        }

        let path = directory.path().join("streams/events.log");
        let mut bytes = fs::read(&path).unwrap();
        assert_eq!(bytes[4], RECORD_FORMAT_VERSION);
        assert_eq!(bytes[5], 0);
        bytes[5] = 1;
        fs::write(&path, bytes).unwrap();

        let error = match Broker::open(directory.path(), BrokerConfig::default()) {
            Err(BrokerError::Io(error)) => error,
            Err(error) => panic!("expected identity kind mutation to fail recovery, got {error}"),
            Ok(_) => panic!("expected identity kind mutation to fail recovery"),
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "RNL3 record checksum mismatch");
    }

    #[test]
    fn record_recovery_rejects_oversized_lengths_before_allocation() {
        for (key_len, request_id_len, body_len) in [
            (MAX_KEY_LEN + 1, 0, 0),
            (0, MAX_REQUEST_ID_LEN + 1, 0),
            (0, 0, MAX_BODY_LEN + 1),
        ] {
            let directory = tempdir().unwrap();
            {
                let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
                broker.create_stream("events").unwrap();
            }
            let path = directory.path().join("streams/events.log");
            let mut header = [0; RECORD_HEADER_LEN];
            header[..4].copy_from_slice(RECORD_MAGIC);
            header[4] = RECORD_FORMAT_VERSION;
            header[5] = 0;
            header[6..8].copy_from_slice(&(RECORD_HEADER_LEN as u16).to_le_bytes());
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
    fn record_checksum_corruption_fails_recovery() {
        let directory = tempdir().unwrap();
        {
            let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
            broker.publish("events", None, b"payload".to_vec()).unwrap();
        }
        let path = directory.path().join("streams/events.log");
        let mut bytes = fs::read(&path).unwrap();
        bytes[RECORD_HEADER_LEN] ^= 1;
        fs::write(&path, bytes).unwrap();

        let result = Broker::open(directory.path(), BrokerConfig::default());
        assert!(matches!(
            result,
            Err(BrokerError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn record_batch_rejects_oversized_items_without_consuming_offsets() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.create_stream("events").unwrap();

        let outcomes = broker
            .publish_batch(
                "events",
                vec![
                    PublishRecord {
                        key: Some("k".repeat(MAX_KEY_LEN as usize + 1)),
                        payload: b"rejected".to_vec(),
                        request_id: None,
                    },
                    PublishRecord {
                        key: Some("ok".to_owned()),
                        payload: b"kept".to_vec(),
                        request_id: None,
                    },
                ],
            )
            .unwrap();

        assert_eq!(outcomes.len(), 2);
        assert!(matches!(&outcomes[0], Err(BrokerError::InvalidRecord(_))));
        assert_eq!(outcomes[1].as_ref().unwrap(), &0);
        assert!(!directory.path().join("consumers/events").exists());
        let message = broker.replay("events", "inspector", 0).unwrap();
        assert_eq!(message.key.as_deref(), Some("ok"));
        assert_eq!(message.payload, b"kept");
        assert_eq!(
            fs::metadata(directory.path().join("streams/events.log"))
                .unwrap()
                .len(),
            (RECORD_HEADER_LEN + 2 + 4) as u64
        );
    }

    #[tokio::test]
    async fn publish_batch_notifies_only_when_a_record_is_accepted() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        broker.create_stream("events").unwrap();
        let stream = broker.get_stream("events").unwrap();
        let availability = stream.lock().unwrap().availability.clone();
        let notified = availability.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let outcomes = broker
            .publish_batch(
                "events",
                vec![PublishRecord {
                    key: Some("k".repeat(MAX_KEY_LEN as usize + 1)),
                    payload: b"rejected".to_vec(),
                    request_id: None,
                }],
            )
            .unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [Err(BrokerError::InvalidRecord(_))]
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(5), notified.as_mut())
                .await
                .is_err()
        );

        let notified = availability.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let outcomes = broker
            .publish_batch(
                "events",
                vec![
                    PublishRecord {
                        key: Some("k".repeat(MAX_KEY_LEN as usize + 1)),
                        payload: b"rejected".to_vec(),
                        request_id: None,
                    },
                    PublishRecord {
                        key: None,
                        payload: b"accepted".to_vec(),
                        request_id: None,
                    },
                ],
            )
            .unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [Err(BrokerError::InvalidRecord(_)), Ok(0)]
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), notified.as_mut())
                .await
                .is_ok()
        );
    }
}
