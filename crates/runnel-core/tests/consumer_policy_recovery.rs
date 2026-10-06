use std::time::Duration;

use runnel_core::{Broker, BrokerConfig, PollResult};
use runnel_engine::{AckBatchOutcome, BrokerError, ConsumeBatchLimits, DeliveryReceipt, Engine};
use tempfile::tempdir;

#[test]
fn attempted_delivery_keeps_its_policy_snapshot_after_restart_and_update() {
    let directory = tempdir().unwrap();
    let config = BrokerConfig {
        ack_timeout: Duration::ZERO,
        max_delivery_attempts: None,
    };

    {
        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        broker.create_stream("events").unwrap();
        let original_policy = broker
            .configure_consumer("events", "worker", 60_000, Some(2), 0)
            .unwrap();
        assert_eq!(original_policy.version, 1);

        broker
            .publish("events", None, b"retry-me".to_vec())
            .unwrap();
        match broker.poll("events", "worker").unwrap() {
            PollResult::Message(message) => {
                assert_eq!(message.offset, 0);
                assert_eq!(message.delivery_attempt, Some(1));
            }
            PollResult::Empty => panic!("expected first delivery"),
        }

        let updated_policy = broker
            .configure_consumer("events", "worker", 0, Some(1), 0)
            .unwrap();
        assert_eq!(updated_policy.version, 2);
        assert_eq!(updated_policy.ack_timeout_ms, 0);
        assert_eq!(updated_policy.max_delivery_attempts, Some(1));
    }

    let reopened = Broker::open(directory.path(), config).unwrap();
    let persisted_policy = reopened.inspect_consumer("events", "worker").unwrap();
    assert_eq!(persisted_policy.version, 2);
    assert_eq!(persisted_policy.ack_timeout_ms, 0);
    assert_eq!(persisted_policy.max_delivery_attempts, Some(1));

    let second_attempt = match reopened.poll("events", "worker").unwrap() {
        PollResult::Message(message) => message,
        PollResult::Empty => panic!("updated policy must not dead-letter the attempted offset"),
    };
    assert_eq!(second_attempt.offset, 0);
    assert_eq!(second_attempt.delivery_attempt, Some(2));

    let same_attempt = match reopened.poll("events", "worker").unwrap() {
        PollResult::Message(message) => message,
        PollResult::Empty => {
            panic!("the pinned acknowledgement timeout must keep attempt two in flight")
        }
    };
    assert_eq!(same_attempt.offset, 0);
    assert_eq!(same_attempt.delivery_attempt, Some(2));
    assert_eq!(same_attempt.delivery_token, second_attempt.delivery_token);
    assert_eq!(reopened.health().unwrap().dead_letters, 0);
}

#[tokio::test]
async fn batch_delivery_keeps_its_policy_snapshot_after_restart_and_update() {
    let directory = tempdir().unwrap();
    let config = BrokerConfig {
        ack_timeout: Duration::ZERO,
        max_delivery_attempts: None,
    };
    let limits = ConsumeBatchLimits {
        max_records: 1,
        max_bytes: 64 * 1024,
        max_wait_ms: 0,
    };
    {
        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        Engine::create_stream(&broker, "events").await.unwrap();
        let original_policy =
            Engine::configure_consumer(&broker, "events", "workers", 60_000, Some(2), 0)
                .await
                .unwrap();
        assert_eq!(original_policy.version, 1);
        Engine::publish(&broker, "events", None, b"retry-me".to_vec(), None)
            .await
            .unwrap();
        let first_attempt =
            Engine::poll_group_batch(&broker, "events", "workers", "member-a", limits)
                .await
                .unwrap();
        assert_eq!(first_attempt.len(), 1);
        assert_eq!(first_attempt[0].delivery_attempt, Some(1));

        let updated_policy =
            Engine::configure_consumer(&broker, "events", "workers", 0, Some(1), 0)
                .await
                .unwrap();
        assert_eq!(updated_policy.version, 2);
    }

    let reopened = Broker::open(directory.path(), config).unwrap();
    let persisted_policy = Engine::inspect_consumer(&reopened, "events", "workers")
        .await
        .unwrap();
    assert_eq!(persisted_policy.version, 2);
    assert_eq!(persisted_policy.ack_timeout_ms, 0);
    assert_eq!(persisted_policy.max_delivery_attempts, Some(1));

    let second_attempt =
        Engine::poll_group_batch(&reopened, "events", "workers", "member-b", limits)
            .await
            .unwrap();
    assert_eq!(second_attempt.len(), 1);
    assert_eq!(second_attempt[0].offset, 0);
    assert_eq!(second_attempt[0].delivery_attempt, Some(2));
    assert_eq!(reopened.health().unwrap().dead_letters, 0);

    let acknowledgement = Engine::ack_group_batch(
        &reopened,
        "events",
        "workers",
        "member-b",
        vec![DeliveryReceipt {
            offset: second_attempt[0].offset,
            delivery_token: second_attempt[0].delivery_token.clone().unwrap(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(
        acknowledgement.outcomes[0].outcome,
        AckBatchOutcome::Confirmed
    );
}

#[test]
fn retry_delay_reserves_its_key_but_allows_unrelated_work() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(
        directory.path(),
        BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: None,
        },
    )
    .unwrap();
    broker.create_stream("events").unwrap();
    broker
        .configure_consumer("events", "workers", 0, None, 500)
        .unwrap();
    broker
        .publish("events", Some("same".to_owned()), b"first".to_vec())
        .unwrap();
    broker
        .publish("events", Some("same".to_owned()), b"successor".to_vec())
        .unwrap();
    broker
        .publish("events", Some("other".to_owned()), b"unrelated".to_vec())
        .unwrap();

    let first = match broker.poll_group("events", "workers", "member-a").unwrap() {
        PollResult::Message(message) => message,
        PollResult::Empty => panic!("expected first delivery"),
    };
    assert!(matches!(
        broker.ack_group(
            "events",
            "workers",
            "member-a",
            first.offset,
            first.delivery_token.as_deref().unwrap(),
        ),
        Err(BrokerError::StaleDelivery { .. })
    ));

    let unrelated = match broker.poll_group("events", "workers", "member-b").unwrap() {
        PollResult::Message(message) => message,
        PollResult::Empty => panic!("unrelated key should remain eligible during retry delay"),
    };
    assert_eq!(unrelated.offset, 2);
    assert_eq!(
        broker.poll_group("events", "workers", "member-c").unwrap(),
        PollResult::Empty
    );
}

#[tokio::test]
async fn observed_retry_delay_survives_restart_without_restarting_its_clock() {
    let directory = tempdir().unwrap();
    let config = BrokerConfig {
        ack_timeout: Duration::ZERO,
        max_delivery_attempts: None,
    };
    let delay = Duration::from_millis(300);
    {
        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        broker.create_stream("events").unwrap();
        broker
            .configure_consumer("events", "worker", 0, None, delay.as_millis() as u64)
            .unwrap();
        broker
            .publish("events", None, b"retry-me".to_vec())
            .unwrap();
        let first = match broker.poll_group("events", "worker", "member-a").unwrap() {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected first delivery"),
        };
        assert!(matches!(
            broker.ack_group(
                "events",
                "worker",
                "member-a",
                first.offset,
                first.delivery_token.as_deref().unwrap(),
            ),
            Err(BrokerError::StaleDelivery { .. })
        ));
        assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let broker = Broker::open(directory.path(), config).unwrap();
    assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
    tokio::time::sleep(Duration::from_millis(230)).await;
    let retry = match broker.poll("events", "worker").unwrap() {
        PollResult::Message(message) => message,
        PollResult::Empty => {
            panic!("persisted retry delay should expire from its first observation")
        }
    };
    assert_eq!(retry.offset, 0);
    assert_eq!(retry.delivery_attempt, Some(2));
}

#[tokio::test]
async fn batch_expiry_persists_retry_delay_before_retry_assignment() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(
        directory.path(),
        BrokerConfig {
            ack_timeout: Duration::ZERO,
            max_delivery_attempts: None,
        },
    )
    .unwrap();
    Engine::create_stream(&broker, "events").await.unwrap();
    Engine::configure_consumer(&broker, "events", "workers", 0, None, 250)
        .await
        .unwrap();
    Engine::publish(&broker, "events", None, b"retry-me".to_vec(), None)
        .await
        .unwrap();
    let limits = ConsumeBatchLimits {
        max_records: 1,
        max_bytes: 64 * 1024,
        max_wait_ms: 0,
    };
    let first = Engine::poll_group_batch(&broker, "events", "workers", "member-a", limits)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].delivery_attempt, Some(1));

    let acknowledgement = Engine::ack_group_batch(
        &broker,
        "events",
        "workers",
        "member-a",
        vec![DeliveryReceipt {
            offset: first[0].offset,
            delivery_token: first[0].delivery_token.clone().unwrap(),
        }],
    )
    .await
    .unwrap();
    assert!(matches!(
        acknowledgement.outcomes[0].outcome,
        AckBatchOutcome::Rejected {
            reason: runnel_engine::AckBatchRejection::StaleDelivery
        }
    ));
    assert!(
        Engine::poll_group_batch(&broker, "events", "workers", "member-b", limits)
            .await
            .unwrap()
            .is_empty()
    );
    tokio::time::sleep(Duration::from_millis(270)).await;
    let retry = Engine::poll_group_batch(&broker, "events", "workers", "member-b", limits)
        .await
        .unwrap();
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].offset, 0);
    assert_eq!(retry[0].delivery_attempt, Some(2));
}

#[tokio::test]
async fn restart_without_durable_expiry_observation_starts_a_full_delay() {
    let directory = tempdir().unwrap();
    let config = BrokerConfig {
        ack_timeout: Duration::ZERO,
        max_delivery_attempts: None,
    };
    {
        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        broker.create_stream("events").unwrap();
        broker
            .configure_consumer("events", "worker", 60_000, None, 160)
            .unwrap();
        broker
            .publish("events", None, b"retry-me".to_vec())
            .unwrap();
        assert!(matches!(
            broker.poll_group("events", "worker", "member-a").unwrap(),
            PollResult::Message(message) if message.delivery_attempt == Some(1)
        ));
    }

    let broker = Broker::open(directory.path(), config).unwrap();
    assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(broker.poll("events", "worker").unwrap(), PollResult::Empty);
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert!(matches!(
        broker.poll("events", "worker").unwrap(),
        PollResult::Message(message) if message.delivery_attempt == Some(2)
    ));
}
