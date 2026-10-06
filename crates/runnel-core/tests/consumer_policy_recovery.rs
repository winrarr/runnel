use std::time::Duration;

use runnel_core::{Broker, BrokerConfig, PollResult};
use runnel_engine::{AckBatchOutcome, ConsumeBatchLimits, DeliveryReceipt, Engine};
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
            .configure_consumer("events", "worker", 60_000, Some(2))
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
            .configure_consumer("events", "worker", 0, Some(1))
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
            Engine::configure_consumer(&broker, "events", "workers", 60_000, Some(2))
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

        let updated_policy = Engine::configure_consumer(&broker, "events", "workers", 0, Some(1))
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
