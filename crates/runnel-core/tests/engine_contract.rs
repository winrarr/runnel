use runnel_core::{Broker, BrokerConfig};
use runnel_engine::{AckResult, BrokerError, Engine, PollResult};
use runnel_test_support::{
    assert_expired_delivery_is_fenced, assert_independent_consumers_contract,
    assert_key_ordering_contract, assert_shared_delivery_contract,
};
use tempfile::tempdir;

#[tokio::test]
async fn local_broker_implements_the_engine_contract() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let engine: &dyn Engine = &broker;

    assert!(engine.create_stream("events").await.unwrap());
    assert_eq!(
        engine
            .publish("events", None, b"hello".to_vec(), None)
            .await
            .unwrap(),
        0
    );

    let message = engine.poll("events", "worker").await.unwrap();
    assert!(matches!(
        message,
        PollResult::Message(message) if message.offset == 0 && message.payload == b"hello"
    ));

    assert_eq!(
        engine.ack("events", "worker", 0).await.unwrap(),
        AckResult::Acknowledged
    );
    assert_eq!(
        engine.poll("events", "worker").await.unwrap(),
        PollResult::Empty
    );
}

#[tokio::test]
async fn local_engine_propagates_storage_errors() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let engine: &dyn Engine = &broker;

    assert!(matches!(
        engine.poll("missing", "worker").await,
        Err(BrokerError::StreamNotFound(stream)) if stream == "missing"
    ));
    assert!(matches!(
        engine.create_stream("invalid/name").await,
        Err(BrokerError::InvalidName { kind: "stream", name }) if name == "invalid/name"
    ));
}

#[tokio::test]
async fn local_engine_ack_recovery_remains_durable_across_restart() {
    let directory = tempdir().unwrap();
    {
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let engine: &dyn Engine = &broker;
        assert_eq!(
            engine
                .publish("events", None, b"recover-me".to_vec(), None)
                .await
                .unwrap(),
            0
        );
        assert!(matches!(
            engine.poll("events", "worker").await.unwrap(),
            PollResult::Message(message) if message.offset == 0 && message.delivery_attempt == Some(1)
        ));
    }

    {
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        let engine: &dyn Engine = &broker;
        let message = match engine.poll("events", "worker").await.unwrap() {
            PollResult::Message(message) => message,
            PollResult::Empty => panic!("expected unacknowledged message after restart"),
        };
        assert_eq!(message.offset, 0);
        assert_eq!(message.delivery_attempt, Some(2));
        assert_eq!(
            engine
                .ack("events", "worker", message.offset)
                .await
                .unwrap(),
            AckResult::Acknowledged
        );
    }

    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let engine: &dyn Engine = &broker;
    assert_eq!(
        engine.poll("events", "worker").await.unwrap(),
        PollResult::Empty
    );
}

#[tokio::test]
async fn local_broker_implements_shared_delivery_contract() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let engine: &dyn Engine = &broker;

    assert_shared_delivery_contract(engine).await;
}

#[tokio::test]
async fn local_broker_keeps_independent_consumers_independent() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let engine: &dyn Engine = &broker;

    assert_independent_consumers_contract(engine).await;
}

#[tokio::test]
async fn local_broker_preserves_key_ordering_for_shared_consumers() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let engine: &dyn Engine = &broker;

    assert_key_ordering_contract(engine).await;
}

#[tokio::test]
async fn local_broker_fences_expired_shared_deliveries() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(
        directory.path(),
        BrokerConfig {
            ack_timeout: std::time::Duration::from_millis(100),
            max_delivery_attempts: None,
        },
    )
    .unwrap();
    let engine: &dyn Engine = &broker;

    assert_expired_delivery_is_fenced(engine, std::time::Duration::from_millis(250)).await;
}

#[tokio::test]
async fn local_grouped_delivery_survives_restart_and_member_replacement() {
    let directory = tempdir().unwrap();
    let config = BrokerConfig {
        ack_timeout: std::time::Duration::from_secs(60),
        max_delivery_attempts: None,
    };
    let old_token = {
        let broker = Broker::open(directory.path(), config.clone()).unwrap();
        broker
            .publish("jobs", Some("order-1".to_owned()), b"recover-me".to_vec())
            .unwrap();
        match broker.poll_group("jobs", "workers", "member-a").unwrap() {
            PollResult::Message(message) => {
                assert_eq!(message.delivery_attempt, Some(1));
                message.delivery_token.unwrap()
            }
            PollResult::Empty => panic!("expected grouped delivery"),
        }
    };

    let broker = Broker::open(directory.path(), config).unwrap();
    let new_token = match broker.poll_group("jobs", "workers", "member-b").unwrap() {
        PollResult::Message(message) => {
            assert_eq!(message.delivery_attempt, Some(2));
            assert_ne!(message.delivery_token.as_deref(), Some(old_token.as_str()));
            message.delivery_token.unwrap()
        }
        PollResult::Empty => panic!("expected reassigned grouped delivery"),
    };
    assert!(matches!(
        broker.ack_group("jobs", "workers", "member-a", 0, &old_token),
        Err(runnel_engine::BrokerError::StaleDelivery { .. })
    ));
    assert_eq!(
        broker
            .ack_group("jobs", "workers", "member-b", 0, &new_token)
            .unwrap(),
        AckResult::Acknowledged
    );
}
