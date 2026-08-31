use runnel_core::{Broker, BrokerConfig};
use runnel_engine::{AckResult, BrokerError, Engine, PollResult};
use runnel_test_support::{
    assert_expired_delivery_is_fenced, assert_independent_consumers_contract,
    assert_key_ordering_contract, assert_publish_batch_contract, assert_shared_delivery_contract,
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
async fn local_broker_implements_publish_batch_contract() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    assert_publish_batch_contract(&broker).await;
}

#[tokio::test]
async fn local_batch_reports_record_rejection_without_losing_later_records() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let outcomes = broker
        .publish_batch(
            "events",
            vec![
                runnel_engine::PublishRecord {
                    key: None,
                    payload: b"rejected".to_vec(),
                    request_id: Some("x".repeat(1_025)),
                },
                runnel_engine::PublishRecord {
                    key: None,
                    payload: b"accepted".to_vec(),
                    request_id: Some("accepted".to_owned()),
                },
            ],
        )
        .unwrap();
    assert!(matches!(
        outcomes.first(),
        Some(Err(BrokerError::Io(error))) if error.kind() == std::io::ErrorKind::InvalidInput
    ));
    assert!(matches!(outcomes.get(1), Some(Ok(0))));
    assert!(matches!(
        broker.poll("events", "worker").unwrap(),
        PollResult::Message(message) if message.offset == 0 && message.payload == b"accepted"
    ));
}

#[tokio::test]
async fn local_batch_request_id_deduplication_survives_restart() {
    let directory = tempdir().unwrap();
    let records = || {
        vec![
            runnel_engine::PublishRecord {
                key: Some("first".to_owned()),
                payload: b"original-first".to_vec(),
                request_id: Some("batch-first".to_owned()),
            },
            runnel_engine::PublishRecord {
                key: None,
                payload: b"original-second".to_vec(),
                request_id: Some("batch-second".to_owned()),
            },
        ]
    };
    {
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        assert!(
            broker
                .publish_batch("events", records())
                .unwrap()
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                == vec![0, 1]
        );
    }

    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let retried = broker
        .publish_batch(
            "events",
            vec![
                runnel_engine::PublishRecord {
                    key: Some("changed".to_owned()),
                    payload: b"changed-first".to_vec(),
                    request_id: Some("batch-first".to_owned()),
                },
                runnel_engine::PublishRecord {
                    key: None,
                    payload: b"changed-second".to_vec(),
                    request_id: Some("batch-second".to_owned()),
                },
            ],
        )
        .unwrap();
    assert!(matches!(retried.as_slice(), [Ok(0), Ok(1)]));
    assert!(matches!(
        broker.poll("events", "worker").unwrap(),
        PollResult::Message(message) if message.offset == 0 && message.payload == b"original-first"
    ));
    assert!(matches!(
        broker.ack("events", "worker", 0).unwrap(),
        AckResult::Acknowledged
    ));
    assert!(matches!(
        broker.poll("events", "worker").unwrap(),
        PollResult::Message(message) if message.offset == 1 && message.payload == b"original-second"
    ));
}

#[tokio::test]
async fn local_engine_rejects_publish_batch_over_record_bound() {
    let directory = tempdir().unwrap();
    let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
    let records = (0..=runnel_engine::MAX_PUBLISH_BATCH_RECORDS)
        .map(|_| runnel_engine::PublishRecord {
            key: None,
            payload: Vec::new(),
            request_id: None,
        })
        .collect();
    assert!(matches!(
        broker.publish_batch("events", records),
        Err(BrokerError::Configuration(message)) if message.contains("more than")
    ));
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
