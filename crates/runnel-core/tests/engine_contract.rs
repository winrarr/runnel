use runnel_core::{Broker, BrokerConfig};
use runnel_engine::{AckResult, Engine, PollResult};
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
