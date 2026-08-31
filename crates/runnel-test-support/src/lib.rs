use std::time::Duration;

use runnel_engine::{AckResult, BrokerError, Engine, PollResult, PublishRecord};

pub async fn assert_publish_batch_contract(engine: &dyn Engine) {
    assert!(engine.create_stream("contract.batch").await.unwrap());
    let records = vec![
        PublishRecord {
            key: Some("order-a".to_owned()),
            payload: b"first".to_vec(),
            request_id: Some("batch-first".to_owned()),
        },
        PublishRecord {
            key: Some("order-a".to_owned()),
            payload: vec![0, 1, 255],
            request_id: Some("batch-second".to_owned()),
        },
        PublishRecord {
            key: None,
            payload: b"third".to_vec(),
            request_id: None,
        },
    ];
    assert_eq!(
        engine
            .publish_batch("contract.batch", records.clone())
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0, 1, 2]
    );
    assert_eq!(
        engine
            .publish_batch("contract.batch", records[..2].to_vec())
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0, 1]
    );
}

pub async fn assert_shared_delivery_contract(engine: &dyn Engine) {
    assert!(engine.create_stream("contract.work").await.unwrap());
    for payload in [
        b"first".as_slice(),
        b"second".as_slice(),
        b"third".as_slice(),
    ] {
        engine
            .publish("contract.work", None, payload.to_vec(), None)
            .await
            .unwrap();
    }

    let (first_offset, first_token) = grouped_message(
        engine
            .poll_group("contract.work", "workers", "member-a")
            .await,
    );
    let (second_offset, second_token) = grouped_message(
        engine
            .poll_group("contract.work", "workers", "member-b")
            .await,
    );
    assert_eq!((first_offset, second_offset), (0, 1));

    assert_eq!(
        engine
            .ack_group(
                "contract.work",
                "workers",
                "member-b",
                second_offset,
                &second_token,
            )
            .await
            .unwrap(),
        AckResult::Acknowledged
    );
    let (third_offset, third_token) = grouped_message(
        engine
            .poll_group("contract.work", "workers", "member-b")
            .await,
    );
    assert_eq!(third_offset, 2);

    assert_eq!(
        engine
            .ack_group(
                "contract.work",
                "workers",
                "member-a",
                first_offset,
                &first_token,
            )
            .await
            .unwrap(),
        AckResult::Acknowledged
    );
    assert_eq!(
        engine
            .ack_group(
                "contract.work",
                "workers",
                "member-b",
                third_offset,
                &third_token,
            )
            .await
            .unwrap(),
        AckResult::Acknowledged
    );
    assert_eq!(
        engine
            .poll_group("contract.work", "workers", "member-a")
            .await
            .unwrap(),
        PollResult::Empty
    );
}

pub async fn assert_independent_consumers_contract(engine: &dyn Engine) {
    assert!(engine.create_stream("contract.fanout").await.unwrap());
    engine
        .publish("contract.fanout", None, b"event".to_vec(), None)
        .await
        .unwrap();

    let first = engine.poll("contract.fanout", "consumer-a").await.unwrap();
    assert!(matches!(
        first,
        PollResult::Message(message) if message.offset == 0 && message.payload == b"event"
    ));
    assert_eq!(
        engine
            .ack("contract.fanout", "consumer-a", 0)
            .await
            .unwrap(),
        AckResult::Acknowledged
    );

    let second = engine.poll("contract.fanout", "consumer-b").await.unwrap();
    assert!(matches!(
        second,
        PollResult::Message(message) if message.offset == 0 && message.payload == b"event"
    ));
}

pub async fn assert_key_ordering_contract(engine: &dyn Engine) {
    assert!(engine.create_stream("contract.keys").await.unwrap());
    for (key, payload) in [
        ("customer-a", b"a1".as_slice()),
        ("customer-a", b"a2".as_slice()),
        ("customer-b", b"b1".as_slice()),
    ] {
        engine
            .publish(
                "contract.keys",
                Some(key.to_owned()),
                payload.to_vec(),
                None,
            )
            .await
            .unwrap();
    }

    let (first_offset, first_token) = grouped_message(
        engine
            .poll_group("contract.keys", "workers", "member-a")
            .await,
    );
    let (other_offset, other_token) = grouped_message(
        engine
            .poll_group("contract.keys", "workers", "member-b")
            .await,
    );
    assert_eq!(first_offset, 0);
    assert_eq!(other_offset, 2);

    let (same_offset, same_token) = grouped_message(
        engine
            .poll_group("contract.keys", "workers", "member-a")
            .await,
    );
    assert_eq!(same_offset, first_offset);
    assert_eq!(same_token, first_token);

    assert_eq!(
        engine
            .ack_group(
                "contract.keys",
                "workers",
                "member-a",
                first_offset,
                &first_token,
            )
            .await
            .unwrap(),
        AckResult::Acknowledged
    );
    assert_eq!(
        engine
            .ack_group(
                "contract.keys",
                "workers",
                "member-b",
                other_offset,
                &other_token,
            )
            .await
            .unwrap(),
        AckResult::Acknowledged
    );

    let (next_offset, next_token) = grouped_message(
        engine
            .poll_group("contract.keys", "workers", "member-b")
            .await,
    );
    assert_eq!(next_offset, 1);
    assert_eq!(
        engine
            .ack_group(
                "contract.keys",
                "workers",
                "member-b",
                next_offset,
                &next_token,
            )
            .await
            .unwrap(),
        AckResult::Acknowledged
    );
}

pub async fn assert_expired_delivery_is_fenced(engine: &dyn Engine, expiration: Duration) {
    assert!(engine.create_stream("contract.expiry").await.unwrap());
    engine
        .publish("contract.expiry", None, b"work".to_vec(), None)
        .await
        .unwrap();

    let (offset, old_token) = grouped_message(
        engine
            .poll_group("contract.expiry", "workers", "member-a")
            .await,
    );
    tokio::time::sleep(expiration).await;
    let (redelivered_offset, new_token) = grouped_message(
        engine
            .poll_group("contract.expiry", "workers", "member-b")
            .await,
    );
    assert_eq!(redelivered_offset, offset);
    assert_ne!(new_token, old_token);

    assert!(matches!(
        engine
            .ack_group("contract.expiry", "workers", "member-a", offset, &old_token,)
            .await,
        Err(BrokerError::StaleDelivery { .. })
    ));
    assert_eq!(
        engine
            .ack_group("contract.expiry", "workers", "member-b", offset, &new_token,)
            .await
            .unwrap(),
        AckResult::Acknowledged
    );
}

fn grouped_message(result: Result<PollResult, BrokerError>) -> (u64, String) {
    match result.unwrap() {
        PollResult::Message(message) => (
            message.offset,
            message
                .delivery_token
                .expect("grouped delivery should include a token"),
        ),
        PollResult::Empty => panic!("expected a grouped message"),
    }
}
