use std::time::Duration;

use runnel_engine::{
    AckBatchOutcome, AckResult, BrokerError, BrokerErrorKind, BrokerErrorOutcome,
    ConsumeBatchLimits, DeliveryReceipt, Engine, PollResult, PublishRecord,
    poll_batch_response_len,
};

/// Verify the semantic error boundary shared by local and distributed engines.
///
/// The assertions intentionally use public engine operations rather than
/// backend-specific helpers. Concrete error values remain available for
/// diagnostics, while callers can make the same retry decision from the
/// stable kind/outcome pair.
pub async fn assert_error_classification_contract(engine: &dyn Engine) {
    let invalid_name = engine
        .create_stream("invalid/name")
        .await
        .expect_err("invalid stream names must be rejected");
    assert_eq!(invalid_name.kind(), BrokerErrorKind::InvalidRequest);
    assert_eq!(invalid_name.outcome(), BrokerErrorOutcome::Rejected);

    let missing_stream = engine
        .poll("missing", "worker")
        .await
        .expect_err("missing streams must be reported");
    assert_eq!(missing_stream.kind(), BrokerErrorKind::ResourceNotFound);
    assert_eq!(missing_stream.outcome(), BrokerErrorOutcome::Rejected);

    assert!(engine.create_stream("contract.errors").await.unwrap());

    engine
        .publish("contract.errors", None, b"work".to_vec(), None)
        .await
        .expect("the error contract should have a delivery to validate");
    assert!(matches!(
        engine.poll("contract.errors", "worker").await,
        Ok(PollResult::Message(message)) if message.offset == 0
    ));

    let missing_delivery = engine
        .ack("contract.errors", "worker", 1)
        .await
        .expect_err("acknowledging an unknown delivery must be rejected");
    assert_eq!(missing_delivery.kind(), BrokerErrorKind::DeliveryRejected);
    assert_eq!(missing_delivery.outcome(), BrokerErrorOutcome::Rejected);

    let unavailable_history = engine
        .replay("contract.errors", "worker", 1)
        .await
        .expect_err("replaying beyond retained history must report unavailability");
    assert_eq!(
        unavailable_history.kind(),
        BrokerErrorKind::HistoryUnavailable
    );
    assert_eq!(unavailable_history.outcome(), BrokerErrorOutcome::Rejected);
}

pub async fn assert_replay_contract(engine: &dyn Engine) {
    assert!(engine.create_stream("contract.replay").await.unwrap());
    for payload in [b"first".as_slice(), b"second".as_slice()] {
        engine
            .publish("contract.replay", None, payload.to_vec(), None)
            .await
            .unwrap();
    }

    let replayed = engine.replay("contract.replay", "worker", 0).await.unwrap();
    assert_eq!(replayed.stream, "contract.replay");
    assert_eq!(replayed.offset, 0);
    assert_eq!(replayed.payload, b"first");
    assert_eq!(replayed.key, None);

    let ordinary = engine.poll("contract.replay", "worker").await.unwrap();
    assert!(matches!(
        ordinary,
        PollResult::Message(message) if message.offset == 0 && message.payload == b"first"
    ));
    assert!(matches!(
        engine.ack("contract.replay", "worker", 0).await,
        Ok(AckResult::Acknowledged)
    ));

    let replayed_acknowledged = engine.replay("contract.replay", "worker", 0).await.unwrap();
    assert_eq!(replayed_acknowledged.offset, 0);
    assert_eq!(replayed_acknowledged.payload, b"first");
    assert!(matches!(
        engine.poll("contract.replay", "worker").await.unwrap(),
        PollResult::Message(message) if message.offset == 1 && message.payload == b"second"
    ));
    assert!(matches!(
        engine.replay("contract.replay", "worker", 2).await,
        Err(BrokerError::HistoryUnavailable {
            stream,
            requested_offset: 2,
            earliest_offset: 0,
            next_offset: 2,
        }) if stream == "contract.replay"
    ));
    assert!(matches!(
        engine.replay("contract.replay", "invalid/consumer", 0).await,
        Err(BrokerError::InvalidName {
            kind: "consumer",
            name,
        }) if name == "invalid/consumer"
    ));
}

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

/// Verify the ordered active-set, receipt, mixed-result, and same-key rules
/// shared by local and clustered consume-batch implementations.
pub async fn assert_consume_batch_contract(engine: &dyn Engine) {
    let stream = "contract.consume-batch";
    assert!(engine.create_stream(stream).await.unwrap());
    for payload in [b"first".as_slice(), b"second", b"third", b"fourth"] {
        engine
            .publish(stream, None, payload.to_vec(), None)
            .await
            .unwrap();
    }

    let limits = ConsumeBatchLimits {
        max_records: 2,
        max_bytes: 64 * 1024,
        max_wait_ms: 0,
    };
    let first = engine.poll_batch(stream, "worker", limits).await.unwrap();
    assert_eq!(
        first
            .iter()
            .map(|message| message.offset)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert!(first.iter().all(|message| message.delivery_token.is_some()));
    assert_eq!(
        engine.poll_batch(stream, "worker", limits).await.unwrap(),
        first,
        "repeating a poll must recover the same active set"
    );
    let scalar_ack = engine
        .ack(stream, "worker", first[0].offset)
        .await
        .expect_err("a scalar offset-only ack must not bypass a batch receipt fence");
    assert_eq!(scalar_ack.kind(), BrokerErrorKind::DeliveryRejected);
    assert_eq!(scalar_ack.outcome(), BrokerErrorOutcome::Rejected);
    let lower_limit = engine
        .poll_batch(
            stream,
            "worker",
            ConsumeBatchLimits {
                max_records: 1,
                ..limits
            },
        )
        .await
        .expect_err("a smaller limit must reject an oversized live set");
    assert_eq!(lower_limit.kind(), BrokerErrorKind::InvalidRequest);

    let receipts = vec![
        DeliveryReceipt {
            offset: first[0].offset,
            delivery_token: first[0].delivery_token.clone().unwrap(),
        },
        DeliveryReceipt {
            offset: first[1].offset,
            delivery_token: "stale-receipt".to_owned(),
        },
    ];
    let outcome = engine
        .ack_batch(stream, "worker", receipts.clone())
        .await
        .unwrap();
    assert_eq!(outcome.outcomes.len(), 2);
    assert!(matches!(
        outcome.outcomes[0].outcome,
        AckBatchOutcome::Confirmed
    ));
    assert!(matches!(
        outcome.outcomes[1].outcome,
        AckBatchOutcome::Rejected { .. }
    ));
    let retry = engine
        .ack_batch(stream, "worker", vec![receipts[0].clone()])
        .await
        .unwrap();
    assert!(matches!(
        retry.outcomes[0].outcome,
        AckBatchOutcome::AlreadyConfirmed
    ));

    let remaining = engine.poll_batch(stream, "worker", limits).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].offset, 1);
    assert_eq!(
        engine
            .ack_batch(
                stream,
                "worker",
                vec![DeliveryReceipt {
                    offset: remaining[0].offset,
                    delivery_token: remaining[0].delivery_token.clone().unwrap(),
                }],
            )
            .await
            .unwrap()
            .outcomes[0]
            .outcome,
        AckBatchOutcome::Confirmed
    );

    let next = engine.poll_batch(stream, "worker", limits).await.unwrap();
    assert_eq!(
        next.iter()
            .map(|message| message.offset)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    let out_of_order_receipts = next
        .iter()
        .rev()
        .map(|message| DeliveryReceipt {
            offset: message.offset,
            delivery_token: message.delivery_token.clone().unwrap(),
        })
        .collect::<Vec<_>>();
    let out_of_order_ack = engine
        .ack_batch(stream, "worker", out_of_order_receipts)
        .await
        .unwrap();
    assert_eq!(
        out_of_order_ack
            .outcomes
            .iter()
            .map(|outcome| outcome.offset)
            .collect::<Vec<_>>(),
        [3, 2],
        "ack results preserve input order even when receipts complete out of order"
    );
    assert!(
        out_of_order_ack
            .outcomes
            .iter()
            .all(|item| matches!(item.outcome, AckBatchOutcome::Confirmed))
    );
    assert!(
        engine
            .poll_batch(stream, "worker", limits)
            .await
            .unwrap()
            .is_empty()
    );

    let duplicate = "contract.consume-batch-duplicate";
    assert!(engine.create_stream(duplicate).await.unwrap());
    engine
        .publish(duplicate, None, b"duplicate-check".to_vec(), None)
        .await
        .unwrap();
    let duplicate_set = engine
        .poll_batch(
            duplicate,
            "worker",
            ConsumeBatchLimits {
                max_records: 1,
                ..limits
            },
        )
        .await
        .unwrap();
    let duplicate_receipt = DeliveryReceipt {
        offset: duplicate_set[0].offset,
        delivery_token: duplicate_set[0].delivery_token.clone().unwrap(),
    };
    let invalid_vector = engine
        .ack_batch(
            duplicate,
            "worker",
            vec![duplicate_receipt.clone(), duplicate_receipt.clone()],
        )
        .await
        .expect_err("duplicate offsets must reject the vector before any ack");
    assert_eq!(invalid_vector.kind(), BrokerErrorKind::InvalidRequest);
    assert_eq!(
        engine
            .poll_batch(
                duplicate,
                "worker",
                ConsumeBatchLimits {
                    max_records: 1,
                    ..limits
                },
            )
            .await
            .unwrap(),
        duplicate_set,
        "a malformed receipt vector must not alter the active set"
    );
    assert_eq!(
        engine
            .ack_batch(duplicate, "worker", vec![duplicate_receipt])
            .await
            .unwrap()
            .outcomes[0]
            .outcome,
        AckBatchOutcome::Confirmed
    );

    let bounded = "contract.consume-batch-bounds";
    assert!(engine.create_stream(bounded).await.unwrap());
    engine
        .publish(bounded, None, b"small".to_vec(), None)
        .await
        .unwrap();
    engine
        .publish(bounded, None, vec![b'x'; 512], None)
        .await
        .unwrap();
    let empty_response_bytes = poll_batch_response_len(bounded, "worker", None, &[]);
    let byte_limits = ConsumeBatchLimits {
        max_records: 2,
        max_bytes: empty_response_bytes + 300,
        max_wait_ms: 0,
    };
    let bounded_batch = engine
        .poll_batch(bounded, "worker", byte_limits)
        .await
        .unwrap();
    assert_eq!(
        bounded_batch
            .iter()
            .map(|message| message.offset)
            .collect::<Vec<_>>(),
        [0],
        "a later record that exceeds the byte limit must leave a smaller prefix"
    );
    assert!(
        poll_batch_response_len(bounded, "worker", None, &bounded_batch) <= byte_limits.max_bytes
    );
    let live_set_bytes = poll_batch_response_len(bounded, "worker", None, &bounded_batch);
    let lower_byte_limit = engine
        .poll_batch(
            bounded,
            "worker",
            ConsumeBatchLimits {
                max_bytes: live_set_bytes - 1,
                ..byte_limits
            },
        )
        .await
        .expect_err("a smaller byte limit must reject an oversized live set");
    assert_eq!(lower_byte_limit.kind(), BrokerErrorKind::InvalidRequest);
    engine
        .ack_batch(
            bounded,
            "worker",
            vec![DeliveryReceipt {
                offset: bounded_batch[0].offset,
                delivery_token: bounded_batch[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();
    let oversized_first = engine
        .poll_batch(bounded, "worker", byte_limits)
        .await
        .expect_err("an oversized first eligible record must be rejected explicitly");
    assert_eq!(oversized_first.kind(), BrokerErrorKind::InvalidRequest);

    let waiting = "contract.consume-batch-wakeup";
    assert!(engine.create_stream(waiting).await.unwrap());
    let wait_limits = ConsumeBatchLimits {
        max_records: 2,
        max_bytes: 64 * 1024,
        max_wait_ms: 100,
    };
    let (collected, published) =
        tokio::join!(engine.poll_batch(waiting, "worker", wait_limits), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tokio::time::timeout(
                Duration::from_millis(50),
                engine.publish(waiting, None, b"wakeup".to_vec(), None),
            )
            .await
            .expect("publishing during batch collection must not wait for the poll deadline")
        });
    assert_eq!(published.unwrap(), 0);
    let collected = collected.unwrap();
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].offset, 0);
    engine
        .ack_batch(
            waiting,
            "worker",
            vec![DeliveryReceipt {
                offset: collected[0].offset,
                delivery_token: collected[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();

    let ack_wakeup = "contract.consume-batch-ack-wakeup";
    assert!(engine.create_stream(ack_wakeup).await.unwrap());
    for payload in [b"first".as_slice(), b"second".as_slice()] {
        engine
            .publish(
                ack_wakeup,
                Some("same-key".to_owned()),
                payload.to_vec(),
                None,
            )
            .await
            .unwrap();
    }
    let first_keyed = engine
        .poll_group_batch(
            ack_wakeup,
            "workers",
            "member-a",
            ConsumeBatchLimits {
                max_records: 2,
                max_wait_ms: 0,
                ..limits
            },
        )
        .await
        .unwrap();
    let (next_keyed, acked) = tokio::join!(
        engine.poll_group_batch(
            ack_wakeup,
            "workers",
            "member-b",
            ConsumeBatchLimits {
                max_records: 1,
                max_wait_ms: 200,
                ..limits
            },
        ),
        async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tokio::time::timeout(
                Duration::from_millis(50),
                engine.ack_group_batch(
                    ack_wakeup,
                    "workers",
                    "member-a",
                    vec![DeliveryReceipt {
                        offset: first_keyed[0].offset,
                        delivery_token: first_keyed[0].delivery_token.clone().unwrap(),
                    }],
                ),
            )
            .await
            .expect("acknowledgement during collection must complete before the wait deadline")
        }
    );
    assert_eq!(
        acked.unwrap().outcomes[0].outcome,
        AckBatchOutcome::Confirmed
    );
    let next_keyed = next_keyed.unwrap();
    assert_eq!(next_keyed.len(), 1);
    assert_eq!(next_keyed[0].offset, 1);
    engine
        .ack_group_batch(
            ack_wakeup,
            "workers",
            "member-b",
            vec![DeliveryReceipt {
                offset: next_keyed[0].offset,
                delivery_token: next_keyed[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();

    let expiry = "contract.consume-batch-expiry";
    assert!(engine.create_stream(expiry).await.unwrap());
    engine
        .publish(expiry, None, b"lease".to_vec(), None)
        .await
        .unwrap();
    engine
        .configure_consumer(expiry, "workers", 25, None)
        .await
        .unwrap();
    let expiry_limits = ConsumeBatchLimits {
        max_records: 1,
        max_bytes: 64 * 1024,
        max_wait_ms: 0,
    };
    let expiring = engine
        .poll_group_batch(expiry, "workers", "member-a", expiry_limits)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    let redelivered = engine
        .poll_group_batch(expiry, "workers", "member-b", expiry_limits)
        .await
        .unwrap();
    assert_eq!(redelivered.len(), 1);
    assert_eq!(redelivered[0].offset, expiring[0].offset);
    assert_eq!(redelivered[0].delivery_attempt, Some(2));
    assert_ne!(
        redelivered[0].delivery_token, expiring[0].delivery_token,
        "expired batch receipts must be replaced"
    );
    assert!(matches!(
        engine
            .ack_group_batch(
                expiry,
                "workers",
                "member-a",
                vec![DeliveryReceipt {
                    offset: expiring[0].offset,
                    delivery_token: expiring[0].delivery_token.clone().unwrap(),
                }],
            )
            .await
            .unwrap()
            .outcomes[0]
            .outcome,
        AckBatchOutcome::Rejected { .. }
    ));
    assert_eq!(
        engine
            .ack_group_batch(
                expiry,
                "workers",
                "member-b",
                vec![DeliveryReceipt {
                    offset: redelivered[0].offset,
                    delivery_token: redelivered[0].delivery_token.clone().unwrap(),
                }],
            )
            .await
            .unwrap()
            .outcomes[0]
            .outcome,
        AckBatchOutcome::Confirmed
    );

    let policy = "contract.consume-batch-policy";
    assert!(engine.create_stream(policy).await.unwrap());
    engine
        .configure_consumer(policy, "workers", 25, Some(3))
        .await
        .unwrap();
    engine
        .publish(policy, None, b"pinned-policy".to_vec(), None)
        .await
        .unwrap();
    let pinned_limits = ConsumeBatchLimits {
        max_records: 1,
        max_bytes: 64 * 1024,
        max_wait_ms: 0,
    };
    let first_policy_delivery = engine
        .poll_group_batch(policy, "workers", "member-a", pinned_limits)
        .await
        .unwrap();
    engine
        .configure_consumer(policy, "workers", 500, Some(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    let redelivered_policy = engine
        .poll_group_batch(policy, "workers", "member-b", pinned_limits)
        .await
        .unwrap();
    assert_eq!(redelivered_policy.len(), 1);
    assert_eq!(
        redelivered_policy[0].offset,
        first_policy_delivery[0].offset
    );
    assert_eq!(redelivered_policy[0].delivery_attempt, Some(2));
    engine
        .ack_group_batch(
            policy,
            "workers",
            "member-b",
            vec![DeliveryReceipt {
                offset: redelivered_policy[0].offset,
                delivery_token: redelivered_policy[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();

    let keyed = "contract.consume-batch-key";
    assert!(engine.create_stream(keyed).await.unwrap());
    for payload in [b"first".as_slice(), b"second".as_slice()] {
        engine
            .publish(keyed, Some("same-key".to_owned()), payload.to_vec(), None)
            .await
            .unwrap();
    }
    let one = engine
        .poll_group_batch(
            keyed,
            "workers",
            "member-a",
            ConsumeBatchLimits {
                max_records: 2,
                ..limits
            },
        )
        .await
        .unwrap();
    assert_eq!(
        one.iter().map(|message| message.offset).collect::<Vec<_>>(),
        [0]
    );
    assert!(
        engine
            .poll_group_batch(
                keyed,
                "workers",
                "member-b",
                ConsumeBatchLimits {
                    max_records: 2,
                    ..limits
                },
            )
            .await
            .unwrap()
            .is_empty()
    );
    engine
        .ack_group_batch(
            keyed,
            "workers",
            "member-a",
            vec![DeliveryReceipt {
                offset: one[0].offset,
                delivery_token: one[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();
    let two = engine
        .poll_group_batch(
            keyed,
            "workers",
            "member-b",
            ConsumeBatchLimits {
                max_records: 2,
                ..limits
            },
        )
        .await
        .unwrap();
    assert_eq!(
        two.iter().map(|message| message.offset).collect::<Vec<_>>(),
        [1]
    );
}

/// Verify public request-ID content matching across local and distributed engines.
pub async fn assert_publish_request_id_contract(engine: &dyn Engine) {
    let stream = "contract.publish-id";
    assert!(engine.create_stream(stream).await.unwrap());

    let payload = vec![0, 1, 255];
    assert_eq!(
        engine
            .publish(stream, None, payload.clone(), Some("same-id".to_owned()))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        engine
            .publish(
                stream,
                Some(String::new()),
                payload.clone(),
                Some("same-id".to_owned()),
            )
            .await
            .unwrap(),
        0,
        "absent and empty keys are equivalent for request-ID comparison"
    );

    for (key, changed_payload) in [
        (Some("ordering-key".to_owned()), payload.clone()),
        (None, b"changed payload".to_vec()),
    ] {
        let error = engine
            .publish(stream, key, changed_payload, Some("same-id".to_owned()))
            .await
            .expect_err("changed representable content must conflict");
        assert_eq!(error.kind(), BrokerErrorKind::RequestIdContentConflict);
        assert_eq!(error.outcome(), BrokerErrorOutcome::Rejected);
    }

    assert_eq!(
        engine
            .publish(stream, Some("next".to_owned()), b"next".to_vec(), None)
            .await
            .unwrap(),
        1,
        "conflicts must not append or allocate an offset"
    );
    let first = engine.poll(stream, "reader").await.unwrap();
    assert!(matches!(
        first,
        PollResult::Message(message)
            if message.offset == 0 && message.key.is_none() && message.payload == payload
    ));
    engine.ack(stream, "reader", 0).await.unwrap();
    assert!(
        matches!(
            engine.poll(stream, "reader").await.unwrap(),
            PollResult::Message(message) if message.offset == 1 && message.payload == b"next"
        ),
        "rejected retries must leave consumer progress and the original record unchanged"
    );

    for (name, expected_payload) in [
        ("contract.publish-id.scope-a", b"a".as_slice()),
        ("contract.publish-id.scope-b", b"b".as_slice()),
    ] {
        assert!(engine.create_stream(name).await.unwrap());
        assert_eq!(
            engine
                .publish(
                    name,
                    None,
                    expected_payload.to_vec(),
                    Some("scoped".to_owned())
                )
                .await
                .unwrap(),
            0
        );
    }

    let batch = "contract.publish-id-batch";
    assert!(engine.create_stream(batch).await.unwrap());
    let outcomes = engine
        .publish_batch(
            batch,
            vec![
                PublishRecord {
                    key: Some("batch-key".to_owned()),
                    payload: b"first".to_vec(),
                    request_id: Some("batch-id".to_owned()),
                },
                PublishRecord {
                    key: Some("batch-key".to_owned()),
                    payload: b"first".to_vec(),
                    request_id: Some("batch-id".to_owned()),
                },
                PublishRecord {
                    key: Some("batch-key".to_owned()),
                    payload: b"changed".to_vec(),
                    request_id: Some("batch-id".to_owned()),
                },
                PublishRecord {
                    key: None,
                    payload: b"independent".to_vec(),
                    request_id: Some("other-id".to_owned()),
                },
                PublishRecord {
                    key: None,
                    payload: b"without-id".to_vec(),
                    request_id: None,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 5);
    assert!(matches!(outcomes[0], Ok(0)));
    assert!(matches!(outcomes[1], Ok(0)));
    assert!(matches!(
        &outcomes[2],
        Err(error)
            if error.kind() == BrokerErrorKind::RequestIdContentConflict
                && error.outcome() == BrokerErrorOutcome::Rejected
    ));
    assert!(matches!(outcomes[3], Ok(1)));
    assert!(matches!(outcomes[4], Ok(2)));

    for (offset, expected) in [
        (0, b"first".as_slice()),
        (1, b"independent"),
        (2, b"without-id"),
    ] {
        assert!(matches!(
            engine.poll(batch, "reader").await.unwrap(),
            PollResult::Message(message) if message.offset == offset && message.payload == expected
        ));
        engine.ack(batch, "reader", offset).await.unwrap();
    }
    assert_eq!(
        engine.poll(batch, "reader").await.unwrap(),
        PollResult::Empty
    );

    let concurrent = "contract.publish-id-concurrent";
    assert!(engine.create_stream(concurrent).await.unwrap());
    let left = engine.publish(
        concurrent,
        Some("left-key".to_owned()),
        b"left".to_vec(),
        Some("racing-id".to_owned()),
    );
    let right = engine.publish(
        concurrent,
        Some("right-key".to_owned()),
        b"right".to_vec(),
        Some("racing-id".to_owned()),
    );
    let (left, right) = tokio::join!(left, right);
    let accepted_payload = match (left, right) {
        (Ok(0), Err(error)) if error.kind() == BrokerErrorKind::RequestIdContentConflict => {
            b"left".as_slice()
        }
        (Err(error), Ok(0)) if error.kind() == BrokerErrorKind::RequestIdContentConflict => {
            b"right".as_slice()
        }
        outcomes => panic!("one concurrent content must win and the other conflict: {outcomes:?}"),
    };
    assert!(matches!(
        engine.poll(concurrent, "reader").await.unwrap(),
        PollResult::Message(message) if message.offset == 0 && message.payload == accepted_payload
    ));
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

pub async fn assert_consumer_policy_configuration_contract(engine: &dyn Engine) {
    assert!(engine.create_stream("contract.policy").await.unwrap());

    let fallback = engine
        .inspect_consumer("contract.policy", "worker")
        .await
        .unwrap();
    assert_eq!(fallback.version, 0);
    assert!(!fallback.configured);

    let other_fallback = engine
        .inspect_consumer("contract.policy", "other-worker")
        .await
        .unwrap();
    assert_eq!(other_fallback.version, 0);
    assert!(!other_fallback.configured);
    assert_eq!(other_fallback, fallback);

    let configured = engine
        .configure_consumer("contract.policy", "worker", 500, Some(2))
        .await
        .unwrap();
    assert_eq!(configured.version, 1);
    assert!(configured.configured);
    assert_eq!(configured.ack_timeout_ms, 500);
    assert_eq!(configured.max_delivery_attempts, Some(2));

    let repeated = engine
        .configure_consumer("contract.policy", "worker", 500, Some(2))
        .await
        .unwrap();
    assert_eq!(repeated, configured);
    assert_eq!(
        engine
            .inspect_consumer("contract.policy", "other-worker")
            .await
            .unwrap(),
        other_fallback
    );
    assert_eq!(
        engine
            .inspect_consumer("contract.policy", "worker")
            .await
            .unwrap(),
        configured
    );

    let updated = engine
        .configure_consumer("contract.policy", "worker", 501, Some(3))
        .await
        .unwrap();
    assert!(updated.version > configured.version);
    assert_eq!(updated.ack_timeout_ms, 501);
    assert_eq!(updated.max_delivery_attempts, Some(3));
    assert_eq!(
        engine
            .inspect_consumer("contract.policy", "worker")
            .await
            .unwrap(),
        updated
    );

    let invalid = engine
        .configure_consumer("contract.policy", "worker", 777, Some(0))
        .await
        .expect_err("a zero attempt limit must be rejected");
    assert_eq!(invalid.kind(), BrokerErrorKind::Configuration);
    assert_eq!(invalid.outcome(), BrokerErrorOutcome::Rejected);
    assert_eq!(
        engine
            .inspect_consumer("contract.policy", "worker")
            .await
            .unwrap(),
        updated
    );
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

    assert!(matches!(
        engine
            .ack_group("contract.expiry", "workers", "member-a", offset, &old_token,)
            .await,
        Err(BrokerError::StaleDelivery {
            ref consumer,
            offset: stale_offset,
        }) if consumer == "workers" && stale_offset == offset
    ));

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
