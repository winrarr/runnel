use runnel_protocol::{BinaryPayload, Request, Response};

#[test]
fn requests_and_responses_round_trip_as_json() {
    let request = Request::Publish {
        stream: "events".to_owned(),
        key: Some("order-1".to_owned()),
        payload: "hello".to_owned(),
        request_id: Some("request-1".to_owned()),
    };
    let encoded = serde_json::to_string(&request).unwrap();
    assert!(encoded.contains(r#""op":"publish""#));
    assert!(encoded.contains(r#""payload":"hello""#));
    let decoded: Request = serde_json::from_str(&encoded).unwrap();
    assert!(matches!(
        decoded,
        Request::Publish {
            stream,
            key: Some(key),
            payload,
            request_id: Some(request_id),
        } if stream == "events"
            && key == "order-1"
            && payload == "hello"
            && request_id == "request-1"
    ));

    let response = Response::Acknowledged {
        stream: "events".to_owned(),
        consumer: "worker".to_owned(),
        offset: 7,
        already_acknowledged: false,
    };
    let encoded = serde_json::to_string(&response).unwrap();
    let decoded: Response = serde_json::from_str(&encoded).unwrap();
    assert!(matches!(
        decoded,
        Response::Acknowledged {
            stream,
            consumer,
            offset: 7,
            already_acknowledged: false,
        } if stream == "events" && consumer == "worker"
    ));
}

#[test]
fn grouped_delivery_round_trips_member_and_token() {
    let request = Request::AckGroup {
        stream: "jobs".to_owned(),
        consumer: "workers".to_owned(),
        member: "worker-a".to_owned(),
        offset: 3,
        delivery_token: "epoch-sequence".to_owned(),
    };
    let encoded = serde_json::to_string(&request).unwrap();
    let decoded: Request = serde_json::from_str(&encoded).unwrap();
    assert!(matches!(
        decoded,
        Request::AckGroup {
            stream,
            consumer,
            member,
            offset: 3,
            delivery_token,
        } if stream == "jobs"
            && consumer == "workers"
            && member == "worker-a"
            && delivery_token == "epoch-sequence"
    ));

    let response = Response::Message {
        stream: "jobs".to_owned(),
        consumer: "workers".to_owned(),
        member: Some("worker-a".to_owned()),
        offset: 3,
        key: Some("customer-1".to_owned()),
        payload: "hello".to_owned(),
        published_at_ms: 10,
        delivery_token: Some("epoch-sequence".to_owned()),
        delivery_attempt: Some(2),
    };
    let encoded = serde_json::to_string(&response).unwrap();
    let decoded: Response = serde_json::from_str(&encoded).unwrap();
    assert!(matches!(
        decoded,
        Response::Message {
            member: Some(member),
            delivery_token: Some(token),
            offset: 3,
            ..
        } if member == "worker-a" && token == "epoch-sequence"
    ));
}

#[test]
fn replay_round_trips_without_delivery_state() {
    let request = Request::Replay {
        stream: "events".to_owned(),
        consumer: "worker".to_owned(),
        offset: 7,
    };
    let encoded = serde_json::to_string(&request).unwrap();
    assert_eq!(
        encoded,
        r#"{"op":"replay","stream":"events","consumer":"worker","offset":7}"#
    );
    assert!(matches!(
        serde_json::from_str::<Request>(&encoded).unwrap(),
        Request::Replay {
            stream,
            consumer,
            offset: 7,
        } if stream == "events" && consumer == "worker"
    ));

    let response = Response::ReplayMessage {
        stream: "events".to_owned(),
        consumer: "worker".to_owned(),
        offset: 7,
        key: Some("order-1".to_owned()),
        payload: "hello".to_owned(),
        published_at_ms: 10,
    };
    let encoded = serde_json::to_string(&response).unwrap();
    assert_eq!(
        encoded,
        r#"{"type":"replay_message","stream":"events","consumer":"worker","offset":7,"key":"order-1","payload":"hello","published_at_ms":10}"#
    );
    assert!(matches!(
        serde_json::from_str::<Response>(&encoded).unwrap(),
        Response::ReplayMessage {
            stream,
            consumer,
            offset: 7,
            key: Some(key),
            payload,
            published_at_ms: 10,
        } if stream == "events"
            && consumer == "worker"
            && key == "order-1"
            && payload == "hello"
    ));
}

#[test]
fn binary_payloads_round_trip_without_utf8_conversion() {
    let empty: Request = serde_json::from_str(
        r#"{"op":"publish_bytes","stream":"events","key":null,"payload_base64":"","request_id":null}"#,
    )
    .unwrap();
    assert!(matches!(
        empty,
        Request::PublishBytes {
            payload_base64, ..
        } if payload_base64.as_bytes().is_empty()
    ));

    let request: Request = serde_json::from_str(
        r#"{"op":"publish_bytes","stream":"events","key":null,"payload_base64":"AAH/Cl8=","request_id":null}"#,
    )
    .unwrap();
    assert!(matches!(
        request,
        Request::PublishBytes {
            payload_base64, ..
        } if payload_base64.as_bytes() == [0, 1, 255, b'\n', b'_']
    ));

    let encoded = serde_json::to_string(&Request::PublishBytes {
        stream: "events".to_owned(),
        key: None,
        payload_base64: BinaryPayload::new(vec![0, 1, 255, b'\n', b'_']),
        request_id: None,
    })
    .unwrap();
    assert_eq!(
        encoded,
        r#"{"op":"publish_bytes","stream":"events","key":null,"payload_base64":"AAH/Cl8=","request_id":null}"#
    );

    let response: Response = serde_json::from_str(
        r#"{"type":"message_bytes","stream":"events","consumer":"worker","offset":0,"key":null,"payload_base64":"AAH/Cl8=","published_at_ms":1}"#,
    )
    .unwrap();
    assert!(matches!(
        response,
        Response::MessageBytes {
            payload_base64, ..
        } if payload_base64.as_bytes() == [0, 1, 255, b'\n', b'_']
    ));

    let encoded = serde_json::to_string(&Response::MessageBytes {
        stream: "events".to_owned(),
        consumer: "worker".to_owned(),
        member: None,
        offset: 0,
        key: None,
        payload_base64: BinaryPayload::new(vec![0, 1, 255, b'\n', b'_']),
        published_at_ms: 1,
        delivery_token: None,
        delivery_attempt: None,
    })
    .unwrap();
    assert_eq!(
        encoded,
        r#"{"type":"message_bytes","stream":"events","consumer":"worker","offset":0,"key":null,"payload_base64":"AAH/Cl8=","published_at_ms":1}"#
    );
}

#[test]
fn binary_payload_rejects_malformed_or_contradictory_json() {
    for json in [
        r#"{"op":"publish_bytes","stream":"events","key":null,"payload_base64":"not base64","request_id":null}"#,
        r#"{"op":"publish","stream":"events","key":null,"payload":"text","payload_base64":"AA==","request_id":null}"#,
    ] {
        assert!(serde_json::from_str::<Request>(json).is_err());
    }
    assert!(serde_json::from_str::<Response>(
        r#"{"type":"message_bytes","stream":"events","consumer":"worker","offset":0,"key":null,"payload_base64":"not base64","published_at_ms":1}"#,
    )
    .is_err());
}

#[test]
fn publish_batch_preserves_binary_records_and_per_record_outcomes() {
    let request = Request::PublishBatch {
        stream: "events".to_owned(),
        records: vec![
            runnel_protocol::PublishBatchRecord {
                key: Some("order-1".to_owned()),
                payload_base64: BinaryPayload::new(vec![0, 255]),
                request_id: Some("record-1".to_owned()),
            },
            runnel_protocol::PublishBatchRecord {
                key: None,
                payload_base64: BinaryPayload::new(b"second".to_vec()),
                request_id: None,
            },
        ],
    };
    let encoded = serde_json::to_string(&request).unwrap();
    let decoded: Request = serde_json::from_str(&encoded).unwrap();
    assert!(matches!(
        decoded,
        Request::PublishBatch { stream, records }
            if stream == "events"
                && records.len() == 2
                && records[0].payload_base64.as_bytes() == [0, 255]
                && records[0].request_id.as_deref() == Some("record-1")
    ));

    let response = Response::PublishBatch {
        stream: "events".to_owned(),
        outcomes: vec![
            runnel_protocol::PublishBatchRecordResponse::Published { offset: 4 },
            runnel_protocol::PublishBatchRecordResponse::Error {
                code: "invalid_name".to_owned(),
                message: "rejected".to_owned(),
            },
        ],
    };
    let encoded = serde_json::to_string(&response).unwrap();
    let decoded: Response = serde_json::from_str(&encoded).unwrap();
    assert!(matches!(
        decoded,
        Response::PublishBatch { stream, outcomes }
            if stream == "events"
                && matches!(outcomes.as_slice(), [
                    runnel_protocol::PublishBatchRecordResponse::Published { offset: 4 },
                    runnel_protocol::PublishBatchRecordResponse::Error { code, .. }
                ] if code == "invalid_name")
    ));
}

#[test]
fn current_request_fixtures_pin_v1_tags_and_fields() {
    let fixtures = [
        (
            "create_stream",
            Request::CreateStream {
                stream: "events".to_owned(),
            },
            serde_json::json!({"op": "create_stream", "stream": "events"}),
        ),
        (
            "publish",
            Request::Publish {
                stream: "events".to_owned(),
                key: Some("order-1".to_owned()),
                payload: "hello".to_owned(),
                request_id: Some("publish-1".to_owned()),
            },
            serde_json::json!({
                "op": "publish",
                "stream": "events",
                "key": "order-1",
                "payload": "hello",
                "request_id": "publish-1"
            }),
        ),
        (
            "publish_bytes",
            Request::PublishBytes {
                stream: "events".to_owned(),
                key: None,
                payload_base64: BinaryPayload::new([0, 255]),
                request_id: None,
            },
            serde_json::json!({
                "op": "publish_bytes",
                "stream": "events",
                "key": null,
                "payload_base64": "AP8=",
                "request_id": null
            }),
        ),
        (
            "publish_batch",
            Request::PublishBatch {
                stream: "events".to_owned(),
                records: vec![
                    runnel_protocol::PublishBatchRecord {
                        key: Some("order-1".to_owned()),
                        payload_base64: BinaryPayload::new([0, 255]),
                        request_id: Some("record-1".to_owned()),
                    },
                    runnel_protocol::PublishBatchRecord {
                        key: None,
                        payload_base64: BinaryPayload::new(b"second"),
                        request_id: None,
                    },
                ],
            },
            serde_json::json!({
                "op": "publish_batch",
                "stream": "events",
                "records": [
                    {"key": "order-1", "payload_base64": "AP8=", "request_id": "record-1"},
                    {"key": null, "payload_base64": "c2Vjb25k", "request_id": null}
                ]
            }),
        ),
        (
            "poll",
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
            serde_json::json!({"op": "poll", "stream": "events", "consumer": "worker"}),
        ),
        (
            "poll_group",
            Request::PollGroup {
                stream: "events".to_owned(),
                consumer: "workers".to_owned(),
                member: "worker-a".to_owned(),
            },
            serde_json::json!({
                "op": "poll_group",
                "stream": "events",
                "consumer": "workers",
                "member": "worker-a"
            }),
        ),
        (
            "ack",
            Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 7,
            },
            serde_json::json!({"op": "ack", "stream": "events", "consumer": "worker", "offset": 7}),
        ),
        (
            "ack_group",
            Request::AckGroup {
                stream: "events".to_owned(),
                consumer: "workers".to_owned(),
                member: "worker-a".to_owned(),
                offset: 7,
                delivery_token: "epoch-sequence".to_owned(),
            },
            serde_json::json!({
                "op": "ack_group",
                "stream": "events",
                "consumer": "workers",
                "member": "worker-a",
                "offset": 7,
                "delivery_token": "epoch-sequence"
            }),
        ),
        (
            "health",
            Request::Health,
            serde_json::json!({"op": "health"}),
        ),
    ];

    for (name, request, expected) in fixtures {
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            expected,
            "v1 request fixture changed: {name}"
        );
    }
}

#[test]
fn current_response_fixtures_pin_v1_tags_and_fields() {
    let fixtures = [
        (
            "stream_created",
            Response::StreamCreated {
                stream: "events".to_owned(),
                created: true,
            },
            serde_json::json!({"type": "stream_created", "stream": "events", "created": true}),
        ),
        (
            "published",
            Response::Published {
                stream: "events".to_owned(),
                offset: 7,
            },
            serde_json::json!({"type": "published", "stream": "events", "offset": 7}),
        ),
        (
            "publish_batch",
            Response::PublishBatch {
                stream: "events".to_owned(),
                outcomes: vec![
                    runnel_protocol::PublishBatchRecordResponse::Published { offset: 7 },
                    runnel_protocol::PublishBatchRecordResponse::Error {
                        code: "invalid_name".to_owned(),
                        message: "rejected".to_owned(),
                    },
                ],
            },
            serde_json::json!({
                "type": "publish_batch",
                "stream": "events",
                "outcomes": [
                    {"type": "published", "offset": 7},
                    {"type": "error", "code": "invalid_name", "message": "rejected"}
                ]
            }),
        ),
        (
            "message",
            Response::Message {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                member: None,
                offset: 7,
                key: None,
                payload: "hello".to_owned(),
                published_at_ms: 10,
                delivery_token: None,
                delivery_attempt: None,
            },
            serde_json::json!({
                "type": "message",
                "stream": "events",
                "consumer": "worker",
                "offset": 7,
                "key": null,
                "payload": "hello",
                "published_at_ms": 10
            }),
        ),
        (
            "message_bytes",
            Response::MessageBytes {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                member: None,
                offset: 7,
                key: None,
                payload_base64: BinaryPayload::new([0, 255]),
                published_at_ms: 10,
                delivery_token: None,
                delivery_attempt: None,
            },
            serde_json::json!({
                "type": "message_bytes",
                "stream": "events",
                "consumer": "worker",
                "offset": 7,
                "key": null,
                "payload_base64": "AP8=",
                "published_at_ms": 10
            }),
        ),
        (
            "empty",
            Response::Empty {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
            serde_json::json!({"type": "empty", "stream": "events", "consumer": "worker"}),
        ),
        (
            "acknowledged",
            Response::Acknowledged {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 7,
                already_acknowledged: false,
            },
            serde_json::json!({
                "type": "acknowledged",
                "stream": "events",
                "consumer": "worker",
                "offset": 7,
                "already_acknowledged": false
            }),
        ),
        (
            "health",
            Response::Health {
                status: "ok".to_owned(),
                streams: 1,
                storage_bytes: 42,
            },
            serde_json::json!({"type": "health", "status": "ok", "streams": 1, "storage_bytes": 42}),
        ),
        (
            "error",
            Response::Error {
                code: "invalid_request".to_owned(),
                message: "bad request".to_owned(),
            },
            serde_json::json!({
                "type": "error",
                "code": "invalid_request",
                "message": "bad request"
            }),
        ),
    ];

    for (name, response, expected) in fixtures {
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            expected,
            "v1 response fixture changed: {name}"
        );
    }
}

#[test]
fn current_optional_fields_keep_legacy_fixtures_readable() {
    let request: Request =
        serde_json::from_str(r#"{"op":"publish","stream":"events","key":null,"payload":"hello"}"#)
            .unwrap();
    assert!(matches!(
        request,
        Request::Publish {
            request_id: None,
            payload,
            ..
        } if payload == "hello"
    ));

    let request: Request = serde_json::from_str(
        r#"{"op":"publish_batch","stream":"events","records":[{"key":null,"payload_base64":"AA=="}]}"#,
    )
    .unwrap();
    assert!(matches!(
        request,
        Request::PublishBatch { records, .. }
            if records.len() == 1 && records[0].request_id.is_none()
    ));

    let response: Response = serde_json::from_str(
        r#"{"type":"message","stream":"events","consumer":"worker","offset":7,"key":null,"payload":"hello","published_at_ms":10}"#,
    )
    .unwrap();
    assert!(matches!(
        response,
        Response::Message {
            member: None,
            delivery_token: None,
            delivery_attempt: None,
            ..
        }
    ));
}

#[test]
fn current_json_object_member_order_does_not_change_request_meaning() {
    let first: Request = serde_json::from_str(
        r#"{"op":"publish","stream":"events","key":"order-1","payload":"hello","request_id":"publish-1"}"#,
    )
    .unwrap();
    let reordered: Request = serde_json::from_str(
        r#"{"request_id":"publish-1","payload":"hello","key":"order-1","stream":"events","op":"publish"}"#,
    )
    .unwrap();

    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(reordered).unwrap()
    );
}

#[test]
fn current_v1_unknown_fields_and_tags_have_explicit_behavior() {
    assert!(serde_json::from_str::<Request>(r#"{"op":"health""#).is_err());
    assert!(serde_json::from_str::<Request>(
        r#"{"op":"publish","stream":"events","key":null,"payload":"hello","future_field":true}"#,
    )
    .is_err());
    assert!(serde_json::from_str::<Request>(
        r#"{"op":"publish_batch","stream":"events","records":[{"key":null,"payload_base64":"AA==","future_field":true}]}"#,
    )
    .is_err());
    assert!(
        serde_json::from_str::<Request>(
            r#"{"op":"publish","stream":"events","stream":"other","key":null,"payload":"hello"}"#,
        )
        .is_err()
    );
    assert!(serde_json::from_str::<Request>(r#"{"op":"future_operation"}"#).is_err());

    let request: Request = serde_json::from_str(r#"{"op":"health","future_field":true}"#).unwrap();
    assert!(matches!(request, Request::Health));

    let response: Response = serde_json::from_str(
        r#"{"type":"health","status":"ok","streams":1,"storage_bytes":42,"future_field":true}"#,
    )
    .unwrap();
    assert!(matches!(response, Response::Health { streams: 1, .. }));
    let response: Response = serde_json::from_str(
        r#"{"type":"publish_batch","stream":"events","outcomes":[{"type":"published","offset":7,"future_field":true}]}"#,
    )
    .unwrap();
    assert!(matches!(
        response,
        Response::PublishBatch { outcomes, .. }
            if matches!(outcomes.as_slice(), [
                runnel_protocol::PublishBatchRecordResponse::Published { offset: 7 }
            ])
    ));
    assert!(serde_json::from_str::<Response>(r#"{"type":"future_response"}"#).is_err());
}

#[test]
fn current_v1_json_lines_decode_as_independent_frames() {
    let wire = concat!(
        r#"{"op":"health"}"#,
        "\n",
        r#"{"op":"create_stream","stream":"events"}"#,
        "\r\n"
    );
    let frames = wire
        .split_terminator('\n')
        .map(|line| serde_json::from_str::<Request>(line).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(frames.len(), 2);
    assert!(matches!(frames[0], Request::Health));
    assert!(matches!(
        &frames[1],
        Request::CreateStream { stream } if stream == "events"
    ));
}

#[test]
fn request_identity_is_not_a_current_response_correlation_id() {
    let request = Request::Publish {
        stream: "events".to_owned(),
        key: None,
        payload: "hello".to_owned(),
        request_id: Some("publish-1".to_owned()),
    };
    let request_json = serde_json::to_value(request).unwrap();
    assert_eq!(request_json["request_id"], "publish-1");

    let response = serde_json::to_value(Response::Published {
        stream: "events".to_owned(),
        offset: 7,
    })
    .unwrap();
    assert!(response.get("request_id").is_none());
}
