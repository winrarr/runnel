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
fn binary_payloads_round_trip_without_utf8_conversion() {
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
