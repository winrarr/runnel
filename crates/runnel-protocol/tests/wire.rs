use runnel_protocol::{Request, Response};

#[test]
fn requests_and_responses_round_trip_as_json() {
    let request = Request::Publish {
        stream: "events".to_owned(),
        key: Some("order-1".to_owned()),
        payload: "hello".to_owned(),
        request_id: Some("request-1".to_owned()),
    };
    let encoded = serde_json::to_string(&request).unwrap();
    let decoded: Request = serde_json::from_str(&encoded).unwrap();
    assert!(matches!(
        decoded,
        Request::Publish {
            stream,
            key: Some(key),
            payload,
            request_id: Some(request_id),
        } if stream == "events" && key == "order-1" && payload == "hello" && request_id == "request-1"
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
