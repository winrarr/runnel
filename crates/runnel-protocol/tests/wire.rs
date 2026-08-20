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
