use runnel_protocol::{
    BinaryPayload, PayloadEncoding, PROTOCOL_NAME, PROTOCOL_SUPPORT, PROTOCOL_VERSION, Request,
    v2::{self, ClientFrame, MAX_CLIENT_TO_SERVER_FRAME_BYTES},
};

#[test]
fn protocol_support_declares_only_the_v2_protobuf_contract() {
    assert_eq!(PROTOCOL_NAME, "runnel-protobuf");
    assert_eq!(PROTOCOL_VERSION, 2);
    assert_eq!(PROTOCOL_SUPPORT.versions.min, 2);
    assert_eq!(PROTOCOL_SUPPORT.versions.max, 2);
    assert!(PROTOCOL_SUPPORT.supports_version(2));
    assert!(!PROTOCOL_SUPPORT.supports_version(1));
    assert_eq!(
        PROTOCOL_SUPPORT.payload_encodings,
        &[PayloadEncoding::Utf8Text, PayloadEncoding::Binary]
    );
}

#[test]
fn binary_request_payload_is_encoded_as_opaque_bytes() {
    let payload = [0, 1, 0xff, 0x80, b'\n'];
    let frame = v2::encode_client_frame(&ClientFrame::Application(Request::PublishBytes {
        stream: "events".to_owned(),
        key: Some("key".to_owned()),
        payload: BinaryPayload::new(payload),
        request_id: None,
    }))
    .unwrap();

    let ClientFrame::Application(Request::PublishBytes { payload: decoded, .. }) =
        v2::decode_client_frame(
            &frame.as_bytes()[4..],
            MAX_CLIENT_TO_SERVER_FRAME_BYTES,
        )
        .unwrap()
    else {
        panic!("binary publish should decode as an application request");
    };
    assert_eq!(decoded.as_bytes(), payload);
}
