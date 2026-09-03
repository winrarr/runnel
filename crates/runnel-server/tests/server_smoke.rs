use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use runnel_protocol::{
    BinaryPayload, MAX_PUBLISH_BATCH_RECORDS, PublishBatchRecord, PublishBatchRecordResponse,
    Request, Response,
};
use tempfile::TempDir;

struct RunningServer {
    child: Child,
    broker_addr: SocketAddr,
    http_addr: SocketAddr,
}

impl RunningServer {
    fn start(data_dir: &Path) -> Self {
        Self::start_with_args(data_dir, &[])
    }

    fn start_with_args(data_dir: &Path, extra_args: &[&str]) -> Self {
        let broker_addr = free_addr();
        let http_addr = free_addr();
        let binary = server_binary();
        let mut command = Command::new(binary);
        command.args([
            "--data-dir",
            data_dir.to_str().expect("temporary path should be UTF-8"),
            "--listen",
            &broker_addr.to_string(),
            "--http-listen",
            &http_addr.to_string(),
        ]);
        command.args(extra_args);
        let child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("runnel server should start");

        wait_for_http(http_addr);
        Self {
            child,
            broker_addr,
            http_addr,
        }
    }

    fn stop(mut self) {
        if self
            .child
            .try_wait()
            .expect("server status should be readable")
            .is_none()
        {
            self.child.kill().expect("server should stop");
        }
        self.child.wait().expect("server should be reaped");
    }
}

fn server_binary() -> PathBuf {
    if let Some(binary) = std::env::var_os("CARGO_BIN_EXE_runnel") {
        return PathBuf::from(binary);
    }

    let test_binary = std::env::current_exe().expect("Cargo should expose the test executable");
    let target_directory = test_binary
        .parent()
        .and_then(Path::parent)
        .expect("test executable should be inside target/debug/deps");
    let binary = target_directory.join("runnel");
    assert!(
        binary.is_file(),
        "Cargo should build the runnel binary at {}",
        binary.display()
    );
    binary
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
fn network_protocol_persists_acknowledgements_across_restart() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "events".to_owned(),
            },
        ),
        Response::StreamCreated { created: true, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "hello".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Message {
            offset: 0,
            payload,
            ..
        } if payload == "hello"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
        ),
        Response::ReplayMessage {
            offset: 0,
            payload,
            ..
        } if payload == "hello"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Empty { .. }
    ));

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "recover-me".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 1, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 2,
            },
        ),
        Response::Error { code, message }
            if code == "history_unavailable" && message.contains("[0, 2)")
    ));
    server.stop();

    let server = RunningServer::start(directory.path());
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
        ),
        Response::ReplayMessage {
            offset: 0,
            payload,
            ..
        } if payload == "hello"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Message {
            offset: 1,
            payload,
            ..
        } if payload == "recover-me"
    ));
}

#[test]
fn network_protocol_round_trips_binary_payload_across_restart() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());
    let binary_json =
        r#"{"op":"publish_bytes","stream":"binary","key":null,"payload_base64":"AAH/Cl8="}"#;

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "binary".to_owned(),
            },
        ),
        Response::StreamCreated { created: true, .. }
    ));
    assert!(matches!(
        request_line(server.broker_addr, binary_json),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request_line(
            server.broker_addr,
            r#"{"op":"poll","stream":"binary","consumer":"worker"}"#,
        ),
        Response::MessageBytes {
            offset: 0,
            payload_base64,
            ..
        } if payload_base64.as_bytes() == [0, 1, 255, b'\n', b'_']
    ));

    server.stop();
    let server = RunningServer::start(directory.path());
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "binary".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
        ),
        Response::ReplayMessageBytes {
            offset: 0,
            payload_base64,
            ..
        } if payload_base64.as_bytes() == [0, 1, 255, b'\n', b'_']
    ));
    assert!(matches!(
        request_line(
            server.broker_addr,
            r#"{"op":"poll","stream":"binary","consumer":"worker"}"#,
        ),
        Response::MessageBytes {
            offset: 0,
            payload_base64,
            ..
        } if payload_base64.as_bytes() == [0, 1, 255, b'\n', b'_']
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Ack {
                stream: "binary".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));
}

#[test]
fn network_protocol_rejects_binary_conflicts_before_engine_execution() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "binary".to_owned(),
            },
        ),
        Response::StreamCreated { created: true, .. }
    ));
    assert!(matches!(
        request_line(
            server.broker_addr,
            r#"{"op":"publish","stream":"binary","key":null,"payload":"text","payload_base64":"AA=="}"#,
        ),
        Response::Error { code, .. } if code == "invalid_request"
    ));
    assert!(matches!(
        request_line(
            server.broker_addr,
            r#"{"op":"publish_bytes","stream":"binary","key":null,"payload_base64":"not base64"}"#,
        ),
        Response::Error { code, .. } if code == "invalid_request"
    ));
    assert!(matches!(
        request_line(
            server.broker_addr,
            r#"{"op":"poll","stream":"binary","consumer":"worker"}"#,
        ),
        Response::Empty { .. }
    ));
}

#[test]
fn metrics_report_messages_returned_by_polls() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    let initial_metrics = http_metrics(server.http_addr);
    assert_eq!(metric_value(&initial_metrics, "runnel_deliveries_total"), 0);
    assert_eq!(
        metric_value(&initial_metrics, "runnel_in_flight_deliveries"),
        0
    );
    assert_eq!(
        metric_value(&initial_metrics, "runnel_active_connections"),
        0
    );
    assert_eq!(metric_value(&initial_metrics, "runnel_active_requests"), 0);
    assert_eq!(
        metric_value(&initial_metrics, "runnel_broker_connections_accepted_total"),
        0
    );
    assert_eq!(
        metric_value(&initial_metrics, "runnel_broker_connections_closed_total"),
        0
    );
    assert_eq!(
        metric_value(&initial_metrics, "runnel_broker_connection_errors_total"),
        0
    );
    assert_eq!(
        metric_value(&initial_metrics, "runnel_broker_request_bytes_total"),
        0
    );
    assert_eq!(
        metric_value(&initial_metrics, "runnel_broker_response_bytes_total"),
        0
    );
    assert!(initial_metrics.contains(
        "# HELP runnel_deliveries_total Messages returned by successful poll operations."
    ));
    assert!(initial_metrics.contains("# TYPE runnel_deliveries_total counter"));
    assert!(initial_metrics.contains("# TYPE runnel_in_flight_deliveries gauge"));
    assert!(initial_metrics.contains("# TYPE runnel_broker_request_duration_seconds histogram"));

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "hello".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Message { offset: 0, .. }
    ));
    let in_flight_metrics = http_metrics(server.http_addr);
    assert_eq!(
        metric_value(&in_flight_metrics, "runnel_in_flight_deliveries"),
        1
    );
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));

    let metrics = http_metrics(server.http_addr);
    assert_eq!(metric_value(&metrics, "runnel_in_flight_deliveries"), 0);
    assert_eq!(metric_value(&metrics, "runnel_deliveries_total"), 1);
    assert_eq!(metric_value(&metrics, "runnel_delivered_bytes_total"), 5);
    assert_eq!(metric_value(&metrics, "runnel_publishes_total"), 1);
    assert_eq!(metric_value(&metrics, "runnel_published_bytes_total"), 5);
    assert_eq!(metric_value(&metrics, "runnel_acknowledgements_total"), 1);
    assert!(
        metric_value(&metrics, "runnel_broker_connections_accepted_total") >= 3,
        "each broker request should be served by an accepted connection"
    );
    assert!(metric_value(&metrics, "runnel_broker_request_bytes_total") > 0);
    assert!(metric_value(&metrics, "runnel_broker_response_bytes_total") > 0);
    assert_eq!(
        labeled_metric_value(
            &metrics,
            "runnel_broker_requests_total",
            "operation=\"publish\""
        ),
        1
    );
    assert_eq!(
        labeled_metric_value(
            &metrics,
            "runnel_broker_request_failures_total",
            "operation=\"publish\""
        ),
        0
    );
    assert_eq!(
        labeled_metric_value(
            &metrics,
            "runnel_broker_request_duration_seconds_count",
            "operation=\"poll\""
        ),
        1
    );
    assert!(metric_value(&metrics, "runnel_metrics_scrapes_total") >= 2);
}

#[test]
fn metrics_report_connection_lifecycle_and_framing_errors() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    let mut connection = TcpStream::connect(server.broker_addr).unwrap();
    connection
        .write_all(&[0xff, b'\n'])
        .expect("invalid UTF-8 should be written");
    drop(connection);

    let metrics =
        wait_for_metric_at_least(server.http_addr, "runnel_broker_connection_errors_total", 1);
    assert_eq!(
        metric_value(&metrics, "runnel_broker_connection_errors_total"),
        1
    );
    assert!(metric_value(&metrics, "runnel_broker_connections_accepted_total") >= 1);
    assert!(metric_value(&metrics, "runnel_broker_connections_closed_total") >= 1);
    assert_eq!(metric_value(&metrics, "runnel_active_connections"), 0);
}

#[test]
fn metrics_report_protocol_failures_without_stream_labels() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "missing-stream".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Error { code, .. } if code == "stream_not_found"
    ));

    let metrics = http_metrics(server.http_addr);
    assert_eq!(
        labeled_metric_value(
            &metrics,
            "runnel_broker_requests_total",
            "operation=\"poll\""
        ),
        1
    );
    assert_eq!(
        labeled_metric_value(
            &metrics,
            "runnel_broker_request_failures_total",
            "operation=\"poll\""
        ),
        1
    );
    assert!(metric_value(&metrics, "runnel_active_connections") <= 1);
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 0);
    assert!(!metrics.contains("missing-stream"));
    assert!(!metrics.contains("worker"));
}

#[test]
fn network_protocol_shares_work_between_group_members() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "jobs".to_owned(),
                key: None,
                payload: "first".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "jobs".to_owned(),
                key: None,
                payload: "second".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 1, .. }
    ));

    let first = request(
        server.broker_addr,
        Request::PollGroup {
            stream: "jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
        },
    );
    let first_token = match first {
        Response::Message {
            offset: 0,
            member: Some(member),
            delivery_token: Some(token),
            ..
        } => {
            assert_eq!(member, "member-a");
            token
        }
        response => panic!("expected first grouped message, got {response:?}"),
    };

    assert!(matches!(
        request(
            server.broker_addr,
            Request::PollGroup {
                stream: "jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
            },
        ),
        Response::Message { offset: 1, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::AckGroup {
                stream: "jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first_token,
            },
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));
}

#[test]
fn network_protocol_reports_attempts_and_dead_letters_after_limit() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start_with_args(
        directory.path(),
        &["--ack-timeout-ms", "10", "--max-delivery-attempts", "2"],
    );

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "events".to_owned(),
                key: Some("order-1".to_owned()),
                payload: "poison".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Message {
            offset: 0,
            delivery_attempt: Some(1),
            ..
        }
    ));
    sleep(Duration::from_millis(20));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Message {
            offset: 0,
            delivery_attempt: Some(2),
            ..
        }
    ));
    sleep(Duration::from_millis(20));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Empty { .. }
    ));

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
            },
        ),
        Response::Message {
            offset: 0,
            payload,
            ..
        } if payload == "poison"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Ack {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 0,
            },
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));
}

#[test]
fn network_protocol_recovers_dead_letter_without_source_redelivery_after_restart() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start_with_args(
        directory.path(),
        &["--ack-timeout-ms", "10", "--max-delivery-attempts", "1"],
    );

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "events".to_owned(),
                key: Some("order-1".to_owned()),
                payload: "poison".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Message {
            offset: 0,
            delivery_attempt: Some(1),
            ..
        }
    ));
    wait_for_empty_poll(server.broker_addr, "events", "worker");
    server.stop();

    let server = RunningServer::start_with_args(
        directory.path(),
        &["--ack-timeout-ms", "10", "--max-delivery-attempts", "1"],
    );
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Empty { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
            },
        ),
        Response::Message {
            offset: 0,
            key: Some(key),
            payload,
            ..
        } if key == "order-1" && payload == "poison"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Ack {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 0,
            },
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
            },
        ),
        Response::Empty { .. }
    ));
}

#[test]
fn network_protocol_reassigns_group_delivery_after_restart() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "jobs".to_owned(),
                key: Some("order-1".to_owned()),
                payload: "recover-me".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    let first_token = match request(
        server.broker_addr,
        Request::PollGroup {
            stream: "jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
        },
    ) {
        Response::Message {
            offset: 0,
            delivery_attempt: Some(1),
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected first grouped delivery, got {response:?}"),
    };
    server.stop();

    let server = RunningServer::start(directory.path());
    let second_token = match request(
        server.broker_addr,
        Request::PollGroup {
            stream: "jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-b".to_owned(),
        },
    ) {
        Response::Message {
            offset: 0,
            delivery_attempt: Some(2),
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected reassigned grouped delivery, got {response:?}"),
    };
    assert!(matches!(
        request(
            server.broker_addr,
            Request::AckGroup {
                stream: "jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first_token,
            },
        ),
        Response::Error { code, .. } if code == "stale_delivery"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::AckGroup {
                stream: "jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
                offset: 0,
                delivery_token: second_token,
            },
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));
}

#[test]
fn network_protocol_recovers_binary_publish_batch_and_request_ids() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    assert!(matches!(
        request(
            server.broker_addr,
            Request::PublishBatch {
                stream: "events".to_owned(),
                records: vec![
                    PublishBatchRecord {
                        key: Some("order-1".to_owned()),
                        payload_base64: BinaryPayload::new(vec![0, 1, 255]),
                        request_id: Some("batch-1".to_owned()),
                    },
                    PublishBatchRecord {
                        key: None,
                        payload_base64: BinaryPayload::new(b"second".to_vec()),
                        request_id: Some("batch-2".to_owned()),
                    },
                ],
            },
        ),
        Response::PublishBatch { outcomes, .. }
            if matches!(outcomes.as_slice(), [
                PublishBatchRecordResponse::Published { offset: 0 },
                PublishBatchRecordResponse::Published { offset: 1 },
            ])
    ));
    server.stop();

    let server = RunningServer::start(directory.path());
    assert!(matches!(
        request(
            server.broker_addr,
            Request::PublishBatch {
                stream: "events".to_owned(),
                records: vec![
                    PublishBatchRecord {
                        key: Some("different-key".to_owned()),
                        payload_base64: BinaryPayload::new(b"different".to_vec()),
                        request_id: Some("batch-1".to_owned()),
                    },
                    PublishBatchRecord {
                        key: None,
                        payload_base64: BinaryPayload::new(b"different".to_vec()),
                        request_id: Some("batch-2".to_owned()),
                    },
                ],
            },
        ),
        Response::PublishBatch { outcomes, .. }
            if matches!(outcomes.as_slice(), [
                PublishBatchRecordResponse::Published { offset: 0 },
                PublishBatchRecordResponse::Published { offset: 1 },
            ])
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::MessageBytes {
            offset: 0,
            payload_base64,
            ..
        } if payload_base64.as_bytes() == [0, 1, 255]
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
        ),
        Response::Acknowledged { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::Message {
            offset: 1,
            payload,
            ..
        } if payload == "second"
    ));
}

#[test]
fn network_protocol_rejects_publish_batches_over_record_bound() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());
    let records = (0..=MAX_PUBLISH_BATCH_RECORDS)
        .map(|_| PublishBatchRecord {
            key: None,
            payload_base64: BinaryPayload::new(Vec::new()),
            request_id: None,
        })
        .collect();
    assert!(matches!(
        request(
            server.broker_addr,
            Request::PublishBatch {
                stream: "events".to_owned(),
                records,
            },
        ),
        Response::Error { code, message }
            if code == "invalid_request" && message.contains("more than")
    ));
}

#[test]
fn network_protocol_returns_partial_publish_batch_outcomes() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());
    assert!(matches!(
        request(
            server.broker_addr,
            Request::PublishBatch {
                stream: "events".to_owned(),
                records: vec![
                    PublishBatchRecord {
                        key: None,
                        payload_base64: BinaryPayload::new(b"rejected".to_vec()),
                        request_id: Some("x".repeat(1_025)),
                    },
                    PublishBatchRecord {
                        key: None,
                        payload_base64: BinaryPayload::new(b"accepted".to_vec()),
                        request_id: Some("accepted".to_owned()),
                    },
                ],
            },
        ),
        Response::PublishBatch { outcomes, .. }
            if matches!(outcomes.as_slice(), [
                PublishBatchRecordResponse::Error { code, .. },
                PublishBatchRecordResponse::Published { offset: 0 },
            ] if code == "invalid_record")
    ));
}

fn request(address: SocketAddr, request: Request) -> Response {
    let encoded = serde_json::to_string(&request).unwrap();
    request_line(address, &encoded)
}

fn request_line(address: SocketAddr, encoded: &str) -> Response {
    let mut stream = TcpStream::connect(address).expect("broker should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout should be set");
    writeln!(stream, "{encoded}").unwrap();
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).unwrap();
    serde_json::from_str(&response).unwrap()
}

fn free_addr() -> SocketAddr {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
}

fn http_metrics(address: SocketAddr) -> String {
    let mut stream =
        TcpStream::connect(address).expect("metrics endpoint should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("metrics read timeout should be set");
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("metrics request should be written");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("metrics response should be readable");
    response
        .split_once("\r\n\r\n")
        .map_or(response.clone(), |(_, body)| body.to_owned())
}

fn metric_value(metrics: &str, name: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn labeled_metric_value(metrics: &str, name: &str, labels: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name}{{{labels}}} ")))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn wait_for_http(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok(mut stream) = TcpStream::connect(address) {
            stream
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            stream
                .write_all(
                    b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            let mut response = String::new();
            if BufReader::new(stream).read_line(&mut response).is_ok() && response.contains("200") {
                return;
            }
        }
        sleep(Duration::from_millis(25));
    }
    panic!("runnel HTTP endpoint did not become ready");
}

fn wait_for_empty_poll(address: SocketAddr, stream: &str, consumer: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut last_response = None;
    while Instant::now() < deadline {
        let response = request(
            address,
            Request::Poll {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
            },
        );
        if matches!(response, Response::Empty { .. }) {
            return;
        }
        last_response = Some(response);
        sleep(Duration::from_millis(25));
    }
    panic!(
        "{stream}/{consumer} did not become empty before the deadline; last response: {last_response:?}"
    );
}

fn wait_for_metric_at_least(address: SocketAddr, name: &str, expected: u64) -> String {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let metrics = http_metrics(address);
        if metric_value(&metrics, name) >= expected {
            return metrics;
        }
        if Instant::now() >= deadline {
            panic!("metric {name} did not reach {expected}");
        }
        sleep(Duration::from_millis(25));
    }
}
