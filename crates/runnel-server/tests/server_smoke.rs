use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use base64::Engine as _;
use runnel_client::{Client, ClientConfig, ClientSecurityConfig, ClientTlsConfig};
use runnel_protocol::{
    BinaryPayload, MAX_PUBLISH_BATCH_RECORDS, PublishBatchRecord, PublishBatchRecordResponse,
    Request, Response,
};
use sha2::Digest as _;
use tempfile::TempDir;
use zeroize::Zeroize;

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
        let child = server_command(data_dir, broker_addr, http_addr, extra_args)
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

    fn start_on(
        data_dir: &Path,
        broker_bind_addr: SocketAddr,
        broker_connect_addr: SocketAddr,
        extra_args: &[&str],
    ) -> Self {
        let http_addr = free_addr();
        let child = server_command(data_dir, broker_bind_addr, http_addr, extra_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("runnel server process should start");
        wait_for_http(http_addr);
        Self {
            child,
            broker_addr: broker_connect_addr,
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

fn server_command(
    data_dir: &Path,
    broker_addr: SocketAddr,
    http_addr: SocketAddr,
    extra_args: &[&str],
) -> Command {
    let mut command = Command::new(server_binary());
    command.args([
        "--data-dir",
        data_dir.to_str().expect("temporary path should be UTF-8"),
        "--listen",
        &broker_addr.to_string(),
        "--http-listen",
        &http_addr.to_string(),
    ]);
    command.args(extra_args);
    command
}

fn run_startup(data_dir: &Path, broker_addr: SocketAddr, extra_args: &[&str]) -> Output {
    server_command(data_dir, broker_addr, free_addr(), extra_args)
        .output()
        .expect("runnel startup should run")
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
    let payload = vec![0, 1, 255, b'\n', b'_'];

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
        request(
            server.broker_addr,
            Request::PublishBytes {
                stream: "binary".to_owned(),
                key: None,
                payload: BinaryPayload::new(payload.clone()),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "binary".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::MessageBytes {
            offset: 0,
            payload: received,
            ..
        } if received.as_bytes() == payload
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
            payload: received,
            ..
        } if received.as_bytes() == payload
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "binary".to_owned(),
                consumer: "worker".to_owned(),
            },
        ),
        Response::MessageBytes {
            offset: 0,
            payload: received,
            ..
        } if received.as_bytes() == payload
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
fn network_protocol_rejects_json_lines_without_processing_the_request() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());
    let mut stream = TcpStream::connect(server.broker_addr).expect("broker should accept TCP");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout should be set");
    stream
        .write_all(b"{\"op\":\"create_stream\",\"stream\":\"events\"}\n")
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    assert!(response.is_empty(), "v1 JSON must not receive a response");

    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Health { streams: 0, .. }
    ));
}

#[test]
fn remote_plaintext_startup_is_rejected_without_the_development_override() {
    let directory = TempDir::new().unwrap();
    let output = run_startup(directory.path(), "0.0.0.0:0".parse().unwrap(), &[]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("non-loopback application listener"));

    let connect_addr = free_addr();
    let bind_addr = SocketAddr::from(([0, 0, 0, 0], connect_addr.port()));
    let server = RunningServer::start_on(
        directory.path(),
        bind_addr,
        connect_addr,
        &["--insecure-development-listen"],
    );
    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Health { .. }
    ));
}

#[test]
fn invalid_security_policy_fails_before_the_listener_accepts_connections() {
    let directory = TempDir::new().unwrap();
    let (certificate, key, policy, _) =
        write_application_security_files(directory.path(), &[("operator", "operator")]);
    write_private_file(&policy, br#"{"credentials":[]}"#);
    let broker_addr = free_addr();
    let output = run_startup(
        directory.path(),
        broker_addr,
        &[
            "--app-tls-cert",
            certificate.to_str().unwrap(),
            "--app-tls-key",
            key.to_str().unwrap(),
            "--credential-policy",
            policy.to_str().unwrap(),
        ],
    );
    assert!(!output.status.success());
    let startup_error = String::from_utf8_lossy(&output.stderr);
    assert!(
        startup_error.contains("application credential policy is invalid"),
        "unexpected startup error: {startup_error}"
    );
    assert!(TcpStream::connect(broker_addr).is_err());
}

#[test]
fn application_tls_and_credentials_are_required_together_and_not_supported_in_raft_mode() {
    let directory = TempDir::new().unwrap();
    let certificate = directory.path().join("not-read.pem");
    let key = directory.path().join("not-read.key");
    let output = run_startup(
        directory.path(),
        free_addr(),
        &[
            "--app-tls-cert",
            certificate.to_str().unwrap(),
            "--app-tls-key",
            key.to_str().unwrap(),
        ],
    );
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("--credential-policy must be configured together")
    );

    let output = run_startup(
        directory.path(),
        free_addr(),
        &[
            "--engine",
            "raft",
            "--app-tls-cert",
            certificate.to_str().unwrap(),
            "--app-tls-key",
            key.to_str().unwrap(),
            "--credential-policy",
            "not-read.json",
        ],
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported with --engine raft"));
}

#[test]
fn real_server_tls_authenticates_before_dispatch_and_enforces_roles() {
    let directory = TempDir::new().unwrap();
    let (certificate, key, policy, token_paths) = write_application_security_files(
        directory.path(),
        &[("operator", "operator"), ("application", "application")],
    );
    let broker_connect_addr = free_addr();
    let broker_bind_addr = SocketAddr::from(([0, 0, 0, 0], broker_connect_addr.port()));
    let secure_args = [
        "--app-tls-cert",
        certificate.to_str().unwrap(),
        "--app-tls-key",
        key.to_str().unwrap(),
        "--credential-policy",
        policy.to_str().unwrap(),
        "--insecure-development-listen",
    ];
    let server = RunningServer::start_on(
        directory.path(),
        broker_bind_addr,
        broker_connect_addr,
        &secure_args,
    );

    let mut plaintext = TcpStream::connect(server.broker_addr).unwrap();
    plaintext
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    plaintext.write_all(&runnel_protocol::v2::PREFACE).unwrap();
    let mut plaintext_response = Vec::new();
    let read_result = plaintext.read_to_end(&mut plaintext_response);
    assert!(
        plaintext_response.is_empty() || plaintext_response[0] != 0,
        "the TLS listener must not return a length-prefixed v2 frame to plaintext"
    );
    assert!(
        read_result.is_ok()
            || matches!(
                &read_result,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::UnexpectedEof
                    )
            ),
        "TLS listener did not close the rejected plaintext connection: {read_result:?}"
    );
    drop(plaintext);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let tls = ClientTlsConfig::new("localhost").with_ca_file(&certificate);
    let missing_credentials = ClientSecurityConfig::plaintext_development().with_tls(tls.clone());
    assert!(matches!(
        runtime.block_on(Client::connect_with_security(
            server.broker_addr,
            ClientConfig::default(),
            missing_credentials,
        )),
        Err(runnel_client::ClientError::AuthenticationRequired)
    ));

    let (unknown_token, _) = generate_test_credential();
    let unknown_token_path = directory.path().join("unknown.token");
    write_private_file(&unknown_token_path, unknown_token.as_bytes());
    let mut unknown_token = unknown_token;
    unknown_token.zeroize();
    let unknown_credentials = ClientSecurityConfig::plaintext_development()
        .with_tls(tls.clone())
        .with_token_file(&unknown_token_path)
        .unwrap();
    assert!(matches!(
        runtime.block_on(Client::connect_with_security(
            server.broker_addr,
            ClientConfig::default(),
            unknown_credentials,
        )),
        Err(runnel_client::ClientError::AuthenticationFailed)
    ));

    let wrong_name = ClientSecurityConfig::plaintext_development()
        .with_tls(ClientTlsConfig::new("wrong.example").with_ca_file(&certificate))
        .with_token_file(&token_paths[0])
        .unwrap();
    assert!(matches!(
        runtime.block_on(Client::connect_with_security(
            server.broker_addr,
            ClientConfig::default(),
            wrong_name,
        )),
        Err(runnel_client::ClientError::TlsHandshake)
    ));

    let application_security = ClientSecurityConfig::plaintext_development()
        .with_tls(tls.clone())
        .with_token_file(&token_paths[1])
        .unwrap();
    let mut application = runtime
        .block_on(Client::connect_with_security(
            server.broker_addr,
            ClientConfig::default(),
            application_security,
        ))
        .unwrap();
    assert!(matches!(
        runtime.block_on(application.request(&Request::CreateStream {
            stream: "denied".to_owned(),
        })),
        Ok(Response::Error { code, .. }) if code == "authorization_denied"
    ));

    let operator_security = ClientSecurityConfig::plaintext_development()
        .with_tls(tls)
        .with_token_file(&token_paths[0])
        .unwrap();
    let mut operator = runtime
        .block_on(Client::connect_with_security(
            server.broker_addr,
            ClientConfig::default(),
            operator_security,
        ))
        .unwrap();
    assert!(matches!(
        runtime.block_on(operator.request(&Request::Health)),
        Ok(Response::Health { streams: 0, .. })
    ));
    assert!(matches!(
        runtime.block_on(operator.request(&Request::CreateStream {
            stream: "events".to_owned(),
        })),
        Ok(Response::StreamCreated { created: true, .. })
    ));
    assert!(matches!(
        runtime.block_on(application.request(&Request::Publish {
            stream: "events".to_owned(),
            key: None,
            payload: "authorized".to_owned(),
            request_id: None,
        })),
        Ok(Response::Published { offset: 0, .. })
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
fn metrics_report_process_uptime() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());

    let initial_metrics = http_metrics(server.http_addr);
    let initial_uptime = metric_float_value(&initial_metrics, "runnel_process_uptime_seconds");
    assert!(initial_uptime.is_finite());
    assert!(initial_uptime >= 0.0);
    assert!(initial_metrics.contains(
        "# HELP runnel_process_uptime_seconds Seconds since this broker process started."
    ));
    assert!(initial_metrics.contains("# TYPE runnel_process_uptime_seconds gauge"));

    sleep(Duration::from_millis(20));
    let later_uptime = metric_float_value(
        &http_metrics(server.http_addr),
        "runnel_process_uptime_seconds",
    );
    assert!(
        later_uptime > initial_uptime,
        "process uptime should increase between scrapes: initial={initial_uptime}, later={later_uptime}"
    );
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
fn network_protocol_reconciles_dead_letter_after_ambiguous_poll_and_restart() {
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
    sleep(Duration::from_millis(20));

    let broker_response = request_through_dropping_proxy(
        server.broker_addr,
        Request::Poll {
            stream: "events".to_owned(),
            consumer: "worker".to_owned(),
        },
    );
    assert!(matches!(broker_response, Response::Empty { .. }));
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
            delivery_attempt: Some(1),
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
fn network_protocol_keeps_mismatching_public_dead_letter_id_separate_after_restart() {
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
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "events.dead-letter".to_owned(),
                key: Some("wrong-key".to_owned()),
                payload: "public-record".to_owned(),
                request_id: Some("runnel-dlq/v1/6:events/6:worker/0".to_owned()),
            },
        ),
        Response::Published { offset: 0, .. }
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
            Request::Replay {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 0,
            },
        ),
        Response::ReplayMessage {
            offset: 0,
            key: Some(key),
            payload,
            ..
        } if key == "wrong-key" && payload == "public-record"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 1,
            },
        ),
        Response::ReplayMessage {
            offset: 1,
            key: Some(key),
            payload,
            ..
        } if key == "order-1" && payload == "poison"
    ));
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
            Request::Publish {
                stream: "events.dead-letter".to_owned(),
                key: Some("replay-key".to_owned()),
                payload: "replay-payload".to_owned(),
                request_id: Some("runnel-dlq/v1/6:events/6:worker/0".to_owned()),
            },
        ),
        Response::Error {
            code,
            ..
        } if code == "request_id_content_conflict"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 0,
            },
        ),
        Response::ReplayMessage {
            offset: 0,
            key: Some(key),
            payload,
            ..
        } if key == "wrong-key" && payload == "public-record"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 1,
            },
        ),
        Response::ReplayMessage {
            offset: 1,
            key: Some(key),
            payload,
            ..
        } if key == "order-1" && payload == "poison"
    ));
}

#[test]
fn network_protocol_does_not_accept_same_content_public_id_as_dead_letter_move() {
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
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "events.dead-letter".to_owned(),
                key: Some("order-1".to_owned()),
                payload: "poison".to_owned(),
                request_id: Some("runnel-dlq/v1/6:events/6:worker/0".to_owned()),
            },
        ),
        Response::Published { offset: 0, .. }
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
            Request::Replay {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 0,
            },
        ),
        Response::ReplayMessage {
            offset: 0,
            key: Some(key),
            payload,
            ..
        } if key == "order-1" && payload == "poison"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 1,
            },
        ),
        Response::ReplayMessage {
            offset: 1,
            key: Some(key),
            payload,
            ..
        } if key == "order-1" && payload == "poison"
    ));
    server.stop();

    let server = RunningServer::start_with_args(
        directory.path(),
        &["--ack-timeout-ms", "10", "--max-delivery-attempts", "1"],
    );
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "events.dead-letter".to_owned(),
                key: Some("changed-key".to_owned()),
                payload: "changed payload".to_owned(),
                request_id: Some("runnel-dlq/v1/6:events/6:worker/0".to_owned()),
            },
        ),
        Response::Error {
            code,
            ..
        } if code == "request_id_content_conflict"
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
            Request::Replay {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 0,
            },
        ),
        Response::ReplayMessage {
            offset: 0,
            key: Some(key),
            payload,
            ..
        } if key == "order-1" && payload == "poison"
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Replay {
                stream: "events.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 1,
            },
        ),
        Response::ReplayMessage {
            offset: 1,
            key: Some(key),
            payload,
            ..
        } if key == "order-1" && payload == "poison"
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
                        payload: BinaryPayload::new(vec![0, 1, 255]),
                        request_id: Some("batch-1".to_owned()),
                    },
                    PublishBatchRecord {
                        key: None,
                        payload: BinaryPayload::new(b"second".to_vec()),
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
                        key: Some("order-1".to_owned()),
                        payload: BinaryPayload::new(vec![0, 1, 255]),
                        request_id: Some("batch-1".to_owned()),
                    },
                    PublishBatchRecord {
                        key: None,
                        payload: BinaryPayload::new(b"second".to_vec()),
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
            payload,
            ..
        } if payload.as_bytes() == [0, 1, 255]
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
            payload: BinaryPayload::new(Vec::new()),
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
                        payload: BinaryPayload::new(b"rejected".to_vec()),
                        request_id: Some("x".repeat(1_025)),
                    },
                    PublishBatchRecord {
                        key: None,
                        payload: BinaryPayload::new(b"accepted".to_vec()),
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
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let mut client = runnel_client::Client::connect(address).await.unwrap();
            client.request(&request).await.unwrap()
        })
}

fn request_through_dropping_proxy(address: SocketAddr, request: Request) -> Response {
    use runnel_protocol::v2::{self, ServerFrame};

    fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut length = [0; 4];
        stream.read_exact(&mut length)?;
        let length = u32::from_be_bytes(length) as usize;
        let mut body = vec![0; length];
        stream.read_exact(&mut body)?;
        Ok(body)
    }

    fn write_frame(stream: &mut TcpStream, body: &[u8]) -> std::io::Result<()> {
        let length = u32::try_from(body.len())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "frame length"))?;
        stream.write_all(&length.to_be_bytes())?;
        stream.write_all(body)
    }

    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("proxy should bind");
    let proxy_addr = listener
        .local_addr()
        .expect("proxy address should be available");
    let (response_sender, response_receiver) = mpsc::channel();
    let proxy = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().expect("proxy should accept a client");
        let mut broker = TcpStream::connect(address).expect("proxy should reach broker");
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        broker
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let mut preface = [0; runnel_protocol::v2::PREFACE.len()];
        client.read_exact(&mut preface).unwrap();
        broker.write_all(&preface).unwrap();

        let client_hello = read_frame(&mut client).unwrap();
        write_frame(&mut broker, &client_hello).unwrap();
        let server_hello = read_frame(&mut broker).unwrap();
        write_frame(&mut client, &server_hello).unwrap();

        let application_request = read_frame(&mut client).unwrap();
        write_frame(&mut broker, &application_request).unwrap();
        let application_response = read_frame(&mut broker).unwrap();
        let response = match v2::decode_server_frame(
            &application_response,
            v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES,
        )
        .unwrap()
        {
            ServerFrame::Application(reply) => reply.response,
            _ => panic!("broker should return an application response"),
        };
        response_sender
            .send(response)
            .expect("test should receive the broker response");
    });

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let mut client = runnel_client::Client::connect(proxy_addr).await.unwrap();
            let _ = client.request(&request).await;
        });

    let response = response_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("proxy should observe the broker response");
    proxy.join().expect("proxy should finish cleanly");
    response
}

fn free_addr() -> SocketAddr {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
}

fn write_application_security_files(
    directory: &Path,
    roles: &[(&str, &str)],
) -> (PathBuf, PathBuf, PathBuf, Vec<PathBuf>) {
    let certificate_path = directory.join("application-cert.pem");
    let key_path = directory.join("application-key.pem");
    let policy_path = directory.join("credentials.json");
    fs::write(
        &certificate_path,
        include_str!("fixtures/application-security-cert.pem"),
    )
    .unwrap();
    write_private_file(
        &key_path,
        include_str!("fixtures/application-security-key.pem").as_bytes(),
    );

    let mut policy_credentials = Vec::with_capacity(roles.len());
    let mut token_paths = Vec::with_capacity(roles.len());
    for (id, role) in roles {
        let (token, digest) = generate_test_credential();
        let token_path = directory.join(format!("{id}.token"));
        write_private_file(&token_path, token.as_bytes());
        let mut token = token;
        token.zeroize();
        token_paths.push(token_path);
        policy_credentials.push(format!(
            r#"{{"id":"{id}","sha256":"{digest}","role":"{role}"}}"#
        ));
    }
    write_private_file(
        &policy_path,
        format!(r#"{{"credentials":[{}]}}"#, policy_credentials.join(",")).as_bytes(),
    );
    (certificate_path, key_path, policy_path, token_paths)
}

fn generate_test_credential() -> (String, String) {
    let mut random = [0_u8; 32];
    getrandom::fill(&mut random).expect("test credential randomness should be available");
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random);
    random.fill(0);
    let digest = sha2::Sha256::digest(token.as_bytes());
    let digest = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    (token, digest)
}

fn write_private_file(path: &Path, content: &[u8]) {
    fs::write(path, content).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
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

fn metric_float_value(metrics: &str, name: &str) -> f64 {
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
