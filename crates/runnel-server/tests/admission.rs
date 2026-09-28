#[cfg(unix)]
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
#[cfg(unix)]
use std::sync::mpsc::{self, Sender};
#[cfg(unix)]
use std::thread::JoinHandle;
use std::thread::sleep;
use std::time::{Duration, Instant};

use runnel_protocol::{Request, Response};
use tempfile::TempDir;

struct RunningServer {
    child: Child,
    broker_addr: SocketAddr,
    http_addr: SocketAddr,
}

impl RunningServer {
    fn start(data_dir: &Path, extra_args: &[&str]) -> Self {
        let broker_addr = free_addr();
        let http_addr = free_addr();
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
        let mut child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("runnel server should start");
        if let Err(error) = wait_for_http(http_addr) {
            // `Child` does not kill or reap itself when dropped before the
            // RunningServer guard is constructed.
            let _ = child.kill();
            let _ = child.wait();
            panic!("{error}");
        }
        Self {
            child,
            broker_addr,
            http_addr,
        }
    }
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
fn configured_admission_limits_are_exposed_as_gauges() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--max-connections",
            "3",
            "--max-request-bytes",
            "2048",
            "--max-in-flight-requests",
            "7",
            "--request-timeout-ms",
            "1250",
        ],
    );

    let metrics = http_metrics(server.http_addr);
    assert_eq!(metric_value(&metrics, "runnel_broker_max_connections"), 3);
    assert_eq!(
        metric_value(&metrics, "runnel_broker_max_request_bytes"),
        2048
    );
    assert_eq!(
        metric_value(&metrics, "runnel_broker_max_in_flight_requests"),
        7
    );
    assert_eq!(
        metric_float_value(&metrics, "runnel_broker_request_timeout_seconds"),
        1.25
    );
    assert!(metrics.contains("# TYPE runnel_broker_max_connections gauge"));
    assert!(metrics.contains("# TYPE runnel_broker_max_request_bytes gauge"));
    assert!(metrics.contains("# TYPE runnel_broker_max_in_flight_requests gauge"));
    assert!(metrics.contains("# TYPE runnel_broker_request_timeout_seconds gauge"));
}

#[test]
fn connection_flood_is_rejected_and_durable_traffic_recovers() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &["--max-connections", "2", "--request-timeout-ms", "500"],
    );

    let first = TcpStream::connect(server.broker_addr).unwrap();
    let second = TcpStream::connect(server.broker_addr).unwrap();
    wait_for_metric_at_least(server.http_addr, "runnel_active_connections", 2);

    let mut rejected = TcpStream::connect(server.broker_addr).unwrap();
    rejected
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut response = String::new();
    BufReader::new(&mut rejected)
        .read_line(&mut response)
        .expect("connection rejection response should be readable");
    assert!(
        matches!(decode_response(&response), Response::Error { code, .. } if code == "connection_limit")
    );

    let metrics = wait_for_metric_at_least(
        server.http_addr,
        "runnel_broker_connections_rejected_total",
        1,
    );
    assert_eq!(metric_value(&metrics, "runnel_active_connections"), 2);

    drop(first);
    drop(second);
    wait_for_metric_at_most(server.http_addr, "runnel_active_connections", 0);
    let mut recovered = TcpStream::connect(server.broker_addr).unwrap();
    recovered
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert!(matches!(
        send_on_connection(
            &mut recovered,
            Request::CreateStream {
                stream: "admission-recovery".to_owned(),
            },
        ),
        Response::StreamCreated { .. }
    ));
    assert!(matches!(
        send_on_connection(
            &mut recovered,
            Request::Publish {
                stream: "admission-recovery".to_owned(),
                key: None,
                payload: "durable-after-flood".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        send_on_connection(&mut recovered, Request::Health),
        Response::Health { .. }
    ));
}

#[test]
fn oversized_unterminated_frame_is_bounded_and_rejected() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &["--max-request-bytes", "128", "--request-timeout-ms", "500"],
    );

    let mut connection = TcpStream::connect(server.broker_addr).unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    connection
        .write_all(&[b'x'; 129])
        .expect("oversized frame should be writable");
    let mut response = String::new();
    BufReader::new(&mut connection)
        .read_line(&mut response)
        .expect("oversized frame response should be readable");
    assert!(
        matches!(decode_response(&response), Response::Error { code, .. } if code == "request_too_large")
    );

    let metrics = wait_for_metric_at_least(
        server.http_addr,
        "runnel_broker_request_size_rejections_total",
        1,
    );
    assert_eq!(
        metric_value(&metrics, "runnel_broker_requests_rejected_total"),
        1
    );

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "after".to_owned(),
                key: None,
                payload: "ok".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
}

#[test]
fn partial_request_times_out_and_unrelated_health_recovers() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--request-timeout-ms",
            // Leave enough budget for a cold durable append on slower CI hosts;
            // the incomplete frame still times out deterministically.
            "500",
            "--max-in-flight-requests",
            "1",
        ],
    );

    let mut slow = TcpStream::connect(server.broker_addr).unwrap();
    slow.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let metrics_before = http_metrics(server.http_addr);
    let requests_rejected_before =
        metric_value(&metrics_before, "runnel_broker_requests_rejected_total");
    slow.write_all(br#"{"op":"health"}"#).unwrap();
    let mut response = String::new();
    BufReader::new(&mut slow)
        .read_line(&mut response)
        .expect("timed-out request response should be readable");
    assert!(
        matches!(decode_response(&response), Response::Error { code, .. } if code == "request_timeout")
    );

    let metrics =
        wait_for_metric_at_least(server.http_addr, "runnel_broker_request_timeouts_total", 1);
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 0);
    assert_eq!(
        metric_value(&metrics, "runnel_broker_requests_rejected_total"),
        requests_rejected_before + 1
    );

    let mut recovered = TcpStream::connect(server.broker_addr).unwrap();
    recovered
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert!(matches!(
        send_on_connection(
            &mut recovered,
            Request::Publish {
                stream: "after-timeout".to_owned(),
                key: None,
                payload: "health-and-durable-traffic".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        send_on_connection(&mut recovered, Request::Health),
        Response::Health { .. }
    ));
}

#[test]
fn slow_reader_does_not_consume_in_flight_capacity() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--request-timeout-ms",
            "500",
            "--max-in-flight-requests",
            "1",
        ],
    );

    let mut slow_reader = TcpStream::connect(server.broker_addr).unwrap();
    slow_reader
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    slow_reader
        .write_all(br#"{"op":"health"}"#)
        .expect("partial request should be writable");

    let started = Instant::now();
    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Health { .. }
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "a slow reader must not consume the only in-flight request permit"
    );

    let mut response = String::new();
    BufReader::new(&mut slow_reader)
        .read_line(&mut response)
        .expect("slow reader timeout response should be readable");
    assert!(
        matches!(decode_response(&response), Response::Error { code, .. } if code == "request_timeout")
    );

    let metrics =
        wait_for_metric_at_least(server.http_addr, "runnel_broker_request_timeouts_total", 1);
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 0);
}

#[test]
fn slow_writer_and_in_flight_admission_are_bounded() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--max-request-bytes",
            "8388608",
            "--request-timeout-ms",
            "500",
            "--max-in-flight-requests",
            "1",
        ],
    );
    let payload = "x".repeat(6 * 1024 * 1024);

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "slow-writer".to_owned(),
            },
        ),
        Response::StreamCreated { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "slow-writer".to_owned(),
                key: None,
                payload,
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));

    let metrics_before = http_metrics(server.http_addr);
    let request_timeouts_before =
        metric_value(&metrics_before, "runnel_broker_request_timeouts_total");
    let requests_rejected_before =
        metric_value(&metrics_before, "runnel_broker_requests_rejected_total");
    let request_saturation_before =
        metric_value(&metrics_before, "runnel_broker_request_saturation_total");
    let response_write_timeouts_before = metric_value(
        &metrics_before,
        "runnel_broker_response_write_timeouts_total",
    );

    let mut slow_writer = TcpStream::connect(server.broker_addr).unwrap();
    slow_writer
        .write_all(
            &serde_json::to_vec(&Request::Poll {
                stream: "slow-writer".to_owned(),
                consumer: "unread".to_owned(),
            })
            .unwrap(),
        )
        .unwrap();
    slow_writer.write_all(b"\n").unwrap();

    let metrics = wait_for_metric_at_least(server.http_addr, "runnel_active_requests", 1);
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 1);

    let started = Instant::now();
    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Error { code, .. } if code == "request_saturated"
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "a full in-flight budget must reject unrelated work promptly"
    );
    let metrics = wait_for_metric_at_least(
        server.http_addr,
        "runnel_broker_request_saturation_total",
        request_saturation_before + 1,
    );
    assert_eq!(
        metric_value(&metrics, "runnel_broker_requests_rejected_total"),
        requests_rejected_before + 1
    );
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 1);

    let metrics = wait_for_metric_at_least(
        server.http_addr,
        "runnel_broker_response_write_timeouts_total",
        response_write_timeouts_before + 1,
    );
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 0);

    let started = Instant::now();
    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Health { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "after-slow-writer".to_owned(),
                key: None,
                payload: "durable-after-slow-writer".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "independent health and durable traffic must not wait for a slow response writer"
    );

    let metrics = http_metrics(server.http_addr);
    assert_eq!(
        metric_value(&metrics, "runnel_broker_request_timeouts_total"),
        request_timeouts_before,
        "response-write timeout must not be reported as a request timeout"
    );
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 0);
    wait_for_metric_at_most(server.http_addr, "runnel_active_connections", 0);
}

#[test]
fn sustained_in_flight_pressure_reports_metrics_and_recovers() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--max-connections",
            "4",
            "--max-request-bytes",
            "8388608",
            "--request-timeout-ms",
            "1000",
            "--max-in-flight-requests",
            "1",
        ],
    );
    let payload = "x".repeat(6 * 1024 * 1024);

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "sustained-pressure".to_owned(),
            },
        ),
        Response::StreamCreated { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "sustained-pressure".to_owned(),
                key: None,
                payload,
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));

    let metrics_before = http_metrics(server.http_addr);
    let saturation_before = metric_value(&metrics_before, "runnel_broker_request_saturation_total");
    let requests_rejected_before =
        metric_value(&metrics_before, "runnel_broker_requests_rejected_total");
    let request_timeouts_before =
        metric_value(&metrics_before, "runnel_broker_request_timeouts_total");
    let response_write_timeouts_before = metric_value(
        &metrics_before,
        "runnel_broker_response_write_timeouts_total",
    );
    assert_metric_type(&metrics_before, "runnel_active_requests", "gauge");
    assert_metric_type(
        &metrics_before,
        "runnel_broker_requests_rejected_total",
        "counter",
    );
    assert_metric_type(
        &metrics_before,
        "runnel_broker_request_timeouts_total",
        "counter",
    );
    assert_metric_type(
        &metrics_before,
        "runnel_broker_request_saturation_total",
        "counter",
    );

    let mut slow_writer = TcpStream::connect(server.broker_addr).unwrap();
    slow_writer
        .write_all(
            &serde_json::to_vec(&Request::Poll {
                stream: "sustained-pressure".to_owned(),
                consumer: "unread".to_owned(),
            })
            .unwrap(),
        )
        .unwrap();
    slow_writer.write_all(b"\n").unwrap();
    wait_for_metric_at_least(server.http_addr, "runnel_active_requests", 1);

    let mut pressure_connections = Vec::new();
    for _ in 0..3 {
        let connection = TcpStream::connect(server.broker_addr).unwrap();
        connection
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        pressure_connections.push(connection);
    }
    let metrics = wait_for_metric_at_least(server.http_addr, "runnel_active_connections", 4);
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 1);

    let attempts = 8;
    for _ in 0..attempts {
        for connection in &mut pressure_connections {
            assert!(matches!(
                send_on_connection(connection, Request::Health),
                Response::Error { code, .. } if code == "request_saturated"
            ));
        }
        let metrics = http_metrics(server.http_addr);
        assert_eq!(metric_value(&metrics, "runnel_active_requests"), 1);
    }

    let expected_rejections = (attempts * pressure_connections.len()) as u64;
    let metrics = wait_for_metric_at_least(
        server.http_addr,
        "runnel_broker_request_saturation_total",
        saturation_before + expected_rejections,
    );
    assert_eq!(
        metric_value(&metrics, "runnel_broker_requests_rejected_total"),
        requests_rejected_before + expected_rejections
    );
    assert_eq!(
        metric_value(&metrics, "runnel_broker_request_timeouts_total"),
        request_timeouts_before,
        "saturation rejections must not be counted as request timeouts"
    );
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 1);
    assert_eq!(metric_value(&metrics, "runnel_active_connections"), 4);

    let metrics = wait_for_metric_at_least(
        server.http_addr,
        "runnel_broker_response_write_timeouts_total",
        response_write_timeouts_before + 1,
    );
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 0);
    assert_eq!(
        metric_value(&metrics, "runnel_active_connections"),
        3,
        "only the timed-out slow-writer connection should be closed"
    );
    assert_eq!(
        metric_value(&metrics, "runnel_broker_request_timeouts_total"),
        request_timeouts_before,
        "a response-write timeout must remain distinct from request execution timeout"
    );

    assert!(matches!(
        send_on_connection(&mut pressure_connections[0], Request::Health),
        Response::Health { .. }
    ));
    assert!(matches!(
        send_on_connection(
            &mut pressure_connections[0],
            Request::Publish {
                stream: "after-sustained-pressure".to_owned(),
                key: None,
                payload: "durable-recovery".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        send_on_connection(
            &mut pressure_connections[0],
            Request::Poll {
                stream: "after-sustained-pressure".to_owned(),
                consumer: "recovery-reader".to_owned(),
            },
        ),
        Response::Message { payload, .. } if payload == "durable-recovery"
    ));
    let metrics = http_metrics(server.http_addr);
    assert_eq!(metric_value(&metrics, "runnel_active_requests"), 0);
    assert_eq!(metric_value(&metrics, "runnel_active_connections"), 3);
    assert_eq!(
        metric_value(&metrics, "runnel_broker_requests_rejected_total"),
        requests_rejected_before + expected_rejections
    );
    assert_eq!(
        metric_value(&metrics, "runnel_broker_request_saturation_total"),
        saturation_before + expected_rejections
    );
    assert_eq!(
        metric_value(&metrics, "runnel_broker_request_timeouts_total"),
        request_timeouts_before
    );
}

#[cfg(unix)]
#[test]
fn sustained_storage_pressure_is_bounded_observable_and_recovers() {
    const STREAM_QUEUE_CAPACITY: usize = 32;
    const WAITING_REQUESTS: usize = STREAM_QUEUE_CAPACITY + 1;

    let directory = TempDir::new().unwrap();
    let fifo = directory
        .path()
        .join("consumers/storage-pressure/blocked.json.tmp");
    let mut fifos = StoragePressureFifos::default();
    let mut clients = StoragePressureClients::default();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--max-connections",
            "64",
            "--max-in-flight-requests",
            "64",
            "--request-timeout-ms",
            "15000",
        ],
    );

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "storage-pressure".to_owned(),
            },
        ),
        Response::StreamCreated { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "storage-pressure".to_owned(),
                key: None,
                payload: "storage pressure probe".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "storage-pressure-unrelated".to_owned(),
            },
        ),
        Response::StreamCreated { .. }
    ));
    fifos.create(&fifo);

    let metrics_before = http_metrics(server.http_addr);
    let poll_failures_before = labeled_metric_value(
        &metrics_before,
        "runnel_broker_request_failures_total",
        "operation=\"poll\"",
    );
    let request_timeouts_before =
        metric_value(&metrics_before, "runnel_broker_request_timeouts_total");
    let (response_sender, response_receiver) = mpsc::channel();

    let blocked_address = server.broker_addr;
    let blocked_response_sender = response_sender.clone();
    clients.push_started(std::thread::spawn(move || {
        let response = try_request(
            blocked_address,
            Request::Poll {
                stream: "storage-pressure".to_owned(),
                consumer: "blocked".to_owned(),
            },
            Duration::from_secs(20),
        );
        let _ = blocked_response_sender.send((usize::MAX, response));
        usize::MAX
    }));
    let blocker_writer = fs::OpenOptions::new()
        .write(true)
        .open(&fifo)
        .expect("test writer should pair with the blocked journal reader");
    wait_for_metric_at_least(server.http_addr, "runnel_active_requests", 1);

    for index in 0..WAITING_REQUESTS {
        let (start_sender, start_receiver) = mpsc::channel();
        let response_sender = response_sender.clone();
        let address = server.broker_addr;
        clients.push(
            start_sender,
            std::thread::spawn(move || {
                start_receiver
                    .recv()
                    .expect("pressure clients should be released together");
                let response = try_request(
                    address,
                    Request::Poll {
                        stream: "storage-pressure".to_owned(),
                        consumer: format!("queued-{index}"),
                    },
                    Duration::from_secs(20),
                );
                let _ = response_sender.send((index, response));
                index
            }),
        );
    }
    drop(response_sender);
    clients.start();

    let (rejected_index, rejected_response) = response_receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("the full per-stream storage queue should reject excess work");
    let rejected_response = rejected_response.unwrap_or_else(|error| {
        panic!("overflow client should receive a protocol response: {error}")
    });
    match rejected_response {
        Response::Error { code, message } => {
            assert_eq!(code, "storage_error");
            assert!(
                message.contains("storage stream queue is full"),
                "storage admission rejection should explain the full queue: {message}"
            );
        }
        response => panic!("expected explicit storage overload response, got {response:?}"),
    }

    let saturation_metrics = wait_for_metric_at_least(
        server.http_addr,
        "runnel_active_requests",
        (STREAM_QUEUE_CAPACITY + 1) as u64,
    );
    assert_eq!(
        metric_value(&saturation_metrics, "runnel_active_requests"),
        (STREAM_QUEUE_CAPACITY + 1) as u64,
        "one active storage operation and the bounded stream waiter queue should remain active"
    );
    // Health dispatch skips stream-lane admission, but Broker::health inspects
    // each stream under its mutex. The blocked poll holds this stream mutex
    // while reading the FIFO, so health times out even though the queued lane
    // requests have not acquired global execution permits.
    assert_eq!(
        metric_value(&saturation_metrics, "runnel_engine_health_available"),
        0,
        "health should time out while inspecting the stream locked by the blocked poll"
    );
    assert!(
        metric_value(&saturation_metrics, "runnel_metrics_scrape_failures_total")
            > metric_value(&metrics_before, "runnel_metrics_scrape_failures_total"),
        "metrics should count the bounded health timeout while remaining scrapeable"
    );
    assert_eq!(
        metric_value(&saturation_metrics, "runnel_broker_request_timeouts_total"),
        request_timeouts_before,
        "storage queue pressure should reject promptly instead of timing out"
    );

    let started = Instant::now();
    let readiness = http_ready(server.http_addr);
    assert!(readiness.starts_with("HTTP/1.1 503"));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "readiness should report the lock-blocked health check within its deadline"
    );
    let started = Instant::now();
    assert!(http_liveness(server.http_addr).starts_with("HTTP/1.1 200"));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "process liveness should remain available while engine health is lock-blocked"
    );
    let started = Instant::now();
    let scrape = http_metrics_response(server.http_addr);
    assert!(scrape.starts_with("HTTP/1.1 200"));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "metrics should remain scrapeable while one stream queue is full"
    );
    assert_eq!(
        metric_value(response_body(&scrape), "runnel_engine_health_available"),
        0
    );
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "storage-pressure-unrelated".to_owned(),
                key: None,
                payload: "unrelated-durable-traffic".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));

    let mut retry_connection = TcpStream::connect(server.broker_addr).unwrap();
    retry_connection
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    for attempt in 0..24 {
        let started = Instant::now();
        assert!(matches!(
            send_on_connection(
                &mut retry_connection,
                Request::Poll {
                    stream: "storage-pressure".to_owned(),
                    consumer: format!("retry-{attempt}"),
                },
            ),
            Response::Error { code, message }
                if code == "storage_error" && message.contains("storage stream queue is full")
        ));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "repeated pressure requests should be rejected promptly"
        );
    }

    let metrics = http_metrics(server.http_addr);
    assert!(
        labeled_metric_value(
            &metrics,
            "runnel_broker_request_failures_total",
            "operation=\"poll\"",
        ) >= poll_failures_before + 25,
        "metrics should count the initial and repeated storage-pressure poll failures"
    );

    drop(blocker_writer);
    release_fifo_write_stall(&fifo);
    let pressure_indices = clients.join();
    assert_eq!(pressure_indices.len(), WAITING_REQUESTS + 1);
    for result in pressure_indices {
        result.expect("pressure client thread should finish");
    }
    let pressure_responses = response_receiver.try_iter().collect::<Vec<_>>();
    assert_eq!(pressure_responses.len(), WAITING_REQUESTS);
    for (index, response) in pressure_responses {
        let response = response.unwrap_or_else(|error| {
            panic!("pressure request {index} should receive a protocol response: {error}")
        });
        if index == usize::MAX {
            assert!(
                matches!(&response, Response::Error { code, .. } if code == "storage_error"),
                "the FIFO-stalled storage operation should report its storage failure: {response:?}"
            );
            continue;
        }
        if index == rejected_index {
            continue;
        }
        assert!(
            matches!(&response, Response::Message { payload, .. } if payload == "storage pressure probe"),
            "queued poll {index} should resume after FIFO release: {response:?}"
        );
    }
    fifos.remove_all();
    wait_for_metric_at_most(server.http_addr, "runnel_active_requests", 0);

    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "storage-pressure-unrelated".to_owned(),
                consumer: "recovery-reader".to_owned(),
            },
        ),
        Response::Message { payload, .. } if payload == "unrelated-durable-traffic"
    ));
    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Health { .. }
    ));
    let recovered_metrics = http_metrics(server.http_addr);
    assert_eq!(
        metric_value(&recovered_metrics, "runnel_engine_health_available"),
        1
    );
    assert_eq!(
        metric_value(&recovered_metrics, "runnel_active_requests"),
        0
    );
}

#[cfg(unix)]
#[test]
fn global_storage_admission_is_bounded_observable_and_recovers() {
    const EXECUTION_CAPACITY: usize = 32;
    const QUEUE_CAPACITY: usize = 32;
    const CANDIDATE_REQUESTS: usize = QUEUE_CAPACITY * 2;
    const ADMISSION_CAPACITY: usize = EXECUTION_CAPACITY + QUEUE_CAPACITY;

    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--max-connections",
            "128",
            "--max-in-flight-requests",
            "128",
            "--request-timeout-ms",
            "30000",
        ],
    );
    let mut fifos = StoragePressureFifos::default();
    let mut clients = StoragePressureClients::default();

    let blocked_streams = (0..EXECUTION_CAPACITY)
        .map(|index| format!("global-blocked-{index}"))
        .collect::<Vec<_>>();
    let candidate_streams = (0..CANDIDATE_REQUESTS)
        .map(|index| format!("global-candidate-{index}"))
        .collect::<Vec<_>>();
    let overflow_stream = "global-overflow-probe".to_owned();
    let all_poll_streams = blocked_streams
        .iter()
        .chain(&candidate_streams)
        .cloned()
        .collect::<Vec<_>>();

    for stream in all_poll_streams
        .iter()
        .chain(std::iter::once(&overflow_stream))
    {
        assert!(matches!(
            request(
                server.broker_addr,
                Request::CreateStream {
                    stream: stream.clone(),
                },
            ),
            Response::StreamCreated { .. }
        ));
    }
    for stream in &blocked_streams {
        fifos.create(
            &directory
                .path()
                .join("consumers")
                .join(stream)
                .join("blocked.json.tmp"),
        );
    }

    let metrics_before = http_metrics(server.http_addr);
    let request_failures_before = labeled_metric_value(
        &metrics_before,
        "runnel_broker_request_failures_total",
        "operation=\"poll\"",
    );
    let request_timeouts_before =
        metric_value(&metrics_before, "runnel_broker_request_timeouts_total");
    let scrape_failures_before =
        metric_value(&metrics_before, "runnel_metrics_scrape_failures_total");
    let (blocked_response_sender, blocked_response_receiver) = mpsc::channel();

    for (index, stream) in blocked_streams.iter().cloned().enumerate() {
        let (start_sender, start_receiver) = mpsc::channel();
        let response_sender = blocked_response_sender.clone();
        let address = server.broker_addr;
        clients.push(
            start_sender,
            std::thread::spawn(move || {
                start_receiver
                    .recv()
                    .expect("blocked poll should be released together");
                let started = Instant::now();
                let response = try_request(
                    address,
                    Request::Poll {
                        stream,
                        consumer: "blocked".to_owned(),
                    },
                    Duration::from_secs(35),
                );
                let _ = response_sender.send((index, started.elapsed(), response));
                index
            }),
        );
    }
    clients.start();

    // Opening one writer for each FIFO confirms the matching server-side read
    // has started. Keeping these descriptors open holds all execution slots
    // without involving a shared stream lane.
    let mut blocked_writers = Vec::with_capacity(EXECUTION_CAPACITY);
    for fifo in &fifos.paths {
        blocked_writers.push(
            fs::OpenOptions::new()
                .write(true)
                .open(fifo)
                .expect("FIFO writer should pair with the server's blocked state read"),
        );
    }

    let (candidate_response_sender, candidate_response_receiver) = mpsc::channel();
    let (candidate_sent_sender, candidate_sent_receiver) = mpsc::channel();
    for (index, stream) in candidate_streams.iter().cloned().enumerate() {
        let (start_sender, start_receiver) = mpsc::channel();
        let response_sender = candidate_response_sender.clone();
        let sent_sender = candidate_sent_sender.clone();
        let address = server.broker_addr;
        let request_index = EXECUTION_CAPACITY + index;
        clients.push(
            start_sender,
            std::thread::spawn(move || {
                start_receiver
                    .recv()
                    .expect("queued poll should be released together");
                let response = try_request_after_send(
                    address,
                    Request::Poll {
                        stream,
                        consumer: "queued".to_owned(),
                    },
                    Duration::from_secs(35),
                    sent_sender,
                    request_index,
                );
                let _ = response_sender.send((request_index, response));
                request_index
            }),
        );
    }
    drop(blocked_response_sender);
    drop(candidate_response_sender);
    drop(candidate_sent_sender);
    clients.start();

    let mut sent_candidate_requests = vec![false; CANDIDATE_REQUESTS];
    for _ in 0..CANDIDATE_REQUESTS {
        let index = candidate_sent_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("every distinct-stream candidate request should be sent");
        assert!(index >= EXECUTION_CAPACITY);
        let candidate_index = index - EXECUTION_CAPACITY;
        assert!(candidate_index < CANDIDATE_REQUESTS);
        assert!(
            !sent_candidate_requests[candidate_index],
            "candidate poll {candidate_index} should be sent once"
        );
        sent_candidate_requests[candidate_index] = true;
    }
    assert!(sent_candidate_requests.into_iter().all(|sent| sent));

    let mut rejected_candidates = vec![false; CANDIDATE_REQUESTS];
    for _ in 0..QUEUE_CAPACITY {
        let (index, response) = candidate_response_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("the full global admission bound should reject excess candidate polls");
        assert!(index >= EXECUTION_CAPACITY);
        let candidate_index = index - EXECUTION_CAPACITY;
        assert!(candidate_index < CANDIDATE_REQUESTS);
        assert!(!rejected_candidates[candidate_index]);
        let (response, elapsed) = response.unwrap_or_else(|error| {
            panic!("candidate poll {candidate_index} should receive a protocol response: {error}")
        });
        assert!(
            elapsed < Duration::from_secs(1),
            "global admission rejection should be prompt: {elapsed:?}"
        );
        assert!(
            matches!(&response, Response::Error { code, message }
            if code.as_str() == "storage_error"
                && message.contains("storage execution queue is full")),
            "excess candidate poll should report global storage admission, got {response:?}"
        );
        rejected_candidates[candidate_index] = true;
    }

    let started = Instant::now();
    let overflow = try_request(
        server.broker_addr,
        Request::Poll {
            stream: overflow_stream,
            consumer: "overflow".to_owned(),
        },
        Duration::from_secs(2),
    )
    .expect("global storage saturation should return a protocol response");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "global storage admission should reject excess work promptly"
    );
    match overflow {
        Response::Error { code, message } => {
            assert_eq!(code, "storage_error");
            assert!(
                message.contains("storage execution queue is full"),
                "the rejection should identify global storage admission: {message}"
            );
        }
        response => panic!("expected global storage admission rejection, got {response:?}"),
    }

    let started = Instant::now();
    assert!(matches!(
        try_request(server.broker_addr, Request::Health, Duration::from_secs(2)),
        Ok(Response::Error { code, message })
            if code == "storage_error" && message.contains("storage execution queue is full")
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "protocol health should fail promptly while global storage admission is full"
    );

    let started = Instant::now();
    let readiness = http_ready(server.http_addr);
    assert!(readiness.starts_with("HTTP/1.1 503"));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "readiness should report unavailable engine health without waiting for a storage slot"
    );
    let started = Instant::now();
    assert!(http_liveness(server.http_addr).starts_with("HTTP/1.1 200"));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "process liveness should remain available under global storage saturation"
    );
    let started = Instant::now();
    let metrics_response = http_metrics_response(server.http_addr);
    assert!(metrics_response.starts_with("HTTP/1.1 200"));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the metrics fallback should remain scrapeable under global storage saturation"
    );
    let saturated_metrics = response_body(&metrics_response);
    assert_eq!(
        metric_value(saturated_metrics, "runnel_active_requests"),
        ADMISSION_CAPACITY as u64,
        "the 32 blocked and 32 queued operations should occupy global admission"
    );
    assert_eq!(
        metric_value(saturated_metrics, "runnel_engine_health_available"),
        0
    );
    assert!(has_metric(saturated_metrics, "runnel_active_connections"));
    assert!(has_metric(
        saturated_metrics,
        "runnel_process_uptime_seconds"
    ));
    assert!(!has_metric(saturated_metrics, "runnel_streams"));
    assert!(
        metric_value(saturated_metrics, "runnel_metrics_scrape_failures_total")
            > scrape_failures_before
    );
    assert!(
        labeled_metric_value(
            saturated_metrics,
            "runnel_broker_request_failures_total",
            "operation=\"poll\"",
        ) > request_failures_before
    );
    assert_eq!(
        metric_value(saturated_metrics, "runnel_broker_request_timeouts_total"),
        request_timeouts_before,
        "storage admission rejection should not be counted as a request timeout"
    );

    drop(blocked_writers);
    let completed = clients.join();
    assert_eq!(completed.len(), EXECUTION_CAPACITY + CANDIDATE_REQUESTS);
    for result in completed {
        result.expect("admitted global-pressure poll should finish after FIFO release");
    }
    let mut seen_blocked = vec![false; EXECUTION_CAPACITY];
    let mut seen_candidates = [false; CANDIDATE_REQUESTS];
    let blocked_responses = blocked_response_receiver.try_iter().collect::<Vec<_>>();
    let candidate_responses = candidate_response_receiver.try_iter().collect::<Vec<_>>();
    let responses = blocked_responses
        .into_iter()
        .chain(candidate_responses.into_iter().map(|(index, result)| {
            let result = result.map(|(response, _)| response);
            (index, Duration::ZERO, result)
        }))
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), ADMISSION_CAPACITY);
    for (index, _elapsed, response) in responses {
        let response = response.unwrap_or_else(|error| {
            panic!("admitted poll {index} should receive a protocol response: {error}")
        });
        if index < EXECUTION_CAPACITY {
            assert!(
                !seen_blocked[index],
                "blocked poll {index} should respond once"
            );
            seen_blocked[index] = true;
        } else {
            let candidate_index = index - EXECUTION_CAPACITY;
            assert!(
                !rejected_candidates[candidate_index],
                "rejected candidate poll {index} must not complete after release"
            );
            assert!(
                !seen_candidates[candidate_index],
                "candidate poll {index} should respond exactly once"
            );
            seen_candidates[candidate_index] = true;
        }
        let expected_consumer = if index < EXECUTION_CAPACITY {
            "blocked"
        } else {
            "queued"
        };
        assert!(
            matches!(&response, Response::Empty { stream, consumer }
                if stream.as_str() == all_poll_streams[index]
                    && consumer.as_str() == expected_consumer),
            "admitted poll {index} should complete after release, got {response:?}"
        );
    }
    assert!(seen_blocked.into_iter().all(|received| received));
    for (index, rejected) in rejected_candidates.into_iter().enumerate() {
        assert_eq!(seen_candidates[index], !rejected);
    }
    fifos.remove_all();
    wait_for_metric_at_most(server.http_addr, "runnel_active_requests", 0);

    let recovered_metrics = http_metrics(server.http_addr);
    assert_eq!(
        metric_value(&recovered_metrics, "runnel_engine_health_available"),
        1
    );
    assert!(http_ready(server.http_addr).starts_with("HTTP/1.1 200"));
    assert!(http_liveness(server.http_addr).starts_with("HTTP/1.1 200"));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: candidate_streams[0].clone(),
                key: None,
                payload: "global-admission-recovered".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: candidate_streams[0].clone(),
                consumer: "recovery-reader".to_owned(),
            },
        ),
        Response::Message { payload, .. } if payload == "global-admission-recovered"
    ));
    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Health { .. }
    ));
}

#[cfg(unix)]
#[test]
fn served_persistent_connection_drains_promptly_on_shutdown() {
    let directory = TempDir::new().unwrap();
    let mut server = RunningServer::start(
        directory.path(),
        &["--max-connections", "1", "--request-timeout-ms", "5000"],
    );
    let mut connection = TcpStream::connect(server.broker_addr).unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    assert!(matches!(
        send_on_connection(&mut connection, Request::Health),
        Response::Health { .. }
    ));
    wait_for_metric_at_least(server.http_addr, "runnel_active_connections", 1);

    // A served persistent connection can begin another frame before shutdown.
    // The partial frame must not keep its connection task alive until the
    // request timeout expires.
    connection
        .write_all(br#"{"op":"health"}"#)
        .expect("partial follow-up request should be writable");
    sleep(Duration::from_millis(100));

    let mut rejected = TcpStream::connect(server.broker_addr).unwrap();
    rejected
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut response = String::new();
    BufReader::new(&mut rejected)
        .read_line(&mut response)
        .expect("connection rejection response should be readable");
    assert!(
        matches!(decode_response(&response), Response::Error { code, .. } if code == "connection_limit")
    );
    let metrics = wait_for_metric_at_least(
        server.http_addr,
        "runnel_broker_connections_rejected_total",
        1,
    );
    assert_eq!(metric_value(&metrics, "runnel_active_connections"), 1);

    send_sigterm(&server.child);
    wait_for_server_exit(
        &mut server,
        "server did not drain a served persistent connection promptly",
    );
    let mut byte = [0; 1];
    assert_eq!(
        connection.read(&mut byte).unwrap(),
        0,
        "the client should observe the persistent connection closing during shutdown"
    );
}

#[cfg(unix)]
#[test]
fn storage_stall_is_bounded_and_durable_traffic_continues() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--request-timeout-ms",
            "500",
            "--max-in-flight-requests",
            "2",
        ],
    );

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "stalled".to_owned(),
            },
        ),
        Response::StreamCreated { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "stalled".to_owned(),
                key: None,
                payload: "blocked-state-write".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));

    let consumer_directory = directory.path().join("consumers/stalled");
    fs::create_dir_all(&consumer_directory).unwrap();
    let fifo = consumer_directory.join("blocked.json.tmp");
    create_fifo(&fifo);

    let started = Instant::now();
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "stalled".to_owned(),
                consumer: "blocked".to_owned(),
            },
        ),
        Response::Error { code, .. } if code == "request_timeout"
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "a storage stall must be bounded by the protocol request timeout"
    );
    let started = Instant::now();
    assert!(http_ready(server.http_addr).starts_with("HTTP/1.1 503"));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "readiness must report a stalled storage dependency within its health deadline"
    );

    let started = Instant::now();
    let stalled_metrics_response = http_metrics_response(server.http_addr);
    assert!(
        stalled_metrics_response.starts_with("HTTP/1.1 200"),
        "metrics should remain scrapeable while engine health is unavailable: {stalled_metrics_response}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "metrics should report a stalled storage dependency within its health deadline"
    );
    let stalled_metrics = response_body(&stalled_metrics_response);
    assert_eq!(
        metric_value(stalled_metrics, "runnel_engine_health_available"),
        0
    );
    assert!(has_metric(stalled_metrics, "runnel_active_connections"));
    assert!(has_metric(stalled_metrics, "runnel_process_uptime_seconds"));
    assert!(has_metric(
        stalled_metrics,
        "runnel_broker_max_in_flight_requests"
    ));
    assert!(has_metric(
        stalled_metrics,
        "runnel_metrics_scrape_failures_total"
    ));
    for name in [
        "runnel_streams",
        "runnel_storage_bytes",
        "runnel_in_flight_deliveries",
        "runnel_redeliveries_total",
        "runnel_dead_letters_total",
    ] {
        assert!(
            !has_metric(stalled_metrics, name),
            "unavailable engine metric {name} must not be presented as fresh"
        );
    }

    let started = Instant::now();
    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Error { code, .. } if code == "request_timeout"
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "protocol health must be bounded by the request timeout"
    );

    let started = Instant::now();
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "after-stall".to_owned(),
                key: None,
                payload: "durable-after-stall".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "after-stall".to_owned(),
                consumer: "reader".to_owned(),
            },
        ),
        Response::Message { payload, .. } if payload == "durable-after-stall"
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "durable traffic must not wait for one stalled storage operation"
    );

    release_fifo_stall(&fifo);

    let metrics = wait_for_metric_at_least(server.http_addr, "runnel_streams", 1);
    assert_eq!(metric_value(&metrics, "runnel_engine_health_available"), 1);
    assert!(has_metric(&metrics, "runnel_streams"));
    assert!(has_metric(&metrics, "runnel_storage_bytes"));
    assert!(has_metric(&metrics, "runnel_in_flight_deliveries"));
    assert!(has_metric(&metrics, "runnel_redeliveries_total"));
    assert!(has_metric(&metrics, "runnel_dead_letters_total"));
}

#[cfg(unix)]
#[test]
fn timed_out_same_stream_waiter_does_not_poison_following_request() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--request-timeout-ms",
            "1500",
            "--max-in-flight-requests",
            "2",
        ],
    );

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "stalled".to_owned(),
            },
        ),
        Response::StreamCreated { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "stalled".to_owned(),
                key: None,
                payload: "survives-queued-timeout".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));

    let consumer_directory = directory.path().join("consumers/stalled");
    fs::create_dir_all(&consumer_directory).unwrap();
    let fifo = consumer_directory.join("blocked.json.tmp");
    create_fifo(&fifo);

    let stalled_address = server.broker_addr;
    let stalled_request = std::thread::spawn(move || {
        request(
            stalled_address,
            Request::Poll {
                stream: "stalled".to_owned(),
                consumer: "blocked".to_owned(),
            },
        )
    });
    wait_for_metric_at_least(server.http_addr, "runnel_active_requests", 1);

    let started = Instant::now();
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "stalled".to_owned(),
                consumer: "queued".to_owned(),
            },
        ),
        Response::Error { code, .. } if code == "request_timeout"
    ));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a same-stream waiter must be canceled by the protocol request timeout"
    );
    assert!(matches!(
        stalled_request.join().unwrap(),
        Response::Error { code, .. } if code == "request_timeout"
    ));

    release_fifo_stall(&fifo);
    fs::remove_file(&fifo).unwrap();

    let started = Instant::now();
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "stalled".to_owned(),
                consumer: "queued".to_owned(),
            },
        ),
        Response::Message { payload, .. } if payload == "survives-queued-timeout"
    ));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a timed-out same-stream waiter must not block the next durable request"
    );
    assert!(matches!(
        request(server.broker_addr, Request::Health),
        Response::Health { streams: 1, .. }
    ));
    let metrics = wait_for_metric_at_most(server.http_addr, "runnel_active_requests", 0);
    assert_eq!(metric_value(&metrics, "runnel_engine_health_available"), 1);
}

#[cfg(unix)]
#[test]
fn storage_stall_shutdown_is_bounded_and_restart_recovers() {
    let directory = TempDir::new().unwrap();
    let mut server = RunningServer::start(
        directory.path(),
        &[
            "--request-timeout-ms",
            "500",
            "--max-in-flight-requests",
            "2",
        ],
    );

    assert!(matches!(
        request(
            server.broker_addr,
            Request::CreateStream {
                stream: "stalled".to_owned(),
            },
        ),
        Response::StreamCreated { .. }
    ));
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Publish {
                stream: "stalled".to_owned(),
                key: None,
                payload: "survives-stall".to_owned(),
                request_id: None,
            },
        ),
        Response::Published { offset: 0, .. }
    ));

    let consumer_directory = directory.path().join("consumers/stalled");
    fs::create_dir_all(&consumer_directory).unwrap();
    let fifo = consumer_directory.join("blocked.json.tmp");
    create_fifo(&fifo);

    let started = Instant::now();
    assert!(matches!(
        request(
            server.broker_addr,
            Request::Poll {
                stream: "stalled".to_owned(),
                consumer: "blocked".to_owned(),
            },
        ),
        Response::Error { code, .. } if code == "request_timeout"
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "storage stall should remain bounded before shutdown"
    );

    assert!(http_ready(server.http_addr).starts_with("HTTP/1.1 503"));
    let stalled_metrics = http_metrics_response(server.http_addr);
    assert!(stalled_metrics.starts_with("HTTP/1.1 200"));
    let stalled_metrics = response_body(&stalled_metrics);
    assert_eq!(
        metric_value(stalled_metrics, "runnel_engine_health_available"),
        0
    );
    assert!(has_metric(stalled_metrics, "runnel_active_connections"));
    assert!(has_metric(
        stalled_metrics,
        "runnel_metrics_scrape_failures_total"
    ));
    assert!(!has_metric(stalled_metrics, "runnel_streams"));

    let shutdown_started = Instant::now();
    send_sigterm(&server.child);
    release_fifo_stall(&fifo);
    fs::remove_file(&fifo).unwrap();
    let shutdown_elapsed = wait_for_server_exit(
        &mut server,
        "server did not exit successfully after releasing the stalled storage operation",
    );
    assert!(
        shutdown_started.elapsed() < Duration::from_secs(2),
        "graceful shutdown after a released storage stall took too long: {shutdown_elapsed:?}"
    );

    let mut recovered = RunningServer::start(
        directory.path(),
        &[
            "--request-timeout-ms",
            "500",
            "--max-in-flight-requests",
            "2",
        ],
    );
    let ready = http_ready(recovered.http_addr);
    assert!(ready.starts_with("HTTP/1.1 200"));
    assert!(ready.contains("\"status\":\"ready\""));
    let metrics = http_metrics_response(recovered.http_addr);
    assert!(metrics.starts_with("HTTP/1.1 200"));
    let metrics = response_body(&metrics);
    assert_eq!(metric_value(metrics, "runnel_engine_health_available"), 1);
    assert!(has_metric(metrics, "runnel_streams"));
    assert!(has_metric(metrics, "runnel_storage_bytes"));

    assert!(matches!(
        request(
            recovered.broker_addr,
            Request::Poll {
                stream: "stalled".to_owned(),
                consumer: "recovered".to_owned(),
            },
        ),
        Response::Message { payload, .. } if payload == "survives-stall"
    ));
    assert!(matches!(
        request(recovered.broker_addr, Request::Health),
        Response::Health { streams: 1, .. }
    ));

    send_sigterm(&recovered.child);
    wait_for_server_exit(&mut recovered, "recovered server did not shut down cleanly");
}

#[cfg(unix)]
#[derive(Default)]
struct StoragePressureFifos {
    paths: Vec<PathBuf>,
}

#[cfg(unix)]
impl StoragePressureFifos {
    fn create(&mut self, path: &Path) {
        fs::create_dir_all(path.parent().expect("FIFO path should have a parent"))
            .expect("consumer directory should be created");
        self.paths.push(path.to_owned());
        create_fifo(path);
    }

    fn remove_all(&mut self) {
        for path in self.paths.drain(..) {
            fs::remove_file(&path).unwrap_or_else(|error| {
                panic!("failed to remove FIFO {}: {error}", path.display())
            });
        }
    }
}

#[cfg(unix)]
impl Drop for StoragePressureFifos {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

#[cfg(unix)]
#[derive(Default)]
struct StoragePressureClients {
    start_senders: Vec<Sender<()>>,
    workers: Vec<JoinHandle<usize>>,
}

#[cfg(unix)]
impl StoragePressureClients {
    fn push(&mut self, start_sender: Sender<()>, worker: JoinHandle<usize>) {
        self.start_senders.push(start_sender);
        self.workers.push(worker);
    }

    fn push_started(&mut self, worker: JoinHandle<usize>) {
        self.workers.push(worker);
    }

    fn start(&mut self) {
        for sender in self.start_senders.drain(..) {
            let _ = sender.send(());
        }
    }

    fn join(&mut self) -> Vec<std::thread::Result<usize>> {
        self.start();
        self.workers.drain(..).map(JoinHandle::join).collect()
    }
}

#[cfg(unix)]
impl Drop for StoragePressureClients {
    fn drop(&mut self) {
        self.start();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
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

#[cfg(unix)]
fn create_fifo(path: &Path) {
    let status = Command::new("mkfifo")
        .arg(path)
        .status()
        .expect("mkfifo should be available on Unix");
    assert!(
        status.success(),
        "mkfifo failed for {}: {status}",
        path.display()
    );
}

#[cfg(unix)]
fn release_fifo_stall(path: &Path) {
    let fifo_writer = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("opening the FIFO writer should release the stalled storage reader");
    drop(fifo_writer);
    let mut fifo_reader = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .expect("opening the FIFO reader should release the stalled storage writer");
    let mut discarded = Vec::new();
    fifo_reader
        .read_to_end(&mut discarded)
        .expect("the stalled storage writer should close after recovery");
}

#[cfg(unix)]
fn release_fifo_write_stall(path: &Path) {
    let mut fifo_reader = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .expect("opening the FIFO reader should release the stalled storage writer");
    let mut discarded = Vec::new();
    fifo_reader
        .read_to_end(&mut discarded)
        .expect("the stalled storage writer should close after recovery");
}

#[cfg(unix)]
fn send_sigterm(child: &Child) {
    let pid = child.id().to_string();
    let status = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("kill should be available on Unix");
    assert!(status.success(), "SIGTERM should be delivered: {status}");
}

#[cfg(unix)]
fn wait_for_server_exit(server: &mut RunningServer, description: &str) -> Duration {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(2);
    loop {
        if let Some(status) = server.child.try_wait().unwrap() {
            assert!(status.success(), "server exited unsuccessfully: {status}");
            return started.elapsed();
        }
        assert!(Instant::now() < deadline, "{description}");
        sleep(Duration::from_millis(25));
    }
}

fn decode_response(encoded: &str) -> Response {
    serde_json::from_str(encoded).expect("response should be valid JSON")
}

#[cfg(unix)]
fn try_request(
    address: SocketAddr,
    request: Request,
    read_timeout: Duration,
) -> Result<Response, String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(read_timeout))
        .map_err(|error| error.to_string())?;
    let encoded = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
    stream
        .write_all(&encoded)
        .map_err(|error| error.to_string())?;
    stream.write_all(b"\n").map_err(|error| error.to_string())?;
    let mut response = String::new();
    BufReader::new(&mut stream)
        .read_line(&mut response)
        .map_err(|error| error.to_string())?;
    if response.is_empty() {
        return Err("server closed the connection without a response".to_owned());
    }
    serde_json::from_str(&response).map_err(|error| error.to_string())
}

#[cfg(unix)]
fn try_request_after_send(
    address: SocketAddr,
    request: Request,
    read_timeout: Duration,
    sent_sender: Sender<usize>,
    index: usize,
) -> Result<(Response, Duration), String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(read_timeout))
        .map_err(|error| error.to_string())?;
    let encoded = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
    stream
        .write_all(&encoded)
        .map_err(|error| error.to_string())?;
    stream.write_all(b"\n").map_err(|error| error.to_string())?;
    sent_sender.send(index).map_err(|error| error.to_string())?;
    let started = Instant::now();
    let mut response = String::new();
    BufReader::new(&mut stream)
        .read_line(&mut response)
        .map_err(|error| error.to_string())?;
    if response.is_empty() {
        return Err("server closed the connection without a response".to_owned());
    }
    let response = serde_json::from_str(&response).map_err(|error| error.to_string())?;
    Ok((response, started.elapsed()))
}

fn request(address: SocketAddr, request: Request) -> Response {
    let mut stream = TcpStream::connect(address).expect("broker should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout should be set");
    send_on_connection(&mut stream, request)
}

fn send_on_connection(stream: &mut TcpStream, request: Request) -> Response {
    let encoded = serde_json::to_vec(&request).unwrap();
    stream.write_all(&encoded).unwrap();
    stream.write_all(b"\n").unwrap();
    let mut response = String::new();
    BufReader::new(&mut *stream)
        .read_line(&mut response)
        .expect("broker response should be readable");
    decode_response(&response)
}

fn free_addr() -> SocketAddr {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
}

fn http_metrics(address: SocketAddr) -> String {
    let response = http_metrics_response(address);
    response_body(&response).to_owned()
}

fn http_metrics_response(address: SocketAddr) -> String {
    let mut stream =
        TcpStream::connect(address).expect("metrics endpoint should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("metrics read timeout should be set");
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("metrics request should be writable");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("metrics response should be readable");
    response
}

fn response_body(response: &str) -> &str {
    response
        .split_once("\r\n\r\n")
        .map_or(response, |(_, body)| body)
}

#[cfg(unix)]
fn http_ready(address: SocketAddr) -> String {
    let mut stream =
        TcpStream::connect(address).expect("health endpoint should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("health read timeout should be set");
    stream
        .write_all(b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("health request should be writable");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("health response should be readable");
    response
}

#[cfg(unix)]
fn http_liveness(address: SocketAddr) -> String {
    let mut stream =
        TcpStream::connect(address).expect("liveness endpoint should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("liveness read timeout should be set");
    stream
        .write_all(b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("liveness request should be writable");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("liveness response should be readable");
    response
}

fn metric_value(metrics: &str, name: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

#[cfg(unix)]
fn labeled_metric_value(metrics: &str, name: &str, labels: &str) -> u64 {
    let prefix = format!("{name}{{{labels}}} ");
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
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

fn has_metric(metrics: &str, name: &str) -> bool {
    metrics
        .lines()
        .any(|line| line.starts_with(&format!("{name} ")))
}

fn assert_metric_type(metrics: &str, name: &str, metric_type: &str) {
    let expected = format!("# TYPE {name} {metric_type}");
    assert!(
        metrics.lines().any(|line| line == expected),
        "metrics should expose {name} as a {metric_type}"
    );
}

fn wait_for_metric_at_least(address: SocketAddr, name: &str, expected: u64) -> String {
    let deadline = Instant::now() + Duration::from_secs(3);
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

fn wait_for_metric_at_most(address: SocketAddr, name: &str, expected: u64) -> String {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let metrics = http_metrics(address);
        if metric_value(&metrics, name) <= expected {
            return metrics;
        }
        if Instant::now() >= deadline {
            panic!("metric {name} did not fall to {expected}");
        }
        sleep(Duration::from_millis(25));
    }
}

fn wait_for_http(address: SocketAddr) -> Result<(), &'static str> {
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
                return Ok(());
            }
        }
        sleep(Duration::from_millis(25));
    }
    Err("runnel HTTP endpoint did not become ready")
}
