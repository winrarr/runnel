use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
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
            "100",
            "--max-in-flight-requests",
            "1",
        ],
    );

    let mut slow = TcpStream::connect(server.broker_addr).unwrap();
    slow.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
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

fn decode_response(encoded: &str) -> Response {
    serde_json::from_str(encoded).expect("response should be valid JSON")
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
