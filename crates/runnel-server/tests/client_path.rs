use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use runnel_client::{
    AttemptFailure, AttemptOutcome, Client, ClientConfig, ClientError, PublishOptions,
};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{TcpListener as AsyncTcpListener, TcpStream as AsyncTcpStream};

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

#[tokio::test]
async fn typed_client_keeps_a_connection_and_preserves_binary_payloads() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &[]);
    let mut client = Client::connect(server.broker_addr).await.unwrap();

    assert!(client.create_stream("events").await.unwrap().created);

    let text_receipt = client
        .publish_with_options(
            "events",
            "hello",
            PublishOptions::default()
                .with_key("text-key")
                .with_request_id("text-message"),
        )
        .await
        .unwrap();
    let binary_payload = vec![0, 1, 255, b'\n', b'_', 0];
    let binary_receipt = client
        .publish_bytes_with_options(
            "events",
            binary_payload.clone(),
            PublishOptions::default()
                .with_key("binary-key")
                .with_request_id("binary-message"),
        )
        .await
        .unwrap();

    let text_message = client
        .poll("events", "worker")
        .await
        .unwrap()
        .expect("the text message should be available");
    assert_eq!(text_message.offset, text_receipt.offset);
    assert_eq!(text_message.key.as_deref(), Some("text-key"));
    assert_eq!(text_message.payload, "hello");
    assert_eq!(
        client
            .ack("events", "worker", text_message.offset)
            .await
            .unwrap()
            .offset,
        text_message.offset
    );

    let binary_message = client
        .poll_bytes("events", "worker")
        .await
        .unwrap()
        .expect("the binary message should be available");
    assert_eq!(binary_message.offset, binary_receipt.offset);
    assert_eq!(binary_message.key.as_deref(), Some("binary-key"));
    assert_eq!(binary_message.payload, binary_payload);
    assert_eq!(
        client
            .ack("events", "worker", binary_message.offset)
            .await
            .unwrap()
            .offset,
        binary_message.offset
    );

    assert!(
        client
            .poll_bytes("events", "worker")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(client.health().await.unwrap().streams, 1);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_client_bounds_timeout_and_cancellation_before_reconnect() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(
        directory.path(),
        &[
            "--request-timeout-ms",
            "5000",
            "--max-in-flight-requests",
            "2",
        ],
    );

    let mut setup = Client::connect(server.broker_addr).await.unwrap();
    setup.create_stream("timeout").await.unwrap();
    setup.publish("timeout", "stalled").await.unwrap();
    setup.create_stream("cancel").await.unwrap();
    setup.publish("cancel", "stalled").await.unwrap();
    drop(setup);

    let timeout_fifo = stalled_consumer_fifo(directory.path(), "timeout", "worker");
    let mut blocker = Client::connect(server.broker_addr).await.unwrap();
    let mut blocked_poll = Box::pin(blocker.poll("timeout", "worker"));
    tokio::select! {
        result = &mut blocked_poll => panic!("the storage-stalled poll unexpectedly completed: {result:?}"),
        _ = wait_for_metric_at_least_async(server.http_addr, "runnel_active_requests", 1) => {}
    }

    let mut timeout_client = Client::connect_with_config(
        server.broker_addr,
        ClientConfig {
            response_timeout: Duration::from_millis(100),
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let started = Instant::now();
    assert!(matches!(
        timeout_client.poll("timeout", "worker").await,
        Err(AttemptOutcome::Unknown(AttemptFailure::Client(
            ClientError::ResponseTimeout { timeout }
        ))) if timeout == Duration::from_millis(100)
    ));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the client response timeout should stay bounded"
    );
    assert!(matches!(
        timeout_client.health().await,
        Err(AttemptOutcome::Retryable(AttemptFailure::Client(
            ClientError::ConnectionUnavailable
        )))
    ));

    drop(timeout_client);
    drop(blocked_poll);
    release_fifo_stall(&timeout_fifo);
    std::fs::remove_file(timeout_fifo).unwrap();
    drop(blocker);
    // The timed-out second request may still be queued behind the first storage
    // operation. Restarting the real broker gives the next application request
    // a clean process boundary without relying on an arbitrary drain delay.
    drop(server);
    let server = RunningServer::start(
        directory.path(),
        &[
            "--request-timeout-ms",
            "5000",
            "--max-in-flight-requests",
            "2",
        ],
    );

    let cancel_fifo = stalled_consumer_fifo(directory.path(), "cancel", "worker");
    let mut cancel_client = Client::connect(server.broker_addr).await.unwrap();
    let mut cancelled_poll = Box::pin(cancel_client.poll("cancel", "worker"));
    tokio::select! {
        result = &mut cancelled_poll => panic!("the storage-stalled poll unexpectedly completed: {result:?}"),
        _ = wait_for_metric_at_least_async(server.http_addr, "runnel_active_requests", 1) => {}
    }
    let started = Instant::now();
    drop(cancelled_poll);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "cancelling a started request should complete promptly"
    );
    assert!(matches!(
        cancel_client.health().await,
        Err(AttemptOutcome::Retryable(AttemptFailure::Client(
            ClientError::ConnectionUnavailable
        )))
    ));

    release_fifo_stall(&cancel_fifo);
    wait_for_metric_at_most_async(server.http_addr, "runnel_active_requests", 0).await;
    std::fs::remove_file(cancel_fifo).unwrap();
    cancel_client.reconnect(server.broker_addr).await.unwrap();
    assert_eq!(cancel_client.health().await.unwrap().status, "ok");
}

#[tokio::test]
async fn typed_publish_retry_with_stable_identity_does_not_duplicate() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &[]);
    let mut setup = Client::connect(server.broker_addr).await.unwrap();
    setup.create_stream("events").await.unwrap();
    drop(setup);

    let proxy_listener = AsyncTcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let proxy = ProxyGuard {
        handle: Some(tokio::spawn(drop_first_response_proxy(
            proxy_listener,
            server.broker_addr,
        ))),
    };

    let options = PublishOptions::default().with_request_id("publish-once");
    let mut client = Client::connect(proxy_address).await.unwrap();
    assert!(matches!(
        client
            .publish_with_options("events", "once", options.clone())
            .await,
        Err(AttemptOutcome::Unknown(AttemptFailure::Client(
            ClientError::Eof
        )))
    ));
    assert!(matches!(
        client.publish("events", "must-not-send").await,
        Err(AttemptOutcome::Retryable(AttemptFailure::Client(
            ClientError::ConnectionUnavailable
        )))
    ));

    client.reconnect(proxy_address).await.unwrap();
    let receipt = client
        .publish_with_options("events", "once", options)
        .await
        .unwrap();
    assert_eq!(receipt.offset, 0);
    drop(client);
    proxy.finish().await.unwrap();

    let mut verifier = Client::connect(server.broker_addr).await.unwrap();
    let message = verifier
        .poll_bytes("events", "verifier")
        .await
        .unwrap()
        .expect("the retried publish should be available");
    assert_eq!(message.offset, 0);
    assert_eq!(message.payload, b"once");
    verifier
        .ack("events", "verifier", message.offset)
        .await
        .unwrap();
    assert!(
        verifier
            .poll_bytes("events", "verifier")
            .await
            .unwrap()
            .is_none()
    );
}

struct ProxyGuard {
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl ProxyGuard {
    async fn finish(mut self) -> Result<(), tokio::task::JoinError> {
        self.handle
            .take()
            .expect("proxy handle should be present")
            .await
    }
}

impl Drop for ProxyGuard {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

async fn drop_first_response_proxy(listener: AsyncTcpListener, broker_addr: SocketAddr) {
    for drop_response in [true, false] {
        let (client, _) = listener.accept().await.unwrap();
        let (client_reader, mut client_writer) = client.into_split();
        let mut client_reader = AsyncBufReader::new(client_reader);
        let mut request = Vec::new();
        client_reader.read_until(b'\n', &mut request).await.unwrap();

        let broker = AsyncTcpStream::connect(broker_addr).await.unwrap();
        let (broker_reader, mut broker_writer) = broker.into_split();
        broker_writer.write_all(&request).await.unwrap();
        let mut broker_reader = AsyncBufReader::new(broker_reader);
        let mut response = Vec::new();
        broker_reader
            .read_until(b'\n', &mut response)
            .await
            .unwrap();

        if drop_response {
            continue;
        }
        client_writer.write_all(&response).await.unwrap();
    }
}

#[cfg(unix)]
fn stalled_consumer_fifo(data_dir: &Path, stream: &str, consumer: &str) -> PathBuf {
    let consumer_directory = data_dir.join("consumers").join(stream);
    std::fs::create_dir_all(&consumer_directory).unwrap();
    let fifo = consumer_directory.join(format!("{consumer}.json.tmp"));
    let status = Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo should be available on Unix");
    assert!(
        status.success(),
        "mkfifo failed for {}: {status}",
        fifo.display()
    );
    fifo
}

#[cfg(unix)]
fn release_fifo_stall(path: &Path) {
    let fifo_writer = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("opening the FIFO writer should release the stalled storage reader");
    drop(fifo_writer);
    let mut fifo_reader = std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .expect("opening the FIFO reader should release the stalled storage writer");
    let mut discarded = Vec::new();
    fifo_reader
        .read_to_end(&mut discarded)
        .expect("the stalled storage writer should close after recovery");
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

fn free_addr() -> SocketAddr {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
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

fn http_metrics(address: SocketAddr) -> String {
    let mut stream =
        TcpStream::connect(address).expect("metrics endpoint should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
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

async fn wait_for_metric_at_least_async(address: SocketAddr, name: &str, expected: u64) {
    let name = name.to_owned();
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if metric_value(&http_metrics(address), &name) >= expected {
                return;
            }
            sleep(Duration::from_millis(25));
        }
        panic!("metric {name} did not reach {expected}");
    })
    .await
    .expect("metric wait should complete");
}

async fn wait_for_metric_at_most_async(address: SocketAddr, name: &str, expected: u64) {
    let name = name.to_owned();
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if metric_value(&http_metrics(address), &name) <= expected {
                return;
            }
            sleep(Duration::from_millis(25));
        }
        panic!("metric {name} did not fall to {expected}");
    })
    .await
    .expect("metric wait should complete");
}
