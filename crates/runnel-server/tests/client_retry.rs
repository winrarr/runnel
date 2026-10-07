use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use runnel_client::{AttemptFailure, AttemptOutcome, Client};
use runnel_protocol::{Request, Response};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener as AsyncTcpListener, TcpStream as AsyncTcpStream};

#[path = "support/v2_proxy.rs"]
mod v2_proxy;
use v2_proxy::{decode_application_response as decode_proxy_response, exchange_one_request};

struct RunningServer {
    child: Child,
    broker_addr: SocketAddr,
}

impl RunningServer {
    fn start(data_dir: &Path) -> Self {
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
        let child = command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("runnel server should start");
        wait_for_http(http_addr);
        Self { child, broker_addr }
    }

    #[cfg(unix)]
    fn wait_for_successful_exit(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .expect("server process status should be readable")
            {
                assert!(
                    status.success(),
                    "server should exit successfully: {status}"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server should finish graceful SIGTERM shutdown"
            );
            sleep(Duration::from_millis(25));
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
async fn request_id_replay_resolves_unknown_outcome_without_duplicate() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path());
    let mut setup = Client::connect(server.broker_addr).await.unwrap();
    assert!(matches!(
        setup
            .request(&Request::CreateStream {
                stream: "events".to_owned(),
            })
            .await
            .unwrap(),
        Response::StreamCreated { created: true, .. }
    ));
    drop(setup);

    let proxy_listener = AsyncTcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let drop_first_response = Arc::new(AtomicBool::new(true));
    let proxy = tokio::spawn(proxy_connections(
        proxy_listener,
        server.broker_addr,
        drop_first_response,
    ));

    let publish = Request::Publish {
        stream: "events".to_owned(),
        key: None,
        payload: "once".to_owned(),
        request_id: Some("publish-once".to_owned()),
    };
    let mut first = Client::connect(proxy_address).await.unwrap();
    assert!(matches!(
        first.request_with_outcome(&publish).await,
        AttemptOutcome::Unknown(AttemptFailure::Client(runnel_client::ClientError::Eof))
    ));
    drop(first);

    let mut second = Client::connect(proxy_address).await.unwrap();
    assert!(matches!(
        second.request_with_outcome(&publish).await,
        AttemptOutcome::Confirmed(Response::Published { offset: 0, .. })
    ));
    drop(second);

    let mut verify = Client::connect(server.broker_addr).await.unwrap();
    assert!(matches!(
        verify
            .request(&Request::Poll {
                stream: "events".to_owned(),
                consumer: "verifier".to_owned(),
            })
            .await
            .unwrap(),
        Response::Message {
            offset: 0,
            payload,
            ..
        } if payload == "once"
    ));
    assert!(matches!(
        verify
            .request(&Request::Ack {
                stream: "events".to_owned(),
                consumer: "verifier".to_owned(),
                offset: 0,
            })
            .await
            .unwrap(),
        Response::Acknowledged { .. }
    ));
    assert!(matches!(
        verify
            .request(&Request::Poll {
                stream: "events".to_owned(),
                consumer: "verifier".to_owned(),
            })
            .await
            .unwrap(),
        Response::Empty { .. }
    ));

    proxy.abort();
    let _ = proxy.await;
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_after_publish_response_reaches_proxy_recovers_unknown_retry_after_restart() {
    let directory = TempDir::new().unwrap();
    let mut server = RunningServer::start(directory.path());
    let mut setup = Client::connect(server.broker_addr).await.unwrap();
    assert!(matches!(
        setup
            .request(&Request::CreateStream {
                stream: "events".to_owned(),
            })
            .await
            .unwrap(),
        Response::StreamCreated { created: true, .. }
    ));
    drop(setup);

    let proxy_listener = AsyncTcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let proxy = tokio::spawn(proxy_publish_response_then_sigterm(
        proxy_listener,
        server.broker_addr,
        server.child.id(),
    ));

    let publish = Request::Publish {
        stream: "events".to_owned(),
        key: None,
        payload: "once".to_owned(),
        request_id: Some("publish-once".to_owned()),
    };
    let mut client = Client::connect(proxy_address).await.unwrap();
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            client.request_with_outcome(&publish)
        )
        .await
        .expect("proxy should close the response connection promptly"),
        AttemptOutcome::Unknown(AttemptFailure::Client(runnel_client::ClientError::Eof))
    ));
    drop(client);
    proxy
        .await
        .expect("response proxy should signal shutdown after observing publish success");
    server.wait_for_successful_exit();

    server = RunningServer::start(directory.path());
    let mut retry = Client::connect(server.broker_addr).await.unwrap();
    assert!(matches!(
        retry.request_with_outcome(&publish).await,
        AttemptOutcome::Confirmed(Response::Published { offset: 0, .. })
    ));

    let mut verifier = Client::connect(server.broker_addr).await.unwrap();
    assert!(matches!(
        verifier
            .request(&Request::Poll {
                stream: "events".to_owned(),
                consumer: "shutdown-replay-verifier".to_owned(),
            })
            .await
            .unwrap(),
        Response::Message {
            offset: 0,
            payload,
            ..
        } if payload == "once"
    ));
    assert!(matches!(
        verifier
            .request(&Request::Ack {
                stream: "events".to_owned(),
                consumer: "shutdown-replay-verifier".to_owned(),
                offset: 0,
            })
            .await
            .unwrap(),
        Response::Acknowledged { .. }
    ));
    assert!(matches!(
        verifier
            .request(&Request::Poll {
                stream: "events".to_owned(),
                consumer: "shutdown-replay-verifier".to_owned(),
            })
            .await
            .unwrap(),
        Response::Empty { .. }
    ));
}

#[cfg(unix)]
async fn proxy_publish_response_then_sigterm(
    listener: AsyncTcpListener,
    broker_addr: SocketAddr,
    server_pid: u32,
) {
    let (client, _) = listener
        .accept()
        .await
        .expect("proxy should accept the publish caller");
    let (client_writer, response) = exchange_one_request(client, broker_addr).await;
    assert!(matches!(
        decode_proxy_response(&response),
        Response::Published { offset: 0, .. }
    ));

    let pid = server_pid.to_string();
    let status = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("kill should be available on Unix");
    assert!(status.success(), "SIGTERM should be delivered: {status}");

    drop(client_writer);
}

async fn proxy_connections(
    listener: AsyncTcpListener,
    broker_addr: SocketAddr,
    drop_first_response: Arc<AtomicBool>,
) {
    for _ in 0..2 {
        let (client, _) = listener.accept().await.unwrap();
        let (mut client_writer, response) = exchange_one_request(client, broker_addr).await;

        if drop_first_response.swap(false, Ordering::AcqRel) {
            continue;
        }
        client_writer.write_all(&response).await.unwrap();
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
