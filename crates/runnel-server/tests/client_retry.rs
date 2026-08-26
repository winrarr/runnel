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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{TcpListener as AsyncTcpListener, TcpStream as AsyncTcpStream};

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

async fn proxy_connections(
    listener: AsyncTcpListener,
    broker_addr: SocketAddr,
    drop_first_response: Arc<AtomicBool>,
) {
    for _ in 0..2 {
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
