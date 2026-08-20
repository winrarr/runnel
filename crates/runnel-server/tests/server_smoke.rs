use std::io::{BufRead, BufReader, Write};
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
}

impl RunningServer {
    fn start(data_dir: &Path) -> Self {
        let broker_addr = free_addr();
        let http_addr = free_addr();
        let binary = server_binary();
        let child = Command::new(binary)
            .args([
                "--data-dir",
                data_dir.to_str().expect("temporary path should be UTF-8"),
                "--listen",
                &broker_addr.to_string(),
                "--http-listen",
                &http_addr.to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("runnel server should start");

        wait_for_http(http_addr);
        Self { child, broker_addr }
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
    server.stop();

    let server = RunningServer::start(directory.path());
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

fn request(address: SocketAddr, request: Request) -> Response {
    let mut stream = TcpStream::connect(address).expect("broker should accept connections");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout should be set");
    let encoded = serde_json::to_string(&request).unwrap();
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
