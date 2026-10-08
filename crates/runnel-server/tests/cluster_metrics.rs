use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use runnel_protocol::{Request, Response};
use tempfile::TempDir;

#[path = "support/peer_tls.rs"]
mod peer_tls;
use peer_tls::PeerCredentials;

const CLUSTER_WAIT_TIMEOUT: Duration = Duration::from_secs(90);

struct RunningNode {
    broker_addr: SocketAddr,
    http_addr: SocketAddr,
    child: Option<Child>,
}

impl RunningNode {
    fn start(
        node_id: u64,
        broker_addr: SocketAddr,
        http_addr: SocketAddr,
        peer_addr: SocketAddr,
        data_dir: std::path::PathBuf,
        cluster_nodes: &[(u64, SocketAddr)],
        bootstrap: bool,
    ) -> Self {
        let peer_map = cluster_nodes
            .iter()
            .map(|(id, address)| (*id, address.to_string()))
            .collect();
        let peer_credentials = PeerCredentials::for_cluster(
            data_dir
                .parent()
                .expect("cluster node data directory should have a common parent"),
            "runnel",
            &peer_map,
        );
        let trust_bundle = peer_credentials.trust_bundle();
        let cert_chain = peer_credentials.certificate_chain(node_id);
        let private_key = peer_credentials.private_key(node_id);
        let mut command = Command::new(env!("CARGO_BIN_EXE_runnel"));
        command
            .args([
                "--engine",
                "raft",
                "--node-id",
                &node_id.to_string(),
                "--listen",
                &broker_addr.to_string(),
                "--http-listen",
                &http_addr.to_string(),
                "--peer-listen",
                &peer_addr.to_string(),
                "--data-dir",
                data_dir.to_str().expect("temporary path should be UTF-8"),
                "--peer-trust-bundle",
                trust_bundle
                    .to_str()
                    .expect("temporary path should be UTF-8"),
                "--peer-cert-chain",
                cert_chain.to_str().expect("temporary path should be UTF-8"),
                "--peer-private-key",
                private_key
                    .to_str()
                    .expect("temporary path should be UTF-8"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for (peer_id, address) in cluster_nodes {
            command.args(["--cluster-node", &format!("{peer_id}={address}")]);
        }
        if bootstrap {
            command.arg("--bootstrap");
        }
        let child = command.spawn().expect("broker process should start");
        Self {
            broker_addr,
            http_addr,
            child: Some(child),
        }
    }

    fn stop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if child
            .try_wait()
            .expect("broker process status should be readable")
            .is_none()
        {
            child.kill().expect("broker process should stop");
        }
        child.wait().expect("broker process should be reaped");
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        self.stop();
    }
}

#[test]
fn metrics_report_bounded_replication_progress_without_stream_labels() {
    let directory = TempDir::new().unwrap();
    let nodes = start_three_node_cluster(&directory);

    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    create_stream(&nodes, "private-metric-stream");
    publish(&nodes, "private-metric-stream");

    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut latest_metrics = Vec::new();
    while Instant::now() < deadline {
        latest_metrics = nodes
            .iter()
            .map(|node| http_metrics(node.http_addr))
            .collect();
        let has_leader_progress = latest_metrics.iter().any(|metrics| {
            metric_value(metrics, "runnel_cluster_replication_progress_available") == Some(1)
                && metrics
                    .lines()
                    .any(|line| line.starts_with("runnel_cluster_replication_lag_entries{"))
        });
        let has_unavailable_follower = latest_metrics.iter().any(|metrics| {
            metric_value(metrics, "runnel_cluster_replication_progress_available") == Some(0)
        });
        if has_leader_progress && has_unavailable_follower {
            break;
        }
        sleep(Duration::from_millis(50));
    }

    assert!(
        latest_metrics.iter().any(|metrics| {
            metric_value(metrics, "runnel_cluster_replication_progress_available") == Some(1)
                && metrics
                    .lines()
                    .any(|line| line.starts_with("runnel_cluster_replication_lag_entries{"))
        }),
        "a locally led group should report peer lag samples; metrics: {latest_metrics:?}"
    );
    assert!(
        latest_metrics.iter().any(|metrics| {
            metric_value(metrics, "runnel_cluster_replication_progress_available") == Some(0)
                && metric_value(
                    metrics,
                    "runnel_cluster_replication_groups_with_local_leadership",
                ) == Some(0)
                && metric_value(metrics, "runnel_cluster_replication_peers_observed") == Some(0)
                && !metrics
                    .lines()
                    .any(|line| line.starts_with("runnel_cluster_replication_lag_entries{"))
        }),
        "a broker without local group leadership should mark progress unavailable and omit lag: {latest_metrics:?}"
    );

    for metrics in &latest_metrics {
        assert!(metrics.contains("runnel_cluster_replication_groups_total"));
        assert!(metrics.contains("runnel_cluster_replication_groups_observed"));
        assert!(!metrics.contains("private-metric-stream"));
        let observed = metric_value(metrics, "runnel_cluster_replication_groups_observed")
            .expect("observed group count should be present");
        assert!(observed <= 256, "group sample must remain bounded");
        for line in metrics
            .lines()
            .filter(|line| line.starts_with("runnel_cluster_replication_lag_entries{"))
        {
            let peer_id = line
                .split("peer_id=\"")
                .nth(1)
                .and_then(|tail| tail.split('\"').next())
                .expect("replication lag must use a peer_id label");
            assert!(peer_id.parse::<u64>().is_ok(), "peer label must be numeric");
        }
    }
}

#[cfg(feature = "persistence-write-counters")]
#[test]
fn persistence_write_metrics_require_the_opt_in_query() {
    let directory = TempDir::new().unwrap();
    let nodes = start_three_node_cluster(&directory);

    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    create_stream(&nodes, "persistence-metrics-probe");
    publish(&nodes, "persistence-metrics-probe");

    let regular_metrics = nodes
        .iter()
        .map(|node| http_metrics(node.http_addr))
        .collect::<Vec<_>>();
    assert!(regular_metrics.iter().all(|metrics| {
        !metrics.contains("runnel_persistence_write_counters_enabled")
            && !metrics.contains("runnel_persistence_operations_total")
    }));

    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut opted_metrics = Vec::new();
    while Instant::now() < deadline {
        opted_metrics = nodes
            .iter()
            .map(|node| {
                http_metrics_with_path(node.http_addr, "/metrics?persistence_write_counters=true")
            })
            .collect();
        if opted_metrics.iter().any(|metrics| {
            metrics.lines().any(|line| {
                line.starts_with(
                    "runnel_persistence_operations_total{role=\"raft_log_rewrite\",operation=\"write_all\"",
                )
            })
        }) {
            break;
        }
        sleep(Duration::from_millis(50));
    }

    assert!(
        opted_metrics
            .iter()
            .all(|metrics| { metrics.contains("runnel_persistence_write_counters_enabled 1") }),
        "each feature-enabled broker should expose counters on explicit request"
    );
    assert!(
        opted_metrics.iter().any(|metrics| {
            metrics.lines().any(|line| {
                line.starts_with(
                    "runnel_persistence_operations_total{role=\"raft_log_rewrite\",operation=\"write_all\"",
                )
            })
        }),
        "real Raft persistence activity should appear in the opt-in series"
    );
    for metrics in &opted_metrics {
        for line in metrics
            .lines()
            .filter(|line| line.starts_with("runnel_persistence_"))
        {
            assert!(!line.contains("stream="));
            assert!(!line.contains("group="));
        }
    }
}

fn start_three_node_cluster(directory: &TempDir) -> Vec<RunningNode> {
    let addresses = (0..9).map(|_| free_addr()).collect::<Vec<_>>();
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[7]), (3, addresses[8])];
    vec![
        RunningNode::start(
            1,
            addresses[0],
            addresses[3],
            addresses[6],
            directory.path().join("node-1"),
            &cluster_nodes,
            true,
        ),
        RunningNode::start(
            2,
            addresses[1],
            addresses[4],
            addresses[7],
            directory.path().join("node-2"),
            &cluster_nodes,
            false,
        ),
        RunningNode::start(
            3,
            addresses[2],
            addresses[5],
            addresses[8],
            directory.path().join("node-3"),
            &cluster_nodes,
            false,
        ),
    ]
}

fn create_stream(nodes: &[RunningNode], stream: &str) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        for node in nodes {
            if matches!(
                request(
                    node.broker_addr,
                    Request::CreateStream {
                        stream: stream.to_owned(),
                    },
                ),
                Ok(Response::StreamCreated { .. })
            ) {
                return;
            }
        }
        sleep(Duration::from_millis(50));
    }
    panic!("cluster did not create stream {stream}");
}

fn publish(nodes: &[RunningNode], stream: &str) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        for node in nodes {
            if matches!(
                request(
                    node.broker_addr,
                    Request::Publish {
                        stream: stream.to_owned(),
                        key: None,
                        payload: "replication-metric-probe".to_owned(),
                        request_id: Some("replication-metric-probe".to_owned()),
                    },
                ),
                Ok(Response::Published { .. })
            ) {
                return;
            }
        }
        sleep(Duration::from_millis(50));
    }
    panic!("cluster did not publish to stream {stream}");
}

fn request(address: SocketAddr, request: Request) -> Result<Response, String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| error.to_string())?;
    use runnel_protocol::v2::{self, ClientHello, ServerFrame, ServerHello};

    stream
        .write_all(&v2::PREFACE)
        .map_err(|error| format!("write v2 preface to {address}: {error}"))?;
    let hello = v2::encode_client_hello(&ClientHello::core_v2())
        .map_err(|error| format!("encode v2 Hello for {address}: {error}"))?;
    stream
        .write_all(hello.as_bytes())
        .map_err(|error| format!("write v2 Hello to {address}: {error}"))?;
    let server_hello = read_v2_frame(&mut stream, v2::HELLO_MAX_BODY_BYTES)
        .map_err(|error| format!("read v2 Hello from {address}: {error}"))?;
    match v2::decode_server_hello(&server_hello)
        .map_err(|error| format!("decode v2 Hello from {address}: {error}"))?
    {
        ServerHello::Accepted(accepted) if accepted.auth_required == Some(false) => {}
        ServerHello::Accepted(_) => {
            return Err(format!("server at {address} requires test credentials"));
        }
        ServerHello::Refused { code, diagnostic } => {
            return Err(format!(
                "server at {address} refused v2 ({code:?}): {diagnostic}"
            ));
        }
    }

    let encoded = v2::encode_application_request(&request)
        .map_err(|error| format!("encode application request for {address}: {error}"))?;
    stream
        .write_all(encoded.as_bytes())
        .map_err(|error| format!("write application request to {address}: {error}"))?;
    let response = read_v2_frame(&mut stream, v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES)
        .map_err(|error| format!("read application response from {address}: {error}"))?;
    match v2::decode_server_frame(&response, v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES)
        .map_err(|error| format!("decode application response from {address}: {error}"))?
    {
        ServerFrame::Application(reply) => Ok(reply.response),
        ServerFrame::Authenticated | ServerFrame::AuthenticationFailed => Err(format!(
            "server at {address} returned an authentication frame before the application response"
        )),
    }
}

fn read_v2_frame(stream: &mut impl Read, max_body_bytes: usize) -> std::io::Result<Vec<u8>> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let body_bytes = u32::from_be_bytes(length) as usize;
    if body_bytes == 0 || body_bytes > max_body_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "v2 frame length is outside the configured limit",
        ));
    }
    let mut body = vec![0; body_bytes];
    stream.read_exact(&mut body)?;
    Ok(body)
}

fn http_metrics(address: SocketAddr) -> String {
    http_metrics_with_path(address, "/metrics")
}

fn http_metrics_with_path(address: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(address).expect("metrics endpoint should accept scrapes");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("metrics read timeout should be set");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .expect("metrics request should be written");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("metrics response should be readable");
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "metrics returned: {response}"
    );
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or(response)
}

fn metric_value(metrics: &str, metric: &str) -> Option<u64> {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{metric} ")))
        .and_then(|value| value.parse().ok())
}

fn wait_for_http(address: SocketAddr) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        if let Ok(mut stream) = TcpStream::connect(address) {
            stream
                .set_read_timeout(Some(Duration::from_millis(200)))
                .expect("health read timeout should be set");
            if stream
                .write_all(
                    b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .is_ok()
            {
                let mut response = String::new();
                if BufReader::new(stream).read_line(&mut response).is_ok()
                    && response.starts_with("HTTP/1.1 200")
                {
                    return;
                }
            }
        }
        sleep(Duration::from_millis(50));
    }
    panic!("node {address} did not become ready");
}

fn free_addr() -> SocketAddr {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
}
