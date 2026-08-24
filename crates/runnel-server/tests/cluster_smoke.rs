use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use runnel_protocol::{Request, Response};
use tempfile::TempDir;

// Restart and log replay can be substantially slower on contended CI disks;
// keep the assertion bounded without treating an intermediate empty poll as
// successful recovery.
const CLUSTER_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
// Durable publishes and snapshot recovery can overlap while a node rejoins;
// keep ordinary request helpers tolerant of the bounded cluster recovery
// window. Retrying helpers use a shorter per-attempt timeout below.
const REQUEST_READ_TIMEOUT: Duration = CLUSTER_WAIT_TIMEOUT;
const REQUEST_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const RECOVERY_REQUEST_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

struct RunningNode {
    node_id: u64,
    broker_addr: SocketAddr,
    http_addr: SocketAddr,
    peer_addr: SocketAddr,
    data_dir: PathBuf,
    cluster_nodes: Vec<(u64, SocketAddr)>,
    child: Option<Child>,
}

impl RunningNode {
    fn start(
        node_id: u64,
        broker_addr: SocketAddr,
        http_addr: SocketAddr,
        peer_addr: SocketAddr,
        data_dir: PathBuf,
        cluster_nodes: Vec<(u64, SocketAddr)>,
        bootstrap: bool,
    ) -> Self {
        let child = Some(spawn_node(
            node_id,
            broker_addr,
            http_addr,
            peer_addr,
            &data_dir,
            &cluster_nodes,
            bootstrap,
        ));
        Self {
            node_id,
            broker_addr,
            http_addr,
            peer_addr,
            data_dir,
            cluster_nodes,
            child,
        }
    }

    fn restart(&mut self) {
        self.stop();
        self.child = Some(spawn_node(
            self.node_id,
            self.broker_addr,
            self.http_addr,
            self.peer_addr,
            &self.data_dir,
            &self.cluster_nodes,
            false,
        ));
        wait_for_http(self.http_addr);
    }

    fn replace_storage(&mut self, data_dir: PathBuf) {
        self.stop();
        self.data_dir = data_dir;
        self.child = Some(spawn_node(
            self.node_id,
            self.broker_addr,
            self.http_addr,
            self.peer_addr,
            &self.data_dir,
            &self.cluster_nodes,
            false,
        ));
        wait_for_http(self.http_addr);
    }

    fn stop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if child
            .try_wait()
            .expect("node status should be readable")
            .is_none()
        {
            child.kill().expect("node should stop");
        }
        child.wait().expect("node should be reaped");
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        self.stop();
    }
}

#[test]
fn three_process_cluster_replicates_and_recovers_after_failures() {
    let directory = TempDir::new().unwrap();
    let addresses = (0..9).map(|_| free_addr()).collect::<Vec<_>>();
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[7]), (3, addresses[8])];
    let mut nodes = vec![
        RunningNode::start(
            1,
            addresses[0],
            addresses[3],
            addresses[6],
            directory.path().join("node-1"),
            cluster_nodes.clone(),
            true,
        ),
        RunningNode::start(
            2,
            addresses[1],
            addresses[4],
            addresses[7],
            directory.path().join("node-2"),
            cluster_nodes.clone(),
            false,
        ),
        RunningNode::start(
            3,
            addresses[2],
            addresses[5],
            addresses[8],
            directory.path().join("node-3"),
            cluster_nodes,
            false,
        ),
    ];
    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    let leader = create_stream_on_any(&nodes, "events");
    let jobs_node = create_stream_on_any(&nodes, "jobs");
    assert!(matches!(
        wait_for_response_at(
            nodes[jobs_node].broker_addr,
            || Request::Publish {
                stream: "jobs".to_owned(),
                key: None,
                payload: "job".to_owned(),
                request_id: Some("first-job".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[(jobs_node + 1) % nodes.len()].broker_addr,
            || Request::Poll {
                stream: "jobs".to_owned(),
                consumer: "worker".to_owned(),
            },
            |response| matches!(response, Response::Message { offset: 0, .. }),
        ),
        Response::Message { offset: 0, .. }
    ));

    let grouped_node = create_stream_on_any(&nodes, "grouped-jobs");
    for (index, payload) in ["first-grouped-job", "second-grouped-job"]
        .into_iter()
        .enumerate()
    {
        assert!(matches!(
            wait_for_response_at(
                nodes[(grouped_node + index) % nodes.len()].broker_addr,
                || Request::Publish {
                    stream: "grouped-jobs".to_owned(),
                    key: None,
                    payload: payload.to_owned(),
                    request_id: Some(format!("grouped-job-{index}")),
                },
                |response| matches!(response, Response::Published { offset, .. } if *offset == index as u64),
            ),
            Response::Published { offset, .. } if offset == index as u64
        ));
    }
    let first_grouped = wait_for_response_at(
        nodes[(grouped_node + 1) % nodes.len()].broker_addr,
        || Request::PollGroup {
            stream: "grouped-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 0,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                }
            )
        },
    );
    let first_grouped_token = match first_grouped {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected first grouped message, got {response:?}"),
    };
    let second_grouped = wait_for_response_at(
        nodes[(grouped_node + 2) % nodes.len()].broker_addr,
        || Request::PollGroup {
            stream: "grouped-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-b".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 1,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                }
            )
        },
    );
    let second_grouped_token = match second_grouped {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected second grouped message, got {response:?}"),
    };
    assert!(matches!(
        wait_for_response_at(
            nodes[grouped_node].broker_addr,
            || Request::AckGroup {
                stream: "grouped-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
                offset: 1,
                delivery_token: second_grouped_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[(grouped_node + 1) % nodes.len()].broker_addr,
            || Request::AckGroup {
                stream: "grouped-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first_grouped_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));

    let legacy_retry_node = create_stream_on_any(&nodes, "legacy-retry");
    assert!(matches!(
        wait_for_response_at(
            nodes[legacy_retry_node].broker_addr,
            || Request::Publish {
                stream: "legacy-retry".to_owned(),
                key: Some("poison".to_owned()),
                payload: "dead-letter-me".to_owned(),
                request_id: Some("legacy-retry-message".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[(legacy_retry_node + 1) % nodes.len()].broker_addr,
            || Request::Poll {
                stream: "legacy-retry".to_owned(),
                consumer: "worker".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 0,
                    delivery_attempt: Some(1),
                    ..
                }
            ),
        ),
        Response::Message {
            offset: 0,
            delivery_attempt: Some(1),
            ..
        }
    ));
    sleep(Duration::from_millis(100));
    assert!(matches!(
        wait_for_response_at(
            nodes[(legacy_retry_node + 2) % nodes.len()].broker_addr,
            || Request::Poll {
                stream: "legacy-retry".to_owned(),
                consumer: "worker".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 0,
                    delivery_attempt: Some(2),
                    ..
                }
            ),
        ),
        Response::Message {
            offset: 0,
            delivery_attempt: Some(2),
            ..
        }
    ));
    sleep(Duration::from_millis(100));
    assert!(matches!(
        wait_for_response_at(
            nodes[legacy_retry_node].broker_addr,
            || Request::Poll {
                stream: "legacy-retry".to_owned(),
                consumer: "worker".to_owned(),
            },
            |response| matches!(response, Response::Empty { .. }),
        ),
        Response::Empty { .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[legacy_retry_node].broker_addr,
            || Request::Poll {
                stream: "legacy-retry.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 0,
                    payload,
                    delivery_attempt: Some(1),
                    ..
                } if payload == "dead-letter-me"
            ),
        ),
        Response::Message {
            offset: 0,
            payload,
            delivery_attempt: Some(1),
            ..
        } if payload == "dead-letter-me"
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[legacy_retry_node].broker_addr,
            || Request::Ack {
                stream: "legacy-retry.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 0,
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));

    let follower = (leader + 1) % nodes.len();
    let create_response = wait_for_response_at(
        nodes[follower].broker_addr,
        || Request::CreateStream {
            stream: "events".to_owned(),
        },
        |response| matches!(response, Response::StreamCreated { .. }),
    );
    assert!(matches!(
        create_response,
        Response::StreamCreated { created: false, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[follower].broker_addr,
            || Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "first".to_owned(),
                request_id: Some("first-message".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[(follower + 1) % nodes.len()].broker_addr,
            || Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "first".to_owned(),
                request_id: Some("first-message".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[(follower + 1) % nodes.len()].broker_addr,
            || Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
            |response| matches!(response, Response::Message { offset: 0, .. }),
        ),
        Response::Message { offset: 0, .. }
    ));
    let ack_response = wait_for_response_at(
        nodes[follower].broker_addr,
        || Request::Ack {
            stream: "events".to_owned(),
            consumer: "worker".to_owned(),
            offset: 0,
        },
        |response| matches!(response, Response::Acknowledged { .. }),
    );
    assert!(
        matches!(&ack_response, Response::Acknowledged { .. }),
        "{ack_response:?}"
    );

    nodes[1].stop();
    let publish_node = nodes
        .iter()
        .enumerate()
        .find(|(_, node)| node.child.is_some())
        .expect("a live node should remain after stopping a follower")
        .0;
    assert!(matches!(
        wait_for_response_at(
            nodes[publish_node].broker_addr,
            || Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "during-follower-restart".to_owned(),
                request_id: Some("follower-restart-message".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 1, .. }),
        ),
        Response::Published { offset: 1, .. }
    ));
    nodes[1].restart();
    wait_for_message(nodes[1].broker_addr, 1, "during-follower-restart");
    for node in &nodes {
        if node.child.is_some() {
            wait_for_message(node.broker_addr, 1, "during-follower-restart");
        }
    }

    nodes[leader].stop();
    let new_leader = wait_for_stream_on_any(&nodes, "events");
    assert_ne!(new_leader, leader);
    wait_for_message(nodes[new_leader].broker_addr, 1, "during-follower-restart");
    let post_failure_node = nodes
        .iter()
        .enumerate()
        .find(|(_, node)| node.child.is_some())
        .expect("a follower should remain available")
        .0;
    let publish_response = wait_for_response_at(
        nodes[post_failure_node].broker_addr,
        || Request::Publish {
            stream: "events".to_owned(),
            key: None,
            payload: "after-leader-failure".to_owned(),
            request_id: Some("after-leader-failure-message".to_owned()),
        },
        |response| matches!(response, Response::Published { offset: 2, .. }),
    );
    assert!(matches!(
        publish_response,
        Response::Published { offset: 2, .. }
    ));
    let replicated_node = nodes
        .iter()
        .enumerate()
        .find(|(index, node)| *index != new_leader && node.child.is_some())
        .expect("a follower should remain available")
        .1;
    assert!(matches!(
        wait_for_response_at(
            nodes[post_failure_node].broker_addr,
            || Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 1,
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));
    wait_for_message(replicated_node.broker_addr, 2, "after-leader-failure");
}

#[test]
fn three_process_cluster_reassigns_group_delivery_after_node_failure() {
    let directory = TempDir::new().unwrap();
    let addresses = (0..9).map(|_| free_addr()).collect::<Vec<_>>();
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[7]), (3, addresses[8])];
    let mut nodes = vec![
        RunningNode::start(
            1,
            addresses[0],
            addresses[3],
            addresses[6],
            directory.path().join("node-1"),
            cluster_nodes.clone(),
            true,
        ),
        RunningNode::start(
            2,
            addresses[1],
            addresses[4],
            addresses[7],
            directory.path().join("node-2"),
            cluster_nodes.clone(),
            false,
        ),
        RunningNode::start(
            3,
            addresses[2],
            addresses[5],
            addresses[8],
            directory.path().join("node-3"),
            cluster_nodes,
            false,
        ),
    ];
    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    create_stream_on_any(&nodes, "failover-jobs");
    assert!(matches!(
        wait_for_response_at(
            nodes[0].broker_addr,
            || Request::Publish {
                stream: "failover-jobs".to_owned(),
                key: None,
                payload: "reassign-me".to_owned(),
                request_id: Some("failover-job".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));

    let first = wait_for_response_at(
        nodes[0].broker_addr,
        || Request::PollGroup {
            stream: "failover-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 0,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                }
            )
        },
    );
    let first_token = match first {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected initial grouped delivery, got {response:?}"),
    };

    nodes[0].stop();
    sleep(Duration::from_millis(100));
    let survivor = nodes
        .iter()
        .enumerate()
        .find(|(_, node)| node.child.is_some())
        .map(|(index, _)| index)
        .expect("a quorum node should remain after the failure");
    let second = wait_for_response_at(
        nodes[survivor].broker_addr,
        || Request::PollGroup {
            stream: "failover-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-b".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 0,
                    delivery_attempt: Some(2),
                    delivery_token: Some(_),
                    ..
                }
            )
        },
    );
    let second_token = match second {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected reassigned grouped delivery, got {response:?}"),
    };
    assert_ne!(first_token, second_token);
    assert!(matches!(
        wait_for_response_at(
            nodes[survivor].broker_addr,
            || Request::AckGroup {
                stream: "failover-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first_token.clone(),
            },
            |response| matches!(response, Response::Error { code, .. } if code == "stale_delivery"),
        ),
        Response::Error { .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[survivor].broker_addr,
            || Request::AckGroup {
                stream: "failover-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
                offset: 0,
                delivery_token: second_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));

    assert!(matches!(
        wait_for_response_at(
            nodes[survivor].broker_addr,
            || Request::Publish {
                stream: "failover-jobs".to_owned(),
                key: Some("poison".to_owned()),
                payload: "dead-letter-me".to_owned(),
                request_id: Some("dead-letter-job".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 1, .. }),
        ),
        Response::Published { offset: 1, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[survivor].broker_addr,
            || Request::PollGroup {
                stream: "failover-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-c".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 1,
                    delivery_attempt: Some(1),
                    ..
                }
            ),
        ),
        Response::Message {
            offset: 1,
            delivery_attempt: Some(1),
            ..
        }
    ));
    sleep(Duration::from_millis(100));
    assert!(matches!(
        wait_for_response_at(
            nodes[survivor].broker_addr,
            || Request::PollGroup {
                stream: "failover-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-d".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 1,
                    delivery_attempt: Some(2),
                    ..
                }
            ),
        ),
        Response::Message {
            offset: 1,
            delivery_attempt: Some(2),
            ..
        }
    ));
    sleep(Duration::from_millis(100));
    assert!(matches!(
        wait_for_response_at(
            nodes[survivor].broker_addr,
            || Request::PollGroup {
                stream: "failover-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-e".to_owned(),
            },
            |response| matches!(response, Response::Empty { .. }),
        ),
        Response::Empty { .. }
    ));
    let dead_letter = wait_for_response_at(
        nodes[survivor].broker_addr,
        || Request::PollGroup {
            stream: "failover-jobs.dead-letter".to_owned(),
            consumer: "inspector".to_owned(),
            member: "inspector-1".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 0,
                    payload,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                } if payload == "dead-letter-me"
            )
        },
    );
    let dead_letter_token = match dead_letter {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected dead-letter message, got {response:?}"),
    };
    assert!(matches!(
        wait_for_response_at(
            nodes[survivor].broker_addr,
            || Request::AckGroup {
                stream: "failover-jobs.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                member: "inspector-1".to_owned(),
                offset: 0,
                delivery_token: dead_letter_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged {
            already_acknowledged: false,
            ..
        }
    ));
    assert!(matches!(
        request(
            nodes[survivor].broker_addr,
            Request::PollGroup {
                stream: "failover-jobs.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                member: "inspector-2".to_owned(),
            },
        ),
        Ok(Response::Empty { .. })
    ));
}

#[test]
fn replacement_node_recovers_from_compacted_snapshot() {
    let directory = TempDir::new().unwrap();
    let addresses = (0..9).map(|_| free_addr()).collect::<Vec<_>>();
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[7]), (3, addresses[8])];
    let mut nodes = vec![
        RunningNode::start(
            1,
            addresses[0],
            addresses[3],
            addresses[6],
            directory.path().join("node-1"),
            cluster_nodes.clone(),
            true,
        ),
        RunningNode::start(
            2,
            addresses[1],
            addresses[4],
            addresses[7],
            directory.path().join("node-2"),
            cluster_nodes.clone(),
            false,
        ),
        RunningNode::start(
            3,
            addresses[2],
            addresses[5],
            addresses[8],
            directory.path().join("node-3"),
            cluster_nodes,
            false,
        ),
    ];
    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    let leader = create_stream_on_any(&nodes, "events");
    assert!(matches!(
        wait_for_response_at(
            nodes[leader].broker_addr,
            || Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "seed".to_owned(),
                request_id: Some("snapshot-seed".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[leader].broker_addr,
            || Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));

    let replacement = (leader + 1) % nodes.len();
    nodes[replacement].stop();
    for index in 1..=48 {
        let payload = if index == 16 {
            "x".repeat(256 * 1024)
        } else {
            format!("snapshot-{index}")
        };
        let request_id = format!("snapshot-message-{index}");
        assert!(matches!(
            wait_for_response_on_any_with_timeout(
                &nodes,
                replacement,
                RECOVERY_REQUEST_ATTEMPT_TIMEOUT,
                || Request::Publish {
                    stream: "events".to_owned(),
                    key: None,
                    payload: payload.clone(),
                    request_id: Some(request_id.clone()),
                },
                |response| matches!(response, Response::Published { offset, .. } if *offset == index),
            ),
            Response::Published { offset, .. } if offset == index
        ));
    }

    let snapshot_node = wait_for_snapshot(&nodes, replacement, "events");
    wait_for_purged_log(&nodes[snapshot_node], "events");
    let replacement_dir = directory.path().join("empty-replacement");
    nodes[replacement].replace_storage(replacement_dir);
    wait_for_active_snapshot_transfer(nodes[replacement].http_addr);
    nodes[replacement].stop();
    nodes[replacement].restart();
    wait_for_metric_at_least(
        nodes[replacement].http_addr,
        "runnel_snapshot_installs_completed_total",
        1,
    );
    wait_for_message_at(nodes[replacement].broker_addr, 1, "snapshot-1");
    let metrics = http_metrics(nodes[replacement].http_addr);
    assert!(
        metric_value(&metrics, "runnel_snapshot_transfer_chunks_received_total") >= 1,
        "replacement metrics did not report snapshot chunks after retry:\n{metrics}"
    );
    assert!(
        metric_value(
            &metrics,
            "runnel_snapshot_transfer_final_chunks_received_total"
        ) >= 1,
        "replacement metrics did not report a completed snapshot transfer:\n{metrics}"
    );
    assert!(
        metric_value(&metrics, "runnel_snapshot_installs_completed_total") >= 1,
        "replacement metrics did not report a completed snapshot install:\n{metrics}"
    );
}

fn spawn_node(
    node_id: u64,
    broker_addr: SocketAddr,
    http_addr: SocketAddr,
    peer_addr: SocketAddr,
    data_dir: &Path,
    cluster_nodes: &[(u64, SocketAddr)],
    bootstrap: bool,
) -> Child {
    let mut command = Command::new(server_binary());
    command
        .args([
            "--engine",
            "raft",
            "--ack-timeout-ms",
            "50",
            "--max-delivery-attempts",
            "2",
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
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (id, address) in cluster_nodes {
        command.args(["--cluster-node", &format!("{id}={address}")]);
    }
    if bootstrap {
        command.arg("--bootstrap");
    }
    command.spawn().expect("runnel node should start")
}

fn create_stream_on_any(nodes: &[RunningNode], stream: &str) -> usize {
    wait_for_response(nodes, |node| {
        matches!(
            request(
                node.broker_addr,
                Request::CreateStream {
                    stream: stream.to_owned(),
                },
            ),
            Ok(Response::StreamCreated { .. })
        )
    })
}

fn wait_for_stream_on_any(nodes: &[RunningNode], stream: &str) -> usize {
    wait_for_response(nodes, |node| {
        matches!(
            request(
                node.broker_addr,
                Request::CreateStream {
                    stream: stream.to_owned(),
                },
            ),
            Ok(Response::StreamCreated { .. })
        )
    })
}

fn wait_for_response(
    nodes: &[RunningNode],
    mut predicate: impl FnMut(&RunningNode) -> bool,
) -> usize {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        for (index, node) in nodes.iter().enumerate() {
            if node.child.is_some() && predicate(node) {
                return index;
            }
        }
        sleep(Duration::from_millis(50));
    }
    panic!("no live node accepted the request before the deadline");
}

fn wait_for_response_at(
    address: SocketAddr,
    request_builder: impl FnMut() -> Request,
    predicate: impl FnMut(&Response) -> bool,
) -> Response {
    wait_for_response_at_with_timeout(address, REQUEST_ATTEMPT_TIMEOUT, request_builder, predicate)
}

fn wait_for_response_at_with_timeout(
    address: SocketAddr,
    attempt_timeout: Duration,
    mut request_builder: impl FnMut() -> Request,
    mut predicate: impl FnMut(&Response) -> bool,
) -> Response {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut last_response = None;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let request_timeout = remaining.min(attempt_timeout);
        match request_with_timeout(address, request_builder(), request_timeout) {
            Ok(response) => {
                if predicate(&response) {
                    return response;
                }
                last_response = Some(Ok(response));
            }
            Err(error) => last_response = Some(Err(error)),
        }
        sleep(Duration::from_millis(50));
    }
    panic!(
        "node {address} did not accept the request before the deadline; last response: {last_response:?}"
    );
}

fn wait_for_response_on_any_with_timeout(
    nodes: &[RunningNode],
    excluded: usize,
    attempt_timeout: Duration,
    mut request_builder: impl FnMut() -> Request,
    mut predicate: impl FnMut(&Response) -> bool,
) -> Response {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut last_response = None;
    while Instant::now() < deadline {
        for (index, node) in nodes.iter().enumerate() {
            if index == excluded || node.child.is_none() {
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let request_timeout = remaining.min(attempt_timeout);
            match request_with_timeout(node.broker_addr, request_builder(), request_timeout) {
                Ok(response) => {
                    if predicate(&response) {
                        return response;
                    }
                    last_response = Some(Ok(response));
                }
                Err(error) => last_response = Some(Err(error)),
            }
        }
        sleep(Duration::from_millis(50));
    }
    panic!(
        "no surviving node accepted the request before the deadline; last response: {last_response:?}"
    );
}

fn wait_for_message(address: SocketAddr, offset: u64, payload: &str) {
    wait_for_message_at(address, offset, payload);
}

fn wait_for_message_at(address: SocketAddr, offset: u64, payload: &str) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut last_response = None;
    while Instant::now() < deadline {
        let response = request_with_timeout(
            address,
            Request::Poll {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
            },
            CLUSTER_WAIT_TIMEOUT,
        );
        if let Ok(Response::Message {
            offset: received,
            payload: received_payload,
            ..
        }) = &response
            && *received == offset
            && received_payload == payload
        {
            return;
        }
        last_response = Some(response);
        sleep(Duration::from_millis(50));
    }
    let metrics = http_metrics(address);
    panic!(
        "node {address} did not recover message at offset {offset}; last response: {last_response:?}; metrics:\n{metrics}"
    );
}

fn wait_for_snapshot(nodes: &[RunningNode], excluded: usize, stream: &str) -> usize {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        for (index, node) in nodes.iter().enumerate() {
            if index == excluded || node.child.is_none() {
                continue;
            }
            let path = node
                .data_dir
                .join("groups/data")
                .join(path_component(stream))
                .join("state-machine/snapshot.json");
            if path.exists() {
                return index;
            }
        }
        sleep(Duration::from_millis(50));
    }
    panic!("no live node produced a snapshot for stream '{stream}'");
}

fn wait_for_purged_log(node: &RunningNode, stream: &str) {
    let path = node
        .data_dir
        .join("groups/data")
        .join(path_component(stream))
        .join("raft-log.json");
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        if let Ok(bytes) = std::fs::read(&path)
            && let Ok(log) = serde_json::from_slice::<serde_json::Value>(&bytes)
            && !log["last_purged_log_id"].is_null()
        {
            return;
        }
        sleep(Duration::from_millis(50));
    }
    panic!("consensus log '{}' was not compacted", path.display());
}

fn wait_for_active_snapshot_transfer(address: SocketAddr) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        let metrics = http_metrics(address);
        let chunks = metric_value(&metrics, "runnel_snapshot_transfer_chunks_received_total");
        let final_chunks = metric_value(
            &metrics,
            "runnel_snapshot_transfer_final_chunks_received_total",
        );
        if chunks > final_chunks {
            return;
        }
        sleep(Duration::from_millis(10));
    }
    panic!("replacement node did not receive a non-final snapshot chunk");
}

fn wait_for_metric_at_least(address: SocketAddr, name: &str, expected: u64) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        if metric_value(&http_metrics(address), name) >= expected {
            return;
        }
        sleep(Duration::from_millis(25));
    }
    panic!("metric '{name}' did not reach {expected}");
}

fn path_component(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn http_metrics(address: SocketAddr) -> String {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        match try_http_metrics(address) {
            Ok(metrics) => return metrics,
            Err(error) => last_error = Some(error),
        }
        sleep(Duration::from_millis(50));
    }
    panic!(
        "metrics endpoint {address} did not respond before the deadline; last error: {last_error:?}"
    );
}

fn try_http_metrics(address: SocketAddr) -> Result<String, String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| error.to_string())?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    Ok(match response.find("\r\n\r\n") {
        Some(index) => response[index + 4..].to_owned(),
        None => response,
    })
}

fn metric_value(metrics: &str, name: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn request(address: SocketAddr, request: Request) -> Result<Response, String> {
    request_with_timeout(address, request, REQUEST_READ_TIMEOUT)
}

fn request_with_timeout(
    address: SocketAddr,
    request: Request,
    read_timeout: Duration,
) -> Result<Response, String> {
    let mut stream =
        TcpStream::connect(address).map_err(|error| format!("connect to {address}: {error}"))?;
    stream
        .set_read_timeout(Some(read_timeout))
        .map_err(|error| format!("set read timeout for {address}: {error}"))?;
    let encoded = serde_json::to_string(&request)
        .map_err(|error| format!("encode request for {address}: {error}"))?;
    writeln!(stream, "{encoded}")
        .map_err(|error| format!("write request to {address}: {error}"))?;
    let mut response = String::new();
    BufReader::new(stream)
        .read_line(&mut response)
        .map_err(|error| format!("read response from {address}: {error}"))?;
    serde_json::from_str(&response)
        .map_err(|error| format!("decode response from {address}: {error}"))
}

fn server_binary() -> PathBuf {
    if let Some(binary) = std::env::var_os("CARGO_BIN_EXE_runnel") {
        return PathBuf::from(binary);
    }
    let test_binary = std::env::current_exe().expect("Cargo should expose the test executable");
    test_binary
        .parent()
        .and_then(Path::parent)
        .expect("test executable should be inside target/debug/deps")
        .join("runnel")
}

fn free_addr() -> SocketAddr {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
}

fn wait_for_http(address: SocketAddr) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
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
        sleep(Duration::from_millis(50));
    }
    panic!("runnel HTTP endpoint did not become ready");
}
