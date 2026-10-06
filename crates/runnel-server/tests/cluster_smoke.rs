use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
#[cfg(feature = "test-replacement-recovery")]
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::sleep;
use std::time::{Duration, Instant};

use runnel_client::{
    AttemptFailure, Client, ClientConfig, ClientError, PublishBatchOutcome, PublishBatchRecord,
    PublishOptions, PublishReceipt,
};
use runnel_protocol::{
    AckBatchItemOutcome, BatchDeliveryReceipt, BatchMessageResponse, BinaryPayload,
    PublishBatchRecordResponse, Request, Response,
};
use tempfile::TempDir;
#[cfg(feature = "test-replacement-recovery")]
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{TcpListener as AsyncTcpListener, TcpStream as AsyncTcpStream};
use tokio::sync::oneshot;
#[cfg(feature = "test-replacement-recovery")]
use tokio::sync::{mpsc, watch};

// Restart and log replay can be substantially slower on contended CI disks;
// keep the assertion bounded without treating an intermediate empty poll as
// successful recovery.
const CLUSTER_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
// The response proxy remains closed until the bounded leader probe proves a
// survivor, so allow that recovery window before classifying a client timeout.
const WITHHELD_BATCH_RESPONSE_TIMEOUT: Duration = Duration::from_secs(180);
// Durable publishes and snapshot recovery can overlap while a node rejoins;
// keep ordinary request helpers tolerant of the bounded cluster recovery
// window. Retrying helpers use a shorter per-attempt timeout below.
const REQUEST_READ_TIMEOUT: Duration = CLUSTER_WAIT_TIMEOUT;
const REQUEST_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(feature = "test-replacement-recovery")]
const RECOVERY_REQUEST_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
// Keep replication probes from holding the worker's delivery lease across failover.
const REPLICATION_OBSERVER: &str = "replication-observer";
#[cfg(feature = "test-replacement-recovery")]
const SNAPSHOT_INTERRUPTION_ATTEMPTS: usize = 3;
// This scenario checks expiry and stale-token fencing before acknowledging the
// current delivery. A longer lease keeps a loaded CI runner from expiring the
// current token while the intentionally stale acknowledgements are committed.
const REASSIGN_ACK_TIMEOUT_MS: u64 = 5_000;
const KEY_ORDERING_ACK_TIMEOUT_MS: u64 = 60_000;
const FULL_CLUSTER_RESTART_ACK_TIMEOUT_MS: u64 = 300_000;

struct RunningNode {
    node_id: u64,
    broker_addr: SocketAddr,
    http_addr: SocketAddr,
    peer_addr: SocketAddr,
    data_dir: PathBuf,
    cluster_nodes: Vec<(u64, SocketAddr)>,
    ack_timeout_ms: u64,
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
        Self::start_with_ack_timeout(
            node_id,
            broker_addr,
            http_addr,
            peer_addr,
            data_dir,
            cluster_nodes,
            bootstrap,
            50,
        )
    }

    // Keep process-launch parameters explicit so each test documents its node
    // topology and lease configuration at the call site.
    #[allow(clippy::too_many_arguments)]
    fn start_with_ack_timeout(
        node_id: u64,
        broker_addr: SocketAddr,
        http_addr: SocketAddr,
        peer_addr: SocketAddr,
        data_dir: PathBuf,
        cluster_nodes: Vec<(u64, SocketAddr)>,
        bootstrap: bool,
        ack_timeout_ms: u64,
    ) -> Self {
        let child = Some(spawn_node(
            node_id,
            broker_addr,
            http_addr,
            peer_addr,
            &data_dir,
            &cluster_nodes,
            bootstrap,
            ack_timeout_ms,
        ));
        Self {
            node_id,
            broker_addr,
            http_addr,
            peer_addr,
            data_dir,
            cluster_nodes,
            ack_timeout_ms,
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
            self.ack_timeout_ms,
        ));
        wait_for_http(self.http_addr);
    }

    #[cfg(feature = "test-replacement-recovery")]
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
            self.ack_timeout_ms,
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

    let leader = create_stream_on_any(&mut nodes, "events");
    let jobs_node = create_stream_on_any(&mut nodes, "jobs");
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
    assert!(matches!(
        wait_for_response_at(
            nodes[(jobs_node + 2) % nodes.len()].broker_addr,
            || Request::Replay {
                stream: "jobs".to_owned(),
                consumer: "worker".to_owned(),
                offset: 0,
            },
            |response| matches!(response, Response::ReplayMessage { offset: 0, .. }),
        ),
        Response::ReplayMessage { offset: 0, .. }
    ));

    let grouped_node = create_stream_on_any(&mut nodes, "grouped-jobs");
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
        wait_for_response_on_any(
            &mut nodes,
            || Request::AckGroup {
                stream: "grouped-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
                offset: 1,
                delivery_token: second_grouped_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));
    assert!(matches!(
        wait_for_response_on_any(
            &mut nodes,
            || Request::AckGroup {
                stream: "grouped-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first_grouped_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));

    let batch_node = create_stream_on_any(&mut nodes, "batch-jobs");
    assert!(matches!(
        wait_for_response_at(
            nodes[batch_node].broker_addr,
            || Request::ConfigureConsumer {
                stream: "batch-jobs".to_owned(),
                consumer: "workers".to_owned(),
                ack_timeout_ms: 60_000,
                max_delivery_attempts: None,
            },
            |response| matches!(response, Response::ConsumerPolicy { .. }),
        ),
        Response::ConsumerPolicy { .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[batch_node].broker_addr,
            || Request::Publish {
                stream: "batch-jobs".to_owned(),
                key: None,
                payload: "first-batch-job".to_owned(),
                request_id: Some("batch-job-0".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));
    let binary_batch_payload = vec![0, 0xff, b'\n', 0x80];
    assert!(matches!(
        wait_for_response_at(
            nodes[(batch_node + 1) % nodes.len()].broker_addr,
            || Request::PublishBytes {
                stream: "batch-jobs".to_owned(),
                key: None,
                payload_base64: BinaryPayload::new(binary_batch_payload.clone()),
                request_id: Some("batch-job-1".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 1, .. }),
        ),
        Response::Published { offset: 1, .. }
    ));
    let offset_only_batch = wait_for_response_at(
        nodes[batch_node].broker_addr,
        || Request::PollBatch {
            stream: "batch-jobs".to_owned(),
            consumer: "offset-only".to_owned(),
            max_records: 2,
            max_bytes: 64 * 1024,
            max_wait_ms: 0,
        },
        |response| matches!(response, Response::PollBatch { messages, .. } if messages.len() == 2),
    );
    let offset_only_receipts = batch_response_receipts(offset_only_batch);
    assert!(matches!(
        request(
            nodes[batch_node].broker_addr,
            Request::Ack {
                stream: "batch-jobs".to_owned(),
                consumer: "offset-only".to_owned(),
                offset: offset_only_receipts[0].0,
            },
        ),
        Ok(Response::Error { ref code, .. }) if code == "stale_delivery"
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[batch_node].broker_addr,
            || Request::AckBatch {
                stream: "batch-jobs".to_owned(),
                consumer: "offset-only".to_owned(),
                receipts: vec![BatchDeliveryReceipt {
                    offset: offset_only_receipts[0].0,
                    delivery_token: offset_only_receipts[0].1.clone(),
                }],
            },
            |response| matches!(response, Response::AckBatch { outcomes, .. } if outcomes.len() == 1),
        ),
        Response::AckBatch { outcomes, .. }
            if matches!(outcomes.first().map(|item| &item.outcome), Some(&AckBatchItemOutcome::Confirmed))
    ));

    let batch_request = || Request::PollGroupBatch {
        stream: "batch-jobs".to_owned(),
        consumer: "workers".to_owned(),
        member: "member-a".to_owned(),
        max_records: 2,
        max_bytes: 64 * 1024,
        max_wait_ms: 0,
    };
    let initial_batch_leader = data_group_leader(&nodes, "batch-jobs");
    let initial_batch_follower = (0..nodes.len())
        .map(|index| (initial_batch_leader + index + 1) % nodes.len())
        .find(|index| *index != initial_batch_leader)
        .expect("one follower should be available for forwarded batch responses");
    let batch_response = wait_for_response_at(
        nodes[initial_batch_follower].broker_addr,
        batch_request,
        |response| matches!(response, Response::PollBatch { messages, .. } if messages.len() == 2),
    );
    let Response::PollBatch { messages, .. } = &batch_response else {
        panic!("expected a consume-batch response, got {batch_response:?}");
    };
    assert!(matches!(
        &messages[0],
        BatchMessageResponse::Text { payload, .. } if payload == "first-batch-job"
    ));
    assert!(matches!(
        &messages[1],
        BatchMessageResponse::Bytes { payload_base64, .. }
            if payload_base64.as_bytes() == binary_batch_payload
    ));
    let batch_receipts = batch_response_receipts(batch_response);
    assert_eq!(
        batch_receipts
            .iter()
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        batch_receipts,
        batch_response_receipts(wait_for_response_at(
            nodes[(batch_node + 1) % nodes.len()].broker_addr,
            batch_request,
            |response| matches!(response, Response::PollBatch { messages, .. } if messages.len() == 2),
        )),
        "a repeated cluster poll must return its complete original active set"
    );
    assert!(matches!(
        request(
            nodes[batch_node].broker_addr,
            Request::AckGroup {
                stream: "batch-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: batch_receipts[0].0,
                delivery_token: String::new(),
            },
        ),
        Ok(Response::Error { ref code, .. }) if code == "stale_delivery"
    ));
    let ack_first = wait_for_response_at(
        nodes[batch_node].broker_addr,
        || Request::AckGroupBatch {
            stream: "batch-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
            receipts: vec![BatchDeliveryReceipt {
                offset: batch_receipts[0].0,
                delivery_token: batch_receipts[0].1.clone(),
            }],
        },
        |response| matches!(response, Response::AckBatch { outcomes, .. } if outcomes.len() == 1),
    );
    assert!(matches!(
        ack_first,
        Response::AckBatch { outcomes, .. }
            if matches!(outcomes.first().map(|item| &item.outcome), Some(&AckBatchItemOutcome::Confirmed))
    ));

    let batch_leader = data_group_leader(&nodes, "batch-jobs");
    let restarted_follower = (0..nodes.len())
        .map(|index| (batch_leader + index + 1) % nodes.len())
        .find(|index| *index != batch_leader)
        .expect("one follower should be available to restart");
    nodes[restarted_follower].restart();
    let remaining_batch = wait_for_response_at(
        nodes[restarted_follower].broker_addr,
        batch_request,
        |response| matches!(response, Response::PollBatch { messages, .. } if messages.len() == 1),
    );
    let remaining_receipts = batch_response_receipts(remaining_batch);
    assert_eq!(remaining_receipts, [batch_receipts[1].clone()]);
    let previous_batch_leader = data_group_leader(&nodes, "batch-jobs");
    nodes[previous_batch_leader].stop();
    let successor_batch_leader = data_group_leader(&nodes, "batch-jobs");
    assert_ne!(successor_batch_leader, previous_batch_leader);
    let after_leader_change = wait_for_response_at(
        nodes[successor_batch_leader].broker_addr,
        batch_request,
        |response| matches!(response, Response::PollBatch { messages, .. } if messages.len() == 1),
    );
    assert_eq!(
        batch_response_receipts(after_leader_change),
        remaining_receipts,
        "a committed active set and its receipt must survive leader change"
    );
    let ack_second = wait_for_response_at(
        nodes[successor_batch_leader].broker_addr,
        || Request::AckGroupBatch {
            stream: "batch-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
            receipts: vec![BatchDeliveryReceipt {
                offset: remaining_receipts[0].0,
                delivery_token: remaining_receipts[0].1.clone(),
            }],
        },
        |response| matches!(response, Response::AckBatch { outcomes, .. } if outcomes.len() == 1),
    );
    assert!(matches!(
        ack_second,
        Response::AckBatch { outcomes, .. }
            if matches!(outcomes.first().map(|item| &item.outcome), Some(&AckBatchItemOutcome::Confirmed))
    ));
    nodes[previous_batch_leader].restart();
    assert!(matches!(
        wait_for_response_at(
            nodes[previous_batch_leader].broker_addr,
            || Request::CreateStream {
                stream: "batch-jobs".to_owned(),
            },
            |response| matches!(response, Response::StreamCreated { created: false, .. }),
        ),
        Response::StreamCreated { created: false, .. }
    ));

    let legacy_retry_node = create_stream_on_any(&mut nodes, "legacy-retry");
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
        Response::Acknowledged { .. }
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
    let mismatch_through_follower = wait_for_response_at(
        nodes[follower].broker_addr,
        || Request::Publish {
            stream: "events".to_owned(),
            key: None,
            payload: "changed-content".to_owned(),
            request_id: Some("first-message".to_owned()),
        },
        |response| {
            matches!(
                response,
                Response::Error { code, .. } if code == "request_id_content_conflict"
            )
        },
    );
    assert!(matches!(
        mismatch_through_follower,
        Response::Error { code, .. } if code == "request_id_content_conflict"
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
    wait_for_message_for_consumer_at(
        nodes[1].broker_addr,
        "events",
        REPLICATION_OBSERVER,
        1,
        "during-follower-restart",
    );
    for node in &nodes {
        if node.child.is_some() {
            wait_for_message_for_consumer_at(
                node.broker_addr,
                "events",
                REPLICATION_OBSERVER,
                1,
                "during-follower-restart",
            );
        }
    }
    assert!(matches!(
        wait_for_response_at(
            nodes[1].broker_addr,
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
    let mismatch_after_follower_restart = wait_for_response_at(
        nodes[1].broker_addr,
        || Request::Publish {
            stream: "events".to_owned(),
            key: None,
            payload: "changed-after-restart".to_owned(),
            request_id: Some("first-message".to_owned()),
        },
        |response| {
            matches!(
                response,
                Response::Error { code, .. } if code == "request_id_content_conflict"
            )
        },
    );
    assert!(matches!(
        mismatch_after_follower_restart,
        Response::Error { code, .. } if code == "request_id_content_conflict"
    ));

    nodes[leader].stop();
    let new_leader = wait_for_stream_on_any(&mut nodes, "events");
    assert_ne!(new_leader, leader);
    wait_for_message_for_consumer_at(
        nodes[new_leader].broker_addr,
        "events",
        "worker",
        1,
        "during-follower-restart",
    );
    assert!(matches!(
        wait_for_response_at(
            nodes[new_leader].broker_addr,
            || Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 1,
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));
    let post_failure_node = nodes
        .iter()
        .enumerate()
        .find(|(index, node)| *index != new_leader && node.child.is_some())
        .expect("a follower should remain available")
        .0;
    let mismatch_after_leader_change = wait_for_response_at(
        nodes[post_failure_node].broker_addr,
        || Request::Publish {
            stream: "events".to_owned(),
            key: None,
            payload: "changed-after-leader-change".to_owned(),
            request_id: Some("first-message".to_owned()),
        },
        |response| {
            matches!(
                response,
                Response::Error { code, .. } if code == "request_id_content_conflict"
            )
        },
    );
    assert!(matches!(
        mismatch_after_leader_change,
        Response::Error { code, .. } if code == "request_id_content_conflict"
    ));
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
    wait_for_message_for_consumer_at(
        replicated_node.broker_addr,
        "events",
        REPLICATION_OBSERVER,
        2,
        "after-leader-failure",
    );
    assert_live_nodes(&mut nodes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_batch_retry_after_leader_change_does_not_duplicate_records() {
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

    let stream = "typed-batch-failover";
    create_stream_on_any(&mut nodes, stream);
    let contacted_leader = data_group_leader(&nodes, stream);

    let proxy_listener = AsyncTcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let (response_sender, response_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let proxy = tokio::spawn(withhold_first_response_proxy(
        proxy_listener,
        nodes[contacted_leader].broker_addr,
        response_sender,
        release_receiver,
    ));

    let binary_payload = vec![0, 1, 255, b'\n', 0];
    let records = vec![
        PublishBatchRecord::with_options(
            binary_payload.clone(),
            PublishOptions::default()
                .with_key("first-key")
                .with_request_id("leader-change-batch-first"),
        ),
        PublishBatchRecord::with_options(
            b"second".to_vec(),
            PublishOptions::default()
                .with_key("second-key")
                .with_request_id("leader-change-batch-second"),
        ),
    ];
    let mut client = Client::connect_with_config(
        proxy_address,
        ClientConfig {
            connect_timeout: REQUEST_ATTEMPT_TIMEOUT,
            request_timeout: REQUEST_ATTEMPT_TIMEOUT,
            response_timeout: WITHHELD_BATCH_RESPONSE_TIMEOUT,
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let mut publish = Box::pin(client.publish_batch(stream, records.clone()));
    let broker_response = tokio::select! {
        attempt = &mut publish => panic!("proxy should withhold the successful batch response: {attempt:?}"),
        response = response_receiver => response.expect("proxy should deliver the broker response to the test"),
    };

    let Response::PublishBatch {
        stream: response_stream,
        outcomes,
    } = serde_json::from_slice::<Response>(&broker_response).unwrap()
    else {
        panic!("the contacted leader should return a publish-batch response");
    };
    assert_eq!(response_stream, stream);
    assert_eq!(outcomes.len(), 2);
    assert!(matches!(
        &outcomes[0],
        PublishBatchRecordResponse::Published { offset: 0 }
    ));
    assert!(matches!(
        &outcomes[1],
        PublishBatchRecordResponse::Published { offset: 1 }
    ));
    assert_eq!(
        data_group_leader(&nodes, stream),
        contacted_leader,
        "the responding broker should still be the data-group leader before it is stopped"
    );

    nodes[contacted_leader].stop();
    let surviving_leader = data_group_leader(&nodes, stream);
    assert_ne!(surviving_leader, contacted_leader);

    release_sender
        .send(())
        .expect("proxy should still be holding the client connection");
    let lost_attempt = publish.await;
    assert!(matches!(
        lost_attempt.attempt.as_ref(),
        Some(AttemptFailure::Client(ClientError::Eof))
    ));
    assert_eq!(lost_attempt.outcomes.len(), records.len());
    assert!(lost_attempt.outcomes.iter().all(|outcome| matches!(
        outcome,
        PublishBatchOutcome::Unknown { code, .. } if code == "client_error"
    )));
    proxy
        .await
        .expect("response proxy should finish after releasing the client");

    client
        .reconnect(nodes[surviving_leader].broker_addr)
        .await
        .unwrap();
    let retry = client.publish_batch(stream, records).await;
    assert!(retry.attempt.is_none());
    assert_eq!(
        retry.outcomes,
        vec![
            PublishBatchOutcome::Confirmed(PublishReceipt {
                stream: stream.to_owned(),
                offset: 0,
            }),
            PublishBatchOutcome::Confirmed(PublishReceipt {
                stream: stream.to_owned(),
                offset: 1,
            }),
        ]
    );

    let first = client
        .poll_bytes(stream, "verifier")
        .await
        .unwrap()
        .expect("the first batch record should be present once after failover");
    assert_eq!(first.offset, 0);
    assert_eq!(first.key.as_deref(), Some("first-key"));
    assert_eq!(first.payload, binary_payload);
    client.ack(stream, "verifier", first.offset).await.unwrap();

    let second = client
        .poll_bytes(stream, "verifier")
        .await
        .unwrap()
        .expect("the second batch record should be present once after failover");
    assert_eq!(second.offset, 1);
    assert_eq!(second.key.as_deref(), Some("second-key"));
    assert_eq!(second.payload, b"second");
    client.ack(stream, "verifier", second.offset).await.unwrap();
    assert!(
        client
            .poll_bytes(stream, "verifier")
            .await
            .unwrap()
            .is_none()
    );
    assert_live_nodes(&mut nodes);
}

#[test]
fn three_process_cluster_serializes_same_key_while_other_keys_progress() {
    let directory = TempDir::new().unwrap();
    let addresses = (0..9).map(|_| free_addr()).collect::<Vec<_>>();
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[7]), (3, addresses[8])];
    let mut nodes = vec![
        RunningNode::start_with_ack_timeout(
            1,
            addresses[0],
            addresses[3],
            addresses[6],
            directory.path().join("node-1"),
            cluster_nodes.clone(),
            true,
            KEY_ORDERING_ACK_TIMEOUT_MS,
        ),
        RunningNode::start_with_ack_timeout(
            2,
            addresses[1],
            addresses[4],
            addresses[7],
            directory.path().join("node-2"),
            cluster_nodes.clone(),
            false,
            KEY_ORDERING_ACK_TIMEOUT_MS,
        ),
        RunningNode::start_with_ack_timeout(
            3,
            addresses[2],
            addresses[5],
            addresses[8],
            directory.path().join("node-3"),
            cluster_nodes,
            false,
            KEY_ORDERING_ACK_TIMEOUT_MS,
        ),
    ];
    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    let producer_node = create_stream_on_any(&mut nodes, "ordered-jobs");
    for (offset, key, payload) in [
        (0, "key-a", "first-a"),
        (1, "key-a", "second-a"),
        (2, "key-b", "first-b"),
    ] {
        assert!(matches!(
            wait_for_response_at(
                nodes[producer_node].broker_addr,
                || Request::Publish {
                    stream: "ordered-jobs".to_owned(),
                    key: Some(key.to_owned()),
                    payload: payload.to_owned(),
                    request_id: Some(format!("ordered-job-{offset}")),
                },
                |response| matches!(response, Response::Published { offset: published, .. } if *published == offset),
            ),
            Response::Published { offset: published, .. } if published == offset
        ));
    }

    let member_a_node = (producer_node + 1) % nodes.len();
    let member_b_node = (producer_node + 2) % nodes.len();
    let first_a = wait_for_response_at(
        nodes[member_a_node].broker_addr,
        || Request::PollGroup {
            stream: "ordered-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 0,
                    key: Some(key),
                    payload,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                } if key == "key-a" && payload == "first-a"
            )
        },
    );
    let first_a_token = match first_a {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected first key-a delivery, got {response:?}"),
    };

    let first_b = wait_for_response_at(
        nodes[member_b_node].broker_addr,
        || Request::PollGroup {
            stream: "ordered-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-b".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 2,
                    key: Some(key),
                    payload,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                } if key == "key-b" && payload == "first-b"
            )
        },
    );
    let first_b_token = match first_b {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected first key-b delivery, got {response:?}"),
    };
    assert!(matches!(
        wait_for_response_at(
            nodes[member_b_node].broker_addr,
            || Request::AckGroup {
                stream: "ordered-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
                offset: 2,
                delivery_token: first_b_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));

    // Treat either semantic response as terminal: if the same-key successor
    // escapes the gate, fail immediately instead of retrying until lease expiry.
    let blocked_poll = wait_for_response_at(
        nodes[member_b_node].broker_addr,
        || Request::PollGroup {
            stream: "ordered-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-b".to_owned(),
        },
        |response| matches!(response, Response::Empty { .. } | Response::Message { .. }),
    );
    assert!(
        matches!(blocked_poll, Response::Empty { .. }),
        "member-b received the same-key successor while member-a held its lease: {blocked_poll:?}"
    );

    let repeated_a = wait_for_response_at(
        nodes[member_a_node].broker_addr,
        || Request::PollGroup {
            stream: "ordered-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
        },
        |response| matches!(response, Response::Message { .. }),
    );
    assert!(
        matches!(
            &repeated_a,
            Response::Message {
                offset: 0,
                key: Some(key),
                payload,
                delivery_attempt: Some(1),
                delivery_token: Some(token),
                ..
            } if key == "key-a" && payload == "first-a" && token == &first_a_token
        ),
        "repeated member-a poll changed its held receipt: {repeated_a:?}"
    );

    assert!(matches!(
        wait_for_response_at(
            nodes[member_a_node].broker_addr,
            || Request::AckGroup {
                stream: "ordered-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first_a_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));

    assert!(matches!(
        wait_for_response_at(
            nodes[member_b_node].broker_addr,
            || Request::PollGroup {
                stream: "ordered-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 1,
                    key: Some(key),
                    payload,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                } if key == "key-a" && payload == "second-a"
            ),
        ),
        Response::Message {
            offset: 1,
            key: Some(key),
            payload,
            ..
        } if key == "key-a" && payload == "second-a"
    ));
    assert_live_nodes(&mut nodes);
}

#[test]
fn three_process_cluster_preserves_group_delivery_through_replica_restart() {
    let directory = TempDir::new().unwrap();
    let addresses = (0..9).map(|_| free_addr()).collect::<Vec<_>>();
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[7]), (3, addresses[8])];
    let mut nodes = vec![
        RunningNode::start_with_ack_timeout(
            1,
            addresses[0],
            addresses[3],
            addresses[6],
            directory.path().join("node-1"),
            cluster_nodes.clone(),
            true,
            REASSIGN_ACK_TIMEOUT_MS,
        ),
        RunningNode::start_with_ack_timeout(
            2,
            addresses[1],
            addresses[4],
            addresses[7],
            directory.path().join("node-2"),
            cluster_nodes.clone(),
            false,
            REASSIGN_ACK_TIMEOUT_MS,
        ),
        RunningNode::start_with_ack_timeout(
            3,
            addresses[2],
            addresses[5],
            addresses[8],
            directory.path().join("node-3"),
            cluster_nodes,
            false,
            REASSIGN_ACK_TIMEOUT_MS,
        ),
    ];
    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    let leader = create_stream_on_any(&mut nodes, "restart-jobs");
    for (index, payload) in ["first", "second"].into_iter().enumerate() {
        assert!(matches!(
            wait_for_response_at(
                nodes[leader].broker_addr,
                || Request::Publish {
                    stream: "restart-jobs".to_owned(),
                    key: None,
                    payload: payload.to_owned(),
                    request_id: Some(format!("restart-job-{index}")),
                },
                |response| matches!(response, Response::Published { offset, .. } if *offset == index as u64),
            ),
            Response::Published { offset, .. } if offset == index as u64
        ));
    }

    // A separate consumer must receive and durably acknowledge both records;
    // the shared consumer below should still receive its own copy.
    let fanout_node = (leader + 2) % nodes.len();
    for (offset, payload) in [(0, "first"), (1, "second")] {
        assert!(matches!(
            wait_for_response_at(
                nodes[fanout_node].broker_addr,
                || Request::Poll {
                    stream: "restart-jobs".to_owned(),
                    consumer: "fanout".to_owned(),
                },
                |response| matches!(
                    response,
                    Response::Message {
                        offset: received_offset,
                        payload: received_payload,
                        ..
                    } if *received_offset == offset && received_payload == payload
                ),
            ),
            Response::Message { offset: received_offset, .. } if received_offset == offset
        ));
        assert!(matches!(
            wait_for_response_at(
                nodes[fanout_node].broker_addr,
                || Request::Ack {
                    stream: "restart-jobs".to_owned(),
                    consumer: "fanout".to_owned(),
                    offset,
                },
                |response| matches!(response, Response::Acknowledged { .. }),
            ),
            Response::Acknowledged { .. }
        ));
    }

    let replica = (leader + 1) % nodes.len();
    let first = wait_for_response_at(
        nodes[replica].broker_addr,
        || Request::PollGroup {
            stream: "restart-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
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
                } if payload == "first"
            )
        },
    );
    let first_token = match first {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected first grouped delivery, got {response:?}"),
    };
    let second = wait_for_response_at(
        nodes[leader].broker_addr,
        || Request::PollGroup {
            stream: "restart-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-b".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 1,
                    payload,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                } if payload == "second"
            )
        },
    );
    let second_token = match second {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected second grouped delivery, got {response:?}"),
    };

    // The grouped leases and tokens are replicated state. A follower restart
    // must return each still-live member's existing delivery, not reassign it.
    nodes[replica].restart();
    assert!(matches!(
        wait_for_response_at(
            nodes[replica].broker_addr,
            || Request::PollGroup {
                stream: "restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 0,
                    delivery_attempt: Some(1),
                    delivery_token: Some(token),
                    ..
                } if token == &first_token
            ),
        ),
        Response::Message { offset: 0, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[replica].broker_addr,
            || Request::PollGroup {
                stream: "restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 1,
                    delivery_attempt: Some(1),
                    delivery_token: Some(token),
                    ..
                } if token == &second_token
            ),
        ),
        Response::Message { offset: 1, .. }
    ));

    // Acknowledging the higher offset first exercises durable out-of-order
    // progress while the lower offset remains eligible for redelivery.
    assert!(matches!(
        wait_for_response_at(
            nodes[replica].broker_addr,
            || Request::AckGroup {
                stream: "restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
                offset: 1,
                delivery_token: second_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));
    sleep(Duration::from_millis(REASSIGN_ACK_TIMEOUT_MS + 100));

    // The expired token is fenced by an acknowledgement command even though
    // no replacement poll has assigned the record yet.
    assert!(matches!(
        wait_for_response_at(
            nodes[replica].broker_addr,
            || Request::AckGroup {
                stream: "restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first_token.clone(),
            },
            |response| matches!(response, Response::Error { code, .. } if code == "stale_delivery"),
        ),
        Response::Error { .. }
    ));

    let redelivered = wait_for_response_at(
        nodes[replica].broker_addr,
        || Request::PollGroup {
            stream: "restart-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-c".to_owned(),
        },
        |response| {
            matches!(
                response,
                Response::Message {
                    offset: 0,
                    payload,
                    delivery_attempt: Some(2),
                    delivery_token: Some(_),
                    ..
                } if payload == "first"
            )
        },
    );
    let redelivered_token = match redelivered {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected redelivered grouped message, got {response:?}"),
    };
    assert_ne!(first_token, redelivered_token);
    assert!(matches!(
        wait_for_response_at(
            nodes[replica].broker_addr,
            || Request::AckGroup {
                stream: "restart-jobs".to_owned(),
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
            nodes[replica].broker_addr,
            || Request::AckGroup {
                stream: "restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-c".to_owned(),
                offset: 0,
                delivery_token: redelivered_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));

    assert!(matches!(
        wait_for_response_at(
            nodes[replica].broker_addr,
            || Request::PollGroup {
                stream: "restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-d".to_owned(),
            },
            |response| matches!(response, Response::Empty { .. }),
        ),
        Response::Empty { .. }
    ));

    // Both independent and grouped acknowledgements must remain durable after
    // the replica rejoins again; neither consumer should see a duplicate.
    nodes[replica].restart();
    assert!(matches!(
        wait_for_response_at(
            nodes[replica].broker_addr,
            || Request::Poll {
                stream: "restart-jobs".to_owned(),
                consumer: "fanout".to_owned(),
            },
            |response| matches!(response, Response::Empty { .. }),
        ),
        Response::Empty { .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[replica].broker_addr,
            || Request::PollGroup {
                stream: "restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-e".to_owned(),
            },
            |response| matches!(response, Response::Empty { .. }),
        ),
        Response::Empty { .. }
    ));
    assert_live_nodes(&mut nodes);
}

#[test]
fn three_process_cluster_preserves_pending_group_delivery_through_full_restart() {
    let directory = TempDir::new().unwrap();
    let addresses = (0..9).map(|_| free_addr()).collect::<Vec<_>>();
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[7]), (3, addresses[8])];
    let mut nodes = vec![
        RunningNode::start_with_ack_timeout(
            1,
            addresses[0],
            addresses[3],
            addresses[6],
            directory.path().join("node-1"),
            cluster_nodes.clone(),
            true,
            FULL_CLUSTER_RESTART_ACK_TIMEOUT_MS,
        ),
        RunningNode::start_with_ack_timeout(
            2,
            addresses[1],
            addresses[4],
            addresses[7],
            directory.path().join("node-2"),
            cluster_nodes.clone(),
            false,
            FULL_CLUSTER_RESTART_ACK_TIMEOUT_MS,
        ),
        RunningNode::start_with_ack_timeout(
            3,
            addresses[2],
            addresses[5],
            addresses[8],
            directory.path().join("node-3"),
            cluster_nodes,
            false,
            FULL_CLUSTER_RESTART_ACK_TIMEOUT_MS,
        ),
    ];
    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    let leader = create_stream_on_any(&mut nodes, "pending-restart-jobs");
    assert!(matches!(
        wait_for_response_at(
            nodes[leader].broker_addr,
            || Request::Publish {
                stream: "pending-restart-jobs".to_owned(),
                key: None,
                payload: "recover-pending-work".to_owned(),
                request_id: Some("pending-restart-work".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));

    let pending = wait_for_response_on_any(
        &mut nodes,
        || Request::PollGroup {
            stream: "pending-restart-jobs".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
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
                } if payload == "recover-pending-work"
            )
        },
    );
    let pending_token = match pending {
        Response::Message {
            delivery_token: Some(token),
            ..
        } => token,
        response => panic!("expected initial grouped delivery, got {response:?}"),
    };

    // All replicas stop with the lease pending. Restart each node from its
    // original directory before asking the recovered group for the receipt.
    for node in &mut nodes {
        node.stop();
    }
    for node in &mut nodes {
        node.restart();
    }

    assert!(matches!(
        wait_for_response_on_any(
            &mut nodes,
            || Request::PollGroup {
                stream: "pending-restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 0,
                    payload,
                    delivery_attempt: Some(1),
                    delivery_token: Some(token),
                    ..
                } if payload == "recover-pending-work" && token == &pending_token
            ),
        ),
        Response::Message { offset: 0, .. }
    ));
    assert!(matches!(
        wait_for_response_on_any(
            &mut nodes,
            || Request::AckGroup {
                stream: "pending-restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: pending_token.clone(),
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));
    assert!(matches!(
        wait_for_response_on_any(
            &mut nodes,
            || Request::PollGroup {
                stream: "pending-restart-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-b".to_owned(),
            },
            |response| matches!(response, Response::Empty { .. }),
        ),
        Response::Empty { .. }
    ));
    assert_live_nodes(&mut nodes);
}

#[test]
fn three_process_cluster_reassigns_group_delivery_after_node_failure() {
    let directory = TempDir::new().unwrap();
    let addresses = (0..9).map(|_| free_addr()).collect::<Vec<_>>();
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[7]), (3, addresses[8])];
    let mut nodes = vec![
        RunningNode::start_with_ack_timeout(
            1,
            addresses[0],
            addresses[3],
            addresses[6],
            directory.path().join("node-1"),
            cluster_nodes.clone(),
            true,
            REASSIGN_ACK_TIMEOUT_MS,
        ),
        RunningNode::start_with_ack_timeout(
            2,
            addresses[1],
            addresses[4],
            addresses[7],
            directory.path().join("node-2"),
            cluster_nodes.clone(),
            false,
            REASSIGN_ACK_TIMEOUT_MS,
        ),
        RunningNode::start_with_ack_timeout(
            3,
            addresses[2],
            addresses[5],
            addresses[8],
            directory.path().join("node-3"),
            cluster_nodes,
            false,
            REASSIGN_ACK_TIMEOUT_MS,
        ),
    ];
    for node in &nodes {
        wait_for_http(node.http_addr);
    }

    create_stream_on_any(&mut nodes, "failover-jobs");
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
    sleep(Duration::from_millis(REASSIGN_ACK_TIMEOUT_MS + 100));
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
        Response::Acknowledged { .. }
    ));

    nodes[0].restart();
    assert!(matches!(
        wait_for_response_at(
            nodes[0].broker_addr,
            || Request::PollGroup {
                stream: "failover-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-f".to_owned(),
            },
            |response| matches!(response, Response::Empty { .. }),
        ),
        Response::Empty { .. }
    ));
    // The survivor's acknowledgement must remain terminal after the failed
    // node rejoins; retrying the old token can only report already-acknowledged
    // progress and must not commit a new delivery.
    assert!(matches!(
        wait_for_response_at(
            nodes[0].broker_addr,
            || Request::AckGroup {
                stream: "failover-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-a".to_owned(),
                offset: 0,
                delivery_token: first_token.clone(),
            },
            |response| matches!(
                response,
                Response::Acknowledged {
                    already_acknowledged: true,
                    ..
                }
            ),
        ),
        Response::Acknowledged {
            already_acknowledged: true,
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
    sleep(Duration::from_millis(REASSIGN_ACK_TIMEOUT_MS + 100));
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
    sleep(Duration::from_millis(REASSIGN_ACK_TIMEOUT_MS + 100));
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
        Response::Acknowledged { .. }
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
    assert_live_nodes(&mut nodes);
}

#[test]
fn three_process_cluster_transfers_consumer_policy_and_delivery_snapshot_after_leader_failure() {
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

    create_stream_on_any(&mut nodes, "policy-transfer-jobs");
    // Public requests forward from followers, so probe the peer listener
    // directly. Its local inspect operation reports NotLeader on followers
    // and returns policy state without changing delivery state on the leader.
    let initial_leader = data_group_leader(&nodes, "policy-transfer-jobs");
    assert!(matches!(
        wait_for_response_at(
            nodes[initial_leader].broker_addr,
            || Request::ConfigureConsumer {
                stream: "policy-transfer-jobs".to_owned(),
                consumer: "workers".to_owned(),
                ack_timeout_ms: 0,
                max_delivery_attempts: Some(1),
            },
            |response| matches!(
                response,
                Response::ConsumerPolicy {
                    version: 1,
                    configured: true,
                    ack_timeout_ms: 0,
                    max_delivery_attempts: Some(1),
                    ..
                }
            ),
        ),
        Response::ConsumerPolicy {
            version: 1,
            configured: true,
            ack_timeout_ms: 0,
            max_delivery_attempts: Some(1),
            ..
        }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[initial_leader].broker_addr,
            || Request::Publish {
                stream: "policy-transfer-jobs".to_owned(),
                key: None,
                payload: "use-pinned-policy".to_owned(),
                request_id: Some("policy-transfer-job".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 0, .. }),
        ),
        Response::Published { offset: 0, .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[initial_leader].broker_addr,
            || Request::PollGroup {
                stream: "policy-transfer-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-before-failure".to_owned(),
            },
            |response| matches!(
                response,
                Response::Message {
                    offset: 0,
                    delivery_attempt: Some(1),
                    delivery_token: Some(_),
                    ..
                }
            ),
        ),
        Response::Message {
            offset: 0,
            delivery_attempt: Some(1),
            delivery_token: Some(_),
            ..
        }
    ));

    // Version 2 applies to future records. The unacknowledged record must
    // retain version 1 even after its state is replayed by a new leader.
    assert!(matches!(
        wait_for_response_at(
            nodes[initial_leader].broker_addr,
            || Request::ConfigureConsumer {
                stream: "policy-transfer-jobs".to_owned(),
                consumer: "workers".to_owned(),
                ack_timeout_ms: 10_000,
                max_delivery_attempts: Some(3),
            },
            |response| matches!(
                response,
                Response::ConsumerPolicy {
                    version: 2,
                    configured: true,
                    ack_timeout_ms: 10_000,
                    max_delivery_attempts: Some(3),
                    ..
                }
            ),
        ),
        Response::ConsumerPolicy {
            version: 2,
            configured: true,
            ack_timeout_ms: 10_000,
            max_delivery_attempts: Some(3),
            ..
        }
    ));
    sleep(Duration::from_millis(10));
    nodes[initial_leader].stop();

    let new_leader = data_group_leader(&nodes, "policy-transfer-jobs");
    assert_ne!(new_leader, initial_leader);
    assert!(matches!(
        wait_for_response_at(
            nodes[new_leader].broker_addr,
            || Request::InspectConsumer {
                stream: "policy-transfer-jobs".to_owned(),
                consumer: "workers".to_owned(),
            },
            |response| matches!(
                response,
                Response::ConsumerPolicy {
                    version: 2,
                    configured: true,
                    ack_timeout_ms: 10_000,
                    max_delivery_attempts: Some(3),
                    ..
                }
            ),
        ),
        Response::ConsumerPolicy {
            version: 2,
            configured: true,
            ack_timeout_ms: 10_000,
            max_delivery_attempts: Some(3),
            ..
        }
    ));

    // Version 1's immediate expiry and one-attempt budget terminally move the
    // first delivery. Version 2 would allow a second attempt, so this Empty
    // response distinguishes the delivery snapshot from current consumer state.
    assert!(matches!(
        request(
            nodes[new_leader].broker_addr,
            Request::PollGroup {
                stream: "policy-transfer-jobs".to_owned(),
                consumer: "workers".to_owned(),
                member: "member-after-failure".to_owned(),
            },
        ),
        Ok(Response::Empty { .. })
    ));
    assert!(matches!(
        request(
            nodes[new_leader].broker_addr,
            Request::PollGroup {
                stream: "policy-transfer-jobs.dead-letter".to_owned(),
                consumer: "inspector".to_owned(),
                member: "inspector-1".to_owned(),
            },
        ),
        Ok(Response::Message {
            offset: 0,
            payload,
            delivery_attempt: Some(1),
            delivery_token: Some(_),
            ..
        }) if payload == "use-pinned-policy"
    ));
    assert_live_nodes(&mut nodes);
}

#[cfg(feature = "test-replacement-recovery")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_node_recovers_after_repeated_snapshot_interruptions() {
    let directory = TempDir::new().unwrap();
    let addresses = (0..10).map(|_| free_addr()).collect::<Vec<_>>();
    let proxy_used = Arc::new(AtomicBool::new(false));
    let (snapshot_chunk_sender, mut snapshot_chunk_receiver) = mpsc::unbounded_channel();
    let (release_snapshot_sender, release_snapshot_receiver) = watch::channel(false);
    let replacement_proxy = AsyncTcpListener::bind(addresses[9]).await.unwrap();
    let proxy = tokio::spawn(replacement_snapshot_gate_proxy(
        replacement_proxy,
        addresses[7],
        Arc::clone(&proxy_used),
        snapshot_chunk_sender,
        release_snapshot_receiver,
    ));
    let cluster_nodes = vec![(1, addresses[6]), (2, addresses[9]), (3, addresses[8])];
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

    let leader = create_stream_on_any(&mut nodes, "events");
    assert_eq!(leader, 0, "the bootstrapped node should lead initially");
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

    let replacement = 1;
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
                &mut nodes,
                Some(replacement),
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
    let transfer_leader = data_group_leader(&nodes, "events");
    nodes[replacement].replace_storage(directory.path().join("empty-replacement"));

    tokio::time::timeout(CLUSTER_WAIT_TIMEOUT, snapshot_chunk_receiver.recv())
        .await
        .expect("replacement did not receive a non-final events data-group snapshot chunk")
        .expect("snapshot gate proxy stopped before observing the events data group");
    assert!(proxy_used.load(Ordering::Acquire));
    let transfer_leader_response =
        direct_peer_inspect_consumer(nodes[transfer_leader].peer_addr, "events");
    assert!(
        transfer_leader_response["Forward"]["ConsumerPolicy"]["Ok"].is_object(),
        "the original data-group leader lost authority before the held snapshot chunk: {transfer_leader_response}"
    );
    nodes[transfer_leader].stop();
    release_snapshot_sender
        .send(true)
        .expect("snapshot gate proxy should still be waiting for release");
    let successor_leader = data_group_leader_excluding(&nodes, "events", Some(replacement));
    assert_ne!(successor_leader, transfer_leader);

    // The receiver accepted a non-final data-group chunk from the failed
    // leader. Restarting it discards that partial transfer while the successor
    // is active, then the remaining attempts preserve repeated interruption
    // coverage.
    nodes[replacement].stop();
    nodes[replacement].restart();
    for attempt in 1..SNAPSHOT_INTERRUPTION_ATTEMPTS {
        wait_for_active_snapshot_transfer(nodes[replacement].http_addr, attempt);
        nodes[replacement].stop();
        if attempt + 1 < SNAPSHOT_INTERRUPTION_ATTEMPTS {
            nodes[replacement].restart();
        }
    }
    nodes[replacement].restart();
    wait_for_metric_at_least(
        nodes[replacement].http_addr,
        "runnel_snapshot_installs_completed_total",
        1,
    );
    wait_for_message_at(nodes[replacement].broker_addr, 1, "snapshot-1");
    wait_for_message_for_consumer_at(
        nodes[successor_leader].broker_addr,
        "events",
        "worker",
        1,
        "snapshot-1",
    );
    wait_for_message_for_consumer_at(
        nodes[replacement].broker_addr,
        "events",
        "inspector",
        0,
        "seed",
    );
    assert!(matches!(
        wait_for_response_at(
            nodes[replacement].broker_addr,
            || Request::Ack {
                stream: "events".to_owned(),
                consumer: "inspector".to_owned(),
                offset: 0,
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));
    assert!(matches!(
        wait_for_response_at(
            nodes[replacement].broker_addr,
            || Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 1,
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));
    let metrics = http_metrics(nodes[replacement].http_addr);
    assert!(
        metric_value(&metrics, "runnel_snapshot_transfer_chunks_received_total") >= 2,
        "replacement metrics did not report a multi-chunk snapshot after repeated retries:\n{metrics}"
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

    nodes[transfer_leader].restart();
    wait_for_message_for_consumer_at(
        nodes[transfer_leader].broker_addr,
        "events",
        "worker",
        2,
        "snapshot-2",
    );
    let post_snapshot_failed_leader = data_group_leader(&nodes, "events");
    nodes[post_snapshot_failed_leader].stop();
    let recovered_leader = wait_for_stream_on_any(&mut nodes, "events");
    assert_ne!(recovered_leader, post_snapshot_failed_leader);
    assert!(matches!(
        wait_for_response_at(
            nodes[recovered_leader].broker_addr,
            || Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "after-recovery-leader-failure".to_owned(),
                request_id: Some("after-recovery-leader-failure".to_owned()),
            },
            |response| matches!(response, Response::Published { offset: 49, .. }),
        ),
        Response::Published { offset: 49, .. }
    ));
    wait_for_message_for_consumer_at(
        nodes[recovered_leader].broker_addr,
        "events",
        "worker",
        2,
        "snapshot-2",
    );
    assert!(matches!(
        wait_for_response_at(
            nodes[recovered_leader].broker_addr,
            || Request::Ack {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 2,
            },
            |response| matches!(response, Response::Acknowledged { .. }),
        ),
        Response::Acknowledged { .. }
    ));
    nodes[post_snapshot_failed_leader].restart();
    wait_for_message_for_consumer_at(
        nodes[post_snapshot_failed_leader].broker_addr,
        "events",
        "worker",
        3,
        "snapshot-3",
    );
    assert_live_nodes(&mut nodes);
    proxy.abort();
    let _ = proxy.await;
}

// Keep process-launch parameters explicit so the test's process and topology
// configuration remains visible without a second configuration abstraction.
#[allow(clippy::too_many_arguments)]
fn spawn_node(
    node_id: u64,
    broker_addr: SocketAddr,
    http_addr: SocketAddr,
    peer_addr: SocketAddr,
    data_dir: &Path,
    cluster_nodes: &[(u64, SocketAddr)],
    bootstrap: bool,
    ack_timeout_ms: u64,
) -> Child {
    let mut command = Command::new(server_binary());
    let ack_timeout_ms = ack_timeout_ms.to_string();
    command
        .args([
            "--engine",
            "raft",
            "--ack-timeout-ms",
            &ack_timeout_ms,
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
    if std::env::var_os("RUNNEL_TEST_CAPTURE_LOGS").is_some() {
        let log_path = std::env::var_os("RUNNEL_ISOLATION_ARTIFACTS")
            .map(PathBuf::from)
            .map(|artifact_dir| artifact_dir.join("cluster-logs"))
            .inspect(|log_dir| {
                fs::create_dir_all(log_dir).expect("node log directory should be writable")
            })
            .map(|log_dir| log_dir.join(format!("node-{node_id}.log")))
            .unwrap_or_else(|| data_dir.with_extension("log"));
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .expect("node log should be writable");
        let stderr = stdout.try_clone().expect("node log should be cloneable");
        command
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
    }
    for (id, address) in cluster_nodes {
        command.args(["--cluster-node", &format!("{id}={address}")]);
    }
    if bootstrap {
        command.arg("--bootstrap");
    }
    command.spawn().expect("runnel node should start")
}

fn create_stream_on_any(nodes: &mut [RunningNode], stream: &str) -> usize {
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

fn data_group_leader(nodes: &[RunningNode], stream: &str) -> usize {
    data_group_leader_excluding(nodes, stream, None)
}

fn data_group_leader_excluding(
    nodes: &[RunningNode],
    stream: &str,
    excluded_index: Option<usize>,
) -> usize {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        let mut leader = None;
        for (index, node) in nodes.iter().enumerate() {
            if node.child.is_none() || excluded_index == Some(index) {
                continue;
            }
            let response = direct_peer_inspect_consumer(node.peer_addr, stream);
            let result = &response["Forward"]["ConsumerPolicy"];
            if result["Ok"].is_object() {
                assert!(
                    leader.replace(index).is_none(),
                    "multiple data-group leaders responded"
                );
                continue;
            }
            assert!(
                result["Err"]["NotLeader"].is_object(),
                "peer {} returned an unexpected leader probe response: {response}",
                node.node_id
            );
        }
        if let Some(leader) = leader {
            return leader;
        }
        sleep(Duration::from_millis(50));
    }
    panic!("no eligible live process reported itself as the data-group leader for {stream}");
}

async fn withhold_first_response_proxy(
    listener: AsyncTcpListener,
    broker_addr: SocketAddr,
    response_sender: oneshot::Sender<Vec<u8>>,
    release_receiver: oneshot::Receiver<()>,
) {
    let (client, _) = listener
        .accept()
        .await
        .expect("proxy should accept the typed client connection");
    let (client_reader, client_writer) = client.into_split();
    let mut client_reader = AsyncBufReader::new(client_reader);
    let mut request = Vec::new();
    client_reader
        .read_until(b'\n', &mut request)
        .await
        .expect("proxy should read the client's batch request");

    let broker = AsyncTcpStream::connect(broker_addr)
        .await
        .expect("proxy should connect to the current leader");
    let (broker_reader, mut broker_writer) = broker.into_split();
    broker_writer
        .write_all(&request)
        .await
        .expect("proxy should forward the batch to the leader");
    let mut broker_reader = AsyncBufReader::new(broker_reader);
    let mut response = Vec::new();
    broker_reader
        .read_until(b'\n', &mut response)
        .await
        .expect("proxy should read the leader's complete response");
    assert!(
        response.ends_with(b"\n"),
        "broker response should be line complete"
    );
    response_sender
        .send(response)
        .expect("test should inspect the successful broker response");

    release_receiver
        .await
        .expect("test should release the withheld client connection");
    drop(client_reader);
    drop(client_writer);
}

fn direct_peer_inspect_consumer(peer_addr: SocketAddr, stream: &str) -> serde_json::Value {
    let request = serde_json::json!({
        "Forward": {
            "InspectConsumer": {
                "stream": stream,
                "consumer": "leader-probe"
            }
        }
    });
    let encoded = serde_json::to_vec(&request).expect("peer probe request should encode");
    let frame_size = u32::try_from(encoded.len()).expect("peer probe frame should fit in u32");
    let mut connection = TcpStream::connect_timeout(&peer_addr, REQUEST_ATTEMPT_TIMEOUT)
        .expect("peer probe should connect to a live process");
    connection
        .set_read_timeout(Some(REQUEST_ATTEMPT_TIMEOUT))
        .expect("peer probe read timeout should be set");
    connection
        .set_write_timeout(Some(REQUEST_ATTEMPT_TIMEOUT))
        .expect("peer probe write timeout should be set");
    connection
        .write_all(&frame_size.to_be_bytes())
        .and_then(|()| connection.write_all(&encoded))
        .expect("peer probe request should be written");

    let mut response_size = [0; 4];
    connection
        .read_exact(&mut response_size)
        .expect("peer probe response frame should be readable");
    let mut response = vec![0; u32::from_be_bytes(response_size) as usize];
    connection
        .read_exact(&mut response)
        .expect("peer probe response should be readable");
    serde_json::from_slice(&response).expect("peer probe response should decode")
}

fn wait_for_stream_on_any(nodes: &mut [RunningNode], stream: &str) -> usize {
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
    nodes: &mut [RunningNode],
    mut predicate: impl FnMut(&RunningNode) -> bool,
) -> usize {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        assert_live_nodes(nodes);
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

fn wait_for_response_on_any(
    nodes: &mut [RunningNode],
    request_builder: impl FnMut() -> Request,
    predicate: impl FnMut(&Response) -> bool,
) -> Response {
    wait_for_response_on_any_with_timeout(
        nodes,
        None,
        REQUEST_ATTEMPT_TIMEOUT,
        request_builder,
        predicate,
    )
}

fn wait_for_response_on_any_with_timeout(
    nodes: &mut [RunningNode],
    excluded: Option<usize>,
    attempt_timeout: Duration,
    mut request_builder: impl FnMut() -> Request,
    mut predicate: impl FnMut(&Response) -> bool,
) -> Response {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut last_response = None;
    while Instant::now() < deadline {
        assert_live_nodes(nodes);
        for (index, node) in nodes.iter().enumerate() {
            if excluded == Some(index) || node.child.is_none() {
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

fn assert_live_nodes(nodes: &mut [RunningNode]) {
    for node in nodes {
        let Some(child) = node.child.as_mut() else {
            continue;
        };
        if let Some(status) = child
            .try_wait()
            .expect("node process status should be readable")
        {
            panic!(
                "node {} exited unexpectedly with status {status}",
                node.node_id
            );
        }
    }
}

#[cfg(feature = "test-replacement-recovery")]
fn wait_for_message_at(address: SocketAddr, offset: u64, payload: &str) {
    wait_for_message_for_consumer_at(address, "events", "worker", offset, payload);
}

fn wait_for_message_for_consumer_at(
    address: SocketAddr,
    stream: &str,
    consumer: &str,
    offset: u64,
    payload: &str,
) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut last_response = None;
    while Instant::now() < deadline {
        let response = request_with_timeout(
            address,
            Request::Poll {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
            },
            REQUEST_ATTEMPT_TIMEOUT,
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
        "node {address} did not recover {stream}/{consumer} message at offset {offset}; expected payload {payload:?}; last response: {last_response:?}; metrics:\n{metrics}"
    );
}

#[cfg(feature = "test-replacement-recovery")]
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

#[cfg(feature = "test-replacement-recovery")]
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

#[cfg(feature = "test-replacement-recovery")]
fn wait_for_active_snapshot_transfer(address: SocketAddr, attempt: usize) {
    let deadline = Instant::now() + CLUSTER_WAIT_TIMEOUT;
    let mut last_metrics = None;
    let mut last_error = None;
    while Instant::now() < deadline {
        match try_http_metrics(address) {
            Ok(metrics) => {
                let chunks =
                    metric_value(&metrics, "runnel_snapshot_transfer_chunks_received_total");
                let final_chunks = metric_value(
                    &metrics,
                    "runnel_snapshot_transfer_final_chunks_received_total",
                );
                if chunks > final_chunks {
                    return;
                }
                last_metrics = Some(metrics);
            }
            Err(error) => last_error = Some(error),
        }
        sleep(Duration::from_millis(10));
    }
    panic!(
        "replacement node did not receive a non-final snapshot chunk during interruption attempt {attempt}; last metrics:\n{}; last scrape error: {last_error:?}",
        last_metrics.as_deref().unwrap_or("<no metrics response>")
    );
}

#[cfg(feature = "test-replacement-recovery")]
async fn replacement_snapshot_gate_proxy(
    listener: AsyncTcpListener,
    backend_address: SocketAddr,
    gate_used: Arc<AtomicBool>,
    gate_sender: mpsc::UnboundedSender<()>,
    release_receiver: watch::Receiver<bool>,
) {
    loop {
        let (mut client, _) = listener
            .accept()
            .await
            .expect("snapshot proxy should accept peer connections");
        let Ok(mut backend) = AsyncTcpStream::connect(backend_address).await else {
            continue;
        };
        let gate_used = Arc::clone(&gate_used);
        let gate_sender = gate_sender.clone();
        let mut release_receiver = release_receiver.clone();
        tokio::spawn(async move {
            loop {
                let request = match read_peer_frame(&mut client).await {
                    Ok(request) => request,
                    Err(_) => return,
                };
                let is_target_chunk =
                    is_non_final_snapshot_for_group(&request, "group/events/data");
                if write_peer_frame(&mut backend, &request).await.is_err() {
                    return;
                }
                let response = match read_peer_frame(&mut backend).await {
                    Ok(response) => response,
                    Err(_) => return,
                };
                let accepted_snapshot_chunk = is_successful_snapshot_response(&response);
                if is_target_chunk
                    && accepted_snapshot_chunk
                    && gate_used
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    if gate_sender.send(()).is_err() {
                        return;
                    }
                    while !*release_receiver.borrow() {
                        if release_receiver.changed().await.is_err() {
                            return;
                        }
                    }
                }
                if write_peer_frame(&mut client, &response).await.is_err() {
                    return;
                }
            }
        });
    }
}

#[cfg(feature = "test-replacement-recovery")]
fn is_non_final_snapshot_for_group(frame: &[u8], group_id: &str) -> bool {
    let Ok(request) = serde_json::from_slice::<serde_json::Value>(frame) else {
        return false;
    };
    let Some(snapshot) = request.get("InstallSnapshot") else {
        return false;
    };
    snapshot.get("group_id").and_then(serde_json::Value::as_str) == Some(group_id)
        && snapshot
            .get("request")
            .and_then(|request| request.get("done"))
            .and_then(serde_json::Value::as_bool)
            == Some(false)
}

#[cfg(feature = "test-replacement-recovery")]
fn is_successful_snapshot_response(frame: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(frame)
        .is_ok_and(|response| response.get("InstallSnapshot").is_some())
}

#[cfg(feature = "test-replacement-recovery")]
async fn read_peer_frame(stream: &mut AsyncTcpStream) -> Result<Vec<u8>, std::io::Error> {
    let length = stream.read_u32().await?;
    if length > 64 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer frame exceeds the test proxy limit",
        ));
    }
    let mut frame = vec![0; length as usize];
    stream.read_exact(&mut frame).await?;
    Ok(frame)
}

#[cfg(feature = "test-replacement-recovery")]
async fn write_peer_frame(stream: &mut AsyncTcpStream, frame: &[u8]) -> Result<(), std::io::Error> {
    let length = u32::try_from(frame.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer frame exceeds the test proxy limit",
        )
    })?;
    if length > 64 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "peer frame exceeds the test proxy limit",
        ));
    }
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(frame).await
}

fn batch_response_receipts(response: Response) -> Vec<(u64, String)> {
    let Response::PollBatch { messages, .. } = response else {
        panic!("expected a consume-batch response, got {response:?}");
    };
    messages
        .into_iter()
        .map(|message| match message {
            BatchMessageResponse::Text {
                offset,
                delivery_token,
                ..
            }
            | BatchMessageResponse::Bytes {
                offset,
                delivery_token,
                ..
            } => (
                offset,
                delivery_token.expect("batch message should carry its receipt"),
            ),
        })
        .collect()
}

#[cfg(feature = "test-replacement-recovery")]
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

#[cfg(feature = "test-replacement-recovery")]
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

#[cfg(feature = "test-replacement-recovery")]
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
