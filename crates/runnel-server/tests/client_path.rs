use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use runnel_client::{
    AttemptFailure, AttemptOutcome, BatchAcknowledgementOutcome, BatchDeliveryReceipt, Client,
    ClientConfig, ClientError, ConsumeBatchLimits, PublishBatchOutcome, PublishBatchRecord,
    PublishOptions, PublishReceipt,
};
use runnel_protocol::{PublishBatchRecordResponse, Response};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::net::{TcpListener as AsyncTcpListener, TcpStream as AsyncTcpStream};
use tokio::sync::oneshot;

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
    assert!(text_message.delivery_token.is_none());
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
    assert!(binary_message.delivery_token.is_none());
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

#[tokio::test]
async fn typed_client_reports_request_id_content_conflicts_without_appending() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &[]);
    let mut client = Client::connect(server.broker_addr).await.unwrap();
    client.create_stream("events").await.unwrap();

    let original = vec![0, 1, 255];
    let first = client
        .publish_bytes_with_options(
            "events",
            original.clone(),
            PublishOptions::default().with_request_id("single-id"),
        )
        .await
        .unwrap();
    assert_eq!(first.offset, 0);
    let exact = client
        .publish_bytes_with_options(
            "events",
            original.clone(),
            PublishOptions::default()
                .with_key("")
                .with_request_id("single-id"),
        )
        .await
        .unwrap();
    assert_eq!(exact.offset, first.offset);

    let key_conflict = client
        .publish_bytes_with_options(
            "events",
            original.clone(),
            PublishOptions::default()
                .with_key("different-ordering-key")
                .with_request_id("single-id"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        key_conflict,
        AttemptOutcome::Rejected(AttemptFailure::Broker(Response::Error {
            code,
            ..
        })) if code == "request_id_content_conflict"
    ));

    let payload_conflict = client
        .publish_bytes_with_options(
            "events",
            b"different payload".to_vec(),
            PublishOptions::default().with_request_id("single-id"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        payload_conflict,
        AttemptOutcome::Rejected(AttemptFailure::Broker(Response::Error {
            code,
            ..
        })) if code == "request_id_content_conflict"
    ));

    let batch = client
        .publish_batch(
            "events",
            [
                PublishBatchRecord::with_options(
                    b"batch-first".to_vec(),
                    PublishOptions::default()
                        .with_key("batch-key")
                        .with_request_id("batch-id"),
                ),
                PublishBatchRecord::with_options(
                    b"batch-first".to_vec(),
                    PublishOptions::default()
                        .with_key("batch-key")
                        .with_request_id("batch-id"),
                ),
                PublishBatchRecord::with_options(
                    b"batch-changed".to_vec(),
                    PublishOptions::default()
                        .with_key("batch-key")
                        .with_request_id("batch-id"),
                ),
                PublishBatchRecord::with_options(
                    b"independent".to_vec(),
                    PublishOptions::default().with_request_id("other-id"),
                ),
            ],
        )
        .await;
    assert!(batch.attempt.is_none());
    assert_eq!(batch.outcomes.len(), 4);
    assert_eq!(
        batch.outcomes[0],
        PublishBatchOutcome::Confirmed(PublishReceipt {
            stream: "events".to_owned(),
            offset: 1,
        })
    );
    assert_eq!(batch.outcomes[1], batch.outcomes[0]);
    assert!(matches!(
        &batch.outcomes[2],
        PublishBatchOutcome::Rejected { code, .. }
            if code == "request_id_content_conflict"
    ));
    assert_eq!(
        batch.outcomes[3],
        PublishBatchOutcome::Confirmed(PublishReceipt {
            stream: "events".to_owned(),
            offset: 2,
        })
    );

    drop(client);
    drop(server);

    let server = RunningServer::start(directory.path(), &[]);
    let mut recovered = Client::connect(server.broker_addr).await.unwrap();
    let replay = recovered
        .publish_bytes_with_options(
            "events",
            original.clone(),
            PublishOptions::default().with_request_id("single-id"),
        )
        .await
        .unwrap();
    assert_eq!(replay.offset, 0);
    let after_restart = recovered
        .publish_bytes_with_options(
            "events",
            b"different payload".to_vec(),
            PublishOptions::default().with_request_id("single-id"),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        after_restart,
        AttemptOutcome::Rejected(AttemptFailure::Broker(Response::Error {
            code,
            ..
        })) if code == "request_id_content_conflict"
    ));

    for (offset, expected) in [
        (0, original.as_slice()),
        (1, b"batch-first".as_slice()),
        (2, b"independent".as_slice()),
    ] {
        let message = recovered
            .poll_bytes("events", "verifier")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.offset, offset);
        assert_eq!(message.payload, expected);
        recovered.ack("events", "verifier", offset).await.unwrap();
    }
    assert!(
        recovered
            .poll_bytes("events", "verifier")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn typed_publish_batch_preserves_outcomes_and_request_id_replay_after_restart() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &[]);
    let mut client = Client::connect(server.broker_addr).await.unwrap();
    client.create_stream("events").await.unwrap();

    let binary_payload = vec![0, 1, 255, b'\n', b'_', 0];
    let records = vec![
        PublishBatchRecord::with_options(
            binary_payload.clone(),
            PublishOptions::default()
                .with_key("binary-key")
                .with_request_id("batch-first"),
        ),
        // Request IDs over the core's 1,024-byte record limit are rejected
        // independently, leaving the later valid batch record eligible.
        PublishBatchRecord::with_options(
            b"rejected".to_vec(),
            PublishOptions::default().with_request_id("x".repeat(1_025)),
        ),
        PublishBatchRecord::with_options(
            b"tail".to_vec(),
            PublishOptions::default()
                .with_key("tail-key")
                .with_request_id("batch-tail"),
        ),
    ];

    let first_attempt = client.publish_batch("events", records.clone()).await;
    assert!(first_attempt.attempt.is_none());
    assert_eq!(first_attempt.outcomes.len(), 3);
    assert_eq!(
        first_attempt.outcomes[0],
        PublishBatchOutcome::Confirmed(PublishReceipt {
            stream: "events".to_owned(),
            offset: 0,
        })
    );
    assert!(matches!(
        &first_attempt.outcomes[1],
        PublishBatchOutcome::Rejected { code, .. } if code == "invalid_record"
    ));
    assert_eq!(
        first_attempt.outcomes[2],
        PublishBatchOutcome::Confirmed(PublishReceipt {
            stream: "events".to_owned(),
            offset: 1,
        })
    );

    drop(client);
    drop(server);

    let server = RunningServer::start(directory.path(), &[]);
    let mut recovered = Client::connect(server.broker_addr).await.unwrap();
    let replay = recovered.publish_batch("events", records).await;
    assert!(replay.attempt.is_none());
    assert_eq!(replay.outcomes, first_attempt.outcomes);

    let first = recovered
        .poll_bytes("events", "verifier")
        .await
        .unwrap()
        .expect("the first accepted batch record should be available");
    assert_eq!(first.offset, 0);
    assert_eq!(first.key.as_deref(), Some("binary-key"));
    assert_eq!(first.payload, binary_payload);
    recovered
        .ack("events", "verifier", first.offset)
        .await
        .unwrap();

    let tail = recovered
        .poll_bytes("events", "verifier")
        .await
        .unwrap()
        .expect("the record following the rejection should be available");
    assert_eq!(tail.offset, 1);
    assert_eq!(tail.key.as_deref(), Some("tail-key"));
    assert_eq!(tail.payload, b"tail");
    recovered
        .ack("events", "verifier", tail.offset)
        .await
        .unwrap();
    assert!(
        recovered
            .poll_bytes("events", "verifier")
            .await
            .unwrap()
            .is_none(),
        "replaying the batch after restart must not append duplicates"
    );
}

#[tokio::test]
async fn typed_consume_batch_fences_scalar_ack_and_recovers_after_restart() {
    let directory = TempDir::new().unwrap();
    let limits = ConsumeBatchLimits {
        max_records: 2,
        max_bytes: 64 * 1024,
        max_wait_ms: 0,
    };
    let server = RunningServer::start(directory.path(), &[]);
    let mut client = Client::connect(server.broker_addr).await.unwrap();
    client.create_stream("events").await.unwrap();

    let binary_payload = [0, 1, 255, b'\n', b'_', 0];
    client
        .publish_bytes("events", binary_payload.to_vec())
        .await
        .unwrap();
    client.publish("events", "second").await.unwrap();

    let first = client.poll_batch("events", "worker", limits).await.unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].offset, 0);
    assert_eq!(first[0].payload, binary_payload);
    assert_eq!(first[1].offset, 1);
    assert!(first.iter().all(|message| message.delivery_token.is_some()));
    assert_eq!(
        client.poll_batch("events", "worker", limits).await.unwrap(),
        first,
        "a repeated poll must recover the same active set"
    );

    assert!(matches!(
        client.ack("events", "worker", first[0].offset).await,
        Err(AttemptOutcome::Rejected(_))
    ));
    let acknowledgement = client
        .ack_batch(
            "events",
            "worker",
            [BatchDeliveryReceipt {
                offset: first[0].offset,
                delivery_token: first[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        acknowledgement.outcomes[0].outcome,
        BatchAcknowledgementOutcome::Confirmed
    );

    drop(client);
    drop(server);

    let server = RunningServer::start(directory.path(), &[]);
    let mut recovered = Client::connect(server.broker_addr).await.unwrap();
    let remaining = recovered
        .poll_batch("events", "worker", limits)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].offset, first[1].offset);
    assert_eq!(remaining[0].payload, b"second");
    assert_eq!(remaining[0].delivery_attempt, Some(2));
    assert_ne!(
        remaining[0].delivery_token.as_deref(),
        first[1].delivery_token.as_deref(),
        "volatile leases receive fresh receipts after restart"
    );
    let recovered_ack = recovered
        .ack_batch(
            "events",
            "worker",
            [BatchDeliveryReceipt {
                offset: remaining[0].offset,
                delivery_token: remaining[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        recovered_ack.outcomes[0].outcome,
        BatchAcknowledgementOutcome::Confirmed
    );
    assert!(
        recovered
            .poll_batch("events", "worker", limits)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn consume_batch_server_timeout_does_not_create_an_assignment() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &["--request-timeout-ms", "50"]);
    let mut client = Client::connect(server.broker_addr).await.unwrap();
    client.create_stream("events").await.unwrap();
    client.publish("events", "work").await.unwrap();

    let outcome = client
        .poll_batch(
            "events",
            "worker",
            ConsumeBatchLimits {
                max_records: 2,
                max_bytes: 64 * 1024,
                max_wait_ms: 500,
            },
        )
        .await;
    assert!(matches!(
        outcome,
        Err(AttemptOutcome::Unknown(AttemptFailure::Broker(
            Response::Error { ref code, .. }
        ))) if code == "request_timeout"
    ));

    drop(client);
    let mut retry = Client::connect(server.broker_addr).await.unwrap();
    let messages = retry
        .poll_batch(
            "events",
            "worker",
            ConsumeBatchLimits {
                max_records: 2,
                max_bytes: 64 * 1024,
                max_wait_ms: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].offset, 0);
    assert_eq!(messages[0].delivery_attempt, Some(1));
    retry
        .ack_batch(
            "events",
            "worker",
            [BatchDeliveryReceipt {
                offset: messages[0].offset,
                delivery_token: messages[0].delivery_token.clone().unwrap(),
            }],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn typed_client_configures_consumer_retry_policy_and_dead_letters() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &[]);
    let mut client = Client::connect(server.broker_addr).await.unwrap();
    client.create_stream("events").await.unwrap();
    let policy = client
        .configure_consumer("events", "worker", 0, Some(2), 300)
        .await
        .unwrap();
    assert!(policy.configured);
    assert_eq!(policy.max_delivery_attempts, Some(2));
    assert_eq!(policy.retry_delay_ms, 300);
    client.publish("events", "poison").await.unwrap();
    assert_eq!(
        client
            .poll("events", "worker")
            .await
            .unwrap()
            .unwrap()
            .delivery_attempt,
        Some(1)
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(client.poll("events", "worker").await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(320)).await;
    assert_eq!(
        client
            .poll("events", "worker")
            .await
            .unwrap()
            .unwrap()
            .delivery_attempt,
        Some(2)
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(client.poll("events", "worker").await.unwrap().is_none());
    let inspected = client.inspect_consumer("events", "worker").await.unwrap();
    assert_eq!(inspected.version, policy.version);
    assert_eq!(
        client
            .poll("events.dead-letter", "inspector")
            .await
            .unwrap()
            .unwrap()
            .payload,
        "poison"
    );
}

#[tokio::test]
async fn typed_client_consumer_policy_idempotency_preserves_and_advances_versions() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &[]);
    let mut client = Client::connect(server.broker_addr).await.unwrap();
    client.create_stream("events").await.unwrap();

    let initial = client
        .configure_consumer("events", "worker", 50, Some(4), 250)
        .await
        .unwrap();
    assert!(initial.configured);
    assert_eq!(initial.version, 1);
    assert_eq!(initial.ack_timeout_ms, 50);
    assert_eq!(initial.max_delivery_attempts, Some(4));
    assert_eq!(initial.retry_delay_ms, 250);
    assert_eq!(
        client.inspect_consumer("events", "worker").await.unwrap(),
        initial
    );

    let repeated = client
        .configure_consumer("events", "worker", 50, Some(4), 250)
        .await
        .unwrap();
    assert_eq!(repeated, initial);
    assert_eq!(
        client.inspect_consumer("events", "worker").await.unwrap(),
        initial
    );

    let changed = client
        .configure_consumer("events", "worker", 75, Some(4), 500)
        .await
        .unwrap();
    assert_eq!(changed.version, repeated.version + 1);
    assert_eq!(changed.ack_timeout_ms, 75);
    assert_eq!(changed.max_delivery_attempts, Some(4));
    assert_eq!(changed.retry_delay_ms, 500);
    assert_eq!(
        client.inspect_consumer("events", "worker").await.unwrap(),
        changed
    );
}

#[tokio::test]
async fn typed_client_accepts_a_large_binary_response_within_its_byte_bound() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &["--max-request-bytes", "32768"]);
    let mut client = Client::connect_with_config(
        server.broker_addr,
        ClientConfig {
            max_response_bytes: 32 * 1024,
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();

    client.create_stream("large").await.unwrap();
    let payload: Vec<u8> = (0..16 * 1024).map(|index| (index % 251) as u8).collect();
    client
        .publish_bytes("large", payload.clone())
        .await
        .unwrap();

    let message = client
        .poll_bytes("large", "reader")
        .await
        .unwrap()
        .expect("the large binary message should be available");
    assert_eq!(message.payload, payload);
}

#[tokio::test]
async fn typed_client_application_flow_recovers_after_broker_restart() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &["--ack-timeout-ms", "50"]);
    let mut client = Client::connect(server.broker_addr).await.unwrap();

    client.create_stream("orders").await.unwrap();
    let payload = vec![0, 1, 255, b'\n', 42];
    let receipt = client
        .publish_bytes_with_options(
            "orders",
            payload.clone(),
            PublishOptions::default().with_request_id("order-1"),
        )
        .await
        .unwrap();
    let message = client
        .poll_bytes("orders", "worker")
        .await
        .unwrap()
        .expect("the application should receive its order");
    assert_eq!(message.offset, receipt.offset);
    assert_eq!(message.payload, payload);
    drop(client);
    drop(server);

    let server = RunningServer::start(directory.path(), &["--ack-timeout-ms", "50"]);
    let mut recovered = Client::connect(server.broker_addr).await.unwrap();
    let redelivered = recovered
        .poll_bytes("orders", "worker")
        .await
        .unwrap()
        .expect("an unacknowledged order should be redelivered after restart");
    assert_eq!(redelivered.offset, receipt.offset);
    assert_eq!(redelivered.payload, payload);
    assert_eq!(redelivered.delivery_attempt, Some(2));
    recovered
        .ack("orders", "worker", redelivered.offset)
        .await
        .unwrap();
    assert!(
        recovered
            .poll_bytes("orders", "worker")
            .await
            .unwrap()
            .is_none()
    );
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
    let dropped_response = proxy.finish().await.unwrap();
    assert!(matches!(
        serde_json::from_slice::<Response>(&dropped_response).unwrap(),
        Response::Published { stream, offset } if stream == "events" && offset == 0
    ));

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

#[tokio::test]
async fn typed_publish_batch_retries_after_lost_response_without_duplicates() {
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

    let binary_payload = vec![0, 1, 255, b'\n', 0];
    let records = vec![
        PublishBatchRecord::with_options(
            binary_payload.clone(),
            PublishOptions::default()
                .with_key("first-key")
                .with_request_id("batch-first-once"),
        ),
        PublishBatchRecord::with_options(
            b"second".to_vec(),
            PublishOptions::default()
                .with_key("second-key")
                .with_request_id("batch-second-once"),
        ),
    ];

    let mut client = Client::connect(proxy_address).await.unwrap();
    let lost_attempt = client.publish_batch("events", records.clone()).await;
    assert!(matches!(
        lost_attempt.attempt.as_ref(),
        Some(AttemptFailure::Client(ClientError::Eof))
    ));
    assert_eq!(lost_attempt.outcomes.len(), records.len());
    assert!(lost_attempt.outcomes.iter().all(|outcome| matches!(
        outcome,
        PublishBatchOutcome::Unknown { code, .. } if code == "client_error"
    )));

    client.reconnect(proxy_address).await.unwrap();
    let retry = client.publish_batch("events", records).await;
    assert!(retry.attempt.is_none());
    assert_eq!(
        retry.outcomes,
        vec![
            PublishBatchOutcome::Confirmed(PublishReceipt {
                stream: "events".to_owned(),
                offset: 0,
            }),
            PublishBatchOutcome::Confirmed(PublishReceipt {
                stream: "events".to_owned(),
                offset: 1,
            }),
        ]
    );
    drop(client);

    let dropped_response = proxy.finish().await.unwrap();
    let Response::PublishBatch { stream, outcomes } =
        serde_json::from_slice::<Response>(&dropped_response).unwrap()
    else {
        panic!("the broker should have accepted the first batch before its response was dropped");
    };
    assert_eq!(stream, "events");
    assert_eq!(outcomes.len(), 2);
    assert!(matches!(
        &outcomes[0],
        PublishBatchRecordResponse::Published { offset: 0 }
    ));
    assert!(matches!(
        &outcomes[1],
        PublishBatchRecordResponse::Published { offset: 1 }
    ));

    let mut verifier = Client::connect(server.broker_addr).await.unwrap();
    let first = verifier
        .poll_bytes("events", "verifier")
        .await
        .unwrap()
        .expect("the first batch record should be available exactly once");
    assert_eq!(first.offset, 0);
    assert_eq!(first.key.as_deref(), Some("first-key"));
    assert_eq!(first.payload, binary_payload);
    verifier
        .ack("events", "verifier", first.offset)
        .await
        .unwrap();

    let second = verifier
        .poll_bytes("events", "verifier")
        .await
        .unwrap()
        .expect("the second batch record should be available exactly once");
    assert_eq!(second.offset, 1);
    assert_eq!(second.key.as_deref(), Some("second-key"));
    assert_eq!(second.payload, b"second");
    verifier
        .ack("events", "verifier", second.offset)
        .await
        .unwrap();
    assert!(
        verifier
            .poll_bytes("events", "verifier")
            .await
            .unwrap()
            .is_none(),
        "retrying after response loss must not append duplicate records"
    );
}

#[tokio::test]
async fn typed_consume_batch_ack_retries_after_lost_response() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &[]);
    let mut setup = Client::connect(server.broker_addr).await.unwrap();
    setup.create_stream("events").await.unwrap();
    setup.publish("events", "work").await.unwrap();
    let delivery = setup
        .poll_batch(
            "events",
            "worker",
            ConsumeBatchLimits {
                max_records: 1,
                max_bytes: 64 * 1024,
                max_wait_ms: 0,
            },
        )
        .await
        .unwrap()
        .pop()
        .expect("the record should be assigned to the batch");
    let receipts = [BatchDeliveryReceipt {
        offset: delivery.offset,
        delivery_token: delivery.delivery_token.clone().unwrap(),
    }];
    drop(setup);

    let proxy_listener = AsyncTcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let proxy = ProxyGuard {
        handle: Some(tokio::spawn(drop_first_response_proxy(
            proxy_listener,
            server.broker_addr,
        ))),
    };
    let mut client = Client::connect(proxy_address).await.unwrap();
    assert!(matches!(
        client.ack_batch("events", "worker", receipts.clone()).await,
        Err(AttemptOutcome::Unknown(AttemptFailure::Client(
            ClientError::Eof
        )))
    ));
    client.reconnect(proxy_address).await.unwrap();
    let retry = client
        .ack_batch("events", "worker", receipts)
        .await
        .unwrap();
    assert_eq!(
        retry.outcomes[0].outcome,
        BatchAcknowledgementOutcome::AlreadyConfirmed
    );
    drop(client);

    let dropped_response = proxy.finish().await.unwrap();
    assert!(matches!(
        serde_json::from_slice::<Response>(&dropped_response).unwrap(),
        Response::AckBatch { outcomes, .. }
            if matches!(outcomes.first().map(|item| &item.outcome), Some(&runnel_protocol::AckBatchItemOutcome::Confirmed))
    ));
    let mut verifier = Client::connect(server.broker_addr).await.unwrap();
    assert!(
        verifier
            .poll_batch(
                "events",
                "worker",
                ConsumeBatchLimits {
                    max_records: 1,
                    max_bytes: 64 * 1024,
                    max_wait_ms: 0,
                },
            )
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn typed_publish_batch_response_timeout_reports_unknown_and_retries() {
    let directory = TempDir::new().unwrap();
    let server = RunningServer::start(directory.path(), &[]);
    let mut setup = Client::connect(server.broker_addr).await.unwrap();
    setup.create_stream("events").await.unwrap();
    drop(setup);

    let proxy_listener = AsyncTcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let (response_sender, response_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let proxy = ProxyGuard {
        handle: Some(tokio::spawn(withhold_response_proxy(
            proxy_listener,
            server.broker_addr,
            response_sender,
            release_receiver,
        ))),
    };

    let binary_payload = vec![0, 1, 255, b'\n', 0];
    let records = vec![
        PublishBatchRecord::with_options(
            binary_payload.clone(),
            PublishOptions::default()
                .with_key("first-key")
                .with_request_id("response-timeout-batch-first"),
        ),
        PublishBatchRecord::with_options(
            b"second".to_vec(),
            PublishOptions::default()
                .with_key("second-key")
                .with_request_id("response-timeout-batch-second"),
        ),
    ];
    let response_timeout = Duration::from_secs(2);
    let mut client = Client::connect_with_config(
        proxy_address,
        ClientConfig {
            response_timeout,
            ..ClientConfig::default()
        },
    )
    .await
    .unwrap();
    let publish_started = Instant::now();
    let mut publish = Box::pin(client.publish_batch("events", records.clone()));
    let (broker_response, response_captured_at) = tokio::select! {
        biased;
        response = response_receiver => response.expect("proxy should report the complete broker response"),
        attempt = &mut publish => panic!("typed client timed out before the proxy observed the broker's successful response: {attempt:?}"),
    };
    assert!(
        response_captured_at.duration_since(publish_started) < response_timeout,
        "proxy should capture the successful broker response before the client timeout"
    );

    let Response::PublishBatch { stream, outcomes } =
        serde_json::from_slice::<Response>(&broker_response).unwrap()
    else {
        panic!("the broker should return a publish-batch response");
    };
    assert_eq!(stream, "events");
    assert_eq!(outcomes.len(), records.len());
    assert!(matches!(
        &outcomes[0],
        PublishBatchRecordResponse::Published { offset: 0 }
    ));
    assert!(matches!(
        &outcomes[1],
        PublishBatchRecordResponse::Published { offset: 1 }
    ));

    let timed_out = publish.await;
    assert!(matches!(
        timed_out.attempt.as_ref(),
        Some(AttemptFailure::Client(ClientError::ResponseTimeout { timeout }))
            if *timeout == response_timeout
    ));
    assert_eq!(timed_out.outcomes.len(), records.len());
    assert!(timed_out.outcomes.iter().all(|outcome| matches!(
        outcome,
        PublishBatchOutcome::Unknown { code, .. } if code == "client_error"
    )));

    release_sender
        .send(())
        .expect("proxy should still be withholding the captured response");
    assert_eq!(proxy.finish().await.unwrap(), broker_response);

    client.reconnect(server.broker_addr).await.unwrap();
    let retry = client.publish_batch("events", records).await;
    assert!(retry.attempt.is_none());
    assert_eq!(
        retry.outcomes,
        vec![
            PublishBatchOutcome::Confirmed(PublishReceipt {
                stream: "events".to_owned(),
                offset: 0,
            }),
            PublishBatchOutcome::Confirmed(PublishReceipt {
                stream: "events".to_owned(),
                offset: 1,
            }),
        ]
    );

    let mut verifier = Client::connect(server.broker_addr).await.unwrap();
    let first = verifier
        .poll_bytes("events", "verifier")
        .await
        .unwrap()
        .expect("the first accepted batch record should be available");
    assert_eq!(first.offset, 0);
    assert_eq!(first.key.as_deref(), Some("first-key"));
    assert_eq!(first.payload, binary_payload);
    verifier
        .ack("events", "verifier", first.offset)
        .await
        .unwrap();

    let second = verifier
        .poll_bytes("events", "verifier")
        .await
        .unwrap()
        .expect("the second accepted batch record should be available");
    assert_eq!(second.offset, 1);
    assert_eq!(second.key.as_deref(), Some("second-key"));
    assert_eq!(second.payload, b"second");
    verifier
        .ack("events", "verifier", second.offset)
        .await
        .unwrap();
    assert!(
        verifier
            .poll_bytes("events", "verifier")
            .await
            .unwrap()
            .is_none(),
        "retrying a timed-out batch must not append duplicate records"
    );
}

struct ProxyGuard {
    handle: Option<tokio::task::JoinHandle<Vec<u8>>>,
}

impl ProxyGuard {
    async fn finish(mut self) -> Result<Vec<u8>, tokio::task::JoinError> {
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

async fn drop_first_response_proxy(listener: AsyncTcpListener, broker_addr: SocketAddr) -> Vec<u8> {
    let mut dropped_response = None;
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
            dropped_response = Some(response);
            continue;
        }
        client_writer.write_all(&response).await.unwrap();
    }
    dropped_response.expect("the proxy should have dropped its first broker response")
}

async fn withhold_response_proxy(
    listener: AsyncTcpListener,
    broker_addr: SocketAddr,
    response_sender: oneshot::Sender<(Vec<u8>, Instant)>,
    release_receiver: oneshot::Receiver<()>,
) -> Vec<u8> {
    let (client, _) = listener.accept().await.unwrap();
    let (client_reader, client_writer) = client.into_split();
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
    assert!(
        response.ends_with(b"\n"),
        "broker response should be complete"
    );
    response_sender
        .send((response.clone(), Instant::now()))
        .expect("test should inspect the broker response before client timeout");

    release_receiver
        .await
        .expect("test should release the withheld client response after timeout");
    drop(client_reader);
    drop(client_writer);
    response
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
