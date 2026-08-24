use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use clap::{Parser, ValueEnum};
use runnel_core::{Broker, BrokerConfig};
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{AckResult, BrokerError, Engine, PollResult};
use runnel_protocol::{Request, Response};
use runnel_raft::{GroupManager, NodeId, PersistentEngine, SnapshotMetricsSnapshot};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "runnel", about = "A lightweight durable message broker")]
struct Args {
    #[arg(long, value_enum, default_value_t = EngineKind::Local)]
    engine: EngineKind,
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:4222")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:8080")]
    http_listen: SocketAddr,
    #[arg(long, default_value_t = 30_000)]
    ack_timeout_ms: u64,
    #[arg(long)]
    max_delivery_attempts: Option<u32>,
    #[arg(long)]
    node_id: Option<NodeId>,
    #[arg(long)]
    peer_listen: Option<SocketAddr>,
    #[arg(long = "cluster-node")]
    cluster_nodes: Vec<String>,
    #[arg(long)]
    bootstrap: bool,
    #[arg(long, default_value = "runnel")]
    cluster_name: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum EngineKind {
    Local,
    Raft,
}

#[derive(Clone)]
struct HttpState {
    engine: Arc<dyn Engine>,
    cluster: Option<Arc<GroupManager>>,
    metrics: Arc<ServerMetrics>,
}

#[derive(Default)]
struct ServerMetrics {
    deliveries: AtomicU64,
}

#[derive(serde::Serialize)]
struct HealthBody {
    status: &'static str,
    streams: usize,
    storage_bytes: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "runnel=info".into()),
        )
        .init();

    let args = Args::parse();
    let tcp_listener = TcpListener::bind(args.listen).await?;
    let http_listener = TcpListener::bind(args.http_listen).await?;

    let (engine, peer, cluster) = match args.engine {
        EngineKind::Local => {
            let broker = Broker::open(
                &args.data_dir,
                BrokerConfig {
                    ack_timeout: Duration::from_millis(args.ack_timeout_ms),
                    max_delivery_attempts: args.max_delivery_attempts,
                },
            )?;
            (Arc::new(broker) as Arc<dyn Engine>, None, None)
        }
        EngineKind::Raft => {
            let node_id = args
                .node_id
                .ok_or("--node-id is required for --engine raft")?;
            let peer_listen = args
                .peer_listen
                .ok_or("--peer-listen is required for --engine raft")?;
            let peers = parse_cluster_nodes(&args.cluster_nodes)?;
            if !peers.contains_key(&node_id) {
                return Err(format!("cluster nodes do not contain node {node_id}").into());
            }
            let peer_listener = TcpListener::bind(peer_listen).await?;
            let raft_engine = PersistentEngine::open_with_config(
                node_id,
                args.cluster_name.clone(),
                &args.data_dir,
                peers,
                args.bootstrap,
                Duration::from_millis(args.ack_timeout_ms),
                args.max_delivery_attempts,
            )
            .await?;
            let manager = raft_engine.manager();
            (
                Arc::new(raft_engine) as Arc<dyn Engine>,
                Some((peer_listener, Arc::clone(&manager))),
                Some(manager),
            )
        }
    };

    info!(
        broker = %args.listen,
        http = %args.http_listen,
        data_dir = ?args.data_dir,
        engine = ?args.engine,
        "runnel started"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let tcp_engine = Arc::clone(&engine);
    let server_metrics = Arc::new(ServerMetrics::default());
    let tcp_metrics = Arc::clone(&server_metrics);
    let tcp_task = tokio::spawn(run_tcp(
        tcp_listener,
        tcp_engine,
        tcp_metrics,
        shutdown_rx.clone(),
    ));
    if let Some((peer_listener, group)) = peer {
        let peer_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(error) = runnel_raft::serve_peer(peer_listener, group, peer_shutdown).await {
                error!(%error, "raft peer listener stopped");
            }
        });
    }

    let app = Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/metrics", get(metrics))
        .with_state(HttpState {
            engine,
            cluster,
            metrics: server_metrics,
        });
    let http_task = tokio::spawn(async move {
        axum::serve(http_listener, app)
            .with_graceful_shutdown(wait_for_shutdown(shutdown_rx))
            .await
    });

    tokio::select! {
        result = tcp_task => {
            result??;
        }
        result = http_task => {
            result??;
        }
        result = shutdown_signal() => {
            result?;
            info!("shutdown signal received");
        }
    }
    let _ = shutdown_tx.send(true);
    Ok(())
}

fn parse_cluster_nodes(
    values: &[String],
) -> Result<BTreeMap<NodeId, String>, Box<dyn std::error::Error>> {
    if values.is_empty() {
        return Err("at least one --cluster-node id=address value is required".into());
    }

    let mut nodes = BTreeMap::new();
    for value in values {
        let (id, address) = value
            .split_once('=')
            .ok_or_else(|| format!("invalid cluster node '{value}'; expected id=address"))?;
        let id = id.parse::<NodeId>()?;
        if address.is_empty() {
            return Err(format!("cluster node {id} has an empty address").into());
        }
        if nodes.insert(id, address.to_owned()).is_some() {
            return Err(format!("duplicate cluster node id {id}").into());
        }
    }
    Ok(nodes)
}

async fn run_tcp(
    listener: TcpListener,
    engine: Arc<dyn Engine>,
    metrics: Arc<ServerMetrics>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            result = listener.accept() => {
                let (stream, peer) = result?;
                let engine = Arc::clone(&engine);
                let metrics = Arc::clone(&metrics);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, engine, metrics).await {
                        warn!(%peer, %error, "connection closed with error");
                    }
                });
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    engine: Arc<dyn Engine>,
    metrics: Arc<ServerMetrics>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("server.protocol_round_trip");
        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request) => handle_request(engine.as_ref(), request, &metrics).await,
            Err(error) => Response::Error {
                code: "invalid_request".to_owned(),
                message: error.to_string(),
            },
        };
        let mut encoded = serde_json::to_vec(&response)?;
        encoded.push(b'\n');
        writer.write_all(&encoded).await?;
    }
    Ok(())
}

async fn handle_request(
    engine: &dyn Engine,
    request: Request,
    metrics: &ServerMetrics,
) -> Response {
    #[cfg(feature = "instrumentation")]
    let _stage_timer = StageTimer::new("server.engine_request");
    let result = match request {
        Request::CreateStream { stream } => engine
            .create_stream(&stream)
            .await
            .map(|created| Response::StreamCreated { stream, created }),
        Request::Publish {
            stream,
            key,
            payload,
            request_id,
        } => engine
            .publish(&stream, key, payload.into_bytes(), request_id)
            .await
            .map(|offset| Response::Published { stream, offset }),
        Request::Poll { stream, consumer } => {
            let result = engine.poll(&stream, &consumer).await;
            record_delivery(metrics, &result);
            result.map(|result| match result {
                PollResult::Message(message) => Response::Message {
                    stream: message.stream,
                    consumer,
                    member: None,
                    offset: message.offset,
                    key: message.key,
                    payload: String::from_utf8_lossy(&message.payload).into_owned(),
                    published_at_ms: message.published_at_ms,
                    delivery_token: None,
                    delivery_attempt: message.delivery_attempt,
                },
                PollResult::Empty => Response::Empty { stream, consumer },
            })
        }
        Request::PollGroup {
            stream,
            consumer,
            member,
        } => {
            let result = engine.poll_group(&stream, &consumer, &member).await;
            record_delivery(metrics, &result);
            result.map(|result| match result {
                PollResult::Message(message) => Response::Message {
                    stream: message.stream,
                    consumer,
                    member: Some(member),
                    offset: message.offset,
                    key: message.key,
                    payload: String::from_utf8_lossy(&message.payload).into_owned(),
                    published_at_ms: message.published_at_ms,
                    delivery_token: message.delivery_token,
                    delivery_attempt: message.delivery_attempt,
                },
                PollResult::Empty => Response::Empty { stream, consumer },
            })
        }
        Request::Ack {
            stream,
            consumer,
            offset,
        } => engine
            .ack(&stream, &consumer, offset)
            .await
            .map(|result| Response::Acknowledged {
                stream,
                consumer,
                offset,
                already_acknowledged: result == AckResult::AlreadyAcknowledged,
            }),
        Request::AckGroup {
            stream,
            consumer,
            member,
            offset,
            delivery_token,
        } => engine
            .ack_group(&stream, &consumer, &member, offset, &delivery_token)
            .await
            .map(|result| Response::Acknowledged {
                stream,
                consumer,
                offset,
                already_acknowledged: result == AckResult::AlreadyAcknowledged,
            }),
        Request::Health => engine.health().await.map(|health| Response::Health {
            status: "ok".to_owned(),
            streams: health.streams,
            storage_bytes: health.storage_bytes,
        }),
    };

    result.unwrap_or_else(|error| error_response(&error))
}

fn record_delivery(metrics: &ServerMetrics, result: &Result<PollResult, BrokerError>) {
    if matches!(result, Ok(PollResult::Message(_))) {
        metrics.deliveries.fetch_add(1, Ordering::Relaxed);
    }
}

fn error_response(error: &BrokerError) -> Response {
    let code = match error {
        BrokerError::InvalidName { .. } => "invalid_name",
        BrokerError::StreamNotFound(_) => "stream_not_found",
        BrokerError::StreamNotReady(_) => "stream_not_ready",
        BrokerError::AckNotInFlight { .. } => "ack_not_in_flight",
        BrokerError::StaleDelivery { .. } => "stale_delivery",
        BrokerError::OutOfOrderAck { .. } => "out_of_order_ack",
        BrokerError::CorruptRecord(_) => "corrupt_record",
        BrokerError::Io(_) => "storage_error",
        BrokerError::State(_) => "consumer_state_error",
        BrokerError::LockPoisoned => "internal_error",
        BrokerError::Configuration(_) => "invalid_configuration",
        BrokerError::NotLeader { .. } => "cluster_error",
        BrokerError::Cluster(_) => "cluster_error",
    };
    Response::Error {
        code: code.to_owned(),
        message: error.to_string(),
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|value| *value).await;
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

async fn liveness() -> StatusCode {
    StatusCode::OK
}

async fn readiness(State(state): State<HttpState>) -> (StatusCode, Json<HealthBody>) {
    match state.engine.health().await {
        Ok(health) => (
            StatusCode::OK,
            Json(HealthBody {
                status: "ready",
                streams: health.streams,
                storage_bytes: health.storage_bytes,
            }),
        ),
        Err(error) => {
            error!(%error, "readiness check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(HealthBody {
                    status: "not_ready",
                    streams: 0,
                    storage_bytes: 0,
                }),
            )
        }
    }
}

async fn metrics(State(state): State<HttpState>) -> (StatusCode, String) {
    match state.engine.health().await {
        Ok(health) => {
            let snapshot_metrics = match &state.cluster {
                Some(cluster) => cluster.snapshot_metrics().await,
                None => SnapshotMetricsSnapshot::default(),
            };
            (
                StatusCode::OK,
                format_metrics(
                    health.streams,
                    health.storage_bytes,
                    health.redeliveries,
                    health.dead_letters,
                    state.metrics.deliveries.load(Ordering::Relaxed),
                    snapshot_metrics,
                ),
            )
        }
        Err(error) => {
            error!(%error, "metrics check failed");
            (StatusCode::SERVICE_UNAVAILABLE, String::new())
        }
    }
}

fn format_metrics(
    streams: usize,
    storage_bytes: u64,
    redeliveries: u64,
    dead_letters: u64,
    deliveries: u64,
    snapshot_metrics: SnapshotMetricsSnapshot,
) -> String {
    format!(
        "# TYPE runnel_streams gauge\nrunnel_streams {streams}\n# TYPE runnel_storage_bytes gauge\nrunnel_storage_bytes {storage_bytes}\n# TYPE runnel_redeliveries_total counter\nrunnel_redeliveries_total {redeliveries}\n# TYPE runnel_dead_letters_total counter\nrunnel_dead_letters_total {dead_letters}\n# HELP runnel_deliveries_total Number of messages returned by successful poll operations.\n# TYPE runnel_deliveries_total counter\nrunnel_deliveries_total {deliveries}\n# TYPE runnel_snapshot_builds_started_total counter\nrunnel_snapshot_builds_started_total {}\n# TYPE runnel_snapshot_builds_completed_total counter\nrunnel_snapshot_builds_completed_total {}\n# TYPE runnel_snapshot_build_failures_total counter\nrunnel_snapshot_build_failures_total {}\n# TYPE runnel_snapshot_installs_started_total counter\nrunnel_snapshot_installs_started_total {}\n# TYPE runnel_snapshot_installs_completed_total counter\nrunnel_snapshot_installs_completed_total {}\n# TYPE runnel_snapshot_install_failures_total counter\nrunnel_snapshot_install_failures_total {}\n# TYPE runnel_snapshot_install_bytes_total counter\nrunnel_snapshot_install_bytes_total {}\n# TYPE runnel_snapshot_installs_in_progress gauge\nrunnel_snapshot_installs_in_progress {}\n# TYPE runnel_snapshot_transfer_chunks_received_total counter\nrunnel_snapshot_transfer_chunks_received_total {}\n# TYPE runnel_snapshot_transfer_final_chunks_received_total counter\nrunnel_snapshot_transfer_final_chunks_received_total {}\n# TYPE runnel_snapshot_transfer_bytes_received_total counter\nrunnel_snapshot_transfer_bytes_received_total {}\n",
        snapshot_metrics.builds_started,
        snapshot_metrics.builds_completed,
        snapshot_metrics.build_failures,
        snapshot_metrics.installs_started,
        snapshot_metrics.installs_completed,
        snapshot_metrics.install_failures,
        snapshot_metrics.install_bytes,
        snapshot_metrics.installs_in_progress,
        snapshot_metrics.transfer_chunks,
        snapshot_metrics.transfer_final_chunks,
        snapshot_metrics.transfer_bytes,
    )
}
