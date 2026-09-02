use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use clap::{Parser, ValueEnum};
use runnel_core::{Broker, BrokerConfig};
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{AckResult, BrokerError, Engine, PollResult, PublishRecord, ReplayMessage};
use runnel_protocol::{
    BinaryPayload, MAX_PUBLISH_BATCH_BYTES, MAX_PUBLISH_BATCH_RECORDS, PublishBatchRecordResponse,
    Request, Response,
};
use runnel_raft::{GroupManager, NodeId, PersistentEngine, SnapshotMetricsSnapshot};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_MAX_CONNECTIONS: usize = 1_024;
const DEFAULT_MAX_REQUEST_BYTES: usize = 1_048_576;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 256;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const MAX_CONFIGURED_REQUEST_BYTES: usize = MAX_PUBLISH_BATCH_BYTES;
const CONNECTION_REJECTION_WRITE_TIMEOUT: Duration = Duration::from_millis(10);

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
    #[arg(
        long,
        visible_alias = "max-client-connections",
        default_value_t = DEFAULT_MAX_CONNECTIONS
    )]
    max_connections: usize,
    #[arg(
        long = "max-request-bytes",
        visible_alias = "max-frame-bytes",
        default_value_t = DEFAULT_MAX_REQUEST_BYTES
    )]
    max_request_bytes: usize,
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_IN_FLIGHT_REQUESTS
    )]
    max_in_flight_requests: usize,
    #[arg(
        long,
        default_value_t = DEFAULT_REQUEST_TIMEOUT_MS
    )]
    request_timeout_ms: u64,
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
    admission: ProtocolAdmission,
    shutting_down: Arc<AtomicBool>,
}

struct ServerMetrics {
    active_connections: AtomicU64,
    connections_accepted: AtomicU64,
    connections_rejected: AtomicU64,
    connections_closed: AtomicU64,
    connection_errors: AtomicU64,
    active_requests: AtomicU64,
    requests_rejected: AtomicU64,
    request_size_rejections: AtomicU64,
    request_saturation_rejections: AtomicU64,
    request_timeouts: AtomicU64,
    response_write_timeouts: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    requests: [AtomicU64; REQUEST_OPERATION_COUNT],
    request_failures: [AtomicU64; REQUEST_OPERATION_COUNT],
    request_durations: [RequestDuration; REQUEST_OPERATION_COUNT],
    stream_creations: AtomicU64,
    publishes: AtomicU64,
    published_bytes: AtomicU64,
    deliveries: AtomicU64,
    delivered_bytes: AtomicU64,
    acknowledgements: AtomicU64,
    metrics_scrapes: AtomicU64,
    metrics_scrape_failures: AtomicU64,
    health_check_failures: AtomicU64,
}

#[derive(Clone, Copy)]
struct ProtocolAdmission {
    max_connections: usize,
    max_request_bytes: usize,
    max_in_flight_requests: usize,
    request_timeout: Duration,
}

const REQUEST_OPERATION_COUNT: usize = 9;
const LATENCY_BUCKET_MICROS: [u64; 6] = [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
const LATENCY_BUCKET_LABELS: [&str; LATENCY_BUCKET_MICROS.len()] =
    ["0.0001", "0.001", "0.01", "0.1", "1", "10"];

#[derive(Clone, Copy)]
enum RequestOperation {
    CreateStream,
    Publish,
    Poll,
    Replay,
    PollGroup,
    Ack,
    AckGroup,
    Health,
    InvalidRequest,
}

impl RequestOperation {
    const ALL: [Self; REQUEST_OPERATION_COUNT] = [
        Self::CreateStream,
        Self::Publish,
        Self::Poll,
        Self::Replay,
        Self::PollGroup,
        Self::Ack,
        Self::AckGroup,
        Self::Health,
        Self::InvalidRequest,
    ];

    const fn index(self) -> usize {
        match self {
            Self::CreateStream => 0,
            Self::Publish => 1,
            Self::Poll => 2,
            Self::Replay => 3,
            Self::PollGroup => 4,
            Self::Ack => 5,
            Self::AckGroup => 6,
            Self::Health => 7,
            Self::InvalidRequest => 8,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::CreateStream => "create_stream",
            Self::Publish => "publish",
            Self::Poll => "poll",
            Self::Replay => "replay",
            Self::PollGroup => "poll_group",
            Self::Ack => "ack",
            Self::AckGroup => "ack_group",
            Self::Health => "health",
            Self::InvalidRequest => "invalid_request",
        }
    }

    fn from_request(request: &Request) -> Self {
        match request {
            Request::CreateStream { .. } => Self::CreateStream,
            Request::Publish { .. } => Self::Publish,
            Request::PublishBytes { .. } => Self::Publish,
            Request::PublishBatch { .. } => Self::Publish,
            Request::Poll { .. } => Self::Poll,
            Request::Replay { .. } => Self::Replay,
            Request::PollGroup { .. } => Self::PollGroup,
            Request::Ack { .. } => Self::Ack,
            Request::AckGroup { .. } => Self::AckGroup,
            Request::Health => Self::Health,
        }
    }
}

struct RequestDuration {
    buckets: [AtomicU64; LATENCY_BUCKET_MICROS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Default for RequestDuration {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            active_connections: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            active_requests: AtomicU64::new(0),
            requests_rejected: AtomicU64::new(0),
            request_size_rejections: AtomicU64::new(0),
            request_saturation_rejections: AtomicU64::new(0),
            request_timeouts: AtomicU64::new(0),
            response_write_timeouts: AtomicU64::new(0),
            request_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            request_failures: std::array::from_fn(|_| AtomicU64::new(0)),
            request_durations: std::array::from_fn(|_| RequestDuration::default()),
            stream_creations: AtomicU64::new(0),
            publishes: AtomicU64::new(0),
            published_bytes: AtomicU64::new(0),
            deliveries: AtomicU64::new(0),
            delivered_bytes: AtomicU64::new(0),
            acknowledgements: AtomicU64::new(0),
            metrics_scrapes: AtomicU64::new(0),
            metrics_scrape_failures: AtomicU64::new(0),
            health_check_failures: AtomicU64::new(0),
        }
    }
}

impl ServerMetrics {
    fn record_request(&self, operation: RequestOperation, elapsed: Duration, failed: bool) {
        let index = operation.index();
        self.requests[index].fetch_add(1, Ordering::Relaxed);
        if failed {
            self.request_failures[index].fetch_add(1, Ordering::Relaxed);
        }

        let duration = &self.request_durations[index];
        let elapsed_micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        duration.count.fetch_add(1, Ordering::Relaxed);
        duration
            .sum_micros
            .fetch_add(elapsed_micros, Ordering::Relaxed);
        if let Some(bucket) = LATENCY_BUCKET_MICROS
            .iter()
            .position(|limit| elapsed_micros <= *limit)
        {
            duration.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct ActiveConnection(Arc<ServerMetrics>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.0.connections_closed.fetch_add(1, Ordering::Relaxed);
    }
}

struct ActiveRequest(Arc<ServerMetrics>);

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.active_requests.fetch_sub(1, Ordering::Relaxed);
    }
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
    validate_admission_config(&args)?;
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
    let shutting_down = Arc::new(AtomicBool::new(false));
    let tcp_engine = Arc::clone(&engine);
    let server_metrics = Arc::new(ServerMetrics::default());
    let tcp_metrics = Arc::clone(&server_metrics);
    let protocol_admission = ProtocolAdmission {
        max_connections: args.max_connections,
        max_request_bytes: args.max_request_bytes,
        max_in_flight_requests: args.max_in_flight_requests,
        request_timeout: Duration::from_millis(args.request_timeout_ms),
    };
    let connection_slots = Arc::new(Semaphore::new(protocol_admission.max_connections));
    let request_slots = Arc::new(Semaphore::new(protocol_admission.max_in_flight_requests));
    let mut tcp_task = tokio::spawn(run_tcp(
        tcp_listener,
        tcp_engine,
        tcp_metrics,
        connection_slots,
        request_slots,
        protocol_admission,
        shutdown_rx.clone(),
    ));
    let mut peer_task = if let Some((peer_listener, group)) = peer {
        let peer_shutdown = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            if let Err(error) = runnel_raft::serve_peer(peer_listener, group, peer_shutdown).await {
                error!(%error, "raft peer listener stopped");
            }
        }))
    } else {
        None
    };

    let http_shutting_down = Arc::clone(&shutting_down);
    let app = Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/metrics", get(metrics))
        .with_state(HttpState {
            engine,
            cluster,
            metrics: server_metrics,
            admission: protocol_admission,
            shutting_down: http_shutting_down,
        });
    let mut http_task = tokio::spawn(async move {
        axum::serve(http_listener, app)
            .with_graceful_shutdown(wait_for_shutdown(shutdown_rx))
            .await
    });

    let shutdown_reason = tokio::select! {
        result = &mut tcp_task => {
            result??;
            "broker listener stopped"
        }
        result = &mut http_task => {
            result??;
            "http listener stopped"
        }
        result = shutdown_signal() => {
            result?;
            "shutdown signal received"
        }
    };

    shutting_down.store(true, Ordering::Release);
    let _ = shutdown_tx.send(true);
    info!(reason = shutdown_reason, "runnel shutting down");

    let drain = async {
        let _ = (&mut tcp_task).await;
        let _ = (&mut http_task).await;
        if let Some(peer_task) = peer_task.as_mut() {
            let _ = peer_task.await;
        }
    };
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, drain).await.is_err() {
        warn!(?SHUTDOWN_TIMEOUT, "runnel shutdown drain timed out");
        tcp_task.abort();
        http_task.abort();
        if let Some(peer_task) = peer_task {
            peer_task.abort();
        }
    }
    Ok(())
}

fn validate_admission_config(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    if args.max_connections == 0 {
        return Err("--max-connections must be greater than zero".into());
    }
    if args.max_request_bytes == 0 || args.max_request_bytes > MAX_CONFIGURED_REQUEST_BYTES {
        return Err(format!(
            "--max-request-bytes must be between 1 and {MAX_CONFIGURED_REQUEST_BYTES}"
        )
        .into());
    }
    if args.max_in_flight_requests == 0 {
        return Err("--max-in-flight-requests must be greater than zero".into());
    }
    if args.request_timeout_ms == 0 {
        return Err("--request-timeout-ms must be greater than zero".into());
    }
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
    connection_slots: Arc<Semaphore>,
    request_slots: Arc<Semaphore>,
    protocol_admission: ProtocolAdmission,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    while let Some(result) = connections.join_next().await {
                        if let Err(error) = result {
                            warn!(%error, "broker connection task stopped during shutdown");
                        }
                    }
                    return Ok(());
                }
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    warn!(%error, "broker connection task stopped");
                }
            }
            result = listener.accept() => {
                let (stream, peer) = result?;
                if *shutdown.borrow() {
                    continue;
                }
                metrics
                    .connections_accepted
                    .fetch_add(1, Ordering::Relaxed);
                let connection_permit = match Arc::clone(&connection_slots).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        metrics
                            .connections_rejected
                            .fetch_add(1, Ordering::Relaxed);
                        metrics
                            .connections_closed
                            .fetch_add(1, Ordering::Relaxed);
                        reject_connection(stream).await;
                        warn!(%peer, "broker connection rejected at connection limit");
                        continue;
                    }
                };
                let engine = Arc::clone(&engine);
                let connection_metrics = Arc::clone(&metrics);
                let connection_request_slots = Arc::clone(&request_slots);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    if let Err(error) = handle_connection(
                        stream,
                        engine,
                        Arc::clone(&connection_metrics),
                        connection_permit,
                        connection_request_slots,
                        protocol_admission,
                        connection_shutdown,
                    )
                    .await
                    {
                        connection_metrics
                            .connection_errors
                            .fetch_add(1, Ordering::Relaxed);
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
    _connection_permit: OwnedSemaphorePermit,
    request_slots: Arc<Semaphore>,
    protocol_admission: ProtocolAdmission,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
    let _active_connection = ActiveConnection(Arc::clone(&metrics));
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut served_request = false;
    loop {
        // Waiting for the first byte is intentionally not timed. An idle
        // persistent connection is valid; once a request starts, its frame
        // must complete within the request deadline. Every idle read still
        // observes shutdown so an already-served persistent connection does
        // not extend the graceful-drain window.
        let has_data = match wait_for_request_data(&mut reader, &mut shutdown).await? {
            Some(has_data) => has_data,
            None => return Ok(()),
        };
        if !has_data {
            return Ok(());
        }

        let started = Instant::now();
        let frame_result = if served_request {
            Some(
                tokio::time::timeout(
                    protocol_admission.request_timeout,
                    read_frame(&mut reader, protocol_admission.max_request_bytes),
                )
                .await,
            )
        } else {
            tokio::select! {
                result = tokio::time::timeout(
                    protocol_admission.request_timeout,
                    read_frame(&mut reader, protocol_admission.max_request_bytes),
                ) => Some(result),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    None
                }
            }
        };
        let Some(frame_result) = frame_result else {
            continue;
        };
        let frame = match frame_result {
            Ok(result) => result?,
            Err(_) => {
                metrics.request_timeouts.fetch_add(1, Ordering::Relaxed);
                metrics.record_request(RequestOperation::InvalidRequest, started.elapsed(), true);
                send_response(
                    &mut writer,
                    &timeout_response(),
                    protocol_admission.request_timeout,
                    &metrics,
                )
                .await?;
                return Ok(());
            }
        };

        match frame {
            Frame::End => return Ok(()),
            Frame::TooLarge { bytes } => {
                metrics
                    .request_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                metrics.requests_rejected.fetch_add(1, Ordering::Relaxed);
                metrics
                    .request_size_rejections
                    .fetch_add(1, Ordering::Relaxed);
                metrics.record_request(RequestOperation::InvalidRequest, started.elapsed(), true);
                let response = request_size_response(protocol_admission.max_request_bytes);
                send_response(
                    &mut writer,
                    &response,
                    response_write_timeout(started, protocol_admission.request_timeout, &response),
                    &metrics,
                )
                .await?;
                return Ok(());
            }
            Frame::Unterminated { bytes } => {
                metrics
                    .request_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                metrics.record_request(RequestOperation::InvalidRequest, started.elapsed(), true);
                let response = invalid_request_response("request frame must end with a newline");
                send_response(
                    &mut writer,
                    &response,
                    response_write_timeout(started, protocol_admission.request_timeout, &response),
                    &metrics,
                )
                .await?;
                return Ok(());
            }
            Frame::Complete { bytes } => {
                metrics
                    .request_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                let line = request_line(&bytes);
                match serde_json::from_slice::<Request>(line) {
                    Ok(request) => {
                        let operation = RequestOperation::from_request(&request);
                        let request_permit = match Arc::clone(&request_slots).try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                metrics.requests_rejected.fetch_add(1, Ordering::Relaxed);
                                metrics
                                    .request_saturation_rejections
                                    .fetch_add(1, Ordering::Relaxed);
                                let response = saturated_response();
                                metrics.record_request(operation, started.elapsed(), true);
                                send_response(
                                    &mut writer,
                                    &response,
                                    response_write_timeout(
                                        started,
                                        protocol_admission.request_timeout,
                                        &response,
                                    ),
                                    &metrics,
                                )
                                .await?;
                                continue;
                            }
                        };
                        // Keep response serialization and socket writes inside the in-flight
                        // bound so slow response writers cannot consume unbounded request work.
                        let _request_permit = request_permit;
                        metrics.active_requests.fetch_add(1, Ordering::Relaxed);
                        let _active_request = ActiveRequest(Arc::clone(&metrics));
                        #[cfg(feature = "instrumentation")]
                        let _stage_timer = StageTimer::new("server.protocol_round_trip");
                        let response = match tokio::time::timeout(
                            remaining_timeout(started, protocol_admission.request_timeout),
                            handle_request(engine.as_ref(), request, &metrics),
                        )
                        .await
                        {
                            Ok(response) => response,
                            Err(_) => {
                                metrics.request_timeouts.fetch_add(1, Ordering::Relaxed);
                                timeout_response()
                            }
                        };
                        let failed = matches!(&response, Response::Error { .. });
                        metrics.record_request(operation, started.elapsed(), failed);
                        send_response(
                            &mut writer,
                            &response,
                            response_write_timeout(
                                started,
                                protocol_admission.request_timeout,
                                &response,
                            ),
                            &metrics,
                        )
                        .await?;
                        served_request = true;
                    }
                    Err(error) => {
                        if std::str::from_utf8(line).is_err() {
                            metrics.record_request(
                                RequestOperation::InvalidRequest,
                                started.elapsed(),
                                true,
                            );
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "request frame was not valid UTF-8",
                            )
                            .into());
                        }
                        let response = invalid_request_response(&error.to_string());
                        metrics.record_request(
                            RequestOperation::InvalidRequest,
                            started.elapsed(),
                            true,
                        );
                        send_response(
                            &mut writer,
                            &response,
                            response_write_timeout(
                                started,
                                protocol_admission.request_timeout,
                                &response,
                            ),
                            &metrics,
                        )
                        .await?;
                        served_request = true;
                    }
                }
            }
        }
    }
}

async fn wait_for_request_data<R>(
    reader: &mut BufReader<R>,
    shutdown: &mut watch::Receiver<bool>,
) -> std::io::Result<Option<bool>>
where
    R: AsyncRead + Unpin,
{
    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }
        tokio::select! {
            result = reader.fill_buf() => return Ok(Some(!result?.is_empty())),
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(None);
                }
            }
        }
    }
}

enum Frame {
    End,
    Complete { bytes: Vec<u8> },
    Unterminated { bytes: Vec<u8> },
    TooLarge { bytes: Vec<u8> },
}

async fn read_frame<R>(
    reader: &mut BufReader<R>,
    max_request_bytes: usize,
) -> std::io::Result<Frame>
where
    R: AsyncRead + Unpin,
{
    let max_frame_bytes = max_request_bytes
        .checked_add(1)
        .ok_or_else(|| std::io::Error::other("maximum request size is too large"))?;
    let mut bytes = Vec::with_capacity(max_frame_bytes.min(8 * 1024));

    loop {
        let buffered = reader.fill_buf().await?;
        if buffered.is_empty() {
            return Ok(if bytes.is_empty() {
                Frame::End
            } else {
                Frame::Unterminated { bytes }
            });
        }

        let newline = buffered.iter().position(|byte| *byte == b'\n');
        let bytes_to_consume = newline.map_or(buffered.len(), |index| index + 1);
        if bytes.len().saturating_add(bytes_to_consume) > max_frame_bytes {
            let remaining = max_frame_bytes - bytes.len();
            bytes.extend_from_slice(&buffered[..remaining]);
            reader.consume(remaining);
            return Ok(Frame::TooLarge { bytes });
        }

        bytes.extend_from_slice(&buffered[..bytes_to_consume]);
        reader.consume(bytes_to_consume);
        if newline.is_some() {
            return Ok(Frame::Complete { bytes });
        }
        if bytes.len() == max_frame_bytes {
            return Ok(Frame::TooLarge { bytes });
        }
    }
}

fn request_line(bytes: &[u8]) -> &[u8] {
    let line = &bytes[..bytes.len() - 1];
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn remaining_timeout(started: Instant, timeout: Duration) -> Duration {
    timeout.saturating_sub(started.elapsed())
}

fn response_write_timeout(
    started: Instant,
    request_timeout: Duration,
    response: &Response,
) -> Duration {
    if matches!(response, Response::Error { .. }) {
        return request_timeout;
    }
    remaining_timeout(started, request_timeout)
}

async fn send_response<W>(
    writer: &mut W,
    response: &Response,
    timeout: Duration,
    metrics: &ServerMetrics,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut encoded = serde_json::to_vec(response).map_err(|error| {
        std::io::Error::other(format!("response serialization failed: {error}"))
    })?;
    encoded.push(b'\n');
    match tokio::time::timeout(timeout, writer.write_all(&encoded)).await {
        Ok(result) => result?,
        Err(_) => {
            metrics
                .response_write_timeouts
                .fetch_add(1, Ordering::Relaxed);
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "response write timed out",
            ));
        }
    }
    metrics
        .response_bytes
        .fetch_add(encoded.len() as u64, Ordering::Relaxed);
    Ok(())
}

async fn reject_connection(stream: TcpStream) {
    // A rejected connection is not assigned a task. try_write keeps the
    // accept loop from being held by a client that will not read the error.
    let response = b"{\"type\":\"error\",\"code\":\"connection_limit\",\"message\":\"maximum client connections reached\"}\n";
    if let Err(error) = stream.try_write(response)
        && error.kind() == std::io::ErrorKind::WouldBlock
    {
        let _ = tokio::time::timeout(CONNECTION_REJECTION_WRITE_TIMEOUT, stream.writable()).await;
        let _ = stream.try_write(response);
    }
}

fn invalid_request_response(message: &str) -> Response {
    Response::Error {
        code: "invalid_request".to_owned(),
        message: message.to_owned(),
    }
}

fn request_size_response(max_request_bytes: usize) -> Response {
    Response::Error {
        code: "request_too_large".to_owned(),
        message: format!(
            "request frame exceeds the configured maximum of {max_request_bytes} bytes"
        ),
    }
}

fn saturated_response() -> Response {
    Response::Error {
        code: "request_saturated".to_owned(),
        message: "maximum in-flight request work is currently active".to_owned(),
    }
}

fn timeout_response() -> Response {
    Response::Error {
        code: "request_timeout".to_owned(),
        message: "request exceeded the configured timeout".to_owned(),
    }
}

async fn handle_request(
    engine: &dyn Engine,
    request: Request,
    metrics: &ServerMetrics,
) -> Response {
    #[cfg(feature = "instrumentation")]
    let _stage_timer = StageTimer::new("server.engine_request");
    if let Request::PublishBatch { records, .. } = &request {
        if records.is_empty() {
            return invalid_request_response("publish batch must contain at least one record");
        }
        if records.len() > MAX_PUBLISH_BATCH_RECORDS {
            return invalid_request_response(&format!(
                "publish batch contains more than {MAX_PUBLISH_BATCH_RECORDS} records"
            ));
        }
    }
    let result = match request {
        Request::CreateStream { stream } => engine.create_stream(&stream).await.map(|created| {
            if created {
                metrics.stream_creations.fetch_add(1, Ordering::Relaxed);
            }
            Response::StreamCreated { stream, created }
        }),
        Request::Publish {
            stream,
            key,
            payload,
            request_id,
        } => {
            let payload_bytes = payload.len() as u64;
            engine
                .publish(&stream, key, payload.into_bytes(), request_id)
                .await
                .map(|offset| {
                    metrics.publishes.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .published_bytes
                        .fetch_add(payload_bytes, Ordering::Relaxed);
                    Response::Published { stream, offset }
                })
        }
        Request::PublishBytes {
            stream,
            key,
            payload_base64,
            request_id,
        } => {
            let payload_bytes = payload_base64.as_bytes().len() as u64;
            engine
                .publish(&stream, key, payload_base64.into_bytes(), request_id)
                .await
                .map(|offset| {
                    metrics.publishes.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .published_bytes
                        .fetch_add(payload_bytes, Ordering::Relaxed);
                    Response::Published { stream, offset }
                })
        }
        Request::PublishBatch { stream, records } => {
            let payload_sizes = records
                .iter()
                .map(|record| record.payload_base64.as_bytes().len() as u64)
                .collect::<Vec<_>>();
            let records = records
                .into_iter()
                .map(|record| PublishRecord {
                    key: record.key,
                    payload: record.payload_base64.into_bytes(),
                    request_id: record.request_id,
                })
                .collect();
            engine
                .publish_batch(&stream, records)
                .await
                .and_then(|outcomes| {
                    if outcomes.len() != payload_sizes.len() {
                        return Err(BrokerError::Cluster(
                            "publish batch returned the wrong number of outcomes".to_owned(),
                        ));
                    }
                    let outcomes = outcomes
                        .into_iter()
                        .zip(payload_sizes)
                        .map(|(outcome, payload_bytes)| match outcome {
                            Ok(offset) => {
                                metrics.publishes.fetch_add(1, Ordering::Relaxed);
                                metrics
                                    .published_bytes
                                    .fetch_add(payload_bytes, Ordering::Relaxed);
                                PublishBatchRecordResponse::Published { offset }
                            }
                            Err(error) => {
                                let Response::Error { code, message } =
                                    publish_batch_error_response(&error)
                                else {
                                    unreachable!("broker errors must map to error responses")
                                };
                                PublishBatchRecordResponse::Error { code, message }
                            }
                        })
                        .collect();
                    Ok(Response::PublishBatch { stream, outcomes })
                })
        }
        Request::Poll { stream, consumer } => {
            let result = engine.poll(&stream, &consumer).await;
            record_delivery(metrics, &result);
            result.map(|result| match result {
                PollResult::Message(message) => message_response(MessageResponse {
                    stream: message.stream,
                    consumer,
                    member: None,
                    offset: message.offset,
                    key: message.key,
                    payload: message.payload,
                    published_at_ms: message.published_at_ms,
                    delivery_token: None,
                    delivery_attempt: message.delivery_attempt,
                }),
                PollResult::Empty => Response::Empty { stream, consumer },
            })
        }
        Request::Replay {
            stream,
            consumer,
            offset,
        } => engine
            .replay(&stream, &consumer, offset)
            .await
            .map(|message| replay_message_response(message, consumer)),
        Request::PollGroup {
            stream,
            consumer,
            member,
        } => {
            let result = engine.poll_group(&stream, &consumer, &member).await;
            record_delivery(metrics, &result);
            result.map(|result| match result {
                PollResult::Message(message) => message_response(MessageResponse {
                    stream: message.stream,
                    consumer,
                    member: Some(member),
                    offset: message.offset,
                    key: message.key,
                    payload: message.payload,
                    published_at_ms: message.published_at_ms,
                    delivery_token: message.delivery_token,
                    delivery_attempt: message.delivery_attempt,
                }),
                PollResult::Empty => Response::Empty { stream, consumer },
            })
        }
        Request::Ack {
            stream,
            consumer,
            offset,
        } => engine.ack(&stream, &consumer, offset).await.map(|result| {
            metrics.acknowledgements.fetch_add(1, Ordering::Relaxed);
            Response::Acknowledged {
                stream,
                consumer,
                offset,
                already_acknowledged: result == AckResult::AlreadyAcknowledged,
            }
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
            .map(|result| {
                metrics.acknowledgements.fetch_add(1, Ordering::Relaxed);
                Response::Acknowledged {
                    stream,
                    consumer,
                    offset,
                    already_acknowledged: result == AckResult::AlreadyAcknowledged,
                }
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
    if let Ok(PollResult::Message(message)) = result {
        metrics.deliveries.fetch_add(1, Ordering::Relaxed);
        metrics
            .delivered_bytes
            .fetch_add(message.payload.len() as u64, Ordering::Relaxed);
    }
}

fn replay_message_response(message: ReplayMessage, consumer: String) -> Response {
    let ReplayMessage {
        stream,
        offset,
        key,
        payload,
        published_at_ms,
    } = message;
    match String::from_utf8(payload) {
        Ok(payload) => Response::ReplayMessage {
            stream,
            consumer,
            offset,
            key,
            payload,
            published_at_ms,
        },
        Err(error) => Response::ReplayMessageBytes {
            stream,
            consumer,
            offset,
            key,
            payload_base64: BinaryPayload::new(error.into_bytes()),
            published_at_ms,
        },
    }
}

struct MessageResponse {
    stream: String,
    consumer: String,
    member: Option<String>,
    offset: u64,
    key: Option<String>,
    payload: Vec<u8>,
    published_at_ms: u64,
    delivery_token: Option<String>,
    delivery_attempt: Option<u32>,
}

fn message_response(message: MessageResponse) -> Response {
    let MessageResponse {
        stream,
        consumer,
        member,
        offset,
        key,
        payload,
        published_at_ms,
        delivery_token,
        delivery_attempt,
    } = message;
    match String::from_utf8(payload) {
        Ok(payload) => Response::Message {
            stream,
            consumer,
            member,
            offset,
            key,
            payload,
            published_at_ms,
            delivery_token,
            delivery_attempt,
        },
        Err(error) => Response::MessageBytes {
            stream,
            consumer,
            member,
            offset,
            key,
            payload_base64: BinaryPayload::new(error.into_bytes()),
            published_at_ms,
            delivery_token,
            delivery_attempt,
        },
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
        BrokerError::HistoryUnavailable { .. } => "history_unavailable",
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

fn publish_batch_error_response(error: &BrokerError) -> Response {
    if let BrokerError::Io(io_error) = error
        && io_error.kind() == std::io::ErrorKind::InvalidInput
    {
        return Response::Error {
            code: "invalid_record".to_owned(),
            message: error.to_string(),
        };
    }
    error_response(error)
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

async fn bounded_health(
    engine: &Arc<dyn Engine>,
) -> Result<runnel_engine::HealthSnapshot, BrokerError> {
    tokio::time::timeout(HEALTH_CHECK_TIMEOUT, engine.health())
        .await
        .map_err(|_| BrokerError::Cluster("health check timed out".to_owned()))?
}

async fn readiness(State(state): State<HttpState>) -> (StatusCode, Json<HealthBody>) {
    if state.shutting_down.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthBody {
                status: "not_ready",
                streams: 0,
                storage_bytes: 0,
            }),
        );
    }

    match bounded_health(&state.engine).await {
        Ok(health) => (
            StatusCode::OK,
            Json(HealthBody {
                status: "ready",
                streams: health.streams,
                storage_bytes: health.storage_bytes,
            }),
        ),
        Err(error) => {
            state
                .metrics
                .health_check_failures
                .fetch_add(1, Ordering::Relaxed);
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
    state
        .metrics
        .metrics_scrapes
        .fetch_add(1, Ordering::Relaxed);
    match bounded_health(&state.engine).await {
        Ok(health) => {
            let snapshot_metrics = match &state.cluster {
                Some(cluster) => cluster.snapshot_metrics().await,
                None => SnapshotMetricsSnapshot::default(),
            };
            (
                StatusCode::OK,
                format_metrics(health, snapshot_metrics, &state.metrics, state.admission),
            )
        }
        Err(error) => {
            state
                .metrics
                .metrics_scrape_failures
                .fetch_add(1, Ordering::Relaxed);
            state
                .metrics
                .health_check_failures
                .fetch_add(1, Ordering::Relaxed);
            error!(%error, "metrics check failed");
            (StatusCode::SERVICE_UNAVAILABLE, String::new())
        }
    }
}

fn format_metrics(
    health: runnel_engine::HealthSnapshot,
    snapshot_metrics: SnapshotMetricsSnapshot,
    metrics: &ServerMetrics,
    admission: ProtocolAdmission,
) -> String {
    let mut output = String::with_capacity(5_000);
    writeln!(
        output,
        "# HELP runnel_streams Number of streams currently known to the broker."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_streams gauge\nrunnel_streams {}",
        health.streams
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_storage_bytes Bytes currently occupied by broker storage."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_storage_bytes gauge\nrunnel_storage_bytes {}",
        health.storage_bytes
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_in_flight_deliveries Messages currently tracked as delivered but not yet acknowledged."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_in_flight_deliveries gauge\nrunnel_in_flight_deliveries {}",
        health.in_flight_deliveries
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_active_connections Broker protocol connections currently being served."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_active_connections gauge\nrunnel_active_connections {}",
        metrics.active_connections.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_max_connections Configured maximum number of broker protocol connections."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_max_connections gauge\nrunnel_broker_max_connections {}",
        admission.max_connections
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_connections_accepted_total Broker protocol connections accepted."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_connections_accepted_total counter\nrunnel_broker_connections_accepted_total {}",
        metrics.connections_accepted.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_connections_rejected_total Broker protocol connections rejected because the connection limit was reached."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_connections_rejected_total counter\nrunnel_broker_connections_rejected_total {}",
        metrics.connections_rejected.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_connections_closed_total Broker protocol connections closed."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_connections_closed_total counter\nrunnel_broker_connections_closed_total {}",
        metrics.connections_closed.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_connection_errors_total Broker protocol connections closed because of a transport or framing error."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_connection_errors_total counter\nrunnel_broker_connection_errors_total {}",
        metrics.connection_errors.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_active_requests Broker protocol requests executing or writing responses."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_active_requests gauge\nrunnel_active_requests {}",
        metrics.active_requests.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_max_in_flight_requests Configured maximum number of broker protocol requests executing or writing responses."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_max_in_flight_requests gauge\nrunnel_broker_max_in_flight_requests {}",
        admission.max_in_flight_requests
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_max_request_bytes Configured maximum broker protocol request frame size in bytes."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_max_request_bytes gauge\nrunnel_broker_max_request_bytes {}",
        admission.max_request_bytes
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_timeout_seconds Configured maximum broker protocol request duration in seconds."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_timeout_seconds gauge\nrunnel_broker_request_timeout_seconds {:.9}",
        admission.request_timeout.as_secs_f64()
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_requests_rejected_total Broker protocol requests rejected before engine execution."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_requests_rejected_total counter\nrunnel_broker_requests_rejected_total {}",
        metrics.requests_rejected.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_size_rejections_total Broker protocol requests rejected because their frame exceeded the configured maximum."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_size_rejections_total counter\nrunnel_broker_request_size_rejections_total {}",
        metrics.request_size_rejections.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_saturation_total Broker protocol requests rejected because all in-flight request permits were occupied."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_saturation_total counter\nrunnel_broker_request_saturation_total {}",
        metrics
            .request_saturation_rejections
            .load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_timeouts_total Broker protocol requests whose frame or engine work exceeded its timeout."
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_response_write_timeouts_total Broker protocol responses whose socket write exceeded the request timeout."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_response_write_timeouts_total counter\nrunnel_broker_response_write_timeouts_total {}",
        metrics.response_write_timeouts.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_timeouts_total counter\nrunnel_broker_request_timeouts_total {}",
        metrics.request_timeouts.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_request_bytes_total Bytes received in broker protocol request lines, including line delimiters."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_bytes_total counter\nrunnel_broker_request_bytes_total {}",
        metrics.request_bytes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_response_bytes_total Bytes written in broker protocol responses, including line delimiters."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_response_bytes_total counter\nrunnel_broker_response_bytes_total {}",
        metrics.response_bytes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_redeliveries_total Messages delivered again after an acknowledgement timeout."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_redeliveries_total counter\nrunnel_redeliveries_total {}",
        health.redeliveries
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_dead_letters_total Messages moved to a dead-letter stream after reaching the delivery limit."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_dead_letters_total counter\nrunnel_dead_letters_total {}",
        health.dead_letters
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_deliveries_total Messages returned by successful poll operations."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_deliveries_total counter\nrunnel_deliveries_total {}",
        metrics.deliveries.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_delivered_bytes_total Payload bytes returned by successful poll operations."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_delivered_bytes_total counter\nrunnel_delivered_bytes_total {}",
        metrics.delivered_bytes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_stream_creations_total Successful stream creation requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_stream_creations_total counter\nrunnel_stream_creations_total {}",
        metrics.stream_creations.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_publishes_total Messages durably accepted by publish requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_publishes_total counter\nrunnel_publishes_total {}",
        metrics.publishes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_published_bytes_total Payload bytes durably accepted by publish requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_published_bytes_total counter\nrunnel_published_bytes_total {}",
        metrics.published_bytes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_acknowledgements_total Successful acknowledgement requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_acknowledgements_total counter\nrunnel_acknowledgements_total {}",
        metrics.acknowledgements.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_metrics_scrapes_total Metrics endpoint requests."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_metrics_scrapes_total counter\nrunnel_metrics_scrapes_total {}",
        metrics.metrics_scrapes.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_metrics_scrape_failures_total Metrics endpoint requests that could not complete a bounded health check."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_metrics_scrape_failures_total counter\nrunnel_metrics_scrape_failures_total {}",
        metrics.metrics_scrape_failures.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_health_check_failures_total Readiness or metrics health checks that failed or timed out."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_health_check_failures_total counter\nrunnel_health_check_failures_total {}",
        metrics.health_check_failures.load(Ordering::Relaxed)
    )
    .unwrap();
    writeln!(
        output,
        "# HELP runnel_broker_requests_total Broker protocol requests by fixed operation kind."
    )
    .unwrap();
    writeln!(output, "# TYPE runnel_broker_requests_total counter").unwrap();
    for operation in RequestOperation::ALL {
        let index = operation.index();
        writeln!(
            output,
            "runnel_broker_requests_total{{operation=\"{}\"}} {}",
            operation.name(),
            metrics.requests[index].load(Ordering::Relaxed)
        )
        .unwrap();
    }
    writeln!(
        output,
        "# HELP runnel_broker_request_failures_total Broker protocol requests that returned an error by fixed operation kind."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_failures_total counter"
    )
    .unwrap();
    for operation in RequestOperation::ALL {
        let index = operation.index();
        writeln!(
            output,
            "runnel_broker_request_failures_total{{operation=\"{}\"}} {}",
            operation.name(),
            metrics.request_failures[index].load(Ordering::Relaxed)
        )
        .unwrap();
    }
    writeln!(
        output,
        "# HELP runnel_broker_request_duration_seconds Broker protocol request duration by fixed operation kind."
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE runnel_broker_request_duration_seconds histogram"
    )
    .unwrap();
    for operation in RequestOperation::ALL {
        let duration = &metrics.request_durations[operation.index()];
        let mut cumulative = 0;
        for (index, label) in LATENCY_BUCKET_LABELS.iter().enumerate() {
            cumulative += duration.buckets[index].load(Ordering::Relaxed);
            writeln!(
                output,
                "runnel_broker_request_duration_seconds_bucket{{operation=\"{}\",le=\"{}\"}} {}",
                operation.name(),
                label,
                cumulative
            )
            .unwrap();
        }
        writeln!(
            output,
            "runnel_broker_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {}",
            operation.name(),
            duration.count.load(Ordering::Relaxed)
        )
        .unwrap();
        writeln!(
            output,
            "runnel_broker_request_duration_seconds_sum{{operation=\"{}\"}} {:.6}",
            operation.name(),
            duration.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
        )
        .unwrap();
        writeln!(
            output,
            "runnel_broker_request_duration_seconds_count{{operation=\"{}\"}} {}",
            operation.name(),
            duration.count.load(Ordering::Relaxed)
        )
        .unwrap();
    }
    writeln!(output, "# TYPE runnel_snapshot_builds_started_total counter\nrunnel_snapshot_builds_started_total {}", snapshot_metrics.builds_started).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_builds_completed_total counter\nrunnel_snapshot_builds_completed_total {}", snapshot_metrics.builds_completed).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_build_failures_total counter\nrunnel_snapshot_build_failures_total {}", snapshot_metrics.build_failures).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_installs_started_total counter\nrunnel_snapshot_installs_started_total {}", snapshot_metrics.installs_started).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_installs_completed_total counter\nrunnel_snapshot_installs_completed_total {}", snapshot_metrics.installs_completed).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_install_failures_total counter\nrunnel_snapshot_install_failures_total {}", snapshot_metrics.install_failures).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_install_bytes_total counter\nrunnel_snapshot_install_bytes_total {}", snapshot_metrics.install_bytes).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_installs_in_progress gauge\nrunnel_snapshot_installs_in_progress {}", snapshot_metrics.installs_in_progress).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_transfer_chunks_received_total counter\nrunnel_snapshot_transfer_chunks_received_total {}", snapshot_metrics.transfer_chunks).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_transfer_final_chunks_received_total counter\nrunnel_snapshot_transfer_final_chunks_received_total {}", snapshot_metrics.transfer_final_chunks).unwrap();
    writeln!(output, "# TYPE runnel_snapshot_transfer_bytes_received_total counter\nrunnel_snapshot_transfer_bytes_received_total {}", snapshot_metrics.transfer_bytes).unwrap();
    output
}
