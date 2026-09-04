use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use runnel_core::{Broker, BrokerConfig};
use runnel_engine::Engine;
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_protocol::{Request, Response};
use runnel_raft::{NodeId, PersistentEngine};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

mod dispatch;
mod observability;
mod protocol;

use dispatch::handle_request;
use observability::{ActiveConnection, ActiveRequest, RequestOperation, ServerMetrics};
pub(crate) use protocol::ProtocolAdmission;
use protocol::{
    Frame, invalid_request_response, read_frame, reject_connection, remaining_timeout,
    request_line, request_size_response, response_write_timeout, saturated_response, send_response,
    timeout_response, wait_for_request_data,
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const DEFAULT_MAX_CONNECTIONS: usize = 1_024;
const DEFAULT_MAX_REQUEST_BYTES: usize = 1_048_576;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 256;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "runnel=info".into()),
        )
        .init();

    let args = Args::parse();
    protocol::validate_admission_config(
        args.max_connections,
        args.max_request_bytes,
        args.max_in_flight_requests,
        args.request_timeout_ms,
    )?;
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
    let app = observability::router(
        engine,
        cluster,
        server_metrics,
        protocol_admission,
        http_shutting_down,
    );
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
    let _active_connection = ActiveConnection::new(Arc::clone(&metrics));
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
                metrics
                    .record_rejected_request(RequestOperation::InvalidRequest, started.elapsed());
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
                metrics
                    .request_size_rejections
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .record_rejected_request(RequestOperation::InvalidRequest, started.elapsed());
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
                metrics
                    .record_rejected_request(RequestOperation::InvalidRequest, started.elapsed());
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
                                metrics
                                    .request_saturation_rejections
                                    .fetch_add(1, Ordering::Relaxed);
                                let response = saturated_response();
                                metrics.record_rejected_request(operation, started.elapsed());
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
                        let _active_request = ActiveRequest::new(Arc::clone(&metrics));
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
                            metrics.record_rejected_request(
                                RequestOperation::InvalidRequest,
                                started.elapsed(),
                            );
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "request frame was not valid UTF-8",
                            )
                            .into());
                        }
                        let response = invalid_request_response(&error.to_string());
                        metrics.record_rejected_request(
                            RequestOperation::InvalidRequest,
                            started.elapsed(),
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
