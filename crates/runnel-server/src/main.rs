use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use runnel_core::{Broker, BrokerConfig};
use runnel_engine::Engine;
use runnel_raft::{NodeId, PersistentEngine};
use tokio::net::TcpListener;
use tracing::info;

mod connection;
mod dispatch;
mod lifecycle;
mod observability;
mod protocol;

use observability::ServerMetrics;
pub(crate) use protocol::ProtocolAdmission;

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

    let server_metrics = Arc::new(ServerMetrics::default());
    let protocol_admission = ProtocolAdmission {
        max_connections: args.max_connections,
        max_request_bytes: args.max_request_bytes,
        max_in_flight_requests: args.max_in_flight_requests,
        request_timeout: Duration::from_millis(args.request_timeout_ms),
    };
    lifecycle::run(
        tcp_listener,
        http_listener,
        engine,
        peer,
        cluster,
        server_metrics,
        protocol_admission,
    )
    .await?;
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
