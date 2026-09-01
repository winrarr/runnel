use std::collections::{BTreeMap, HashMap};
use std::io;
use std::mem::size_of;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use openraft::BasicNode;
use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore, watch};

use crate::{GroupManager, METADATA_GROUP_ID, StreamMetadata, TypeConfig};
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{AckResult, BrokerError, Offset, PollResult};

const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;
const MAX_REUSABLE_FRAME_BUFFER_SIZE: usize = 1024 * 1024;
// Stateless forwarding has no owner that can explicitly close an idle pool.
// Reap old sockets lazily on the next checkout so an inactive peer does not
// retain file descriptors indefinitely. A background reaper belongs with a
// future long-lived transport owner rather than this compatibility bridge.
const MAX_IDLE_CONNECTION_AGE: Duration = Duration::from_secs(30);
// Topology-free forwarding and stateless control RPCs receive no long-lived
// network object on which to scope their connections. Keep this compatibility
// bridge process-wide but bounded until that ownership moves into the engine.
// Full registries evict idle pools by recency; busy pools use the bounded
// fallback path instead of allowing an address burst to create unbounded work.
const MAX_POOLED_PEERS: usize = 64;
const MAX_CONNECTIONS_PER_POOLED_PEER: usize = 5;
// Keep one connection available for Raft heartbeats and votes while forwarded
// operations and data-group setup use the remaining bounded capacity. These
// requests share a peer address, but control traffic must not wait behind a
// slow data operation. The extra shared slot keeps the control reservation
// from reducing forwarding parallelism more than necessary.
const RESERVED_CONTROL_CONNECTIONS_PER_POOLED_PEER: usize = 1;
const MAX_SHARED_CONNECTIONS_PER_POOLED_PEER: usize =
    MAX_CONNECTIONS_PER_POOLED_PEER - RESERVED_CONTROL_CONNECTIONS_PER_POOLED_PEER;

static PEER_POOLS: OnceLock<std::sync::Mutex<PeerPoolRegistry>> = OnceLock::new();
static FALLBACK_PERMITS: OnceLock<PeerPoolPermits> = OnceLock::new();

#[derive(Clone)]
pub struct TcpNetwork {
    peers: Arc<BTreeMap<u64, String>>,
    group_id: String,
}

impl TcpNetwork {
    pub fn new(peers: BTreeMap<u64, String>, group_id: impl Into<String>) -> Self {
        Self {
            peers: Arc::new(peers),
            group_id: group_id.into(),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for TcpNetwork {
    type Network = TcpConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let address = (!node.addr.is_empty())
            .then(|| node.addr.clone())
            .or_else(|| self.peers.get(&target).cloned());
        TcpConnection {
            target,
            address,
            group_id: self.group_id.clone(),
            stream: None,
            read_buffer: Vec::new(),
        }
    }
}

pub struct TcpConnection {
    target: u64,
    address: Option<String>,
    group_id: String,
    stream: Option<TcpStream>,
    read_buffer: Vec<u8>,
}

impl TcpConnection {
    fn new(address: impl Into<String>) -> Self {
        Self {
            target: 0,
            address: Some(address.into()),
            group_id: METADATA_GROUP_ID.to_owned(),
            stream: None,
            read_buffer: Vec::new(),
        }
    }

    async fn pooled_request<Res>(
        &self,
        request: PeerRequest,
        ttl: Duration,
    ) -> Result<Res, io::Error>
    where
        Res: DeserializeOwned,
    {
        let address = self.address.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("node {} has no valid peer address", self.target),
            )
        })?;
        peer_request(&address, request, ttl).await
    }

    async fn request<Req, Res>(&mut self, request: Req, ttl: Duration) -> Result<Res, io::Error>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.peer_rpc");
        let address = self.address.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("node {} has no valid peer address", self.target),
            )
        })?;
        let stream = self.stream.take();
        let mut read_buffer = std::mem::take(&mut self.read_buffer);
        let result = tokio::time::timeout(ttl, async move {
            let mut stream = match stream {
                Some(stream) => stream,
                None => {
                    #[cfg(feature = "instrumentation")]
                    let _connect_timer = StageTimer::new("raft.peer_rpc.connect");
                    let stream = TcpStream::connect(address).await?;
                    stream.set_nodelay(true)?;
                    stream
                }
            };
            #[cfg(feature = "instrumentation")]
            let _write_timer = StageTimer::new("raft.peer_rpc.write");
            write_frame(&mut stream, &request).await?;
            #[cfg(feature = "instrumentation")]
            drop(_write_timer);
            #[cfg(feature = "instrumentation")]
            let _read_timer = StageTimer::new("raft.peer_rpc.read");
            let response = read_frame(&mut stream, &mut read_buffer).await?;
            Ok::<_, io::Error>((stream, response, read_buffer))
        })
        .await;

        match result {
            Ok(Ok((stream, response, read_buffer))) => {
                self.stream = Some(stream);
                self.read_buffer = read_buffer;
                Ok(response)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(timed_out_error()),
        }
    }
}

struct PeerConnectionPool {
    control_permits: Arc<Semaphore>,
    shared_permits: Arc<Semaphore>,
    idle: Mutex<Vec<IdleConnection>>,
}

struct IdleConnection {
    connection: TcpConnection,
    last_used: Instant,
}

#[derive(Clone, Copy)]
enum PeerRequestLane {
    Control,
    Shared,
}

#[derive(Default)]
struct PeerPoolRegistry {
    pools: HashMap<String, PeerPoolEntry>,
    access_counter: u64,
}

struct PeerPoolEntry {
    pool: Arc<PeerConnectionPool>,
    last_used: u64,
}

impl PeerPoolRegistry {
    fn pool(&mut self, address: &str) -> Option<Arc<PeerConnectionPool>> {
        let access = self.next_access();
        if let Some(entry) = self.pools.get_mut(address) {
            entry.last_used = access;
            return Some(Arc::clone(&entry.pool));
        }

        if self.pools.len() >= MAX_POOLED_PEERS {
            let idle_address = self
                .pools
                .iter()
                .filter(|(_, entry)| Arc::strong_count(&entry.pool) == 1)
                .min_by(|(left_address, left), (right_address, right)| {
                    left.last_used
                        .cmp(&right.last_used)
                        .then_with(|| left_address.cmp(right_address))
                })
                .map(|(address, _)| address.clone());
            if let Some(idle_address) = idle_address {
                self.pools.remove(&idle_address);
            }
        }
        if self.pools.len() >= MAX_POOLED_PEERS {
            return None;
        }
        let pool = Arc::new(PeerConnectionPool::new());
        self.pools.insert(
            address.to_owned(),
            PeerPoolEntry {
                pool: Arc::clone(&pool),
                last_used: access,
            },
        );
        Some(pool)
    }

    fn next_access(&mut self) -> u64 {
        let access = self.access_counter;
        self.access_counter = self.access_counter.saturating_add(1);
        access
    }
}

impl PeerConnectionPool {
    fn new() -> Self {
        Self {
            control_permits: Arc::new(Semaphore::new(RESERVED_CONTROL_CONNECTIONS_PER_POOLED_PEER)),
            shared_permits: Arc::new(Semaphore::new(MAX_SHARED_CONNECTIONS_PER_POOLED_PEER)),
            idle: Mutex::new(Vec::with_capacity(MAX_CONNECTIONS_PER_POOLED_PEER)),
        }
    }

    async fn request<Res>(
        &self,
        address: &str,
        request: PeerRequest,
        ttl: Duration,
    ) -> Result<Res, io::Error>
    where
        Res: DeserializeOwned,
    {
        let started = tokio::time::Instant::now();
        let lane = request.lane();
        let permit = tokio::time::timeout(ttl, self.permits(lane).acquire_owned())
            .await
            .map_err(|_| timed_out_error())?
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "peer RPC pool is closed"))?;
        let mut connection = self.take_connection(address, ttl, started).await?;
        let remaining = ttl.saturating_sub(started.elapsed());
        let result = connection.request(request, remaining).await;
        if result.is_ok() {
            self.return_connection(connection, ttl, started).await;
        }
        drop(permit);
        result
    }

    async fn take_connection(
        &self,
        address: &str,
        ttl: Duration,
        started: tokio::time::Instant,
    ) -> Result<TcpConnection, io::Error> {
        let remaining = ttl.saturating_sub(started.elapsed());
        let mut idle = tokio::time::timeout(remaining, self.idle.lock())
            .await
            .map_err(|_| timed_out_error())?;
        let now = Instant::now();
        let connection = loop {
            let Some(idle_connection) = idle.pop() else {
                break TcpConnection::new(address);
            };
            if now.duration_since(idle_connection.last_used) <= MAX_IDLE_CONNECTION_AGE {
                break idle_connection.connection;
            }
        };
        drop(idle);
        Ok(connection)
    }

    async fn return_connection(
        &self,
        connection: TcpConnection,
        ttl: Duration,
        started: tokio::time::Instant,
    ) {
        let remaining = ttl.saturating_sub(started.elapsed());
        let Ok(mut idle) = tokio::time::timeout(remaining, self.idle.lock()).await else {
            return;
        };
        idle.push(IdleConnection {
            connection,
            last_used: Instant::now(),
        });
    }

    fn permits(&self, lane: PeerRequestLane) -> Arc<Semaphore> {
        match lane {
            PeerRequestLane::Control => Arc::clone(&self.control_permits),
            PeerRequestLane::Shared => Arc::clone(&self.shared_permits),
        }
    }
}

fn peer_pool(address: &str) -> Option<Arc<PeerConnectionPool>> {
    let pools = PEER_POOLS.get_or_init(|| std::sync::Mutex::new(PeerPoolRegistry::default()));
    let mut pools = pools
        .lock()
        .expect("peer RPC pool registry is not poisoned");
    pools.pool(address)
}

async fn peer_request<Res>(
    address: &str,
    request: PeerRequest,
    ttl: Duration,
) -> Result<Res, io::Error>
where
    Res: DeserializeOwned,
{
    peer_request_with_pool(peer_pool(address), address, request, ttl).await
}

async fn peer_request_with_pool<Res>(
    pool: Option<Arc<PeerConnectionPool>>,
    address: &str,
    request: PeerRequest,
    ttl: Duration,
) -> Result<Res, io::Error>
where
    Res: DeserializeOwned,
{
    if let Some(pool) = pool {
        return pool.request(address, request, ttl).await;
    }

    let started = tokio::time::Instant::now();
    let permit = tokio::time::timeout(ttl, fallback_permits(request.lane()).acquire_owned())
        .await
        .map_err(|_| timed_out_error())?
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "peer RPC fallback is closed"))?;
    let remaining = ttl.saturating_sub(started.elapsed());
    let mut connection = TcpConnection::new(address);
    let result = connection.request(request, remaining).await;
    drop(permit);
    result
}

fn fallback_permits(lane: PeerRequestLane) -> Arc<Semaphore> {
    let permits = FALLBACK_PERMITS.get_or_init(|| PeerPoolPermits {
        control: Arc::new(Semaphore::new(RESERVED_CONTROL_CONNECTIONS_PER_POOLED_PEER)),
        shared: Arc::new(Semaphore::new(MAX_SHARED_CONNECTIONS_PER_POOLED_PEER)),
    });
    match lane {
        PeerRequestLane::Control => Arc::clone(&permits.control),
        PeerRequestLane::Shared => Arc::clone(&permits.shared),
    }
}

struct PeerPoolPermits {
    control: Arc<Semaphore>,
    shared: Arc<Semaphore>,
}

fn timed_out_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "peer RPC timed out")
}

pub(crate) async fn forward(
    address: &str,
    operation: ForwardedOperation,
    timeout: Duration,
) -> Result<ForwardedResponse, io::Error> {
    let response = peer_request(address, PeerRequest::Forward(operation), timeout).await?;
    match response {
        PeerResponse::Forward(response) => Ok(response),
        PeerResponse::Error(error) => Err(io::Error::other(error)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer returned the wrong forwarded response",
        )),
    }
}

impl RaftNetwork<TypeConfig> for TcpConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let request = PeerRequest::AppendEntries {
            group_id: self.group_id.clone(),
            request: rpc,
        };
        let response: PeerResponse = if request_is_heartbeat(&request) && self.stream.is_none() {
            self.pooled_request(request, option.hard_ttl()).await
        } else {
            self.request(request, option.hard_ttl()).await
        }
        .map_err(|error| unreachable_error(&error))?;
        match response {
            PeerResponse::AppendEntries(response) => Ok(response),
            PeerResponse::Error(error) => Err(unreachable_error(&io::Error::other(error))),
            _ => Err(unreachable_error(&io::Error::new(
                io::ErrorKind::InvalidData,
                "peer returned the wrong RPC response",
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        let response: PeerResponse = self
            .request(
                PeerRequest::InstallSnapshot {
                    group_id: self.group_id.clone(),
                    request: rpc,
                },
                option.hard_ttl(),
            )
            .await
            .map_err(|error| unreachable_snapshot_error(&error))?;
        match response {
            PeerResponse::InstallSnapshot(response) => Ok(response),
            PeerResponse::Error(error) => Err(unreachable_snapshot_error(&io::Error::other(error))),
            _ => Err(unreachable_snapshot_error(&io::Error::new(
                io::ErrorKind::InvalidData,
                "peer returned the wrong RPC response",
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let request = PeerRequest::Vote {
            group_id: self.group_id.clone(),
            request: rpc,
        };
        let response: PeerResponse = if self.stream.is_none() {
            self.pooled_request(request, option.hard_ttl()).await
        } else {
            self.request(request, option.hard_ttl()).await
        }
        .map_err(|error| unreachable_error(&error))?;
        match response {
            PeerResponse::Vote(response) => Ok(response),
            PeerResponse::Error(error) => Err(unreachable_error(&io::Error::other(error))),
            _ => Err(unreachable_error(&io::Error::new(
                io::ErrorKind::InvalidData,
                "peer returned the wrong RPC response",
            ))),
        }
    }
}

fn request_is_heartbeat(request: &PeerRequest) -> bool {
    matches!(
        request,
        PeerRequest::AppendEntries {
            request,
            ..
        } if request.entries.is_empty()
    )
}

#[derive(Debug, Serialize, Deserialize)]
enum PeerRequest {
    AppendEntries {
        group_id: String,
        request: AppendEntriesRequest<TypeConfig>,
    },
    InstallSnapshot {
        group_id: String,
        request: InstallSnapshotRequest<TypeConfig>,
    },
    Vote {
        group_id: String,
        request: VoteRequest<u64>,
    },
    Forward(ForwardedOperation),
    EnsureDataGroup {
        stream: String,
        stream_id: String,
        group_id: String,
    },
}

impl PeerRequest {
    fn lane(&self) -> PeerRequestLane {
        if request_is_heartbeat(self) || matches!(self, Self::Vote { .. }) {
            PeerRequestLane::Control
        } else {
            PeerRequestLane::Shared
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum PeerResponse {
    AppendEntries(AppendEntriesResponse<u64>),
    InstallSnapshot(InstallSnapshotResponse<u64>),
    Vote(VoteResponse<u64>),
    Forward(ForwardedResponse),
    Ready,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ForwardedOperation {
    CreateStream {
        stream: String,
    },
    Publish {
        stream: String,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
        published_at_ms: u64,
    },
    Poll {
        stream: String,
        consumer: String,
    },
    Ack {
        stream: String,
        consumer: String,
        offset: Offset,
    },
    PollGroup {
        stream: String,
        consumer: String,
        member: String,
    },
    AckGroup {
        stream: String,
        consumer: String,
        member: String,
        offset: Offset,
        delivery_token: String,
    },
    InitializeDataStream {
        stream: String,
        stream_id: String,
        group_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ForwardedResponse {
    CreateStream(Result<bool, ForwardError>),
    Publish(Result<Offset, ForwardError>),
    Poll(Result<PollResult, ForwardError>),
    Ack(Result<AckResult, ForwardError>),
    PollGroup(Result<PollResult, ForwardError>),
    AckGroup(Result<AckResult, ForwardError>),
    InitializeDataStream(Result<bool, ForwardError>),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ForwardError {
    NotLeader { leader_id: Option<u64> },
    AckNotInFlight { consumer: String, offset: Offset },
    StaleDelivery { consumer: String, offset: Offset },
    Message(String),
}

pub async fn serve(
    listener: TcpListener,
    manager: Arc<GroupManager>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), io::Error> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                stream.set_nodelay(true)?;
                let manager = Arc::clone(&manager);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, manager).await {
                        tracing::warn!(%peer, %error, "raft peer connection failed");
                    }
                });
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    manager: Arc<GroupManager>,
) -> Result<(), io::Error> {
    let mut read_buffer = Vec::new();
    loop {
        let request: PeerRequest = match read_frame(&mut stream, &mut read_buffer).await {
            Ok(request) => request,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = match request {
            PeerRequest::AppendEntries { group_id, request } => {
                match resolve_group(&manager, &group_id).await? {
                    Some(group) => match group.raft().append_entries(request).await {
                        Ok(response) => PeerResponse::AppendEntries(response),
                        Err(error) => PeerResponse::Error(error.to_string()),
                    },
                    None => PeerResponse::Error(format!("unknown Raft group '{group_id}'")),
                }
            }
            PeerRequest::InstallSnapshot { group_id, request } => {
                match resolve_group(&manager, &group_id).await? {
                    Some(group) => {
                        group.record_snapshot_chunk(request.data.len() as u64, request.done);
                        match group.raft().install_snapshot(request).await {
                            Ok(response) => PeerResponse::InstallSnapshot(response),
                            Err(error) => PeerResponse::Error(error.to_string()),
                        }
                    }
                    None => PeerResponse::Error(format!("unknown Raft group '{group_id}'")),
                }
            }
            PeerRequest::Vote { group_id, request } => {
                match resolve_group(&manager, &group_id).await? {
                    Some(group) => match group.raft().vote(request).await {
                        Ok(response) => PeerResponse::Vote(response),
                        Err(error) => PeerResponse::Error(error.to_string()),
                    },
                    None => PeerResponse::Error(format!("unknown Raft group '{group_id}'")),
                }
            }
            PeerRequest::Forward(operation) => {
                PeerResponse::Forward(handle_forwarded(&manager, operation).await)
            }
            PeerRequest::EnsureDataGroup {
                stream,
                stream_id,
                group_id,
            } => match manager
                .ensure_data_group_local(
                    &stream,
                    &StreamMetadata {
                        stream_id,
                        group_id,
                        lifecycle: crate::StreamLifecycle::Creating,
                    },
                )
                .await
            {
                Ok(_) => PeerResponse::Ready,
                Err(error) => PeerResponse::Error(error.to_string()),
            },
        };
        write_frame(&mut stream, &response).await?;
    }
}

async fn resolve_group(
    manager: &GroupManager,
    group_id: &str,
) -> Result<Option<Arc<crate::RaftGroup>>, io::Error> {
    manager
        .ensure_group_for_id(group_id)
        .await
        .map_err(|error| io::Error::other(error.to_string()))
}

async fn handle_forwarded(
    manager: &GroupManager,
    operation: ForwardedOperation,
) -> ForwardedResponse {
    match operation {
        ForwardedOperation::CreateStream { stream } => ForwardedResponse::CreateStream(
            manager
                .create_stream_local(stream)
                .await
                .map_err(forward_error),
        ),
        ForwardedOperation::Publish {
            stream,
            key,
            payload,
            request_id,
            published_at_ms,
        } => ForwardedResponse::Publish(
            manager
                .publish_local(stream, key, payload, published_at_ms, request_id)
                .await
                .map_err(forward_error),
        ),
        ForwardedOperation::Poll { stream, consumer } => {
            let Ok(group) = manager.data_group_for_stream(&stream).await else {
                return ForwardedResponse::Poll(Err(ForwardError::Message(
                    "stream data group is unavailable".to_owned(),
                )));
            };
            let leader_id = group.raft().current_leader().await;
            if leader_id != Some(manager.node_id()) {
                return ForwardedResponse::Poll(Err(ForwardError::NotLeader { leader_id }));
            }
            ForwardedResponse::Poll(group.poll(&stream, &consumer).await.map_err(forward_error))
        }
        ForwardedOperation::Ack {
            stream,
            consumer,
            offset,
        } => ForwardedResponse::Ack(
            manager
                .ack_local(stream, consumer, offset)
                .await
                .map_err(forward_error),
        ),
        ForwardedOperation::PollGroup {
            stream,
            consumer,
            member,
        } => ForwardedResponse::PollGroup(
            manager
                .poll_group_local(&stream, &consumer, &member)
                .await
                .map_err(forward_error),
        ),
        ForwardedOperation::AckGroup {
            stream,
            consumer,
            member,
            offset,
            delivery_token,
        } => ForwardedResponse::AckGroup(
            manager
                .ack_group_local(stream, consumer, member, offset, delivery_token)
                .await
                .map_err(forward_error),
        ),
        ForwardedOperation::InitializeDataStream {
            stream,
            stream_id,
            group_id,
        } => ForwardedResponse::InitializeDataStream(
            manager
                .initialize_data_stream_local(stream, stream_id, group_id)
                .await
                .map_err(forward_error),
        ),
    }
}

pub(crate) async fn ensure_data_group(
    address: &str,
    stream: String,
    stream_id: String,
    group_id: String,
    timeout: Duration,
) -> Result<(), io::Error> {
    match peer_request(
        address,
        PeerRequest::EnsureDataGroup {
            stream,
            stream_id,
            group_id,
        },
        timeout,
    )
    .await?
    {
        PeerResponse::Ready => Ok(()),
        PeerResponse::Error(error) => Err(io::Error::other(error)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer returned the wrong data-group response",
        )),
    }
}

fn forward_error(error: BrokerError) -> ForwardError {
    match error {
        BrokerError::NotLeader { leader_id } => ForwardError::NotLeader { leader_id },
        BrokerError::AckNotInFlight { consumer, offset } => {
            ForwardError::AckNotInFlight { consumer, offset }
        }
        BrokerError::StaleDelivery { consumer, offset } => {
            ForwardError::StaleDelivery { consumer, offset }
        }
        error => ForwardError::Message(error.to_string()),
    }
}

async fn write_frame<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<(), io::Error> {
    let mut frame = Vec::with_capacity(size_of::<u32>());
    frame.extend_from_slice(&[0; size_of::<u32>()]);
    serde_json::to_writer(&mut frame, value).map_err(io::Error::other)?;
    let length = u32::try_from(frame.len() - size_of::<u32>())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "peer RPC is too large"))?;
    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the frame limit",
        ));
    }
    frame[..size_of::<u32>()].copy_from_slice(&length.to_be_bytes());
    stream.write_all(&frame).await
}

async fn read_frame<T: DeserializeOwned>(
    stream: &mut TcpStream,
    payload: &mut Vec<u8>,
) -> Result<T, io::Error> {
    let length = stream.read_u32().await?;
    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the frame limit",
        ));
    }
    payload.resize(length as usize, 0);
    stream.read_exact(payload).await?;
    let value = serde_json::from_slice(payload).map_err(io::Error::other)?;
    if payload.capacity() > MAX_REUSABLE_FRAME_BUFFER_SIZE {
        *payload = Vec::new();
    }
    Ok(value)
}

fn unreachable_error(error: &io::Error) -> RPCError<u64, BasicNode, RaftError<u64>> {
    RPCError::Unreachable(Unreachable::new(error))
}

fn unreachable_snapshot_error(
    error: &io::Error,
) -> RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>> {
    RPCError::Unreachable(Unreachable::new(error))
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use super::*;

    struct TestPeer {
        address: String,
        connections: Arc<AtomicUsize>,
        max_active_connections: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
        framed_bytes: Arc<AtomicUsize>,
        server: tokio::task::JoinHandle<()>,
    }

    impl TestPeer {
        async fn start(close_after_requests: Option<usize>) -> Self {
            Self::start_with_delay(close_after_requests, None).await
        }

        async fn start_with_delay(
            close_after_requests: Option<usize>,
            response_delay: Option<Duration>,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let connections = Arc::new(AtomicUsize::new(0));
            let connection_count = Arc::clone(&connections);
            let active_connections = Arc::new(AtomicUsize::new(0));
            let active_connection_count = Arc::clone(&active_connections);
            let max_active_connections = Arc::new(AtomicUsize::new(0));
            let max_active_connection_count = Arc::clone(&max_active_connections);
            let requests = Arc::new(AtomicUsize::new(0));
            let request_count = Arc::clone(&requests);
            let framed_bytes = Arc::new(AtomicUsize::new(0));
            let framed_byte_count = Arc::clone(&framed_bytes);
            let server = tokio::spawn(async move {
                let mut handlers = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let Ok((mut stream, _)) = accepted else {
                                return;
                            };
                            stream.set_nodelay(true).unwrap();
                            connection_count.fetch_add(1, Ordering::Relaxed);
                            let active =
                                active_connection_count.fetch_add(1, Ordering::Relaxed) + 1;
                            max_active_connection_count.fetch_max(active, Ordering::Relaxed);
                            let active_connection_count = Arc::clone(&active_connection_count);
                            let request_count = Arc::clone(&request_count);
                            let framed_byte_count = Arc::clone(&framed_byte_count);
                            handlers.spawn(async move {
                                let mut read_buffer = Vec::new();
                                let mut requests = 0;
                                loop {
                                    let request = match read_frame::<PeerRequest>(
                                        &mut stream,
                                        &mut read_buffer,
                                    )
                                    .await
                                    {
                                        Ok(request) => request,
                                        Err(_) => break,
                                    };
                                    requests += 1;
                                    request_count.fetch_add(1, Ordering::Relaxed);
                                    let frame_size = serde_json::to_vec(&request).unwrap().len()
                                        + size_of::<u32>();
                                    framed_byte_count.fetch_add(frame_size, Ordering::Relaxed);
                                    let response = match request {
                                        PeerRequest::AppendEntries { .. } => {
                                            PeerResponse::AppendEntries(AppendEntriesResponse::Success)
                                        }
                                        PeerRequest::Vote { request, .. } => {
                                            PeerResponse::Vote(VoteResponse::new(
                                                request.vote,
                                                request.last_log_id,
                                                true,
                                            ))
                                        }
                                        PeerRequest::Forward(_) => {
                                            PeerResponse::Forward(ForwardedResponse::CreateStream(Ok(true)))
                                        }
                                        _ => PeerResponse::Error("unexpected test request".to_owned()),
                                    };
                                    if let Some(delay) = response_delay {
                                        tokio::time::sleep(delay).await;
                                    }
                                    if write_frame(&mut stream, &response).await.is_err() {
                                        break;
                                    }
                                    if close_after_requests.is_some_and(|limit| requests >= limit) {
                                        break;
                                    }
                                }
                                active_connection_count.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Some(_) = handlers.join_next(), if !handlers.is_empty() => {}
                    }
                }
            });
            Self {
                address,
                connections,
                max_active_connections,
                requests,
                framed_bytes,
                server,
            }
        }

        fn connection(&self) -> TcpConnection {
            TcpConnection {
                target: 1,
                address: Some(self.address.clone()),
                group_id: "test".to_owned(),
                stream: None,
                read_buffer: Vec::new(),
            }
        }
    }

    impl Drop for TestPeer {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    fn request() -> PeerRequest {
        PeerRequest::Forward(forwarded_operation())
    }

    fn control_request() -> PeerRequest {
        PeerRequest::Vote {
            group_id: "test".to_owned(),
            request: VoteRequest::new(openraft::Vote::new(1, 1), None),
        }
    }

    fn forwarded_operation() -> ForwardedOperation {
        ForwardedOperation::CreateStream {
            stream: "test".to_owned(),
        }
    }

    fn capped_registry() -> PeerPoolRegistry {
        let mut registry = PeerPoolRegistry::default();
        for index in 0..MAX_POOLED_PEERS {
            assert!(registry.pool(&format!("test-peer-{index}")).is_some());
        }
        registry
    }

    fn busy_registry() -> (PeerPoolRegistry, Vec<Arc<PeerConnectionPool>>) {
        let mut registry = PeerPoolRegistry::default();
        let mut pools = Vec::with_capacity(MAX_POOLED_PEERS);
        for index in 0..MAX_POOLED_PEERS {
            pools.push(registry.pool(&format!("test-peer-{index}")).unwrap());
        }
        (registry, pools)
    }

    #[test]
    fn full_registry_evicts_the_oldest_idle_pool_deterministically() {
        let mut registry = capped_registry();

        assert!(registry.pool("new-peer-1").is_some());
        assert_eq!(registry.pools.len(), MAX_POOLED_PEERS);
        assert!(!registry.pools.contains_key("test-peer-0"));

        assert!(registry.pool("test-peer-1").is_some());
        assert!(registry.pool("new-peer-2").is_some());
        assert_eq!(registry.pools.len(), MAX_POOLED_PEERS);
        assert!(!registry.pools.contains_key("test-peer-2"));
    }

    #[test]
    fn full_registry_keeps_busy_pools_for_bounded_fallback() {
        let (mut registry, held_pools) = busy_registry();

        assert!(registry.pool("busy-overflow-peer").is_none());
        assert_eq!(registry.pools.len(), MAX_POOLED_PEERS);

        drop(held_pools);
        assert!(registry.pool("busy-overflow-peer").is_some());
        assert_eq!(registry.pools.len(), MAX_POOLED_PEERS);
    }

    #[tokio::test]
    async fn repeated_requests_reuse_connection() {
        let peer = TestPeer::start(None).await;
        let mut connection = peer.connection();
        let started = Instant::now();
        for _ in 0..256 {
            let response: PeerResponse = connection
                .request(request(), Duration::from_secs(1))
                .await
                .unwrap();
            assert!(matches!(
                response,
                PeerResponse::Forward(ForwardedResponse::CreateStream(Ok(true)))
            ));
        }
        eprintln!(
            "repeated peer requests: connections={}, elapsed={:?}",
            peer.connections.load(Ordering::Relaxed),
            started.elapsed()
        );
        assert_eq!(peer.connections.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn repeated_framed_reads_reuse_the_payload_buffer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            write_frame(&mut stream, &request()).await.unwrap();
            write_frame(&mut stream, &request()).await.unwrap();
            write_frame(
                &mut stream,
                &PeerRequest::Forward(ForwardedOperation::Publish {
                    stream: "test".to_owned(),
                    key: None,
                    payload: vec![0; MAX_REUSABLE_FRAME_BUFFER_SIZE + 1],
                    request_id: None,
                    published_at_ms: 0,
                }),
            )
            .await
            .unwrap();
        });

        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut payload = Vec::new();
        let _: PeerRequest = read_frame(&mut stream, &mut payload).await.unwrap();
        let capacity = payload.capacity();
        let _: PeerRequest = read_frame(&mut stream, &mut payload).await.unwrap();

        assert_eq!(payload.capacity(), capacity);
        let _: PeerRequest = read_frame(&mut stream, &mut payload).await.unwrap();
        assert!(payload.is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn topology_free_forwarding_reuses_a_connection() {
        const REQUESTS: usize = 64;

        let peer = TestPeer::start(None).await;
        let started = Instant::now();
        for _ in 0..REQUESTS {
            let response = forward(&peer.address, forwarded_operation(), Duration::from_secs(1))
                .await
                .unwrap();
            assert!(matches!(
                response,
                ForwardedResponse::CreateStream(Ok(true))
            ));
        }
        let connections = peer.connections.load(Ordering::Relaxed);
        eprintln!(
            "topology-free forwarding: requests={REQUESTS}, connections={connections}, elapsed={:?}",
            started.elapsed()
        );
        assert_eq!(connections, 1);
    }

    #[tokio::test]
    async fn expired_idle_pool_connection_is_replaced_before_reuse() {
        let peer = TestPeer::start(None).await;
        let pool = PeerConnectionPool::new();

        let response: PeerResponse = pool
            .request(&peer.address, request(), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(matches!(
            response,
            PeerResponse::Forward(ForwardedResponse::CreateStream(Ok(true)))
        ));

        {
            let mut idle = pool.idle.lock().await;
            idle.last_mut().unwrap().last_used =
                Instant::now() - (MAX_IDLE_CONNECTION_AGE + Duration::from_secs(1));
        }

        let response: PeerResponse = pool
            .request(&peer.address, request(), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(matches!(
            response,
            PeerResponse::Forward(ForwardedResponse::CreateStream(Ok(true)))
        ));
        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn pool_idle_lock_wait_is_bounded_by_request_ttl() {
        let peer = TestPeer::start(None).await;
        let pool = PeerConnectionPool::new();
        let idle = pool.idle.lock().await;

        let error = pool
            .request::<PeerResponse>(&peer.address, request(), Duration::from_millis(1))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(peer.connections.load(Ordering::Relaxed), 0);

        drop(idle);
        let response: PeerResponse = pool
            .request(&peer.address, request(), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(matches!(
            response,
            PeerResponse::Forward(ForwardedResponse::CreateStream(Ok(true)))
        ));
        assert_eq!(peer.connections.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn control_plane_heartbeats_and_votes_reuse_a_bounded_connection() {
        const REQUESTS: usize = 64;

        let peer = TestPeer::start(None).await;
        let mut network = TcpNetwork::new(BTreeMap::from([(1, peer.address.clone())]), "test");
        let started = Instant::now();
        for request_number in 0..REQUESTS {
            let mut client = network.new_client(1, &BasicNode::new(&peer.address)).await;
            if request_number % 2 == 0 {
                let response = client
                    .append_entries(
                        AppendEntriesRequest {
                            vote: openraft::Vote::new(1, 1),
                            prev_log_id: None,
                            entries: Vec::new(),
                            leader_commit: None,
                        },
                        RPCOption::new(Duration::from_secs(1)),
                    )
                    .await
                    .unwrap();
                assert!(response.is_success());
            } else {
                let response = client
                    .vote(
                        VoteRequest::new(openraft::Vote::new(1, 1), None),
                        RPCOption::new(Duration::from_secs(1)),
                    )
                    .await
                    .unwrap();
                assert!(response.vote_granted);
            }
        }

        let connections = peer.connections.load(Ordering::Relaxed);
        let requests = peer.requests.load(Ordering::Relaxed);
        let framed_bytes = peer.framed_bytes.load(Ordering::Relaxed);
        eprintln!(
            "control-plane framing: requests={requests}, framed_bytes={framed_bytes}, connections={connections}, elapsed={:?}",
            started.elapsed()
        );
        assert_eq!(requests, REQUESTS);
        assert!(framed_bytes > REQUESTS * size_of::<u32>());
        assert_eq!(connections, 1);
    }

    #[tokio::test]
    async fn failed_control_plane_connection_is_replaced_before_next_request() {
        let peer = TestPeer::start(Some(1)).await;
        let mut network = TcpNetwork::new(BTreeMap::from([(1, peer.address.clone())]), "test");

        let mut client = network.new_client(1, &BasicNode::new(&peer.address)).await;
        client
            .vote(
                VoteRequest::new(openraft::Vote::new(1, 1), None),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap();

        let mut client = network.new_client(1, &BasicNode::new(&peer.address)).await;
        assert!(
            client
                .vote(
                    VoteRequest::new(openraft::Vote::new(1, 1), None),
                    RPCOption::new(Duration::from_secs(1)),
                )
                .await
                .is_err()
        );

        let mut client = network.new_client(1, &BasicNode::new(&peer.address)).await;
        client
            .vote(
                VoteRequest::new(openraft::Vote::new(1, 1), None),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap();

        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn concurrent_forwarding_uses_a_bounded_connection_pool() {
        const REQUESTS: usize = 32;

        let peer = TestPeer::start_with_delay(None, Some(Duration::from_millis(50))).await;
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            let address = peer.address.clone();
            tasks.push(tokio::spawn(async move {
                forward(&address, forwarded_operation(), Duration::from_secs(1)).await
            }));
        }

        for task in tasks {
            let response = task.await.unwrap().unwrap();
            assert!(matches!(
                response,
                ForwardedResponse::CreateStream(Ok(true))
            ));
        }

        let connections = peer.connections.load(Ordering::Relaxed);
        eprintln!(
            "concurrent topology-free forwarding: requests={REQUESTS}, connections={connections}, elapsed={:?}",
            started.elapsed()
        );
        assert_eq!(connections, MAX_SHARED_CONNECTIONS_PER_POOLED_PEER);
        assert_eq!(
            peer.max_active_connections.load(Ordering::Relaxed),
            MAX_SHARED_CONNECTIONS_PER_POOLED_PEER
        );
    }

    async fn assert_reserved_control_connection(
        peer: &TestPeer,
        pool: Option<Arc<PeerConnectionPool>>,
    ) {
        let mut data_tasks = Vec::with_capacity(MAX_SHARED_CONNECTIONS_PER_POOLED_PEER);
        for _ in 0..MAX_SHARED_CONNECTIONS_PER_POOLED_PEER {
            let address = peer.address.clone();
            let pool = pool.clone();
            data_tasks.push(tokio::spawn(async move {
                peer_request_with_pool::<PeerResponse>(
                    pool,
                    &address,
                    request(),
                    Duration::from_secs(5),
                )
                .await
            }));
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while peer.connections.load(Ordering::Relaxed) < MAX_SHARED_CONNECTIONS_PER_POOLED_PEER
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("shared peer connections did not become active");

        let address = peer.address.clone();
        let control = tokio::spawn(async move {
            peer_request_with_pool::<PeerResponse>(
                pool,
                &address,
                control_request(),
                Duration::from_millis(250),
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while peer.connections.load(Ordering::Relaxed)
                < MAX_SHARED_CONNECTIONS_PER_POOLED_PEER
                    + RESERVED_CONTROL_CONNECTIONS_PER_POOLED_PEER
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("control traffic did not receive its reserved connection");

        assert_eq!(
            peer.max_active_connections.load(Ordering::Relaxed),
            MAX_CONNECTIONS_PER_POOLED_PEER
        );

        let error = control.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        for task in data_tasks {
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test]
    async fn control_plane_keeps_a_reserved_connection_during_forwarding_contention() {
        let peer = TestPeer::start_with_delay(None, Some(Duration::from_secs(1))).await;
        assert_reserved_control_connection(&peer, Some(Arc::new(PeerConnectionPool::new()))).await;
    }

    #[tokio::test]
    async fn fallback_control_plane_keeps_a_reserved_connection_during_forwarding_contention() {
        let peer = TestPeer::start_with_delay(None, Some(Duration::from_secs(1))).await;
        assert_reserved_control_connection(&peer, None).await;
    }

    #[tokio::test]
    async fn busy_capped_peer_address_uses_bounded_fallback_connections() {
        const REQUESTS: usize = 16;

        let (mut registry, _held_pools) = busy_registry();
        let peer = TestPeer::start_with_delay(None, Some(Duration::from_millis(50))).await;
        let fallback_pool = registry.pool(&peer.address);
        assert!(fallback_pool.is_none());
        let frame_size = serde_json::to_vec(&request()).unwrap().len() + size_of::<u32>();

        let mut tasks = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            let address = peer.address.clone();
            let pool = fallback_pool.clone();
            tasks.push(tokio::spawn(async move {
                let response: PeerResponse =
                    peer_request_with_pool(pool, &address, request(), Duration::from_secs(1))
                        .await?;
                Ok::<_, io::Error>(response)
            }));
        }

        for task in tasks {
            let response = task.await.unwrap().unwrap();
            assert!(matches!(
                response,
                PeerResponse::Forward(ForwardedResponse::CreateStream(Ok(true)))
            ));
        }

        assert_eq!(peer.connections.load(Ordering::Relaxed), REQUESTS);
        assert_eq!(peer.requests.load(Ordering::Relaxed), REQUESTS);
        assert_eq!(
            peer.max_active_connections.load(Ordering::Relaxed),
            MAX_SHARED_CONNECTIONS_PER_POOLED_PEER
        );
        assert_eq!(
            peer.framed_bytes.load(Ordering::Relaxed),
            REQUESTS * frame_size
        );
    }

    #[tokio::test]
    async fn capped_peer_fallback_timeout_drops_connection_before_next_request() {
        let (mut registry, _held_pools) = busy_registry();
        let peer = TestPeer::start_with_delay(None, Some(Duration::from_millis(50))).await;
        let error = peer_request_with_pool::<PeerResponse>(
            registry.pool(&peer.address),
            &peer.address,
            request(),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        let response: PeerResponse = peer_request_with_pool(
            registry.pool(&peer.address),
            &peer.address,
            request(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(matches!(
            response,
            PeerResponse::Forward(ForwardedResponse::CreateStream(Ok(true)))
        ));
        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn capped_peer_fallback_drops_failed_connection_before_next_request() {
        let (mut registry, _held_pools) = busy_registry();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let connections = Arc::new(AtomicUsize::new(0));
        let connection_count = Arc::clone(&connections);
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                connection_count.fetch_add(1, Ordering::Relaxed);
                drop(stream);
            }
        });

        for _ in 0..2 {
            let error = peer_request_with_pool::<PeerResponse>(
                registry.pool(&address),
                &address,
                request(),
                Duration::from_secs(1),
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::UnexpectedEof
            ));
        }

        server.await.unwrap();
        assert_eq!(connections.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn failed_forward_connection_is_replaced_before_next_request() {
        let peer = TestPeer::start(Some(1)).await;

        forward(&peer.address, forwarded_operation(), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(
            forward(&peer.address, forwarded_operation(), Duration::from_secs(1))
                .await
                .is_err()
        );
        forward(&peer.address, forwarded_operation(), Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn timed_out_forward_request_drops_connection_before_reconnect() {
        let peer = TestPeer::start_with_delay(None, Some(Duration::from_millis(50))).await;

        let error = forward(
            &peer.address,
            forwarded_operation(),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        forward(&peer.address, forwarded_operation(), Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn failed_persistent_connection_is_replaced_before_next_request() {
        let peer = TestPeer::start(Some(1)).await;
        let mut connection = peer.connection();

        connection
            .request::<_, PeerResponse>(request(), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(
            connection
                .request::<_, PeerResponse>(request(), Duration::from_secs(1))
                .await
                .is_err()
        );
        connection
            .request::<_, PeerResponse>(request(), Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn timed_out_request_drops_connection_before_reconnect() {
        let peer = TestPeer::start_with_delay(None, Some(Duration::from_millis(50))).await;
        let mut connection = peer.connection();

        let error = connection
            .request::<_, PeerResponse>(request(), Duration::from_millis(1))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        connection
            .request::<_, PeerResponse>(request(), Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
    }
}
