use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use openraft::BasicNode;
use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};

use crate::{METADATA_GROUP_ID, TypeConfig};
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;

use super::{
    ForwardedOperation, ForwardedResponse, PeerRequest, PeerResponse, read_frame, write_frame,
};

// Reap old sockets lazily on the next checkout so an inactive peer does not
// retain file descriptors indefinitely. The transport owner closes all
// remaining sockets when its engine lifetime ends.
const MAX_IDLE_CONNECTION_AGE: Duration = Duration::from_secs(30);
// Full registries evict idle pools by recency; busy pools use the bounded
// fallback path instead of allowing an address burst to create unbounded work.
const MAX_POOLED_PEERS: usize = 64;
const MAX_CONNECTIONS_PER_POOLED_PEER: usize = 5;
// Keep one connection available for Raft heartbeats and votes while forwarded
// operations and data-group setup use the remaining shared capacity. These
// requests share a peer address, but control traffic must not wait behind a
// slow data operation.
const RESERVED_CONTROL_CONNECTIONS_PER_POOLED_PEER: usize = 1;
const MAX_SHARED_CONNECTIONS_PER_POOLED_PEER: usize =
    MAX_CONNECTIONS_PER_POOLED_PEER - RESERVED_CONTROL_CONNECTIONS_PER_POOLED_PEER;

#[derive(Clone)]
pub struct TcpNetwork {
    peers: Arc<BTreeMap<u64, String>>,
    group_id: String,
    transport: Arc<PeerTransport>,
}

impl TcpNetwork {
    pub(crate) fn with_transport(
        peers: BTreeMap<u64, String>,
        group_id: impl Into<String>,
        transport: Arc<PeerTransport>,
    ) -> Self {
        Self {
            peers: Arc::new(peers),
            group_id: group_id.into(),
            transport,
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
            transport: Some(Arc::clone(&self.transport)),
        }
    }
}

pub struct TcpConnection {
    target: u64,
    address: Option<String>,
    group_id: String,
    stream: Option<TcpStream>,
    read_buffer: Vec<u8>,
    transport: Option<Arc<PeerTransport>>,
}

impl TcpConnection {
    fn new(address: impl Into<String>) -> Self {
        Self {
            target: 0,
            address: Some(address.into()),
            group_id: METADATA_GROUP_ID.to_owned(),
            stream: None,
            read_buffer: Vec::new(),
            transport: None,
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
        self.transport
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "peer RPC connection has no transport owner",
                )
            })?
            .request(&address, request, ttl)
            .await
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

/// Owns all compatibility peer connections for one broker engine lifetime.
///
/// OpenRaft retains one lazy network client per replication target and group,
/// while forwarding and setup requests do not receive such an owner. Keeping
/// this registry with the group manager bounds the latter path to that
/// manager's lifecycle and prevents unrelated engines in one process from
/// sharing sockets or pool capacity.
pub(crate) struct PeerTransport {
    pools: StdMutex<PeerPoolRegistry>,
    fallback_permits: PeerPoolPermits,
}

impl PeerTransport {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            pools: StdMutex::new(PeerPoolRegistry::default()),
            fallback_permits: PeerPoolPermits::default(),
        })
    }

    fn pool(&self, address: &str) -> Option<Arc<PeerConnectionPool>> {
        let mut pools = self
            .pools
            .lock()
            .expect("peer RPC transport registry is not poisoned");
        pools.pool(address)
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
        self.request_with_pool(self.pool(address), address, request, ttl)
            .await
    }

    async fn request_with_pool<Res>(
        &self,
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
        let permit = tokio::time::timeout(
            ttl,
            self.fallback_permits
                .permits(request.lane())
                .acquire_owned(),
        )
        .await
        .map_err(|_| timed_out_error())?
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "peer RPC fallback is closed"))?;
        let remaining = ttl.saturating_sub(started.elapsed());
        let mut connection = TcpConnection::new(address);
        let result = connection.request(request, remaining).await;
        drop(permit);
        result
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

struct PeerPoolPermits {
    control: Arc<Semaphore>,
    shared: Arc<Semaphore>,
}

impl Default for PeerPoolPermits {
    fn default() -> Self {
        Self {
            control: Arc::new(Semaphore::new(RESERVED_CONTROL_CONNECTIONS_PER_POOLED_PEER)),
            shared: Arc::new(Semaphore::new(MAX_SHARED_CONNECTIONS_PER_POOLED_PEER)),
        }
    }
}

impl PeerPoolPermits {
    fn permits(&self, lane: PeerRequestLane) -> Arc<Semaphore> {
        match lane {
            PeerRequestLane::Control => Arc::clone(&self.control),
            PeerRequestLane::Shared => Arc::clone(&self.shared),
        }
    }
}

fn timed_out_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "peer RPC timed out")
}

pub(crate) async fn forward(
    transport: &PeerTransport,
    address: &str,
    operation: ForwardedOperation,
    timeout: Duration,
) -> Result<ForwardedResponse, io::Error> {
    let response = transport
        .request(address, PeerRequest::Forward(operation), timeout)
        .await?;
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

impl PeerRequest {
    fn lane(&self) -> PeerRequestLane {
        if request_is_heartbeat(self) || matches!(self, Self::Vote { .. }) {
            PeerRequestLane::Control
        } else {
            PeerRequestLane::Shared
        }
    }
}

pub(crate) async fn ensure_data_group(
    transport: &PeerTransport,
    address: &str,
    stream: String,
    stream_id: String,
    group_id: String,
    timeout: Duration,
) -> Result<(), io::Error> {
    match transport
        .request(
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
    use crate::network::MAX_REUSABLE_FRAME_BUFFER_SIZE;

    use std::mem::size_of;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use tokio::net::TcpListener;

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
                transport: None,
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
        let transport = PeerTransport::new();
        let started = Instant::now();
        for _ in 0..REQUESTS {
            let response = forward(
                &transport,
                &peer.address,
                forwarded_operation(),
                Duration::from_secs(1),
            )
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
    async fn compatibility_connections_follow_transport_lifetime() {
        let peer = TestPeer::start(None).await;

        let transport = PeerTransport::new();
        forward(
            &transport,
            &peer.address,
            forwarded_operation(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        drop(transport);

        let transport = PeerTransport::new();
        forward(
            &transport,
            &peer.address,
            forwarded_operation(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
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
        let mut network = TcpNetwork::with_transport(
            BTreeMap::from([(1, peer.address.clone())]),
            "test",
            PeerTransport::new(),
        );
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
        let mut network = TcpNetwork::with_transport(
            BTreeMap::from([(1, peer.address.clone())]),
            "test",
            PeerTransport::new(),
        );

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
        let transport = PeerTransport::new();
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            let address = peer.address.clone();
            let transport = Arc::clone(&transport);
            tasks.push(tokio::spawn(async move {
                forward(
                    &transport,
                    &address,
                    forwarded_operation(),
                    Duration::from_secs(1),
                )
                .await
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
        let transport = PeerTransport::new();
        let mut data_tasks = Vec::with_capacity(MAX_SHARED_CONNECTIONS_PER_POOLED_PEER);
        for _ in 0..MAX_SHARED_CONNECTIONS_PER_POOLED_PEER {
            let address = peer.address.clone();
            let pool = pool.clone();
            let transport = Arc::clone(&transport);
            data_tasks.push(tokio::spawn(async move {
                transport
                    .request_with_pool::<PeerResponse>(
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
        let transport_for_control = Arc::clone(&transport);
        let control = tokio::spawn(async move {
            transport_for_control
                .request_with_pool::<PeerResponse>(
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
        let transport = PeerTransport::new();
        let fallback_pool = registry.pool(&peer.address);
        assert!(fallback_pool.is_none());
        let frame_size = serde_json::to_vec(&request()).unwrap().len() + size_of::<u32>();

        let mut tasks = Vec::with_capacity(REQUESTS);
        for _ in 0..REQUESTS {
            let address = peer.address.clone();
            let pool = fallback_pool.clone();
            let transport = Arc::clone(&transport);
            tasks.push(tokio::spawn(async move {
                let response: PeerResponse = transport
                    .request_with_pool(pool, &address, request(), Duration::from_secs(1))
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
        let transport = PeerTransport::new();
        let error = transport
            .request_with_pool::<PeerResponse>(
                registry.pool(&peer.address),
                &peer.address,
                request(),
                Duration::from_millis(1),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        let response: PeerResponse = transport
            .request_with_pool(
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
        let transport = PeerTransport::new();

        for _ in 0..2 {
            let error = transport
                .request_with_pool::<PeerResponse>(
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
        let transport = PeerTransport::new();

        forward(
            &transport,
            &peer.address,
            forwarded_operation(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(
            forward(
                &transport,
                &peer.address,
                forwarded_operation(),
                Duration::from_secs(1),
            )
            .await
            .is_err()
        );
        forward(
            &transport,
            &peer.address,
            forwarded_operation(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert_eq!(peer.connections.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn timed_out_forward_request_drops_connection_before_reconnect() {
        let peer = TestPeer::start_with_delay(None, Some(Duration::from_millis(50))).await;
        let transport = PeerTransport::new();

        let error = forward(
            &transport,
            &peer.address,
            forwarded_operation(),
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        forward(
            &transport,
            &peer.address,
            forwarded_operation(),
            Duration::from_secs(1),
        )
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
