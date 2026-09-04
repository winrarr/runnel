use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openraft::BasicNode;
use openraft::error::{ClientWriteError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_engine::{
    AckResult, BrokerError, Engine, EngineFuture, Offset, PollResult, ReplayMessage,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::forwarding::ClientForwarder;
use super::group_manager;
use super::state_machine::{Command, CommandResponse, StreamMetadata, stream_identity};
use super::state_machine_store::StateMachineStore;
use super::{GroupManager, NodeId, Raft, TypeConfig, validate_name};

pub(super) const DEFAULT_RAFT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const STORAGE_METADATA_FORMAT_VERSION: u32 = 1;
pub(super) const STORAGE_METADATA_FILE: &str = "storage.json";
const LEGACY_SINGLE_GROUP_PATHS: &[&str] = &[
    "raft-log.json",
    "state-machine",
    "state-machine.json",
    "snapshot.json",
    "state-machine.log",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PersistedStorageMetadata {
    pub(super) version: u32,
    pub(super) cluster_name: String,
    pub(super) node_id: NodeId,
}

fn ensure_storage_metadata(
    data_dir: &Path,
    cluster_name: &str,
    node_id: NodeId,
) -> Result<(), BrokerError> {
    let metadata_path = data_dir.join(STORAGE_METADATA_FILE);
    if metadata_path.exists() {
        let bytes = fs::read(&metadata_path).map_err(|error| {
            BrokerError::Cluster(format!(
                "could not read persisted storage metadata '{}': {error}",
                metadata_path.display()
            ))
        })?;
        let metadata: PersistedStorageMetadata =
            serde_json::from_slice(&bytes).map_err(|error| {
                BrokerError::Cluster(format!(
                    "invalid persisted storage metadata '{}': {error}",
                    metadata_path.display()
                ))
            })?;
        if metadata.version != STORAGE_METADATA_FORMAT_VERSION {
            return Err(BrokerError::Cluster(format!(
                "unsupported storage metadata format version {} in '{}' (supported version {})",
                metadata.version,
                metadata_path.display(),
                STORAGE_METADATA_FORMAT_VERSION
            )));
        }
        if metadata.cluster_name != cluster_name {
            return Err(BrokerError::Cluster(format!(
                "cluster identity mismatch: storage belongs to cluster '{}', configured cluster is '{}'",
                metadata.cluster_name, cluster_name
            )));
        }
        if metadata.node_id != node_id {
            return Err(BrokerError::Cluster(format!(
                "node identity mismatch: storage belongs to node {}, configured node is {}",
                metadata.node_id, node_id
            )));
        }
        return Ok(());
    }

    if data_dir.join("groups").exists() {
        return Err(BrokerError::Cluster(
            "persisted clustered storage is missing storage metadata; refusing to open it because its cluster identity cannot be verified"
                .to_owned(),
        ));
    }

    let metadata = PersistedStorageMetadata {
        version: STORAGE_METADATA_FORMAT_VERSION,
        cluster_name: cluster_name.to_owned(),
        node_id,
    };
    let bytes = serde_json::to_vec(&metadata).map_err(|error| {
        BrokerError::Cluster(format!(
            "could not encode storage metadata '{}': {error}",
            metadata_path.display()
        ))
    })?;
    super::atomic_write(&metadata_path, &bytes).map_err(|error| {
        BrokerError::Cluster(format!(
            "could not persist storage metadata '{}': {error}",
            metadata_path.display()
        ))
    })?;
    Ok(())
}

fn fatal_error(error: openraft::error::Fatal<NodeId>) -> BrokerError {
    BrokerError::Cluster(error.to_string())
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[derive(Clone)]
struct UnreachableNetwork;

impl RaftNetworkFactory<TypeConfig> for UnreachableNetwork {
    type Network = UnreachableConnection;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        UnreachableConnection
    }
}

struct UnreachableConnection;

fn unreachable<T>() -> Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
    Err(RPCError::Unreachable(Unreachable::new(
        &std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "in-process network is not configured",
        ),
    )))
}

fn unreachable_snapshot<T>()
-> Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>>
{
    Err(RPCError::Unreachable(Unreachable::new(
        &std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "in-process network is not configured",
        ),
    )))
}

impl RaftNetwork<TypeConfig> for UnreachableConnection {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        unreachable()
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        unreachable_snapshot()
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        unreachable()
    }
}

#[derive(Clone)]
struct InMemoryNetwork {
    peers: Arc<RwLock<BTreeMap<NodeId, Raft>>>,
}

impl RaftNetworkFactory<TypeConfig> for InMemoryNetwork {
    type Network = InMemoryConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        InMemoryConnection {
            target,
            peers: Arc::clone(&self.peers),
        }
    }
}

struct InMemoryConnection {
    target: NodeId,
    peers: Arc<RwLock<BTreeMap<NodeId, Raft>>>,
}

impl InMemoryConnection {
    async fn target(&self) -> Result<Raft, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.peers
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!("node {} is not registered", self.target),
                )))
            })
    }
}

impl RaftNetwork<TypeConfig> for InMemoryConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let target = self.target().await?;
        target
            .append_entries(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let target = self
            .peers
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!("node {} is not registered", self.target),
                )))
            })?;
        target
            .install_snapshot(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let target = self.target().await?;
        target
            .vote(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }
}

pub struct InMemoryCluster {
    nodes: BTreeMap<NodeId, Arc<RaftGroup>>,
}

impl InMemoryCluster {
    pub async fn new<I>(node_ids: I) -> Result<Self, BrokerError>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let ids = node_ids.into_iter().collect::<Vec<_>>();
        if ids.is_empty() {
            return Err(BrokerError::Cluster(
                "cluster must contain at least one node".to_owned(),
            ));
        }

        let peers = Arc::new(RwLock::new(BTreeMap::new()));
        let mut nodes = BTreeMap::new();
        for node_id in ids.iter().copied() {
            let network = InMemoryNetwork {
                peers: Arc::clone(&peers),
            };
            let (raft, state_machine) =
                group_manager::build_raft(node_id, "runnel-in-memory-cluster".to_owned(), network)
                    .await?;
            peers.write().await.insert(node_id, raft.clone());
            nodes.insert(
                node_id,
                Arc::new(RaftGroup {
                    node_id,
                    raft,
                    state_machine,
                    ack_timeout: DEFAULT_RAFT_ACK_TIMEOUT,
                    max_delivery_attempts: None,
                }),
            );
        }

        let members = nodes
            .keys()
            .copied()
            .map(|node_id| (node_id, BasicNode::new(format!("in-memory-{node_id}"))))
            .collect::<BTreeMap<_, _>>();
        let first = nodes.values().next().expect("non-empty cluster").clone();
        first
            .raft
            .initialize(members)
            .await
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        first
            .raft
            .wait(Some(Duration::from_secs(2)))
            .current_leader(
                *nodes.keys().next().expect("non-empty cluster"),
                "cluster election",
            )
            .await
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;

        Ok(Self { nodes })
    }

    pub fn node(&self, node_id: NodeId) -> Option<Arc<RaftGroup>> {
        self.nodes.get(&node_id).cloned()
    }

    pub async fn leader(&self) -> Result<Arc<RaftGroup>, BrokerError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            for node in self.nodes.values() {
                if let Some(leader_id) = node.raft.current_leader().await
                    && self.nodes.contains_key(&leader_id)
                {
                    return Ok(self
                        .nodes
                        .get(&leader_id)
                        .expect("leader is registered")
                        .clone());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BrokerError::Cluster("cluster has no leader".to_owned()));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub struct RaftGroup {
    pub(super) node_id: NodeId,
    pub(super) raft: Raft,
    pub(super) state_machine: Arc<StateMachineStore>,
    pub(super) ack_timeout: Duration,
    pub(super) max_delivery_attempts: Option<u32>,
}

fn map_client_write_error(
    error: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
) -> BrokerError {
    if let Some(forward) = error.forward_to_leader::<BasicNode>() {
        return BrokerError::NotLeader {
            leader_id: forward.leader_id,
        };
    }
    BrokerError::Cluster(error.to_string())
}

impl RaftGroup {
    pub async fn new_single(node_id: NodeId) -> Result<Self, BrokerError> {
        let (raft, state_machine) = group_manager::build_raft(
            node_id,
            format!("runnel-single-{node_id}"),
            UnreachableNetwork,
        )
        .await?;
        raft.initialize(BTreeMap::from([(node_id, BasicNode::new("in-process"))]))
            .await
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        raft.wait(Some(Duration::from_secs(2)))
            .current_leader(node_id, "single-node election")
            .await
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        Ok(Self {
            node_id,
            raft,
            state_machine,
            ack_timeout: DEFAULT_RAFT_ACK_TIMEOUT,
            max_delivery_attempts: None,
        })
    }

    pub async fn create_stream(&self, stream: String) -> Result<bool, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.create_stream");
        let (stream_id, group_id) = stream_identity(&stream);
        let response = self
            .raft
            .client_write(Command::CreateStream {
                stream,
                stream_id: Some(stream_id),
                group_id: Some(group_id),
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::StreamCreated { created } => Ok(created),
            other => Err(BrokerError::Cluster(format!(
                "unexpected stream response: {other:?}"
            ))),
        }
    }

    pub(super) async fn initialize_data_stream(
        &self,
        stream: String,
        stream_id: String,
        group_id: String,
    ) -> Result<bool, BrokerError> {
        let response = self
            .raft
            .client_write(Command::InitializeDataStream {
                stream,
                stream_id,
                group_id,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::DataStreamInitialized { initialized } => Ok(initialized),
            other => Err(BrokerError::Cluster(format!(
                "unexpected data-stream response: {other:?}"
            ))),
        }
    }

    pub(super) async fn activate_stream(&self, stream: String) -> Result<bool, BrokerError> {
        let response = self
            .raft
            .client_write(Command::ActivateStream { stream })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::StreamActivated { activated } => Ok(activated),
            other => Err(BrokerError::Cluster(format!(
                "unexpected stream-activation response: {other:?}"
            ))),
        }
    }

    pub async fn publish(
        &self,
        stream: String,
        key: Option<String>,
        payload: Vec<u8>,
        published_at_ms: u64,
        request_id: Option<String>,
    ) -> Result<Offset, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.publish_quorum");
        let stream_name = stream.clone();
        let response = self
            .raft
            .client_write(Command::Publish {
                stream,
                key,
                payload,
                published_at_ms,
                request_id,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::Published { offset } => Ok(offset),
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected publish response: {other:?}"
            ))),
        }
    }

    pub async fn poll(&self, stream: &str, consumer: &str) -> Result<PollResult, BrokerError> {
        self.poll_group(stream.to_owned(), consumer.to_owned(), consumer.to_owned())
            .await
    }

    pub async fn replay(
        &self,
        stream: String,
        consumer: String,
        offset: Offset,
    ) -> Result<ReplayMessage, BrokerError> {
        validate_name("stream", &stream)?;
        validate_name("consumer", &consumer)?;
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.replay_quorum");
        let stream_name = stream.clone();
        let response = self
            .raft
            .client_write(Command::Replay {
                stream,
                consumer,
                offset,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::Replay { result } => Ok(result),
            CommandResponse::HistoryUnavailable {
                requested_offset,
                earliest_offset,
                next_offset,
            } => Err(BrokerError::HistoryUnavailable {
                stream: stream_name,
                requested_offset,
                earliest_offset,
                next_offset,
            }),
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected replay response: {other:?}"
            ))),
        }
    }

    pub async fn stream_metadata(&self, stream: &str) -> Result<StreamMetadata, BrokerError> {
        self.state_machine.metadata(stream).await
    }

    pub async fn ack(
        &self,
        stream: String,
        consumer: String,
        offset: Offset,
    ) -> Result<AckResult, BrokerError> {
        self.ack_group(stream, consumer.clone(), consumer, offset, String::new())
            .await
    }

    pub async fn poll_group(
        &self,
        stream: String,
        consumer: String,
        member: String,
    ) -> Result<PollResult, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.poll_quorum");
        let now_ms = now_ms();
        let lease_deadline_ms = now_ms.saturating_add(duration_ms(self.ack_timeout));
        let stream_name = stream.clone();
        let response = self
            .raft
            .client_write(Command::PollGroup {
                stream,
                consumer,
                member,
                now_ms,
                lease_deadline_ms,
                max_delivery_attempts: self.max_delivery_attempts,
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::GroupPoll { result } => Ok(result),
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected grouped poll response: {other:?}"
            ))),
        }
    }

    pub async fn ack_group(
        &self,
        stream: String,
        consumer: String,
        member: String,
        offset: Offset,
        delivery_token: String,
    ) -> Result<AckResult, BrokerError> {
        #[cfg(feature = "instrumentation")]
        let _stage_timer = StageTimer::new("raft.ack_quorum");
        let stream_name = stream.clone();
        let consumer_name = consumer.clone();
        let response = self
            .raft
            .client_write(Command::AckGroup {
                stream,
                consumer,
                member,
                offset,
                delivery_token,
                now_ms: now_ms(),
            })
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            CommandResponse::GroupAcknowledged => Ok(AckResult::Acknowledged),
            CommandResponse::GroupAlreadyAcknowledged => Ok(AckResult::AlreadyAcknowledged),
            CommandResponse::GroupAckNotInFlight { consumer, offset } => {
                Err(BrokerError::AckNotInFlight { consumer, offset })
            }
            CommandResponse::GroupStaleDelivery { consumer, offset } => {
                Err(BrokerError::StaleDelivery { consumer, offset })
            }
            CommandResponse::StreamNotFound => Err(BrokerError::StreamNotFound(stream_name)),
            other => Err(BrokerError::Cluster(format!(
                "unexpected grouped acknowledgement response for consumer '{consumer_name}': {other:?}"
            ))),
        }
    }

    pub async fn health(&self) -> runnel_engine::HealthSnapshot {
        self.state_machine.health().await
    }

    pub fn raft(&self) -> Raft {
        self.raft.clone()
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) fn record_snapshot_chunk(&self, bytes: u64, final_chunk: bool) {
        self.state_machine.record_snapshot_chunk(bytes, final_chunk);
    }

    pub async fn trigger_snapshot(&self) -> Result<(), BrokerError> {
        self.raft.trigger().snapshot().await.map_err(fatal_error)
    }

    pub(super) async fn is_initialized(&self) -> Result<bool, BrokerError> {
        self.raft.is_initialized().await.map_err(fatal_error)
    }
}

pub struct SingleNodeEngine {
    group: Arc<RaftGroup>,
}

impl SingleNodeEngine {
    pub async fn new(node_id: NodeId) -> Result<Self, BrokerError> {
        Ok(Self {
            group: Arc::new(RaftGroup::new_single(node_id).await?),
        })
    }
}

impl Engine for SingleNodeEngine {
    fn create_stream<'a>(&'a self, stream: &'a str) -> EngineFuture<'a, bool> {
        Box::pin(async move { self.group.create_stream(stream.to_owned()).await })
    }

    fn publish<'a>(
        &'a self,
        stream: &'a str,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
    ) -> EngineFuture<'a, Offset> {
        Box::pin(async move {
            self.group
                .publish(stream.to_owned(), key, payload, now_ms(), request_id)
                .await
        })
    }

    fn poll<'a>(&'a self, stream: &'a str, consumer: &'a str) -> EngineFuture<'a, PollResult> {
        Box::pin(async move { self.group.poll(stream, consumer).await })
    }

    fn replay<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, ReplayMessage> {
        Box::pin(async move {
            self.group
                .replay(stream.to_owned(), consumer.to_owned(), offset)
                .await
        })
    }

    fn poll_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
    ) -> EngineFuture<'a, PollResult> {
        Box::pin(async move {
            self.group
                .poll_group(stream.to_owned(), consumer.to_owned(), member.to_owned())
                .await
        })
    }

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move {
            self.group
                .ack(stream.to_owned(), consumer.to_owned(), offset)
                .await
        })
    }

    fn ack_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        offset: Offset,
        delivery_token: &'a str,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move {
            self.group
                .ack_group(
                    stream.to_owned(),
                    consumer.to_owned(),
                    member.to_owned(),
                    offset,
                    delivery_token.to_owned(),
                )
                .await
        })
    }

    fn health<'a>(&'a self) -> EngineFuture<'a, runnel_engine::HealthSnapshot> {
        Box::pin(async move { Ok(self.group.health().await) })
    }
}

pub struct PersistentEngine {
    pub(super) manager: Arc<GroupManager>,
    pub(super) node_id: NodeId,
    pub(super) peers: BTreeMap<NodeId, String>,
}

impl PersistentEngine {
    pub async fn open(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<std::path::Path>,
        peers: BTreeMap<NodeId, String>,
        bootstrap: bool,
    ) -> Result<Self, BrokerError> {
        Self::open_with_ack_timeout(
            node_id,
            cluster_name,
            data_dir,
            peers,
            bootstrap,
            DEFAULT_RAFT_ACK_TIMEOUT,
        )
        .await
    }

    pub async fn open_with_ack_timeout(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<std::path::Path>,
        peers: BTreeMap<NodeId, String>,
        bootstrap: bool,
        ack_timeout: Duration,
    ) -> Result<Self, BrokerError> {
        Self::open_with_config(
            node_id,
            cluster_name,
            data_dir,
            peers,
            bootstrap,
            ack_timeout,
            None,
        )
        .await
    }

    pub async fn open_with_config(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<std::path::Path>,
        peers: BTreeMap<NodeId, String>,
        bootstrap: bool,
        ack_timeout: Duration,
        max_delivery_attempts: Option<u32>,
    ) -> Result<Self, BrokerError> {
        if max_delivery_attempts == Some(0) {
            return Err(BrokerError::Configuration(
                "max delivery attempts must be greater than zero".to_owned(),
            ));
        }
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir)?;
        let legacy_paths = LEGACY_SINGLE_GROUP_PATHS
            .iter()
            .map(|relative| data_dir.join(relative))
            .filter(|path| path.exists())
            .collect::<Vec<_>>();
        if !legacy_paths.is_empty() {
            return Err(BrokerError::Cluster(format!(
                "legacy single-group storage detected at {}; migrate the data directory before starting this clustered layout",
                legacy_paths
                    .iter()
                    .map(|path| format!("'{}'", path.display()))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        ensure_storage_metadata(data_dir, &cluster_name, node_id)?;
        group_manager::validate_persisted_cluster_storage(data_dir)?;
        let manager = GroupManager::open(
            node_id,
            cluster_name,
            data_dir,
            peers.clone(),
            ack_timeout,
            max_delivery_attempts,
        )
        .await?;
        let metadata_group = manager.metadata_group().await;

        if bootstrap && !metadata_group.is_initialized().await? {
            let members = peers
                .keys()
                .copied()
                .map(|id| {
                    let address = peers.get(&id).expect("peer map key must have an address");
                    (id, BasicNode::new(address))
                })
                .collect::<BTreeMap<_, _>>();
            metadata_group
                .raft()
                .initialize(members)
                .await
                .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        }

        Ok(Self {
            manager,
            node_id,
            peers,
        })
    }

    pub fn raft(&self) -> Raft {
        self.manager.metadata_group_sync().raft()
    }

    pub fn group(&self) -> Arc<RaftGroup> {
        self.manager.metadata_group_sync()
    }

    pub fn manager(&self) -> Arc<GroupManager> {
        Arc::clone(&self.manager)
    }

    fn forwarder(&self) -> ClientForwarder<'_> {
        ClientForwarder::new(&self.manager, self.node_id, &self.peers)
    }
}

impl Engine for PersistentEngine {
    fn create_stream<'a>(&'a self, stream: &'a str) -> EngineFuture<'a, bool> {
        Box::pin(async move {
            let stream_name = stream.to_owned();
            match self.manager.create_stream_local(stream_name.clone()).await {
                Ok(created) => Ok(created),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forwarder().create_stream(stream_name, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn publish<'a>(
        &'a self,
        stream: &'a str,
        key: Option<String>,
        payload: Vec<u8>,
        request_id: Option<String>,
    ) -> EngineFuture<'a, Offset> {
        Box::pin(async move {
            let stream_name = stream.to_owned();
            let published_at_ms = now_ms();
            let operation = super::network::ForwardedOperation::Publish {
                stream: stream_name.clone(),
                key: key.clone(),
                payload: payload.clone(),
                request_id: request_id.clone(),
                published_at_ms,
            };
            match self
                .manager
                .publish_local(stream_name, key, payload, published_at_ms, request_id)
                .await
            {
                Ok(offset) => Ok(offset),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forwarder().publish(operation, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn poll<'a>(&'a self, stream: &'a str, consumer: &'a str) -> EngineFuture<'a, PollResult> {
        Box::pin(async move {
            let data_group = self.manager.data_group_for_stream(stream).await?;
            let Some(leader_id) = data_group.raft().current_leader().await else {
                return Err(BrokerError::NotLeader { leader_id: None });
            };
            if leader_id != self.node_id {
                return self
                    .forwarder()
                    .poll(stream.to_owned(), consumer.to_owned(), Some(leader_id))
                    .await;
            }
            self.manager.poll_local(stream, consumer).await
        })
    }

    fn replay<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, ReplayMessage> {
        Box::pin(async move {
            validate_name("stream", stream)?;
            validate_name("consumer", consumer)?;
            let operation = super::network::ForwardedOperation::Replay {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                offset,
            };
            match self.manager.replay_local(stream, consumer, offset).await {
                Ok(message) => Ok(message),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forwarder().replay(operation, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn poll_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
    ) -> EngineFuture<'a, PollResult> {
        Box::pin(async move {
            let data_group = self.manager.data_group_for_stream(stream).await?;
            let Some(leader_id) = data_group.raft().current_leader().await else {
                return Err(BrokerError::NotLeader { leader_id: None });
            };
            if leader_id != self.node_id {
                return self
                    .forwarder()
                    .poll_group(
                        stream.to_owned(),
                        consumer.to_owned(),
                        member.to_owned(),
                        Some(leader_id),
                    )
                    .await;
            }
            self.manager
                .poll_group_local(stream, consumer, member)
                .await
        })
    }

    fn ack<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        offset: Offset,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move {
            let operation = super::network::ForwardedOperation::Ack {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                offset,
            };
            match self
                .manager
                .ack_local(stream.to_owned(), consumer.to_owned(), offset)
                .await
            {
                Ok(result) => Ok(result),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forwarder().ack(operation, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn ack_group<'a>(
        &'a self,
        stream: &'a str,
        consumer: &'a str,
        member: &'a str,
        offset: Offset,
        delivery_token: &'a str,
    ) -> EngineFuture<'a, AckResult> {
        Box::pin(async move {
            let operation = super::network::ForwardedOperation::AckGroup {
                stream: stream.to_owned(),
                consumer: consumer.to_owned(),
                member: member.to_owned(),
                offset,
                delivery_token: delivery_token.to_owned(),
            };
            match self
                .manager
                .ack_group_local(
                    stream.to_owned(),
                    consumer.to_owned(),
                    member.to_owned(),
                    offset,
                    delivery_token.to_owned(),
                )
                .await
            {
                Ok(result) => Ok(result),
                Err(BrokerError::NotLeader { leader_id }) => {
                    self.forwarder().ack_group(operation, leader_id).await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn health<'a>(&'a self) -> EngineFuture<'a, runnel_engine::HealthSnapshot> {
        Box::pin(async move {
            if !self.manager.metadata_group().await.is_initialized().await? {
                return Err(BrokerError::Cluster(
                    "cluster is not initialized".to_owned(),
                ));
            }
            if self
                .manager
                .metadata_group()
                .await
                .raft()
                .current_leader()
                .await
                .is_none()
            {
                return Err(BrokerError::Cluster(
                    "cluster has no elected leader".to_owned(),
                ));
            }
            Ok(self.manager.health().await)
        })
    }
}
