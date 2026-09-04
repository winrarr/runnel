use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::network::RaftNetworkFactory;
use openraft::{Config, SnapshotPolicy};
use runnel_engine::{AckResult, BrokerError, Offset, PollResult, ReplayMessage};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use super::state_machine_store::validate_state_machine_storage;
use super::{
    GroupKind, METADATA_GROUP_ID, NodeId, Raft, RaftGroup, SnapshotMetricsSnapshot,
    StateMachineStore, StreamLifecycle, StreamMetadata, TypeConfig, atomic_write,
    forward_error_to_broker, log_store, network, path_component, stream_identity, validate_name,
};

// These defaults keep the consensus log bounded while the snapshot format is
// still intentionally simple. The snapshot cadence must be revisited with
// retained-data benchmarks before this backend is used for large streams.
const SNAPSHOT_LOG_THRESHOLD: u64 = 32;
const REPLICATION_LAG_THRESHOLD: u64 = 64;
const SNAPSHOT_LOGS_TO_KEEP: u64 = 4;
// Keep individual peer snapshot RPCs bounded; interrupted transfers restart
// from the beginning in the current in-memory receiver.
const SNAPSHOT_CHUNK_SIZE: u64 = 64 * 1024;

pub(super) async fn build_raft<N>(
    node_id: NodeId,
    cluster_name: String,
    network: N,
) -> Result<(Raft, Arc<StateMachineStore>), BrokerError>
where
    N: RaftNetworkFactory<TypeConfig>,
{
    build_raft_with_storage(
        node_id,
        cluster_name,
        network,
        log_store::LogStore::default(),
        Arc::new(StateMachineStore::default()),
    )
    .await
}

async fn build_raft_with_storage<N>(
    node_id: NodeId,
    cluster_name: String,
    network: N,
    log_store: log_store::LogStore<TypeConfig>,
    state_machine: Arc<StateMachineStore>,
) -> Result<(Raft, Arc<StateMachineStore>), BrokerError>
where
    N: RaftNetworkFactory<TypeConfig>,
{
    let config = Config {
        cluster_name,
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        replication_lag_threshold: REPLICATION_LAG_THRESHOLD,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(SNAPSHOT_LOG_THRESHOLD),
        max_in_snapshot_log_to_keep: SNAPSHOT_LOGS_TO_KEEP,
        snapshot_max_chunk_size: SNAPSHOT_CHUNK_SIZE,
        ..Default::default()
    }
    .validate()
    .map_err(|error| BrokerError::Cluster(error.to_string()))?;
    let raft = Raft::new(
        node_id,
        Arc::new(config),
        network,
        log_store,
        Arc::clone(&state_machine),
    )
    .await
    .map_err(|error| BrokerError::Cluster(error.to_string()))?;
    Ok((raft, state_machine))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DataGroupManifest {
    pub(super) stream: String,
    pub(super) stream_id: String,
    pub(super) group_id: String,
}

pub struct GroupManager {
    node_id: NodeId,
    cluster_name: String,
    data_dir: PathBuf,
    peers: BTreeMap<NodeId, String>,
    ack_timeout: Duration,
    max_delivery_attempts: Option<u32>,
    groups: RwLock<BTreeMap<String, Arc<RaftGroup>>>,
    creation_lock: Mutex<()>,
    peer_transport: Arc<network::PeerTransport>,
}

impl GroupManager {
    pub(super) async fn open(
        node_id: NodeId,
        cluster_name: String,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
        ack_timeout: Duration,
        max_delivery_attempts: Option<u32>,
    ) -> Result<Arc<Self>, BrokerError> {
        let manager = Arc::new(Self {
            node_id,
            cluster_name,
            data_dir: data_dir.as_ref().to_path_buf(),
            peers,
            ack_timeout,
            max_delivery_attempts,
            groups: RwLock::new(BTreeMap::new()),
            creation_lock: Mutex::new(()),
            peer_transport: network::PeerTransport::new(),
        });
        let metadata = manager
            .open_group(
                METADATA_GROUP_ID,
                GroupKind::Metadata,
                manager.group_directory(METADATA_GROUP_ID),
            )
            .await?;
        manager
            .groups
            .write()
            .await
            .insert(METADATA_GROUP_ID.to_owned(), metadata);
        manager.restore_data_groups().await?;
        Ok(manager)
    }

    async fn open_group(
        &self,
        group_id: &str,
        kind: GroupKind,
        directory: PathBuf,
    ) -> Result<Arc<RaftGroup>, BrokerError> {
        fs::create_dir_all(&directory)?;
        let network = network::TcpNetwork::with_transport(
            self.peers.clone(),
            group_id,
            Arc::clone(&self.peer_transport),
        );
        let log_store = log_store::LogStore::open(directory.join("raft-log.json"))
            .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        let state_machine = Arc::new(StateMachineStore::open(
            directory.join("state-machine"),
            kind,
        )?);
        let cluster_name = format!("{}/{}", self.cluster_name, group_id);
        let (raft, state_machine) = build_raft_with_storage(
            self.node_id,
            cluster_name,
            network,
            log_store,
            state_machine,
        )
        .await?;
        Ok(Arc::new(RaftGroup {
            node_id: self.node_id,
            raft,
            state_machine,
            ack_timeout: self.ack_timeout,
            max_delivery_attempts: self.max_delivery_attempts,
        }))
    }

    async fn restore_data_groups(&self) -> Result<(), BrokerError> {
        let directory = self.data_dir.join("groups").join("data");
        if !directory.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                return Err(BrokerError::Cluster(format!(
                    "invalid data-group entry '{}': expected a directory",
                    entry.path().display()
                )));
            }
            let manifest_path = entry.path().join("group.json");
            if !manifest_path.exists() {
                return Err(BrokerError::Cluster(format!(
                    "missing data-group manifest '{}'",
                    manifest_path.display()
                )));
            }
            let manifest: DataGroupManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
            self.ensure_data_group_local(
                &manifest.stream,
                &StreamMetadata {
                    stream_id: manifest.stream_id,
                    group_id: manifest.group_id,
                    lifecycle: StreamLifecycle::Active,
                },
            )
            .await?;
        }
        Ok(())
    }

    fn group_directory(&self, group_id: &str) -> PathBuf {
        self.data_dir.join("groups").join(group_id)
    }

    fn data_group_directory(&self, stream: &str) -> PathBuf {
        self.data_dir
            .join("groups")
            .join("data")
            .join(path_component(stream))
    }

    pub(crate) async fn group(&self, group_id: &str) -> Option<Arc<RaftGroup>> {
        self.groups.read().await.get(group_id).cloned()
    }

    pub(crate) async fn ensure_group_for_id(
        &self,
        group_id: &str,
    ) -> Result<Option<Arc<RaftGroup>>, BrokerError> {
        if let Some(group) = self.group(group_id).await {
            return Ok(Some(group));
        }
        if group_id == METADATA_GROUP_ID {
            return Ok(None);
        }
        let metadata_group = self.metadata_group().await;
        let Some((stream, metadata)) = metadata_group
            .state_machine
            .metadata_by_group_id(group_id)
            .await
        else {
            return Ok(None);
        };
        self.ensure_data_group_local(&stream, &metadata)
            .await
            .map(Some)
    }

    pub(super) async fn metadata_group(&self) -> Arc<RaftGroup> {
        self.groups
            .read()
            .await
            .get(METADATA_GROUP_ID)
            .expect("metadata group must be opened before serving requests")
            .clone()
    }

    pub(crate) fn metadata_group_sync(&self) -> Arc<RaftGroup> {
        self.groups
            .try_read()
            .expect("metadata group lock should not be held")
            .get(METADATA_GROUP_ID)
            .expect("metadata group must be opened")
            .clone()
    }

    pub(crate) fn peer_transport(&self) -> &Arc<network::PeerTransport> {
        &self.peer_transport
    }

    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) async fn ensure_data_group_local(
        &self,
        stream: &str,
        metadata: &StreamMetadata,
    ) -> Result<Arc<RaftGroup>, BrokerError> {
        if let Some(group) = self.group(&metadata.group_id).await {
            return Ok(group);
        }
        let _guard = self.creation_lock.lock().await;
        if let Some(group) = self.group(&metadata.group_id).await {
            return Ok(group);
        }
        let directory = self.data_group_directory(stream);
        let manifest_path = directory.join("group.json");
        if directory.exists() && !directory.is_dir() {
            return Err(BrokerError::Cluster(format!(
                "invalid data-group storage '{}': expected a directory",
                directory.display()
            )));
        }
        if directory.exists() && !manifest_path.exists() {
            let has_entries = fs::read_dir(&directory)?.next().transpose()?.is_some();
            if has_entries {
                return Err(BrokerError::Cluster(format!(
                    "missing data-group manifest '{}'",
                    manifest_path.display()
                )));
            }
        }
        let manifest = DataGroupManifest {
            stream: stream.to_owned(),
            stream_id: metadata.stream_id.clone(),
            group_id: metadata.group_id.clone(),
        };
        if !manifest_path.exists() {
            let bytes = serde_json::to_vec(&manifest)?;
            atomic_write(&manifest_path, &bytes)?;
        }
        let group = self
            .open_group(
                &metadata.group_id,
                GroupKind::Data {
                    stream: stream.to_owned(),
                    stream_id: metadata.stream_id.clone(),
                    group_id: metadata.group_id.clone(),
                },
                directory,
            )
            .await?;
        self.groups
            .write()
            .await
            .insert(metadata.group_id.clone(), group.clone());
        Ok(group)
    }

    pub(crate) async fn data_group_for_stream(
        &self,
        stream: &str,
    ) -> Result<Arc<RaftGroup>, BrokerError> {
        let metadata_group = self.metadata_group().await;
        let (group_stream, metadata) = match metadata_group.stream_metadata(stream).await {
            Ok(metadata) => (stream.to_owned(), metadata),
            Err(BrokerError::StreamNotFound(_)) => metadata_group
                .state_machine
                .dead_letter_source(stream)
                .await
                .ok_or_else(|| BrokerError::StreamNotFound(stream.to_owned()))?,
            Err(error) => return Err(error),
        };
        if metadata.lifecycle != StreamLifecycle::Active {
            return Err(BrokerError::StreamNotReady(stream.to_owned()));
        }
        self.ensure_data_group_local(&group_stream, &metadata).await
    }

    pub(crate) async fn create_stream_local(&self, stream: String) -> Result<bool, BrokerError> {
        let metadata_group = self.metadata_group().await;
        let created = metadata_group.create_stream(stream.clone()).await?;
        let metadata = metadata_group.stream_metadata(&stream).await?;
        self.ensure_data_group_local(&stream, &metadata).await?;
        if metadata.lifecycle == StreamLifecycle::Creating {
            self.reconcile_stream(&stream, &metadata).await?;
        }
        Ok(created)
    }

    async fn reconcile_stream(
        &self,
        stream: &str,
        metadata: &StreamMetadata,
    ) -> Result<(), BrokerError> {
        let metadata_group = self.metadata_group().await;
        let leader_id = metadata_group
            .raft()
            .current_leader()
            .await
            .ok_or_else(|| BrokerError::Cluster("metadata group has no leader".to_owned()))?;
        if leader_id != self.node_id {
            return Err(BrokerError::NotLeader {
                leader_id: Some(leader_id),
            });
        }

        let data_group = self.ensure_data_group_local(stream, metadata).await?;
        for (node_id, address) in &self.peers {
            if *node_id == self.node_id {
                continue;
            }
            network::ensure_data_group(
                &self.peer_transport,
                address,
                stream.to_owned(),
                metadata.stream_id.clone(),
                metadata.group_id.clone(),
                Duration::from_secs(2),
            )
            .await
            .map_err(|error| {
                BrokerError::Cluster(format!(
                    "could not prepare data group on node {node_id}: {error}"
                ))
            })?;
        }

        if !data_group.is_initialized().await? {
            let members = self
                .peers
                .iter()
                .map(|(id, address)| (*id, BasicNode::new(address)))
                .collect::<BTreeMap<_, _>>();
            data_group
                .raft()
                .initialize(members)
                .await
                .map_err(|error| BrokerError::Cluster(error.to_string()))?;
            data_group
                .raft()
                .wait(Some(Duration::from_secs(2)))
                .current_leader(self.node_id, "data-group election")
                .await
                .map_err(|error| BrokerError::Cluster(error.to_string()))?;
        }

        if data_group.stream_metadata(stream).await.is_err() {
            self.initialize_data_stream(stream, metadata, data_group)
                .await?;
        }
        if metadata.lifecycle == StreamLifecycle::Creating {
            metadata_group.activate_stream(stream.to_owned()).await?;
        }
        Ok(())
    }

    async fn initialize_data_stream(
        &self,
        stream: &str,
        metadata: &StreamMetadata,
        data_group: Arc<RaftGroup>,
    ) -> Result<(), BrokerError> {
        match data_group
            .initialize_data_stream(
                stream.to_owned(),
                metadata.stream_id.clone(),
                metadata.group_id.clone(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(BrokerError::NotLeader { leader_id }) => {
                let target = leader_id
                    .or(data_group.raft().current_leader().await)
                    .ok_or_else(|| BrokerError::Cluster("data group has no leader".to_owned()))?;
                let address = self.peers.get(&target).ok_or_else(|| {
                    BrokerError::Cluster(format!("data leader node {target} has no address"))
                })?;
                let response = network::forward(
                    &self.peer_transport,
                    address,
                    network::ForwardedOperation::InitializeDataStream {
                        stream: stream.to_owned(),
                        stream_id: metadata.stream_id.clone(),
                        group_id: metadata.group_id.clone(),
                    },
                    Duration::from_secs(2),
                )
                .await
                .map_err(|error| BrokerError::Cluster(error.to_string()))?;
                match response {
                    network::ForwardedResponse::InitializeDataStream(result) => {
                        result.map(|_| ()).map_err(forward_error_to_broker)
                    }
                    _ => Err(BrokerError::Cluster(
                        "data leader returned the wrong initialization response".to_owned(),
                    )),
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn initialize_data_stream_local(
        &self,
        stream: String,
        stream_id: String,
        group_id: String,
    ) -> Result<bool, BrokerError> {
        let group = self
            .ensure_data_group_local(
                &stream,
                &StreamMetadata {
                    stream_id: stream_id.clone(),
                    group_id: group_id.clone(),
                    lifecycle: StreamLifecycle::Creating,
                },
            )
            .await?;
        group
            .initialize_data_stream(stream, stream_id, group_id)
            .await
    }

    pub(crate) async fn publish_local(
        &self,
        stream: String,
        key: Option<String>,
        payload: Vec<u8>,
        published_at_ms: u64,
        request_id: Option<String>,
    ) -> Result<Offset, BrokerError> {
        self.data_group_for_stream(&stream)
            .await?
            .publish(stream, key, payload, published_at_ms, request_id)
            .await
    }

    pub(crate) async fn poll_local(
        &self,
        stream: &str,
        consumer: &str,
    ) -> Result<PollResult, BrokerError> {
        self.data_group_for_stream(stream)
            .await?
            .poll_group(stream.to_owned(), consumer.to_owned(), consumer.to_owned())
            .await
    }

    pub(crate) async fn replay_local(
        &self,
        stream: &str,
        consumer: &str,
        offset: Offset,
    ) -> Result<ReplayMessage, BrokerError> {
        validate_name("stream", stream)?;
        validate_name("consumer", consumer)?;
        self.data_group_for_stream(stream)
            .await?
            .replay(stream.to_owned(), consumer.to_owned(), offset)
            .await
    }

    pub(crate) async fn ack_local(
        &self,
        stream: String,
        consumer: String,
        offset: Offset,
    ) -> Result<AckResult, BrokerError> {
        self.data_group_for_stream(&stream)
            .await?
            .ack_group(stream, consumer.clone(), consumer, offset, String::new())
            .await
    }

    pub(crate) async fn poll_group_local(
        &self,
        stream: &str,
        consumer: &str,
        member: &str,
    ) -> Result<PollResult, BrokerError> {
        self.data_group_for_stream(stream)
            .await?
            .poll_group(stream.to_owned(), consumer.to_owned(), member.to_owned())
            .await
    }

    pub(crate) async fn ack_group_local(
        &self,
        stream: String,
        consumer: String,
        member: String,
        offset: Offset,
        delivery_token: String,
    ) -> Result<AckResult, BrokerError> {
        self.data_group_for_stream(&stream)
            .await?
            .ack_group(stream, consumer, member, offset, delivery_token)
            .await
    }

    pub(super) async fn health(&self) -> runnel_engine::HealthSnapshot {
        let metadata_health = self.metadata_group().await.health().await;
        let groups = self
            .groups
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut storage_bytes = 0;
        let mut in_flight_deliveries = 0;
        let mut redeliveries = metadata_health.redeliveries;
        let mut dead_letters = metadata_health.dead_letters;
        for group in groups {
            let health = group.health().await;
            storage_bytes += health.storage_bytes;
            in_flight_deliveries += health.in_flight_deliveries;
            redeliveries += health.redeliveries;
            dead_letters += health.dead_letters;
        }
        runnel_engine::HealthSnapshot {
            streams: metadata_health.streams,
            storage_bytes,
            in_flight_deliveries,
            redeliveries,
            dead_letters,
        }
    }

    pub async fn snapshot_metrics(&self) -> SnapshotMetricsSnapshot {
        let groups = self
            .groups
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        groups
            .iter()
            .map(|group| group.state_machine.snapshot_metrics())
            .fold(SnapshotMetricsSnapshot::default(), add_snapshot_metrics)
    }
}

fn add_snapshot_metrics(
    left: SnapshotMetricsSnapshot,
    right: SnapshotMetricsSnapshot,
) -> SnapshotMetricsSnapshot {
    SnapshotMetricsSnapshot {
        builds_started: left.builds_started + right.builds_started,
        builds_completed: left.builds_completed + right.builds_completed,
        build_failures: left.build_failures + right.build_failures,
        installs_started: left.installs_started + right.installs_started,
        installs_completed: left.installs_completed + right.installs_completed,
        install_failures: left.install_failures + right.install_failures,
        install_bytes: left.install_bytes + right.install_bytes,
        installs_in_progress: left.installs_in_progress + right.installs_in_progress,
        transfer_chunks: left.transfer_chunks + right.transfer_chunks,
        transfer_final_chunks: left.transfer_final_chunks + right.transfer_final_chunks,
        transfer_bytes: left.transfer_bytes + right.transfer_bytes,
    }
}

fn validate_persisted_group_storage(directory: &Path) -> Result<(), BrokerError> {
    if !directory.exists() {
        return Ok(());
    }
    if !directory.is_dir() {
        return Err(BrokerError::Cluster(format!(
            "invalid clustered group storage '{}': expected a directory",
            directory.display()
        )));
    }
    log_store::LogStore::<TypeConfig>::validate(directory.join("raft-log.json"))
        .map_err(|error| BrokerError::Cluster(error.to_string()))?;
    validate_state_machine_storage(&directory.join("state-machine"))
}

pub(super) fn validate_persisted_cluster_storage(data_dir: &Path) -> Result<(), BrokerError> {
    let groups_directory = data_dir.join("groups");
    if !groups_directory.exists() {
        return Ok(());
    }
    if !groups_directory.is_dir() {
        return Err(BrokerError::Cluster(format!(
            "invalid clustered storage '{}': expected a groups directory",
            groups_directory.display()
        )));
    }
    let metadata_directory = groups_directory.join(METADATA_GROUP_ID);
    if !metadata_directory.exists() {
        return Err(BrokerError::Cluster(format!(
            "missing metadata group storage '{}'; refusing to open a partial clustered layout",
            metadata_directory.display()
        )));
    }
    validate_persisted_group_storage(&metadata_directory)?;

    let data_groups_directory = groups_directory.join("data");
    if !data_groups_directory.exists() {
        return Ok(());
    }
    if !data_groups_directory.is_dir() {
        return Err(BrokerError::Cluster(format!(
            "invalid clustered storage '{}': expected a data-groups directory",
            data_groups_directory.display()
        )));
    }
    for entry in fs::read_dir(&data_groups_directory)? {
        let entry = entry?;
        let entry_path = entry.path();
        if !entry.file_type()?.is_dir() {
            return Err(BrokerError::Cluster(format!(
                "invalid data-group entry '{}': expected a directory",
                entry_path.display()
            )));
        }
        let manifest_path = entry_path.join("group.json");
        if !manifest_path.exists() {
            return Err(BrokerError::Cluster(format!(
                "missing data-group manifest '{}'",
                manifest_path.display()
            )));
        }
        let manifest: DataGroupManifest = serde_json::from_slice(&fs::read(&manifest_path)?)
            .map_err(|error| {
                BrokerError::Cluster(format!(
                    "invalid data-group manifest '{}': {error}",
                    manifest_path.display()
                ))
            })?;
        let expected_directory = data_groups_directory.join(path_component(&manifest.stream));
        if entry_path != expected_directory {
            return Err(BrokerError::Cluster(format!(
                "data-group manifest '{}' does not match its directory '{}'",
                manifest_path.display(),
                entry_path.display()
            )));
        }
        let (expected_stream_id, expected_group_id) = stream_identity(&manifest.stream);
        if manifest.stream_id != expected_stream_id || manifest.group_id != expected_group_id {
            return Err(BrokerError::Cluster(format!(
                "data-group manifest '{}' has incompatible stream/group identity",
                manifest_path.display()
            )));
        }
        validate_persisted_group_storage(&entry_path)?;
    }
    Ok(())
}
