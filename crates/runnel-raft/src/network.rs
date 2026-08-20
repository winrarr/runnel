use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

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
use tokio::sync::watch;

use crate::{GroupManager, METADATA_GROUP_ID, StreamMetadata, TypeConfig};
use runnel_engine::{AckResult, BrokerError, Offset, PollResult};

const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

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
        }
    }
}

pub struct TcpConnection {
    target: u64,
    address: Option<String>,
    group_id: String,
}

impl TcpConnection {
    async fn request<Req, Res>(&self, request: Req, ttl: Duration) -> Result<Res, io::Error>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let address = self.address.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("node {} has no valid peer address", self.target),
            )
        })?;
        tokio::time::timeout(ttl, async move {
            let mut stream = TcpStream::connect(address).await?;
            stream.set_nodelay(true)?;
            write_frame(&mut stream, &request).await?;
            read_frame(&mut stream).await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer RPC timed out"))?
    }
}

pub(crate) async fn forward(
    address: &str,
    operation: ForwardedOperation,
    timeout: Duration,
) -> Result<ForwardedResponse, io::Error> {
    let connection = TcpConnection {
        target: 0,
        address: Some(address.to_owned()),
        group_id: METADATA_GROUP_ID.to_owned(),
    };
    let response = connection
        .request(PeerRequest::Forward(operation), timeout)
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
        let response: PeerResponse = self
            .request(
                PeerRequest::AppendEntries {
                    group_id: self.group_id.clone(),
                    request: rpc,
                },
                option.hard_ttl(),
            )
            .await
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
        let response: PeerResponse = self
            .request(
                PeerRequest::Vote {
                    group_id: self.group_id.clone(),
                    request: rpc,
                },
                option.hard_ttl(),
            )
            .await
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
    InitializeDataStream(Result<bool, ForwardError>),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ForwardError {
    NotLeader { leader_id: Option<u64> },
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
    let request: PeerRequest = read_frame(&mut stream).await?;
    let response = match request {
        PeerRequest::AppendEntries { group_id, request } => {
            let Some(group) = resolve_group(&manager, &group_id).await? else {
                return write_frame(
                    &mut stream,
                    &PeerResponse::Error(format!("unknown Raft group '{group_id}'")),
                )
                .await;
            };
            match group.raft().append_entries(request).await {
                Ok(response) => PeerResponse::AppendEntries(response),
                Err(error) => PeerResponse::Error(error.to_string()),
            }
        }
        PeerRequest::InstallSnapshot { group_id, request } => {
            let Some(group) = resolve_group(&manager, &group_id).await? else {
                return write_frame(
                    &mut stream,
                    &PeerResponse::Error(format!("unknown Raft group '{group_id}'")),
                )
                .await;
            };
            group.record_snapshot_chunk(request.data.len() as u64, request.done);
            match group.raft().install_snapshot(request).await {
                Ok(response) => PeerResponse::InstallSnapshot(response),
                Err(error) => PeerResponse::Error(error.to_string()),
            }
        }
        PeerRequest::Vote { group_id, request } => {
            let Some(group) = resolve_group(&manager, &group_id).await? else {
                return write_frame(
                    &mut stream,
                    &PeerResponse::Error(format!("unknown Raft group '{group_id}'")),
                )
                .await;
            };
            match group.raft().vote(request).await {
                Ok(response) => PeerResponse::Vote(response),
                Err(error) => PeerResponse::Error(error.to_string()),
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
    write_frame(&mut stream, &response).await
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
    let connection = TcpConnection {
        target: 0,
        address: Some(address.to_owned()),
        group_id: METADATA_GROUP_ID.to_owned(),
    };
    match connection
        .request(
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
        error => ForwardError::Message(error.to_string()),
    }
}

async fn write_frame<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<(), io::Error> {
    let payload = serde_json::to_vec(value).map_err(io::Error::other)?;
    let length = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "peer RPC is too large"))?;
    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the frame limit",
        ));
    }
    stream.write_u32(length).await?;
    stream.write_all(&payload).await?;
    stream.flush().await
}

async fn read_frame<T: DeserializeOwned>(stream: &mut TcpStream) -> Result<T, io::Error> {
    let length = stream.read_u32().await?;
    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer RPC exceeds the frame limit",
        ));
    }
    let mut payload = vec![0; length as usize];
    stream.read_exact(&mut payload).await?;
    serde_json::from_slice(&payload).map_err(io::Error::other)
}

fn unreachable_error(error: &io::Error) -> RPCError<u64, BasicNode, RaftError<u64>> {
    RPCError::Unreachable(Unreachable::new(error))
}

fn unreachable_snapshot_error(
    error: &io::Error,
) -> RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>> {
    RPCError::Unreachable(Unreachable::new(error))
}
