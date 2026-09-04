use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use crate::TypeConfig;
use runnel_engine::{AckResult, Offset, PollResult, ReplayMessage};

mod framing;
mod inbound;
mod outbound;

pub(crate) use inbound::serve;
pub(crate) use outbound::{PeerTransport, TcpNetwork, ensure_data_group, forward};

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
    Replay {
        stream: String,
        consumer: String,
        offset: Offset,
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
    Replay(Result<ReplayMessage, ForwardError>),
    Ack(Result<AckResult, ForwardError>),
    PollGroup(Result<PollResult, ForwardError>),
    AckGroup(Result<AckResult, ForwardError>),
    InitializeDataStream(Result<bool, ForwardError>),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ForwardError {
    NotLeader {
        leader_id: Option<u64>,
    },
    AckNotInFlight {
        consumer: String,
        offset: Offset,
    },
    StaleDelivery {
        consumer: String,
        offset: Offset,
    },
    HistoryUnavailable {
        stream: String,
        requested_offset: Offset,
        earliest_offset: Offset,
        next_offset: Offset,
    },
    Message(String),
}
