use std::io;
use std::mem::size_of;

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::TypeConfig;
use runnel_engine::{AckResult, Offset, PollResult, ReplayMessage};

mod inbound;
mod outbound;

pub(crate) use inbound::serve;
pub(crate) use outbound::{PeerTransport, TcpNetwork, ensure_data_group, forward};

const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;
const MAX_REUSABLE_FRAME_BUFFER_SIZE: usize = 1024 * 1024;

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
