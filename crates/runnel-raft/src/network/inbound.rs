use std::io;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use super::{
    ForwardError, ForwardedOperation, ForwardedResponse, PeerRequest, PeerResponse, read_frame,
    write_frame,
};
use crate::{GroupManager, StreamMetadata};

pub(crate) async fn serve(
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
        ForwardedOperation::Replay {
            stream,
            consumer,
            offset,
        } => ForwardedResponse::Replay(
            manager
                .replay_local(&stream, &consumer, offset)
                .await
                .map_err(forward_error),
        ),
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

fn forward_error(error: crate::BrokerError) -> ForwardError {
    match error {
        crate::BrokerError::NotLeader { leader_id } => ForwardError::NotLeader { leader_id },
        crate::BrokerError::AckNotInFlight { consumer, offset } => {
            ForwardError::AckNotInFlight { consumer, offset }
        }
        crate::BrokerError::StaleDelivery { consumer, offset } => {
            ForwardError::StaleDelivery { consumer, offset }
        }
        crate::BrokerError::HistoryUnavailable {
            stream,
            requested_offset,
            earliest_offset,
            next_offset,
        } => ForwardError::HistoryUnavailable {
            stream,
            requested_offset,
            earliest_offset,
            next_offset,
        },
        error => ForwardError::Message(error.to_string()),
    }
}
