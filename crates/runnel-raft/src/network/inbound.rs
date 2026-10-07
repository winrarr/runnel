use std::io;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use super::{
    ForwardError, ForwardedBatchMessage, ForwardedOperation, ForwardedResponse, PeerRequest,
    PeerResponse,
    framing::{read_frame_bounded, write_frame_bounded},
};
use crate::peer_tls::{PeerTlsConnectionPermit, PeerTlsHandshakePermit};
use crate::{GroupManager, PeerTlsConfig, StreamMetadata};

const FRAME_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub(crate) async fn serve(
    listener: TcpListener,
    manager: Arc<GroupManager>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), io::Error> {
    let peer_tls = manager.peer_tls().cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "peer TLS configuration is required before serving the peer listener",
        )
    })?;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                stream.set_nodelay(true)?;
                let Some(connection_permit) = peer_tls.try_acquire_inbound_connection() else {
                    continue;
                };
                let Some(handshake_permit) = peer_tls.try_acquire_inbound_handshake() else {
                    drop(connection_permit);
                    continue;
                };
                let manager = Arc::clone(&manager);
                let peer_tls = Arc::clone(&peer_tls);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(
                        stream,
                        manager,
                        peer_tls,
                        handshake_permit,
                        connection_permit,
                    )
                    .await
                    {
                        tracing::warn!(%error, "raft peer connection failed");
                    }
                });
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    manager: Arc<GroupManager>,
    peer_tls: Arc<PeerTlsConfig>,
    handshake_permit: PeerTlsHandshakePermit,
    connection_permit: PeerTlsConnectionPermit,
) -> Result<(), io::Error> {
    let (mut stream, _peer_id) = peer_tls.accept(stream, handshake_permit).await?;
    let _connection_permit = connection_permit;
    let mut read_buffer = Vec::new();
    let frame_memory = peer_tls.frame_memory();
    let frame_write_slots = peer_tls.frame_write_slots();
    loop {
        let frame = match tokio::time::timeout(
            FRAME_READ_TIMEOUT,
            read_frame_bounded(&mut stream, &mut read_buffer, &frame_memory),
        )
        .await
        {
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "peer frame read timed out",
                ));
            }
            Ok(result) => match result {
                Ok(frame) => frame,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            },
        };
        let (request, _frame_memory_permit) = frame.into_parts();
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
        tokio::time::timeout(
            FRAME_READ_TIMEOUT,
            write_frame_bounded(&mut stream, &response, &frame_write_slots),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer frame write timed out"))??;
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
        ForwardedOperation::Poll {
            stream,
            consumer,
            max_response_bytes,
        } => {
            let Ok(group) = manager.data_group_for_stream(&stream).await else {
                return ForwardedResponse::Poll(Err(ForwardError::Message(
                    "stream data group is unavailable".to_owned(),
                )));
            };
            let leader_id = group.raft().current_leader().await;
            if leader_id != Some(manager.node_id()) {
                return ForwardedResponse::Poll(Err(ForwardError::NotLeader { leader_id }));
            }
            let result = match max_response_bytes {
                Some(max_bytes) => group
                    .poll_with_response_limit(stream, consumer, max_bytes)
                    .await,
                None => group.poll(&stream, &consumer).await,
            };
            ForwardedResponse::Poll(result.map_err(forward_error))
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
        ForwardedOperation::ConfigureConsumer {
            stream,
            consumer,
            ack_timeout_ms,
            max_delivery_attempts,
            retry_delay_ms,
        } => ForwardedResponse::ConsumerPolicy(
            manager
                .configure_consumer_local(
                    stream,
                    consumer,
                    ack_timeout_ms,
                    max_delivery_attempts,
                    retry_delay_ms,
                )
                .await
                .map_err(forward_error),
        ),
        ForwardedOperation::InspectConsumer { stream, consumer } => {
            ForwardedResponse::ConsumerPolicy(
                manager
                    .inspect_consumer_local(&stream, &consumer)
                    .await
                    .map_err(forward_error),
            )
        }
        ForwardedOperation::PollGroup {
            stream,
            consumer,
            member,
            max_response_bytes,
        } => {
            let Ok(group) = manager.data_group_for_stream(&stream).await else {
                return ForwardedResponse::PollGroup(Err(ForwardError::Message(
                    "stream data group is unavailable".to_owned(),
                )));
            };
            let leader_id = group.raft().current_leader().await;
            if leader_id != Some(manager.node_id()) {
                return ForwardedResponse::PollGroup(Err(ForwardError::NotLeader { leader_id }));
            }
            let result = match max_response_bytes {
                Some(max_bytes) => group
                    .poll_group_with_response_limit(stream, consumer, member, max_bytes)
                    .await,
                None => group.poll_group(stream, consumer, member).await,
            };
            ForwardedResponse::PollGroup(result.map_err(forward_error))
        }
        ForwardedOperation::PollGroupBatch {
            stream,
            consumer,
            member,
            response_member,
            max_records,
            max_bytes,
            max_wait_ms,
        } => {
            let limits = runnel_engine::ConsumeBatchLimits {
                max_records,
                max_bytes,
                max_wait_ms,
            };
            let result = if response_member.is_some() {
                manager
                    .poll_group_batch_for_forwarding(&stream, &consumer, &member, limits)
                    .await
            } else {
                manager
                    .poll_batch_for_forwarding(&stream, &consumer, limits)
                    .await
            };
            ForwardedResponse::PollGroupBatch(
                result
                    .map(|messages| {
                        messages
                            .into_iter()
                            .map(ForwardedBatchMessage::from)
                            .collect()
                    })
                    .map_err(forward_error),
            )
        }
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
        ForwardedOperation::AckGroupBatch {
            stream,
            consumer,
            member,
            receipts,
        } => ForwardedResponse::AckGroupBatch(
            manager
                .ack_group_batch_local(stream, consumer, member, receipts)
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
        crate::BrokerError::RequestIdContentConflict => ForwardError::RequestIdContentConflict,
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
        crate::BrokerError::InvalidBatchRequest(message) => {
            ForwardError::InvalidBatchRequest(message)
        }
        crate::BrokerError::ConsumeBatchRecordTooLarge { max_bytes } => {
            ForwardError::ConsumeBatchRecordTooLarge { max_bytes }
        }
        crate::BrokerError::ResponseTooLarge { max_bytes } => {
            ForwardError::ResponseTooLarge { max_bytes }
        }
        error => ForwardError::Message(error.to_string()),
    }
}
