use std::io;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, watch};

use super::{
    ForwardError, ForwardedBatchMessage, ForwardedOperation, ForwardedPollResult,
    ForwardedReplayMessage, ForwardedResponse, PeerRequest, PeerResponse,
    framing::{read_frame_bounded, write_frame_bounded, write_frame_response_bounded},
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
        let (request, request_memory_permit) = frame.into_parts();
        let response_memory_permit = match forwarded_response_memory_bytes(&request) {
            Some(max_response_bytes) => {
                Some(acquire_response_memory(peer_tls.response_memory(), max_response_bytes).await?)
            }
            None => None,
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
        drop(request_memory_permit);
        let write = async {
            if response_memory_permit.is_some() {
                write_frame_response_bounded(&mut stream, &response, &frame_write_slots).await
            } else {
                write_frame_bounded(&mut stream, &response, &frame_write_slots, &frame_memory).await
            }
        };
        tokio::time::timeout(FRAME_READ_TIMEOUT, write)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer frame write timed out"))??;
        drop(response_memory_permit);
    }
}

const RESPONSE_MEMORY_CAPACITY: usize = 256 * 1024 * 1024;
const RESPONSE_MEMORY_QUANTUM: usize = 1024 * 1024;
const RESPONSE_MEMORY_HEADROOM: usize = 2 * RESPONSE_MEMORY_QUANTUM;

fn forwarded_response_memory_bytes(request: &PeerRequest) -> Option<usize> {
    match request {
        PeerRequest::Forward(ForwardedOperation::Poll {
            max_response_bytes, ..
        })
        | PeerRequest::Forward(ForwardedOperation::PollGroup {
            max_response_bytes, ..
        }) => Some(max_response_bytes.unwrap_or(runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES)),
        PeerRequest::Forward(ForwardedOperation::PollGroupBatch { max_bytes, .. }) => {
            Some(*max_bytes)
        }
        PeerRequest::Forward(ForwardedOperation::Replay { .. }) => {
            Some(runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES)
        }
        _ => None,
    }
}

fn response_memory_units(max_response_bytes: usize) -> u32 {
    // The raw decoded result, a temporary base64 String, and the final JSON
    // frame can coexist during serialization. Reserve 3.75x the admitted
    // protobuf response plus per-record and envelope headroom before dispatch.
    let response_bytes = max_response_bytes.min(runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES);
    let reserved_bytes = response_bytes
        .saturating_mul(15)
        .div_ceil(4)
        .saturating_add(RESPONSE_MEMORY_HEADROOM);
    let units = reserved_bytes.div_ceil(RESPONSE_MEMORY_QUANTUM).max(1);
    u32::try_from(units).unwrap_or(u32::MAX)
}

async fn acquire_response_memory(
    response_memory: Arc<tokio::sync::Semaphore>,
    max_response_bytes: usize,
) -> Result<OwnedSemaphorePermit, io::Error> {
    let units = response_memory_units(max_response_bytes);
    if units as usize > RESPONSE_MEMORY_CAPACITY / RESPONSE_MEMORY_QUANTUM {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer response exceeds the response memory budget",
        ));
    }
    tokio::time::timeout(
        FRAME_READ_TIMEOUT,
        response_memory.acquire_many_owned(units),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "peer response memory admission timed out",
        )
    })?
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "peer response memory admission is closed",
        )
    })
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
                Some(max_bytes) => {
                    group
                        .poll_with_response_limit(stream, consumer, max_bytes)
                        .await
                }
                None => group.poll(&stream, &consumer).await,
            };
            ForwardedResponse::Poll(result.map(ForwardedPollResult::from).map_err(forward_error))
        }
        ForwardedOperation::Replay {
            stream,
            consumer,
            offset,
        } => ForwardedResponse::Replay(
            manager
                .replay_local(&stream, &consumer, offset)
                .await
                .map(ForwardedReplayMessage::from)
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
                Some(max_bytes) => {
                    group
                        .poll_group_with_response_limit(stream, consumer, member, max_bytes)
                        .await
                }
                None => group.poll_group(stream, consumer, member).await,
            };
            ForwardedResponse::PollGroup(
                result.map(ForwardedPollResult::from).map_err(forward_error),
            )
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

#[cfg(test)]
mod response_memory_tests {
    use super::*;
    use std::future::pending;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;
    use tokio::sync::Semaphore;

    #[test]
    fn large_forwarded_reads_reserve_their_reply_budget_before_dispatch() {
        let max_bytes = runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES;
        let request = PeerRequest::Forward(ForwardedOperation::PollGroupBatch {
            stream: "events".to_owned(),
            consumer: "workers".to_owned(),
            member: "member-a".to_owned(),
            response_member: Some("member-a".to_owned()),
            max_records: runnel_engine::MAX_CONSUME_BATCH_RECORDS,
            max_bytes,
            max_wait_ms: 0,
        });
        assert_eq!(forwarded_response_memory_bytes(&request), Some(max_bytes));

        let units = response_memory_units(max_bytes);
        assert_eq!(units, 246);
        let raw_response_bytes = max_bytes;
        let maximum_json_frame_bytes = max_bytes + max_bytes / 3 + 1024 * 1024;
        let largest_temporary_base64_bytes = max_bytes + max_bytes / 3;
        let peak = raw_response_bytes
            + maximum_json_frame_bytes
            + largest_temporary_base64_bytes
            + RESPONSE_MEMORY_HEADROOM;
        assert!(units as usize * RESPONSE_MEMORY_QUANTUM >= peak);
        let budget = Arc::new(Semaphore::new(
            RESPONSE_MEMORY_CAPACITY / RESPONSE_MEMORY_QUANTUM,
        ));
        let permit = budget
            .clone()
            .try_acquire_many_owned(units)
            .expect("one maximum response must fit the response budget");
        assert_eq!(budget.available_permits(), 10);
        assert!(
            budget.clone().try_acquire_many_owned(units).is_err(),
            "a second maximum response must wait before dispatch"
        );
        drop(permit);
        assert!(
            budget.clone().try_acquire_many_owned(units).is_ok(),
            "the reservation is released after the response finishes"
        );
    }

    #[test]
    fn forwarded_poll_and_replay_are_response_budgeted() {
        let poll = PeerRequest::Forward(ForwardedOperation::Poll {
            stream: "events".to_owned(),
            consumer: "workers".to_owned(),
            max_response_bytes: None,
        });
        let replay = PeerRequest::Forward(ForwardedOperation::Replay {
            stream: "events".to_owned(),
            consumer: "workers".to_owned(),
            offset: 0,
        });
        let ack = PeerRequest::Forward(ForwardedOperation::Ack {
            stream: "events".to_owned(),
            consumer: "workers".to_owned(),
            offset: 0,
        });
        assert_eq!(
            forwarded_response_memory_bytes(&poll),
            Some(runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES)
        );
        assert_eq!(
            forwarded_response_memory_bytes(&replay),
            Some(runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES)
        );
        assert_eq!(forwarded_response_memory_bytes(&ack), None);
    }

    #[test]
    fn frame_budget_bounds_maximum_reads_and_generic_writes() {
        use super::super::framing::{
            FRAME_MEMORY_QUANTUM, MAX_BUFFERED_FRAME_MEMORY, MAX_FRAME_SIZE,
        };

        let capacity = MAX_BUFFERED_FRAME_MEMORY / FRAME_MEMORY_QUANTUM;
        let max_write_units = (MAX_FRAME_SIZE as usize).div_ceil(FRAME_MEMORY_QUANTUM) as u32;
        let budget = Arc::new(Semaphore::new(capacity));

        let first_write = budget
            .clone()
            .try_acquire_many_owned(max_write_units)
            .unwrap();
        let second_write = budget
            .clone()
            .try_acquire_many_owned(max_write_units)
            .unwrap();
        assert!(
            budget
                .clone()
                .try_acquire_many_owned(max_write_units)
                .is_err()
        );
        drop((first_write, second_write));

        let largest_read_units =
            ((MAX_FRAME_SIZE as usize) * 2).div_ceil(FRAME_MEMORY_QUANTUM) as u32;
        let largest_read = budget
            .clone()
            .try_acquire_many_owned(largest_read_units)
            .unwrap();
        assert!(
            budget
                .clone()
                .try_acquire_many_owned(max_write_units)
                .is_err()
        );
        drop(largest_read);
        assert_eq!(budget.available_permits(), capacity);
    }

    #[tokio::test]
    async fn response_reservation_covers_input_contention_and_releases_on_cancel() {
        let frame_memory = Arc::new(Semaphore::new(192));
        let request_frame = frame_memory.clone().acquire_many_owned(192).await.unwrap();
        let response_memory = Arc::new(Semaphore::new(256));
        let response = acquire_response_memory(
            Arc::clone(&response_memory),
            runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(frame_memory.available_permits(), 0);
        assert_eq!(response_memory.available_permits(), 10);
        drop(response);
        drop(request_frame);

        let response_memory = Arc::new(Semaphore::new(256));
        let task_budget = Arc::clone(&response_memory);
        let task = tokio::spawn(async move {
            let _permit = acquire_response_memory(
                task_budget,
                runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
            )
            .await
            .unwrap();
            pending::<()>().await;
        });
        tokio::task::yield_now().await;
        assert_eq!(response_memory.available_permits(), 10);
        task.abort();
        let _ = task.await;
        assert_eq!(response_memory.available_permits(), 256);
    }

    #[tokio::test]
    async fn response_reservation_is_released_after_a_failed_write() {
        struct BrokenWriter;

        impl AsyncWrite for BrokenWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _context: &mut Context<'_>,
                _buffer: &[u8],
            ) -> Poll<Result<usize, io::Error>> {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "test peer closed",
                )))
            }

            fn poll_flush(
                self: Pin<&mut Self>,
                _context: &mut Context<'_>,
            ) -> Poll<Result<(), io::Error>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: Pin<&mut Self>,
                _context: &mut Context<'_>,
            ) -> Poll<Result<(), io::Error>> {
                Poll::Ready(Ok(()))
            }
        }

        let response_memory = Arc::new(Semaphore::new(256));
        let write_slots = Arc::new(Semaphore::new(1));
        let mut writer = BrokenWriter;
        let result = async {
            let _response_permit = acquire_response_memory(
                Arc::clone(&response_memory),
                runnel_engine::MAX_CONSUME_BATCH_RESPONSE_BYTES,
            )
            .await?;
            super::super::framing::write_frame_response_bounded(
                &mut writer,
                &"large response",
                &write_slots,
            )
            .await
        }
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(response_memory.available_permits(), 256);
        assert_eq!(write_slots.available_permits(), 1);
    }
}
