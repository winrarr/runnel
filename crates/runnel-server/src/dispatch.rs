use std::sync::atomic::Ordering;

use runnel_engine::{AckResult, BrokerError, Engine, PollResult, PublishRecord, ReplayMessage};
use runnel_protocol::{
    BinaryPayload, MAX_PUBLISH_BATCH_RECORDS, PublishBatchRecordResponse, Request, Response,
};

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;

use crate::observability::{ServerMetrics, record_delivery};
use crate::protocol::invalid_request_response;

pub(crate) async fn handle_request(
    engine: &dyn Engine,
    request: Request,
    metrics: &ServerMetrics,
) -> Response {
    #[cfg(feature = "instrumentation")]
    let _stage_timer = StageTimer::new("server.engine_request");
    if let Request::PublishBatch { records, .. } = &request {
        if records.is_empty() {
            return invalid_request_response("publish batch must contain at least one record");
        }
        if records.len() > MAX_PUBLISH_BATCH_RECORDS {
            return invalid_request_response(&format!(
                "publish batch contains more than {MAX_PUBLISH_BATCH_RECORDS} records"
            ));
        }
    }
    let result = match request {
        Request::CreateStream { stream } => engine.create_stream(&stream).await.map(|created| {
            if created {
                metrics.stream_creations.fetch_add(1, Ordering::Relaxed);
            }
            Response::StreamCreated { stream, created }
        }),
        Request::Publish {
            stream,
            key,
            payload,
            request_id,
        } => {
            let payload_bytes = payload.len() as u64;
            engine
                .publish(&stream, key, payload.into_bytes(), request_id)
                .await
                .map(|offset| {
                    metrics.publishes.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .published_bytes
                        .fetch_add(payload_bytes, Ordering::Relaxed);
                    Response::Published { stream, offset }
                })
        }
        Request::PublishBytes {
            stream,
            key,
            payload_base64,
            request_id,
        } => {
            let payload_bytes = payload_base64.as_bytes().len() as u64;
            engine
                .publish(&stream, key, payload_base64.into_bytes(), request_id)
                .await
                .map(|offset| {
                    metrics.publishes.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .published_bytes
                        .fetch_add(payload_bytes, Ordering::Relaxed);
                    Response::Published { stream, offset }
                })
        }
        Request::PublishBatch { stream, records } => {
            let payload_sizes = records
                .iter()
                .map(|record| record.payload_base64.as_bytes().len() as u64)
                .collect::<Vec<_>>();
            let records = records
                .into_iter()
                .map(|record| PublishRecord {
                    key: record.key,
                    payload: record.payload_base64.into_bytes(),
                    request_id: record.request_id,
                })
                .collect();
            engine
                .publish_batch(&stream, records)
                .await
                .and_then(|outcomes| {
                    if outcomes.len() != payload_sizes.len() {
                        return Err(BrokerError::Cluster(
                            "publish batch returned the wrong number of outcomes".to_owned(),
                        ));
                    }
                    let outcomes = outcomes
                        .into_iter()
                        .zip(payload_sizes)
                        .map(|(outcome, payload_bytes)| match outcome {
                            Ok(offset) => {
                                metrics.publishes.fetch_add(1, Ordering::Relaxed);
                                metrics
                                    .published_bytes
                                    .fetch_add(payload_bytes, Ordering::Relaxed);
                                PublishBatchRecordResponse::Published { offset }
                            }
                            Err(error) => {
                                let Response::Error { code, message } =
                                    publish_batch_error_response(&error)
                                else {
                                    unreachable!("broker errors must map to error responses")
                                };
                                PublishBatchRecordResponse::Error { code, message }
                            }
                        })
                        .collect();
                    Ok(Response::PublishBatch { stream, outcomes })
                })
        }
        Request::Poll { stream, consumer } => {
            let result = engine.poll(&stream, &consumer).await;
            record_delivery(metrics, &result);
            result.map(|result| match result {
                PollResult::Message(message) => message_response(MessageResponse {
                    stream: message.stream,
                    consumer,
                    member: None,
                    offset: message.offset,
                    key: message.key,
                    payload: message.payload,
                    published_at_ms: message.published_at_ms,
                    delivery_token: None,
                    delivery_attempt: message.delivery_attempt,
                }),
                PollResult::Empty => Response::Empty { stream, consumer },
            })
        }
        Request::Replay {
            stream,
            consumer,
            offset,
        } => engine
            .replay(&stream, &consumer, offset)
            .await
            .map(|message| replay_message_response(message, consumer)),
        Request::PollGroup {
            stream,
            consumer,
            member,
        } => {
            let result = engine.poll_group(&stream, &consumer, &member).await;
            record_delivery(metrics, &result);
            result.map(|result| match result {
                PollResult::Message(message) => message_response(MessageResponse {
                    stream: message.stream,
                    consumer,
                    member: Some(member),
                    offset: message.offset,
                    key: message.key,
                    payload: message.payload,
                    published_at_ms: message.published_at_ms,
                    delivery_token: message.delivery_token,
                    delivery_attempt: message.delivery_attempt,
                }),
                PollResult::Empty => Response::Empty { stream, consumer },
            })
        }
        Request::Ack {
            stream,
            consumer,
            offset,
        } => engine.ack(&stream, &consumer, offset).await.map(|result| {
            metrics.acknowledgements.fetch_add(1, Ordering::Relaxed);
            Response::Acknowledged {
                stream,
                consumer,
                offset,
                already_acknowledged: result == AckResult::AlreadyAcknowledged,
            }
        }),
        Request::AckGroup {
            stream,
            consumer,
            member,
            offset,
            delivery_token,
        } => engine
            .ack_group(&stream, &consumer, &member, offset, &delivery_token)
            .await
            .map(|result| {
                metrics.acknowledgements.fetch_add(1, Ordering::Relaxed);
                Response::Acknowledged {
                    stream,
                    consumer,
                    offset,
                    already_acknowledged: result == AckResult::AlreadyAcknowledged,
                }
            }),
        Request::Health => engine.health().await.map(|health| Response::Health {
            status: "ok".to_owned(),
            streams: health.streams,
            storage_bytes: health.storage_bytes,
        }),
    };

    result.unwrap_or_else(|error| error_response(&error))
}

fn replay_message_response(message: ReplayMessage, consumer: String) -> Response {
    let ReplayMessage {
        stream,
        offset,
        key,
        payload,
        published_at_ms,
    } = message;
    match String::from_utf8(payload) {
        Ok(payload) => Response::ReplayMessage {
            stream,
            consumer,
            offset,
            key,
            payload,
            published_at_ms,
        },
        Err(error) => Response::ReplayMessageBytes {
            stream,
            consumer,
            offset,
            key,
            payload_base64: BinaryPayload::new(error.into_bytes()),
            published_at_ms,
        },
    }
}

struct MessageResponse {
    stream: String,
    consumer: String,
    member: Option<String>,
    offset: u64,
    key: Option<String>,
    payload: Vec<u8>,
    published_at_ms: u64,
    delivery_token: Option<String>,
    delivery_attempt: Option<u32>,
}

fn message_response(message: MessageResponse) -> Response {
    let MessageResponse {
        stream,
        consumer,
        member,
        offset,
        key,
        payload,
        published_at_ms,
        delivery_token,
        delivery_attempt,
    } = message;
    match String::from_utf8(payload) {
        Ok(payload) => Response::Message {
            stream,
            consumer,
            member,
            offset,
            key,
            payload,
            published_at_ms,
            delivery_token,
            delivery_attempt,
        },
        Err(error) => Response::MessageBytes {
            stream,
            consumer,
            member,
            offset,
            key,
            payload_base64: BinaryPayload::new(error.into_bytes()),
            published_at_ms,
            delivery_token,
            delivery_attempt,
        },
    }
}

fn error_response(error: &BrokerError) -> Response {
    let code = match error {
        BrokerError::InvalidName { .. } => "invalid_name",
        BrokerError::StreamNotFound(_) => "stream_not_found",
        BrokerError::StreamNotReady(_) => "stream_not_ready",
        BrokerError::AckNotInFlight { .. } => "ack_not_in_flight",
        BrokerError::StaleDelivery { .. } => "stale_delivery",
        BrokerError::OutOfOrderAck { .. } => "out_of_order_ack",
        BrokerError::HistoryUnavailable { .. } => "history_unavailable",
        BrokerError::CorruptRecord(_) => "corrupt_record",
        BrokerError::Io(_) => "storage_error",
        BrokerError::State(_) => "consumer_state_error",
        BrokerError::LockPoisoned => "internal_error",
        BrokerError::Configuration(_) => "invalid_configuration",
        BrokerError::NotLeader { .. } => "cluster_error",
        BrokerError::Cluster(_) => "cluster_error",
    };
    Response::Error {
        code: code.to_owned(),
        message: error.to_string(),
    }
}

fn publish_batch_error_response(error: &BrokerError) -> Response {
    if let BrokerError::Io(io_error) = error
        && io_error.kind() == std::io::ErrorKind::InvalidInput
    {
        return Response::Error {
            code: "invalid_record".to_owned(),
            message: error.to_string(),
        };
    }
    error_response(error)
}
