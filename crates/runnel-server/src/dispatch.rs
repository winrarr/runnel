use std::sync::atomic::Ordering;

use runnel_engine::{
    AckBatchOutcome, AckBatchRejection, AckResult, BrokerError, ConsumeBatchLimits, ConsumerPolicy,
    Engine, PollResult, PublishRecord, ReplayMessage,
};
use runnel_protocol::{
    AckBatchItemOutcome, AckBatchItemResponse, BatchMessageResponse, BinaryPayload,
    MAX_PUBLISH_BATCH_RECORDS, PublishBatchRecordResponse, Request, Response,
};
use runnel_protocol::v2::{ApplicationReply, ItemMetadata, Outcome, Stage};

#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;

use crate::observability::{ServerMetrics, record_batch_delivery, record_delivery};
use crate::protocol::invalid_request_response;

pub(crate) async fn handle_request_with_metadata(
    engine: &dyn Engine,
    request: Request,
    metrics: &ServerMetrics,
    max_response_bytes: usize,
) -> ApplicationReply {
    #[cfg(feature = "instrumentation")]
    let _stage_timer = StageTimer::new("server.engine_request");
    let batch_request = matches!(
        &request,
        Request::PublishBatch { .. }
            | Request::PollBatch { .. }
            | Request::PollGroupBatch { .. }
            | Request::AckBatch { .. }
            | Request::AckGroupBatch { .. }
    );
    let state_changing = matches!(
        &request,
        Request::CreateStream { .. }
            | Request::Publish { .. }
            | Request::PublishBytes { .. }
            | Request::PublishBatch { .. }
            | Request::PollGroup { .. }
            | Request::PollGroupBatch { .. }
            | Request::ConfigureConsumer { .. }
            | Request::Ack { .. }
            | Request::AckBatch { .. }
            | Request::AckGroup { .. }
            | Request::AckGroupBatch { .. }
    );
    let grouped_poll = matches!(&request, Request::PollGroup { .. });
    let mut item_metadata = Vec::new();
    if let Request::PublishBatch { records, .. } = &request {
        if records.is_empty() {
            return ApplicationReply::failed(
                invalid_request_response("publish batch must contain at least one record"),
                Outcome::Rejected,
                Stage::Validated,
            );
        }
        if records.len() > MAX_PUBLISH_BATCH_RECORDS {
            return ApplicationReply::failed(
                invalid_request_response(&format!(
                    "publish batch contains more than {MAX_PUBLISH_BATCH_RECORDS} records"
                )),
                Outcome::Rejected,
                Stage::Validated,
            );
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
            payload,
            request_id,
        } => {
            let payload_bytes = payload.as_bytes().len() as u64;
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
        Request::PublishBatch { stream, records } => {
            let payload_sizes = records
                .iter()
                .map(|record| record.payload.as_bytes().len() as u64)
                .collect::<Vec<_>>();
            let records = records
                .into_iter()
                .map(|record| PublishRecord {
                    key: record.key,
                    payload: record.payload.into_bytes(),
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
                                item_metadata.push(ItemMetadata {
                                    outcome: Outcome::Confirmed,
                                    stage: Stage::Durable,
                                });
                                metrics.publishes.fetch_add(1, Ordering::Relaxed);
                                metrics
                                    .published_bytes
                                    .fetch_add(payload_bytes, Ordering::Relaxed);
                                PublishBatchRecordResponse::Published { offset }
                            }
                            Err(error) => {
                                item_metadata.push(ItemMetadata {
                                    outcome: broker_outcome(error.outcome()),
                                    stage: Stage::ExecutionStarted,
                                });
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
            let result = engine
                .poll_with_response_limit(&stream, &consumer, max_response_bytes)
                .await;
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
        Request::PollBatch {
            stream,
            consumer,
            max_records,
            max_bytes,
            max_wait_ms,
        } => {
            let result = engine
                .poll_batch(
                    &stream,
                    &consumer,
                    ConsumeBatchLimits {
                        max_records,
                        max_bytes: max_bytes.min(max_response_bytes),
                        max_wait_ms,
                    },
                )
                .await;
            match result {
                Ok(messages) => {
                    item_metadata = messages
                        .iter()
                        .map(|message| ItemMetadata {
                            outcome: Outcome::Confirmed,
                            stage: if message.delivery_token.is_some() {
                                Stage::Durable
                            } else {
                                Stage::Completed
                            },
                        })
                        .collect();
                    record_batch_delivery(metrics, &messages);
                    Ok(Response::PollBatch {
                        stream,
                        consumer: consumer.clone(),
                        messages: messages
                            .into_iter()
                            .map(|message| batch_message_response(message, &consumer, None))
                            .collect(),
                    })
                }
                Err(error) => Err(error),
            }
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
            let result = engine
                .poll_group_with_response_limit(
                    &stream,
                    &consumer,
                    &member,
                    max_response_bytes,
                )
                .await;
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
        Request::PollGroupBatch {
            stream,
            consumer,
            member,
            max_records,
            max_bytes,
            max_wait_ms,
        } => {
            let result = engine
                .poll_group_batch(
                    &stream,
                    &consumer,
                    &member,
                    ConsumeBatchLimits {
                        max_records,
                        max_bytes: max_bytes.min(max_response_bytes),
                        max_wait_ms,
                    },
                )
                .await;
            match result {
                Ok(messages) => {
                    item_metadata = messages
                        .iter()
                        .map(|_| ItemMetadata {
                            outcome: Outcome::Confirmed,
                            stage: Stage::Durable,
                        })
                        .collect();
                    record_batch_delivery(metrics, &messages);
                    Ok(Response::PollBatch {
                        stream,
                        consumer: consumer.clone(),
                        messages: messages
                            .into_iter()
                            .map(|message| {
                                batch_message_response(message, &consumer, Some(&member))
                            })
                            .collect(),
                    })
                }
                Err(error) => Err(error),
            }
        }
        Request::ConfigureConsumer {
            stream,
            consumer,
            ack_timeout_ms,
            max_delivery_attempts,
        } => engine
            .configure_consumer(&stream, &consumer, ack_timeout_ms, max_delivery_attempts)
            .await
            .map(|policy| consumer_policy_response(stream, consumer, policy)),
        Request::InspectConsumer { stream, consumer } => engine
            .inspect_consumer(&stream, &consumer)
            .await
            .map(|policy| consumer_policy_response(stream, consumer, policy)),
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
        Request::AckBatch {
            stream,
            consumer,
            receipts,
        } => engine
            .ack_batch(
                &stream,
                &consumer,
                receipts
                    .into_iter()
                    .map(|receipt| runnel_engine::DeliveryReceipt {
                        offset: receipt.offset,
                        delivery_token: receipt.delivery_token,
                    })
                    .collect(),
            )
            .await
            .map(|result| {
                item_metadata = result
                    .outcomes
                    .iter()
                    .map(|item| match item.outcome {
                        AckBatchOutcome::Confirmed | AckBatchOutcome::AlreadyConfirmed => {
                            ItemMetadata {
                                outcome: Outcome::Confirmed,
                                stage: Stage::Durable,
                            }
                        }
                        AckBatchOutcome::Rejected { .. } => ItemMetadata {
                            outcome: Outcome::Rejected,
                            stage: Stage::ExecutionStarted,
                        },
                    })
                    .collect();
                metrics.acknowledgements.fetch_add(
                    result
                        .outcomes
                        .iter()
                        .filter(|item| matches!(item.outcome, AckBatchOutcome::Confirmed))
                        .count() as u64,
                    Ordering::Relaxed,
                );
                Response::AckBatch {
                    stream,
                    consumer,
                    outcomes: result
                        .outcomes
                        .into_iter()
                        .map(ack_batch_item_response)
                        .collect(),
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
        Request::AckGroupBatch {
            stream,
            consumer,
            member,
            receipts,
        } => engine
            .ack_group_batch(
                &stream,
                &consumer,
                &member,
                receipts
                    .into_iter()
                    .map(|receipt| runnel_engine::DeliveryReceipt {
                        offset: receipt.offset,
                        delivery_token: receipt.delivery_token,
                    })
                    .collect(),
            )
            .await
            .map(|result| {
                item_metadata = result
                    .outcomes
                    .iter()
                    .map(|item| match item.outcome {
                        AckBatchOutcome::Confirmed | AckBatchOutcome::AlreadyConfirmed => {
                            ItemMetadata {
                                outcome: Outcome::Confirmed,
                                stage: Stage::Durable,
                            }
                        }
                        AckBatchOutcome::Rejected { .. } => ItemMetadata {
                            outcome: Outcome::Rejected,
                            stage: Stage::ExecutionStarted,
                        },
                    })
                    .collect();
                metrics.acknowledgements.fetch_add(
                    result
                        .outcomes
                        .iter()
                        .filter(|item| matches!(item.outcome, AckBatchOutcome::Confirmed))
                        .count() as u64,
                    Ordering::Relaxed,
                );
                Response::AckBatch {
                    stream,
                    consumer,
                    outcomes: result
                        .outcomes
                        .into_iter()
                        .map(ack_batch_item_response)
                        .collect(),
                }
            }),
        Request::Health => engine.health().await.map(|health| Response::Health {
            status: "ok".to_owned(),
            streams: health.streams,
            storage_bytes: health.storage_bytes,
        }),
    };

    match result {
        Ok(response) if batch_request => ApplicationReply::batch(response, item_metadata),
        Ok(response) => {
            let stage = match (&response, grouped_poll) {
                (Response::Message { .. } | Response::MessageBytes { .. }, true) => Stage::Durable,
                (Response::Empty { .. }, true) => Stage::Completed,
                _ if state_changing => Stage::Durable,
                _ => Stage::Completed,
            };
            ApplicationReply {
                response,
                outcome: Some(Outcome::Confirmed),
                stage: Some(stage),
                items: Vec::new(),
            }
        }
        Err(error) => ApplicationReply::failed(
            error_response(&error),
            broker_outcome(error.outcome()),
            Stage::ExecutionStarted,
        ),
    }
}

fn broker_outcome(outcome: runnel_engine::BrokerErrorOutcome) -> Outcome {
    match outcome {
        runnel_engine::BrokerErrorOutcome::Rejected => Outcome::Rejected,
        runnel_engine::BrokerErrorOutcome::Retryable => Outcome::Retryable,
        runnel_engine::BrokerErrorOutcome::Unknown => Outcome::Unknown,
    }
}

fn consumer_policy_response(stream: String, consumer: String, policy: ConsumerPolicy) -> Response {
    Response::ConsumerPolicy {
        stream,
        consumer,
        version: policy.version,
        configured: policy.configured,
        ack_timeout_ms: policy.ack_timeout_ms,
        max_delivery_attempts: policy.max_delivery_attempts,
    }
}

fn batch_message_response(
    message: runnel_engine::Message,
    consumer: &str,
    member: Option<&str>,
) -> BatchMessageResponse {
    let runnel_engine::Message {
        stream,
        offset,
        key,
        payload,
        published_at_ms,
        delivery_token,
        delivery_attempt,
    } = message;
    if let Ok(payload_text) = std::str::from_utf8(&payload) {
        BatchMessageResponse::Text {
            stream,
            consumer: consumer.to_owned(),
            member: member.map(str::to_owned),
            offset,
            key,
            payload: payload_text.to_owned(),
            published_at_ms,
            delivery_token,
            delivery_attempt,
        }
    } else {
        BatchMessageResponse::Bytes {
            stream,
            consumer: consumer.to_owned(),
            member: member.map(str::to_owned),
            offset,
            key,
            payload: BinaryPayload::new(payload),
            published_at_ms,
            delivery_token,
            delivery_attempt,
        }
    }
}

fn ack_batch_item_response(item: runnel_engine::AckBatchItem) -> AckBatchItemResponse {
    match item.outcome {
        AckBatchOutcome::Confirmed => AckBatchItemResponse {
            offset: item.offset,
            outcome: AckBatchItemOutcome::Confirmed,
            code: None,
            message: None,
        },
        AckBatchOutcome::AlreadyConfirmed => AckBatchItemResponse {
            offset: item.offset,
            outcome: AckBatchItemOutcome::AlreadyConfirmed,
            code: None,
            message: None,
        },
        AckBatchOutcome::Rejected { reason } => {
            let (code, message) = match reason {
                AckBatchRejection::NotInFlight => {
                    ("ack_not_in_flight", "record has no delivery attempt")
                }
                AckBatchRejection::StaleDelivery => (
                    "stale_delivery",
                    "delivery receipt is stale or belongs to another member",
                ),
            };
            AckBatchItemResponse {
                offset: item.offset,
                outcome: AckBatchItemOutcome::Rejected,
                code: Some(code.to_owned()),
                message: Some(message.to_owned()),
            }
        }
    }
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
            payload: BinaryPayload::new(error.into_bytes()),
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
            payload: BinaryPayload::new(error.into_bytes()),
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
        BrokerError::RequestIdContentConflict => "request_id_content_conflict",
        BrokerError::OutOfOrderAck { .. } => "out_of_order_ack",
        BrokerError::HistoryUnavailable { .. } => "history_unavailable",
        BrokerError::CorruptRecord(_) => "corrupt_record",
        BrokerError::Io(_) => "storage_error",
        BrokerError::State(_) => "consumer_state_error",
        BrokerError::ConsumerStatePersistence {
            stage: runnel_engine::ConsumerStatePersistStage::BeforeAppend,
            ..
        } => "consumer_state_retryable",
        BrokerError::ConsumerStatePersistence { .. } => "consumer_state_error",
        BrokerError::LockPoisoned => "internal_error",
        BrokerError::Configuration(_) => "invalid_configuration",
        BrokerError::InvalidBatchRequest(_) => "invalid_batch_request",
        BrokerError::ConsumeBatchRecordTooLarge { .. } => "consume_batch_record_too_large",
        BrokerError::ResponseTooLarge { .. } => "response_too_large",
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
