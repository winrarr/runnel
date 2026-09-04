use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use runnel_engine::Engine;
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_protocol::{Request, Response};
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
use tracing::warn;

use crate::dispatch::handle_request;
use crate::observability::{ActiveConnection, ActiveRequest, RequestOperation, ServerMetrics};
use crate::protocol::{
    Frame, ProtocolAdmission, invalid_request_response, read_frame, reject_connection,
    remaining_timeout, request_line, request_size_response, response_write_timeout,
    saturated_response, send_response, timeout_response, wait_for_request_data,
};

pub(crate) fn spawn(
    listener: TcpListener,
    engine: Arc<dyn Engine>,
    metrics: Arc<ServerMetrics>,
    protocol_admission: ProtocolAdmission,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<Result<(), std::io::Error>> {
    let connection_slots = Arc::new(Semaphore::new(protocol_admission.max_connections));
    let request_slots = Arc::new(Semaphore::new(protocol_admission.max_in_flight_requests));
    tokio::spawn(run_tcp(
        listener,
        engine,
        metrics,
        connection_slots,
        request_slots,
        protocol_admission,
        shutdown,
    ))
}

async fn run_tcp(
    listener: TcpListener,
    engine: Arc<dyn Engine>,
    metrics: Arc<ServerMetrics>,
    connection_slots: Arc<Semaphore>,
    request_slots: Arc<Semaphore>,
    protocol_admission: ProtocolAdmission,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    while let Some(result) = connections.join_next().await {
                        if let Err(error) = result {
                            warn!(%error, "broker connection task stopped during shutdown");
                        }
                    }
                    return Ok(());
                }
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    warn!(%error, "broker connection task stopped");
                }
            }
            result = listener.accept() => {
                let (stream, peer) = result?;
                if *shutdown.borrow() {
                    continue;
                }
                metrics
                    .connections_accepted
                    .fetch_add(1, Ordering::Relaxed);
                let connection_permit = match Arc::clone(&connection_slots).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        metrics
                            .connections_rejected
                            .fetch_add(1, Ordering::Relaxed);
                        metrics
                            .connections_closed
                            .fetch_add(1, Ordering::Relaxed);
                        reject_connection(stream).await;
                        warn!(%peer, "broker connection rejected at connection limit");
                        continue;
                    }
                };
                let engine = Arc::clone(&engine);
                let connection_metrics = Arc::clone(&metrics);
                let connection_request_slots = Arc::clone(&request_slots);
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    if let Err(error) = handle_connection(
                        stream,
                        engine,
                        Arc::clone(&connection_metrics),
                        connection_permit,
                        connection_request_slots,
                        protocol_admission,
                        connection_shutdown,
                    )
                    .await
                    {
                        connection_metrics
                            .connection_errors
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(%peer, %error, "connection closed with error");
                    }
                });
            }
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    engine: Arc<dyn Engine>,
    metrics: Arc<ServerMetrics>,
    _connection_permit: OwnedSemaphorePermit,
    request_slots: Arc<Semaphore>,
    protocol_admission: ProtocolAdmission,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
    let _active_connection = ActiveConnection::new(Arc::clone(&metrics));
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        // Waiting for the first byte is intentionally not timed. An idle
        // persistent connection is valid; once a request starts, its frame
        // must complete within the request deadline. Both idle reads and
        // partial frame reads observe shutdown so a persistent connection
        // cannot extend the graceful-drain window.
        let has_data = match wait_for_request_data(&mut reader, &mut shutdown).await? {
            Some(has_data) => has_data,
            None => return Ok(()),
        };
        if !has_data {
            return Ok(());
        }

        let started = Instant::now();
        let frame_result = tokio::select! {
            result = tokio::time::timeout(
                protocol_admission.request_timeout,
                read_frame(&mut reader, protocol_admission.max_request_bytes),
            ) => Some(result),
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                None
            }
        };
        let Some(frame_result) = frame_result else {
            continue;
        };
        let frame = match frame_result {
            Ok(result) => result?,
            Err(_) => {
                metrics.request_timeouts.fetch_add(1, Ordering::Relaxed);
                metrics
                    .record_rejected_request(RequestOperation::InvalidRequest, started.elapsed());
                send_response(
                    &mut writer,
                    &timeout_response(),
                    protocol_admission.request_timeout,
                    &metrics,
                )
                .await?;
                return Ok(());
            }
        };

        match frame {
            Frame::End => return Ok(()),
            Frame::TooLarge { bytes } => {
                metrics
                    .request_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                metrics
                    .request_size_rejections
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .record_rejected_request(RequestOperation::InvalidRequest, started.elapsed());
                let response = request_size_response(protocol_admission.max_request_bytes);
                send_response(
                    &mut writer,
                    &response,
                    response_write_timeout(started, protocol_admission.request_timeout, &response),
                    &metrics,
                )
                .await?;
                return Ok(());
            }
            Frame::Unterminated { bytes } => {
                metrics
                    .request_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                metrics
                    .record_rejected_request(RequestOperation::InvalidRequest, started.elapsed());
                let response = invalid_request_response("request frame must end with a newline");
                send_response(
                    &mut writer,
                    &response,
                    response_write_timeout(started, protocol_admission.request_timeout, &response),
                    &metrics,
                )
                .await?;
                return Ok(());
            }
            Frame::Complete { bytes } => {
                metrics
                    .request_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                let line = request_line(&bytes);
                match serde_json::from_slice::<Request>(line) {
                    Ok(request) => {
                        let operation = RequestOperation::from_request(&request);
                        let request_permit = match Arc::clone(&request_slots).try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                metrics
                                    .request_saturation_rejections
                                    .fetch_add(1, Ordering::Relaxed);
                                let response = saturated_response();
                                metrics.record_rejected_request(operation, started.elapsed());
                                send_response(
                                    &mut writer,
                                    &response,
                                    response_write_timeout(
                                        started,
                                        protocol_admission.request_timeout,
                                        &response,
                                    ),
                                    &metrics,
                                )
                                .await?;
                                continue;
                            }
                        };
                        // Keep response serialization and socket writes inside the in-flight
                        // bound so slow response writers cannot consume unbounded request work.
                        let _request_permit = request_permit;
                        metrics.active_requests.fetch_add(1, Ordering::Relaxed);
                        let _active_request = ActiveRequest::new(Arc::clone(&metrics));
                        #[cfg(feature = "instrumentation")]
                        let _stage_timer = StageTimer::new("server.protocol_round_trip");
                        let response = match tokio::time::timeout(
                            remaining_timeout(started, protocol_admission.request_timeout),
                            handle_request(engine.as_ref(), request, &metrics),
                        )
                        .await
                        {
                            Ok(response) => response,
                            Err(_) => {
                                metrics.request_timeouts.fetch_add(1, Ordering::Relaxed);
                                timeout_response()
                            }
                        };
                        let failed = matches!(&response, Response::Error { .. });
                        metrics.record_request(operation, started.elapsed(), failed);
                        send_response(
                            &mut writer,
                            &response,
                            response_write_timeout(
                                started,
                                protocol_admission.request_timeout,
                                &response,
                            ),
                            &metrics,
                        )
                        .await?;
                    }
                    Err(error) => {
                        if std::str::from_utf8(line).is_err() {
                            metrics.record_rejected_request(
                                RequestOperation::InvalidRequest,
                                started.elapsed(),
                            );
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "request frame was not valid UTF-8",
                            )
                            .into());
                        }
                        let response = invalid_request_response(&error.to_string());
                        metrics.record_rejected_request(
                            RequestOperation::InvalidRequest,
                            started.elapsed(),
                        );
                        send_response(
                            &mut writer,
                            &response,
                            response_write_timeout(
                                started,
                                protocol_admission.request_timeout,
                                &response,
                            ),
                            &metrics,
                        )
                        .await?;
                    }
                }
            }
        }
    }
}
