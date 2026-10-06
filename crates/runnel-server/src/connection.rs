use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use runnel_engine::Engine;
#[cfg(feature = "instrumentation")]
use runnel_engine::StageTimer;
use runnel_protocol::v2::{
    self as v2, ApplicationReply, ClientFrame, Outcome, RefusalCode, ServerFrame, Stage,
};
use runnel_protocol::Response;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
use tracing::warn;
use zeroize::Zeroize;

#[path = "app_security.rs"]
pub(crate) mod app_security;

use crate::dispatch::handle_request_with_metadata;
use crate::observability::{ActiveConnection, ActiveRequest, RequestOperation, ServerMetrics};
use crate::protocol::{
    ProtocolAdmission, remaining_timeout, saturated_response, timeout_response,
    wait_for_request_data,
};

pub(crate) fn spawn(
    listener: TcpListener,
    engine: Arc<dyn Engine>,
    metrics: Arc<ServerMetrics>,
    protocol_admission: ProtocolAdmission,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<Result<(), std::io::Error>> {
    spawn_with_security(
        listener,
        engine,
        metrics,
        protocol_admission,
        app_security::ApplicationSecurity::development(),
        shutdown,
    )
}

pub(crate) fn spawn_with_security(
    listener: TcpListener,
    engine: Arc<dyn Engine>,
    metrics: Arc<ServerMetrics>,
    protocol_admission: ProtocolAdmission,
    security: app_security::ApplicationSecurity,
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
        security,
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
    security: app_security::ApplicationSecurity,
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
                        drop(stream);
                        warn!(%peer, "broker connection rejected at connection limit");
                        continue;
                    }
                };
                let engine = Arc::clone(&engine);
                let connection_metrics = Arc::clone(&metrics);
                let connection_request_slots = Arc::clone(&request_slots);
                let connection_shutdown = shutdown.clone();
                let connection_security = security.clone();
                connections.spawn(async move {
                    if let Err(error) = handle_connection(
                        stream,
                        engine,
                        Arc::clone(&connection_metrics),
                        connection_permit,
                        connection_request_slots,
                        protocol_admission,
                        connection_security,
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
    security: app_security::ApplicationSecurity,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    metrics.active_connections.fetch_add(1, Ordering::Relaxed);
    let _active_connection = ActiveConnection::new(Arc::clone(&metrics));

    let stream: Box<dyn ApplicationIo> = if let Some(acceptor) = security.tls_acceptor.clone() {
        match tokio::time::timeout(
            protocol_admission.request_timeout,
            acceptor.accept(stream),
        )
        .await
        {
            Ok(Ok(stream)) => Box::new(stream),
            Ok(Err(_)) => return Err("TLS handshake failed".into()),
            Err(_) => {
                metrics.request_timeouts.fetch_add(1, Ordering::Relaxed);
                return Err("TLS handshake timed out".into());
            }
        }
    } else {
        Box::new(stream)
    };
    let mut reader = BufReader::new(stream);

    let negotiation = tokio::time::timeout(protocol_admission.request_timeout, async {
        let mut preface = [0_u8; v2::PREFACE.len()];
        reader.read_exact(&mut preface).await?;
        if preface != v2::PREFACE {
            return Err(ConnectionProtocolError::InvalidPreface);
        }
        let body = read_bounded_frame(&mut reader, v2::HELLO_MAX_BODY_BYTES).await?;
        let hello = v2::decode_client_hello(&body)?;
        let server_hello = select_server_hello(&hello, protocol_admission, security.auth_required());
        let accepted = match &server_hello {
            v2::ServerHello::Accepted(accepted) => Some((
                accepted.client_to_server_frame_bytes,
                accepted.server_to_client_frame_bytes,
                accepted.auth_required.unwrap_or(false),
            )),
            v2::ServerHello::Refused { .. } => None,
        };
        let encoded = v2::encode_server_hello(&server_hello)?;
        write_encoded_frame(
            reader.get_mut(),
            &encoded,
            protocol_admission.request_timeout,
            &metrics,
        )
        .await?;
        let Some((max_request_bytes, max_response_bytes, auth_required)) = accepted else {
            return Ok::<_, ConnectionProtocolError>(None);
        };
        if auth_required {
            let auth_max = v2::AUTH_MAX_BODY_BYTES.min(max_request_bytes);
            let mut auth_body = read_bounded_frame(&mut reader, auth_max).await?;
            let auth_frame = v2::decode_client_frame(&auth_body, auth_max);
            auth_body.zeroize();
            let auth_frame = match auth_frame {
                Ok(frame) => frame,
                Err(_) => return Ok(None),
            };
            let role = match auth_frame {
                ClientFrame::BearerAuth(token) => security
                    .credentials
                    .as_ref()
                    .and_then(|credentials| credentials.authenticate(&token)),
                ClientFrame::Application(_) => None,
            };
            let Some(role) = role else {
                let failed = v2::encode_server_frame(&ServerFrame::AuthenticationFailed)?;
                write_encoded_frame(
                    reader.get_mut(),
                    &failed,
                    protocol_admission.request_timeout,
                    &metrics,
                )
                .await?;
                return Ok(None);
            };
            let authenticated = v2::encode_server_frame(&ServerFrame::Authenticated)?;
            write_encoded_frame(
                reader.get_mut(),
                &authenticated,
                protocol_admission.request_timeout,
                &metrics,
            )
            .await?;
            Ok(Some((max_request_bytes, max_response_bytes, Some(role))))
        } else {
            Ok(Some((max_request_bytes, max_response_bytes, None)))
        }
    })
    .await;
    let (max_request_bytes, max_response_bytes, authenticated_role) = match negotiation {
        Ok(Ok(Some(negotiated))) => negotiated,
        Ok(Ok(None)) | Ok(Err(_)) => return Ok(()),
        Err(_) => {
            metrics.request_timeouts.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
    };

    loop {
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
                read_bounded_frame(&mut reader, max_request_bytes),
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
        let mut body = match frame_result {
            Ok(Ok(body)) => body,
            Ok(Err(_)) => {
                metrics.record_rejected_request(RequestOperation::InvalidRequest, started.elapsed());
                return Ok(());
            }
            Err(_) => {
                metrics.request_timeouts.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        };

        metrics.request_bytes.fetch_add(body.len() as u64 + 4, Ordering::Relaxed);
        let client_frame = match v2::decode_client_frame(&body, max_request_bytes) {
            Ok(frame) => frame,
            Err(_) => {
                body.zeroize();
                metrics.record_rejected_request(RequestOperation::InvalidRequest, started.elapsed());
                return Ok(());
            }
        };
        let request = match client_frame {
            ClientFrame::BearerAuth(token) => {
                drop(token);
                body.zeroize();
                return Ok(());
            }
            ClientFrame::Application(request) => request,
        };
        let operation = RequestOperation::from_request(&request);
        let denied = security.credentials.as_ref().is_some_and(|_| {
            !app_security::request_is_authorized(
                authenticated_role.expect("secure connections authenticate before application"),
                &request,
            )
        });
        let reply = if denied {
            metrics.record_rejected_request(operation, started.elapsed());
            ApplicationReply::failed(
                Response::Error {
                    code: "authorization_denied".to_owned(),
                    message: "operation is not permitted for this credential role".to_owned(),
                },
                Outcome::Rejected,
                Stage::Validated,
            )
        } else {
            let request_permit = match Arc::clone(&request_slots).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    metrics.request_saturation_rejections.fetch_add(1, Ordering::Relaxed);
                    let reply = ApplicationReply::failed(
                        saturated_response(),
                        Outcome::Retryable,
                        Stage::Received,
                    );
                    metrics.record_rejected_request(operation, started.elapsed());
                    write_application_reply(
                        reader.get_mut(),
                        &reply,
                        max_response_bytes,
                        started,
                        protocol_admission.request_timeout,
                        &metrics,
                    )
                    .await?;
                    continue;
                }
            };
            let _request_permit = request_permit;
            metrics.active_requests.fetch_add(1, Ordering::Relaxed);
            let _active_request = ActiveRequest::new(Arc::clone(&metrics));
            #[cfg(feature = "instrumentation")]
            let _stage_timer = StageTimer::new("server.protocol_round_trip");
            match tokio::time::timeout(
                remaining_timeout(started, protocol_admission.request_timeout),
                handle_request_with_metadata(
                    engine.as_ref(),
                    request,
                    &metrics,
                    max_response_bytes,
                ),
            )
            .await
            {
                Ok(reply) => reply,
                Err(_) => {
                    metrics.request_timeouts.fetch_add(1, Ordering::Relaxed);
                    ApplicationReply::failed(
                        timeout_response(),
                        Outcome::Unknown,
                        Stage::ExecutionStarted,
                    )
                }
            }
        };
        let failed = reply.outcome.is_some_and(|outcome| outcome != Outcome::Confirmed)
            || matches!(&reply.response, Response::Error { .. });
        metrics.record_request(operation, started.elapsed(), failed);
        write_application_reply(
            reader.get_mut(),
            &reply,
            max_response_bytes,
            started,
            protocol_admission.request_timeout,
            &metrics,
        )
        .await?;
        }
}

trait ApplicationIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T> ApplicationIo for T where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send
{
}

#[derive(Debug)]
enum ConnectionProtocolError {
    Io,
    Codec,
    InvalidPreface,
    InvalidNegotiation,
}

impl From<std::io::Error> for ConnectionProtocolError {
    fn from(_: std::io::Error) -> Self {
        Self::Io
    }
}

impl From<v2::V2ProtocolError> for ConnectionProtocolError {
    fn from(_: v2::V2ProtocolError) -> Self {
        Self::Codec
    }
}

async fn read_bounded_frame<R>(reader: &mut R, max_bytes: usize) -> Result<Vec<u8>, ConnectionProtocolError>
where
    R: AsyncRead + Unpin,
{
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length).await?;
    let body_len = u32::from_be_bytes(length) as usize;
    if body_len == 0 || body_len > max_bytes {
        return Err(ConnectionProtocolError::InvalidNegotiation);
    }
    let mut body = vec![0; body_len];
    reader.read_exact(&mut body).await?;
    Ok(body)
}

fn select_server_hello(
    client: &v2::ClientHello,
    admission: ProtocolAdmission,
    auth_required: bool,
) -> v2::ServerHello {
    let refusal = |code| v2::ServerHello::Refused {
        code,
        diagnostic: match code {
            RefusalCode::InvalidHello => "Hello is invalid".to_owned(),
            RefusalCode::UnsupportedVersion => "no supported protocol version".to_owned(),
            RefusalCode::UnsupportedCapability => "required capability is unsupported".to_owned(),
            RefusalCode::LimitTooSmall => "frame limit is below the minimum".to_owned(),
            RefusalCode::LimitTooLarge => "frame limit exceeds the protocol maximum".to_owned(),
        },
    };
    if let Err(code) = v2::validate_client_hello(client) {
        return refusal(code);
    }
    if client.required_capabilities.iter().any(|_| true) {
        return refusal(RefusalCode::UnsupportedCapability);
    }
    if !client.versions.iter().any(|range| {
        range.major == v2::CURRENT_MAJOR
            && range.min_minor <= v2::CURRENT_MINOR
            && range.max_minor >= v2::CURRENT_MINOR
    }) {
        return refusal(RefusalCode::UnsupportedVersion);
    }
    if admission.max_request_bytes < v2::MIN_FRAME_BODY_BYTES {
        return refusal(RefusalCode::LimitTooSmall);
    }
    if admission.max_request_bytes > v2::MAX_CLIENT_TO_SERVER_FRAME_BYTES {
        return refusal(RefusalCode::LimitTooLarge);
    }
    let server_inbound = admission.max_request_bytes;
    let server_outbound = v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES;
    v2::ServerHello::Accepted(v2::HelloAccepted {
        major: v2::CURRENT_MAJOR,
        minor: v2::CURRENT_MINOR,
        capabilities: Vec::new(),
        max_inbound_frame_bytes: server_inbound,
        max_outbound_frame_bytes: server_outbound,
        client_to_server_frame_bytes: client
            .max_outbound_frame_bytes
            .min(server_inbound),
        server_to_client_frame_bytes: client
            .max_inbound_frame_bytes
            .min(server_outbound),
        auth_required: Some(auth_required),
    })
}

async fn write_encoded_frame<W>(
    writer: &mut W,
    frame: &v2::EncodedFrame,
    timeout: std::time::Duration,
    metrics: &ServerMetrics,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match tokio::time::timeout(timeout, writer.write_all(frame.as_bytes())).await {
        Ok(result) => result?,
        Err(_) => {
            metrics.response_write_timeouts.fetch_add(1, Ordering::Relaxed);
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "protocol response write timed out"));
        }
    }
    metrics.response_bytes.fetch_add(frame.len() as u64, Ordering::Relaxed);
    Ok(())
}

async fn write_application_reply<W>(
    writer: &mut W,
    reply: &ApplicationReply,
    max_response_bytes: usize,
    started: Instant,
    request_timeout: std::time::Duration,
    metrics: &ServerMetrics,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let frame = v2::encode_server_frame(&ServerFrame::Application(reply.clone()))
        .map_err(|_| std::io::Error::other("application response encoding failed"))?;
    let body_bytes = frame.len().saturating_sub(4);
    if body_bytes > max_response_bytes {
        let too_large = ApplicationReply::failed(
            Response::Error {
                code: "response_too_large".to_owned(),
                message: "response exceeds the negotiated maximum".to_owned(),
            },
            Outcome::Rejected,
            Stage::Validated,
        );
        let bounded = v2::encode_server_frame(&ServerFrame::Application(too_large))
            .map_err(|_| std::io::Error::other("bounded error response encoding failed"))?;
        return write_encoded_frame(
            writer,
            &bounded,
            remaining_timeout(started, request_timeout),
            metrics,
        )
        .await;
    }
    write_encoded_frame(
        writer,
        &frame,
        remaining_timeout(started, request_timeout),
        metrics,
    )
    .await
}
