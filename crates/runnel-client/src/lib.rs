use std::fmt;
use std::fs;
use std::io::{self, BufReader as IoBufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

use runnel_protocol::{
    AckBatchItemOutcome as WireAckBatchItemOutcome, BatchMessageResponse, BinaryPayload,
    MAX_CONSUME_BATCH_RECORDS, MAX_PUBLISH_BATCH_RECORDS,
    PublishBatchRecord as WirePublishBatchRecord, PublishBatchRecordResponse, Request, Response,
};
use runnel_protocol::v2::{
    self as v2, ApplicationReply, Outcome as V2Outcome, ServerFrame,
};
pub use runnel_protocol::{PayloadEncoding, ProtocolSupport, ProtocolVersionRange};
pub use runnel_protocol::BearerToken;
use rustls::RootCertStore;
use rustls::pki_types::ServerName;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_rustls::TlsConnector;
use zeroize::Zeroize;

/// Protocol compatibility declared by this client and its broker-facing types.
///
/// Connections negotiate this supported range on each v2 connection.
pub const PROTOCOL_SUPPORT: ProtocolSupport = runnel_protocol::PROTOCOL_SUPPORT;

/// Timeouts applied to each stage of a client connection and request.
#[derive(Debug, Clone, Copy)]
pub struct ClientConfig {
    /// Maximum time allowed to establish the TCP connection.
    pub connect_timeout: Duration,
    /// Maximum time allowed to write one complete application frame.
    pub request_timeout: Duration,
    /// Maximum time allowed to read one complete response frame.
    pub response_timeout: Duration,
    /// Maximum encoded Protobuf response body.
    ///
    /// The default is [`runnel_protocol::v2::DEFAULT_SERVER_TO_CLIENT_FRAME_BYTES`].
    /// Lower this bound when a client needs a smaller memory budget. A response
    /// that exceeds the bound invalidates the connection and is classified as
    /// an unknown operation outcome once its request may have been sent.
    pub max_response_bytes: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(30),
            max_response_bytes: v2::DEFAULT_SERVER_TO_CLIENT_FRAME_BYTES,
        }
    }
}

/// Errors returned while communicating with a broker.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connecting to the broker timed out after {timeout:?}")]
    ConnectTimeout { timeout: Duration },

    #[error("connecting to the broker failed: {source}")]
    Connect {
        #[source]
        source: io::Error,
    },

    #[error("TLS and token configuration is incomplete or invalid")]
    InvalidSecurityConfiguration,

    #[error("credential file is missing, unreadable, or not protected")]
    CredentialFile,

    #[error("server TLS certificate validation or handshake failed")]
    TlsHandshake,

    #[error("server TLS trust roots could not be loaded")]
    InvalidTrustRoots,

    #[error("server name is invalid for TLS verification")]
    InvalidServerName,

    #[error("server refused v2 negotiation: {code}")]
    HandshakeRefused { code: &'static str },

    #[error("server's v2 Hello response is invalid")]
    InvalidHello,

    #[error("server requires a bearer credential")]
    AuthenticationRequired,

    #[error("server authentication failed")]
    AuthenticationFailed,

    #[error("secure client configuration does not match the server Hello")]
    SecurityMismatch,

    #[error("v2 protocol frame is invalid")]
    Protocol,

    #[error("encoded request exceeds the negotiated frame limit of {max_bytes} bytes")]
    RequestTooLarge { max_bytes: usize },

    /// No usable connection remains after a failed or cancelled request.
    #[error("client connection is unavailable; reconnect before sending another request")]
    ConnectionUnavailable,

    #[error("request cannot be represented by the v2 protocol")]
    RequestEncoding,

    #[error("invalid publish batch: {message}")]
    InvalidBatch { message: String },

    #[error("writing request timed out after {timeout:?}")]
    WriteTimeout { timeout: Duration },

    #[error("writing request failed: {source}")]
    Write {
        #[source]
        source: io::Error,
    },

    #[error("reading response timed out after {timeout:?}")]
    ResponseTimeout { timeout: Duration },

    #[error("broker response exceeds the configured maximum of {max_bytes} bytes")]
    ResponseTooLarge { max_bytes: usize },

    #[error("maximum response size must be greater than zero")]
    InvalidResponseLimit,

    #[error("reading response failed: {source}")]
    Read {
        #[source]
        source: io::Error,
    },

    #[error("broker closed the connection before sending a response")]
    Eof,

    #[error("broker returned an invalid v2 response")]
    InvalidResponse,

    #[error("unexpected response for {operation}: {response:?}")]
    UnexpectedResponse {
        operation: &'static str,
        response: Box<Response>,
    },
}

/// TLS server identity and optional private trust roots for a broker connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTlsConfig {
    /// DNS name or IP literal that must appear in the server certificate.
    pub server_name: String,
    /// Optional PEM trust bundle added to the platform trust roots.
    pub ca_file: Option<PathBuf>,
}

impl ClientTlsConfig {
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            ca_file: None,
        }
    }

    pub fn with_ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_file = Some(path.into());
        self
    }
}

/// Per-connection TLS and bearer settings. A token is never accepted from an
/// argument or environment variable and is redacted from formatting.
pub struct ClientSecurityConfig {
    tls: Option<ClientTlsConfig>,
    token: Option<BearerToken>,
}

impl ClientSecurityConfig {
    pub fn plaintext_development() -> Self {
        Self {
            tls: None,
            token: None,
        }
    }

    pub fn with_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_token_file(mut self, path: impl AsRef<Path>) -> Result<Self, ClientError> {
        self.token = Some(read_token_file(path.as_ref())?);
        Ok(self)
    }

    pub fn with_bearer_token(mut self, token: BearerToken) -> Self {
        self.token = Some(token);
        self
    }

    pub fn has_tls(&self) -> bool {
        self.tls.is_some()
    }

    pub fn has_bearer_token(&self) -> bool {
        self.token.is_some()
    }
}

impl Default for ClientSecurityConfig {
    fn default() -> Self {
        Self::plaintext_development()
    }
}

impl fmt::Debug for ClientSecurityConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientSecurityConfig")
            .field("tls", &self.tls)
            .field("bearer_token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Optional fields for a text publish.
///
/// `request_id` is an application-provided identity. Reuse it with the same
/// key and payload bytes when explicitly resolving an ambiguous publish. A
/// changed-content reuse of a retained ID is rejected by brokers implementing
/// the current request-ID contract. This client never generates identities or
/// retries publishes automatically.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublishOptions {
    /// Optional ordering key attached to the message.
    pub key: Option<String>,
    /// Optional stable identity for explicitly resolving an ambiguous publish.
    pub request_id: Option<String>,
}

impl PublishOptions {
    /// Set the optional ordering key.
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the stable identity used for an explicitly retried publish.
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

/// The result of a stream-creation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCreation {
    /// The stream name acknowledged by the broker.
    pub stream: String,
    /// Whether this request created the stream (`false` means it already existed).
    pub created: bool,
}

/// The broker-assigned offset returned by a successful publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishReceipt {
    /// The stream that accepted the message.
    pub stream: String,
    /// The broker-assigned logical message offset.
    pub offset: u64,
}

/// One opaque record supplied to a publish batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchRecord {
    /// Optional ordering key attached to the message.
    pub key: Option<String>,
    /// Exact application payload bytes.
    pub payload: Vec<u8>,
    /// Optional stable identity for explicitly resolving an ambiguous record.
    pub request_id: Option<String>,
}

impl PublishBatchRecord {
    /// Construct a record with no key or retry identity.
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            key: None,
            payload: payload.into(),
            request_id: None,
        }
    }

    /// Construct a record with the same options used by single-record publishes.
    pub fn with_options(payload: impl Into<Vec<u8>>, options: PublishOptions) -> Self {
        Self {
            key: options.key,
            payload: payload.into(),
            request_id: options.request_id,
        }
    }

    /// Construct a UTF-8 record without changing its bytes.
    pub fn text(payload: impl Into<String>) -> Self {
        Self::new(payload.into().into_bytes())
    }
}

/// Per-record outcome from a publish-batch attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishBatchOutcome {
    /// The broker durably accepted this record.
    Confirmed(PublishReceipt),
    /// The broker definitely rejected this record.
    Rejected { code: String, message: String },
    /// The broker did not accept this record because the request may be retried safely.
    Retryable { code: String, message: String },
    /// The record may have been accepted; retry only with its stable request identity.
    Unknown { code: String, message: String },
}

/// Per-record results plus the whole-request failure, when no complete batch response arrived.
#[derive(Debug)]
pub struct PublishBatchAttempt {
    /// One outcome for every supplied record, in input order.
    pub outcomes: Vec<PublishBatchOutcome>,
    /// The whole-request failure behind repeated retryable or unknown outcomes, if any.
    pub attempt: Option<AttemptFailure>,
}

/// Explicit response, record-count, and collection-wait bounds for one consume batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeBatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_wait_ms: u64,
}

/// A per-record receipt returned by consume-batch polling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchDeliveryReceipt {
    pub offset: u64,
    pub delivery_token: String,
}

/// One acknowledgement result in the same order as the supplied receipts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchAcknowledgementItem {
    pub offset: u64,
    pub outcome: BatchAcknowledgementOutcome,
}

/// Independent result for one acknowledgement receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchAcknowledgementOutcome {
    Confirmed,
    AlreadyConfirmed,
    Rejected { code: String, message: String },
}

/// Ordered per-receipt outcomes from one acknowledgement-vector request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BatchAcknowledgement {
    pub outcomes: Vec<BatchAcknowledgementItem>,
}

/// A message returned by a successful poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The stream containing the message.
    pub stream: String,
    /// The consumer that received the message.
    pub consumer: String,
    /// The shared-consumer member that received the message, when grouped polling was used.
    pub member: Option<String>,
    /// The broker-assigned logical message offset.
    pub offset: u64,
    /// The optional ordering key attached to the message.
    pub key: Option<String>,
    /// The UTF-8 payload returned through the text convenience API.
    pub payload: String,
    /// The broker timestamp associated with the publish.
    pub published_at_ms: u64,
    /// The token required for a grouped acknowledgement, when present.
    pub delivery_token: Option<String>,
    /// The delivery attempt number, when the broker reports one.
    pub delivery_attempt: Option<u32>,
}

/// A message returned by a binary-aware poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryMessage {
    /// The stream containing the message.
    pub stream: String,
    /// The consumer that received the message.
    pub consumer: String,
    /// The shared-consumer member that received the message, when grouped polling was used.
    pub member: Option<String>,
    /// The broker-assigned logical message offset.
    pub offset: u64,
    /// The optional ordering key attached to the message.
    pub key: Option<String>,
    /// The exact application payload bytes returned by the broker.
    pub payload: Vec<u8>,
    /// The broker timestamp associated with the publish.
    pub published_at_ms: u64,
    /// The token required for a grouped acknowledgement, when present.
    pub delivery_token: Option<String>,
    /// The delivery attempt number, when the broker reports one.
    pub delivery_attempt: Option<u32>,
}

/// A message returned by an explicit replay read.
///
/// Replay messages are read-only and have no delivery token or attempt. They
/// must not be acknowledged through the ordinary consumer acknowledgement
/// methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayMessage {
    /// The stream containing the retained message.
    pub stream: String,
    /// The consumer identity used to scope this replay request.
    pub consumer: String,
    /// The broker-assigned logical message offset.
    pub offset: u64,
    /// The optional ordering key attached to the message.
    pub key: Option<String>,
    /// The UTF-8 payload returned by the text replay method.
    pub payload: String,
    /// The broker timestamp associated with the publish.
    pub published_at_ms: u64,
}

/// A replay message with the exact application payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryReplayMessage {
    /// The stream containing the retained message.
    pub stream: String,
    /// The consumer identity used to scope this replay request.
    pub consumer: String,
    /// The broker-assigned logical message offset.
    pub offset: u64,
    /// The optional ordering key attached to the message.
    pub key: Option<String>,
    /// The exact application payload bytes.
    pub payload: Vec<u8>,
    /// The broker timestamp associated with the publish.
    pub published_at_ms: u64,
}

/// The result of an acknowledgement request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acknowledgement {
    /// The acknowledged stream.
    pub stream: String,
    /// The acknowledged consumer.
    pub consumer: String,
    /// The acknowledged logical message offset.
    pub offset: u64,
    /// Whether the broker had already durably acknowledged this offset.
    pub already_acknowledged: bool,
}

/// The health snapshot returned by the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Health {
    /// The broker-reported health status.
    pub status: String,
    /// Number of streams known to the broker.
    pub streams: usize,
    /// Bytes currently occupied by broker storage.
    pub storage_bytes: u64,
}

/// Durable retry settings associated with one consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerPolicy {
    pub stream: String,
    pub consumer: String,
    pub version: u64,
    pub configured: bool,
    pub ack_timeout_ms: u64,
    pub max_delivery_attempts: Option<u32>,
    pub retry_delay_ms: u64,
}

/// The result classification for one v2 request attempt.
///
/// The classification is deliberately not serialized. It describes what the
/// client can safely infer from v2 response metadata and transport behavior.
#[derive(Debug)]
pub enum AttemptOutcome {
    /// The broker returned a non-error response, so the operation is confirmed.
    Confirmed(Response),
    /// The broker definitely rejected the request or the request could not be encoded locally.
    Rejected(AttemptFailure),
    /// Retrying on a new connection is safe according to the response or failure boundary.
    Retryable(AttemptFailure),
    /// The broker may have processed the request, so retrying can duplicate it.
    Unknown(AttemptFailure),
}

/// The response or client error behind a non-confirmed attempt classification.
#[derive(Debug)]
pub enum AttemptFailure {
    Broker(Response),
    Client(ClientError),
}

impl AttemptOutcome {
    /// Classify a failure that occurred before a request was attempted.
    ///
    /// Connection failures are retryable because no request bytes were sent.
    /// Callers should use [`Client::request_with_outcome`] for an established
    /// connection; failures from that method are classified conservatively as
    /// unknown once request writing may have started.
    pub fn from_client_error(error: ClientError) -> Self {
        match &error {
            ClientError::ConnectTimeout { .. }
            | ClientError::Connect { .. }
            | ClientError::ConnectionUnavailable => Self::Retryable(AttemptFailure::Client(error)),
            ClientError::RequestEncoding
            | ClientError::InvalidBatch { .. }
            | ClientError::InvalidResponseLimit
            | ClientError::InvalidSecurityConfiguration
            | ClientError::CredentialFile
            | ClientError::InvalidTrustRoots
            | ClientError::InvalidServerName
            | ClientError::HandshakeRefused { .. }
            | ClientError::InvalidHello
            | ClientError::AuthenticationRequired
            | ClientError::AuthenticationFailed
            | ClientError::SecurityMismatch
            | ClientError::RequestTooLarge { .. } => Self::Rejected(AttemptFailure::Client(error)),
            _ => Self::Unknown(AttemptFailure::Client(error)),
        }
    }

    /// Return the broker response carried by this outcome, if one exists.
    pub fn response(&self) -> Option<&Response> {
        match self {
            Self::Confirmed(response)
            | Self::Rejected(AttemptFailure::Broker(response))
            | Self::Retryable(AttemptFailure::Broker(response))
            | Self::Unknown(AttemptFailure::Broker(response)) => Some(response),
            Self::Rejected(AttemptFailure::Client(_))
            | Self::Retryable(AttemptFailure::Client(_))
            | Self::Unknown(AttemptFailure::Client(_)) => None,
        }
    }

    /// Return the client error carried by this outcome, if one exists.
    pub fn client_error(&self) -> Option<&ClientError> {
        match self {
            Self::Rejected(AttemptFailure::Client(error))
            | Self::Retryable(AttemptFailure::Client(error))
            | Self::Unknown(AttemptFailure::Client(error)) => Some(error),
            Self::Confirmed(_)
            | Self::Rejected(AttemptFailure::Broker(_))
            | Self::Retryable(AttemptFailure::Broker(_))
            | Self::Unknown(AttemptFailure::Broker(_)) => None,
        }
    }
}

/// A persistent, sequential client for the negotiated Runnel v2 protocol.
///
/// A client sends one request and reads one response per [`Client::request`] call.
/// Calls borrow the client mutably so responses cannot be interleaved. Use
/// [`Client::reconnect`] to replace a connection explicitly after a
/// connection-invalidating failure; reconnecting never retries a request.
/// The connection is invalidated automatically when a request cannot complete,
/// including when its future is cancelled after polling begins. This prevents
/// a later response from being mistaken for the response to a new request.
///
/// The typed convenience methods use the same connection and outcome rules as
/// [`Client::request_with_outcome`]. They return `Err(AttemptOutcome)` rather
/// than retrying or converting an ambiguous result into an ordinary error. If
/// a request future is cancelled after it may have started writing, treat its
/// outcome as unknown, discard or reconnect the client, and decide explicitly
/// whether a new request is safe. A cancelled reconnect leaves the existing
/// connection unchanged because the replacement is installed only after the
/// new connection succeeds.
pub struct Client {
    connection: Option<Connection>,
    config: ClientConfig,
    security: ClientSecurityConfig,
}

trait ApplicationIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ApplicationIo for T {}

struct Connection {
    reader: BufReader<ReadHalf<Box<dyn ApplicationIo>>>,
    writer: WriteHalf<Box<dyn ApplicationIo>>,
    max_request_bytes: usize,
    max_response_bytes: usize,
}

impl Client {
    /// Connect to a broker using the default timeout configuration.
    pub async fn connect(address: impl ToSocketAddrs) -> Result<Self, ClientError> {
        Self::connect_with_config(address, ClientConfig::default()).await
    }

    /// Connect to a broker using explicit connection, request, response-size, and response timeouts.
    pub async fn connect_with_config(
        address: impl ToSocketAddrs,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        Self::connect_with_security(
            address,
            config,
            ClientSecurityConfig::default(),
        )
        .await
    }

    /// Connect with explicit transport security, trust roots, credentials, and timeouts.
    pub async fn connect_with_security(
        address: impl ToSocketAddrs,
        config: ClientConfig,
        security: ClientSecurityConfig,
    ) -> Result<Self, ClientError> {
        if config.max_response_bytes < v2::MIN_FRAME_BODY_BYTES
            || config.max_response_bytes > v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES
        {
            return Err(ClientError::InvalidResponseLimit);
        }
        let connection = connect(address, config, &security).await?;

        Ok(Self {
            connection: Some(connection),
            config,
            security,
        })
    }

    /// Connect to a broker while classifying connection failures as retryable.
    pub async fn connect_with_outcome(address: impl ToSocketAddrs) -> Result<Self, AttemptOutcome> {
        Self::connect_with_config_outcome(address, ClientConfig::default()).await
    }

    /// Connect with explicit timeouts while classifying connection failures.
    pub async fn connect_with_config_outcome(
        address: impl ToSocketAddrs,
        config: ClientConfig,
    ) -> Result<Self, AttemptOutcome> {
        Self::connect_with_config(address, config)
            .await
            .map_err(AttemptOutcome::from_client_error)
    }

    /// Replace this client's TCP connection without retrying any request.
    ///
    /// The new connection is established before the current connection is
    /// replaced. A failed or cancelled reconnect therefore leaves the client
    /// unchanged when it already has a connection. After a request has
    /// returned a transport error or has been cancelled, the client has no
    /// usable connection until this method succeeds. Callers must treat that
    /// request's outcome according to [`AttemptOutcome`] and decide explicitly
    /// whether to issue a new request after reconnecting. In particular, this
    /// method never replays the failed request.
    pub async fn reconnect(&mut self, address: impl ToSocketAddrs) -> Result<(), ClientError> {
        let config = self.config;
        let connection = connect(address, config, &self.security).await?;
        self.connection = Some(connection);
        Ok(())
    }

    /// Send one request and read exactly one response from this connection.
    ///
    /// After a write, response timeout, read, EOF, or response-decoding error,
    /// the connection is discarded automatically. A broker result such as
    /// `response_too_large` is a completed protocol response and leaves the
    /// connection reusable. If this future is cancelled after
    /// polling begins, its connection is also discarded; the request outcome
    /// is unknown and must be resolved explicitly before retrying.
    ///
    /// A local request-encoding error occurs before the connection is taken and
    /// leaves an otherwise healthy connection available for reuse.
    pub async fn request(&mut self, request: &Request) -> Result<Response, ClientError> {
        self.request_with_reply(request)
            .await
            .map(|reply| reply.response)
    }

    /// Send one request and preserve its negotiated safety classification and stage.
    pub async fn request_with_reply(
        &mut self,
        request: &Request,
    ) -> Result<ApplicationReply, ClientError> {
        let request_bytes =
            v2::encode_application_request(request).map_err(|_| ClientError::RequestEncoding)?;
        let request_body_bytes = request_bytes.len().saturating_sub(4);
        let Some(current) = self.connection.as_ref() else {
            return Err(ClientError::ConnectionUnavailable);
        };
        if request_body_bytes > current.max_request_bytes {
            return Err(ClientError::RequestTooLarge {
                max_bytes: current.max_request_bytes,
            });
        }
        let mut connection = self
            .connection
            .take()
            .ok_or(ClientError::ConnectionUnavailable)?;
        let config = self.config;
        let result = async {
            tokio::time::timeout(
                config.request_timeout,
                connection.writer.write_all(request_bytes.as_bytes()),
            )
            .await
            .map_err(|_| ClientError::WriteTimeout {
                timeout: config.request_timeout,
            })?
            .map_err(|source| ClientError::Write { source })?;

            let max_response_bytes = connection.max_response_bytes;
            let response = tokio::time::timeout(
                config.response_timeout,
                read_v2_frame(&mut connection.reader, max_response_bytes),
            )
            .await
            .map_err(|_| ClientError::ResponseTimeout {
                timeout: config.response_timeout,
            })??;

            match v2::decode_server_frame(&response, max_response_bytes) {
                Ok(ServerFrame::Application(reply)) => Ok(reply),
                Ok(ServerFrame::AuthenticationFailed) => Err(ClientError::AuthenticationFailed),
                Ok(ServerFrame::Authenticated) => Err(ClientError::Protocol),
                Err(_) => Err(ClientError::InvalidResponse),
            }
        }
        .await;

        if result.is_ok() {
            self.connection = Some(connection);
        }
        result
    }

    /// Send one request and classify what can safely be inferred about its outcome.
    ///
    /// A successful non-error response is confirmed. Deterministic broker
    /// rejections are rejected, admission responses that are safe to retry on a
    /// new connection are retryable, and transport failures after this method
    /// starts writing are unknown. In particular, `request_timeout` remains
    /// unknown because the server uses it for both incomplete frames and
    /// engine work that may already have been applied. Cancellation does not
    /// produce an [`AttemptOutcome`]; callers must apply the same unknown
    /// outcome rule when cancellation may have followed a partial write.
    pub async fn request_with_outcome(&mut self, request: &Request) -> AttemptOutcome {
        match self.request_with_reply(request).await {
            Ok(reply) => classify_v2_reply(reply),
            Err(error) => AttemptOutcome::from_client_error(error),
        }
    }

    /// Create a stream and return whether it was newly created.
    ///
    /// No retry is performed. A retryable or unknown result is returned in
    /// the [`AttemptOutcome`] error so callers can reconnect and decide what
    /// to do explicitly.
    pub async fn create_stream(
        &mut self,
        stream: impl Into<String>,
    ) -> Result<StreamCreation, AttemptOutcome> {
        let stream = stream.into();
        self.request_typed(
            "create_stream",
            Request::CreateStream {
                stream: stream.clone(),
            },
            move |response| match response {
                Response::StreamCreated {
                    stream: response_stream,
                    created,
                } if response_stream == stream => Ok(StreamCreation {
                    stream: response_stream,
                    created,
                }),
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Publish a UTF-8 text payload with no ordering key or retry identity.
    pub async fn publish(
        &mut self,
        stream: impl Into<String>,
        payload: impl Into<String>,
    ) -> Result<PublishReceipt, AttemptOutcome> {
        self.publish_with_options(stream, payload, PublishOptions::default())
            .await
    }

    /// Publish a UTF-8 text payload with optional ordering and retry identity.
    ///
    /// This operation is never retried automatically. A stable
    /// `PublishOptions::request_id` lets an application explicitly retry an
    /// unknown publish. Reuse the same identity, key, and payload bytes; a
    /// changed-content reuse of a retained ID is a confirmed rejection.
    pub async fn publish_with_options(
        &mut self,
        stream: impl Into<String>,
        payload: impl Into<String>,
        options: PublishOptions,
    ) -> Result<PublishReceipt, AttemptOutcome> {
        let stream = stream.into();
        self.request_typed(
            "publish",
            Request::Publish {
                stream: stream.clone(),
                key: options.key,
                payload: payload.into(),
                request_id: options.request_id,
            },
            move |response| match response {
                Response::Published {
                    stream: response_stream,
                    offset,
                } if response_stream == stream => Ok(PublishReceipt {
                    stream: response_stream,
                    offset,
                }),
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Publish an opaque binary payload with no ordering key or retry identity.
    pub async fn publish_bytes(
        &mut self,
        stream: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<PublishReceipt, AttemptOutcome> {
        self.publish_bytes_with_options(stream, payload, PublishOptions::default())
            .await
    }

    /// Publish an opaque binary payload with optional ordering and retry identity.
    pub async fn publish_bytes_with_options(
        &mut self,
        stream: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        options: PublishOptions,
    ) -> Result<PublishReceipt, AttemptOutcome> {
        let stream = stream.into();
        self.request_typed(
            "publish",
            Request::PublishBytes {
                stream: stream.clone(),
                key: options.key,
                payload: BinaryPayload::new(payload),
                request_id: options.request_id,
            },
            move |response| match response {
                Response::Published {
                    stream: response_stream,
                    offset,
                } if response_stream == stream => Ok(PublishReceipt {
                    stream: response_stream,
                    offset,
                }),
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Publish a bounded batch of opaque binary records.
    ///
    /// Records are sent and processed in input order. The broker does not
    /// make the batch atomic: a completed response can contain both confirmed
    /// and rejected records. Reuse each record ID only with equivalent key
    /// and payload bytes; changed content is rejected independently. A
    /// transport, timeout, or leader-change failure
    /// returns one retry classification for every record because the broker
    /// may have processed a prefix. Reuse each record's `request_id` when
    /// resolving an unknown result.
    pub async fn publish_batch(
        &mut self,
        stream: impl Into<String>,
        records: impl IntoIterator<Item = PublishBatchRecord>,
    ) -> PublishBatchAttempt {
        let stream = stream.into();
        let records = records.into_iter().collect::<Vec<_>>();
        if records.is_empty() {
            return publish_batch_invalid(
                0,
                "publish batch must contain at least one record".to_owned(),
            );
        }
        if records.len() > MAX_PUBLISH_BATCH_RECORDS {
            return publish_batch_invalid(
                records.len(),
                format!("publish batch contains more than {MAX_PUBLISH_BATCH_RECORDS} records"),
            );
        }

        let record_count = records.len();
        let request = Request::PublishBatch {
            stream: stream.clone(),
            records: records
                .into_iter()
                .map(|record| WirePublishBatchRecord {
                    key: record.key,
                    payload: BinaryPayload::new(record.payload),
                    request_id: record.request_id,
                })
                .collect(),
        };
        let attempt = match self.request_with_reply(&request).await {
            Ok(reply) => publish_batch_reply(stream, record_count, reply),
            Err(error) => publish_batch_attempt(
                stream,
                record_count,
                AttemptOutcome::from_client_error(error),
            ),
        };
        if matches!(
            &attempt.attempt,
            Some(AttemptFailure::Client(
                ClientError::UnexpectedResponse { .. }
            ))
        ) {
            self.connection = None;
        }
        attempt
    }

    /// Poll a consumer, returning `None` when the broker reports an empty poll.
    ///
    /// Polling does not retry automatically. A cancelled or transport-failed
    /// poll must be treated according to its outcome and the connection should
    /// be replaced before another request when the request may have been sent.
    pub async fn poll(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
    ) -> Result<Option<Message>, AttemptOutcome> {
        self.poll_request("poll", stream.into(), consumer.into(), None)
            .await
    }

    /// Poll an ordinary consumer for one bounded active set of binary-safe messages.
    ///
    /// The caller supplies count, byte, and collection-wait limits. The byte
    /// limit is reduced to this client's configured response limit before the
    /// request is sent. Each returned message includes the receipt needed by
    /// `ack_batch`; a transport failure after sending remains an unknown poll
    /// outcome, so reconnect and repeat the same poll to resolve it.
    pub async fn poll_batch(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        limits: ConsumeBatchLimits,
    ) -> Result<Vec<BinaryMessage>, AttemptOutcome> {
        self.poll_batch_request("poll_batch", stream.into(), consumer.into(), None, limits)
            .await
    }

    /// Poll one shared-consumer member for one bounded active set.
    pub async fn poll_group_batch(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        member: impl Into<String>,
        limits: ConsumeBatchLimits,
    ) -> Result<Vec<BinaryMessage>, AttemptOutcome> {
        self.poll_batch_request(
            "poll_group_batch",
            stream.into(),
            consumer.into(),
            Some(member.into()),
            limits,
        )
        .await
    }

    /// Configure durable retry settings for one consumer.
    pub async fn configure_consumer(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        ack_timeout_ms: u64,
        max_delivery_attempts: Option<u32>,
        retry_delay_ms: u64,
    ) -> Result<ConsumerPolicy, AttemptOutcome> {
        let stream = stream.into();
        let consumer = consumer.into();
        self.request_typed(
            "configure_consumer",
            Request::ConfigureConsumer {
                stream: stream.clone(),
                consumer: consumer.clone(),
                ack_timeout_ms,
                max_delivery_attempts,
                retry_delay_ms,
            },
            move |response| match response {
                Response::ConsumerPolicy {
                    stream: response_stream,
                    consumer: response_consumer,
                    version,
                    configured,
                    ack_timeout_ms,
                    max_delivery_attempts,
                    retry_delay_ms,
                } if response_stream == stream && response_consumer == consumer => {
                    Ok(ConsumerPolicy {
                        stream: response_stream,
                        consumer: response_consumer,
                        version,
                        configured,
                        ack_timeout_ms,
                        max_delivery_attempts,
                        retry_delay_ms,
                    })
                }
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Inspect the durable retry settings for one consumer.
    pub async fn inspect_consumer(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
    ) -> Result<ConsumerPolicy, AttemptOutcome> {
        let stream = stream.into();
        let consumer = consumer.into();
        self.request_typed(
            "inspect_consumer",
            Request::InspectConsumer {
                stream: stream.clone(),
                consumer: consumer.clone(),
            },
            move |response| match response {
                Response::ConsumerPolicy {
                    stream: response_stream,
                    consumer: response_consumer,
                    version,
                    configured,
                    ack_timeout_ms,
                    max_delivery_attempts,
                    retry_delay_ms,
                } if response_stream == stream && response_consumer == consumer => {
                    Ok(ConsumerPolicy {
                        stream: response_stream,
                        consumer: response_consumer,
                        version,
                        configured,
                        ack_timeout_ms,
                        max_delivery_attempts,
                        retry_delay_ms,
                    })
                }
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Poll a shared consumer member, returning `None` when the broker reports
    /// an empty poll.
    pub async fn poll_group(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        member: impl Into<String>,
    ) -> Result<Option<Message>, AttemptOutcome> {
        self.poll_request(
            "poll_group",
            stream.into(),
            consumer.into(),
            Some(member.into()),
        )
        .await
    }

    /// Read one retained message at an inclusive logical offset without
    /// changing ordinary consumer progress.
    ///
    /// This first replay operation is intentionally one-record and
    /// offset-based. An unavailable offset is returned as a rejected broker
    /// outcome; it is never silently converted into an empty result.
    pub async fn replay(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        offset: u64,
    ) -> Result<ReplayMessage, AttemptOutcome> {
        let stream = stream.into();
        let consumer = consumer.into();
        self.request_typed(
            "replay",
            Request::Replay {
                stream: stream.clone(),
                consumer: consumer.clone(),
                offset,
            },
            move |response| match response {
                Response::ReplayMessage {
                    stream: response_stream,
                    consumer: response_consumer,
                    offset: response_offset,
                    key,
                    payload,
                    published_at_ms,
                } if response_stream == stream
                    && response_consumer == consumer
                    && response_offset == offset =>
                {
                    Ok(ReplayMessage {
                        stream: response_stream,
                        consumer: response_consumer,
                        offset: response_offset,
                        key,
                        payload,
                        published_at_ms,
                    })
                }
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Read one retained message at an inclusive logical offset with exact
    /// application payload bytes.
    pub async fn replay_bytes(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        offset: u64,
    ) -> Result<BinaryReplayMessage, AttemptOutcome> {
        let stream = stream.into();
        let consumer = consumer.into();
        self.request_typed(
            "replay_bytes",
            Request::Replay {
                stream: stream.clone(),
                consumer: consumer.clone(),
                offset,
            },
            move |response| match response {
                Response::ReplayMessage {
                    stream: response_stream,
                    consumer: response_consumer,
                    offset: response_offset,
                    key,
                    payload,
                    published_at_ms,
                } if response_stream == stream
                    && response_consumer == consumer
                    && response_offset == offset =>
                {
                    Ok(BinaryReplayMessage {
                        stream: response_stream,
                        consumer: response_consumer,
                        offset: response_offset,
                        key,
                        payload: payload.into_bytes(),
                        published_at_ms,
                    })
                }
                Response::ReplayMessageBytes {
                    stream: response_stream,
                    consumer: response_consumer,
                    offset: response_offset,
                    key,
                    payload,
                    published_at_ms,
                } if response_stream == stream
                    && response_consumer == consumer
                    && response_offset == offset =>
                {
                    Ok(BinaryReplayMessage {
                        stream: response_stream,
                        consumer: response_consumer,
                        offset: response_offset,
                        key,
                        payload: payload.into_bytes(),
                        published_at_ms,
                    })
                }
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Poll a consumer and return the exact application payload bytes.
    ///
    /// Text convenience responses are converted to their UTF-8 bytes. Binary
    /// responses preserve their exact Protobuf `bytes` field.
    pub async fn poll_bytes(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
    ) -> Result<Option<BinaryMessage>, AttemptOutcome> {
        self.poll_bytes_request("poll_bytes", stream.into(), consumer.into(), None)
            .await
    }

    /// Poll a shared consumer member and return the exact application payload bytes.
    pub async fn poll_group_bytes(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        member: impl Into<String>,
    ) -> Result<Option<BinaryMessage>, AttemptOutcome> {
        self.poll_bytes_request(
            "poll_group_bytes",
            stream.into(),
            consumer.into(),
            Some(member.into()),
        )
        .await
    }

    /// Acknowledge a message delivered by the ordinary scalar poll operation.
    ///
    /// A message assigned through `poll_batch` requires its per-record receipt
    /// and must be acknowledged through `ack_batch`; this offset-only method
    /// cannot bypass that batch delivery fence.
    pub async fn ack(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        offset: u64,
    ) -> Result<Acknowledgement, AttemptOutcome> {
        let stream = stream.into();
        let consumer = consumer.into();
        self.request_typed(
            "ack",
            Request::Ack {
                stream: stream.clone(),
                consumer: consumer.clone(),
                offset,
            },
            move |response| match response {
                Response::Acknowledged {
                    stream: response_stream,
                    consumer: response_consumer,
                    offset: response_offset,
                    already_acknowledged,
                } if response_stream == stream
                    && response_consumer == consumer
                    && response_offset == offset =>
                {
                    Ok(Acknowledgement {
                        stream: response_stream,
                        consumer: response_consumer,
                        offset: response_offset,
                        already_acknowledged,
                    })
                }
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Acknowledge a subset of an ordinary consumer's active batch.
    ///
    /// Every receipt is evaluated independently and the valid subset is
    /// durably applied as one engine transition. A lost whole-request response
    /// is unknown; retry the same receipts to resolve committed offsets.
    pub async fn ack_batch(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        receipts: impl IntoIterator<Item = BatchDeliveryReceipt>,
    ) -> Result<BatchAcknowledgement, AttemptOutcome> {
        self.ack_batch_request("ack_batch", stream.into(), consumer.into(), None, receipts)
            .await
    }

    /// Acknowledge a subset of one shared-consumer member's active batch.
    pub async fn ack_group_batch(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        member: impl Into<String>,
        receipts: impl IntoIterator<Item = BatchDeliveryReceipt>,
    ) -> Result<BatchAcknowledgement, AttemptOutcome> {
        self.ack_batch_request(
            "ack_group_batch",
            stream.into(),
            consumer.into(),
            Some(member.into()),
            receipts,
        )
        .await
    }

    /// Acknowledge a message delivered to a shared consumer member.
    pub async fn ack_group(
        &mut self,
        stream: impl Into<String>,
        consumer: impl Into<String>,
        member: impl Into<String>,
        offset: u64,
        delivery_token: impl Into<String>,
    ) -> Result<Acknowledgement, AttemptOutcome> {
        let stream = stream.into();
        let consumer = consumer.into();
        self.request_typed(
            "ack_group",
            Request::AckGroup {
                stream: stream.clone(),
                consumer: consumer.clone(),
                member: member.into(),
                offset,
                delivery_token: delivery_token.into(),
            },
            move |response| match response {
                Response::Acknowledged {
                    stream: response_stream,
                    consumer: response_consumer,
                    offset: response_offset,
                    already_acknowledged,
                } if response_stream == stream
                    && response_consumer == consumer
                    && response_offset == offset =>
                {
                    Ok(Acknowledgement {
                        stream: response_stream,
                        consumer: response_consumer,
                        offset: response_offset,
                        already_acknowledged,
                    })
                }
                response => Err(Box::new(response)),
            },
        )
        .await
    }

    /// Read the broker's current health snapshot.
    pub async fn health(&mut self) -> Result<Health, AttemptOutcome> {
        self.request_typed("health", Request::Health, |response| match response {
            Response::Health {
                status,
                streams,
                storage_bytes,
            } => Ok(Health {
                status,
                streams,
                storage_bytes,
            }),
            response => Err(Box::new(response)),
        })
        .await
    }

    async fn poll_request(
        &mut self,
        operation: &'static str,
        stream: String,
        consumer: String,
        member: Option<String>,
    ) -> Result<Option<Message>, AttemptOutcome> {
        let request = match member.as_ref() {
            Some(member) => Request::PollGroup {
                stream: stream.clone(),
                consumer: consumer.clone(),
                member: member.clone(),
            },
            None => Request::Poll {
                stream: stream.clone(),
                consumer: consumer.clone(),
            },
        };
        self.request_typed(operation, request, move |response| match response {
            Response::Message {
                stream: response_stream,
                consumer: response_consumer,
                member: response_member,
                offset,
                key,
                payload,
                published_at_ms,
                delivery_token,
                delivery_attempt,
            } if response_stream == stream
                && response_consumer == consumer
                && response_member.as_ref() == member.as_ref() =>
            {
                Ok(Some(Message {
                    stream: response_stream,
                    consumer: response_consumer,
                    member: response_member,
                    offset,
                    key,
                    payload,
                    published_at_ms,
                    delivery_token,
                    delivery_attempt,
                }))
            }
            Response::Empty {
                stream: response_stream,
                consumer: response_consumer,
            } if response_stream == stream && response_consumer == consumer => Ok(None),
            response => Err(Box::new(response)),
        })
        .await
    }

    async fn poll_batch_request(
        &mut self,
        operation: &'static str,
        stream: String,
        consumer: String,
        member: Option<String>,
        limits: ConsumeBatchLimits,
    ) -> Result<Vec<BinaryMessage>, AttemptOutcome> {
        if !(1..=MAX_CONSUME_BATCH_RECORDS).contains(&limits.max_records) {
            return Err(AttemptOutcome::Rejected(AttemptFailure::Client(
                ClientError::InvalidBatch {
                    message: format!(
                        "max_records must be between 1 and {MAX_CONSUME_BATCH_RECORDS}"
                    ),
                },
            )));
        }
        if limits.max_bytes == 0 {
            return Err(AttemptOutcome::Rejected(AttemptFailure::Client(
                ClientError::InvalidBatch {
                    message: "max_bytes must be greater than zero".to_owned(),
                },
            )));
        }
        let negotiated_response_bytes = self
            .connection
            .as_ref()
            .map_or(self.config.max_response_bytes, |connection| {
                connection.max_response_bytes
            });
        let max_bytes = limits
            .max_bytes
            .min(negotiated_response_bytes)
            .min(v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES);
        let empty_reply = ApplicationReply::batch(
            Response::PollBatch {
                stream: stream.clone(),
                consumer: consumer.clone(),
                messages: Vec::new(),
            },
            Vec::new(),
        );
        let minimum_bytes = v2::encode_server_frame(&ServerFrame::Application(empty_reply))
            .map(|frame| frame.len().saturating_sub(4))
            .map_err(|_| {
                AttemptOutcome::Rejected(AttemptFailure::Client(ClientError::Protocol))
            })?;
        if minimum_bytes > max_bytes {
            return Err(AttemptOutcome::Rejected(AttemptFailure::Client(
                ClientError::InvalidBatch {
                    message: "max_bytes is smaller than the empty response envelope".to_owned(),
                },
            )));
        }
        let request = match member.as_ref() {
            Some(member) => Request::PollGroupBatch {
                stream: stream.clone(),
                consumer: consumer.clone(),
                member: member.clone(),
                max_records: limits.max_records,
                max_bytes,
                max_wait_ms: limits.max_wait_ms,
            },
            None => Request::PollBatch {
                stream: stream.clone(),
                consumer: consumer.clone(),
                max_records: limits.max_records,
                max_bytes,
                max_wait_ms: limits.max_wait_ms,
            },
        };
        self.request_typed(operation, request, move |response| match response {
            Response::PollBatch {
                stream: response_stream,
                consumer: response_consumer,
                messages,
            } if response_stream == stream && response_consumer == consumer => {
                let decoded = messages
                    .into_iter()
                    .map(|message| {
                        decode_batch_message(message, &stream, &consumer, member.as_deref())
                    })
                    .collect::<Option<Vec<_>>>();
                decoded.ok_or_else(|| {
                    Box::new(Response::Error {
                        code: "invalid_batch_response".to_owned(),
                        message: "poll batch response contained invalid message metadata"
                            .to_owned(),
                    })
                })
            }
            response => Err(Box::new(response)),
        })
        .await
    }

    async fn ack_batch_request(
        &mut self,
        operation: &'static str,
        stream: String,
        consumer: String,
        member: Option<String>,
        receipts: impl IntoIterator<Item = BatchDeliveryReceipt>,
    ) -> Result<BatchAcknowledgement, AttemptOutcome> {
        let receipts = receipts.into_iter().collect::<Vec<_>>();
        if receipts.is_empty() || receipts.len() > MAX_CONSUME_BATCH_RECORDS {
            return Err(AttemptOutcome::Rejected(AttemptFailure::Client(
                ClientError::InvalidBatch {
                    message: format!(
                        "acknowledgement receipt count must be between 1 and {MAX_CONSUME_BATCH_RECORDS}"
                    ),
                },
            )));
        }
        let mut offsets = std::collections::HashSet::with_capacity(receipts.len());
        if let Some(duplicate) = receipts
            .iter()
            .map(|receipt| receipt.offset)
            .find(|offset| !offsets.insert(*offset))
        {
            return Err(AttemptOutcome::Rejected(AttemptFailure::Client(
                ClientError::InvalidBatch {
                    message: format!("duplicate acknowledgement offset {duplicate}"),
                },
            )));
        }
        let request = match member {
            Some(member) => Request::AckGroupBatch {
                stream: stream.clone(),
                consumer: consumer.clone(),
                member,
                receipts: receipts
                    .iter()
                    .map(|receipt| runnel_protocol::BatchDeliveryReceipt {
                        offset: receipt.offset,
                        delivery_token: receipt.delivery_token.clone(),
                    })
                    .collect(),
            },
            None => Request::AckBatch {
                stream: stream.clone(),
                consumer: consumer.clone(),
                receipts: receipts
                    .iter()
                    .map(|receipt| runnel_protocol::BatchDeliveryReceipt {
                        offset: receipt.offset,
                        delivery_token: receipt.delivery_token.clone(),
                    })
                    .collect(),
            },
        };
        let expected_offsets = receipts
            .into_iter()
            .map(|receipt| receipt.offset)
            .collect::<Vec<_>>();
        self.request_typed(operation, request, move |response| match response {
            Response::AckBatch {
                stream: response_stream,
                consumer: response_consumer,
                outcomes,
            } if response_stream == stream
                && response_consumer == consumer
                && outcomes.len() == expected_offsets.len()
                && outcomes
                    .iter()
                    .zip(&expected_offsets)
                    .all(|(outcome, offset)| outcome.offset == *offset) =>
            {
                Ok(BatchAcknowledgement {
                    outcomes: outcomes
                        .into_iter()
                        .map(|item| BatchAcknowledgementItem {
                            offset: item.offset,
                            outcome: match item.outcome {
                                WireAckBatchItemOutcome::Confirmed => {
                                    BatchAcknowledgementOutcome::Confirmed
                                }
                                WireAckBatchItemOutcome::AlreadyConfirmed => {
                                    BatchAcknowledgementOutcome::AlreadyConfirmed
                                }
                                WireAckBatchItemOutcome::Rejected => {
                                    BatchAcknowledgementOutcome::Rejected {
                                        code: item.code.unwrap_or_else(|| "rejected".to_owned()),
                                        message: item.message.unwrap_or_else(|| {
                                            "broker rejected the acknowledgement receipt".to_owned()
                                        }),
                                    }
                                }
                            },
                        })
                        .collect(),
                })
            }
            response => Err(Box::new(response)),
        })
        .await
    }

    async fn poll_bytes_request(
        &mut self,
        operation: &'static str,
        stream: String,
        consumer: String,
        member: Option<String>,
    ) -> Result<Option<BinaryMessage>, AttemptOutcome> {
        let request = match member.as_ref() {
            Some(member) => Request::PollGroup {
                stream: stream.clone(),
                consumer: consumer.clone(),
                member: member.clone(),
            },
            None => Request::Poll {
                stream: stream.clone(),
                consumer: consumer.clone(),
            },
        };
        self.request_typed(operation, request, move |response| match response {
            Response::Message {
                stream: response_stream,
                consumer: response_consumer,
                member: response_member,
                offset,
                key,
                payload,
                published_at_ms,
                delivery_token,
                delivery_attempt,
            } if response_stream == stream
                && response_consumer == consumer
                && response_member.as_ref() == member.as_ref() =>
            {
                Ok(Some(BinaryMessage {
                    stream: response_stream,
                    consumer: response_consumer,
                    member: response_member,
                    offset,
                    key,
                    payload: payload.into_bytes(),
                    published_at_ms,
                    delivery_token,
                    delivery_attempt,
                }))
            }
            Response::MessageBytes {
                stream: response_stream,
                consumer: response_consumer,
                member: response_member,
                offset,
                key,
                payload,
                published_at_ms,
                delivery_token,
                delivery_attempt,
            } if response_stream == stream
                && response_consumer == consumer
                && response_member.as_ref() == member.as_ref() =>
            {
                Ok(Some(BinaryMessage {
                    stream: response_stream,
                    consumer: response_consumer,
                    member: response_member,
                    offset,
                    key,
                    payload: payload.into_bytes(),
                    published_at_ms,
                    delivery_token,
                    delivery_attempt,
                }))
            }
            Response::Empty {
                stream: response_stream,
                consumer: response_consumer,
            } if response_stream == stream && response_consumer == consumer => Ok(None),
            response => Err(Box::new(response)),
        })
        .await
    }

    async fn request_typed<T>(
        &mut self,
        operation: &'static str,
        request: Request,
        parse: impl FnOnce(Response) -> Result<T, Box<Response>>,
    ) -> Result<T, AttemptOutcome> {
        match typed_response(operation, self.request_with_outcome(&request).await, parse) {
            TypedResponse::Value(value) => Ok(value),
            TypedResponse::Outcome(outcome) => {
                if unexpected_response(&outcome) {
                    self.connection = None;
                }
                Err(outcome)
            }
        }
    }
}

fn unexpected_response(outcome: &AttemptOutcome) -> bool {
    matches!(
        outcome,
        AttemptOutcome::Unknown(AttemptFailure::Client(
            ClientError::UnexpectedResponse { .. }
        ))
    )
}

async fn connect(
    address: impl ToSocketAddrs,
    config: ClientConfig,
    security: &ClientSecurityConfig,
) -> Result<Connection, ClientError> {
    if security.token.is_some() && security.tls.is_none() {
        return Err(ClientError::InvalidSecurityConfiguration);
    }
    let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(address))
        .await
        .map_err(|_| ClientError::ConnectTimeout {
            timeout: config.connect_timeout,
        })?
        .map_err(|source| ClientError::Connect { source })?;

    let stream: Box<dyn ApplicationIo> = if let Some(tls) = &security.tls {
        let name = ServerName::try_from(tls.server_name.clone())
            .map_err(|_| ClientError::InvalidServerName)?;
        let tls_config = client_tls_config(tls)?;
        let connector = TlsConnector::from(tls_config);
        let stream = tokio::time::timeout(config.connect_timeout, connector.connect(name, stream))
            .await
            .map_err(|_| ClientError::TlsHandshake)?
            .map_err(|_| ClientError::TlsHandshake)?;
        Box::new(stream)
    } else {
        Box::new(stream)
    };

    let (reader, writer) = tokio::io::split(stream);
    let mut connection = Connection {
        reader: BufReader::new(reader),
        writer,
        max_request_bytes: v2::MAX_CLIENT_TO_SERVER_FRAME_BYTES,
        max_response_bytes: config.max_response_bytes,
    };
    negotiate(&mut connection, config, security).await?;
    Ok(connection)
}

async fn negotiate(
    connection: &mut Connection,
    config: ClientConfig,
    security: &ClientSecurityConfig,
) -> Result<(), ClientError> {
    let mut hello = v2::ClientHello::core_v2();
    hello.max_inbound_frame_bytes = config.max_response_bytes;
    let hello_frame = v2::encode_client_hello(&hello).map_err(|_| ClientError::InvalidHello)?;
    tokio::time::timeout(config.request_timeout, async {
        connection.writer.write_all(&v2::PREFACE).await?;
        connection.writer.write_all(hello_frame.as_bytes()).await
    })
    .await
    .map_err(|_| ClientError::WriteTimeout {
        timeout: config.request_timeout,
    })?
    .map_err(|source| ClientError::Write { source })?;

    let hello_body = tokio::time::timeout(
        config.response_timeout,
        read_v2_frame(&mut connection.reader, v2::HELLO_MAX_BODY_BYTES),
    )
    .await
    .map_err(|_| ClientError::ResponseTimeout {
        timeout: config.response_timeout,
    })??;
    let server_hello = v2::decode_server_hello(&hello_body).map_err(|_| ClientError::InvalidHello)?;
    let v2::ServerHello::Accepted(accepted) = server_hello else {
        let v2::ServerHello::Refused { code, .. } = server_hello else {
            unreachable!()
        };
        return Err(ClientError::HandshakeRefused {
            code: refusal_name(code),
        });
    };
    validate_server_hello(&hello, &accepted)?;
    let auth_required = accepted.auth_required.ok_or(ClientError::InvalidHello)?;
    if !auth_required && (security.tls.is_some() || security.token.is_some()) {
        return Err(ClientError::SecurityMismatch);
    }
    if auth_required {
        let token = security
            .token
            .as_ref()
            .ok_or(ClientError::AuthenticationRequired)?;
        let auth_frame = v2::encode_bearer_auth(token)
            .map_err(|_| ClientError::InvalidSecurityConfiguration)?;
        tokio::time::timeout(config.request_timeout, connection.writer.write_all(auth_frame.as_bytes()))
            .await
            .map_err(|_| ClientError::WriteTimeout {
                timeout: config.request_timeout,
            })?
            .map_err(|source| ClientError::Write { source })?;
        let reply = tokio::time::timeout(
            config.response_timeout,
            read_v2_frame(&mut connection.reader, v2::AUTH_MAX_BODY_BYTES),
        )
        .await
        .map_err(|_| ClientError::ResponseTimeout {
            timeout: config.response_timeout,
        })??;
        match v2::decode_server_frame(&reply, v2::AUTH_MAX_BODY_BYTES)
            .map_err(|_| ClientError::Protocol)?
        {
            ServerFrame::Authenticated => {}
            ServerFrame::AuthenticationFailed => return Err(ClientError::AuthenticationFailed),
            ServerFrame::Application(_) => return Err(ClientError::Protocol),
        }
    }
    connection.max_request_bytes = accepted.client_to_server_frame_bytes;
    connection.max_response_bytes = accepted.server_to_client_frame_bytes;
    Ok(())
}

fn validate_server_hello(
    client: &v2::ClientHello,
    server: &v2::HelloAccepted,
) -> Result<(), ClientError> {
    if server.major != v2::CURRENT_MAJOR
        || !client.versions.iter().any(|range| {
            range.major == server.major
                && server.minor >= range.min_minor
                && server.minor <= range.max_minor
        })
        || server.max_inbound_frame_bytes < v2::MIN_FRAME_BODY_BYTES
        || server.max_inbound_frame_bytes > v2::MAX_CLIENT_TO_SERVER_FRAME_BYTES
        || server.max_outbound_frame_bytes < v2::MIN_FRAME_BODY_BYTES
        || server.max_outbound_frame_bytes > v2::MAX_SERVER_TO_CLIENT_FRAME_BYTES
        || server.capabilities.iter().any(|capability| {
            !client.offered_capabilities.contains(capability)
        })
        || !client
            .required_capabilities
            .iter()
            .all(|capability| server.capabilities.contains(capability))
    {
        return Err(ClientError::InvalidHello);
    }
    let client_to_server = client
        .max_outbound_frame_bytes
        .min(server.max_inbound_frame_bytes);
    let server_to_client = client
        .max_inbound_frame_bytes
        .min(server.max_outbound_frame_bytes);
    if server.client_to_server_frame_bytes != client_to_server
        || server.server_to_client_frame_bytes != server_to_client
        || client_to_server < v2::MIN_FRAME_BODY_BYTES
        || server_to_client < v2::MIN_FRAME_BODY_BYTES
        || server.auth_required.is_none()
    {
        return Err(ClientError::InvalidHello);
    }
    Ok(())
}

fn refusal_name(code: v2::RefusalCode) -> &'static str {
    match code {
        v2::RefusalCode::InvalidHello => "invalid_hello",
        v2::RefusalCode::UnsupportedVersion => "unsupported_version",
        v2::RefusalCode::UnsupportedCapability => "unsupported_capability",
        v2::RefusalCode::LimitTooSmall => "limit_too_small",
        v2::RefusalCode::LimitTooLarge => "limit_too_large",
    }
}

fn client_tls_config(tls: &ClientTlsConfig) -> Result<std::sync::Arc<rustls::ClientConfig>, ClientError> {
    let mut roots = RootCertStore::empty();
    let system = rustls_native_certs::load_native_certs();
    if !system.errors.is_empty() || system.certs.is_empty() {
        return Err(ClientError::InvalidTrustRoots);
    }
    for certificate in system.certs {
        roots
            .add(certificate)
            .map_err(|_| ClientError::InvalidTrustRoots)?;
    }
    if let Some(path) = &tls.ca_file {
        let file = fs::File::open(path).map_err(|_| ClientError::InvalidTrustRoots)?;
        let certificates = rustls_pemfile::certs(&mut IoBufReader::new(file))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ClientError::InvalidTrustRoots)?;
        if certificates.is_empty() {
            return Err(ClientError::InvalidTrustRoots);
        }
        for certificate in certificates {
            roots
                .add(certificate)
                .map_err(|_| ClientError::InvalidTrustRoots)?;
        }
    }
    let mut config = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.enable_early_data = false;
    Ok(std::sync::Arc::new(config))
}

async fn read_v2_frame<R>(reader: &mut R, max_body_bytes: usize) -> Result<Vec<u8>, ClientError>
where
    R: AsyncRead + Unpin,
{
    let mut length_bytes = [0; 4];
    reader.read_exact(&mut length_bytes).await.map_err(map_read_error)?;
    let body_bytes = u32::from_be_bytes(length_bytes) as usize;
    if body_bytes == 0 || body_bytes > max_body_bytes {
        return Err(ClientError::ResponseTooLarge {
            max_bytes: max_body_bytes,
        });
    }
    let mut body = vec![0; body_bytes];
    reader.read_exact(&mut body).await.map_err(map_read_error)?;
    Ok(body)
}

fn map_read_error(source: io::Error) -> ClientError {
    if source.kind() == io::ErrorKind::UnexpectedEof {
        ClientError::Eof
    } else {
        ClientError::Read { source }
    }
}

fn read_token_file(path: &Path) -> Result<BearerToken, ClientError> {
    let file = fs::File::open(path).map_err(|_| ClientError::CredentialFile)?;
    let metadata = file
        .metadata()
        .map_err(|_| ClientError::CredentialFile)?;
    if !metadata.is_file() || metadata.len() > 256 {
        return Err(ClientError::CredentialFile);
    }
    validate_credential_file_permissions(&metadata)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(257)
        .read_to_end(&mut bytes)
        .map_err(|_| ClientError::CredentialFile)?;
    if bytes.len() > 256 {
        bytes.zeroize();
        return Err(ClientError::CredentialFile);
    }
    if bytes.ends_with(b"\r\n") {
        bytes.truncate(bytes.len() - 2);
    } else if bytes.ends_with(b"\n") {
        bytes.truncate(bytes.len() - 1);
    }
    let token = match std::str::from_utf8(&bytes) {
        Ok(value) => BearerToken::parse(value.to_owned()).map_err(|_| ClientError::CredentialFile),
        Err(_) => Err(ClientError::CredentialFile),
    };
    bytes.zeroize();
    token
}

#[cfg(unix)]
fn validate_credential_file_permissions(metadata: &fs::Metadata) -> Result<(), ClientError> {
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(ClientError::CredentialFile);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_credential_file_permissions(_: &fs::Metadata) -> Result<(), ClientError> {
    Ok(())
}

fn classify_v2_reply(reply: ApplicationReply) -> AttemptOutcome {
    let ApplicationReply {
        response,
        outcome,
        stage: _,
        items: _,
    } = reply;
    match outcome {
        Some(V2Outcome::Confirmed) if !matches!(response, Response::Error { .. }) => {
            AttemptOutcome::Confirmed(response)
        }
        Some(V2Outcome::Rejected) => {
            AttemptOutcome::Rejected(AttemptFailure::Broker(response))
        }
        Some(V2Outcome::Retryable) => {
            AttemptOutcome::Retryable(AttemptFailure::Broker(response))
        }
        Some(V2Outcome::Unknown) | Some(V2Outcome::Confirmed) | None => {
            AttemptOutcome::Unknown(AttemptFailure::Broker(response))
        }
    }
}

#[derive(Clone, Copy)]
enum BatchFailureKind {
    Rejected,
    Retryable,
    Unknown,
}

fn publish_batch_invalid(record_count: usize, message: String) -> PublishBatchAttempt {
    let error = AttemptFailure::Client(ClientError::InvalidBatch { message });
    publish_batch_failure(record_count, BatchFailureKind::Rejected, error)
}

fn publish_batch_attempt(
    _stream: String,
    record_count: usize,
    outcome: AttemptOutcome,
) -> PublishBatchAttempt {
    match outcome {
        AttemptOutcome::Confirmed(response) => match response {
            response => publish_batch_unexpected(record_count, response),
        },
        AttemptOutcome::Rejected(failure) => {
            publish_batch_failure(record_count, BatchFailureKind::Rejected, failure)
        }
        AttemptOutcome::Retryable(failure) => {
            publish_batch_failure(record_count, BatchFailureKind::Retryable, failure)
        }
        AttemptOutcome::Unknown(failure) => {
            publish_batch_failure(record_count, BatchFailureKind::Unknown, failure)
        }
    }
}

fn publish_batch_unexpected(record_count: usize, response: Response) -> PublishBatchAttempt {
    let failure = AttemptFailure::Client(ClientError::UnexpectedResponse {
        operation: "publish_batch",
        response: Box::new(response),
    });
    publish_batch_failure(record_count, BatchFailureKind::Unknown, failure)
}

fn publish_batch_failure(
    record_count: usize,
    kind: BatchFailureKind,
    failure: AttemptFailure,
) -> PublishBatchAttempt {
    let (code, message) = attempt_failure_details(&failure);
    let outcomes = (0..record_count)
        .map(|_| publish_batch_record_failure(kind, code.clone(), message.clone()))
        .collect();
    PublishBatchAttempt {
        outcomes,
        attempt: Some(failure),
    }
}

fn publish_batch_record_failure(
    kind: BatchFailureKind,
    code: String,
    message: String,
) -> PublishBatchOutcome {
    match kind {
        BatchFailureKind::Rejected => PublishBatchOutcome::Rejected { code, message },
        BatchFailureKind::Retryable => PublishBatchOutcome::Retryable { code, message },
        BatchFailureKind::Unknown => PublishBatchOutcome::Unknown { code, message },
    }
}

fn publish_batch_reply(
    stream: String,
    record_count: usize,
    reply: ApplicationReply,
) -> PublishBatchAttempt {
    if reply.outcome.is_some() {
        return publish_batch_attempt(stream, record_count, classify_v2_reply(reply));
    }
    let ApplicationReply {
        response,
        items,
        ..
    } = reply;
    let Response::PublishBatch {
        stream: response_stream,
        outcomes: wire_outcomes,
    } = response
    else {
        return publish_batch_unexpected(record_count, response);
    };
    if response_stream != stream || items.len() != record_count || wire_outcomes.len() != record_count {
        return publish_batch_unexpected(
            record_count,
            Response::PublishBatch {
                stream: response_stream,
                outcomes: wire_outcomes,
            },
        );
    }

    let outcomes = items
        .into_iter()
        .zip(wire_outcomes)
        .map(|(metadata, outcome)| match (metadata.outcome, outcome) {
            (V2Outcome::Confirmed, PublishBatchRecordResponse::Published { offset }) => {
                Some(PublishBatchOutcome::Confirmed(PublishReceipt {
                    stream: stream.clone(),
                    offset,
                }))
            }
            (
                V2Outcome::Rejected | V2Outcome::Retryable | V2Outcome::Unknown,
                PublishBatchRecordResponse::Error { code, message },
            ) => Some(publish_batch_record_failure(
                match metadata.outcome {
                    V2Outcome::Rejected => BatchFailureKind::Rejected,
                    V2Outcome::Retryable => BatchFailureKind::Retryable,
                    V2Outcome::Unknown => BatchFailureKind::Unknown,
                    V2Outcome::Confirmed => unreachable!(),
                },
                code,
                message,
            )),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    match outcomes {
        Some(outcomes) => PublishBatchAttempt {
            outcomes,
            attempt: None,
        },
        None => publish_batch_unexpected(
            record_count,
            Response::PublishBatch {
                stream: response_stream,
                outcomes: Vec::new(),
            },
        ),
    }
}

fn attempt_failure_details(failure: &AttemptFailure) -> (String, String) {
    match failure {
        AttemptFailure::Broker(Response::Error { code, message }) => {
            (code.clone(), message.clone())
        }
        AttemptFailure::Broker(response) => (
            "unexpected_response".to_owned(),
            format!("unexpected response: {response:?}"),
        ),
        AttemptFailure::Client(error) => ("client_error".to_owned(), error.to_string()),
    }
}

fn decode_batch_message(
    message: BatchMessageResponse,
    stream: &str,
    consumer: &str,
    member: Option<&str>,
) -> Option<BinaryMessage> {
    let decoded = match message {
        BatchMessageResponse::Text {
            stream: response_stream,
            consumer: response_consumer,
            member: response_member,
            offset,
            key,
            payload,
            published_at_ms,
            delivery_token,
            delivery_attempt,
        } => BinaryMessage {
            stream: response_stream,
            consumer: response_consumer,
            member: response_member,
            offset,
            key,
            payload: payload.into_bytes(),
            published_at_ms,
            delivery_token,
            delivery_attempt,
        },
        BatchMessageResponse::Bytes {
            stream: response_stream,
            consumer: response_consumer,
            member: response_member,
            offset,
            key,
            payload,
            published_at_ms,
            delivery_token,
            delivery_attempt,
        } => BinaryMessage {
            stream: response_stream,
            consumer: response_consumer,
            member: response_member,
            offset,
            key,
            payload: payload.into_bytes(),
            published_at_ms,
            delivery_token,
            delivery_attempt,
        },
    };
    if decoded.stream != stream
        || decoded.consumer != consumer
        || decoded.member.as_deref() != member
        || decoded.delivery_token.is_none()
        || decoded.delivery_attempt.is_none()
    {
        return None;
    }
    Some(decoded)
}

enum TypedResponse<T> {
    Value(T),
    Outcome(AttemptOutcome),
}

fn typed_response<T>(
    operation: &'static str,
    outcome: AttemptOutcome,
    parse: impl FnOnce(Response) -> Result<T, Box<Response>>,
) -> TypedResponse<T> {
    match outcome {
        AttemptOutcome::Confirmed(response) => match parse(response) {
            Ok(value) => TypedResponse::Value(value),
            Err(response) => TypedResponse::Outcome(AttemptOutcome::Unknown(
                AttemptFailure::Client(ClientError::UnexpectedResponse {
                    operation,
                    response,
                }),
            )),
        },
        outcome => TypedResponse::Outcome(outcome),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use runnel_protocol::v2::{
        self as v2, ClientFrame, ClientHello, HelloAccepted, ServerHello, VersionRange,
        DEFAULT_SERVER_TO_CLIENT_FRAME_BYTES, HELLO_MAX_BODY_BYTES,
        MAX_CLIENT_TO_SERVER_FRAME_BYTES, MAX_SERVER_TO_CLIENT_FRAME_BYTES, PREFACE,
    };
    use tokio::net::TcpListener;

    fn test_config() -> ClientConfig {
        ClientConfig {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            response_timeout: Duration::from_millis(200),
            max_response_bytes: 64 * 1024,
        }
    }

    async fn listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        (listener, address)
    }

    async fn read_frame<R>(reader: &mut R, maximum: usize) -> io::Result<Vec<u8>>
    where
        R: AsyncRead + Unpin,
    {
        let mut length = [0; 4];
        reader.read_exact(&mut length).await?;
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 || length > maximum {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "frame length"));
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).await?;
        Ok(body)
    }

    async fn accept_v2(
        listener: &TcpListener,
        request_limit: usize,
        auth_required: bool,
    ) -> tokio::io::BufReader<tokio::net::TcpStream> {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = tokio::io::BufReader::new(stream);
        let mut preface = [0; PREFACE.len()];
        reader.read_exact(&mut preface).await.unwrap();
        assert_eq!(preface, PREFACE);

        let hello_body = read_frame(&mut reader, HELLO_MAX_BODY_BYTES).await.unwrap();
        let hello = v2::decode_client_hello(&hello_body).unwrap();
        let inbound = request_limit.min(MAX_CLIENT_TO_SERVER_FRAME_BYTES);
        let outbound = MAX_SERVER_TO_CLIENT_FRAME_BYTES;
        let accepted = HelloAccepted {
            major: v2::CURRENT_MAJOR,
            minor: v2::CURRENT_MINOR,
            capabilities: Vec::new(),
            max_inbound_frame_bytes: inbound,
            max_outbound_frame_bytes: outbound,
            client_to_server_frame_bytes: hello.max_outbound_frame_bytes.min(inbound),
            server_to_client_frame_bytes: hello.max_inbound_frame_bytes.min(outbound),
            auth_required: Some(auth_required),
        };
        let hello = v2::encode_server_hello(&ServerHello::Accepted(accepted)).unwrap();
        reader.get_mut().write_all(hello.as_bytes()).await.unwrap();
        reader
    }

    async fn read_application_request(
        reader: &mut tokio::io::BufReader<tokio::net::TcpStream>,
    ) -> Request {
        let body = read_frame(reader, MAX_CLIENT_TO_SERVER_FRAME_BYTES)
            .await
            .unwrap();
        match v2::decode_client_frame(&body, MAX_CLIENT_TO_SERVER_FRAME_BYTES).unwrap() {
            ClientFrame::Application(request) => request,
            ClientFrame::BearerAuth(_) => panic!("unexpected auth frame"),
        }
    }

    async fn write_response(
        reader: &mut tokio::io::BufReader<tokio::net::TcpStream>,
        response: Response,
    ) {
        let reply = if matches!(response, Response::Error { .. }) {
            ApplicationReply::failed(response, V2Outcome::Rejected, v2::Stage::Validated)
        } else {
            ApplicationReply::confirmed(response, false)
        };
        let frame = v2::encode_server_frame(&ServerFrame::Application(reply)).unwrap();
        reader.get_mut().write_all(frame.as_bytes()).await.unwrap();
    }

    #[tokio::test]
    async fn negotiates_v2_and_reuses_one_persistent_connection() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let mut reader = accept_v2(&listener, MAX_CLIENT_TO_SERVER_FRAME_BYTES, false).await;
            assert!(matches!(read_application_request(&mut reader).await, Request::Health));
            write_response(
                &mut reader,
                Response::Health {
                    status: "ok".to_owned(),
                    streams: 0,
                    storage_bytes: 0,
                },
            )
            .await;
            assert!(matches!(
                read_application_request(&mut reader).await,
                Request::CreateStream { stream } if stream == "events"
            ));
            write_response(
                &mut reader,
                Response::StreamCreated {
                    stream: "events".to_owned(),
                    created: true,
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config()).await.unwrap();
        assert!(matches!(
            client.request(&Request::Health).await.unwrap(),
            Response::Health { status, .. } if status == "ok"
        ));
        assert!(matches!(
            client.request(&Request::CreateStream { stream: "events".to_owned() }).await.unwrap(),
            Response::StreamCreated { created: true, .. }
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn does_not_send_application_data_when_server_requires_missing_credentials() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let mut reader = accept_v2(&listener, MAX_CLIENT_TO_SERVER_FRAME_BYTES, true).await;
            let mut length = [0; 4];
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                reader.read_exact(&mut length),
            )
            .await;
            assert!(matches!(result, Ok(Err(error)) if error.kind() == io::ErrorKind::UnexpectedEof));
        });

        let result = Client::connect_with_config(address, test_config()).await;
        assert!(matches!(result, Err(ClientError::AuthenticationRequired)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_larger_than_negotiated_limit_is_rejected_before_write() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let mut reader = accept_v2(&listener, 1_024, false).await;
            assert!(matches!(read_application_request(&mut reader).await, Request::Health));
            write_response(
                &mut reader,
                Response::Health {
                    status: "ok".to_owned(),
                    streams: 0,
                    storage_bytes: 0,
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config()).await.unwrap();
        let result = client
            .request(&Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "x".repeat(2_048),
                request_id: None,
            })
            .await;
        assert!(matches!(result, Err(ClientError::RequestTooLarge { max_bytes: 1_024 })));
        assert!(matches!(client.request(&Request::Health).await.unwrap(), Response::Health { .. }));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_a_zero_response_limit_before_connecting() {
        let result = Client::connect_with_config(
            "127.0.0.1:1",
            ClientConfig { max_response_bytes: 0, ..test_config() },
        )
        .await;
        assert!(matches!(result, Err(ClientError::InvalidResponseLimit)));
    }

    #[test]
    fn bearer_credentials_are_redacted_from_security_configuration_debug() {
        let secret = "A".repeat(43);
        let token = BearerToken::parse(secret.clone()).unwrap();
        let security = ClientSecurityConfig::plaintext_development().with_bearer_token(token);
        assert!(!format!("{security:?}").contains(&secret));
        assert!(format!("{security:?}").contains("[REDACTED]"));
    }

    #[test]
    fn client_uses_shared_v2_protocol_support() {
        assert_eq!(PROTOCOL_SUPPORT, runnel_protocol::PROTOCOL_SUPPORT);
        assert!(PROTOCOL_SUPPORT.supports_version(runnel_protocol::PROTOCOL_VERSION));
        assert!(!PROTOCOL_SUPPORT.supports_version(1));
        assert!(PROTOCOL_SUPPORT.supports_payload_encoding(PayloadEncoding::Utf8Text));
        assert!(PROTOCOL_SUPPORT.supports_payload_encoding(PayloadEncoding::Binary));
    }

    #[test]
    fn client_hello_limits_are_protocol_bounded() {
        let hello = ClientHello::core_v2();
        assert_eq!(hello.versions, vec![VersionRange {
            major: 2,
            min_minor: 0,
            max_minor: 0,
        }]);
        assert!(hello.max_outbound_frame_bytes <= MAX_CLIENT_TO_SERVER_FRAME_BYTES);
        assert!(hello.max_inbound_frame_bytes <= DEFAULT_SERVER_TO_CLIENT_FRAME_BYTES);
    }
}
