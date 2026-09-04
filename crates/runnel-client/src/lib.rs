use std::io;
use std::time::Duration;

use runnel_protocol::{
    BinaryPayload, MAX_PUBLISH_BATCH_BYTES, MAX_PUBLISH_BATCH_RECORDS,
    PublishBatchRecord as WirePublishBatchRecord, PublishBatchRecordResponse, Request, Response,
};
pub use runnel_protocol::{PayloadEncoding, ProtocolSupport, ProtocolVersionRange};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

/// Protocol compatibility declared by this client and its broker-facing types.
///
/// The current JSON-lines listener has no runtime handshake, so this is a
/// source-level declaration rather than a negotiated connection property.
pub const PROTOCOL_SUPPORT: ProtocolSupport = runnel_protocol::PROTOCOL_SUPPORT;

/// Timeouts applied to each stage of a client connection and request.
#[derive(Debug, Clone, Copy)]
pub struct ClientConfig {
    /// Maximum time allowed to establish the TCP connection.
    pub connect_timeout: Duration,
    /// Maximum time allowed to write one complete request line.
    pub request_timeout: Duration,
    /// Maximum time allowed to read one complete response line.
    pub response_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(30),
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

    /// No usable connection remains after a failed or cancelled request.
    #[error("client connection is unavailable; reconnect before sending another request")]
    ConnectionUnavailable,

    #[error("encoding request as JSON failed: {source}")]
    EncodeRequest {
        #[source]
        source: serde_json::Error,
    },

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

    #[error("reading response failed: {source}")]
    Read {
        #[source]
        source: io::Error,
    },

    #[error("broker closed the connection before sending a response")]
    Eof,

    #[error("decoding response as JSON failed: {source}")]
    InvalidResponse {
        #[source]
        source: serde_json::Error,
    },

    #[error("unexpected response for {operation}: {response:?}")]
    UnexpectedResponse {
        operation: &'static str,
        response: Box<Response>,
    },
}

/// Optional fields for a text publish.
///
/// `request_id` is an application-provided identity. Reuse the same identity
/// when explicitly resolving an ambiguous publish against a broker that
/// supports request deduplication. This client never generates identities or
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
    /// The UTF-8 payload returned by the legacy text response.
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

/// The result classification for one request attempt.
///
/// The classification is deliberately not serialized. It describes what the
/// client can safely infer about an attempt from the existing provisional
/// protocol and transport behavior.
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
            ClientError::EncodeRequest { .. } => Self::Rejected(AttemptFailure::Client(error)),
            ClientError::InvalidBatch { .. } => Self::Rejected(AttemptFailure::Client(error)),
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

/// A persistent, sequential TCP client for the provisional JSON-lines protocol.
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
}

struct Connection {
    reader: BufReader<ReadHalf<TcpStream>>,
    writer: WriteHalf<TcpStream>,
}

impl Client {
    /// Connect to a broker using the default timeout configuration.
    pub async fn connect(address: impl ToSocketAddrs) -> Result<Self, ClientError> {
        Self::connect_with_config(address, ClientConfig::default()).await
    }

    /// Connect to a broker using explicit connection, request, and response timeouts.
    pub async fn connect_with_config(
        address: impl ToSocketAddrs,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        let connection = connect(address, config).await?;

        Ok(Self {
            connection: Some(connection),
            config,
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
        let connection = connect(address, config).await?;
        self.connection = Some(connection);
        Ok(())
    }

    /// Send one request and read exactly one response from this connection.
    ///
    /// After a write, response timeout, read, EOF, or response-decoding error,
    /// the connection is discarded automatically. The broker's
    /// `request_too_large` response also closes the protocol connection, so it
    /// is discarded even though the response itself was received. Reconnect
    /// before issuing another request. If this future is cancelled after
    /// polling begins, its connection is also discarded; the request outcome
    /// is unknown and must be resolved explicitly before retrying.
    ///
    /// A local request-encoding error occurs before the connection is taken and
    /// leaves an otherwise healthy connection available for reuse.
    pub async fn request(&mut self, request: &Request) -> Result<Response, ClientError> {
        let mut request_bytes =
            serde_json::to_vec(request).map_err(|source| ClientError::EncodeRequest { source })?;
        request_bytes.push(b'\n');

        let mut connection = self
            .connection
            .take()
            .ok_or(ClientError::ConnectionUnavailable)?;
        let config = self.config;
        let result = async {
            tokio::time::timeout(
                config.request_timeout,
                connection.writer.write_all(&request_bytes),
            )
            .await
            .map_err(|_| ClientError::WriteTimeout {
                timeout: config.request_timeout,
            })?
            .map_err(|source| ClientError::Write { source })?;

            let mut response_line = String::new();
            let bytes_read = tokio::time::timeout(
                config.response_timeout,
                connection.reader.read_line(&mut response_line),
            )
            .await
            .map_err(|_| ClientError::ResponseTimeout {
                timeout: config.response_timeout,
            })?
            .map_err(|source| ClientError::Read { source })?;

            if bytes_read == 0 {
                return Err(ClientError::Eof);
            }

            serde_json::from_str(&response_line)
                .map_err(|source| ClientError::InvalidResponse { source })
        }
        .await;

        if let Ok(response) = &result
            && !response_closes_connection(response)
        {
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
        match self.request(request).await {
            Ok(response) => classify_response(response),
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
    /// unknown publish against the current engines' deduplication path; use
    /// the same identity for that retry.
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
                payload_base64: BinaryPayload::new(payload),
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
    /// and rejected records. A transport, timeout, or leader-change failure
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
                    payload_base64: BinaryPayload::new(record.payload),
                    request_id: record.request_id,
                })
                .collect(),
        };
        let encoded_size = match serde_json::to_vec(&request) {
            Ok(encoded) => encoded.len(),
            Err(source) => {
                return publish_batch_failure(
                    record_count,
                    BatchFailureKind::Rejected,
                    AttemptFailure::Client(ClientError::EncodeRequest { source }),
                );
            }
        };
        if encoded_size > MAX_PUBLISH_BATCH_BYTES {
            return publish_batch_invalid(
                record_count,
                format!(
                    "encoded publish batch exceeds the maximum of {MAX_PUBLISH_BATCH_BYTES} bytes"
                ),
            );
        }
        let attempt = publish_batch_attempt(
            stream,
            record_count,
            self.request_with_outcome(&request).await,
        );
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
                    payload_base64,
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
                        payload: payload_base64.into_bytes(),
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
    /// Legacy UTF-8 responses are converted to their UTF-8 bytes. Messages
    /// that require the binary representation are decoded from base64.
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

    /// Acknowledge a message delivered to an ordinary consumer.
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
                payload_base64,
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
                    payload: payload_base64.into_bytes(),
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

fn response_closes_connection(response: &Response) -> bool {
    matches!(
        response,
        Response::Error { code, .. } if code == "request_too_large"
    )
}

async fn connect(
    address: impl ToSocketAddrs,
    config: ClientConfig,
) -> Result<Connection, ClientError> {
    let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(address))
        .await
        .map_err(|_| ClientError::ConnectTimeout {
            timeout: config.connect_timeout,
        })?
        .map_err(|source| ClientError::Connect { source })?;
    let (reader, writer) = tokio::io::split(stream);
    Ok(Connection {
        reader: BufReader::new(reader),
        writer,
    })
}

fn classify_response(response: Response) -> AttemptOutcome {
    let Response::Error { ref code, .. } = response else {
        return AttemptOutcome::Confirmed(response);
    };

    match classify_error_code(code) {
        BatchFailureKind::Retryable => AttemptOutcome::Retryable(AttemptFailure::Broker(response)),
        BatchFailureKind::Unknown => AttemptOutcome::Unknown(AttemptFailure::Broker(response)),
        BatchFailureKind::Rejected => AttemptOutcome::Rejected(AttemptFailure::Broker(response)),
    }
}

#[derive(Clone, Copy)]
enum BatchFailureKind {
    Rejected,
    Retryable,
    Unknown,
}

fn classify_error_code(code: &str) -> BatchFailureKind {
    match code {
        "connection_limit" | "request_saturated" | "stream_not_ready" => {
            BatchFailureKind::Retryable
        }
        "request_timeout"
        | "storage_error"
        | "consumer_state_error"
        | "internal_error"
        | "cluster_error"
        | "corrupt_record" => BatchFailureKind::Unknown,
        _ => BatchFailureKind::Rejected,
    }
}

fn publish_batch_invalid(record_count: usize, message: String) -> PublishBatchAttempt {
    let error = AttemptFailure::Client(ClientError::InvalidBatch { message });
    publish_batch_failure(record_count, BatchFailureKind::Rejected, error)
}

fn publish_batch_attempt(
    stream: String,
    record_count: usize,
    outcome: AttemptOutcome,
) -> PublishBatchAttempt {
    match outcome {
        AttemptOutcome::Confirmed(response) => match response {
            Response::PublishBatch {
                stream: response_stream,
                outcomes,
            } if response_stream == stream && outcomes.len() == record_count => {
                let outcomes = outcomes
                    .into_iter()
                    .map(|outcome| match outcome {
                        PublishBatchRecordResponse::Published { offset } => {
                            PublishBatchOutcome::Confirmed(PublishReceipt {
                                stream: stream.clone(),
                                offset,
                            })
                        }
                        PublishBatchRecordResponse::Error { code, message } => {
                            publish_batch_record_error(code, message)
                        }
                    })
                    .collect();
                PublishBatchAttempt {
                    outcomes,
                    attempt: None,
                }
            }
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

fn publish_batch_record_error(code: String, message: String) -> PublishBatchOutcome {
    publish_batch_record_failure(classify_error_code(&code), code, message)
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
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    fn test_config() -> ClientConfig {
        ClientConfig {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            response_timeout: Duration::from_millis(100),
        }
    }

    async fn listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        (listener, address)
    }

    async fn read_request(reader: &mut BufReader<tokio::net::TcpStream>) -> Request {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    }

    async fn write_response(reader: &mut BufReader<tokio::net::TcpStream>, response: Response) {
        let mut encoded = serde_json::to_vec(&response).unwrap();
        encoded.push(b'\n');
        reader.get_mut().write_all(&encoded).await.unwrap();
    }

    #[tokio::test]
    async fn sends_sequential_requests_on_one_connection() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);

            let mut first_request = String::new();
            reader.read_line(&mut first_request).await.unwrap();
            assert!(matches!(
                serde_json::from_str::<Request>(&first_request).unwrap(),
                Request::Health
            ));
            reader
                .get_mut()
                .write_all(
                    b"{\"type\":\"health\",\"status\":\"ok\",\"streams\":0,\"storage_bytes\":0}\n",
                )
                .await
                .unwrap();

            let mut second_request = String::new();
            reader.read_line(&mut second_request).await.unwrap();
            assert!(matches!(
                serde_json::from_str::<Request>(&second_request).unwrap(),
                Request::CreateStream { stream } if stream == "events"
            ));
            reader
                .get_mut()
                .write_all(
                    b"{\"type\":\"stream_created\",\"stream\":\"events\",\"created\":true}\n",
                )
                .await
                .unwrap();
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let health = client.request(&Request::Health).await.unwrap();
        assert!(matches!(health, Response::Health { status, .. } if status == "ok"));

        let created = client
            .request(&Request::CreateStream {
                stream: "events".to_owned(),
            })
            .await
            .unwrap();
        assert!(matches!(
            created,
            Response::StreamCreated { stream, created } if stream == "events" && created
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn publish_batch_returns_ordered_per_record_outcomes() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            match read_request(&mut reader).await {
                Request::PublishBatch { stream, records } => {
                    assert_eq!(stream, "events");
                    assert_eq!(records.len(), 2);
                    assert_eq!(records[0].payload_base64.as_bytes(), [0, 255]);
                    assert_eq!(records[0].request_id.as_deref(), Some("record-1"));
                    assert_eq!(records[1].payload_base64.as_bytes(), b"second");
                }
                request => panic!("expected publish batch, got {request:?}"),
            }
            write_response(
                &mut reader,
                Response::PublishBatch {
                    stream: "events".to_owned(),
                    outcomes: vec![
                        PublishBatchRecordResponse::Published { offset: 3 },
                        PublishBatchRecordResponse::Error {
                            code: "request_saturated".to_owned(),
                            message: "busy".to_owned(),
                        },
                    ],
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let attempt = client
            .publish_batch(
                "events",
                [
                    PublishBatchRecord::with_options(
                        vec![0, 255],
                        PublishOptions::default().with_request_id("record-1"),
                    ),
                    PublishBatchRecord::text("second"),
                ],
            )
            .await;
        assert!(attempt.attempt.is_none());
        assert_eq!(
            attempt.outcomes,
            vec![
                PublishBatchOutcome::Confirmed(PublishReceipt {
                    stream: "events".to_owned(),
                    offset: 3,
                }),
                PublishBatchOutcome::Retryable {
                    code: "request_saturated".to_owned(),
                    message: "busy".to_owned(),
                },
            ]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn publish_batch_marks_every_record_unknown_after_disconnect() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(
                read_request(&mut reader).await,
                Request::PublishBatch { records, .. } if records.len() == 2
            ));
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let attempt = client
            .publish_batch(
                "events",
                [
                    PublishBatchRecord::with_options(
                        "first",
                        PublishOptions::default().with_request_id("first"),
                    ),
                    PublishBatchRecord::with_options(
                        "second",
                        PublishOptions::default().with_request_id("second"),
                    ),
                ],
            )
            .await;
        assert!(matches!(
            attempt.attempt,
            Some(AttemptFailure::Client(ClientError::Eof))
        ));
        assert!(attempt.outcomes.iter().all(|outcome| matches!(
            outcome,
            PublishBatchOutcome::Unknown { code, .. } if code == "client_error"
        )));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn publish_batch_marks_every_record_unknown_after_response_timeout() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(
                read_request(&mut reader).await,
                Request::PublishBatch { records, .. } if records.len() == 2
            ));
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let attempt = client
            .publish_batch(
                "events",
                [
                    PublishBatchRecord::new("first"),
                    PublishBatchRecord::new("second"),
                ],
            )
            .await;
        assert!(matches!(
            attempt.attempt,
            Some(AttemptFailure::Client(ClientError::ResponseTimeout { .. }))
        ));
        assert!(attempt.outcomes.iter().all(|outcome| matches!(
            outcome,
            PublishBatchOutcome::Unknown { code, .. } if code == "client_error"
        )));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn publish_batch_response_mismatch_invalidates_connection() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(
                read_request(&mut reader).await,
                Request::PublishBatch { .. }
            ));
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

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let attempt = client
            .publish_batch("events", [PublishBatchRecord::text("hello")])
            .await;
        assert!(matches!(
            attempt.attempt,
            Some(AttemptFailure::Client(
                ClientError::UnexpectedResponse { .. }
            ))
        ));
        assert!(matches!(
            client.request(&Request::Health).await,
            Err(ClientError::ConnectionUnavailable)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_operations_map_current_protocol_results() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);

            assert!(matches!(
                read_request(&mut reader).await,
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

            match read_request(&mut reader).await {
                Request::Publish {
                    stream,
                    key,
                    payload,
                    request_id,
                } => {
                    assert_eq!(stream, "events");
                    assert_eq!(key.as_deref(), Some("order-1"));
                    assert_eq!(payload, "hello");
                    assert_eq!(request_id.as_deref(), Some("publish-1"));
                }
                request => panic!("expected publish, got {request:?}"),
            }
            write_response(
                &mut reader,
                Response::Published {
                    stream: "events".to_owned(),
                    offset: 4,
                },
            )
            .await;

            assert!(matches!(
                read_request(&mut reader).await,
                Request::Poll { stream, consumer }
                    if stream == "events" && consumer == "reader"
            ));
            write_response(
                &mut reader,
                Response::Message {
                    stream: "events".to_owned(),
                    consumer: "reader".to_owned(),
                    member: None,
                    offset: 4,
                    key: Some("order-1".to_owned()),
                    payload: "hello".to_owned(),
                    published_at_ms: 123,
                    delivery_token: None,
                    delivery_attempt: None,
                },
            )
            .await;

            assert!(matches!(
                read_request(&mut reader).await,
                Request::Poll { stream, consumer }
                    if stream == "events" && consumer == "reader"
            ));
            write_response(
                &mut reader,
                Response::Empty {
                    stream: "events".to_owned(),
                    consumer: "reader".to_owned(),
                },
            )
            .await;

            assert!(matches!(
                read_request(&mut reader).await,
                Request::PollGroup {
                    stream,
                    consumer,
                    member,
                } if stream == "events" && consumer == "workers" && member == "member-a"
            ));
            write_response(
                &mut reader,
                Response::Message {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    member: Some("member-a".to_owned()),
                    offset: 5,
                    key: None,
                    payload: "grouped".to_owned(),
                    published_at_ms: 456,
                    delivery_token: Some("token-5".to_owned()),
                    delivery_attempt: Some(2),
                },
            )
            .await;

            assert!(matches!(
                read_request(&mut reader).await,
                Request::Ack {
                    stream,
                    consumer,
                    offset,
                } if stream == "events" && consumer == "reader" && offset == 4
            ));
            write_response(
                &mut reader,
                Response::Acknowledged {
                    stream: "events".to_owned(),
                    consumer: "reader".to_owned(),
                    offset: 4,
                    already_acknowledged: false,
                },
            )
            .await;

            match read_request(&mut reader).await {
                Request::AckGroup {
                    stream,
                    consumer,
                    member,
                    offset,
                    delivery_token,
                } => {
                    assert_eq!(stream, "events");
                    assert_eq!(consumer, "workers");
                    assert_eq!(member, "member-a");
                    assert_eq!(offset, 5);
                    assert_eq!(delivery_token, "token-5");
                }
                request => panic!("expected grouped acknowledgement, got {request:?}"),
            }
            write_response(
                &mut reader,
                Response::Acknowledged {
                    stream: "events".to_owned(),
                    consumer: "workers".to_owned(),
                    offset: 5,
                    already_acknowledged: true,
                },
            )
            .await;

            assert!(matches!(read_request(&mut reader).await, Request::Health));
            write_response(
                &mut reader,
                Response::Health {
                    status: "ok".to_owned(),
                    streams: 1,
                    storage_bytes: 4096,
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        assert_eq!(
            client.create_stream("events").await.unwrap(),
            StreamCreation {
                stream: "events".to_owned(),
                created: true,
            }
        );
        assert_eq!(
            client
                .publish_with_options(
                    "events",
                    "hello",
                    PublishOptions::default()
                        .with_key("order-1")
                        .with_request_id("publish-1"),
                )
                .await
                .unwrap(),
            PublishReceipt {
                stream: "events".to_owned(),
                offset: 4,
            }
        );
        assert_eq!(
            client.poll("events", "reader").await.unwrap(),
            Some(Message {
                stream: "events".to_owned(),
                consumer: "reader".to_owned(),
                member: None,
                offset: 4,
                key: Some("order-1".to_owned()),
                payload: "hello".to_owned(),
                published_at_ms: 123,
                delivery_token: None,
                delivery_attempt: None,
            })
        );
        assert_eq!(client.poll("events", "reader").await.unwrap(), None);
        assert_eq!(
            client
                .poll_group("events", "workers", "member-a")
                .await
                .unwrap(),
            Some(Message {
                stream: "events".to_owned(),
                consumer: "workers".to_owned(),
                member: Some("member-a".to_owned()),
                offset: 5,
                key: None,
                payload: "grouped".to_owned(),
                published_at_ms: 456,
                delivery_token: Some("token-5".to_owned()),
                delivery_attempt: Some(2),
            })
        );
        assert_eq!(
            client.ack("events", "reader", 4).await.unwrap(),
            Acknowledgement {
                stream: "events".to_owned(),
                consumer: "reader".to_owned(),
                offset: 4,
                already_acknowledged: false,
            }
        );
        assert_eq!(
            client
                .ack_group("events", "workers", "member-a", 5, "token-5")
                .await
                .unwrap(),
            Acknowledgement {
                stream: "events".to_owned(),
                consumer: "workers".to_owned(),
                offset: 5,
                already_acknowledged: true,
            }
        );
        assert_eq!(
            client.health().await.unwrap(),
            Health {
                status: "ok".to_owned(),
                streams: 1,
                storage_bytes: 4096,
            }
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn binary_publish_and_poll_preserve_payload_bytes() {
        let (listener, address) = listener().await;
        let payload = vec![0, 1, 255, b'\n', b'_'];
        let server_payload = payload.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);

            match read_request(&mut reader).await {
                Request::PublishBytes {
                    stream,
                    key,
                    payload_base64,
                    request_id,
                } => {
                    assert_eq!(stream, "events");
                    assert_eq!(key.as_deref(), Some("binary-key"));
                    assert_eq!(payload_base64.as_bytes(), server_payload.as_slice());
                    assert_eq!(request_id.as_deref(), Some("binary-publish-1"));
                }
                request => panic!("expected binary publish, got {request:?}"),
            }
            write_response(
                &mut reader,
                Response::Published {
                    stream: "events".to_owned(),
                    offset: 8,
                },
            )
            .await;

            assert!(matches!(
                read_request(&mut reader).await,
                Request::Poll { stream, consumer }
                    if stream == "events" && consumer == "reader"
            ));
            write_response(
                &mut reader,
                Response::MessageBytes {
                    stream: "events".to_owned(),
                    consumer: "reader".to_owned(),
                    member: None,
                    offset: 8,
                    key: Some("binary-key".to_owned()),
                    payload_base64: BinaryPayload::new(server_payload),
                    published_at_ms: 123,
                    delivery_token: None,
                    delivery_attempt: Some(1),
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        assert_eq!(
            client
                .publish_bytes_with_options(
                    "events",
                    payload,
                    PublishOptions::default()
                        .with_key("binary-key")
                        .with_request_id("binary-publish-1"),
                )
                .await
                .unwrap(),
            PublishReceipt {
                stream: "events".to_owned(),
                offset: 8,
            }
        );
        assert_eq!(
            client.poll_bytes("events", "reader").await.unwrap(),
            Some(BinaryMessage {
                stream: "events".to_owned(),
                consumer: "reader".to_owned(),
                member: None,
                offset: 8,
                key: Some("binary-key".to_owned()),
                payload: vec![0, 1, 255, b'\n', b'_'],
                published_at_ms: 123,
                delivery_token: None,
                delivery_attempt: Some(1),
            })
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn replay_is_read_only_and_reports_unavailable_history() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);

            assert!(matches!(
                read_request(&mut reader).await,
                Request::Replay {
                    stream,
                    consumer,
                    offset: 4,
                } if stream == "events" && consumer == "worker"
            ));
            write_response(
                &mut reader,
                Response::ReplayMessage {
                    stream: "events".to_owned(),
                    consumer: "worker".to_owned(),
                    offset: 4,
                    key: Some("order-1".to_owned()),
                    payload: "replayed".to_owned(),
                    published_at_ms: 12,
                },
            )
            .await;

            assert!(matches!(
                read_request(&mut reader).await,
                Request::Replay {
                    stream,
                    consumer,
                    offset: 99,
                } if stream == "events" && consumer == "worker"
            ));
            write_response(
                &mut reader,
                Response::Error {
                    code: "history_unavailable".to_owned(),
                    message: "requested offset is unavailable".to_owned(),
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        assert_eq!(
            client.replay("events", "worker", 4).await.unwrap(),
            ReplayMessage {
                stream: "events".to_owned(),
                consumer: "worker".to_owned(),
                offset: 4,
                key: Some("order-1".to_owned()),
                payload: "replayed".to_owned(),
                published_at_ms: 12,
            }
        );
        assert!(matches!(
            client.replay("events", "worker", 99).await,
            Err(AttemptOutcome::Rejected(AttemptFailure::Broker(
                Response::Error { code, .. }
            ))) if code == "history_unavailable"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_response_mismatch_is_an_unknown_outcome() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(
                read_request(&mut reader).await,
                Request::CreateStream { stream } if stream == "events"
            ));
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

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let result = client.create_stream("events").await;
        assert!(matches!(
            result,
            Err(AttemptOutcome::Unknown(AttemptFailure::Client(
                ClientError::UnexpectedResponse {
                    operation: "create_stream",
                    response,
                }
            ))) if matches!(response.as_ref(), Response::Health { .. })
        ));
        assert!(matches!(
            client.request(&Request::Health).await,
            Err(ClientError::ConnectionUnavailable)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_operation_preserves_broker_outcome_classification() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(
                read_request(&mut reader).await,
                Request::Publish { stream, payload, .. }
                    if stream == "events" && payload == "hello"
            ));
            write_response(
                &mut reader,
                Response::Error {
                    code: "request_saturated".to_owned(),
                    message: "busy".to_owned(),
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        assert!(matches!(
            client.publish("events", "hello").await,
            Err(AttemptOutcome::Retryable(AttemptFailure::Broker(
                Response::Error { code, .. }
            ))) if code == "request_saturated"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_typed_request_is_not_retried_and_reconnect_is_explicit() {
        let (listener, address) = listener().await;
        let (started_tx, started_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(read_request(&mut reader).await, Request::Health));
            started_tx.send(()).unwrap();

            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(read_request(&mut reader).await, Request::Health));
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

        let mut client = Client::connect_with_config(
            address,
            ClientConfig {
                response_timeout: Duration::from_secs(5),
                ..test_config()
            },
        )
        .await
        .unwrap();
        let mut operation = Box::pin(client.health());
        tokio::select! {
            result = &mut operation => panic!("health request unexpectedly completed: {result:?}"),
            _ = started_rx => {}
        }
        drop(operation);

        assert!(matches!(
            client.health().await,
            Err(AttemptOutcome::Retryable(AttemptFailure::Client(
                ClientError::ConnectionUnavailable
            )))
        ));
        client.reconnect(address).await.unwrap();
        assert_eq!(
            client.health().await.unwrap(),
            Health {
                status: "ok".to_owned(),
                streams: 0,
                storage_bytes: 0,
            }
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_publish_retry_requires_reconnect_and_stable_identity() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            match read_request(&mut reader).await {
                Request::Publish {
                    stream,
                    payload,
                    request_id,
                    ..
                } => {
                    assert_eq!(stream, "events");
                    assert_eq!(payload, "once");
                    assert_eq!(request_id.as_deref(), Some("publish-1"));
                }
                request => panic!("expected publish, got {request:?}"),
            }
            drop(reader);

            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            match read_request(&mut reader).await {
                Request::Publish {
                    stream,
                    payload,
                    request_id,
                    ..
                } => {
                    assert_eq!(stream, "events");
                    assert_eq!(payload, "once");
                    assert_eq!(request_id.as_deref(), Some("publish-1"));
                }
                request => panic!("expected retried publish, got {request:?}"),
            }
            write_response(
                &mut reader,
                Response::Published {
                    stream: "events".to_owned(),
                    offset: 0,
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let options = PublishOptions::default().with_request_id("publish-1");
        assert!(matches!(
            client
                .publish_with_options("events", "once", options.clone())
                .await,
            Err(AttemptOutcome::Unknown(AttemptFailure::Client(
                ClientError::Eof
            )))
        ));

        client.reconnect(address).await.unwrap();
        assert_eq!(
            client
                .publish_with_options("events", "once", options)
                .await
                .unwrap(),
            PublishReceipt {
                stream: "events".to_owned(),
                offset: 0,
            }
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn times_out_when_response_is_not_sent() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let result = client.request(&Request::Health).await;
        assert!(matches!(result, Err(ClientError::ResponseTimeout { .. })));
        assert!(matches!(
            client.request(&Request::Health).await,
            Err(ClientError::ConnectionUnavailable)
        ));
        drop(client);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reports_eof_before_a_response() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let result = client.request(&Request::Health).await;
        assert!(matches!(result, Err(ClientError::Eof)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnects_explicitly_after_unknown_response_failure() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut first_request = String::new();
            reader.read_line(&mut first_request).await.unwrap();
            assert!(matches!(
                serde_json::from_str::<Request>(&first_request).unwrap(),
                Request::Health
            ));
            drop(reader);

            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut second_request = String::new();
            reader.read_line(&mut second_request).await.unwrap();
            assert!(matches!(
                serde_json::from_str::<Request>(&second_request).unwrap(),
                Request::Health
            ));
            reader
                .get_mut()
                .write_all(
                    b"{\"type\":\"health\",\"status\":\"ok\",\"streams\":0,\"storage_bytes\":0}\n",
                )
                .await
                .unwrap();
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        assert!(matches!(
            client.request_with_outcome(&Request::Health).await,
            AttemptOutcome::Unknown(AttemptFailure::Client(ClientError::Eof))
        ));

        client.reconnect(address).await.unwrap();
        assert!(matches!(
            client.request(&Request::Health).await,
            Ok(Response::Health { status, .. }) if status == "ok"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn failed_reconnect_keeps_a_healthy_existing_connection() {
        let (server_listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = server_listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(read_request(&mut reader).await, Request::Health));
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

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let (unavailable_listener, unavailable_address) = listener().await;
        drop(unavailable_listener);
        assert!(matches!(
            client.reconnect(unavailable_address).await,
            Err(ClientError::Connect { .. } | ClientError::ConnectTimeout { .. })
        ));
        assert_eq!(
            client.health().await.unwrap(),
            Health {
                status: "ok".to_owned(),
                streams: 0,
                storage_bytes: 0,
            }
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn failed_reconnect_does_not_change_unknown_outcome() {
        let (server_listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = server_listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            assert!(matches!(
                serde_json::from_str::<Request>(&request).unwrap(),
                Request::Publish { .. }
            ));
            drop(reader);
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let outcome = client
            .request_with_outcome(&Request::Publish {
                stream: "events".to_owned(),
                key: None,
                payload: "once".to_owned(),
                request_id: None,
            })
            .await;
        assert!(matches!(
            &outcome,
            AttemptOutcome::Unknown(AttemptFailure::Client(ClientError::Eof))
        ));

        let (unavailable_listener, unavailable_address) = listener().await;
        drop(unavailable_listener);
        assert!(matches!(
            client.reconnect(unavailable_address).await,
            Err(ClientError::Connect { .. } | ClientError::ConnectTimeout { .. })
        ));
        assert!(matches!(
            &outcome,
            AttemptOutcome::Unknown(AttemptFailure::Client(ClientError::Eof))
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reports_invalid_response() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            reader.get_mut().write_all(b"not-json\n").await.unwrap();
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let result = client.request(&Request::Health).await;
        assert!(matches!(result, Err(ClientError::InvalidResponse { .. })));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_too_large_response_invalidates_persistent_connection() {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            assert!(matches!(read_request(&mut reader).await, Request::Health));
            write_response(
                &mut reader,
                Response::Error {
                    code: "request_too_large".to_owned(),
                    message: "request exceeds the configured maximum".to_owned(),
                },
            )
            .await;
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        assert!(matches!(
            client.request(&Request::Health).await,
            Ok(Response::Error { code, .. }) if code == "request_too_large"
        ));
        assert!(matches!(
            client.request(&Request::Health).await,
            Err(ClientError::ConnectionUnavailable)
        ));
        server.await.unwrap();
    }

    async fn request_outcome(response: Response) -> AttemptOutcome {
        let (listener, address) = listener().await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            reader.read_line(&mut request).await.unwrap();
            let mut encoded = serde_json::to_vec(&response).unwrap();
            encoded.push(b'\n');
            reader.get_mut().write_all(&encoded).await.unwrap();
        });

        let mut client = Client::connect_with_config(address, test_config())
            .await
            .unwrap();
        let outcome = client.request_with_outcome(&Request::Health).await;
        server.await.unwrap();
        outcome
    }

    #[tokio::test]
    async fn classifies_success_as_confirmed() {
        let outcome = request_outcome(Response::Health {
            status: "ok".to_owned(),
            streams: 0,
            storage_bytes: 0,
        })
        .await;

        assert!(matches!(
            outcome,
            AttemptOutcome::Confirmed(Response::Health { .. })
        ));
    }

    #[tokio::test]
    async fn classifies_deterministic_broker_rejection() {
        let outcome = request_outcome(Response::Error {
            code: "invalid_name".to_owned(),
            message: "invalid stream".to_owned(),
        })
        .await;

        assert!(
            matches!(outcome, AttemptOutcome::Rejected(AttemptFailure::Broker(Response::Error { code, .. })) if code == "invalid_name")
        );
    }

    #[tokio::test]
    async fn classifies_admission_response_as_retryable() {
        let outcome = request_outcome(Response::Error {
            code: "request_saturated".to_owned(),
            message: "busy".to_owned(),
        })
        .await;

        assert!(
            matches!(outcome, AttemptOutcome::Retryable(AttemptFailure::Broker(Response::Error { code, .. })) if code == "request_saturated")
        );
    }

    #[tokio::test]
    async fn classifies_timeout_response_as_unknown() {
        let outcome = request_outcome(Response::Error {
            code: "request_timeout".to_owned(),
            message: "deadline exceeded".to_owned(),
        })
        .await;

        assert!(
            matches!(outcome, AttemptOutcome::Unknown(AttemptFailure::Broker(Response::Error { code, .. })) if code == "request_timeout")
        );
    }

    #[test]
    fn classifies_connection_failure_before_writing_as_retryable() {
        let outcome = AttemptOutcome::from_client_error(ClientError::Connect {
            source: io::Error::new(io::ErrorKind::ConnectionRefused, "refused"),
        });

        assert!(matches!(
            outcome,
            AttemptOutcome::Retryable(AttemptFailure::Client(ClientError::Connect { .. }))
        ));
    }

    #[test]
    fn classifies_missing_connection_before_writing_as_retryable() {
        let outcome = AttemptOutcome::from_client_error(ClientError::ConnectionUnavailable);

        assert!(matches!(
            outcome,
            AttemptOutcome::Retryable(AttemptFailure::Client(ClientError::ConnectionUnavailable))
        ));
    }

    #[test]
    fn client_uses_shared_protocol_support() {
        assert_eq!(PROTOCOL_SUPPORT, runnel_protocol::PROTOCOL_SUPPORT);
        assert!(PROTOCOL_SUPPORT.supports_version(runnel_protocol::PROTOCOL_VERSION));
        assert!(PROTOCOL_SUPPORT.supports_payload_encoding(PayloadEncoding::Utf8Text));
        assert!(PROTOCOL_SUPPORT.supports_payload_encoding(PayloadEncoding::Base64));
    }
}
