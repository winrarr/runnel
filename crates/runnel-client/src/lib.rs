use std::io;
use std::time::Duration;

use runnel_protocol::{Request, Response};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

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

    #[error("encoding request as JSON failed: {source}")]
    EncodeRequest {
        #[source]
        source: serde_json::Error,
    },

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
            ClientError::ConnectTimeout { .. } | ClientError::Connect { .. } => {
                Self::Retryable(AttemptFailure::Client(error))
            }
            ClientError::EncodeRequest { .. } => Self::Rejected(AttemptFailure::Client(error)),
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
pub struct Client {
    reader: BufReader<ReadHalf<TcpStream>>,
    writer: WriteHalf<TcpStream>,
    config: ClientConfig,
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
        let (reader, writer) = connect(address, config).await?;

        Ok(Self {
            reader: BufReader::new(reader),
            writer,
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
    /// unchanged. After a request has returned a transport error, callers must
    /// treat that request's outcome according to [`AttemptOutcome`] and decide
    /// explicitly whether to issue a new request after reconnecting. In
    /// particular, this method never replays the failed request.
    pub async fn reconnect(&mut self, address: impl ToSocketAddrs) -> Result<(), ClientError> {
        let config = self.config;
        let (reader, writer) = connect(address, config).await?;
        self.reader = BufReader::new(reader);
        self.writer = writer;
        Ok(())
    }

    /// Send one request and read exactly one response from this connection.
    ///
    /// After a write, response timeout, read, EOF, or response-decoding error,
    /// the connection may not be reusable. Drop this client and connect a new
    /// one before retrying.
    pub async fn request(&mut self, request: &Request) -> Result<Response, ClientError> {
        let mut request_bytes =
            serde_json::to_vec(request).map_err(|source| ClientError::EncodeRequest { source })?;
        request_bytes.push(b'\n');

        tokio::time::timeout(
            self.config.request_timeout,
            self.writer.write_all(&request_bytes),
        )
        .await
        .map_err(|_| ClientError::WriteTimeout {
            timeout: self.config.request_timeout,
        })?
        .map_err(|source| ClientError::Write { source })?;

        let mut response_line = String::new();
        let bytes_read = tokio::time::timeout(
            self.config.response_timeout,
            self.reader.read_line(&mut response_line),
        )
        .await
        .map_err(|_| ClientError::ResponseTimeout {
            timeout: self.config.response_timeout,
        })?
        .map_err(|source| ClientError::Read { source })?;

        if bytes_read == 0 {
            return Err(ClientError::Eof);
        }

        serde_json::from_str(&response_line)
            .map_err(|source| ClientError::InvalidResponse { source })
    }

    /// Send one request and classify what can safely be inferred about its outcome.
    ///
    /// A successful non-error response is confirmed. Deterministic broker
    /// rejections are rejected, admission responses that are safe to retry on a
    /// new connection are retryable, and transport failures after this method
    /// starts writing are unknown. In particular, `request_timeout` remains
    /// unknown because the server uses it for both incomplete frames and
    /// engine work that may already have been applied.
    pub async fn request_with_outcome(&mut self, request: &Request) -> AttemptOutcome {
        match self.request(request).await {
            Ok(response) => classify_response(response),
            Err(error @ ClientError::EncodeRequest { .. }) => {
                AttemptOutcome::Rejected(AttemptFailure::Client(error))
            }
            Err(error) => AttemptOutcome::Unknown(AttemptFailure::Client(error)),
        }
    }
}

async fn connect(
    address: impl ToSocketAddrs,
    config: ClientConfig,
) -> Result<(ReadHalf<TcpStream>, WriteHalf<TcpStream>), ClientError> {
    let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(address))
        .await
        .map_err(|_| ClientError::ConnectTimeout {
            timeout: config.connect_timeout,
        })?
        .map_err(|source| ClientError::Connect { source })?;
    Ok(tokio::io::split(stream))
}

fn classify_response(response: Response) -> AttemptOutcome {
    let Response::Error { ref code, .. } = response else {
        return AttemptOutcome::Confirmed(response);
    };

    match code.as_str() {
        "connection_limit" | "request_saturated" | "stream_not_ready" => {
            AttemptOutcome::Retryable(AttemptFailure::Broker(response))
        }
        "request_timeout"
        | "storage_error"
        | "consumer_state_error"
        | "internal_error"
        | "cluster_error"
        | "corrupt_record" => AttemptOutcome::Unknown(AttemptFailure::Broker(response)),
        _ => AttemptOutcome::Rejected(AttemptFailure::Broker(response)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

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
}
