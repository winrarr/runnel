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

/// A persistent, sequential TCP client for the provisional JSON-lines protocol.
///
/// A client sends one request and reads one response per [`Client::request`] call.
/// Calls borrow the client mutably so responses cannot be interleaved. Drop the
/// client and create another with [`Client::connect`] to close or reconnect.
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
        let stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(address))
            .await
            .map_err(|_| ClientError::ConnectTimeout {
                timeout: config.connect_timeout,
            })?
            .map_err(|source| ClientError::Connect { source })?;
        let (reader, writer) = tokio::io::split(stream);

        Ok(Self {
            reader: BufReader::new(reader),
            writer,
            config,
        })
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
}
