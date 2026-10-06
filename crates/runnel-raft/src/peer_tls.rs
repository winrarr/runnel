use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use data_encoding::BASE32_NOPAD;
use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;
use x509_parser::extensions::GeneralName;

const MAX_SIMULTANEOUS_HANDSHAKES: usize = 32;
// Keep accepted and opened peer sessions finite even when many Raft groups
// address the same broker. Admission waits are bounded by the RPC TTL.
const MAX_ACTIVE_PEER_CONNECTIONS: usize = 256;
const FRAME_MEMORY_QUANTUM: usize = 1024 * 1024;
// At most 256 MiB of peer frame payloads can be buffered per broker process.
const MAX_CONCURRENT_PEER_FRAME_MEMORY: usize = 256 * 1024 * 1024;
const MAX_CONCURRENT_FRAME_WRITES: usize = 4;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Static peer TLS credentials and identity policy for one broker process.
///
/// Its debug representation intentionally omits file paths, certificate
/// material, and private-key material. It uses TLS 1.3 only, trusts only the
/// configured bundle, disables early data, and reads credentials at startup;
/// credential changes require a process restart.
pub struct PeerTlsConfig {
    local_node_id: u64,
    cluster_name: Arc<str>,
    identities: HashMap<String, u64>,
    client: Arc<ClientConfig>,
    #[cfg(test)]
    server: Arc<ServerConfig>,
    acceptor: TlsAcceptor,
    inbound_handshakes: Arc<Semaphore>,
    outbound_handshakes: Arc<Semaphore>,
    inbound_connections: Arc<Semaphore>,
    outbound_connections: Arc<Semaphore>,
    frame_memory: Arc<Semaphore>,
    frame_writes: Arc<Semaphore>,
}

impl fmt::Debug for PeerTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerTlsConfig")
            .field("local_node_id", &self.local_node_id)
            .field("configured_peer_count", &self.identities.len())
            .finish_non_exhaustive()
    }
}

/// An inbound handshake slot acquired before a peer socket is spawned.
/// Keeping admission separate from the async handshake prevents an unbounded
/// queue of accepted sockets waiting for TLS capacity.
pub(crate) struct PeerTlsHandshakePermit(OwnedSemaphorePermit);

/// A peer-session slot held for the lifetime of a stream.
pub(crate) struct PeerTlsConnectionPermit {
    _permit: OwnedSemaphorePermit,
}

impl PeerTlsConfig {
    /// Load explicit cluster trust and this process's leaf identity.
    ///
    /// The peer map must contain this node and must not map two node IDs to
    /// the same dial address. Only the supplied trust bundle is used. These
    /// paths should refer to operator-managed secret files and are not logged.
    pub fn from_files(
        local_node_id: u64,
        cluster_name: &str,
        peers: &BTreeMap<u64, String>,
        trust_bundle: impl AsRef<Path>,
        certificate_chain: impl AsRef<Path>,
        private_key: impl AsRef<Path>,
    ) -> io::Result<Self> {
        validate_peer_map(local_node_id, peers)?;

        let mut roots = RootCertStore::empty();
        let mut root_reader =
            BufReader::new(open_file(trust_bundle.as_ref(), "peer trust bundle")?);
        let mut root_count = 0usize;
        for certificate in rustls_pemfile::certs(&mut root_reader) {
            let certificate =
                certificate.map_err(|_| invalid_config("peer trust bundle PEM is malformed"))?;
            roots.add(certificate).map_err(|_| {
                invalid_config("peer trust bundle contains an invalid trust anchor")
            })?;
            root_count += 1;
        }
        if root_count == 0 {
            return Err(invalid_config("peer trust bundle contains no certificates"));
        }

        let mut chain_reader = BufReader::new(open_file(
            certificate_chain.as_ref(),
            "peer certificate chain",
        )?);
        let certificates = rustls_pemfile::certs(&mut chain_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid_config("peer certificate chain PEM is malformed"))?;
        let Some(leaf) = certificates.first() else {
            return Err(invalid_config(
                "peer certificate chain contains no certificates",
            ));
        };

        let mut key_reader = BufReader::new(open_file(private_key.as_ref(), "peer private key")?);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|_| invalid_config("peer private key PEM is malformed"))?
            .ok_or_else(|| invalid_config("peer private key file contains no supported key"))?;

        let identities = peer_identities(local_node_id, cluster_name, peers);
        let local_identity = identity_for(local_node_id, cluster_name);
        require_exact_peer_identity(leaf, &local_identity).map_err(|_| {
            invalid_config("local peer certificate SAN does not match this node and cluster")
        })?;

        let root_store = Arc::new(roots);
        validate_local_certificate(&root_store, &certificates, &local_identity)?;

        let client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates((*root_store).clone())
            .with_client_auth_cert(certificates.clone(), key.clone_key())
            .map_err(|_| invalid_config("peer certificate and private key do not match"))?;
        let mut client = client;
        client.enable_early_data = false;

        let client_verifier = WebPkiClientVerifier::builder(root_store)
            .build()
            .map_err(|_| {
                invalid_config("peer trust bundle cannot configure client verification")
            })?;
        let mut server = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certificates, key)
            .map_err(|_| invalid_config("peer certificate and private key do not match"))?;
        server.max_early_data_size = 0;
        server.send_half_rtt_data = false;

        let server = Arc::new(server);
        Ok(Self {
            local_node_id,
            cluster_name: Arc::from(cluster_name),
            identities,
            client: Arc::new(client),
            acceptor: TlsAcceptor::from(Arc::clone(&server)),
            #[cfg(test)]
            server,
            inbound_handshakes: Arc::new(Semaphore::new(MAX_SIMULTANEOUS_HANDSHAKES)),
            outbound_handshakes: Arc::new(Semaphore::new(MAX_SIMULTANEOUS_HANDSHAKES)),
            inbound_connections: Arc::new(Semaphore::new(MAX_ACTIVE_PEER_CONNECTIONS)),
            outbound_connections: Arc::new(Semaphore::new(MAX_ACTIVE_PEER_CONNECTIONS)),
            frame_memory: Arc::new(Semaphore::new(
                MAX_CONCURRENT_PEER_FRAME_MEMORY / FRAME_MEMORY_QUANTUM,
            )),
            frame_writes: Arc::new(Semaphore::new(MAX_CONCURRENT_FRAME_WRITES)),
        })
    }

    /// Ensure the engine and network map use the same node IDs and cluster.
    pub(crate) fn validate_for(
        &self,
        local_node_id: u64,
        cluster_name: &str,
        peers: &BTreeMap<u64, String>,
    ) -> io::Result<()> {
        validate_peer_map(local_node_id, peers)?;
        if self.local_node_id != local_node_id
            || self.cluster_name.as_ref() != cluster_name
            || self.identities != peer_identities(local_node_id, cluster_name, peers)
        {
            return Err(invalid_config(
                "peer TLS credentials do not match configured cluster membership",
            ));
        }
        Ok(())
    }

    /// Refuse excess accepted sockets before a task is spawned.
    pub(crate) fn try_acquire_inbound_handshake(&self) -> Option<PeerTlsHandshakePermit> {
        self.inbound_handshakes
            .clone()
            .try_acquire_owned()
            .ok()
            .map(PeerTlsHandshakePermit)
    }

    /// Refuse excess peer sessions before their connection tasks are spawned.
    pub(crate) fn try_acquire_inbound_connection(&self) -> Option<PeerTlsConnectionPermit> {
        self.inbound_connections
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|_permit| PeerTlsConnectionPermit { _permit })
    }

    pub(crate) async fn acquire_outbound_connection(
        &self,
        timeout: Duration,
    ) -> io::Result<PeerTlsConnectionPermit> {
        tokio::time::timeout(timeout, self.outbound_connections.clone().acquire_owned())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "peer connection admission timed out",
                )
            })?
            .map(|_permit| PeerTlsConnectionPermit { _permit })
            .map_err(|_| invalid_config("peer TLS connection admission is closed"))
    }

    pub(crate) fn frame_memory(&self) -> Arc<Semaphore> {
        Arc::clone(&self.frame_memory)
    }

    pub(crate) fn frame_write_slots(&self) -> Arc<Semaphore> {
        Arc::clone(&self.frame_writes)
    }

    /// Complete mutual TLS and bind the client certificate to one configured
    /// remote node. Call this before reading any peer-protocol frame.
    pub(crate) async fn accept(
        &self,
        stream: TcpStream,
        permit: PeerTlsHandshakePermit,
    ) -> io::Result<(ServerTlsStream<TcpStream>, u64)> {
        let _permit = permit.0;
        let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.acceptor.accept(stream))
            .await
            .map_err(|_| handshake_timeout())?
            .map_err(peer_handshake_error)?;
        let certificate = stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certificates| certificates.first())
            .ok_or_else(|| invalid_peer("peer TLS certificate is missing"))?;
        let identity = peer_identity_from_certificate(certificate)?;
        let peer_id = self
            .identities
            .get(&identity)
            .copied()
            .filter(|peer_id| *peer_id != self.local_node_id)
            .ok_or_else(|| invalid_peer("peer TLS identity is not a configured remote node"))?;
        Ok((stream, peer_id))
    }

    /// Open a TLS connection to an address while validating the configured
    /// peer's expected node identity, independent of the dial address.
    pub(crate) async fn connect(
        &self,
        target_node_id: u64,
        address: &str,
    ) -> io::Result<ClientTlsStream<TcpStream>> {
        if target_node_id == self.local_node_id
            || !self.identities.values().any(|id| *id == target_node_id)
        {
            return Err(invalid_config(
                "outbound peer node ID is not a configured remote node",
            ));
        }
        let permit = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            self.outbound_handshakes.clone().acquire_owned(),
        )
        .await
        .map_err(|_| handshake_timeout())?
        .map_err(|_| invalid_config("peer TLS handshake admission is closed"))?;
        let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| handshake_timeout())??;
        stream.set_nodelay(true)?;
        let expected = identity_for(target_node_id, &self.cluster_name);
        let name = ServerName::try_from(expected.clone())
            .map_err(|_| invalid_config("configured peer identity is invalid"))?;
        let tls = tokio::time::timeout(
            HANDSHAKE_TIMEOUT,
            tokio_rustls::TlsConnector::from(Arc::clone(&self.client)).connect(name, stream),
        )
        .await
        .map_err(|_| handshake_timeout())?
        .map_err(peer_handshake_error)?;
        let certificate = tls
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certificates| certificates.first())
            .ok_or_else(|| invalid_peer("peer TLS certificate is missing"))?;
        require_exact_peer_identity(certificate, &expected)?;
        drop(permit);
        Ok(tls)
    }
}

fn validate_peer_map(local_node_id: u64, peers: &BTreeMap<u64, String>) -> io::Result<()> {
    if !peers.contains_key(&local_node_id) {
        return Err(invalid_config("static peer map does not include this node"));
    }
    let mut addresses = HashMap::<&str, u64>::new();
    for (node_id, address) in peers {
        if address.is_empty() {
            return Err(invalid_config("static peer map contains an empty address"));
        }
        if addresses.insert(address, *node_id).is_some() {
            return Err(invalid_config("static peer map has duplicate addresses"));
        }
    }
    Ok(())
}

fn peer_identities(
    _local_node_id: u64,
    cluster_name: &str,
    peers: &BTreeMap<u64, String>,
) -> HashMap<String, u64> {
    peers
        .keys()
        .map(|node_id| (identity_for(*node_id, cluster_name), *node_id))
        .collect()
}

fn identity_for(node_id: u64, cluster_name: &str) -> String {
    let hash = BASE32_NOPAD
        .encode(&Sha256::digest(cluster_name.as_bytes()))
        .to_ascii_lowercase();
    format!("n{node_id}.c-{hash}.peer.runnel.invalid")
}

fn open_file(path: &Path, label: &str) -> io::Result<File> {
    File::open(path).map_err(|_| {
        invalid_config(match label {
            "peer trust bundle" => "peer trust bundle cannot be opened",
            "peer certificate chain" => "peer certificate chain cannot be opened",
            "peer private key" => "peer private key cannot be opened",
            _ => "peer credential file cannot be opened",
        })
    })
}

fn validate_local_certificate(
    roots: &Arc<RootCertStore>,
    certificates: &[CertificateDer<'static>],
    identity: &str,
) -> io::Result<()> {
    let leaf = certificates
        .first()
        .ok_or_else(|| invalid_config("peer certificate chain contains no certificates"))?;
    let name = ServerName::try_from(identity.to_owned())
        .map_err(|_| invalid_config("local peer identity is invalid"))?;
    let server_verifier = rustls::client::WebPkiServerVerifier::builder(Arc::clone(roots))
        .build()
        .map_err(|_| invalid_config("peer trust bundle cannot configure server verification"))?;
    server_verifier
        .verify_server_cert(leaf, &certificates[1..], &name, &[], UnixTime::now())
        .map_err(|_| {
            invalid_config("local peer certificate is not valid for server authentication")
        })?;
    let client_verifier = WebPkiClientVerifier::builder(Arc::clone(roots))
        .build()
        .map_err(|_| invalid_config("peer trust bundle cannot configure client verification"))?;
    client_verifier
        .verify_client_cert(leaf, &certificates[1..], UnixTime::now())
        .map_err(|_| {
            invalid_config("local peer certificate is not valid for client authentication")
        })?;
    Ok(())
}

fn require_exact_peer_identity(certificate: &CertificateDer<'_>, expected: &str) -> io::Result<()> {
    let actual = peer_identity_from_certificate(certificate)?;
    if actual == expected {
        return Ok(());
    }
    Err(invalid_peer(
        "peer certificate SAN does not match expected node and cluster",
    ))
}

fn peer_identity_from_certificate(certificate: &CertificateDer<'_>) -> io::Result<String> {
    let (_, parsed) = x509_parser::parse_x509_certificate(certificate.as_ref())
        .map_err(|_| invalid_peer("peer certificate is malformed"))?;
    let san = parsed
        .subject_alternative_name()
        .map_err(|_| invalid_peer("peer certificate SAN is malformed"))?
        .ok_or_else(|| invalid_peer("peer certificate has no SAN extension"))?;
    let mut identities = san
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::DNSName(name) if name.ends_with(".peer.runnel.invalid") => {
                Some((*name).to_owned())
            }
            _ => None,
        });
    let Some(identity) = identities.next() else {
        return Err(invalid_peer("peer certificate has no Runnel peer DNS SAN"));
    };
    if identities.next().is_some() {
        return Err(invalid_peer(
            "peer certificate has multiple Runnel peer DNS SANs",
        ));
    }
    Ok(identity)
}

fn invalid_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_peer(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

fn handshake_timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "peer TLS handshake timed out")
}

fn peer_handshake_error(error: io::Error) -> io::Error {
    let message = match error
        .get_ref()
        .and_then(|source| source.downcast_ref::<rustls::Error>())
    {
        Some(rustls::Error::InvalidCertificate(
            rustls::CertificateError::NotValidForName
            | rustls::CertificateError::NotValidForNameContext { .. },
        )) => "peer TLS certificate identity does not match the expected node",
        Some(rustls::Error::InvalidCertificate(_))
        | Some(rustls::Error::NoCertificatesPresented) => "peer TLS certificate validation failed",
        Some(rustls::Error::PeerIncompatible(_)) => "peer TLS protocol version is incompatible",
        _ => "peer TLS handshake failed",
    };
    invalid_peer(message)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        KeyUsagePurpose,
    };
    use std::fs;
    use tempfile::TempDir;

    struct TestAuthority {
        ca_params: CertificateParams,
        ca_key: KeyPair,
        ca_cert_pem: String,
    }

    impl TestAuthority {
        fn new() -> Self {
            let mut ca_params = CertificateParams::default();
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params.key_usages = vec![
                KeyUsagePurpose::KeyCertSign,
                KeyUsagePurpose::CrlSign,
                KeyUsagePurpose::DigitalSignature,
            ];
            let ca_key = KeyPair::generate().unwrap();
            let ca_cert = ca_params.self_signed(&ca_key).unwrap();
            let ca_cert_pem = ca_cert.pem();
            Self {
                ca_params,
                ca_key,
                ca_cert_pem,
            }
        }

        fn issue(&self, sans: Vec<String>) -> CredentialFiles {
            let leaf_directory = tempfile::tempdir().unwrap();
            let issuer = Issuer::from_params(&self.ca_params, &self.ca_key);
            let mut leaf_params = CertificateParams::new(sans).unwrap();
            leaf_params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ];
            leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            let leaf_key = KeyPair::generate().unwrap();
            let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer).unwrap();

            let ca = leaf_directory.path().join("ca.pem");
            let cert = leaf_directory.path().join("peer.pem");
            let key = leaf_directory.path().join("peer-key.pem");
            fs::write(&ca, &self.ca_cert_pem).unwrap();
            fs::write(&cert, leaf_cert.pem()).unwrap();
            fs::write(&key, leaf_key.serialize_pem()).unwrap();
            CredentialFiles {
                _directory: leaf_directory,
                ca,
                cert,
                key,
            }
        }
    }

    struct CredentialFiles {
        _directory: TempDir,
        ca: std::path::PathBuf,
        cert: std::path::PathBuf,
        key: std::path::PathBuf,
    }

    impl CredentialFiles {
        fn new(node_id: u64, cluster: &str) -> Self {
            TestAuthority::new().issue(vec![identity_for(node_id, cluster)])
        }

        fn new_with_sans(sans: Vec<String>) -> Self {
            TestAuthority::new().issue(sans)
        }

        fn config(&self, node_id: u64, peers: &BTreeMap<u64, String>) -> PeerTlsConfig {
            PeerTlsConfig::from_files(node_id, "events", peers, &self.ca, &self.cert, &self.key)
                .unwrap()
        }

        fn config_trusting(
            &self,
            node_id: u64,
            peers: &BTreeMap<u64, String>,
            trust_bundle: &Path,
        ) -> io::Result<PeerTlsConfig> {
            PeerTlsConfig::from_files(
                node_id,
                "events",
                peers,
                trust_bundle,
                &self.cert,
                &self.key,
            )
        }

        fn tls12_client(&self) -> Arc<ClientConfig> {
            let mut root_reader = BufReader::new(File::open(&self.ca).unwrap());
            let mut roots = RootCertStore::empty();
            for certificate in rustls_pemfile::certs(&mut root_reader) {
                roots.add(certificate.unwrap()).unwrap();
            }
            let mut cert_reader = BufReader::new(File::open(&self.cert).unwrap());
            let certificates = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let mut key_reader = BufReader::new(File::open(&self.key).unwrap());
            let key = rustls_pemfile::private_key(&mut key_reader)
                .unwrap()
                .unwrap();
            Arc::new(
                ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
                    .with_root_certificates(roots)
                    .with_client_auth_cert(certificates, key)
                    .unwrap(),
            )
        }
    }

    pub(crate) fn peer_pair_configs(
        peers: &BTreeMap<u64, String>,
    ) -> (Arc<PeerTlsConfig>, Arc<PeerTlsConfig>) {
        let authority = TestAuthority::new();
        let node_zero = authority.issue(vec![identity_for(0, "events")]);
        let node_one = authority.issue(vec![identity_for(1, "events")]);
        (
            Arc::new(node_zero.config(0, peers)),
            Arc::new(node_one.config(1, peers)),
        )
    }

    #[test]
    fn identity_has_canonical_node_and_cluster_encoding() {
        assert_eq!(
            identity_for(7, "events"),
            "n7.c-qysbpophynzaxszsmpgyooyjrewxq6bdw342b5ct4qucjrne2s3a.peer.runnel.invalid"
        );
    }

    #[test]
    fn peer_tls_configuration_is_tls13_only_and_rejects_early_data() {
        let files = CredentialFiles::new(0, "events");
        let peers = BTreeMap::from([
            (0, "127.0.0.1:7000".to_owned()),
            (1, "127.0.0.1:7001".to_owned()),
        ]);
        let config = files.config(0, &peers);
        assert!(!config.client.enable_early_data);
        assert_eq!(config.server.max_early_data_size, 0);
        assert!(!config.server.send_half_rtt_data);
    }

    #[tokio::test]
    async fn mutually_authenticated_connection_returns_the_bound_peer_id() {
        let peers = BTreeMap::from([
            (0, "127.0.0.1:7000".to_owned()),
            (1, "127.0.0.1:7001".to_owned()),
        ]);
        let authority = TestAuthority::new();
        let node_zero = authority.issue(vec![identity_for(0, "events")]);
        let node_one = authority.issue(vec![identity_for(1, "events")]);
        let client = node_zero.config(0, &peers);
        let server = node_one.config(1, &peers);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let server_handshake = async {
            let (stream, _) = listener.accept().await.unwrap();
            let permit = server.try_acquire_inbound_handshake().unwrap();
            server.accept(stream, permit).await
        };
        let client_handshake = client.connect(1, &address);
        let (server_result, client_result) = tokio::join!(server_handshake, client_handshake);
        let (server_stream, peer_id) = server_result.unwrap();
        let client_stream = client_result.unwrap();
        assert_eq!(peer_id, 0);
        assert_eq!(
            server_stream.get_ref().1.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3)
        );
        assert_eq!(
            client_stream.get_ref().1.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3)
        );
    }

    #[tokio::test]
    async fn outbound_tls_rejects_a_valid_certificate_for_the_wrong_target_node() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peers = BTreeMap::from([
            (0, "127.0.0.1:7000".to_owned()),
            (1, "127.0.0.1:7001".to_owned()),
            (2, address.clone()),
        ]);
        let authority = TestAuthority::new();
        let node_zero = authority.issue(vec![identity_for(0, "events")]);
        let node_two = authority.issue(vec![identity_for(2, "events")]);
        let client = Arc::new(node_zero.config(0, &peers));
        let server = Arc::new(node_two.config(2, &peers));

        let server_handshake = async {
            let (stream, _) = listener.accept().await.unwrap();
            let permit = server.try_acquire_inbound_handshake().unwrap();
            server.accept(stream, permit).await
        };
        let client_handshake = client.connect(1, &address);
        let (_server_result, client_result) = tokio::join!(server_handshake, client_handshake);
        assert!(client_result.is_err());
    }

    #[test]
    fn peer_identity_must_match_the_exact_node_and_cluster_name() {
        let files = CredentialFiles::new(2, "events");
        let mut reader = BufReader::new(File::open(files.cert).unwrap());
        let certificate = rustls_pemfile::certs(&mut reader).next().unwrap().unwrap();
        assert!(require_exact_peer_identity(&certificate, &identity_for(2, "events")).is_ok());
        assert!(require_exact_peer_identity(&certificate, &identity_for(3, "events")).is_err());
        assert!(
            require_exact_peer_identity(&certificate, &identity_for(2, "other-cluster")).is_err()
        );
    }

    #[test]
    fn startup_rejects_wrong_local_identity_and_untrusted_leaf() {
        let peers = BTreeMap::from([
            (0, "127.0.0.1:7000".to_owned()),
            (1, "127.0.0.1:7001".to_owned()),
        ]);
        let wrong_node = CredentialFiles::new(1, "events");
        assert!(
            PeerTlsConfig::from_files(
                0,
                "events",
                &peers,
                &wrong_node.ca,
                &wrong_node.cert,
                &wrong_node.key,
            )
            .is_err()
        );

        let valid_leaf = CredentialFiles::new(0, "events");
        let unrelated_root = CredentialFiles::new(1, "events");
        assert!(
            valid_leaf
                .config_trusting(0, &peers, &unrelated_root.ca)
                .is_err()
        );
    }

    #[test]
    fn wildcard_and_multiple_peer_sans_do_not_produce_an_identity() {
        let wildcard = CredentialFiles::new_with_sans(vec!["*.peer.runnel.invalid".to_owned()]);
        let mut reader = BufReader::new(File::open(wildcard.cert).unwrap());
        let certificate = rustls_pemfile::certs(&mut reader).next().unwrap().unwrap();
        assert!(require_exact_peer_identity(&certificate, &identity_for(0, "events")).is_err());

        let multiple = CredentialFiles::new_with_sans(vec![
            identity_for(0, "events"),
            identity_for(1, "events"),
        ]);
        let mut reader = BufReader::new(File::open(multiple.cert).unwrap());
        let certificate = rustls_pemfile::certs(&mut reader).next().unwrap().unwrap();
        assert!(peer_identity_from_certificate(&certificate).is_err());
    }

    #[tokio::test]
    async fn tls12_only_client_cannot_connect_to_the_tls13_peer_listener() {
        let peers = BTreeMap::from([
            (0, "127.0.0.1:7000".to_owned()),
            (1, "127.0.0.1:7001".to_owned()),
        ]);
        let authority = TestAuthority::new();
        let node_zero = authority.issue(vec![identity_for(0, "events")]);
        let node_one = authority.issue(vec![identity_for(1, "events")]);
        let server = node_one.config(1, &peers);
        let tls12_client = node_zero.tls12_client();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_handshake = async {
            let (stream, _) = listener.accept().await.unwrap();
            let permit = server.try_acquire_inbound_handshake().unwrap();
            server.accept(stream, permit).await
        };
        let client_handshake = async {
            let stream = TcpStream::connect(address).await.unwrap();
            let name = ServerName::try_from(identity_for(1, "events")).unwrap();
            tokio_rustls::TlsConnector::from(tls12_client)
                .connect(name, stream)
                .await
        };
        let (server_result, client_result) = tokio::join!(server_handshake, client_handshake);
        assert!(server_result.is_err());
        assert!(client_result.is_err());
    }

    #[test]
    fn inbound_handshake_admission_is_bounded() {
        let files = CredentialFiles::new(0, "events");
        let peers = BTreeMap::from([
            (0, "127.0.0.1:7000".to_owned()),
            (1, "127.0.0.1:7001".to_owned()),
        ]);
        let config = files.config(0, &peers);
        let permits = (0..MAX_SIMULTANEOUS_HANDSHAKES)
            .map(|_| config.try_acquire_inbound_handshake().unwrap())
            .collect::<Vec<_>>();
        assert!(config.try_acquire_inbound_handshake().is_none());
        drop(permits);
        assert!(config.try_acquire_inbound_handshake().is_some());
    }

    #[test]
    fn inbound_peer_session_admission_is_bounded() {
        let peers = BTreeMap::from([
            (0, "127.0.0.1:7000".to_owned()),
            (1, "127.0.0.1:7001".to_owned()),
        ]);
        let config = CredentialFiles::new(0, "events").config(0, &peers);
        let permits = (0..MAX_ACTIVE_PEER_CONNECTIONS)
            .map(|_| config.try_acquire_inbound_connection().unwrap())
            .collect::<Vec<_>>();
        assert!(config.try_acquire_inbound_connection().is_none());
        drop(permits);
        assert!(config.try_acquire_inbound_connection().is_some());
    }

    #[test]
    fn duplicate_dial_addresses_are_rejected() {
        let peers = BTreeMap::from([(0, "peer:7000".to_owned()), (1, "peer:7000".to_owned())]);
        assert_eq!(
            validate_peer_map(0, &peers).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn engine_membership_must_match_the_loaded_tls_identity_map() {
        let peers = BTreeMap::from([
            (0, "127.0.0.1:7000".to_owned()),
            (1, "127.0.0.1:7001".to_owned()),
        ]);
        let config = CredentialFiles::new(0, "events").config(0, &peers);

        assert!(config.validate_for(0, "events", &peers).is_ok());
        assert!(config.validate_for(1, "events", &peers).is_err());
        assert!(config.validate_for(0, "other-cluster", &peers).is_err());
        assert!(
            config
                .validate_for(
                    0,
                    "events",
                    &BTreeMap::from([
                        (0, "127.0.0.1:7000".to_owned()),
                        (2, "127.0.0.1:7002".to_owned()),
                    ])
                )
                .is_err()
        );
    }
}
