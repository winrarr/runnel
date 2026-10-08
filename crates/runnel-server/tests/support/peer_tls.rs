#![allow(dead_code)] // Shared across separate integration-test binaries and feature sets.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use data_encoding::BASE32_NOPAD;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
#[cfg(feature = "test-replacement-recovery")]
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use rustls::{ClientConfig, RootCertStore};
use sha2::{Digest, Sha256};
#[cfg(feature = "test-replacement-recovery")]
use tokio_rustls::TlsAcceptor;

const UNKNOWN_NODE_ID: u64 = 999;

#[derive(Clone)]
pub struct PeerCredentials {
    directory: PathBuf,
    cluster_name: String,
}

impl PeerCredentials {
    pub fn for_cluster(root: &Path, cluster_name: &str, peers: &BTreeMap<u64, String>) -> Self {
        let directory = root.join("peer-test-credentials");
        let mut node_ids = peers.keys().copied().collect::<Vec<_>>();
        node_ids.push(UNKNOWN_NODE_ID);
        node_ids.sort_unstable();
        node_ids.dedup();

        if !Self::is_complete(&directory, &node_ids) {
            if directory.exists() {
                fs::remove_dir_all(&directory)
                    .expect("old test peer credentials should be removable");
            }
            fs::create_dir_all(&directory)
                .expect("test peer credential directory should be writable");
            generate_credentials(&directory, cluster_name, &node_ids);
        }

        Self {
            directory,
            cluster_name: cluster_name.to_owned(),
        }
    }

    fn is_complete(directory: &Path, node_ids: &[u64]) -> bool {
        directory.join("ca.pem").is_file()
            && node_ids.iter().all(|node_id| {
                directory.join(format!("node-{node_id}.pem")).is_file()
                    && directory.join(format!("node-{node_id}-key.pem")).is_file()
            })
    }

    pub fn trust_bundle(&self) -> PathBuf {
        self.directory.join("ca.pem")
    }

    pub fn certificate_chain(&self, node_id: u64) -> PathBuf {
        self.directory.join(format!("node-{node_id}.pem"))
    }

    pub fn private_key(&self, node_id: u64) -> PathBuf {
        self.directory.join(format!("node-{node_id}-key.pem"))
    }

    pub fn unknown_node_id(&self) -> u64 {
        UNKNOWN_NODE_ID
    }

    pub fn client_config(&self, node_id: u64) -> Arc<ClientConfig> {
        let roots = self.roots();
        let certificates = read_certificates(&self.certificate_chain(node_id));
        let key = read_private_key(&self.private_key(node_id));
        let mut config = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, key)
            .expect("test client certificate and key should match");
        config.enable_early_data = false;
        Arc::new(config)
    }

    #[cfg(feature = "test-replacement-recovery")]
    pub fn server_acceptor(&self, node_id: u64) -> TlsAcceptor {
        let roots = Arc::new(self.roots());
        let verifier = rustls::server::WebPkiClientVerifier::builder(roots)
            .build()
            .expect("test CA should configure client verification");
        let mut config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                read_certificates(&self.certificate_chain(node_id)),
                read_private_key(&self.private_key(node_id)),
            )
            .expect("test server certificate and key should match");
        config.max_early_data_size = 0;
        config.send_half_rtt_data = false;
        TlsAcceptor::from(Arc::new(config))
    }

    #[cfg(feature = "test-replacement-recovery")]
    pub fn node_id_for_certificate(&self, certificate: &[u8], node_ids: &[u64]) -> Option<u64> {
        node_ids.iter().copied().find(|node_id| {
            read_certificates(&self.certificate_chain(*node_id))[0].as_ref() == certificate
        })
    }

    pub fn peer_identity(&self, node_id: u64) -> String {
        identity_for(node_id, &self.cluster_name)
    }

    fn roots(&self) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        for certificate in read_certificates(&self.trust_bundle()) {
            roots
                .add(certificate)
                .expect("test CA should be a valid root");
        }
        roots
    }
}

fn generate_credentials(directory: &Path, cluster_name: &str, node_ids: &[u64]) {
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().expect("test CA key should generate");
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .expect("test CA certificate should generate");
    fs::write(directory.join("ca.pem"), ca_cert.pem()).expect("test CA should be written");
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    for node_id in node_ids {
        let mut leaf_params = CertificateParams::new(vec![identity_for(*node_id, cluster_name)])
            .expect("test peer DNS identity should be valid");
        leaf_params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        leaf_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let leaf_key = KeyPair::generate().expect("test peer key should generate");
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &issuer)
            .expect("test peer certificate should be signed");
        fs::write(
            directory.join(format!("node-{node_id}.pem")),
            leaf_cert.pem(),
        )
        .expect("test peer certificate should be written");
        fs::write(
            directory.join(format!("node-{node_id}-key.pem")),
            leaf_key.serialize_pem(),
        )
        .expect("test peer key should be written");
    }
}

fn identity_for(node_id: u64, cluster_name: &str) -> String {
    let digest = Sha256::digest(cluster_name.as_bytes());
    let encoded = BASE32_NOPAD.encode(&digest).to_ascii_lowercase();
    format!("n{node_id}.c-{encoded}.peer.runnel.invalid")
}

fn read_certificates(path: &Path) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_reader_iter(BufReader::new(
        File::open(path).expect("test certificate file should be readable"),
    ))
    .collect::<Result<Vec<_>, _>>()
    .expect("test certificate PEM should be valid")
}

fn read_private_key(path: &Path) -> PrivateKeyDer<'static> {
    PrivateKeyDer::pem_reader_iter(BufReader::new(
        File::open(path).expect("test private key file should be readable"),
    ))
    .next()
    .expect("test private key should be present")
    .expect("test private key PEM should be valid")
}
