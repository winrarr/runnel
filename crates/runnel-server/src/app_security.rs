use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use runnel_protocol::{BearerToken, Request, SecurityRole};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroize;

const MIN_TOKEN_BYTES: usize = 32;
const MAX_POLICY_BYTES: u64 = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SecurityConfigError {
    #[error("application TLS certificate or key could not be read")]
    TlsFiles,
    #[error("application TLS certificate or key is incomplete or invalid")]
    InvalidTlsIdentity,
    #[error("application credential policy could not be read")]
    PolicyFile,
    #[error("application credential policy is invalid")]
    InvalidPolicy,
    #[error("application TLS and credential policy must be configured together")]
    PartialSecureConfiguration,
}

#[derive(Clone)]
pub(crate) struct ApplicationSecurity {
    pub(crate) tls_acceptor: Option<TlsAcceptor>,
    pub(crate) credentials: Option<Arc<CredentialPolicy>>,
}

impl ApplicationSecurity {
    pub(crate) fn development() -> Self {
        Self {
            tls_acceptor: None,
            credentials: None,
        }
    }

    pub(crate) fn new(
        tls_acceptor: Option<TlsAcceptor>,
        credentials: Option<CredentialPolicy>,
    ) -> Result<Self, SecurityConfigError> {
        validate_secure_pair(tls_acceptor.is_some(), credentials.is_some())?;
        Ok(Self {
            tls_acceptor,
            credentials: credentials.map(Arc::new),
        })
    }

    pub(crate) const fn auth_required(&self) -> bool {
        self.credentials.is_some()
    }
}

impl fmt::Debug for ApplicationSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationSecurity")
            .field("tls_configured", &self.tls_acceptor.is_some())
            .field("credential_policy_configured", &self.credentials.is_some())
            .finish()
    }
}

pub(crate) fn load_tls_acceptor(
    certificate_path: &Path,
    private_key_path: &Path,
) -> Result<TlsAcceptor, SecurityConfigError> {
    let certificates = read_certificates(certificate_path)?;
    let private_key = read_private_key(private_key_path)?;
    let mut config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|_| SecurityConfigError::InvalidTlsIdentity)?;
    config.max_early_data_size = 0;
    config.send_half_rtt_data = false;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, SecurityConfigError> {
    let file = File::open(path).map_err(|_| SecurityConfigError::TlsFiles)?;
    let mut reader = BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| SecurityConfigError::InvalidTlsIdentity)?;
    if certificates.is_empty() {
        return Err(SecurityConfigError::InvalidTlsIdentity);
    }
    Ok(certificates)
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, SecurityConfigError> {
    let file = File::open(path).map_err(|_| SecurityConfigError::TlsFiles)?;
    let metadata = file.metadata().map_err(|_| SecurityConfigError::TlsFiles)?;
    validate_private_file(&metadata).map_err(|_| SecurityConfigError::InvalidTlsIdentity)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|_| SecurityConfigError::InvalidTlsIdentity)?
        .ok_or(SecurityConfigError::InvalidTlsIdentity)
}

fn validate_secure_pair(
    tls_configured: bool,
    credentials_configured: bool,
) -> Result<(), SecurityConfigError> {
    if tls_configured != credentials_configured {
        return Err(SecurityConfigError::PartialSecureConfiguration);
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDocument {
    credentials: Vec<PolicyCredential>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyCredential {
    id: String,
    sha256: String,
    role: PolicyRole,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PolicyRole {
    Application,
    Operator,
}

#[derive(Clone)]
struct CredentialVerifier {
    digest: [u8; 32],
    role: SecurityRole,
}

pub(crate) struct CredentialPolicy {
    verifiers: Vec<CredentialVerifier>,
}

impl CredentialPolicy {
    pub(crate) fn load(path: &Path) -> Result<Self, SecurityConfigError> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| SecurityConfigError::PolicyFile)?;
        let metadata = file
            .metadata()
            .map_err(|_| SecurityConfigError::PolicyFile)?;
        if !metadata.is_file() || metadata.len() > MAX_POLICY_BYTES {
            return Err(SecurityConfigError::InvalidPolicy);
        }
        validate_private_file(&metadata)?;

        let mut source = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_POLICY_BYTES + 1)
            .read_to_end(&mut source)
            .map_err(|_| SecurityConfigError::PolicyFile)?;
        if source.len() as u64 > MAX_POLICY_BYTES {
            source.zeroize();
            return Err(SecurityConfigError::InvalidPolicy);
        }
        let result = Self::parse(&source);
        source.zeroize();
        result
    }

    fn parse(source: &[u8]) -> Result<Self, SecurityConfigError> {
        let document: PolicyDocument =
            serde_json::from_slice(source).map_err(|_| SecurityConfigError::InvalidPolicy)?;
        if document.credentials.is_empty() || document.credentials.len() > 10_000 {
            return Err(SecurityConfigError::InvalidPolicy);
        }
        let mut ids = BTreeSet::new();
        let mut verifiers = Vec::with_capacity(document.credentials.len());
        for credential in document.credentials {
            if !valid_credential_id(&credential.id) || !ids.insert(credential.id.clone()) {
                return Err(SecurityConfigError::InvalidPolicy);
            }
            let digest =
                decode_digest(&credential.sha256).ok_or(SecurityConfigError::InvalidPolicy)?;
            if verifiers
                .iter()
                .any(|entry: &CredentialVerifier| bool::from(entry.digest.ct_eq(&digest)))
            {
                return Err(SecurityConfigError::InvalidPolicy);
            }
            let role = match credential.role {
                PolicyRole::Application => SecurityRole::Application,
                PolicyRole::Operator => SecurityRole::Operator,
            };
            verifiers.push(CredentialVerifier { digest, role });
        }
        Ok(Self { verifiers })
    }

    /// Authenticate one token without returning an identifier or a reason code.
    pub(crate) fn authenticate(&self, token: &BearerToken) -> Option<SecurityRole> {
        let value = token.expose_secret();
        let mut decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value.as_bytes())
            .ok()?;
        let mut canonical = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&decoded);
        let valid = decoded.len() == MIN_TOKEN_BYTES && canonical == value;
        decoded.zeroize();
        canonical.zeroize();
        if !valid {
            return None;
        }
        let candidate: [u8; 32] = Sha256::digest(value.as_bytes()).into();
        let mut matched_role = None;
        for entry in &self.verifiers {
            let matches = entry.digest.ct_eq(&candidate);
            if bool::from(matches) {
                matched_role = Some(entry.role);
            }
        }
        Some(matched_role).flatten()
    }

    #[cfg(test)]
    fn credential_count(&self) -> usize {
        self.verifiers.len()
    }
}

impl fmt::Debug for CredentialPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialPolicy")
            .field("credential_count", &self.verifiers.len())
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
fn generate_token() -> Result<(BearerToken, String), getrandom::Error> {
    let mut random = [0_u8; MIN_TOKEN_BYTES];
    getrandom::fill(&mut random)?;
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random);
    random.zeroize();
    let digest = Sha256::digest(token.as_bytes());
    let verifier = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    let token = BearerToken::parse(token).expect("32 random bytes have a canonical encoding");
    Ok((token, verifier))
}

pub(crate) fn request_is_authorized(role: SecurityRole, request: &Request) -> bool {
    role == SecurityRole::Operator || request.required_role() == SecurityRole::Application
}

fn decode_digest(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return None;
    }
    let mut digest = [0; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let high = hex_nibble(value.as_bytes()[index * 2])?;
        let low = hex_nibble(value.as_bytes()[index * 2 + 1])?;
        *byte = high << 4 | low;
    }
    Some(digest)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn valid_credential_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(unix)]
fn validate_private_file(metadata: &fs::Metadata) -> Result<(), SecurityConfigError> {
    use std::os::unix::fs::PermissionsExt;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(SecurityConfigError::InvalidPolicy);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_file(_: &fs::Metadata) -> Result<(), SecurityConfigError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn policy_authenticates_secret_and_redacts_verifier_and_id() {
        let (token, verifier) = generate_token().unwrap();
        let token_string = token.expose_secret().to_owned();
        let policy = CredentialPolicy::parse(
            format!(
                r#"{{"credentials":[{{"id":"operator-one","sha256":"{verifier}","role":"operator"}}]}}"#
            )
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(policy.authenticate(&token), Some(SecurityRole::Operator));
        assert_eq!(policy.credential_count(), 1);
        let formatted = format!("{policy:?}");
        assert!(!formatted.contains(&verifier));
        assert!(!formatted.contains("operator-one"));
        assert!(!formatted.contains(&token_string));

        let unknown = BearerToken::from_wire("A".repeat(43));
        assert_eq!(policy.authenticate(&unknown), None);
        let malformed = BearerToken::from_wire("bad".to_owned());
        assert_eq!(policy.authenticate(&malformed), None);
    }

    #[test]
    fn policy_rejects_empty_duplicate_incomplete_and_noncanonical_entries() {
        for invalid in [
            r#"{"credentials":[]}"#,
            r#"{"credentials":[{"id":"x","sha256":"","role":"application"}]}"#,
            r#"{"credentials":[{"id":"x","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","role":"application"},{"id":"x","sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","role":"operator"}]}"#,
            r#"{"credentials":[{"id":"x","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","role":"custom"}]}"#,
        ] {
            assert!(CredentialPolicy::parse(invalid.as_bytes()).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn policy_file_requires_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = TempDir::new().unwrap();
        let path = temporary.path().join("credentials.json");
        fs::write(&path, r#"{"credentials":[]}"#).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            CredentialPolicy::load(&path),
            Err(SecurityConfigError::InvalidPolicy)
        ));
    }

    #[test]
    fn tls_and_credentials_must_be_configured_together() {
        assert!(matches!(
            validate_secure_pair(true, false),
            Err(SecurityConfigError::PartialSecureConfiguration)
        ));
        assert!(matches!(
            validate_secure_pair(false, true),
            Err(SecurityConfigError::PartialSecureConfiguration)
        ));
        assert!(validate_secure_pair(false, false).is_ok());
        assert!(validate_secure_pair(true, true).is_ok());
    }
}
