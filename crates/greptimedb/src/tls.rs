//! TLS trust policy for GreptimeDB gRPC connections.
//!
//! [`TlsOptions`] collects the trust anchors and optional client identity for a
//! connection. [`TlsOptions::build_grpc_tls_config`] turns them into a
//! [`TlsClientConfig`] that permits TLS 1.3 only and advertises HTTP/2 via ALPN.

use std::{fs, io, path::Path, sync::Arc};

use grpc::core::tls::TlsClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Trust anchors and client identity for one TLS connection.
///
/// The default trusts the platform certificate store and presents no client
/// certificate.
#[derive(Clone)]
pub struct TlsOptions {
    /// Overrides the certificate verification name and SNI; unset uses the
    /// server name supplied by the dialer.
    pub domain: Option<String>,
    /// PEM-encoded trust anchors added on top of the platform store.
    pub ca_pem: Option<Vec<u8>>,
    /// PEM-encoded client certificate chain presented when the server asks.
    pub client_cert_pem: Option<Vec<u8>>,
    /// PEM-encoded private key matching `client_cert_pem`.
    pub client_key_pem: Option<Vec<u8>>,
    /// Whether the platform certificate store is trusted.
    pub use_system_roots: bool,
}

impl Default for TlsOptions {
    fn default() -> Self {
        Self {
            domain: None,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            use_system_roots: true,
        }
    }
}

/// Reports which material is configured without exposing its bytes.
///
/// A derived implementation would print the private key PEM into logs and
/// error messages, so the key is reported as redacted.
impl std::fmt::Debug for TlsOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TlsOptions")
            .field("domain", &self.domain)
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "<present>"))
            .field(
                "client_cert_pem",
                &self.client_cert_pem.as_ref().map(|_| "<present>"),
            )
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("use_system_roots", &self.use_system_roots)
            .finish()
    }
}

impl TlsOptions {
    /// Adds PEM-encoded trust anchors to the platform store.
    #[must_use]
    pub fn with_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(pem.into());
        self
    }

    /// Reads PEM-encoded trust anchors from a file.
    ///
    /// # Errors
    /// Returns the file system error when the file cannot be read.
    pub fn with_ca_pem_file(mut self, path: impl AsRef<Path>) -> io::Result<Self> {
        self.ca_pem = Some(fs::read(path)?);
        Ok(self)
    }

    /// Presents a client certificate chain and its private key.
    ///
    /// Both arguments are PEM-encoded; the key is the private key matching the
    /// leaf certificate in `cert_pem`.
    #[must_use]
    pub fn with_client_identity_pem(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_cert_pem = Some(cert_pem.into());
        self.client_key_pem = Some(key_pem.into());
        self
    }

    /// Builds a TLS 1.3 client configuration with HTTP/2 ALPN.
    ///
    /// Trust anchors come from the platform store when `use_system_roots` is
    /// set, plus every certificate in `ca_pem` when it is set. At least one
    /// anchor is required. A client identity is configured only when both
    /// `client_cert_pem` and `client_key_pem` are set.
    ///
    /// # Errors
    /// Returns [`io::ErrorKind::InvalidInput`] when no trust anchor is
    /// configured, when a PEM input cannot be parsed, when only one half of a
    /// client identity is set, when the client certificate and key do not form
    /// a usable identity, or when the crypto provider cannot offer TLS 1.3.
    pub fn build_client_config(&self) -> Result<TlsClientConfig, io::Error> {
        self.build_grpc_tls_config()
    }

    /// Resolves the name used for certificate verification and SNI.
    ///
    /// `domain` overrides the name supplied by the dialer when set. The result
    /// owns its name so it outlives the strings borrowed here.
    ///
    /// # Errors
    /// Returns [`io::ErrorKind::InvalidInput`] when the selected name is not a
    /// valid DNS name or IP address.
    pub fn server_name(
        &self,
        dialed: &str,
    ) -> Result<rustls::pki_types::ServerName<'static>, io::Error> {
        let name = self.domain.as_deref().unwrap_or(dialed);
        name.to_owned()
            .try_into()
            .map_err(|error| invalid_input(format!("invalid TLS server name {name:?}: {error}")))
    }

    /// Builds a TLS 1.3 client configuration with HTTP/2 ALPN.
    ///
    /// Trust anchors come from the platform store when `use_system_roots` is
    /// set, plus every certificate in `ca_pem` when it is set. At least one
    /// anchor is required. A client identity is configured only when both
    /// `client_cert_pem` and `client_key_pem` are set.
    ///
    /// # Errors
    /// Returns [`io::ErrorKind::InvalidInput`] when no trust anchor is
    /// configured, when a PEM input cannot be parsed, when only one half of a
    /// client identity is set, when the client certificate and key do not form
    /// a usable identity, or when the crypto provider cannot offer TLS 1.3.
    pub fn build_grpc_tls_config(&self) -> Result<TlsClientConfig, io::Error> {
        let mut roots = rustls::RootCertStore::empty();
        let mut system_failure = None;

        if self.use_system_roots {
            let native = rustls_native_certs::load_native_certs();
            let (loaded, _rejected) = roots.add_parsable_certificates(native.certs);
            if loaded == 0 {
                system_failure = Some(if native.errors.is_empty() {
                    "the platform store returned no usable certificates".to_owned()
                } else {
                    native
                        .errors
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ")
                });
            }
        }

        if let Some(pem) = &self.ca_pem {
            let mut cursor = pem.as_slice();
            let mut parsed = 0_usize;
            for certificate in rustls_pemfile::certs(&mut cursor) {
                let certificate = certificate.map_err(|error| {
                    invalid_input(format!("failed to parse a CA certificate: {error}"))
                })?;
                roots.add(certificate).map_err(|error| {
                    invalid_input(format!("failed to add a CA certificate: {error}"))
                })?;
                parsed += 1;
            }
            if parsed == 0 {
                return Err(invalid_input(
                    "the CA certificate PEM contains no certificates",
                ));
            }
        }

        if roots.is_empty() {
            return Err(invalid_input(match system_failure {
                Some(detail) => format!(
                    "no trust anchors configured: loading system root certificates failed: {detail}"
                ),
                None => "no trust anchors configured".to_owned(),
            }));
        }

        let identity = match (&self.client_cert_pem, &self.client_key_pem) {
            (None, None) => None,
            (Some(_), None) => {
                return Err(invalid_input(
                    "a client certificate PEM requires a client key PEM",
                ));
            }
            (None, Some(_)) => {
                return Err(invalid_input(
                    "a client key PEM requires a client certificate PEM",
                ));
            }
            (Some(cert_pem), Some(key_pem)) => Some(parse_identity(cert_pem, key_pem)?),
        };

        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| {
            invalid_input(format!(
                "the crypto provider does not support TLS 1.3: {error}"
            ))
        })?
        .with_root_certificates(roots);

        let config = match identity {
            Some((chain, key)) => builder.with_client_auth_cert(chain, key).map_err(|error| {
                invalid_input(format!(
                    "the client certificate and key were rejected: {error}"
                ))
            })?,
            None => builder.with_no_client_auth(),
        };

        Ok(TlsClientConfig::from_rustls(config))
    }
}

/// Parses a PEM certificate chain and its private key into rustls types.
fn parse_identity(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let mut cert_cursor = cert_pem;
    let chain = rustls_pemfile::certs(&mut cert_cursor)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            invalid_input(format!(
                "failed to parse the client certificate PEM: {error}"
            ))
        })?;
    if chain.is_empty() {
        return Err(invalid_input(
            "the client certificate PEM contains no certificates",
        ));
    }

    let key = load_private_key(key_pem)?;

    Ok((chain, key))
}

/// Parses an unencrypted PEM private key.
///
/// rustls cannot decrypt an encrypted key, so the encrypted PEM form is
/// rejected with an actionable message instead of a generic parse failure.
fn load_private_key(pem: &[u8]) -> io::Result<PrivateKeyDer<'static>> {
    const ENCRYPTED_MARKER: &[u8] = b"ENCRYPTED PRIVATE KEY";
    if pem
        .windows(ENCRYPTED_MARKER.len())
        .any(|window| window == ENCRYPTED_MARKER)
    {
        return Err(invalid_input(
            "encrypted client private key PEM is not supported; provide an unencrypted private key",
        ));
    }

    let mut cursor = pem;
    rustls_pemfile::private_key(&mut cursor)
        .map_err(|error| invalid_input(format!("failed to parse the client key PEM: {error}")))?
        .ok_or_else(|| invalid_input("the client key PEM contains no private key"))
}

/// Builds an [`io::Error`] that names a rejected TLS configuration.
fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use std::{
        fs, io,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};

    use super::TlsOptions;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn default_trusts_system_roots() {
        assert!(TlsOptions::default().use_system_roots);
    }

    #[test]
    fn with_ca_pem_and_identity_build_a_config() {
        let (ca_pem, cert_pem, key_pem) = ca_and_client_identity();
        let options = TlsOptions {
            use_system_roots: false,
            ..TlsOptions::default()
        }
        .with_ca_pem(ca_pem)
        .with_client_identity_pem(cert_pem, key_pem);

        let config = options
            .build_grpc_tls_config()
            .expect("CA-anchored client identity must build");
        assert_eq!(config.get_ref().alpn_protocols, [b"h2".to_vec()]);
        assert!(
            !config.get_ref().enable_early_data,
            "0-RTT must stay disabled"
        );
        assert!(
            config.get_ref().client_auth_cert_resolver.has_certs(),
            "the client identity must be installed"
        );
    }

    #[test]
    fn system_roots_and_custom_ca_build_a_config() {
        let (ca_pem, _cert_pem, _key_pem) = ca_and_client_identity();
        let options = TlsOptions::default().with_ca_pem(ca_pem);
        assert!(options.use_system_roots);

        let config = options
            .build_grpc_tls_config()
            .expect("platform roots plus a custom CA must build");
        assert_eq!(config.get_ref().alpn_protocols, [b"h2".to_vec()]);
    }

    #[test]
    fn certificate_without_key_is_rejected() {
        let (ca_pem, cert_pem, _key_pem) = ca_and_client_identity();
        let options = TlsOptions {
            use_system_roots: false,
            client_cert_pem: Some(cert_pem),
            ..TlsOptions::default()
        }
        .with_ca_pem(ca_pem);

        let error = options
            .build_grpc_tls_config()
            .err()
            .expect("a client certificate without a key must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn mismatched_key_is_rejected() {
        let (ca_pem, cert_pem, _key_pem) = ca_and_client_identity();
        let other_key = KeyPair::generate().expect("second client key");
        let key_pem = pem_encode("PRIVATE KEY", &other_key.serialize_der());
        let options = TlsOptions {
            use_system_roots: false,
            ..TlsOptions::default()
        }
        .with_ca_pem(ca_pem)
        .with_client_identity_pem(cert_pem, key_pem);

        let error = options
            .build_grpc_tls_config()
            .err()
            .expect("a key that does not match the certificate must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn key_without_certificate_is_rejected() {
        let (ca_pem, _cert_pem, key_pem) = ca_and_client_identity();
        let options = TlsOptions {
            use_system_roots: false,
            client_key_pem: Some(key_pem),
            ..TlsOptions::default()
        }
        .with_ca_pem(ca_pem);

        let error = options
            .build_grpc_tls_config()
            .err()
            .expect("a key without a certificate must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn empty_client_certificate_chain_is_rejected() {
        let (ca_pem, _cert_pem, key_pem) = ca_and_client_identity();
        let options = TlsOptions {
            use_system_roots: false,
            ..TlsOptions::default()
        }
        .with_ca_pem(ca_pem)
        .with_client_identity_pem(Vec::new(), key_pem);

        let error = options
            .build_grpc_tls_config()
            .err()
            .expect("an empty certificate chain must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn client_key_pem_without_a_key_is_rejected() {
        let (ca_pem, cert_pem, _key_pem) = ca_and_client_identity();
        let options = TlsOptions {
            use_system_roots: false,
            ..TlsOptions::default()
        }
        .with_ca_pem(ca_pem)
        .with_client_identity_pem(cert_pem, b"not a key".to_vec());

        let error = options
            .build_grpc_tls_config()
            .err()
            .expect("a key PEM without a private key must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn invalid_ca_pem_is_rejected() {
        let options = TlsOptions {
            use_system_roots: false,
            ..TlsOptions::default()
        }
        .with_ca_pem(b"not a certificate".to_vec());

        let error = options
            .build_grpc_tls_config()
            .err()
            .expect("garbage CA bytes must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn with_ca_pem_file_reads_the_file() {
        let (ca_pem, _cert_pem, _key_pem) = ca_and_client_identity();
        let path = unique_temp_path();
        fs::write(&path, &ca_pem).expect("write the CA PEM file");

        let options = TlsOptions::default()
            .with_ca_pem_file(&path)
            .expect("read the CA PEM file");
        fs::remove_file(&path).expect("remove the CA PEM file");

        assert_eq!(options.ca_pem.as_deref(), Some(ca_pem.as_slice()));
        assert!(options.use_system_roots);
    }

    /// Returns PEM trust anchor, client certificate, and client key.
    ///
    /// `rcgen` is built without its `pem` feature here, so the test encodes the
    /// DER output itself.
    fn ca_and_client_identity() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut ca_params =
            CertificateParams::new(Vec::<String>::new()).expect("CA certificate parameters");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().expect("CA key pair");
        let ca = ca_params
            .self_signed(&ca_key)
            .expect("self-signed CA certificate");

        let client_params = CertificateParams::new(vec!["client".to_owned()])
            .expect("client certificate parameters");
        let client_key = KeyPair::generate().expect("client key pair");
        let client = client_params
            .signed_by(&client_key, &ca, &ca_key)
            .expect("CA-signed client certificate");

        (
            pem_encode("CERTIFICATE", ca.der()),
            pem_encode("CERTIFICATE", client.der()),
            pem_encode("PRIVATE KEY", &client_key.serialize_der()),
        )
    }

    fn unique_temp_path() -> PathBuf {
        let unique = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "greptimedb-tls-{}-{unique}.pem",
            std::process::id()
        ))
    }

    fn pem_encode(label: &str, der: &[u8]) -> Vec<u8> {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut body = Vec::with_capacity(der.len().div_ceil(3) * 4);
        for chunk in der.chunks(3) {
            let first = u32::from(chunk[0]);
            let second = chunk.get(1).copied().map_or(0, u32::from);
            let third = chunk.get(2).copied().map_or(0, u32::from);
            let triple = (first << 16) | (second << 8) | third;
            for (position, shift) in [18, 12, 6, 0].into_iter().enumerate() {
                if position < chunk.len() + 1 {
                    body.push(ALPHABET[((triple >> shift) & 0x3f) as usize]);
                } else {
                    body.push(b'=');
                }
            }
        }

        let mut pem = Vec::with_capacity(body.len() + label.len() * 2 + 32);
        pem.extend_from_slice(format!("-----BEGIN {label}-----\n").as_bytes());
        for line in body.chunks(64) {
            pem.extend_from_slice(line);
            pem.push(b'\n');
        }
        pem.extend_from_slice(format!("-----END {label}-----\n").as_bytes());
        pem
    }
}
