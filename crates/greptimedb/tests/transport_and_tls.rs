//! Loopback integration coverage for the polymorphic transport and TLS policy.
//! Every server thread is bounded and every client issues a finite number of RPCs.

use std::{io, net::SocketAddr, thread, time::Duration};

use greptimedb::transport::EndpointTransport;
use grpc::{
    Codec, Response,
    channel::{CallOptions, ChannelConfig, SyncChannel},
    core::{
        Status,
        transport::{MemoryPipe, SyncTransport, TcpTransportListener},
    },
    server::SyncServer,
    stream::StreamError,
};

#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use greptimedb::tls::TlsOptions;

#[cfg(feature = "tls")]
use grpc::core::{
    Code,
    tls::{TlsServerConfig, TlsTransport},
};

#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

#[cfg(unix)]
use grpc::core::transport::UnixTransportListener;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Bound for channel calls and server-side socket I/O.
const TIMEOUT: Duration = Duration::from_secs(5);
/// Distinct bounds, so swapping two of them cannot go unnoticed below.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(3);
const WRITE_TIMEOUT: Duration = Duration::from_secs(4);

/// Encodes a request value and decodes the response value over gRPC frames.
#[derive(Clone, Default)]
struct IntegerCodec;

impl Codec for IntegerCodec {
    type EncodeItem = u32;
    type DecodeItem = u32;
    type Error = io::Error;
    const CONTENT_TYPE: &'static str = "application/grpc+test";

    fn encode(&mut self, value: &u32, destination: &mut Vec<u8>) -> io::Result<()> {
        destination.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn decode(&mut self, payload: &[u8]) -> io::Result<u32> {
        Ok(u32::from_be_bytes(payload.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "integer payload must be four bytes",
            )
        })?))
    }
}

fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking operation unexpectedly pending"),
    }
}

fn io_status(error: io::Error) -> Status {
    Status::unavailable(error.to_string())
}

fn channel_config() -> ChannelConfig {
    ChannelConfig {
        default_timeout: Some(TIMEOUT),
        ..ChannelConfig::default()
    }
}

fn configure<T: SyncTransport>(transport: &mut T) -> io::Result<()> {
    transport.set_read_timeout(Some(TIMEOUT))?;
    transport.set_write_timeout(Some(TIMEOUT))
}

/// Registers the unary and server-streaming echo methods used by every test.
fn register<T: SyncTransport + 'static>(server: &mut SyncServer<T>) -> Result<(), Status> {
    server.register_unary("/test.Echo/Unary", IntegerCodec, |request| {
        Ok(Response::new(request.get_ref() + 1))
    })?;
    server.register_server_streaming("/test.Echo/Server", IntegerCodec, |request, sink| {
        sink.send(request.get_ref()).map_err(stream_status)?;
        sink.send(&(request.get_ref() + 1)).map_err(stream_status)?;
        Ok(())
    })?;
    Ok(())
}

fn spawn_serve<T: SyncTransport + Send + 'static>(
    transport: T,
) -> thread::JoinHandle<Result<(), Status>> {
    thread::spawn(move || {
        let mut server = SyncServer::new(transport)?;
        register(&mut server)?;
        server.serve_connection()
    })
}

/// Asserts that exactly the named variant is reported by the accessors.
fn assert_variant(transport: &EndpointTransport, expected: &str) {
    let observed = [
        ("tcp", transport.is_tcp()),
        ("tls", transport.is_tls()),
        ("unix", transport.is_unix()),
        ("memory", transport.is_memory()),
    ];
    let selected: Vec<&str> = observed
        .iter()
        .filter(|(_, enabled)| *enabled)
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(
        selected,
        [expected],
        "exactly the {expected} variant must be reported"
    );
}

fn unary_round_trip(channel: &mut SyncChannel<EndpointTransport>) -> Result<u32, Status> {
    channel.call_unary("/test.Echo/Unary", &41, &mut IntegerCodec)
}

/// Releases a server blocked in `accept` after a client failed before dialing.
fn release_listener(addr: SocketAddr) {
    drop(std::net::TcpStream::connect(addr));
}

#[test]
fn memory_pipe_unary_and_streaming_round_trip() -> TestResult {
    let (client, server) = MemoryPipe::pair();
    let worker = spawn_serve(server);

    let transport = EndpointTransport::from_memory_pipe(client);
    assert_variant(&transport, "memory");
    let mut channel = transport.into_channel(channel_config())?;
    assert_eq!(unary_round_trip(&mut channel)?, 42);

    let mut stream = channel.server_streaming(
        "/test.Echo/Server",
        &7,
        IntegerCodec,
        CallOptions::default(),
    )?;
    assert_eq!(stream.recv_next().map_err(stream_status)?, Some(7));
    assert_eq!(stream.finish().map_err(stream_status)?, [8]);
    drop(stream);
    drop(channel);

    let served = worker.join().expect("server thread");
    served?;
    Ok(())
}

#[test]
fn tcp_loopback_with_timeouts_and_nodelay() -> TestResult {
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let worker = thread::spawn(move || -> Result<(), Status> {
        let (mut socket, _) = listener.accept().map_err(io_status)?;
        configure(&mut socket).map_err(io_status)?;
        let mut server = SyncServer::new(socket)?;
        register(&mut server)?;
        server.serve_connection()
    });

    let result = (|| -> TestResult {
        let transport = EndpointTransport::connect_tcp(
            &addr.to_string(),
            Some(CONNECT_TIMEOUT),
            Some(READ_TIMEOUT),
            Some(WRITE_TIMEOUT),
            true,
        )?;
        assert_variant(&transport, "tcp");
        let EndpointTransport::Tcp(tcp) = &transport else {
            panic!("connect_tcp must build a TCP transport");
        };
        let stream = tcp.get_ref();
        assert!(stream.nodelay()?, "TCP_NODELAY must be enabled");
        assert_eq!(stream.read_timeout()?, Some(READ_TIMEOUT));
        assert_eq!(stream.write_timeout()?, Some(WRITE_TIMEOUT));
        assert_eq!(transport.peer_endpoint()?, addr.to_string());

        let mut channel = transport.into_channel(channel_config())?;
        assert_eq!(unary_round_trip(&mut channel)?, 42);
        drop(channel);
        Ok(())
    })();

    if result.is_err() {
        release_listener(addr);
    }
    let served = worker.join().expect("server thread");
    result?;
    served?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn unix_socket_round_trip() -> TestResult {
    let path = unique_socket_path();
    let listener = UnixTransportListener::bind(&path)?;
    let worker = thread::spawn(move || -> Result<(), Status> {
        let (mut socket, _) = listener.accept().map_err(io_status)?;
        configure(&mut socket).map_err(io_status)?;
        let mut server = SyncServer::new(socket)?;
        register(&mut server)?;
        server.serve_connection()
    });

    let result = (|| -> TestResult {
        let transport = EndpointTransport::connect_unix_with_timeout(
            &path,
            Some(CONNECT_TIMEOUT),
            Some(READ_TIMEOUT),
            Some(WRITE_TIMEOUT),
        )?;
        assert_variant(&transport, "unix");
        let EndpointTransport::Unix(unix) = &transport else {
            panic!("connect_unix_with_timeout must build a Unix transport");
        };
        assert_eq!(unix.get_ref().read_timeout()?, Some(READ_TIMEOUT));
        assert_eq!(unix.get_ref().write_timeout()?, Some(WRITE_TIMEOUT));

        let mut channel = transport.into_channel(channel_config())?;
        assert_eq!(unary_round_trip(&mut channel)?, 42);
        drop(channel);
        Ok(())
    })();

    if result.is_err() {
        drop(std::os::unix::net::UnixStream::connect(&path));
    }
    let served = worker.join().expect("server thread");
    let cleanup = std::fs::remove_file(&path);
    result?;
    served?;
    cleanup?;
    Ok(())
}

#[cfg(unix)]
fn unique_socket_path() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    let unique = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "greptimedb-transport-{}-{unique}.sock",
        std::process::id()
    ))
}

#[cfg(feature = "tls")]
struct TestCa {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

#[cfg(feature = "tls")]
impl TestCa {
    fn generate(common_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};

        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        let key = KeyPair::generate()?;
        let cert = params.self_signed(&key)?;
        Ok(Self { cert, key })
    }

    fn der(&self) -> CertificateDer<'static> {
        self.cert.der().clone()
    }

    fn pem(&self) -> Vec<u8> {
        pem_encode("CERTIFICATE", self.cert.der())
    }
}

#[cfg(feature = "tls")]
struct TestPki {
    ca: TestCa,
    server_chain: Vec<CertificateDer<'static>>,
    server_key: PrivateKeyDer<'static>,
    client_cert_pem: Vec<u8>,
    client_key_pem: Vec<u8>,
}

/// Generates a CA, a `localhost` server certificate, and a client identity.
#[cfg(feature = "tls")]
fn generate_pki() -> Result<TestPki, Box<dyn std::error::Error>> {
    use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, KeyPair};

    let ca = TestCa::generate("greptimedb test CA")?;
    let (server_chain, server_key) = server_identity(&ca, None)?;

    let mut client_params = CertificateParams::new(vec!["greptimedb-client".to_owned()])?;
    client_params
        .distinguished_name
        .push(DnType::CommonName, "greptimedb-client");
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = KeyPair::generate()?;
    let client_cert = client_params.signed_by(&client_key, &ca.cert, &ca.key)?;

    Ok(TestPki {
        ca,
        server_chain,
        server_key,
        client_cert_pem: pem_encode("CERTIFICATE", client_cert.der()),
        client_key_pem: pem_encode("PRIVATE KEY", &client_key.serialize_der()),
    })
}

#[cfg(feature = "tls")]
type Validity = ((i32, u8, u8), (i32, u8, u8));
#[cfg(feature = "tls")]
type ServerIdentity = (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>);

#[cfg(feature = "tls")]
fn server_identity(
    ca: &TestCa,
    validity: Option<Validity>,
) -> Result<ServerIdentity, Box<dyn std::error::Error>> {
    use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, KeyPair};

    let mut params = CertificateParams::new(vec!["localhost".to_owned()])?;
    params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    if let Some(((from_year, from_month, from_day), (to_year, to_month, to_day))) = validity {
        params.not_before = rcgen::date_time_ymd(from_year, from_month, from_day);
        params.not_after = rcgen::date_time_ymd(to_year, to_month, to_day);
    }
    let key = KeyPair::generate()?;
    let cert = params.signed_by(&key, &ca.cert, &ca.key)?;
    Ok((
        vec![cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
    ))
}

/// Builds a TLS 1.3 server that requires a client certificate from `client_ca`.
#[cfg(feature = "tls")]
fn server_config(
    client_ca: CertificateDer<'static>,
    server_chain: Vec<CertificateDer<'static>>,
    server_key: PrivateKeyDer<'static>,
) -> io::Result<TlsServerConfig> {
    let mut client_roots = rustls::RootCertStore::empty();
    client_roots.add(client_ca).map_err(io::Error::other)?;
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(client_roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .map_err(io::Error::other)?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(io::Error::other)?
    .with_client_cert_verifier(verifier)
    .with_single_cert(server_chain, server_key)
    .map_err(io::Error::other)?;
    Ok(TlsServerConfig::from_rustls(config))
}

#[cfg(feature = "tls")]
fn spawn_handshake_server(
    config: TlsServerConfig,
) -> io::Result<(SocketAddr, thread::JoinHandle<()>)> {
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let worker = thread::spawn(move || {
        let Ok((mut socket, _)) = listener.accept() else {
            return;
        };
        drop(configure(&mut socket));
        drop(TlsTransport::server(socket, &config).and_then(|mut tls| tls.handshake()));
    });
    Ok((addr, worker))
}

#[cfg(feature = "tls")]
fn assert_handshake_rejected(addr: SocketAddr, options: &TlsOptions, scenario: &str) {
    let result = EndpointTransport::connect_tls(
        &addr.to_string(),
        "localhost",
        options,
        Some(TIMEOUT),
        Some(TIMEOUT),
        Some(TIMEOUT),
        true,
    );
    // Unblock `accept` even if the client failed before dialing.
    release_listener(addr);
    let error = result
        .err()
        .unwrap_or_else(|| panic!("{scenario} must be rejected"));
    assert!(
        matches!(
            error.kind(),
            io::ErrorKind::InvalidData
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
        ),
        "unexpected {scenario} failure: {error}"
    );
}

/// Recognizes the EOF a server sees when the channel closes without `close_notify`.
#[cfg(feature = "tls")]
fn is_unclean_tls_eof(status: &Status) -> bool {
    status.code() == Code::Unavailable && status.message().contains("close_notify")
}

#[cfg(feature = "tls")]
#[test]
fn tls13_mtls_loopback_round_trip() -> TestResult {
    let pki = generate_pki()?;
    let tls_server_config = server_config(
        pki.ca.der(),
        pki.server_chain.clone(),
        pki.server_key.clone_key(),
    )?;
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;

    let worker = thread::spawn(move || -> Result<(), Status> {
        // First connection: the channel under test presents a client identity.
        let (mut socket, _) = listener.accept().map_err(io_status)?;
        configure(&mut socket).map_err(io_status)?;
        let mut tls = TlsTransport::server(socket, &tls_server_config).map_err(io_status)?;
        tls.handshake().map_err(io_status)?;
        let mut server = SyncServer::new(tls)?;
        register(&mut server)?;
        match server.serve_connection() {
            Ok(()) => {}
            // The channel owns the client transport, and dropping it closes the
            // TCP stream without a TLS close_notify.
            Err(status) if is_unclean_tls_eof(&status) => {}
            Err(status) => return Err(status),
        }

        // Second connection: a client without an identity must be rejected.
        let (mut socket, _) = listener.accept().map_err(io_status)?;
        configure(&mut socket).map_err(io_status)?;
        let mut tls = TlsTransport::server(socket, &tls_server_config).map_err(io_status)?;
        match tls.handshake() {
            Ok(()) => Err(Status::internal(
                "the TLS server accepted a client without a certificate",
            )),
            Err(error) if error.kind() == io::ErrorKind::InvalidData => Ok(()),
            Err(error) => Err(Status::unavailable(format!(
                "unexpected client-certificate handshake failure: {error}"
            ))),
        }
    });

    let result = (|| -> TestResult {
        let options = TlsOptions {
            use_system_roots: false,
            ..TlsOptions::default()
        }
        .with_ca_pem(pki.ca.pem())
        .with_client_identity_pem(pki.client_cert_pem.clone(), pki.client_key_pem.clone());
        let transport = EndpointTransport::connect_tls(
            &addr.to_string(),
            "localhost",
            &options,
            Some(TIMEOUT),
            Some(TIMEOUT),
            Some(TIMEOUT),
            true,
        )?;
        assert_variant(&transport, "tls");
        let EndpointTransport::Tls(tls) = &transport else {
            panic!("connect_tls must build a TLS transport");
        };
        assert_eq!(
            tls.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3)
        );
        assert_eq!(tls.negotiated_alpn(), Some(b"h2".as_slice()));
        assert!(tls.get_ref().get_ref().nodelay()?);
        assert_eq!(transport.peer_endpoint()?, addr.to_string());

        let mut channel = transport.into_channel(tls_channel_config())?;
        assert_eq!(unary_round_trip(&mut channel)?, 42);
        drop(channel);

        // A client that trusts the CA but presents no identity must not get a
        // working RPC; TLS 1.3 either fails the handshake or the first call.
        let anonymous = TlsOptions {
            use_system_roots: false,
            ..TlsOptions::default()
        }
        .with_ca_pem(pki.ca.pem());
        match EndpointTransport::connect_tls(
            &addr.to_string(),
            "localhost",
            &anonymous,
            Some(TIMEOUT),
            Some(TIMEOUT),
            Some(TIMEOUT),
            true,
        ) {
            Ok(transport) => {
                let mut channel = transport.into_channel(channel_config())?;
                let error = channel
                    .call_unary("/test.Echo/Unary", &1, &mut IntegerCodec)
                    .expect_err("a client without a certificate must be rejected");
                assert!(
                    matches!(error.code(), Code::Unavailable | Code::DeadlineExceeded),
                    "unexpected rejection status: {error}"
                );
            }
            Err(error) => assert!(
                matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData
                        | io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                ),
                "unexpected negative handshake failure: {error}"
            ),
        }
        Ok(())
    })();

    if result.is_err() {
        release_listener(addr);
    }
    let served = worker.join().expect("server thread");
    result?;
    served?;
    Ok(())
}

/// A server certificate signed outside the trusted CA must fail the handshake.
#[cfg(feature = "tls")]
#[test]
fn tls_rejects_a_server_signed_by_an_untrusted_ca() -> TestResult {
    let trusted = TestCa::generate("trusted CA")?;
    let signer = TestCa::generate("untrusted signer CA")?;
    let (chain, key) = server_identity(&signer, None)?;
    let (addr, worker) = spawn_handshake_server(server_config(signer.der(), chain, key)?)?;

    let options = TlsOptions {
        use_system_roots: false,
        ..TlsOptions::default()
    }
    .with_ca_pem(trusted.pem());
    assert_handshake_rejected(addr, &options, "a server signed by an untrusted CA");
    worker.join().expect("server thread");
    Ok(())
}

/// A server certificate outside its validity window must fail the handshake.
#[cfg(feature = "tls")]
#[test]
fn tls_rejects_an_expired_server_certificate() -> TestResult {
    let ca = TestCa::generate("expired-certificate CA")?;
    let (chain, key) = server_identity(&ca, Some(((2020, 1, 1), (2020, 1, 2))))?;
    let (addr, worker) = spawn_handshake_server(server_config(ca.der(), chain, key)?)?;

    let options = TlsOptions {
        use_system_roots: false,
        ..TlsOptions::default()
    }
    .with_ca_pem(ca.pem());
    assert_handshake_rejected(addr, &options, "an expired server certificate");
    worker.join().expect("server thread");
    Ok(())
}

/// An encrypted client key cannot be used by rustls and must be rejected early.
#[cfg(feature = "tls")]
#[test]
fn tls_rejects_an_encrypted_client_private_key() -> TestResult {
    let pki = generate_pki()?;
    let encrypted = pem_encode("ENCRYPTED PRIVATE KEY", b"not-a-real-key");
    let options = TlsOptions {
        use_system_roots: false,
        ..TlsOptions::default()
    }
    .with_ca_pem(pki.ca.pem())
    .with_client_identity_pem(pki.client_cert_pem, encrypted);

    let error = options
        .build_client_config()
        .err()
        .expect("an encrypted client key must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    let message = error.to_string();
    assert!(
        message.contains("encrypted") && message.contains("not supported"),
        "the rejection must name the encryption limitation: {message}"
    );
    Ok(())
}

/// The manual `Debug` implementation must never print private key material.
#[cfg(feature = "tls")]
#[test]
fn tls_options_debug_redacts_the_client_key() -> TestResult {
    // The body is base64 for `secret-key-material`.
    const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
                           c2VjcmV0LWtleS1tYXRlcmlhbA==\n\
                           -----END PRIVATE KEY-----\n";
    let options = TlsOptions::default().with_client_identity_pem(
        b"-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n".to_vec(),
        KEY_PEM.as_bytes().to_vec(),
    );
    let rendered = format!("{options:?}");
    assert!(
        rendered.contains(r#"client_key_pem: Some("<redacted>")"#),
        "the key field must be redacted: {rendered}"
    );
    for secret in [
        KEY_PEM,
        "c2VjcmV0LWtleS1tYXRlcmlhbA==",
        "secret-key-material",
    ] {
        assert!(
            !rendered.contains(secret),
            "the client key must not leak: {rendered}"
        );
    }
    Ok(())
}

/// Channel configuration for a TLS transport: gRPC requires `https` over TLS.
#[cfg(feature = "tls")]
fn tls_channel_config() -> ChannelConfig {
    ChannelConfig {
        authority: "localhost".into(),
        scheme: "https".into(),
        default_timeout: Some(TIMEOUT),
        ..ChannelConfig::default()
    }
}

/// `tls.domain` must override the dialed server name in both directions.
#[cfg(feature = "tls")]
#[test]
fn tls_domain_overrides_the_dialed_server_name() -> TestResult {
    let pki = generate_pki()?;
    let tls_server_config = server_config(
        pki.ca.der(),
        pki.server_chain.clone(),
        pki.server_key.clone_key(),
    )?;
    let listener = TcpTransportListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;

    let worker = thread::spawn(move || -> Result<(), Status> {
        // The certificate names `localhost`; the client dials a wrong name and
        // repairs it with `tls.domain`.
        let (mut socket, _) = listener.accept().map_err(io_status)?;
        configure(&mut socket).map_err(io_status)?;
        let mut tls = TlsTransport::server(socket, &tls_server_config).map_err(io_status)?;
        tls.handshake().map_err(io_status)?;
        let mut server = SyncServer::new(tls)?;
        register(&mut server)?;
        match server.serve_connection() {
            Ok(()) => {}
            Err(status) if is_unclean_tls_eof(&status) => {}
            Err(status) => return Err(status),
        }

        // The reverse override names a host the certificate does not cover, so
        // the client must abort the handshake.
        let (mut socket, _) = listener.accept().map_err(io_status)?;
        configure(&mut socket).map_err(io_status)?;
        let mut tls = TlsTransport::server(socket, &tls_server_config).map_err(io_status)?;
        // The client aborts certificate verification, so the server-side outcome is
        // expected to be an error and is not part of this phase's assertion.
        let _rejected_handshake = tls.handshake();
        Ok(())
    });

    let result = (|| -> TestResult {
        let identity = || {
            TlsOptions {
                use_system_roots: false,
                ..TlsOptions::default()
            }
            .with_ca_pem(pki.ca.pem())
            .with_client_identity_pem(pki.client_cert_pem.clone(), pki.client_key_pem.clone())
        };

        let overridden = TlsOptions {
            domain: Some("localhost".to_owned()),
            ..identity()
        };
        let transport = EndpointTransport::connect_tls(
            &addr.to_string(),
            "wrong.example",
            &overridden,
            Some(TIMEOUT),
            Some(TIMEOUT),
            Some(TIMEOUT),
            true,
        )?;
        let mut channel = transport.into_channel(tls_channel_config())?;
        assert_eq!(unary_round_trip(&mut channel)?, 42);
        drop(channel);

        let wrong = TlsOptions {
            domain: Some("wrong.example".to_owned()),
            ..identity()
        };
        let error = EndpointTransport::connect_tls(
            &addr.to_string(),
            "localhost",
            &wrong,
            Some(TIMEOUT),
            Some(TIMEOUT),
            Some(TIMEOUT),
            true,
        )
        .err()
        .expect("a domain outside the certificate must fail the handshake");
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::InvalidData
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
            ),
            "unexpected wrong-domain failure: {error}"
        );
        Ok(())
    })();

    if result.is_err() {
        release_listener(addr);
    }
    let served = worker.join().expect("server thread");
    result?;
    served?;
    Ok(())
}

/// Encodes DER as PEM; `rcgen` is built without its `pem` feature here.
#[cfg(feature = "tls")]
fn pem_encode(label: &str, der: &[u8]) -> Vec<u8> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

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
