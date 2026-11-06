//! Public connection admission keeps one deadline through dialing and TLS progress.

use greptimedb::{Client, ClientOptions, Endpoint, EndpointTransport, Error};
use socket2::{Domain, SockAddr, Socket, Type};
use std::{
    io,
    path::PathBuf,
    time::{Duration, Instant},
};

struct PathLease(PathBuf);
impl Drop for PathLease {
    fn drop(&mut self) {
        drop(std::fs::remove_file(&self.0));
    }
}

#[test]
fn scenario_client_unix_connect_uses_its_own_deadline() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join(format!(
        "greptimedb-client-deadline-{}.sock",
        std::process::id()
    ));
    let _lease = PathLease(path.clone());
    let listener = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    listener.bind(&SockAddr::unix(&path)?)?;
    listener.listen(0)?;
    let _queued = EndpointTransport::connect_unix_with_timeout(
        &path,
        Some(Duration::from_secs(1)),
        None,
        None,
    )?;
    let mut options =
        ClientOptions::from_endpoint(Endpoint::parse(&format!("unix://{}", path.display()))?);
    options.connect_timeout = Duration::from_millis(60);
    options.request_timeout = Duration::from_secs(5);
    let start = Instant::now();
    let error = match Client::connect(options) {
        Ok(_) => return Err("saturated endpoint unexpectedly admitted a client".into()),
        Err(error) => error,
    };
    assert!(matches!(error, Error::Transport(error) if error.kind() == io::ErrorKind::TimedOut));
    assert!(start.elapsed() >= Duration::from_millis(60));
    assert!(start.elapsed() < Duration::from_secs(1));
    Ok(())
}

#[cfg(feature = "tls")]
#[path = "support/timer.rs"]
mod timer;

#[cfg(feature = "tls")]
#[test]
fn slow_drip_tls_records_cannot_extend_the_connect_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    let (options, _) = tls_identity()?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let worker = thread::spawn(move || -> io::Result<usize> {
        let (mut socket, _) = listener.accept()?;
        socket.set_read_timeout(Some(Duration::from_secs(2)))?;
        assert!(
            socket.read(&mut [0; 4096])? > 0,
            "client sent its TLS hello"
        );
        let mut sent = 0;
        for byte in [22_u8, 3, 3, 0, 100, 2, 0, 0, 96, 0, 0, 0, 0, 0, 0] {
            if socket.write_all(&[byte]).is_err() {
                break;
            }
            sent += 1;
            timer::pause(Duration::from_millis(35))?;
        }
        Ok(sent)
    });
    let budget = Duration::from_millis(60);
    let start = Instant::now();
    let result = EndpointTransport::connect_tls(
        &address.to_string(),
        "localhost",
        &options,
        Some(budget),
        None,
        None,
        true,
    );
    let elapsed = start.elapsed();
    // Release accept even if local TLS validation failed before dialing.
    drop(std::net::TcpStream::connect(address));
    let error = match result {
        Ok(_) => panic!("incomplete TLS record must not complete a handshake"),
        Err(error) => error,
    };
    let sent = worker.join().expect("slow TLS peer")?;
    assert!(sent >= 2, "the peer made repeated partial progress");
    assert_eq!(
        error.kind(),
        io::ErrorKind::TimedOut,
        "elapsed={elapsed:?}: {error}"
    );
    assert!(elapsed >= budget);
    assert!(
        elapsed < budget + Duration::from_millis(80),
        "one connect budget must bound the handshake: {elapsed:?}"
    );
    Ok(())
}

#[cfg(feature = "tls")]
fn tls_identity() -> Result<
    (
        greptimedb::tls::TlsOptions,
        grpc::core::tls::TlsServerConfig,
    ),
    Box<dyn std::error::Error>,
> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use rustls::pki_types::PrivatePkcs8KeyDer;
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let pem = format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        STANDARD.encode(identity.cert.der())
    );
    let options = greptimedb::tls::TlsOptions {
        use_system_roots: false,
        ca_pem: Some(pem.into_bytes()),
        ..greptimedb::tls::TlsOptions::default()
    };
    let server = grpc::core::tls::TlsServerConfig::new(
        vec![identity.cert.der().clone()],
        PrivatePkcs8KeyDer::from(identity.key_pair.serialize_der()).into(),
    )?;
    Ok((options, server))
}

#[cfg(feature = "tls")]
#[test]
fn completed_tls_uses_independent_application_timeouts() -> Result<(), Box<dyn std::error::Error>> {
    use grpc::core::{
        tls::TlsTransport,
        transport::{SyncTransport, TcpTransport},
    };
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };
    for (connect_budget, read_budget, sends_payload) in [
        (Duration::from_millis(80), Duration::from_millis(300), true),
        (Duration::from_secs(1), Duration::from_millis(40), false),
    ] {
        let (options, config) = tls_identity()?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let worker = thread::spawn(move || -> io::Result<()> {
            let (socket, _) = listener.accept()?;
            let mut socket = TcpTransport::from_std(socket)?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            socket.set_write_timeout(Some(Duration::from_secs(2)))?;
            let mut tls = TlsTransport::server(socket, &config)?;
            tls.handshake()?;
            timer::pause(Duration::from_millis(170))?;
            if sends_payload {
                tls.write_all(b"x")?;
                tls.flush()?;
            }
            Ok(())
        });
        let write_budget = Duration::from_millis(250);
        let result = (|| -> io::Result<()> {
            let mut transport = EndpointTransport::connect_tls(
                &address.to_string(),
                "localhost",
                &options,
                Some(connect_budget),
                Some(read_budget),
                Some(write_budget),
                true,
            )?;
            let EndpointTransport::Tls(tls) = &transport else {
                panic!("TLS variant");
            };
            assert_eq!(tls.get_ref().get_ref().read_timeout()?, Some(read_budget));
            assert_eq!(tls.get_ref().get_ref().write_timeout()?, Some(write_budget));
            assert_eq!(transport.read_timeout()?, Some(read_budget));
            assert_eq!(transport.write_timeout()?, Some(write_budget));
            let start = Instant::now();
            let received = transport.read_exact(&mut [0; 1]);
            let elapsed = start.elapsed();
            if sends_payload {
                received?;
                assert!(
                    elapsed > connect_budget,
                    "the completed handshake must release its connect deadline"
                );
            } else {
                assert_eq!(
                    received.expect_err("application read timeout").kind(),
                    io::ErrorKind::TimedOut
                );
                assert!(elapsed >= read_budget);
                assert!(
                    elapsed < Duration::from_millis(140),
                    "application read budget: {elapsed:?}"
                );
            }
            Ok(())
        })();
        if result.is_err() {
            drop(std::net::TcpStream::connect(address));
        }
        let served = worker.join().expect("TLS timeout peer");
        result?;
        served?;
    }
    Ok(())
}

#[cfg(feature = "tls")]
#[test]
fn absent_connect_budget_does_not_apply_application_timeouts_to_handshake()
-> Result<(), Box<dyn std::error::Error>> {
    use grpc::core::{
        tls::TlsTransport,
        transport::{SyncTransport, TcpTransport},
    };
    use std::{io::Read, net::TcpListener, thread};
    let (options, config) = tls_identity()?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let delay = Duration::from_millis(150);
    let worker = thread::spawn(move || -> io::Result<()> {
        let (socket, _) = listener.accept()?;
        let mut socket = TcpTransport::from_std(socket)?;
        socket.set_read_timeout(Some(Duration::from_secs(2)))?;
        socket.set_write_timeout(Some(Duration::from_secs(2)))?;
        timer::pause(delay)?;
        let mut tls = TlsTransport::server(socket, &config)?;
        tls.handshake()?;
        timer::pause(delay)?;
        Ok(())
    });
    let read_budget = Duration::from_millis(40);
    let write_budget = Duration::from_millis(250);
    let result = (|| -> io::Result<()> {
        let start = Instant::now();
        let connected = EndpointTransport::connect_tls(
            &address.to_string(),
            "localhost",
            &options,
            None,
            Some(read_budget),
            Some(write_budget),
            true,
        );
        let elapsed = start.elapsed();
        let mut transport = connected.map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("unbounded connect failed after {elapsed:?}: {error}"),
            )
        })?;
        assert!(elapsed >= delay, "the delayed trusted handshake completed");
        assert_eq!(transport.read_timeout()?, Some(read_budget));
        assert_eq!(transport.write_timeout()?, Some(write_budget));
        let start = Instant::now();
        let error = transport
            .read(&mut [0; 1])
            .expect_err("application read timeout");
        let elapsed = start.elapsed();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(elapsed >= read_budget);
        assert!(
            elapsed < Duration::from_millis(120),
            "application budget after handshake: {elapsed:?}"
        );
        Ok(())
    })();
    if result.is_err() {
        drop(std::net::TcpStream::connect(address));
    }
    let served = worker.join().expect("delayed TLS peer");
    result?;
    served?;
    Ok(())
}

#[cfg(feature = "tls")]
#[test]
fn invalid_application_timeouts_are_rejected_before_tls_dialing()
-> Result<(), Box<dyn std::error::Error>> {
    let (options, _) = tls_identity()?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?.to_string();
    for (read_budget, write_budget) in [(Some(Duration::ZERO), None), (None, Some(Duration::ZERO))]
    {
        let error = match EndpointTransport::connect_tls(
            &address,
            "localhost",
            &options,
            Some(Duration::from_millis(60)),
            read_budget,
            write_budget,
            true,
        ) {
            Ok(_) => panic!("zero application timeout must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock),
            "invalid local arguments must not dial"
        );
    }
    Ok(())
}
