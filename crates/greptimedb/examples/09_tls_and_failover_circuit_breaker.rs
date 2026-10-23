//! Offline: `cargo run -p greptimedb --example 09_tls_and_failover_circuit_breaker`.
//! Optional live health check: set `GREPTIMEDB_ENDPOINT=grpcs://host:4001` and
//! `GREPTIMEDB_CA_PEM=/path/to/ca.pem` for a private certificate authority.
//!
//! Circuit health belongs to each calling-thread client. A half-open endpoint
//! must pass HealthCheck before admitting application RPCs. Ingestion hints are
//! not permission to replay an uncertain write: only the server's explicit
//! retry decision authorizes replay after bytes have been transmitted.

#[path = "../tests/support/timer.rs"]
mod timer;

use std::{
    cell::RefCell,
    net::{Shutdown, TcpListener, TcpStream},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, ensure};
use greptimedb::{
    Client, ClientOptions, Endpoint, EndpointSelection, Error, FailoverConfig, FailoverEvent,
    FailoverPolicy, WriteHints, proto::v1,
};
use grpc::core::transport::{SyncTransport, TcpTransport};
use grpc::{ProstCodec, Request, Response, Status, SyncServer};

const HEALTH: &str = "/greptime.v1.HealthCheck/HealthCheck";
const PROM: &str = "/greptime.v1.PrometheusGateway/Handle";
const COOLDOWN: Duration = Duration::from_millis(20);

fn main() -> Result<()> {
    demonstrate_selection()?;
    demonstrate_replay_safety()?;
    #[cfg(feature = "tls")]
    demonstrate_tls()?;
    demonstrate_recovery()?;
    if let Ok(endpoint) = std::env::var("GREPTIMEDB_ENDPOINT") {
        let mut options = ClientOptions::from_endpoint(Endpoint::parse(&endpoint)?);
        #[cfg(feature = "tls")]
        if let Ok(path) = std::env::var("GREPTIMEDB_CA_PEM") {
            options.tls = Some(greptimedb::TlsOptions::default().with_ca_pem_file(path)?);
        }
        options.request_timeout = Duration::from_secs(3);
        println!(
            "live health: {:?}",
            Client::connect(options)?.probe_health()?
        );
    }
    Ok(())
}

fn demonstrate_selection() -> Result<()> {
    for selection in [
        EndpointSelection::RoundRobin,
        EndpointSelection::Sticky,
        EndpointSelection::Random,
        EndpointSelection::LatencyAware,
    ] {
        let config = FailoverConfig {
            selection,
            max_attempts: 2,
            circuit_breaker_threshold: 1,
            circuit_breaker_cooldown: COOLDOWN,
            random_seed: Some(7),
            endpoints: vec!["primary".into(), "secondary".into()],
        };
        let mut policy = FailoverPolicy::from_config(config);
        policy.record_success(0, Duration::from_micros(900));
        policy.record_success(1, Duration::from_micros(100));
        policy.record_success(1, Duration::from_micros(140));
        let chosen = policy.select_endpoint(Instant::now())?;
        if selection == EndpointSelection::LatencyAware {
            ensure!(chosen == 1);
        }
        println!(
            "{selection:?}: endpoint={chosen}, secondary EWMA={:?}",
            policy.health(1)
        );
    }
    Ok(())
}

fn demonstrate_replay_safety() -> Result<()> {
    let uncertain = Error::Grpc(Status::unavailable("connection lost after send"));
    ensure!(!FailoverPolicy::should_failover_write(true, &uncertain));
    ensure!(FailoverPolicy::should_failover_write(false, &uncertain));
    let retry_allowed = Error::ServerStatus {
        code: greptimedb::ErrorCode::RegionBusy,
        raw_code: 4003,
        message: "server explicitly permits retry".into(),
        retry_hint: true,
    };
    ensure!(FailoverPolicy::should_failover_write(true, &retry_allowed));
    // Ordinary ingestion directives do not override the server retry decision.
    let hints = WriteHints::new().with_auto_create_table(true);
    println!("write hints: {hints:?}; uncertain write without server permission is not replayed");
    Ok(())
}

#[cfg(feature = "tls")]
fn demonstrate_tls() -> Result<()> {
    // The public API names SNI override `domain` and native roots `use_system_roots`.
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let key = rcgen::KeyPair::generate()?;
    let certificate =
        rcgen::CertificateParams::new(vec!["greptime.internal".into()])?.self_signed(&key)?;
    let ca_pem = format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        STANDARD.encode(certificate.der())
    );
    let key_pem = format!(
        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
        STANDARD.encode(key.serialize_der())
    );
    let options = greptimedb::TlsOptions {
        domain: Some("greptime.internal".into()),
        use_system_roots: false,
        ..greptimedb::TlsOptions::default()
    }
    .with_ca_pem(ca_pem.as_bytes().to_vec())
    .with_client_identity_pem(ca_pem.as_bytes().to_vec(), key_pem.as_bytes().to_vec());
    let debug = format!("{options:?}");
    ensure!(!debug.contains("PRIVATE KEY") && !debug.contains("CERTIFICATE"));
    println!("TLS configuration (identity bytes redacted): {debug}");
    let _config = options.build_client_config()?;
    let invalid = greptimedb::TlsOptions {
        use_system_roots: false,
        ..greptimedb::TlsOptions::default()
    }
    .with_ca_pem(b"invalid CA".to_vec());
    ensure!(
        invalid.build_client_config().is_err(),
        "invalid trust material must fail closed"
    );
    Ok(())
}

fn demonstrate_recovery() -> Result<()> {
    let primary = Peer::start(true)?;
    let secondary = Peer::start(false)?;
    let endpoints = vec![primary.endpoint.clone(), secondary.endpoint.clone()];
    let events = Rc::new(RefCell::new(Vec::<FailoverEvent>::new()));
    let observed = Rc::clone(&events);
    let policy = FailoverPolicy::new(endpoints.iter().map(Endpoint::authority))
        .with_selection(EndpointSelection::RoundRobin)
        .with_circuit_breaker(1, COOLDOWN)
        .on_failover(move |event| observed.borrow_mut().push(event.clone()));
    let mut options = ClientOptions::new(endpoints);
    options.request_timeout = Duration::from_secs(2);
    let mut client = Client::connect_with_policy(options, policy)?;
    // Connect selects primary; round-robin sends the first read to secondary.
    client.prom_instant_query("up", "0", "1m")?;
    // Primary rejects its first read; a replay-safe query reaches secondary.
    client.prom_instant_query("up", "0", "1m")?;
    let policy = client.failover().context("failover policy missing")?;
    let opened = policy
        .health(0)
        .and_then(|state| state.circuit_opened_at)
        .context("primary circuit did not open")?;
    ensure!(!policy.needs_probe(0, opened));
    ensure!(policy.needs_probe(0, opened + COOLDOWN));
    // Cold-path demonstration of cooldown, not a scheduler for ingestion loops.
    timer::pause(COOLDOWN + Duration::from_millis(5)).expect("fixture timer");
    client.prom_instant_query("up", "0", "1m")?;
    ensure!(
        primary.health_hits.load(Ordering::SeqCst) >= 1,
        "half-open primary must be admitted by a real HealthCheck RPC"
    );
    ensure!(
        client
            .failover()
            .and_then(|p| p.health(0))
            .is_some_and(|s| s.healthy && s.circuit_opened_at.is_none())
    );
    ensure!(!events.borrow().is_empty());
    for event in events.borrow().iter() {
        println!(
            "failover {} -> {}: {} (attempt {})",
            event.from_endpoint, event.to_endpoint, event.reason, event.attempt
        );
    }
    println!(
        "primary recovered: queries={}, admission probes={}",
        primary.query_hits.load(Ordering::SeqCst),
        primary.health_hits.load(Ordering::SeqCst)
    );
    drop(client);
    primary.stop()?;
    secondary.stop()?;
    Ok(())
}

/// Finite loopback fixture; closing it interrupts active sockets and joins workers.
struct Peer {
    endpoint: Endpoint,
    health_hits: Arc<AtomicUsize>,
    query_hits: Arc<AtomicUsize>,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<Result<()>>>,
}

impl Peer {
    fn start(fail_first: bool) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = Endpoint::parse(&format!("grpc://{}", listener.local_addr()?))?;
        let stopping = Arc::new(AtomicBool::new(false));
        let health_hits = Arc::new(AtomicUsize::new(0));
        let query_hits = Arc::new(AtomicUsize::new(0));
        let stop = Arc::clone(&stopping);
        let probes = Arc::clone(&health_hits);
        let reads = Arc::clone(&query_hits);
        let worker = thread::spawn(move || {
            let mut connections = Vec::new();
            let mut sockets = Vec::new();
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        sockets.push(socket.try_clone()?);
                        let probes = Arc::clone(&probes);
                        let reads = Arc::clone(&reads);
                        connections.push(thread::spawn(move || {
                            serve(socket, fail_first, probes, reads)
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        timer::pause(Duration::from_millis(1)).expect("fixture timer")
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            for socket in sockets {
                if let Err(error) = socket.shutdown(Shutdown::Both)
                    && error.kind() != std::io::ErrorKind::NotConnected
                {
                    return Err(error.into());
                }
            }
            for connection in connections {
                connection
                    .join()
                    .map_err(|_| anyhow!("mock connection panicked"))??;
            }
            Ok(())
        });
        Ok(Self {
            endpoint,
            health_hits,
            query_hits,
            stopping,
            worker: Some(worker),
        })
    }

    fn stop(mut self) -> Result<()> {
        self.close()
    }

    fn close(&mut self) -> Result<()> {
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow!("mock listener panicked"))??;
        }
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            eprintln!("mock cleanup: {error:#}");
        }
    }
}

fn serve(
    socket: TcpStream,
    fail_first: bool,
    probes: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
) -> Result<()> {
    let mut transport = TcpTransport::from_std(socket)?;
    transport.set_read_timeout(Some(Duration::from_secs(2)))?;
    transport.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut server = SyncServer::new(transport)?;
    server.register_unary(
        HEALTH,
        ProstCodec::<v1::HealthCheckResponse, v1::HealthCheckRequest>::new(),
        move |_: Request<v1::HealthCheckRequest>| {
            probes.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(v1::HealthCheckResponse {}))
        },
    )?;
    server.register_unary(
        PROM,
        ProstCodec::<v1::PromqlResponse, v1::PromqlRequest>::new(),
        move |_: Request<v1::PromqlRequest>| {
            let count = reads.fetch_add(1, Ordering::SeqCst);
            if fail_first && count == 0 {
                return Err(Status::unavailable("primary restarting"));
            }
            Ok(Response::new(v1::PromqlResponse {
                header: None,
                body: b"{\"status\":\"success\"}".to_vec(),
            }))
        },
    )?;
    // Socket shutdown is the fixture's explicit termination signal.
    if let Err(error) = server.serve_connection() {
        eprintln!("mock connection terminated: {error}");
    }
    Ok(())
}
