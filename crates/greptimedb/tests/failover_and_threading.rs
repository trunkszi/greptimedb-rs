//! Endpoint selection, write-replay rules, and per-thread client placement.

#[path = "support/timer.rs"]
mod timer;

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use greptimedb::proto::v1;
use greptimedb::threading::{stripe_shift, striped_endpoints};
use greptimedb::{
    Client, ClientOptions, ColumnDataType, ColumnarBatch, Endpoint, EndpointSelection, Error,
    ErrorCode, FailoverPolicy, RetryableError, ThreadLocalClient, TimestampUnit, ValueRef,
    WorkerBalance, WorkerClient, WorkerConfig,
};
use grpc::core::transport::{SyncTransport, TcpTransportListener};
use grpc::{ProstCodec, Request, Response, Status, SyncServer};

const HEALTH_PATH: &str = "/greptime.v1.HealthCheck/HealthCheck";
const HANDLE_PATH: &str = "/greptime.v1.GreptimeDatabase/Handle";
const SOCKET_TIMEOUT: Duration = Duration::from_secs(5);

fn now() -> Instant {
    Instant::now()
}

fn endpoint(port: u16) -> Endpoint {
    Endpoint::parse(&format!("grpc://127.0.0.1:{port}")).expect("endpoint")
}

fn options(ports: &[u16]) -> ClientOptions {
    ClientOptions::new(ports.iter().copied().map(endpoint).collect())
}

fn server(retry_hint: bool) -> Error {
    Error::ServerStatus {
        code: ErrorCode::Unknown,
        raw_code: ErrorCode::Unknown.as_u32(),
        message: "busy".into(),
        retry_hint,
    }
}

#[test]
fn round_robin_walks_then_wraps() {
    let mut policy = FailoverPolicy::new(["a", "b", "c"]);
    let at = now();
    assert_eq!(policy.selection(), EndpointSelection::RoundRobin);
    assert_eq!(policy.max_attempts(), 3);
    assert_eq!(
        [
            policy.select_endpoint(at).expect("first"),
            policy.select_endpoint(at).expect("second"),
            policy.select_endpoint(at).expect("third"),
            policy.select_endpoint(at).expect("wrap"),
        ],
        [0, 1, 2, 0]
    );
}

#[test]
fn sticky_stays_until_the_circuit_opens() {
    let events = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::clone(&events);
    let at = now();
    let mut policy = FailoverPolicy::new(["a", "b"])
        .with_selection(EndpointSelection::Sticky)
        .with_circuit_breaker(1, Duration::from_secs(60))
        .on_failover(move |event| seen.borrow_mut().push(event.clone()));
    assert!(policy.has_observer());
    assert_eq!(policy.select_endpoint(at).expect("stick"), 0);
    assert_eq!(policy.select_endpoint(at).expect("still"), 0);
    assert!(events.borrow().is_empty());
    policy.record_failure(0, at);
    assert_eq!(policy.select_endpoint(at).expect("move"), 1);
    let event = events.borrow()[0].clone();
    assert_eq!(event.from_endpoint, "a");
    assert_eq!(event.to_endpoint, "b");
    assert_eq!(event.reason, "circuit open");
    assert!(event.attempt >= 1);
}

#[test]
fn random_seed_repeats() {
    let at = now();
    let mut left = FailoverPolicy::new(["a", "b", "c", "d"])
        .with_selection(EndpointSelection::Random)
        .with_random_seed(0);
    assert_eq!(left.config_parts().4, Some(1));
    let left_picks: Vec<_> = (0..6)
        .map(|_| left.select_endpoint(at).expect("left"))
        .collect();
    let restored = FailoverPolicy::from_config(left.config());
    assert_eq!(restored.selection(), EndpointSelection::Random);
    assert_eq!(restored.config_parts().4, Some(1));
    let mut again = FailoverPolicy::new(["a", "b", "c", "d"])
        .with_selection(EndpointSelection::Random)
        .with_random_seed(1);
    let again_picks: Vec<_> = (0..6)
        .map(|_| again.select_endpoint(at).expect("again"))
        .collect();
    assert_eq!(left_picks, again_picks);
}

#[test]
fn latency_prefers_the_smaller_sample_and_unknowns_lose() {
    let at = now();
    let mut policy = FailoverPolicy::new(["slow", "fast", "unknown"])
        .with_selection(EndpointSelection::LatencyAware);
    assert_eq!(policy.select_endpoint(at).expect("unsampled"), 0);
    policy.record_success(0, Duration::from_millis(40));
    policy.record_success(1, Duration::from_millis(2));
    assert_eq!(policy.select_endpoint(at).expect("fast"), 1);
    assert!(policy.health(1).expect("fast health").ewma_rtt_us > 0);
    assert_eq!(policy.health(2).expect("unknown").ewma_rtt_us, 0);
    assert!(policy.health(1).expect("fast health").healthy);
}

#[test]
fn circuit_skips_until_cooldown_then_success_closes_it() {
    let at = now();
    let mut policy =
        FailoverPolicy::new(["a", "b"]).with_circuit_breaker(2, Duration::from_secs(60));
    policy.record_failure(0, at);
    assert!(
        policy
            .health(0)
            .expect("health")
            .circuit_opened_at
            .is_none()
    );
    policy.record_failure(0, at);
    assert!(!policy.health(0).expect("health").healthy);
    assert_eq!(policy.select_endpoint(at).expect("other"), 1);
    let later = at + Duration::from_secs(61);
    assert_eq!(policy.select_endpoint(later).expect("half open"), 0);
    policy.record_success(0, Duration::from_millis(3));
    let health = policy.health(0).expect("closed");
    assert!(health.healthy);
    assert_eq!(health.consecutive_failures, 0);
    assert!(health.circuit_opened_at.is_none());
}

#[test]
fn disabled_breaker_and_bad_indexes_do_not_open_or_panic() {
    let at = now();
    let mut policy = FailoverPolicy::new(["only"]).with_circuit_breaker(0, Duration::from_secs(30));
    policy.record_failure(0, at);
    policy.record_failure(4, at);
    policy.record_success(4, Duration::from_millis(1));
    assert!(
        policy
            .health(0)
            .expect("health")
            .circuit_opened_at
            .is_none()
    );
    assert_eq!(policy.select_endpoint(at).expect("still open"), 0);
    assert!(policy.endpoint(0).is_some());
    assert!(policy.endpoint(3).is_none());
}

#[test]
fn empty_policy_and_zero_attempts_fail() {
    let at = now();
    let mut empty = FailoverPolicy::new(Vec::<&str>::new());
    assert!(empty.is_empty());
    assert!(matches!(
        empty.select_endpoint(at),
        Err(Error::InvalidConfig(_))
    ));
    let mut blocked = FailoverPolicy::new(["a"]).with_max_attempts(0);
    assert!(matches!(
        blocked.select_endpoint(at),
        Err(Error::AllEndpointsFailed { attempts: 0, .. })
    ));
}

#[test]
fn one_attempt_selects_an_available_endpoint_after_skipping_an_open_circuit() {
    let at = now();
    let mut policy = FailoverPolicy::new(["down", "up"])
        .with_circuit_breaker(1, Duration::from_secs(120))
        .with_max_attempts(1);
    policy.record_failure(0, at);
    assert_eq!(policy.select_endpoint(at).expect("healthy endpoint"), 1);
    assert!(matches!(
        policy.select_endpoint_excluding(at, &[1]),
        Err(Error::AllEndpointsFailed { .. })
    ));
}

#[test]
fn write_replay_requires_an_explicit_hint_after_bytes_are_sent() {
    let hinted = server(true);
    let refused = server(false);
    let transport = Error::from(std::io::Error::other("reset"));
    assert_eq!(RetryableError::retry_hint(&hinted), Some(true));
    assert_eq!(RetryableError::retry_hint(&refused), Some(false));
    assert_eq!(RetryableError::retry_hint(&transport), None);
    assert!(FailoverPolicy::should_failover_write(false, &transport));
    assert!(FailoverPolicy::should_failover_write(false, &refused));
    assert!(FailoverPolicy::should_failover_write(false, &hinted));
    assert!(FailoverPolicy::should_failover_write(true, &hinted));
    assert!(!FailoverPolicy::should_failover_write(true, &refused));
    assert!(!FailoverPolicy::should_failover_write(true, &transport));
}

#[test]
fn worker_defaults_and_stripe_formula() {
    let config = WorkerConfig::default();
    assert_eq!(config.slots_per_thread, 4);
    assert_eq!(config.balance, WorkerBalance::RoundRobin);
    assert!(config.stripe_endpoints);
    assert_eq!(
        WorkerConfig {
            slots_per_thread: 0,
            balance: config.balance,
            stripe_endpoints: config.stripe_endpoints,
        }
        .slot_count(),
        1
    );
    assert_eq!(stripe_shift(3, 0, 1), 1);
    assert_eq!(stripe_shift(3, 1, 1), 2);
    assert_eq!(
        striped_endpoints(&["a".into(), "b".into(), "c".into()], 0, 1, true),
        vec!["b".to_owned(), "c".to_owned(), "a".to_owned()]
    );
    assert_eq!(
        striped_endpoints(&["a".into(), "b".into()], 5, 1, false),
        vec!["a".to_owned(), "b".to_owned()]
    );
    let worker =
        WorkerClient::new(options(&[4001, 4002, 4003]), WorkerConfig::default()).expect("worker");
    let preview = worker.preview_endpoints(0, 1);
    assert_eq!(preview[0], endpoint(4002).socket_addr_str());
    assert_eq!(preview[1], endpoint(4003).socket_addr_str());
}

#[test]
fn shared_factories_reject_observers_and_closed_ports() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ThreadLocalClient>();
    assert_send_sync::<WorkerClient>();

    let watched = FailoverPolicy::new(["grpc://127.0.0.1:1"]).on_failover(|_| {});
    assert!(ThreadLocalClient::with_policy(options(&[1]), watched).is_err());
    let watched = FailoverPolicy::new(["grpc://127.0.0.1:1"]).on_failover(|_| {});
    assert!(WorkerClient::with_policy(options(&[1]), WorkerConfig::default(), watched).is_err());

    let local = ThreadLocalClient::new(options(&[1])).expect("factory");
    assert!(local.failover_config().is_none());
    assert!(matches!(
        local.with_client(|_| Ok(())),
        Err(Error::Transport(_))
    ));
    assert!(matches!(
        local.with_client(|_| Ok(())),
        Err(Error::Transport(_))
    ));

    let worker = WorkerClient::new(options(&[1]), WorkerConfig::default()).expect("worker");
    assert!(matches!(worker.lease(), Err(Error::Transport(_))));
    assert!(matches!(worker.lease_by_key(42), Err(Error::Transport(_))));
    let worker = Arc::new(worker);
    let moved = Arc::clone(&worker);
    thread::spawn(move || {
        assert!(matches!(
            moved.with_client(|_| Ok(())),
            Err(Error::Transport(_))
        ));
    })
    .join()
    .expect("thread");
}

#[derive(Clone, Copy)]
enum HealthReply {
    Healthy,
    Unavailable,
}

#[derive(Clone, Copy)]
enum HandleReply {
    Affected,
    Reject,
}

struct MockServer {
    addr: SocketAddr,
    health_hits: Arc<AtomicUsize>,
    handle_hits: Arc<AtomicUsize>,
}

impl MockServer {
    fn endpoint(&self) -> Endpoint {
        Endpoint::parse(&format!("grpc://{}", self.addr)).expect("mock endpoint")
    }

    fn health_hits(&self) -> usize {
        self.health_hits.load(Ordering::SeqCst)
    }

    fn handle_hits(&self) -> usize {
        self.handle_hits.load(Ordering::SeqCst)
    }
}

fn affected_rows(value: u32) -> v1::GreptimeResponse {
    v1::GreptimeResponse {
        header: Some(v1::ResponseHeader {
            status: Some(v1::Status {
                status_code: 0,
                err_msg: String::new(),
                retry_hint: String::new(),
            }),
        }),
        response: Some(v1::greptime_response::Response::AffectedRows(
            v1::AffectedRows { value },
        )),
    }
}

/// Serves unlimited connections; each accepted socket runs its own server thread.
fn spawn_mock_server(health: HealthReply, handle: HandleReply) -> MockServer {
    let listener = TcpTransportListener::bind("127.0.0.1:0").expect("bind mock listener");
    let addr = listener.local_addr().expect("mock listener address");
    let health_hits = Arc::new(AtomicUsize::new(0));
    let handle_hits = Arc::new(AtomicUsize::new(0));
    let health_total = Arc::clone(&health_hits);
    let handle_total = Arc::clone(&handle_hits);
    thread::spawn(move || {
        loop {
            let Ok((mut transport, _)) = listener.accept() else {
                return;
            };
            let health_total = Arc::clone(&health_total);
            let handle_total = Arc::clone(&handle_total);
            thread::spawn(move || {
                transport
                    .set_read_timeout(Some(SOCKET_TIMEOUT))
                    .expect("mock read timeout");
                transport
                    .set_write_timeout(Some(SOCKET_TIMEOUT))
                    .expect("mock write timeout");
                let Ok(mut server) = SyncServer::new(transport) else {
                    return;
                };
                let health_handler = move |_request: Request<v1::HealthCheckRequest>| {
                    health_total.fetch_add(1, Ordering::SeqCst);
                    match health {
                        HealthReply::Healthy => Ok(Response::new(v1::HealthCheckResponse {})),
                        HealthReply::Unavailable => Err(Status::unavailable("health unavailable")),
                    }
                };
                server
                    .register_unary(
                        HEALTH_PATH,
                        ProstCodec::<v1::HealthCheckResponse, v1::HealthCheckRequest>::new(),
                        health_handler,
                    )
                    .expect("register health route");
                let handle_handler = move |_request: Request<v1::GreptimeRequest>| {
                    handle_total.fetch_add(1, Ordering::SeqCst);
                    match handle {
                        HandleReply::Affected => Ok(Response::new(affected_rows(1))),
                        HandleReply::Reject => Err(Status::internal("reject")),
                    }
                };
                server
                    .register_unary(
                        HANDLE_PATH,
                        ProstCodec::<v1::GreptimeResponse, v1::GreptimeRequest>::new(),
                        handle_handler,
                    )
                    .expect("register handle route");
                server.serve_connection().expect("mock server connection");
            });
        }
    });
    MockServer {
        addr,
        health_hits,
        handle_hits,
    }
}

fn batch() -> ColumnarBatch {
    let mut batch = ColumnarBatch::new("cpu_ticks");
    batch
        .add_tag_column("host", ColumnDataType::String)
        .expect("tag column");
    batch
        .add_timestamp_column("ts", TimestampUnit::Millisecond)
        .expect("timestamp column");
    batch
        .add_field_column("value", ColumnDataType::Float64)
        .expect("field column");
    batch
        .push_row(&[
            ValueRef::String("a"),
            ValueRef::TimestampMillisecond(1_700_000_000_000),
            ValueRef::Float64(1.5),
        ])
        .expect("row");
    batch
}

#[test]
fn client_round_robin_rotates_three_live_endpoints() {
    let servers = [
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
    ];
    let options = ClientOptions::new(servers.iter().map(MockServer::endpoint).collect())
        .with_failover(FailoverPolicy::new(["a", "b", "c"]).config());
    let mut client = Client::connect(options).expect("connect");
    for _ in 0..6 {
        client.ping().expect("ping");
    }
    for server in &servers {
        assert_eq!(server.health_hits(), 2);
        assert_eq!(server.handle_hits(), 0);
    }
}

#[test]
fn client_skips_open_circuit_and_probes_after_cooldown() {
    let healthy = spawn_mock_server(HealthReply::Healthy, HandleReply::Affected);
    let unhealthy = spawn_mock_server(HealthReply::Unavailable, HandleReply::Affected);
    let policy = FailoverPolicy::new(["healthy", "unhealthy"])
        .with_max_attempts(2)
        .with_circuit_breaker(1, Duration::from_millis(50));
    let options = ClientOptions::new(vec![healthy.endpoint(), unhealthy.endpoint()])
        .with_failover(policy.config());
    let mut client = Client::connect(options).expect("connect");

    client.ping().expect("first ping fails over");
    assert_eq!(unhealthy.health_hits(), 1);
    assert!(
        client
            .failover()
            .expect("policy")
            .health(1)
            .expect("unhealthy state")
            .circuit_opened_at
            .is_some()
    );

    client.ping().expect("second ping avoids the open circuit");
    assert_eq!(unhealthy.health_hits(), 1);

    timer::pause(Duration::from_millis(70)).expect("fixture timer");
    client
        .ping()
        .expect("third ping probes the half-open circuit");
    assert_eq!(unhealthy.health_hits(), 2);
    assert_eq!(healthy.health_hits(), 3);
}

#[test]
fn client_does_not_replay_a_sent_mutation_without_a_retry_hint() {
    let accepting = spawn_mock_server(HealthReply::Healthy, HandleReply::Affected);
    let rejecting = spawn_mock_server(HealthReply::Healthy, HandleReply::Reject);
    let policy = FailoverPolicy::new(["accepting", "rejecting"])
        .with_max_attempts(2)
        .with_circuit_breaker(10, Duration::from_secs(10));
    let options = ClientOptions::new(vec![accepting.endpoint(), rejecting.endpoint()])
        .with_failover(policy.config());
    let mut client = Client::connect(options).expect("connect");

    let error = client
        .insert_batch(&batch())
        .expect_err("the rejecting endpoint must fail the mutation");
    assert_eq!(accepting.handle_hits(), 0);
    assert_eq!(rejecting.handle_hits(), 1);
    match error {
        Error::Grpc(status) => assert_eq!(status.code(), grpc::core::Code::Internal),
        other => panic!("expected a gRPC Internal error, got {other:?}"),
    }
}

#[test]
fn eight_threads_ingest_through_thread_local_clients() {
    let servers = [
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
    ];
    let shared = Arc::new(
        ThreadLocalClient::new(ClientOptions::new(
            servers.iter().map(MockServer::endpoint).collect(),
        ))
        .expect("thread-local factory"),
    );
    let inserted = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let shared = Arc::clone(&shared);
        let inserted = Arc::clone(&inserted);
        workers.push(thread::spawn(move || {
            let rows = shared
                .with_client(|client| client.insert_batch(&batch()))
                .expect("thread-local insert");
            inserted.fetch_add(usize::try_from(rows).expect("row count"), Ordering::SeqCst);
        }));
    }
    for worker in workers {
        worker.join().expect("worker thread");
    }
    assert_eq!(inserted.load(Ordering::SeqCst), 8);
    assert_eq!(servers[0].handle_hits(), 8);
    assert_eq!(servers[1].handle_hits(), 0);
    assert_eq!(servers[2].handle_hits(), 0);
}

#[test]
fn eight_threads_ingest_through_striped_worker_clients() {
    let servers = [
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
        spawn_mock_server(HealthReply::Healthy, HandleReply::Affected),
    ];
    let worker = Arc::new(
        WorkerClient::new(
            ClientOptions::new(servers.iter().map(MockServer::endpoint).collect()),
            WorkerConfig {
                slots_per_thread: 1,
                balance: WorkerBalance::RoundRobin,
                stripe_endpoints: true,
            },
        )
        .expect("worker factory"),
    );
    let barrier = Arc::new(Barrier::new(8));
    let inserted = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let worker = Arc::clone(&worker);
        let barrier = Arc::clone(&barrier);
        let inserted = Arc::clone(&inserted);
        threads.push(thread::spawn(move || {
            barrier.wait();
            let rows = worker
                .with_client(|client| client.insert_batch(&batch()))
                .expect("striped insert");
            inserted.fetch_add(usize::try_from(rows).expect("row count"), Ordering::SeqCst);
        }));
    }
    for thread in threads {
        thread.join().expect("worker thread");
    }
    assert_eq!(inserted.load(Ordering::SeqCst), 8);
    let mut counts = servers.map(|server| server.handle_hits());
    counts.sort_unstable();
    assert_eq!(counts, [2, 3, 3]);
}
