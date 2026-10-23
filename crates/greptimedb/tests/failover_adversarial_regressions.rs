//! Deterministic failover admission, replay, connection lifetime, and TLS regressions.

use std::{
    net::{Shutdown, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use greptimedb::{
    Client, ClientOptions, ColumnDataType, ColumnarBatch, Endpoint, EndpointSelection,
    FailoverPolicy, ThreadLocalClient, TimestampUnit, ValueRef, WorkerClient, WorkerConfig,
    proto::v1,
};
use grpc::core::transport::{SyncTransport, TcpTransport};
use grpc::{Metadata, ProstCodec, Request, Response, Status, SyncServer};

#[path = "support/events.rs"]
mod events;
#[path = "support/signal.rs"]
mod signal;
use events::Events;

const HEALTH: &str = "/greptime.v1.HealthCheck/HealthCheck";
const HANDLE: &str = "/greptime.v1.GreptimeDatabase/Handle";
const STREAM: &str = "/greptime.v1.GreptimeDatabase/HandleRequests";
const PROM: &str = "/greptime.v1.PrometheusGateway/Handle";
const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum Reply {
    Success,
    RetryHeader,
    NoRetryHeader,
    MissingRetryHeader,
    StreamUnavailable,
    TrailerError,
}

struct Peer {
    endpoint: Endpoint,
    health_ok: Arc<AtomicBool>,
    events: Arc<Events>,
}

impl Peer {
    fn start(reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let endpoint = Endpoint::parse(&format!("grpc://{}", listener.local_addr().expect("addr")))
            .expect("endpoint");
        let health_ok = Arc::new(AtomicBool::new(true));
        let events = Arc::new(Events::new());
        let observed = Arc::clone(&events);
        let healthy = Arc::clone(&health_ok);
        thread::spawn(move || {
            let (socket, _) = listener.accept().expect("accept");
            serve(socket, reply, healthy, observed);
        });
        Self {
            endpoint,
            health_ok,
            events,
        }
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.snapshot()
    }
}

fn header(reply: Reply) -> Option<v1::ResponseHeader> {
    Some(v1::ResponseHeader {
        status: Some(v1::Status {
            status_code: if matches!(
                reply,
                Reply::RetryHeader | Reply::NoRetryHeader | Reply::MissingRetryHeader
            ) {
                4003
            } else {
                0
            },
            err_msg: "region busy".into(),
            retry_hint: match reply {
                Reply::NoRetryHeader => "false",
                Reply::MissingRetryHeader => "",
                _ => "true",
            }
            .into(),
        }),
    })
}

fn affected(reply: Reply) -> v1::GreptimeResponse {
    v1::GreptimeResponse {
        header: header(reply),
        response: Some(v1::greptime_response::Response::AffectedRows(
            v1::AffectedRows { value: 1 },
        )),
    }
}

fn serve(socket: std::net::TcpStream, reply: Reply, healthy: Arc<AtomicBool>, events: Arc<Events>) {
    let mut transport = TcpTransport::from_std(socket).expect("transport");
    transport
        .set_read_timeout(Some(TIMEOUT))
        .expect("read timeout");
    transport
        .set_write_timeout(Some(TIMEOUT))
        .expect("write timeout");
    let mut server = SyncServer::new(transport).expect("server");
    let observed = Arc::clone(&events);
    server
        .register_unary(
            HEALTH,
            ProstCodec::<v1::HealthCheckResponse, v1::HealthCheckRequest>::new(),
            move |_: Request<v1::HealthCheckRequest>| {
                observed.push("health");
                if healthy.load(Ordering::SeqCst) {
                    Ok(Response::new(v1::HealthCheckResponse {}))
                } else {
                    Err(Status::unavailable("unhealthy admission probe"))
                }
            },
        )
        .expect("health route");
    let observed = Arc::clone(&events);
    server
        .register_bidi_streaming(
            HANDLE,
            ProstCodec::<v1::GreptimeResponse, v1::GreptimeRequest>::new(),
            move |mut incoming, sink| {
                while incoming.recv_next().map_err(stream_error)?.is_some() {}
                observed.push("handle");
                sink.send(&affected(reply)).map_err(stream_error)?;
                let mut trailers = Metadata::new();
                if matches!(reply, Reply::TrailerError) {
                    trailers
                        .append_ascii("x-greptime-err-code", "4003")
                        .map_err(|error| Status::internal(error.to_string()))?;
                }
                sink.finish(Status::ok(""), trailers).map_err(stream_error)
            },
        )
        .expect("handle route");
    let observed = Arc::clone(&events);
    server
        .register_unary(
            PROM,
            ProstCodec::<v1::PromqlResponse, v1::PromqlRequest>::new(),
            move |_: Request<v1::PromqlRequest>| {
                observed.push("prom");
                Ok(Response::new(v1::PromqlResponse {
                    header: header(reply),
                    body: b"{}".to_vec(),
                }))
            },
        )
        .expect("prom route");
    server
        .register_bidi_streaming(
            STREAM,
            ProstCodec::<v1::GreptimeResponse, v1::GreptimeRequest>::new(),
            move |mut incoming, sink| {
                events.push("stream");
                while incoming.recv_next().map_err(stream_error)?.is_some() {}
                if matches!(reply, Reply::StreamUnavailable) {
                    return sink
                        .finish(Status::unavailable("stream failed"), Metadata::new())
                        .map_err(stream_error);
                }
                sink.send(&affected(Reply::Success)).map_err(stream_error)
            },
        )
        .expect("stream route");
    drop(server.serve_connection());
}

fn stream_error(error: grpc::stream::StreamError) -> Status {
    match error {
        grpc::stream::StreamError::Status(status) => status,
        grpc::stream::StreamError::WouldBlock => Status::internal("blocking stream stalled"),
    }
}

fn options(endpoints: Vec<Endpoint>) -> ClientOptions {
    let mut options = ClientOptions::new(endpoints);
    options.request_timeout = TIMEOUT;
    options
}

fn batch() -> ColumnarBatch {
    let mut batch = ColumnarBatch::new("ticks");
    batch
        .add_timestamp_column("ts", TimestampUnit::Millisecond)
        .expect("timestamp");
    batch
        .add_field_column("value", ColumnDataType::Int64)
        .expect("field");
    batch
        .push_row(&[ValueRef::TimestampMillisecond(1), ValueRef::Int64(2)])
        .expect("row");
    batch
}

#[test]
fn dead_single_endpoint_channel_is_evicted_before_the_next_operation() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let endpoint = Endpoint::parse(&format!("grpc://{}", listener.local_addr().expect("addr")))
        .expect("endpoint");
    let (closed_tx, closed_rx) = signal::channel();
    let server = thread::spawn(move || {
        let (first, _) = listener.accept().expect("first accept");
        first.shutdown(Shutdown::Both).expect("shutdown");
        drop(first);
        closed_tx.send(1).expect("closed signal");
        let (fresh, _) = listener.accept().expect("fresh accept");
        serve(
            fresh,
            Reply::Success,
            Arc::new(AtomicBool::new(true)),
            Arc::new(Events::new()),
        );
    });
    let mut client = Client::connect(options(vec![endpoint])).expect("connect");
    closed_rx
        .recv_timeout(TIMEOUT)
        .expect("peer closed before call");
    assert!(client.ping().is_err(), "the closed connection must fail");
    client
        .ping()
        .expect("the next call must establish a fresh connection");
    drop(client);
    server.join().expect("server thread");
}

#[test]
fn sticky_and_latency_connect_fallback_exclude_the_failed_candidate() {
    for selection in [EndpointSelection::Sticky, EndpointSelection::LatencyAware] {
        let refused = TcpListener::bind("127.0.0.1:0").expect("reserve");
        let down = Endpoint::parse(&format!("grpc://{}", refused.local_addr().expect("addr")))
            .expect("endpoint");
        drop(refused);
        let up = Peer::start(Reply::Success);
        let policy = FailoverPolicy::new(["down", "up"])
            .with_selection(selection)
            .with_circuit_breaker(3, Duration::from_secs(60));
        let mut client =
            Client::connect_with_policy(options(vec![down, up.endpoint.clone()]), policy)
                .expect("the first connect operation must rotate before threshold 3");
        assert_eq!(
            client
                .failover()
                .expect("policy")
                .health(0)
                .expect("down")
                .consecutive_failures,
            1
        );
        client
            .insert_batch(&batch())
            .expect("first business RPC succeeds on peer");
        assert_eq!(up.events(), ["handle"]);
    }
}

#[test]
fn half_open_admission_probes_before_insert_and_failed_probe_sends_no_mutation() {
    for healthy in [true, false] {
        let a = Peer::start(Reply::Success);
        let b = Peer::start(Reply::Success);
        b.health_ok.store(healthy, Ordering::SeqCst);
        let policy = FailoverPolicy::new(["a", "b"]).with_circuit_breaker(1, Duration::ZERO);
        let mut client = Client::connect_with_policy(
            options(vec![a.endpoint.clone(), b.endpoint.clone()]),
            policy,
        )
        .expect("connect a");
        client
            .failover_mut()
            .expect("policy")
            .record_failure(1, Instant::now());
        client.insert_batch(&batch()).expect("insert or fallback");
        if healthy {
            assert_eq!(b.events(), ["health", "handle"]);
            assert!(a.events().is_empty());
        } else {
            assert_eq!(b.events(), ["health"]);
            assert_eq!(a.events(), ["handle"]);
            assert!(
                client
                    .failover()
                    .expect("policy")
                    .health(1)
                    .expect("b")
                    .circuit_opened_at
                    .is_some()
            );
        }
    }
}

#[test]
fn explicit_header_retry_hint_retries_ddl_insert_and_promql_inside_unary() {
    for operation in 0..3 {
        let healthy = Peer::start(Reply::Success);
        let busy = Peer::start(Reply::RetryHeader);
        let policy = FailoverPolicy::new(["healthy", "busy"]);
        let mut client = Client::connect_with_policy(
            options(vec![healthy.endpoint.clone(), busy.endpoint.clone()]),
            policy,
        )
        .expect("connect");
        match operation {
            0 => {
                assert_eq!(client.drop_table("ticks", true).expect("DDL retries"), 1);
            }
            1 => {
                assert_eq!(client.insert_batch(&batch()).expect("insert retries"), 1);
            }
            _ => {
                assert_eq!(
                    client
                        .prom_instant_query("up", "0", "5m")
                        .expect("PromQL retries")
                        .body,
                    b"{}"
                );
            }
        }
        let expected = if operation == 2 { "prom" } else { "handle" };
        assert_eq!(busy.events(), [expected]);
        assert_eq!(healthy.events(), [expected]);
        assert_eq!(
            client
                .failover()
                .expect("policy")
                .health(1)
                .expect("busy")
                .ewma_rtt_us,
            0,
            "an application-rejected RPC is not a successful latency sample"
        );
        assert_eq!(
            client
                .failover()
                .expect("policy")
                .health(1)
                .expect("busy")
                .consecutive_failures,
            0,
            "an application rejection does not poison transport health"
        );
    }
}

#[test]
fn ok_grpc_trailers_cannot_hide_an_application_error() {
    let peer = Peer::start(Reply::TrailerError);
    let mut client = Client::connect(options(vec![peer.endpoint])).expect("connect");
    assert!(matches!(
        client.insert_batch(&batch()),
        Err(greptimedb::Error::ServerStatus { raw_code: 4003, .. })
    ));
}

#[test]
fn stream_skips_open_circuit_and_reports_completion_without_unary_latency() {
    let a = Peer::start(Reply::Success);
    let b = Peer::start(Reply::Success);
    let policy = FailoverPolicy::new(["a", "b"]).with_circuit_breaker(1, Duration::from_secs(60));
    let mut client = Client::connect_with_policy(
        options(vec![a.endpoint.clone(), b.endpoint.clone()]),
        policy,
    )
    .expect("connect");
    client
        .failover_mut()
        .expect("policy")
        .record_failure(0, Instant::now());
    let mut stream = client.stream_inserter().expect("eligible stream endpoint");
    stream.send_batch(&batch()).expect("send");
    assert_eq!(stream.finish().expect("finish"), 1);
    let state = client.failover_mut().expect("policy").health(1).expect("b");
    assert!(state.healthy);
    assert_eq!(state.consecutive_failures, 0);
    assert_eq!(state.ewma_rtt_us, 0);
    assert_eq!(b.events(), ["stream"]);
    assert!(a.events().is_empty());
}

#[test]
fn terminal_stream_failure_is_recorded_once_and_circuit_is_skipped() {
    let a = Peer::start(Reply::Success);
    let b = Peer::start(Reply::StreamUnavailable);
    let policy = FailoverPolicy::new(["a", "b"]).with_circuit_breaker(1, Duration::from_secs(60));
    let mut client = Client::connect_with_policy(
        options(vec![a.endpoint.clone(), b.endpoint.clone()]),
        policy,
    )
    .expect("connect");
    let mut stream = client.stream_inserter().expect("stream b");
    stream.send_batch(&batch()).expect("send");
    assert!(stream.finish().is_err());
    let state = client.failover_mut().expect("policy").health(1).expect("b");
    assert_eq!(state.consecutive_failures, 1);
    assert!(state.circuit_opened_at.is_some());
    client
        .insert_batch(&batch())
        .expect("next call uses healthy peer");
    assert_eq!(b.events(), ["stream"]);
    assert_eq!(a.events(), ["handle"]);
}

#[test]
fn abandoned_thread_local_factory_entries_are_pruned_on_persistent_threads() {
    let peer = Peer::start(Reply::Success);
    let old = Arc::new(ThreadLocalClient::new(options(vec![peer.endpoint])).expect("old factory"));
    let (ready_tx, ready_rx) = signal::channel();
    let (continue_tx, continue_rx) = signal::channel();
    let fresh_peer = Peer::start(Reply::Success);
    let copy = Arc::clone(&old);
    let worker = thread::spawn(move || {
        copy.with_client(|client| client.ping())
            .expect("cache old client");
        drop(copy);
        ready_tx.send(1).expect("ready");
        continue_rx.recv_timeout(TIMEOUT).expect("continue");
        let fresh =
            ThreadLocalClient::new(options(vec![fresh_peer.endpoint])).expect("fresh factory");
        fresh
            .with_client(|client| client.ping())
            .expect("cache fresh client");
        assert_eq!(
            greptimedb::threading::close_thread_caches(),
            1,
            "only the new connected entry remains after pruning the abandoned connected entry"
        );
    });
    ready_rx.recv_timeout(TIMEOUT).expect("old cached");
    drop(old);
    continue_tx.send(1).expect("continue");
    worker.join().expect("persistent thread");
}

#[test]
fn abandoned_worker_factory_entries_are_pruned_on_persistent_threads() {
    let peer = Peer::start(Reply::Success);
    let old = Arc::new(
        WorkerClient::new(options(vec![peer.endpoint]), WorkerConfig::default())
            .expect("old worker"),
    );
    let (ready_tx, ready_rx) = signal::channel();
    let (continue_tx, continue_rx) = signal::channel();
    let fresh_peer = Peer::start(Reply::Success);
    let copy = Arc::clone(&old);
    let worker = thread::spawn(move || {
        copy.with_client(|client| client.ping())
            .expect("cache old worker");
        drop(copy);
        ready_tx.send(1).expect("ready");
        continue_rx.recv_timeout(TIMEOUT).expect("continue");
        let fresh = WorkerClient::new(options(vec![fresh_peer.endpoint]), WorkerConfig::default())
            .expect("fresh factory");
        fresh
            .with_client(|client| client.ping())
            .expect("cache fresh worker");
        assert_eq!(
            greptimedb::threading::close_thread_caches(),
            1,
            "only the new connected pool remains after pruning the abandoned connected pool"
        );
    });
    ready_rx.recv_timeout(TIMEOUT).expect("old cached");
    drop(old);
    continue_tx.send(1).expect("continue");
    worker.join().expect("persistent thread");
}

#[test]
fn sticky_and_latency_unary_retries_exclude_a_failed_but_not_open_endpoint() {
    for selection in [EndpointSelection::Sticky, EndpointSelection::LatencyAware] {
        let a = Peer::start(Reply::Success);
        let b = Peer::start(Reply::Success);
        a.health_ok.store(false, Ordering::SeqCst);
        let policy = FailoverPolicy::new(["a", "b"])
            .with_selection(selection)
            .with_circuit_breaker(3, Duration::from_secs(60));
        let mut client = Client::connect_with_policy(
            options(vec![a.endpoint.clone(), b.endpoint.clone()]),
            policy,
        )
        .expect("connect a");
        if selection == EndpointSelection::LatencyAware {
            client
                .failover_mut()
                .expect("policy")
                .record_success(0, Duration::from_millis(1));
        }
        client
            .ping()
            .expect("the same operation immediately rotates to b");
        assert_eq!(a.events(), ["health"]);
        assert_eq!(b.events(), ["health"]);
        let failed = client.failover().expect("policy").health(0).expect("a");
        assert_eq!(failed.consecutive_failures, 1);
        assert!(failed.circuit_opened_at.is_none());
    }
}

#[test]
fn absent_or_false_application_retry_hints_never_replay_a_mutation() {
    for reply in [Reply::NoRetryHeader, Reply::MissingRetryHeader] {
        let healthy = Peer::start(Reply::Success);
        let busy = Peer::start(reply);
        let mut client = Client::connect_with_policy(
            options(vec![healthy.endpoint.clone(), busy.endpoint.clone()]),
            FailoverPolicy::new(["healthy", "busy"]),
        )
        .expect("connect");
        assert!(matches!(
            client.insert_batch(&batch()),
            Err(greptimedb::Error::ServerStatus { raw_code: 4003, .. })
        ));
        assert_eq!(busy.events(), ["handle"]);
        assert!(healthy.events().is_empty());
    }
}

#[test]
fn exhausted_selection_preserves_the_actual_rpc_error() {
    let peer = Peer::start(Reply::Success);
    peer.health_ok.store(false, Ordering::SeqCst);
    let policy = FailoverPolicy::new(["only"])
        .with_max_attempts(3)
        .with_circuit_breaker(1, Duration::from_secs(60));
    let mut client =
        Client::connect_with_policy(options(vec![peer.endpoint]), policy).expect("connect");
    match client.ping().expect_err("unhealthy") {
        greptimedb::Error::Grpc(status) => {
            assert_eq!(status.code(), grpc::core::Code::Unavailable);
            assert_eq!(status.message(), "unhealthy admission probe");
        }
        other => panic!("last real RPC error must survive selection exhaustion: {other:?}"),
    }
}

#[test]
fn explicit_policy_overrides_a_stale_options_policy_endpoint_count() {
    let peer = Peer::start(Reply::Success);
    let mut opts = options(vec![peer.endpoint.clone()]);
    opts.failover = Some(FailoverPolicy::new(["obsolete-a", "obsolete-b"]).config());
    let mut client = Client::connect_with_policy(opts, FailoverPolicy::new(["placeholder"]))
        .expect("explicit policy wins before validation");
    client.ping().expect("valid explicit policy");
    assert_eq!(
        client.failover().expect("policy").endpoint(0),
        Some(format!("grpc://{}", peer.endpoint.socket_addr_str()).as_str())
    );
}

#[test]
fn panicking_thread_local_callback_and_worker_lease_restore_cached_clients() {
    let local_peer = Peer::start(Reply::Success);
    let local =
        ThreadLocalClient::new(options(vec![local_peer.endpoint.clone()])).expect("factory");
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(local.with_client::<_, ()>(|client| {
            client.ping()?;
            panic!("user callback");
        }));
    }));
    assert!(panic.is_err());
    local
        .with_client(|client| client.ping())
        .expect("same cached connection survives callback panic");
    assert_eq!(local_peer.events(), ["health", "health"]);
    assert!(local.close_current_thread());
    let worker_peer = Peer::start(Reply::Success);
    let worker = WorkerClient::new(
        options(vec![worker_peer.endpoint.clone()]),
        WorkerConfig {
            slots_per_thread: 1,
            ..WorkerConfig::default()
        },
    )
    .expect("worker");
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut lease = worker.lease().expect("lease");
        lease.ping().expect("ping");
        panic!("user holding lease");
    }));
    assert!(panic.is_err());
    worker
        .with_client(|client| client.ping())
        .expect("lease slot returned during unwind");
    assert_eq!(worker_peer.events(), ["health", "health"]);
    assert!(worker.close_current_thread());
}
