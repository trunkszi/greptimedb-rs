//! Thread-affine clients with shared, lock-free-on-the-calling-thread managers.
//!
//! `Client` (the Greptime client) is `!Send + !Sync`: its synchronous HTTP/2
//! session belongs to one OS thread and has no internal client mutex. Share
//! `ThreadLocalClient` or `WorkerClient` instead; each OS thread lazily opens
//! its own connections. The actual SDK APIs are `with_client`, `lease_by_key`,
//! `with_client_by_key`, and the RAII `LeaseGuard`, rather than a shared client
//! getter or a table checkout API. Keyed slots preserve placement, not a
//! cross-thread global ordering guarantee.
//!
//! Run offline with `cargo run -p greptimedb --example
//! 08_thread_local_and_worker_striping`. Optionally set `GREPTIMEDB_ENDPOINT`
//! to health-check a live server after the local demonstration. All local RPCs
//! use a real loopback TCP `SyncServer`; no GreptimeDB installation is needed.

#[path = "../tests/support/timer.rs"]
mod timer;

use std::{
    env,
    io::ErrorKind,
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use greptimedb::{
    Client, ClientOptions, Endpoint, ThreadLocalClient, WorkerBalance, WorkerClient, WorkerConfig,
    proto::v1, threading::close_thread_caches,
};
use grpc::{
    ProstCodec, Request, Response, SyncServer,
    core::transport::{SyncTransport, TcpTransport},
};

const HEALTH: &str = "/greptime.v1.HealthCheck/HealthCheck";
const SLOTS: usize = 3;
const THREADS: usize = 3;

fn options(endpoint: &str) -> Result<ClientOptions> {
    let mut options = ClientOptions::new(vec![Endpoint::parse(endpoint)?]);
    options.connect_timeout = Duration::from_secs(3);
    options.request_timeout = Duration::from_secs(3);
    Ok(options)
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn shareable<T: Send + Sync>(_: &T) {}

fn demonstrate(endpoint: &str) -> Result<()> {
    let local = ThreadLocalClient::new(options(endpoint)?)?;
    let workers = WorkerClient::new(
        options(endpoint)?,
        WorkerConfig {
            slots_per_thread: SLOTS,
            balance: WorkerBalance::KeyHash,
            stripe_endpoints: true,
        },
    )?;
    shareable(&local);
    shareable(&workers);
    thread::scope(|scope| -> Result<()> {
        let mut tasks = Vec::new();
        for worker in 0..THREADS {
            let local = &local;
            let workers = &workers;
            tasks.push(scope.spawn(move || -> Result<()> {
                // Each OS thread reuses its own client across both calls.
                local.with_client(Client::ping)?;
                local.with_client(Client::ping)?;
                ensure!(local.close_current_thread(), "cached client must close");

                for table in ["l1_books", "trades", "funding"] {
                    let key = fnv1a(table.as_bytes());
                    // lease_by_key applies FNV-1a to the u64 key's bytes.
                    let slot = fnv1a(&key.to_le_bytes()) % u64::try_from(SLOTS)?;
                    {
                        let mut lease = workers.lease_by_key(key)?;
                        lease.ping()?;
                        ensure!(
                            workers.lease_by_key(key).is_err(),
                            "the same keyed slot cannot be leased twice"
                        );
                    } // LeaseGuard returns the client to its local slot.
                    workers.with_client_by_key(key, Client::ping)?;
                    println!("thread={worker} table={table} key={key:#x} slot={slot}");
                }
                let closed = close_thread_caches();
                ensure!(closed == 1, "one worker pool should remain, got {closed}");
                ensure!(close_thread_caches() == 0, "cleanup must be idempotent");
                Ok(())
            }));
        }
        let mut first_error = None;
        for task in tasks {
            let outcome = task.join().map_err(|_| anyhow::anyhow!("worker panicked"));
            let result = outcome.and_then(std::convert::identity);
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    })
}

fn serve(listener: TcpListener, stop: &AtomicBool, hits: &Arc<AtomicUsize>) -> Result<()> {
    thread::scope(|scope| -> Result<()> {
        let mut sessions = Vec::new();
        while !stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((socket, _)) => {
                    let hits = Arc::clone(hits);
                    sessions.push(scope.spawn(move || -> Result<()> {
                        let mut transport = TcpTransport::from_std(socket)?;
                        transport.set_read_timeout(Some(Duration::from_secs(5)))?;
                        transport.set_write_timeout(Some(Duration::from_secs(5)))?;
                        let mut server = SyncServer::new(transport)?;
                        server.register_unary(
                            HEALTH,
                            ProstCodec::<v1::HealthCheckResponse, v1::HealthCheckRequest>::new(),
                            move |_: Request<v1::HealthCheckRequest>| {
                                hits.fetch_add(1, Ordering::Relaxed);
                                Ok(Response::new(v1::HealthCheckResponse {}))
                            },
                        )?;
                        server.serve_connection()?;
                        Ok(())
                    }));
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    // Only the offline fixture's accept loop waits here.
                    timer::pause(Duration::from_millis(1)).expect("fixture timer");
                }
                Err(error) => return Err(error.into()),
            }
        }
        for session in sessions {
            session
                .join()
                .map_err(|_| anyhow::anyhow!("server session panicked"))??;
        }
        Ok(())
    })
}

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("grpc://{}", listener.local_addr()?);
    listener.set_nonblocking(true)?;
    let stop = Arc::new(AtomicBool::new(false));
    let hits = Arc::new(AtomicUsize::new(0));
    let server_stop = Arc::clone(&stop);
    let server_hits = Arc::clone(&hits);
    let server = thread::spawn(move || serve(listener, &server_stop, &server_hits));
    let result = demonstrate(&endpoint);
    stop.store(true, Ordering::Release);
    let server_result = server
        .join()
        .map_err(|_| anyhow::anyhow!("loopback server panicked"))?;
    result?;
    server_result?;
    let total = hits.load(Ordering::Relaxed);
    ensure!(
        total == THREADS * 8,
        "unexpected HealthCheck count: {total}"
    );
    println!("offline: {total} health RPCs, thread-local reuse and keyed lease cleanup verified");

    match env::var("GREPTIMEDB_ENDPOINT") {
        Ok(endpoint) => {
            let mut client = Client::connect(options(&endpoint)?)?;
            client.ping().context("live GreptimeDB health check")?;
            println!("live: health check succeeded at {endpoint}");
        }
        Err(env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
