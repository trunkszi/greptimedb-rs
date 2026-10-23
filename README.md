# greptimedb-rs

`greptimedb-rs` is a synchronous, completion-driven, thread-affine Rust SDK for
GreptimeDB built on [grpc-rs](https://github.com/trunkszi/grpc-rs). It provides
caller-driven unary RPCs, client-streaming ingestion, Arrow Flight `DoPut` bulk
writes and `DoGet` queries, `#[derive(Row)]` typed columnar batches and borrowed
views, multi-endpoint failover, and application-thread client pools.

Clients, channels, streams, batches, and stateful failover policies are confined
to their creating OS thread (`!Send` and `!Sync`). The SDK spawns no internal
worker threads, background executors, or client mutexes; multi-threaded
applications scale across OS threads with `ThreadLocalClient` and
`WorkerClient`. The dependency graph excludes executor runtimes, alternate HTTP stacks,
and platform TLS backends.

## Why greptimedb-rs?

A time-series collector often already owns one worker's timestamp, tag and value
buffers. Its next steps are concrete: append rows, send a columnar batch, observe
the ingestion acknowledgement, then reuse storage. Arrow Flight adds a second
streaming lifecycle: `DoPut` writes must finish before their outcome is known,
and `DoGet` queries must drain record batches under backpressure. Keeping those
lifetimes on the collector's thread makes buffer ownership and retry decisions
visible at the point where data moves.

The [official GreptimeDB Rust ingester](https://github.com/GreptimeTeam/greptimedb-ingester-rust/blob/main/Cargo.toml)
uses tonic and tokio, and the [standard Rust Flight bindings](https://arrow.apache.org/rust/arrow_flight/index.html)
are generated with tonic. For a synchronous collector, adapting these APIs can
mean runtime scheduling variability, tail-latency spikes, and bridges between
local processing and spawned tasks. `Send + 'static` bounds from task spawning
can propagate into buffers and application state. Stepping through a suspended
Flight stream also means following an asynchronous state machine; AI-generated
changes across `.await` points can mishandle a batch's lifetime, backpressure or
its final acknowledgement.

`greptimedb-rs` uses synchronous grpc-rs with caller-owned cares-rs and
liburing-rs Proactors. DNS resolution, TCP/Unix connection attempts and TLS
handshaking share one absolute connection deadline, so a slow resolver or a
peer sending a few handshake bytes at a time cannot renew the budget. Application
read/write timeouts start after connection setup. This gives the worker one
explicit timeout contract for admitting a connection, without a Rust executor
choosing when another task resumes it.

`#[derive(Row)]` turns the collector's schema into typed columnar batches and
borrowed views. `StreamInserter`, `BulkStreamWriter` and `QueryStream` expose
write acknowledgement and Flight progress through synchronous calls on their
owner thread. Applications scale with `ThreadLocalClient` or `WorkerClient`:
each worker drives its own clients and reuses its own batches. The goal is a
traceable path from a sampled row to a confirmed write or decoded query batch.

## Caller-thread transport architecture

```text
Application / typed rows / dynamic batches
                  |
        greptimedb Client and streams
                  |
        grpc-rs synchronous HTTP/2 channel
                  |
     TCP / TLS / Unix transport adapters
                  |
        liburing-rs Proactor completions
```

The transport integration follows grpc-rs `main`: network progress runs on the
owner thread through the liburing Proactor, while `MemoryPipe` provides a
controlled in-memory seam for protocol tests. Public operations remain
synchronous. Connected socket reads and writes use a persistent caller-thread
Proactor with reusable DMA storage. MemoryPipe keeps payloads in a byte-bounded
atomic buffer and uses Proactor-driven socket notifications for wakeups.
DNS uses a c-ares ProactorResolver and returns the first usable address family
without waiting for an unanswered sibling. Configuration loading is charged
to the same deadline. DNS retirement stops submitting resolver work and counts
all final completions through the resolver's own pending-I/O accounting; if
its budget expires, the caller-owned ring and driver are
quarantined without running ring destruction. TCP/Unix dialing submits connect and
optional timeout SQEs. The connect budget covers DNS, all candidate addresses,
and TLS handshaking. IP literals bypass DNS. Cancellation drains the target
and cancel CQEs before releasing socket-address or timer storage.
TCP/TLS/Unix adapters preserve the SDK's connect deadlines,
read/write timeouts, TLS identity verification, and HTTP/2 ALPN. Mock-peer
threads belong to the examples and tests; the SDK starts no background workers.

Cargo dependencies use single-line inline tables in the workspace manifest:

```toml
[workspace.dependencies]
grpc = { git = "https://github.com/trunkszi/grpc-rs.git", branch = "main" }
sonic-rs = { version = "0.5.10" }
```

Member crates inherit them with `{ workspace = true }`. `Cargo.lock` pins the
upstream Git revision; refresh all shared grpc packages together after an
upstream transport release, then rerun the transport/TLS/Flight suites:

```sh
cargo update -p grpc -p grpc-core -p grpc-macros -p grpc-build
cargo test --workspace --locked
```

## JSON metadata and Prometheus responses

Flight `DoPutMetadata` and `DoPutResponse` use sonic-rs 0.5.10 with their existing
Serde field contracts. Structured server errors retain numeric codes, messages,
and explicit retry hints. `PromQueryResult::as_json_str` checks UTF-8 and leaves
JSON interpretation to the caller:

```rust
use sonic_rs::JsonValueTrait;

fn check_prometheus(body: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let response: sonic_rs::Value = sonic_rs::from_slice(body)?;
    assert_eq!(response.get("status").and_then(|v| v.as_str()), Some("success"));
    let encoded = sonic_rs::to_string(&response)?;
    let decoded: sonic_rs::Value = sonic_rs::from_str(&encoded)?;
    assert_eq!(decoded, response);
    Ok(())
}
```

Workspace CI runs formatting, strict Clippy, tests and documentation through
Cargo. Dependency, source and ownership rules are documented in `AGENTS.md`;
review them when adding packages or changing transport code. Authored text stays
ASCII and within 800 lines; the generated `Cargo.lock` keeps its native format
and is exempt from that length limit.

## Capabilities and boundaries

- `Client` supports unary health probes (`ping`, `probe_health`), typed and
  dynamic columnar inserts (`insert`, `insert_to`, `insert_batch`,
  `insert_batch_to`, `insert_batch_with_hints`, `insert_batch_to_with_hints`,
  `insert_batches`, `insert_multi_batch`), key-only columnar deletes
  (`delete_batch`, `delete_batch_from`), row-oriented inserts and deletes
  (`insert_rows`, `insert_rows_to`, `delete_rows`), and Prometheus Gateway
  queries (`prom_instant_query`, `prom_range_query`). Its 11 named DDL helpers
  are `create_database`, `alter_database`, `create_table`, `alter_table`,
  `drop_table`, `truncate_table`, `create_view`, `drop_view`, `create_flow`,
  `drop_flow`, and `comment_on`; `execute_ddl` sends any `DdlExpr`.
- `StreamInserter` sends client-streaming requests to
  `/greptime.v1.GreptimeDatabase/HandleRequests` within one immutable session
  scope and returns server-committed affected rows on `finish`. `write` and
  `write_delete` clear reusable source batches according to their `MemoryBuffer`
  policy only after the stream accepts the frame. Accepted sends do not
  acknowledge server commits; dropping an unfinished stream cancels the HTTP/2
  stream without undoing writes already processed by the server.
- Arrow Flight `BulkStreamWriter` implements `DoPut` with schema acknowledgement
  (`request_id = 0`), a bounded in-flight acknowledgement window
  (`BulkWriteOptions::max_in_flight`, default `8`), optional hourly timestamp
  partitioning (`partition_by_time_window`), and `FlightCompression` (`None`,
  `Lz4Frame`, or `Zstd`). `QueryStream` implements `DoGet` for SQL (`sql`),
  PromQL range queries (`prom_range_query`), serialized logical plans
  (`logical_plan`), and `INSERT INTO` plans (`insert_into_plan`), decoding
  schema, `DictionaryBatch`, `RecordBatch`, and `FlightMetadata` frames
  (`affected_rows`, `metrics`).
- `#[derive(Row)]` (`greptimedb_macros::Row`) generates `Row` metadata
  (`TABLE_NAME`, `COLUMNS`), a typed columnar `<Name>Batch`, a borrowed input
  row `<Name>Ref` (`BorrowedRow`), a zero-copy row projection `<Name>View` and
  `<Name>ViewIter`, and name-bound Arrow decoding via `RowDecode` and
  `PreparedRecordBatch`. Container attributes support
  `#[row(table = "...", rename_all = "...", crate_path = "...")]`; field
  attributes support
  `#[greptime(tag | timestamp | field, column = "...", unit = "...", decimal(precision = ..., scale = ...), json)]`.
  Every derived row schema requires exactly one non-nullable timestamp column.
- `ColumnarBatch`, `RowBatch`, `MultiTableBatch`, `TypedColumnBuf`, and
  `NullMask` support dynamic schema assembly and Protobuf request encoding.
  `BufferPolicy` (`DEFAULT_MAX_RETAINED_ROWS = 65_536`,
  `DEFAULT_MAX_RETAINED_BYTES = 8 MiB`, `unbounded()`, `zero()`) governs
  retained capacity when clearing buffers via `MemoryBuffer`. Row appends
  validate every column before mutating batch state; errors preserve existing
  logical rows.
- `FailoverPolicy` and `FailoverConfig` provide `RoundRobin`, `Random`,
  `LatencyAware`, and `Sticky` endpoint selection, consecutive-failure circuit
  breaking with cooldowns, half-open `HealthCheck` admission probes, and write
  replay safety (`FailoverPolicy::should_failover_write`). A write that already
  placed bytes on the wire can fail over only when the server sets `retry_hint`
  to `Some(true)`.
- `ThreadLocalClient` caches one lazy `Client` per OS thread (`with_client`,
  `close_current_thread`). `WorkerClient` manages per-thread slot pools with
  `RoundRobin`, `LeastInFlight`, or `KeyHash` leasing (`lease`, `lease_by_key`,
  `with_client`, `with_client_by_key`), optional `(worker_id + slot) %
  endpoints.len()` endpoint striping, and RAII `LeaseGuard` checkout.
  `threading::close_thread_caches()` clears both thread-local registries on the
  calling OS thread.
- `EndpointTransport` supports plaintext TCP (`grpc://`, `http://`), TLS 1.3
  (`grpcs://`, `https://`), Unix domain sockets (`unix://`), and in-process
  `MemoryPipe` pairs. The default `tls` feature uses `rustls` (`ring` provider)
  with TLS 1.3 only, HTTP/2 ALPN (`h2`), platform root certificates, optional
  custom CA bundles (`TlsOptions::with_ca_pem`, `with_ca_pem_file`), SNI
  override (`domain`), and mutual TLS client identities
  (`with_client_identity_pem`).

## Build, test, and live Docker E2E

The workspace targets Linux x86_64 and Rust Edition 2024. Transport tests and
examples, including MemoryPipe wakeups, require a kernel/environment that
permits io_uring. MemoryPipe coverage does not establish TCP or TLS coverage. `crates/greptimedb-proto/build.rs`
compiles the checked-in Arrow Flight schema (`proto/Flight.proto`) together with
upstream `GreptimeTeam/greptime-proto` pinned at commit
`acb247415b8f4c2f0750783dab4e1f38c71f5860`. Set `GREPTIME_PROTO_DIR` to an
existing `greptime-proto` checkout (or its `proto/` directory), or let the build
script populate and pin `$HOME/.cache/greptime-proto`. `PROTOC` overrides the
`protoc` executable path (falling back to `/root/.local/bin/protoc` or `PATH`).

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check -p greptimedb --no-default-features
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo test -p greptimedb --no-default-features --lib
```

| Package / Feature | Behavior |
| --- | --- |
| `greptimedb` (`default = ["tls"]`) | Enables `TlsOptions` and `grpcs://` / `https://` transport via `rustls` |
| `greptimedb` (`--no-default-features`) | Builds TCP, Unix socket, and `MemoryPipe` transports without `rustls` crates |

Run the live GreptimeDB standalone end-to-end test:

```sh
GREPTIMEDB_E2E_REQUIRED=1 cargo test -p greptimedb --test docker_greptimedb_e2e -- --nocapture
GREPTIMEDB_TEST_ENDPOINT=grpc://127.0.0.1:4001 GREPTIMEDB_E2E_REQUIRED=1 \
  cargo test -p greptimedb --test docker_greptimedb_e2e -- --nocapture
```

When `GREPTIMEDB_TEST_ENDPOINT` is unset, `docker_greptimedb_e2e` starts or
reuses the `greptimedb-rs-e2e` Docker container from `greptime/greptimedb:latest`
and waits for `/greptime.v1.HealthCheck/HealthCheck` readiness. Missing Docker
skips ordinary `cargo test` runs; `GREPTIMEDB_E2E_REQUIRED=1` turns an
unavailable Docker daemon or endpoint into a test failure. The container remains
running for reuse unless `GREPTIMEDB_E2E_REMOVE_CONTAINER=1` is set; external
endpoints are never removed.

## Derived rows, borrowed views, and Arrow round-trips

This excerpt from
[`02_derive_row_and_zero_copy_views.rs`](crates/greptimedb/examples/02_derive_row_and_zero_copy_views.rs)
derives a typed table schema, pushes owned and borrowed rows without growing a
warmed batch, reads borrowed views, and round-trips through an Arrow
`RecordBatch`:

```rust
use greptimedb::{
    BatchEncode, BufferPolicy, ColumnDataType, Decimal128, MemoryBuffer, Row, SemanticType,
    bulk::batch_to_record_batch,
    row::RowDecode,
};

#[derive(Clone, Debug, PartialEq, Row)]
#[row(table = "order_book_ticks", rename_all = "snake_case")]
struct OrderTick {
    #[greptime(tag)]
    symbol: String,
    #[greptime(timestamp, unit = "ns")]
    ts: i64,
    #[greptime(field)]
    best_bid: f64,
    #[greptime(field)]
    size: Option<f64>,
    #[greptime(json)]
    payload: String,
    #[greptime(decimal(precision = 18, scale = 4))]
    notional: Decimal128,
    #[greptime(field)]
    blob: Vec<u8>,
}

fn main() -> greptimedb::Result<()> {
    assert_eq!(OrderTick::TABLE_NAME, "order_book_ticks");
    assert_eq!(OrderTick::COLUMNS[0].semantic_type, SemanticType::Tag);
    assert_eq!(
        OrderTick::COLUMNS[1].data_type,
        ColumnDataType::TimestampNanosecond
    );

    let mut batch = OrderTickBatch::with_capacity_and_policy(2, BufferPolicy::unbounded());
    let first = OrderTick {
        symbol: "BTCUSDT".into(),
        ts: 1_700_000_000_000_000_000,
        best_bid: 100.25,
        size: Some(1.5),
        payload: "{\"bid\":100.25}".into(),
        notional: Decimal128::new(123_456, 18, 4),
        blob: vec![0, 1, 2],
    };
    batch.push(&first);
    batch.push_ref(&OrderTickRef {
        symbol: "ETHUSDT",
        ts: first.ts + 1,
        best_bid: 2_000.5,
        size: None,
        payload: "{\"bid\":2000.5}",
        notional: Decimal128::new(-987_654, 18, 4),
        blob: &[0xff, 0x00],
    });

    assert_eq!(batch.row_count(), 2);
    let view = batch.get_view(1).expect("row 1 is present");
    assert_eq!(view.symbol, "ETHUSDT");
    assert_eq!(view.size, None);

    let record = batch_to_record_batch(&batch)?;
    let decoded = <OrderTick as RowDecode>::decode_record_batch(&record)?;
    assert_eq!(decoded.row_count(), 2);
    batch.clear();
    Ok(())
}
```

## Runnable examples

All ten examples run offline against in-process `MemoryPipe` or loopback TCP
`SyncServer` peers:

```sh
cargo run -p greptimedb --example 01_quickstart_unary_ddl_and_crud
```

Setting `GREPTIMEDB_ENDPOINT` adds an optional live operation after the offline
assertions: examples `01`-`04`, `08`, and `09` run a read-only `ping` /
`probe_health` (with optional `GREPTIMEDB_CA_PEM` in `09`), while `05` streams
into `example_stream_ticks`, `06` creates and drops a temporary Flight `DoPut`
table, `07` executes live Flight `DoGet` queries, and `10` writes and queries
`l1_book_ticks`.

| Example under `crates/greptimedb/examples/` | Demonstration |
| --- | --- |
| `01_quickstart_unary_ddl_and_crud.rs` | Unary health checks, DDL helpers, columnar/row CRUD, and multi-table ingestion |
| `02_derive_row_and_zero_copy_views.rs` | `#[derive(Row)]`, borrowed `OrderTickRef` / `OrderTickView`, and name-bound Arrow decoding |
| `03_memory_policy_and_batch_reuse.rs` | `BufferPolicy` watermarks, warm capacity retention, and reclamation across batch types |
| `04_full_column_type_matrix.rs` | Full 21-column `ColumnDataType` matrix across Protobuf and Arrow IPC round-trips |
| `05_stream_inserter_and_backpressure.rs` | `StreamInserter` client streaming, bounded batch reuse, completion, and cancellation |
| `06_arrow_flight_doput_bulk_writer.rs` | Flight `BulkStreamWriter` (`DoPut`), IPC compression (`None`, `Lz4Frame`, `Zstd`), hourly partitioning, and ACK windowing |
| `07_arrow_flight_doget_and_promql.rs` | Flight `QueryStream` (`DoGet`), dictionary batches, metadata frames, and unary/Flight PromQL |
| `08_thread_local_and_worker_striping.rs` | `ThreadLocalClient`, `WorkerClient` slot balancing, endpoint striping, and `LeaseGuard` lifecycle |
| `09_tls_and_failover_circuit_breaker.rs` | `TlsOptions` (TLS 1.3 + `h2`), `FailoverPolicy` selection, circuit recovery, and write-replay safety |
| `10_market_data_pipeline.rs` | End-to-end L1 market-data staging, Zstd Flight `DoPut` ingestion, and typed `DoGet` analytical replay |

## Workspace crates and modules

| Crate directory | Package | Responsibility |
| --- | --- | --- |
| `crates/greptimedb-proto` | `greptimedb-proto` | Generated Protobuf messages (`greptime.v1.*`, `arrow.flight.protocol.*`, Prometheus/Substrait) and `grpc-rs` service bindings |
| `crates/greptimedb-macros` | `greptimedb-macros` | Procedural `#[derive(Row)]` macro generating typed batches, borrowed input refs, row views, and Arrow decoders |
| `crates/greptimedb` | `greptimedb` | Public synchronous SDK facade: client, transports, batches, Flight `DoPut`/`DoGet`, failover, and thread-local/worker pools |

| Module in `crates/greptimedb/src/` | Responsibility |
| --- | --- |
| `config`, `error`, `hints`, `types`, `buffer_policy` | Endpoint parsing, credentials, `Error` / `ErrorCode` / `RetryableError`, `WriteHints`, scalar/temporal/decimal types, and `BufferPolicy` |
| `column_buf`, `batch`, `row` | `NullMask`, `TypedColumnBuf`, `ColumnarBatch`, `RowBatch`, `MultiTableBatch`, `Row` / `BorrowedRow`, and `RowDecode` |
| `transport`, `tls` | `EndpointTransport` (TCP, TLS 1.3, Unix socket, `MemoryPipe`) and `TlsOptions` |
| `client`, `ddl`, `prom`, `stream_inserter` | Unary `Client`, 11 DDL helpers + `execute_ddl`, Prometheus Gateway queries, and `StreamInserter` |
| `bulk`, `query` | Arrow Flight `BulkStreamWriter` (`DoPut`), `FlightIpcEncoder`, hourly partitioning, and `QueryStream` (`DoGet`) |
| `failover`, `threading` | `FailoverPolicy` / `FailoverConfig` circuit breaker and `ThreadLocalClient` / `WorkerClient` per-thread pools |

See [AGENTS.md](AGENTS.md) for engineering invariants and verification gates.
