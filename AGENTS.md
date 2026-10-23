# greptimedb-rs - Shared Agent Guide

## Project overview

`greptimedb-rs` is a synchronous, completion-driven, thread-affine Rust Edition 2024
SDK for GreptimeDB built on `grpc-rs`. It provides caller-driven unary RPCs,
client-streaming ingestion (`StreamInserter`), Arrow Flight bulk ingestion
(`BulkStreamWriter`) and queries (`QueryStream`), `#[derive(Row)]` typed
columnar batches and borrowed views, multi-endpoint failover, and per-thread
client pools.

This guide applies to Claude Code, Codex, Gemini, and human maintainers.
The grpc-rs network adapters use liburing-rs Proactor completions on the
caller thread; MemoryPipe remains the in-memory test seam. Do not replace
completion progress with a background executor or readiness runtime.

`crates/` is the source of truth for shipped APIs and protocol contracts;
inspect the owning module before changing or documenting behavior.

## Hard invariants and runtime contracts

- Keep `Client`, `StreamInserter`, `BulkStreamWriter`, `QueryStream`,
  `ColumnarBatch`, `RowBatch`, `MultiTableBatch`, and `FailoverPolicy` confined
  to their creating OS thread (`!Send` and `!Sync`). Progress runs on the
  caller thread; SDK components spawn no internal worker threads or mutexes.
  Scale across application threads with `ThreadLocalClient` or `WorkerClient`,
  keeping leased clients on their owner thread.
- Keep the dependency graph synchronous and completion-driven. Exclude
  blocking coordination primitives, executor keywords, legacy JSON/HTTP clients,
  blocking host lookup helpers, and alternate HTTP/TLS stacks across the tree.
- Return structured `greptimedb::Result<T>` (`Result<T, Error>`) across public
  SDK boundaries and preserve specialized error types (`RowError`,
  `ColumnBufError`) where declared. Production sources (`crates/*/src/**/*.rs`
  outside `#[cfg(test)]`) and `crates/*/build.rs` forbid `.unwrap()`,
  `.expect(`, `todo!(`, and `unimplemented!(`.
- Validate every column before committing a row append so failed writes leave
  existing logical rows intact. Honor `BufferPolicy` row and byte watermarks in
  every `MemoryBuffer` implementation.
- Treat accepted stream writes as in-flight until terminal acknowledgement
  (`StreamInserter::finish` or `BulkStreamWriter::finish`). Replay a write that
  already placed bytes on the wire only when `RetryableError::retry_hint` is
  `Some(true)`, following `FailoverPolicy::should_failover_write`.
- Keep authored source, tests, scripts, configuration, and documentation ASCII
  English and at most 800 lines. Preserve native Cargo.lock formatting; this
  generated file is exempt from the line limit. Declare all dependencies
  in `[workspace.dependencies]` using single-line inline `{ ... }` tables, and
  satisfy all workspace `rust` and `clippy` lints (`-D warnings`).
- Document every `unsafe` block with an accurate `// SAFETY:` comment and
  include `# Errors` / `# Panics` sections on fallible or panicking public
  functions as enforced by workspace clippy lints.

## Build, lint, and test commands

Run verification commands from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check -p greptimedb --no-default-features
cargo test --workspace --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo test -p greptimedb --no-default-features --lib
cargo run -p greptimedb --example 02_derive_row_and_zero_copy_views
GREPTIMEDB_E2E_REQUIRED=1 cargo test -p greptimedb --test docker_greptimedb_e2e -- --nocapture
git diff --check
```

Run checks covering the modified crate, feature gates (`default = ["tls"]` and
`--no-default-features`), and callers. For documentation-only edits, verify
commands, tracked file paths, and `git diff --check`. Report actual command
outcomes and distinguish skipped Docker E2E runs from verified live server
round trips.

## Crate layering and module responsibilities

| Path | Responsibility |
| --- | --- |
| `Cargo.toml` | Workspace members, pinned dependency versions, and strict lint/profile policy |
| `crates/greptimedb-proto/` | `build.rs` proto resolution (`GREPTIME_PROTO_DIR` or `$HOME/.cache/greptime-proto` at `acb247415b8f4c2f0750783dab4e1f38c71f5860`), `proto/Flight.proto`, generated Prost messages, and `grpc-build` service bindings |
| `crates/greptimedb-macros/` | Procedural `#[derive(Row)]` parser (`model.rs`) and code generator (`generate.rs`) |
| `crates/greptimedb/src/config.rs`, `error.rs`, `hints.rs`, `types.rs`, `buffer_policy.rs` | Endpoint validation, `Auth`, `ClientOptions`, `Error` / `ErrorCode` / `RetryableError`, `WriteHints`, scalar/temporal/decimal types, and `BufferPolicy` |
| `crates/greptimedb/src/column_buf.rs`, `batch/`, `row/` | `NullMask`, `TypedColumnBuf`, `ColumnarBatch`, `RowBatch`, `MultiTableBatch`, `Row` / `BorrowedRow`, and name-bound Arrow `RowDecode` |
| `crates/greptimedb/src/transport.rs`, `tls.rs` | `EndpointTransport` (TCP, TLS 1.3 via `rustls`, Unix socket, `MemoryPipe`) and `TlsOptions` |
| `crates/greptimedb/src/client.rs`, `client/attempt.rs`, `ddl.rs`, `prom.rs`, `stream_inserter.rs` | Unary `Client`, failover/probe attempt driver, 11 DDL helpers + `execute_ddl`, Prometheus Gateway queries, and `StreamInserter` |
| `crates/greptimedb/src/bulk/`, `query/` | Arrow Flight `BulkStreamWriter` (`DoPut`), `FlightIpcEncoder`, hourly partitioning, and `QueryStream` (`DoGet`) |
| `crates/greptimedb/src/failover.rs`, `threading/` | `FailoverPolicy` / `FailoverConfig` circuit breaker and `ThreadLocalClient` / `WorkerClient` per-thread pools |
| `crates/greptimedb/examples/` | Ten runnable offline-first examples (`01` through `10`) with optional `GREPTIMEDB_ENDPOINT` live probes/writes |
| `crates/greptimedb/tests/` | Integration, adversarial regression, and live Docker E2E (`docker_greptimedb_e2e.rs`) test suites |
| `.github/workflows/ci.yml` | Official Ubuntu runner executing formatting, Clippy, tests, and documentation through Cargo |

## Context pointers

- Read [README.md](README.md) when selecting public SDK APIs, feature flags,
  runnable examples, or crate/module boundaries.
- Read [crates/greptimedb/src/lib.rs](crates/greptimedb/src/lib.rs) before
  changing the public re-export surface of the `greptimedb` crate.
- Read [crates/greptimedb-proto/build.rs](crates/greptimedb-proto/build.rs)
  before changing Protobuf compilation, `GREPTIME_PROTO_DIR` resolution, or the
  pinned `greptime-proto` revision.
- Read [crates/greptimedb/tests/docker_greptimedb_e2e.rs](crates/greptimedb/tests/docker_greptimedb_e2e.rs)
  before running or modifying live GreptimeDB Docker E2E tests, container
  lifecycle, or `GREPTIMEDB_E2E_*` environment variables.
- Read [Cargo.toml](Cargo.toml) before adding dependencies or changing lints;
  preserve the hard invariants above when splitting modules or modifying
  `build.rs` and production error handling.

## JSON and dependency maintenance

Use sonic-rs 0.5.10 for JSON values, typed metadata, and serialization. Preserve
Serde derives and wire field names. Import `JsonValueTrait` / `JsonContainerTrait`
where dynamic values require typed access or container lookup. Keep malformed
or out-of-range server diagnostics from replacing the original gRPC status.

All workspace dependency sources live in root `[workspace.dependencies]`;
member manifests inherit with `{ workspace = true }`. Update the grpc family
as one unit after the upstream Proactor version has been published:

```sh
cargo update -p grpc -p grpc-core -p grpc-macros -p grpc-build
```

Check the resolved Git SHA and its upstream source before claiming a transport
migration. Run TCP, Unix, TLS, failover, streaming, and Flight tests against the
locked revision. Preserve no-default-features support.

## Four-dimensional testing matrix

| Dimension | Observable boundary |
| --- | --- |
| Integration | Generated row APIs, native column buffers, Protobuf headers, JSON metadata, and Arrow schema/record decoding agree across modules. |
| E2E | Offline loopback/MemoryPipe peers complete unary, streaming, Flight, and PromQL exchanges; the explicit Docker gate verifies live GreptimeDB. |
| Lifecycle | Accepted writes remain in flight until ACK/finish; drop cancels streams, leases return to owner-thread pools, and failed sessions stay failed. |
| Scenario | Malformed JSON and frames, invalid metadata, flow limits, slow TLS peers, circuit recovery, and retry hints preserve typed failures and replay safety. |

Run all ten examples with `cargo run -p greptimedb --example <name> --locked`
when SDK behavior changes. Review staged, unstaged, and untracked files against
the hard invariants above; Cargo lints do not enforce every source policy.

## Change hygiene

1. Read the task, this guide, affected modules, manifests, and current Git state.
2. Keep edits within the authorized repository and preserve concurrent work.
3. Verify changed public behavior with controlled peers and independent expected
   results. Preserve thread affinity and immutable in-flight write ownership.
4. Run focused checks, then the workspace Cargo gates, feature-profile checks, and
   all ten examples. Record skipped Docker coverage separately from live E2E.
5. For policy changes, probe forbidden source references, aliased dependencies,
   non-ASCII files, and oversized files, including untracked files.
6. Finish with `git diff --check` and a file ownership review. Commit, rewrite
   history, or push only when explicitly requested by the current user.

## DNS and connection deadlines

Domain names resolve through cares-rs ProactorResolver with caller-thread
callbacks. Return an available family without waiting for its unanswered sibling.
Charge resolver configuration loading to the shared budget and check the
deadline before submitting queries. Literal IP addresses bypass lookup. DNS, every candidate connection,
and TLS handshaking share one monotonic deadline. Both TCP and Unix dialing
use connect SQEs and owned address storage. Expiry cancels the pending connect;
retain operands until the timer, connect, and cancellation CQEs retire.
DNS retirement uses the resolver driver's exact pending-I/O accounting,
including its cancellation acknowledgments, and drains only within the
remaining budget. The caller owns both the ring and completion dispatcher. Apply address
selection after the final harvest. If retirement cannot finish, retain the
entire ring and caller-owned driver without invoking their destructors. A failed
shutdown must also quarantine the entire ring.

The SDK forwards connect_timeout to Unix endpoints as well as network endpoints.
Keep local test notifications on completion-driven byte pipes and shared event
histories in finite atomic slots. Fixture listeners own their socket registries
and close them before joining connection workers.
