//! Synchronous client-streaming ingestion with bounded caller-owned batches.
//!
//! ```text
//! cargo run -p greptimedb --example 05_stream_inserter_and_backpressure
//! GREPTIMEDB_ENDPOINT=grpc://127.0.0.1:4001 cargo run -p greptimedb --example 05_stream_inserter_and_backpressure
//! ```
//!
//! The offline server exercises real HTTP/2 flow control and gRPC frames without
//! a database. A synchronous send waits for transport capacity; keep a bounded
//! batch instead of building an unbounded producer queue. Sending is not a commit
//! acknowledgement: only `finish()` returns the server's affected-row count.
//! The optional live section writes `example_stream_ticks` in the default schema.
//!
//! This SDK calls its send-and-clear method `write`. The local `write_and_clear`
//! and `write_with_policy` adapters below show the corresponding retention choices.
//! Failed requests stay caller-owned; the local `take_failed_requests` drains our
//! explicit recovery queue. Never automatically replay an uncertain write.

use std::{thread, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use greptimedb::{
    Client,
    batch::{BatchEncode, ColumnarBatch, MultiTableBatch, RowBatch},
    buffer_policy::{BufferPolicy, MemoryBuffer},
    config::{ClientOptions, Endpoint},
    hints::WriteHints,
    stream_inserter::StreamInserter,
    types::{ColumnDataType, SemanticType, TimestampUnit, ValueRef},
};
use greptimedb_proto::v1::{
    AffectedRows, GreptimeRequest, GreptimeResponse, InsertRequest, ResponseHeader,
    greptime_request::Request, greptime_response::Response,
};
use grpc::{
    ProstCodec, Status,
    core::transport::{MemoryPipe, SyncTransport},
    server::SyncServer,
};

#[path = "../tests/support/signal.rs"]
mod signal;

const PATH: &str = "/greptime.v1.GreptimeDatabase/HandleRequests";

fn stream_status(error: grpc::StreamError) -> Status {
    match error {
        grpc::StreamError::Status(status) => status,
        grpc::StreamError::WouldBlock => Status::internal("blocking server returned pending"),
    }
}

fn row_count(request: &Request) -> u32 {
    match request {
        Request::Inserts(group) => group.inserts.iter().map(|batch| batch.row_count).sum(),
        Request::Deletes(group) => group.deletes.iter().map(|batch| batch.row_count).sum(),
        Request::RowInserts(group) => group
            .inserts
            .iter()
            .filter_map(|batch| batch.rows.as_ref())
            .map(|rows| rows.rows.len() as u32)
            .sum(),
        _ => 0,
    }
}

fn offline_server(pipe: MemoryPipe, received: signal::Sender) -> std::result::Result<(), Status> {
    let mut server = SyncServer::new(pipe)?;
    server.register_client_streaming(
        PATH,
        ProstCodec::<GreptimeResponse, GreptimeRequest>::new(),
        move |mut requests| {
            let mut affected_rows = 0;
            while let Some(message) = requests.recv_next().map_err(stream_status)? {
                let request = message
                    .request
                    .ok_or_else(|| Status::invalid_argument("missing request"))?;
                if matches!(&request, Request::Inserts(group)
                    if group.inserts.iter().any(|batch| batch.table_name == "_reject"))
                {
                    return Err(Status::unavailable("simulated region failure"));
                }
                affected_rows += row_count(&request);
                received
                    .send(1)
                    .map_err(|error| Status::internal(error.to_string()))?;
            }
            Ok(grpc::Response::new(GreptimeResponse {
                header: Some(ResponseHeader::default()),
                response: Some(Response::AffectedRows(AffectedRows {
                    value: affected_rows,
                })),
            }))
        },
    )?;
    server.serve_connection()
}

fn batch(table: &str, policy: BufferPolicy) -> Result<ColumnarBatch> {
    let mut batch = ColumnarBatch::with_policy(table, 8, policy);
    batch.add_timestamp_column("ts", TimestampUnit::Millisecond)?;
    batch.add_tag_column("host", ColumnDataType::String)?;
    batch.add_field_column("value", ColumnDataType::Float64)?;
    batch.push_row(&[
        ValueRef::TimestampMillisecond(1_700_000_000_000),
        ValueRef::String("offline-host"),
        ValueRef::Float64(42.0),
    ])?;
    Ok(batch)
}

fn write_and_clear(writer: &mut StreamInserter, batch: &mut ColumnarBatch) -> Result<()> {
    writer.write(batch)?;
    Ok(())
}

fn write_with_policy(
    writer: &mut StreamInserter,
    batch: &mut ColumnarBatch,
    policy: BufferPolicy,
) -> Result<()> {
    writer.send_batch(batch)?;
    batch.clear_with_policy(policy);
    Ok(())
}

fn take_failed_requests(queue: &mut Vec<InsertRequest>) -> Vec<InsertRequest> {
    std::mem::take(queue)
}

fn ingestion(client: &mut Client) -> Result<()> {
    let first = batch("example_stream_ticks", BufferPolicy::default())?;
    let second = batch("example_stream_other", BufferPolicy::default())?;
    let mut rows = RowBatch::new("example_stream_ticks");
    rows.add_column_schema(
        "ts",
        SemanticType::Timestamp,
        ColumnDataType::TimestampMillisecond,
    )?;
    rows.add_column_schema("host", SemanticType::Tag, ColumnDataType::String)?;
    rows.add_column_schema("value", SemanticType::Field, ColumnDataType::Float64)?;
    rows.push_row(&[
        ValueRef::TimestampMillisecond(1_700_000_000_001),
        ValueRef::String("offline-host"),
        ValueRef::Float64(43.0),
    ])?;
    let mut writer = client.stream_inserter()?;
    writer.send_batch(&first)?;
    writer.send_batch_to("example_stream_other", &first)?;
    writer.send_batches(&[&first, &second])?;
    // MultiTableBatch holds owned snapshots; send_batches above accepts borrowed
    // BatchEncode sources. send_request sends an owned MultiTableBatch snapshot.
    let mut tables = MultiTableBatch::new();
    tables.add_batch(&first)?;
    tables.add_batch(&second)?;
    writer.send_request(Request::Inserts(tables.into_insert_requests()))?;
    writer.send_rows(&rows)?;
    writer.send_delete_batch(&first)?;
    let mut reusable = batch("example_stream_ticks", BufferPolicy::default())?;
    write_and_clear(&mut writer, &mut reusable)?;
    ensure!(reusable.row_count() == 0, "successful write clears rows");
    let mut burst = batch("example_stream_ticks", BufferPolicy::unbounded())?;
    write_with_policy(&mut writer, &mut burst, BufferPolicy::zero())?;
    ensure!(burst.row_capacity() == 0, "zero policy releases buffers");
    ensure!(writer.batches_sent() == 8 && writer.rows_sent() == 10);
    println!(
        "sent {} requests / {} rows",
        writer.batches_sent(),
        writer.rows_sent()
    );
    let affected = writer.finish()?;
    ensure!(
        affected == 10,
        "offline peer must acknowledge every submitted row"
    );
    println!("finish acknowledged {affected} affected rows");
    Ok(())
}

fn bounded_producer(client: &mut Client) -> Result<()> {
    let mut bounded = batch("example_stream_ticks", BufferPolicy::default())?;
    // Reuse 512 rows for twelve frames, exceeding the initial HTTP/2 window
    // without retaining twelve separate batches or spawning producer tasks.
    let label = "bounded-producer-host-".repeat(4);
    for timestamp in 1..512 {
        bounded.push_row(&[
            ValueRef::TimestampMillisecond(1_700_000_000_000 + timestamp),
            ValueRef::String(&label),
            ValueRef::Float64(42.0),
        ])?;
    }
    let mut writer = client.stream_inserter()?;
    for _ in 0..12 {
        writer.send_batch(&bounded)?;
    }
    ensure!(writer.batches_sent() == 12 && writer.rows_sent() == 6_144);
    ensure!(writer.finish()? == 6_144);
    println!("flow-controlled 6,144 rows using one bounded 512-row batch");
    Ok(())
}

fn recovery(client: &mut Client, received: &signal::Receiver) -> Result<()> {
    // The server rejects this open stream. A successful probe on the same channel
    // drives receipt of that rejection before we try the second write: no sleeps.
    let mut retained = batch("_reject", BufferPolicy::default())?;
    let snapshot = retained.encode_insert_request()?;
    let mut rejected = client.stream_inserter()?;
    rejected.send_batch(&retained)?;
    let mut probe = client.stream_inserter()?;
    probe.send_batch_to("_probe", &retained)?;
    ensure!(probe.finish()? == 1);
    let failure = write_with_policy(&mut rejected, &mut retained, BufferPolicy::zero());
    ensure!(
        failure.is_err(),
        "simulated rejection must reject subsequent writes"
    );
    ensure!(retained.encode_insert_request()? == snapshot);
    let mut failed_requests = vec![snapshot];
    let recovered = take_failed_requests(&mut failed_requests);
    ensure!(recovered.len() == 1 && failed_requests.is_empty());
    println!(
        "recovered {} caller-owned request; replay requires an application decision",
        recovered.len()
    );
    drop(rejected);

    // Drain earlier notifications so the next notification proves this stream's
    // first message reached the peer before cancellation.
    while received.try_recv().is_ok() {}
    let mut cancelled = client.stream_inserter()?;
    cancelled.send_batch_to("cancelled", &retained)?;
    received.recv_timeout(Duration::from_secs(5))?;
    cancelled.cancel()?;
    let mut next = client.stream_inserter()?;
    next.send_batch_to("after_cancel", &retained)?;
    ensure!(next.finish()? == 1);
    println!("cancelled stream released the shared channel for its next stream");
    Ok(())
}

fn main() -> Result<()> {
    let (mut client_pipe, mut server_pipe) = MemoryPipe::with_capacity(16 * 1024)?;
    for pipe in [&mut client_pipe, &mut server_pipe] {
        pipe.set_read_timeout(Some(Duration::from_secs(10)))?;
        pipe.set_write_timeout(Some(Duration::from_secs(10)))?;
    }
    let (sent, received) = signal::channel();
    let worker = thread::spawn(move || offline_server(server_pipe, sent));
    let options = ClientOptions::from_endpoint(Endpoint::parse("grpc://127.0.0.1:4001")?);
    let mut client = Client::from_memory_pipe(options, client_pipe)?;
    let result = ingestion(&mut client)
        .and_then(|()| bounded_producer(&mut client))
        .and_then(|()| recovery(&mut client, &received));
    drop(client);
    worker
        .join()
        .map_err(|_| anyhow!("offline server panicked"))??;
    result?;

    if let Ok(endpoint) = std::env::var("GREPTIMEDB_ENDPOINT") {
        let mut options = ClientOptions::from_endpoint(Endpoint::parse(&endpoint)?);
        options.write_hints = WriteHints::new().with_auto_create_table(true);
        let mut client = Client::connect(options).context("connect to GREPTIMEDB_ENDPOINT")?;
        // Live affected rows depend on existing keys and deletion semantics, so
        // run a minimal insert rather than asserting the offline peer's counters.
        let batch = batch("example_stream_ticks", BufferPolicy::default())?;
        let mut writer = client.stream_inserter()?;
        writer.send_batch(&batch)?;
        println!("live finish: {} affected rows", writer.finish()?);
    }
    Ok(())
}
