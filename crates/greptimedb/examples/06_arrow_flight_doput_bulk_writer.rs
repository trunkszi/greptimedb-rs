//! Bounded Arrow Flight DoPut ingestion with compression and hourly partitioning.
//!
//! Offline: `cargo run -p greptimedb --example 06_arrow_flight_doput_bulk_writer`.
//! Live: `GREPTIMEDB_ENDPOINT=grpc://127.0.0.1:4001 cargo run -p greptimedb
//! --example 06_arrow_flight_doput_bulk_writer`. Live mode creates and removes a
//! uniquely named table in the default database. Offline mode uses real HTTP/2,
//! gRPC, and Arrow IPC frames over MemoryPipe without opening a network socket.

use std::{env, thread, time::Duration};

use anyhow::{Context, Result, ensure};
use greptimedb::{
    BulkStreamWriter, BulkWriteOptions, BulkWriteStats, Client, ClientOptions, ColumnDataType,
    ColumnarBatch, DoPutMetadata, DoPutResponse, Endpoint, FlightCompression, RecordBatch, Row,
    TimestampUnit, ValueRef, WriteHints,
    bulk::{batch_to_record_batch, partition_record_batch_by_hour},
    query::{decode_flight_record_batch, decode_flight_schema},
};
use greptimedb_proto::flight::{FlightData, PutResult, flight_descriptor::DescriptorType};
use grpc::{
    ProstCodec, Status,
    core::transport::{MemoryPipe, SyncTransport},
    server::SyncServer,
    stream::{RequestStream, ResponseSink, StreamError},
};

const TABLE: &str = "example_bulk";
const DO_PUT: &str = "/arrow.flight.protocol.FlightService/DoPut";
const BUDGET: Duration = Duration::from_secs(5);
const HOUR_MS: i64 = 3_600_000;

#[derive(Row)]
#[row(table = "example_bulk")]
struct Reading {
    #[greptime(timestamp, unit = "ms")]
    ts: i64,
    #[greptime(tag)]
    sensor: String,
    value: f64,
}

fn main() -> Result<()> {
    for compression in [
        FlightCompression::None,
        FlightCompression::Lz4Frame,
        FlightCompression::Zstd,
    ] {
        offline(compression)?;
    }
    match env::var("GREPTIMEDB_ENDPOINT") {
        Ok(endpoint) if !endpoint.is_empty() => live(&endpoint)?,
        Ok(_) | Err(env::VarError::NotPresent) => {
            println!("Offline DoPut checks passed; set GREPTIMEDB_ENDPOINT for live writes.");
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn options(endpoint: &str) -> Result<ClientOptions> {
    let mut options = ClientOptions::from_endpoint(Endpoint::parse(endpoint)?);
    options.request_timeout = BUDGET;
    options.write_hints = WriteHints::new().with_auto_create_table(true);
    Ok(options)
}

fn columns(table: &str) -> Result<ColumnarBatch> {
    let mut batch = ColumnarBatch::with_capacity(table, 3);
    batch.add_timestamp_column("ts", TimestampUnit::Millisecond)?;
    batch.add_tag_column("sensor", ColumnDataType::String)?;
    batch.add_field_column("value", ColumnDataType::Float64)?;
    for (timestamp, value) in [(0, 10.0), (HOUR_MS, 20.0), (2 * HOUR_MS, 30.0)] {
        batch.push_row(&[
            ValueRef::TimestampMillisecond(timestamp),
            ValueRef::String("sensor-a"),
            ValueRef::Float64(value),
        ])?;
    }
    Ok(batch)
}

// Each write spans three hours, so three source batches produce nine IPC batches.
// The third partition forces the writer to drain an ACK before sending again:
// only two record batches can be outstanding at any point.
fn write_all(
    client: &mut Client,
    table: &str,
    compression: FlightCompression,
) -> Result<BulkWriteStats> {
    let columnar = columns(table)?;
    let record = batch_to_record_batch(&columnar)?;
    let partitions = partition_record_batch_by_hour(&record)?;
    ensure!(partitions.len() == 3, "expected three hourly partitions");
    ensure!(partitions.iter().all(|part| part.num_rows() == 1));

    let mut derived = Reading::new_batch_with_capacity(3);
    for (ts, value) in [(0, 10.0), (HOUR_MS, 20.0), (2 * HOUR_MS, 30.0)] {
        Reading {
            ts,
            sensor: "sensor-a".into(),
            value,
        }
        .push_into_batch(&mut derived);
    }
    let mut writer: BulkStreamWriter = client.bulk_writer(
        table,
        record.schema().as_ref(),
        BulkWriteOptions {
            compression,
            max_in_flight: 2,
            partition_by_time_window: true,
            ..BulkWriteOptions::default()
        },
    )?;
    writer.write_record_batch(&record)?;
    writer.write_batch(&derived)?;
    writer.write_batch(&columnar)?;
    let stats = writer.finish()?;
    ensure!(stats.batches_sent == 9, "unexpected frame count: {stats:?}");
    ensure!(
        stats.total_affected_rows == 9,
        "unexpected row count: {stats:?}"
    );
    println!("{compression:?}: {stats:?}");
    Ok(stats)
}

fn offline(compression: FlightCompression) -> Result<()> {
    let (mut pipe, mut peer) = MemoryPipe::pair();
    pipe.set_read_timeout(Some(BUDGET))?;
    pipe.set_write_timeout(Some(BUDGET))?;
    peer.set_read_timeout(Some(BUDGET))?;
    peer.set_write_timeout(Some(BUDGET))?;
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(peer)?;
        server.register_bidi_streaming(
            DO_PUT,
            ProstCodec::<PutResult, FlightData>::new(),
            |mut incoming, sink| serve_put(&mut incoming, sink),
        )?;
        server.serve_connection()
    });
    let outcome = (|| -> Result<()> {
        let mut client = Client::from_memory_pipe(options("grpc://offline:4001")?, pipe)?;
        write_all(&mut client, TABLE, compression)?;
        // Dropping the client closes the connection, letting SyncServer return.
        Ok(())
    })();
    let server_result = worker
        .join()
        .map_err(|_| anyhow::anyhow!("DoPut server thread panicked"))?;
    outcome?;
    server_result.context("DoPut server failed")?;
    Ok(())
}

fn status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking DoPut stream would block"),
    }
}

fn acknowledge(sink: &mut ResponseSink<PutResult>, id: i64, rows: usize) -> Result<(), Status> {
    let result = PutResult {
        app_metadata: sonic_rs::to_vec(&DoPutResponse {
            request_id: id,
            affected_rows: rows,
        })
        .map_err(status)?,
    };
    sink.send(&result).map_err(stream_status)
}

// Decode the actual schema and compressed Arrow bodies, rather than counting
// mock calls. An ACK is sent for the schema and then for every ordered batch.
fn serve_put(
    incoming: &mut RequestStream<FlightData>,
    sink: &mut ResponseSink<PutResult>,
) -> Result<(), Status> {
    let first = incoming
        .recv_next()
        .map_err(stream_status)?
        .ok_or_else(|| Status::internal("missing schema frame"))?;
    let descriptor = first
        .flight_descriptor
        .ok_or_else(|| Status::internal("missing Flight descriptor"))?;
    if descriptor.r#type != DescriptorType::Path as i32 || descriptor.path != [TABLE] {
        return Err(Status::internal("unexpected DoPut table descriptor"));
    }
    let schema = decode_flight_schema(&first.data_header).map_err(status)?;
    let metadata: DoPutMetadata = sonic_rs::from_slice(&first.app_metadata).map_err(status)?;
    if metadata.request_id != 0
        || schema
            .field(0)
            .metadata()
            .get("greptime:semantic_type")
            .map(String::as_str)
            != Some("timestamp")
    {
        return Err(Status::internal("invalid schema metadata"));
    }
    acknowledge(sink, 0, 0)?;
    let mut frames = 0;
    let mut rows = 0;
    while let Some(frame) = incoming.recv_next().map_err(stream_status)? {
        let metadata: DoPutMetadata = sonic_rs::from_slice(&frame.app_metadata).map_err(status)?;
        let record: RecordBatch =
            decode_flight_record_batch(&schema, &frame.data_header, &frame.data_body)
                .map_err(status)?
                .ok_or_else(|| Status::internal("missing record batch"))?;
        frames += 1;
        rows += record.num_rows();
        if metadata.request_id != frames || record.num_rows() != 1 {
            return Err(Status::internal(
                "out-of-order or unpartitioned record batch",
            ));
        }
        acknowledge(sink, metadata.request_id, record.num_rows())?;
    }
    if frames != 9 || rows != 9 {
        return Err(Status::internal(
            "DoPut completed with unexpected frame/row counts",
        ));
    }
    Ok(())
}

fn live(endpoint: &str) -> Result<()> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let table = format!("example_bulk_{}_{suffix}", std::process::id());
    let mut client = Client::connect(options(endpoint)?)?;
    // Ordinary insertion with auto_create_table establishes the target schema.
    client.insert_batch(&columns(&table)?)?;
    let outcome = write_all(&mut client, &table, FlightCompression::Zstd);
    let cleanup = client.drop_table(&table, false);
    outcome?;
    cleanup?;
    println!("Live bulk writes completed and table {table} was removed.");
    Ok(())
}
