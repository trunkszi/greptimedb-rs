//! Synchronous L1 capture, compressed Flight ingestion, and typed analytical replay.
//!
//! Run offline with `cargo run -p greptimedb --example 10_market_data_pipeline`.
//! A real HTTP/2 MemoryPipe peer stores the received Arrow batches for DoGet replay.
//! Set GREPTIMEDB_ENDPOINT to also write to an existing `l1_book_ticks` live table
//! with matching columns, then SELECT it back. Source rows and batch views borrow
//! data; staging copies strings/bytes into warmed reusable slots. Arrow IPC,
//! transport, and typed replay can allocate.

use std::{
    cell::RefCell,
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow, ensure};
use greptimedb::{
    BufferPolicy, Client, ClientOptions, Endpoint, EndpointTransport, Row,
    bulk::{
        BulkStreamWriter, BulkWriteOptions, DoPutMetadata, DoPutResponse, FlightCompression,
        FlightIpcEncoder, RecordBatch, batch_to_record_batch,
    },
    query::{QueryStream, decode_flight_record_batch, decode_flight_schema},
};
use greptimedb_proto::{
    flight::{FlightData, PutResult, Ticket},
    v1::{GreptimeRequest, greptime_request, query_request},
};
use grpc::{ProstCodec, Status, StreamError, core::transport::MemoryPipe, server::SyncServer};
use prost::Message;

const TABLE: &str = "l1_book_ticks";
const SQL: &str = "SELECT symbol, venue, ts_ns, bid_px, ask_px, bid_sz, ask_sz, sequence, payload FROM l1_book_ticks WHERE venue = 'example' AND symbol = 'BTCUSDT' ORDER BY ts_ns";
const ROWS: usize = 256;
const HOUR: i64 = 3_600_000_000_000;
const PAYLOAD: &[u8] = b"L1\0\xff";

#[derive(Row)]
#[row(table = "l1_book_ticks")]
struct L1BookTick<'a> {
    #[greptime(tag)]
    symbol: &'a str,
    #[greptime(tag)]
    venue: &'a str,
    #[greptime(timestamp, unit = "ns")]
    ts_ns: i64,
    bid_px: f64,
    ask_px: f64,
    bid_sz: f64,
    ask_sz: f64,
    sequence: u64,
    payload: &'a [u8],
}

fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking Flight operation returned pending"),
    }
}

fn server_error(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn frame(encoded: greptimedb::bulk::EncodedFlightBody) -> FlightData {
    FlightData {
        flight_descriptor: None,
        data_header: encoded.data_header,
        data_body: encoded.data_body,
        app_metadata: Vec::new(),
    }
}

// This is a bounded demonstration store, not a database or exchange feed simulator.
fn offline_server(peer: MemoryPipe) -> Result<()> {
    let stored = Rc::new(RefCell::new(Vec::<RecordBatch>::new()));
    let put_store = Rc::clone(&stored);
    let mut server = SyncServer::new(peer)?;
    server.register_bidi_streaming(
        "/arrow.flight.protocol.FlightService/DoPut",
        ProstCodec::<PutResult, FlightData>::new(),
        move |mut source, sink| {
            let first = source
                .recv_next()
                .map_err(stream_status)?
                .ok_or_else(|| Status::invalid_argument("missing schema"))?;
            let descriptor = first
                .flight_descriptor
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing table descriptor"))?;
            if descriptor.path != [TABLE] {
                return Err(Status::invalid_argument("wrong table"));
            }
            let schema = decode_flight_schema(&first.data_header).map_err(server_error)?;
            let header: DoPutMetadata =
                sonic_rs::from_slice(&first.app_metadata).map_err(server_error)?;
            if header.request_id != 0 {
                return Err(Status::invalid_argument("schema request id must be zero"));
            }
            sink.send(&PutResult {
                app_metadata: sonic_rs::to_vec(&DoPutResponse {
                    request_id: 0,
                    affected_rows: 0,
                })
                .map_err(server_error)?,
            })
            .map_err(stream_status)?;
            let mut expected_id = 1;
            while let Some(data) = source.recv_next().map_err(stream_status)? {
                let metadata: DoPutMetadata =
                    sonic_rs::from_slice(&data.app_metadata).map_err(server_error)?;
                if metadata.request_id != expected_id {
                    return Err(Status::invalid_argument("non-monotonic batch request id"));
                }
                let batch = decode_flight_record_batch(&schema, &data.data_header, &data.data_body)
                    .map_err(server_error)?
                    .ok_or_else(|| Status::internal("missing batch"))?;
                let rows = batch.num_rows();
                put_store.borrow_mut().push(batch);
                sink.send(&PutResult {
                    app_metadata: sonic_rs::to_vec(&DoPutResponse {
                        request_id: metadata.request_id,
                        affected_rows: rows,
                    })
                    .map_err(server_error)?,
                })
                .map_err(stream_status)?;
                expected_id += 1;
            }
            Ok(())
        },
    )?;
    server.register_server_streaming(
        "/arrow.flight.protocol.FlightService/DoGet",
        ProstCodec::<FlightData, Ticket>::new(),
        move |request, sink| {
            let request = GreptimeRequest::decode(request.into_parts().1.ticket.as_slice())
                .map_err(server_error)?;
            match request.request {
                Some(greptime_request::Request::Query(query)) => match query.query {
                    Some(query_request::Query::Sql(sql)) if sql == SQL => {}
                    _ => return Err(Status::invalid_argument("unexpected SQL")),
                },
                _ => return Err(Status::invalid_argument("expected query ticket")),
            }
            let batches = stored.borrow();
            let first = batches
                .first()
                .ok_or_else(|| Status::internal("no ingested data"))?;
            let mut encoder =
                FlightIpcEncoder::new(FlightCompression::Lz4Frame).map_err(server_error)?;
            sink.send(&frame(
                encoder
                    .encode_schema(first.schema().as_ref())
                    .map_err(server_error)?,
            ))
            .map_err(stream_status)?;
            for batch in batches.iter() {
                sink.send(&frame(encoder.encode_batch(batch).map_err(server_error)?))
                    .map_err(stream_status)?;
            }
            Ok(())
        },
    )?;
    server.serve_connection()?;
    Ok(())
}

fn fill(batch: &mut L1BookTickBatch) {
    for index in 0..ROWS {
        batch.push(&L1BookTick {
            symbol: "BTCUSDT",
            venue: "example",
            ts_ns: HOUR - 128 + index as i64,
            bid_px: 100.0,
            ask_px: 100.5,
            bid_sz: 2.0,
            ask_sz: 3.0,
            sequence: index as u64,
            payload: PAYLOAD,
        });
    }
}

fn spread(view: L1BookTickView<'_>) -> f64 {
    view.ask_px - view.bid_px
}

fn pipeline(
    channel: &mut grpc::SyncChannel<EndpointTransport>,
    options: &ClientOptions,
    offline: bool,
) -> Result<()> {
    let mut batch = L1BookTickBatch::with_policy(ROWS, BufferPolicy::default());
    // Warm reusable value slots before timing the capture loop.
    fill(&mut batch);
    batch.clear_with_policy(BufferPolicy::default());
    let started = Instant::now();
    fill(&mut batch);
    let capture_time = started.elapsed();
    let average_spread = batch.iter_views().map(spread).sum::<f64>() / batch.len() as f64;
    ensure!(average_spread == 0.5, "borrowed spread calculation failed");
    let encode_start = Instant::now();
    let record = batch_to_record_batch(&batch)?;
    let conversion_time = encode_start.elapsed();
    let put_start = Instant::now();
    let mut writer = BulkStreamWriter::open(
        channel,
        options,
        TABLE,
        record.schema().as_ref(),
        BulkWriteOptions {
            max_in_flight: 2,
            compression: FlightCompression::Lz4Frame,
            partition_by_time_window: true,
            ..BulkWriteOptions::default()
        },
    )?;
    writer.write_record_batch(&record)?;
    let stats = writer.finish()?;
    let put_time = put_start.elapsed();
    ensure!(
        stats.total_affected_rows == ROWS && stats.batches_sent == 2,
        "hour partition/ACK mismatch: {stats:?}"
    );
    // Clear only after the server acknowledges every row. Arrow owns its copy.
    batch.clear_with_policy(BufferPolicy::default());
    ensure!(batch.is_empty(), "capture buffer was not cleared");
    let replay_start = Instant::now();
    let mut query = QueryStream::sql(channel, options, SQL)?;
    let mut count = 0;
    while let Some(record) = query.next_batch()? {
        let replay = L1BookTickBatch::from_record_batch(&record)?;
        for view in replay.iter_views() {
            ensure!(
                view.symbol == "BTCUSDT" && view.venue == "example",
                "identity mismatch"
            );
            ensure!(
                view.bid_px == 100.0
                    && view.ask_px == 100.5
                    && spread(view) == 0.5
                    && view.bid_sz == 2.0
                    && view.ask_sz == 3.0
                    && view.payload == PAYLOAD,
                "value mismatch"
            );
            if offline {
                ensure!(
                    view.sequence == count as u64 && view.ts_ns == HOUR - 128 + count as i64,
                    "replay ordering mismatch"
                );
            }
            count += 1;
        }
    }
    if offline {
        ensure!(count == ROWS, "replayed {count} instead of {ROWS}");
    }
    println!(
        "{}: capture={capture_time:?}, Arrow conversion={conversion_time:?}, DoPut including open/ACK={put_time:?} ({:.0} rows/s), typed DoGet replay={:?}, rows={count}, spread={average_spread}",
        if offline { "MemoryPipe" } else { "live" },
        ROWS as f64 / put_time.as_secs_f64(),
        replay_start.elapsed()
    );
    Ok(())
}

fn main() -> Result<()> {
    let (pipe, peer) = MemoryPipe::pair();
    let server = thread::spawn(move || offline_server(peer).map_err(|error| error.to_string()));
    let mut channel =
        EndpointTransport::from_memory_pipe(pipe).into_channel(grpc::ChannelConfig::default())?;
    let mut options = ClientOptions::from_endpoint(Endpoint::parse("grpc://127.0.0.1:4001")?);
    options.request_timeout = Duration::from_secs(5);
    let result = pipeline(&mut channel, &options, true);
    drop(channel);
    server
        .join()
        .map_err(|_| anyhow!("Flight server thread panicked"))?
        .map_err(anyhow::Error::msg)?;
    result?;
    if let Ok(endpoint) = std::env::var("GREPTIMEDB_ENDPOINT") {
        let options = ClientOptions::from_endpoint(Endpoint::parse(&endpoint)?);
        let mut client = Client::connect(options.clone())?;
        pipeline(client.channel_mut()?, &options, false)?;
    }
    Ok(())
}
