//! Arrow Flight `DoPut` and `DoGet` against a real in-process HTTP/2 peer.
//!
//! Each scenario drives the public bulk writer or query stream while a
//! `SyncServer` on another thread inspects the wire contract: descriptors,
//! Arrow IPC schema metadata, per-batch metadata, and acknowledgements.

#[path = "support/timer.rs"]
mod timer;

use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, TimestampNanosecondType};
use greptimedb::{
    BatchEncode, ClientOptions, ColumnDataType, ColumnarBatch, DataType, Endpoint,
    EndpointTransport, Error, RecordBatch, Schema, SchemaRef, SemanticType, TimeUnit,
    TimestampUnit, ValueRef,
    bulk::{
        BulkStreamWriter, BulkWriteOptions, DoPutMetadata, DoPutResponse, FlightCompression,
        FlightIpcEncoder, batch_to_record_batch, schema::SEMANTIC_TYPE_KEY, timestamp_bounds,
    },
    query::{
        QueryStream, decode_flight_record_batch, decode_flight_schema,
        decode_record_batch_to_columnar_batch,
    },
};
use greptimedb_proto::{
    flight::{self, FlightData, FlightDescriptor, PutResult, Ticket},
    v1::{GreptimeRequest, greptime_request, query_request},
};
use grpc::{
    ProstCodec, Status,
    core::transport::MemoryPipe,
    server::SyncServer,
    stream::{RequestStream, ResponseSink, StreamError},
};
use prost::Message;

#[path = "support/signal.rs"]
mod signal;

const DO_PUT_PATH: &str = "/arrow.flight.protocol.FlightService/DoPut";
const DO_GET_PATH: &str = "/arrow.flight.protocol.FlightService/DoGet";
const TABLE: &str = "ticks";
const SYMBOL: &str = "BTCUSDT";
const QUERY: &str = "SELECT ts, symbol, price FROM ticks";
const BUDGET: Duration = Duration::from_secs(5);

// The ambiguity trick: a batch or stream that gained either auto trait would
// make these calls ambiguous and fail to compile.
const _: fn() = || {
    struct Marker;
    trait AmbiguousIfSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfSend<Marker> for T {}
    let _ = <BulkStreamWriter as AmbiguousIfSend<_>>::check;
    let _ = <QueryStream as AmbiguousIfSend<_>>::check;
    trait AmbiguousIfSync<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<Marker> for T {}
    let _ = <BulkStreamWriter as AmbiguousIfSync<_>>::check;
    let _ = <QueryStream as AmbiguousIfSync<_>>::check;
};

type Worker<T> = thread::JoinHandle<Result<T, Status>>;

fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => {
            Status::internal("blocking stream operation returned WouldBlock")
        }
    }
}

fn decode_status(error: Error) -> Status {
    Status::internal(error.to_string())
}

fn options() -> ClientOptions {
    let mut options = ClientOptions::from_endpoint(
        Endpoint::parse("grpc://127.0.0.1:4001").expect("valid endpoint"),
    );
    options.catalog = "research".into();
    options.schema = "ticks".into();
    options.request_timeout = BUDGET;
    options
}

fn channel(pipe: MemoryPipe) -> grpc::SyncChannel<EndpointTransport> {
    EndpointTransport::from_memory_pipe(pipe)
        .into_channel(grpc::ChannelConfig::default())
        .expect("memory channel")
}

fn columnar(first_ts: i64, rows: usize) -> ColumnarBatch {
    let mut batch = ColumnarBatch::with_capacity(TABLE, rows);
    batch
        .add_timestamp_column("ts", TimestampUnit::Nanosecond)
        .expect("timestamp column");
    batch
        .add_tag_column("symbol", ColumnDataType::String)
        .expect("tag column");
    batch
        .add_field_column("price", ColumnDataType::Float64)
        .expect("field column");
    for row in 0..rows {
        batch
            .push_row(&[
                ValueRef::TimestampNanosecond(first_ts + row as i64),
                ValueRef::String(SYMBOL),
                ValueRef::Float64(100.0 + row as f64),
            ])
            .expect("row values match the columns");
    }
    batch
}

fn record(first_ts: i64, rows: usize) -> RecordBatch {
    batch_to_record_batch(&columnar(first_ts, rows)).expect("columnar batch converts")
}

fn assert_record(batch: &RecordBatch, first_ts: i64, rows: usize) {
    assert_eq!(batch.num_rows(), rows);
    assert_eq!(batch.num_columns(), 3);
    let ts = batch.column(0).as_primitive::<TimestampNanosecondType>();
    let symbol = batch.column(1).as_string::<i32>();
    let price = batch.column(2).as_primitive::<Float64Type>();
    for row in 0..rows {
        assert_eq!(ts.value(row), first_ts + row as i64, "timestamp row {row}");
        assert_eq!(symbol.value(row), SYMBOL, "symbol row {row}");
        assert_eq!(price.value(row), 100.0 + row as f64, "price row {row}");
    }
}

fn assert_semantics(schema: &Schema) {
    let ts = schema.field_with_name("ts").expect("timestamp field");
    assert_eq!(
        ts.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert!(!ts.is_nullable(), "timestamp columns are non-null");
    assert_eq!(
        ts.metadata().get(SEMANTIC_TYPE_KEY).map(String::as_str),
        Some("timestamp")
    );
    let symbol = schema.field_with_name("symbol").expect("tag field");
    assert!(symbol.is_nullable());
    assert_eq!(
        symbol.metadata().get(SEMANTIC_TYPE_KEY).map(String::as_str),
        Some("tag")
    );
    let price = schema.field_with_name("price").expect("field column");
    assert!(price.is_nullable());
    assert_eq!(
        price.metadata().get(SEMANTIC_TYPE_KEY).map(String::as_str),
        Some("field")
    );
}

fn ack(request_id: i64, affected_rows: usize) -> PutResult {
    PutResult {
        app_metadata: sonic_rs::to_vec(&DoPutResponse {
            request_id,
            affected_rows,
        })
        .expect("acknowledgement metadata encodes"),
    }
}

fn frame(data_header: Vec<u8>, data_body: Vec<u8>) -> FlightData {
    FlightData {
        flight_descriptor: None,
        data_header,
        app_metadata: Vec::new(),
        data_body,
    }
}

fn decode_data(
    schema: &SchemaRef,
    frame: &FlightData,
) -> Result<(DoPutMetadata, RecordBatch), Status> {
    let metadata: DoPutMetadata = sonic_rs::from_slice(&frame.app_metadata)
        .map_err(|error| Status::internal(error.to_string()))?;
    let batch = decode_flight_record_batch(schema, &frame.data_header, &frame.data_body)
        .map_err(decode_status)?
        .ok_or_else(|| Status::internal("record-batch frame decoded to no batch"))?;
    Ok((metadata, batch))
}

fn receive(incoming: &mut RequestStream<FlightData>) -> Result<Option<FlightData>, Status> {
    incoming.recv_next().map_err(stream_status)
}

#[derive(Clone, Copy)]
enum PutMode {
    Happy,
    RejectAck,
    Bounded,
}

struct PutCapture {
    descriptor: FlightDescriptor,
    schema: Schema,
    frames: Vec<(DoPutMetadata, RecordBatch)>,
    acks: Vec<DoPutResponse>,
    max_unacked: usize,
}

fn serve_put(mode: PutMode) -> (MemoryPipe, Worker<PutCapture>) {
    let (client, peer) = MemoryPipe::pair();
    let worker = thread::spawn(move || {
        let capture = Rc::new(RefCell::new(None));
        let handler_capture = Rc::clone(&capture);
        let mut server = SyncServer::new(peer)?;
        server.register_bidi_streaming(
            DO_PUT_PATH,
            ProstCodec::<PutResult, FlightData>::new(),
            move |mut incoming, sink| run_put(mode, &mut incoming, sink, &handler_capture),
        )?;
        let served = server.serve_connection();
        drop(server);
        let captured = Rc::try_unwrap(capture)
            .ok()
            .and_then(|cell| cell.into_inner());
        match (served, captured) {
            (Err(status), _) => Err(status),
            (Ok(()), Some(captured)) => Ok(captured),
            (Ok(()), None) => Err(Status::internal("DoPut handler captured no stream")),
        }
    });
    (client, worker)
}

fn run_put(
    mode: PutMode,
    incoming: &mut RequestStream<FlightData>,
    sink: &mut ResponseSink<PutResult>,
    capture: &Rc<RefCell<Option<PutCapture>>>,
) -> Result<(), Status> {
    let first = receive(incoming)?
        .ok_or_else(|| Status::internal("DoPut closed before the schema frame"))?;
    let descriptor = first
        .flight_descriptor
        .ok_or_else(|| Status::internal("schema frame has no descriptor"))?;
    let schema = decode_flight_schema(&first.data_header).map_err(decode_status)?;
    let schema_metadata: DoPutMetadata = sonic_rs::from_slice(&first.app_metadata)
        .map_err(|error| Status::internal(error.to_string()))?;
    if schema_metadata.request_id != 0 {
        return Err(Status::internal("schema frame must use request_id 0"));
    }
    sink.send(&ack(0, 0)).map_err(stream_status)?;

    let mut frames = Vec::new();
    let mut acks = Vec::new();
    let mut max_unacked = 0;
    match mode {
        PutMode::Happy | PutMode::RejectAck => {
            while let Some(frame) = receive(incoming)? {
                let (metadata, batch) = decode_data(&schema, &frame)?;
                let request_id = match mode {
                    PutMode::RejectAck => metadata.request_id + 100,
                    _ => metadata.request_id,
                };
                let response = DoPutResponse {
                    request_id,
                    affected_rows: batch.num_rows(),
                };
                sink.send(&ack(response.request_id, response.affected_rows))
                    .map_err(stream_status)?;
                acks.push(response);
                frames.push((metadata, batch));
            }
        }
        PutMode::Bounded => {
            let mut pending = VecDeque::new();
            let mut unacked = 0usize;
            let mut eof = false;
            while !eof {
                match receive(incoming)? {
                    Some(frame) => {
                        pending.push_back(frame);
                        unacked += 1;
                        max_unacked = max_unacked.max(unacked);
                    }
                    None => break,
                }
                // A frame sent before this one is acknowledged would already be
                // queued here. Probe before acking so the observation is causal.
                loop {
                    match incoming.try_recv_next() {
                        Ok(Some(frame)) => {
                            pending.push_back(frame);
                            unacked += 1;
                            max_unacked = max_unacked.max(unacked);
                        }
                        Ok(None) => {
                            eof = true;
                            break;
                        }
                        Err(StreamError::WouldBlock) => break,
                        Err(error) => return Err(stream_status(error)),
                    }
                }
                while let Some(frame) = pending.pop_front() {
                    let (metadata, batch) = decode_data(&schema, &frame)?;
                    let response = DoPutResponse {
                        request_id: metadata.request_id,
                        affected_rows: batch.num_rows(),
                    };
                    sink.send(&ack(response.request_id, response.affected_rows))
                        .map_err(stream_status)?;
                    acks.push(response);
                    frames.push((metadata, batch));
                    unacked -= 1;
                }
            }
        }
    }
    *capture.borrow_mut() = Some(PutCapture {
        descriptor,
        schema: schema.as_ref().clone(),
        frames,
        acks,
        max_unacked,
    });
    Ok(())
}

struct GetCapture {
    request: GreptimeRequest,
}

fn serve_get(
    schema: Schema,
    batches: Vec<RecordBatch>,
    compression: FlightCompression,
    fail_after: Option<usize>,
) -> (MemoryPipe, Worker<GetCapture>) {
    let (client, peer) = MemoryPipe::pair();
    let worker = thread::spawn(move || {
        let capture = Rc::new(RefCell::new(None));
        let handler_capture = Rc::clone(&capture);
        let mut server = SyncServer::new(peer)?;
        server.register_server_streaming(
            DO_GET_PATH,
            ProstCodec::<FlightData, Ticket>::new(),
            move |request, sink| {
                run_get(
                    &schema,
                    &batches,
                    compression,
                    fail_after,
                    request,
                    sink,
                    &handler_capture,
                )
            },
        )?;
        let served = server.serve_connection();
        drop(server);
        let captured = Rc::try_unwrap(capture)
            .ok()
            .and_then(|cell| cell.into_inner());
        match (served, captured) {
            (Err(status), _) => Err(status),
            (Ok(()), Some(captured)) => Ok(captured),
            (Ok(()), None) => Err(Status::internal("DoGet handler captured no request")),
        }
    });
    (client, worker)
}

fn run_get(
    schema: &Schema,
    batches: &[RecordBatch],
    compression: FlightCompression,
    fail_after: Option<usize>,
    request: grpc::Request<Ticket>,
    sink: &mut ResponseSink<FlightData>,
    capture: &Rc<RefCell<Option<GetCapture>>>,
) -> Result<(), Status> {
    let (_, ticket) = request.into_parts();
    let request = GreptimeRequest::decode(ticket.ticket.as_slice())
        .map_err(|error| Status::internal(error.to_string()))?;
    *capture.borrow_mut() = Some(GetCapture { request });

    let mut encoder = FlightIpcEncoder::new(compression).map_err(decode_status)?;
    let encoded = encoder.encode_schema(schema).map_err(decode_status)?;
    sink.send(&frame(encoded.data_header, encoded.data_body))
        .map_err(stream_status)?;
    for (index, batch) in batches.iter().enumerate() {
        if fail_after == Some(index) {
            return Err(Status::internal("injected mid-stream DoGet failure"));
        }
        let encoded = encoder.encode_batch(batch).map_err(decode_status)?;
        sink.send(&frame(encoded.data_header, encoded.data_body))
            .map_err(stream_status)?;
    }
    Ok(())
}

fn assert_query(request: &GreptimeRequest) {
    assert!(request.header.is_some(), "request must carry a header");
    let Some(greptime_request::Request::Query(query)) = request.request.as_ref() else {
        panic!("ticket must carry a query request");
    };
    let Some(query_request::Query::Sql(sql)) = query.query.as_ref() else {
        panic!("query request must carry SQL");
    };
    assert_eq!(sql, QUERY);
}

fn join_within<T>(worker: Worker<T>, budget: Duration) -> Result<T, Status> {
    let deadline = Instant::now() + budget;
    while !worker.is_finished() {
        assert!(
            Instant::now() < deadline,
            "server thread did not exit within {budget:?}"
        );
        timer::pause(Duration::from_millis(5)).expect("fixture timer");
    }
    worker.join().expect("server thread must not panic")
}

#[test]
fn doput_sends_schema_then_batches_and_collects_acks() {
    let (pipe, worker) = serve_put(PutMode::Happy);
    let mut channel = channel(pipe);
    let first = record(1_000, 3);
    let stats = {
        let options = options();
        let mut writer = BulkStreamWriter::open(
            &mut channel,
            &options,
            TABLE,
            first.schema().as_ref(),
            BulkWriteOptions {
                max_in_flight: 4,
                ..BulkWriteOptions::default()
            },
        )
        .expect("bulk stream opens");
        writer
            .write_record_batch(&first)
            .expect("first record batch");
        writer
            .write_batch(&columnar(1_003, 2))
            .expect("second columnar batch");
        writer.finish().expect("bulk stream finishes")
    };
    assert_eq!(stats.batches_sent, 2);
    assert_eq!(stats.total_affected_rows, 5);

    drop(channel);
    let captured = join_within(worker, BUDGET).expect("server completes");

    assert_eq!(
        captured.descriptor.r#type,
        flight::flight_descriptor::DescriptorType::Path as i32
    );
    assert_eq!(captured.descriptor.path, [TABLE]);
    assert!(captured.descriptor.cmd.is_empty());
    assert_semantics(&captured.schema);
    assert_eq!(captured.frames.len(), 2);
    for (index, (metadata, batch)) in captured.frames.iter().enumerate() {
        assert_eq!(metadata.request_id, index as i64 + 1);
        let (min, max) = timestamp_bounds(batch).expect("timestamp bounds");
        assert_eq!((metadata.min_timestamp, metadata.max_timestamp), (min, max));
    }
    assert_record(&captured.frames[0].1, 1_000, 3);
    assert_record(&captured.frames[1].1, 1_003, 2);
    assert_eq!(
        captured
            .acks
            .iter()
            .map(|response| response.request_id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        captured
            .acks
            .iter()
            .map(|response| response.affected_rows)
            .collect::<Vec<_>>(),
        [3, 2]
    );
}

#[test]
fn doput_bounds_the_frames_in_flight() {
    let (pipe, worker) = serve_put(PutMode::Bounded);
    let mut channel = channel(pipe);
    let schema = record(0, 1);
    let stats = {
        let options = options();
        let mut writer = BulkStreamWriter::open(
            &mut channel,
            &options,
            TABLE,
            schema.schema().as_ref(),
            BulkWriteOptions {
                max_in_flight: 1,
                compression: FlightCompression::Lz4Frame,
                ..BulkWriteOptions::default()
            },
        )
        .expect("bulk stream opens");
        for step in 0..4 {
            writer
                .write_record_batch(&record(step * 100, 2))
                .expect("batches within the in-flight limit");
        }
        writer.finish().expect("bulk stream finishes")
    };
    assert_eq!(stats.batches_sent, 4);
    assert_eq!(stats.total_affected_rows, 8);

    drop(channel);
    let captured = join_within(worker, BUDGET).expect("server completes");
    assert_eq!(
        captured.max_unacked, 1,
        "client sent a second frame before the first acknowledgement"
    );
    assert_eq!(captured.frames.len(), 4);
    assert_eq!(
        captured
            .frames
            .iter()
            .map(|(metadata, _)| metadata.request_id)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
}

#[test]
fn doput_rejects_an_unexpected_request_id() {
    let (pipe, worker) = serve_put(PutMode::RejectAck);
    let mut channel = channel(pipe);
    let options = options();
    let first = record(0, 2);
    let mut writer = BulkStreamWriter::open(
        &mut channel,
        &options,
        TABLE,
        first.schema().as_ref(),
        BulkWriteOptions {
            max_in_flight: 1,
            ..BulkWriteOptions::default()
        },
    )
    .expect("bulk stream opens");
    writer.write_record_batch(&first).expect("first batch");
    let error = writer
        .write_record_batch(&record(2, 2))
        .expect_err("an unknown acknowledgement must fail the stream");
    assert!(
        matches!(error, Error::Protocol(_)),
        "expected a protocol error, got {error:?}"
    );

    drop(writer);
    drop(channel);
    let _served = join_within(worker, BUDGET);
}

#[test]
fn dropping_the_writer_cancels_the_put_stream() {
    let (client, peer) = MemoryPipe::pair();
    let (sender, receiver) = signal::channel();
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(peer)?;
        server.register_bidi_streaming(
            DO_PUT_PATH,
            ProstCodec::<PutResult, FlightData>::new(),
            move |mut incoming, sink| {
                let first = incoming
                    .recv_next()
                    .map_err(stream_status)?
                    .ok_or_else(|| Status::internal("DoPut closed before the schema frame"))?;
                let descriptor = first
                    .flight_descriptor
                    .ok_or_else(|| Status::internal("schema frame has no descriptor"))?;
                if descriptor.path != [TABLE] {
                    return Err(Status::invalid_argument("unexpected table path"));
                }
                sink.send(&ack(0, 0)).map_err(stream_status)?;
                let data = incoming
                    .recv_next()
                    .map_err(stream_status)?
                    .ok_or_else(|| Status::internal("DoPut closed before the data frame"))?;
                let _metadata: DoPutMetadata = sonic_rs::from_slice(&data.app_metadata)
                    .map_err(|error| Status::internal(error.to_string()))?;
                sender.send(1).expect("test receiver is alive");
                // Withhold the acknowledgement and wait for the client action.
                let observed = match incoming.recv_next() {
                    Ok(None) => 2,
                    Ok(Some(_)) => 3,
                    Err(StreamError::Status(_)) => 4,
                    Err(StreamError::WouldBlock) => 5,
                };
                sender.send(observed).expect("test receiver is alive");
                Ok(())
            },
        )?;
        server.serve_connection()
    });

    let mut channel = channel(client);
    let options = options();
    let first = record(0, 2);
    let mut writer = BulkStreamWriter::open(
        &mut channel,
        &options,
        TABLE,
        first.schema().as_ref(),
        BulkWriteOptions {
            max_in_flight: 4,
            ..BulkWriteOptions::default()
        },
    )
    .expect("bulk stream opens");
    writer.write_record_batch(&first).expect("first batch");
    assert_eq!(
        receiver
            .recv_timeout(BUDGET)
            .expect("server must receive the batch"),
        1
    );
    drop(writer);
    assert_eq!(
        receiver
            .recv_timeout(BUDGET)
            .expect("server must observe the cancellation"),
        4
    );

    drop(channel);
    let _served = join_within(worker, BUDGET);
}

#[test]
fn doget_streams_schema_and_batches_from_a_sql_ticket() {
    let first = record(500, 4);
    let second = record(600, 3);
    let (pipe, worker) = serve_get(
        first.schema().as_ref().clone(),
        vec![first, second],
        FlightCompression::Zstd,
        None,
    );
    let mut channel = channel(pipe);
    let options = options();
    let mut stream = QueryStream::sql(&mut channel, &options, QUERY).expect("query opens");
    let head = stream
        .next_batch()
        .expect("first batch")
        .expect("a batch before EOF");
    assert_record(&head, 500, 4);
    let schema = stream.schema().expect("schema frame decoded").clone();
    assert_semantics(schema.as_ref());
    let tail = stream.collect_batches().expect("remaining batches");
    assert_eq!(tail.len(), 1);
    assert_record(&tail[0], 600, 3);

    let columnar =
        decode_record_batch_to_columnar_batch(&head).expect("batch decodes into columnar storage");
    assert_eq!(columnar.row_count(), 4);
    assert_eq!(
        columnar
            .column(0)
            .expect("timestamp column")
            .semantic_type(),
        SemanticType::Timestamp
    );
    assert_eq!(
        columnar.column(1).expect("tag column").data_type(),
        ColumnDataType::String
    );

    drop(channel);
    let captured = join_within(worker, BUDGET).expect("server completes");
    assert_query(&captured.request);
}

#[test]
fn doget_surfaces_a_mid_stream_status() {
    let first = record(0, 2);
    let second = record(2, 2);
    let (pipe, worker) = serve_get(
        first.schema().as_ref().clone(),
        vec![first, second],
        FlightCompression::None,
        Some(1),
    );
    let mut channel = channel(pipe);
    let options = options();
    let mut stream = QueryStream::sql(&mut channel, &options, QUERY).expect("query opens");
    let head = stream
        .next_batch()
        .expect("batch before the failure")
        .expect("a batch before the injected status");
    assert_record(&head, 0, 2);
    let error = stream
        .next_batch()
        .expect_err("the mid-stream status must surface");
    assert!(
        matches!(
            error,
            Error::Grpc(ref status) if status.code() == grpc::core::Code::Internal
        ),
        "expected the injected internal status, got {error:?}"
    );

    drop(stream);
    drop(channel);
    let _captured = join_within(worker, BUDGET).expect("server completes");
}
