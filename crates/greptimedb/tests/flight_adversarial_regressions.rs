//! Integration scenarios for Arrow Flight ingestion and query failure boundaries.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use arrow_array::{
    Array, DictionaryArray, Int32Array, RecordBatch, StringArray, TimestampMillisecondArray,
    TimestampNanosecondArray, types::Int32Type,
};
use arrow_ipc::writer::{
    DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use greptimedb::{
    BatchEncode, BorrowedRow, ClientOptions, ColumnDataType, ColumnarBatch, Endpoint,
    EndpointTransport, Error, Row, TimestampUnit, ValueRef,
    bulk::{
        BulkStreamWriter, BulkWriteOptions, DoPutMetadata, DoPutResponse, FlightCompression,
        FlightIpcEncoder, batch_to_record_batch, partition_record_batch_by_hour,
    },
    query::{QueryStream, decode_record_batch_to_columnar_batch},
};
use greptimedb_proto::flight::{FlightData, PutResult, Ticket};
use grpc::{
    Metadata, ProstCodec, Status, StreamError, SyncChannel,
    core::transport::MemoryPipe,
    server::SyncServer,
    stream::{RequestStream, ResponseSink},
};

#[path = "support/terminal_gate.rs"]
mod terminal_gate;
use terminal_gate::TerminalGate;

type TestResult = anyhow::Result<()>;
const PUT: &str = "/arrow.flight.protocol.FlightService/DoPut";
const GET: &str = "/arrow.flight.protocol.FlightService/DoGet";

fn status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("unexpected pending blocking operation"),
    }
}

fn options() -> ClientOptions {
    let mut options =
        ClientOptions::from_endpoint(Endpoint::parse("localhost:4001").expect("endpoint"));
    options.request_timeout = Duration::from_secs(3);
    options
}

struct Fixture {
    channel: Option<SyncChannel<EndpointTransport>>,
    stopping: Arc<AtomicBool>,
    terminal_observed: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<Result<(), Status>>>,
}

impl Fixture {
    fn new(
        handler: impl FnOnce(&mut SyncServer<TerminalGate>) -> Result<(), Status> + Send + 'static,
    ) -> anyhow::Result<Self> {
        Self::controlled(false, handler)
    }

    fn controlled(
        gate_terminal: bool,
        handler: impl FnOnce(&mut SyncServer<TerminalGate>) -> Result<(), Status> + Send + 'static,
    ) -> anyhow::Result<Self> {
        let (client, peer) = MemoryPipe::with_capacity(4096)?;
        let stopping = Arc::new(AtomicBool::new(false));
        let terminal_observed = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stopping);
        let observed = Arc::clone(&terminal_observed);
        let worker = thread::spawn(move || {
            let mut server = SyncServer::new(TerminalGate::new(
                peer,
                Arc::clone(&stop),
                observed,
                gate_terminal,
            ))?;
            handler(&mut server)?;
            while !stop.load(Ordering::Acquire) {
                if !server.poll_once()? {
                    thread::yield_now();
                }
            }
            server.shutdown_with_goaway(0)?;
            server.serve_connection()
        });
        let channel =
            EndpointTransport::from_memory_pipe(client).into_channel(grpc::ChannelConfig {
                default_timeout: Some(Duration::from_secs(3)),
                ..grpc::ChannelConfig::default()
            })?;
        Ok(Self {
            channel: Some(channel),
            stopping,
            terminal_observed,
            worker: Some(worker),
        })
    }

    fn channel(&mut self) -> &mut SyncChannel<EndpointTransport> {
        self.channel.as_mut().expect("fixture channel")
    }

    fn query(&mut self) -> greptimedb::Result<QueryStream> {
        QueryStream::sql(self.channel(), &options(), "SELECT value FROM ticks")
    }

    fn finish(mut self) -> TestResult {
        // The peer may still flush RST_STREAM after publishing error trailers.
        // Keep its reader alive until graceful server draining has completed.
        self.stopping.store(true, Ordering::Release);
        self.worker
            .take()
            .expect("server worker")
            .join()
            .map_err(|_| anyhow::anyhow!("server panicked"))??;
        drop(self.channel.take());
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.channel.take());
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            drop(worker.join());
        }
    }
}

fn flight(encoded: EncodedData) -> FlightData {
    FlightData {
        data_header: encoded.ipc_message,
        data_body: encoded.arrow_data,
        ..FlightData::default()
    }
}

fn frames(batch: &RecordBatch) -> anyhow::Result<Vec<FlightData>> {
    let generator = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    let schema = generator.schema_to_bytes_with_dictionary_tracker(
        batch.schema().as_ref(),
        &mut tracker,
        &options,
    );
    let (dictionaries, batch) = generator.encode(
        batch,
        &mut tracker,
        &options,
        &mut IpcWriteContext::default(),
    )?;
    let mut frames = vec![flight(schema)];
    frames.extend(dictionaries.into_iter().map(flight));
    frames.push(flight(batch));
    Ok(frames)
}

fn get_fixture(frames: Vec<FlightData>, trailers: Metadata) -> anyhow::Result<Fixture> {
    Fixture::new(move |server| {
        server.register_server_streaming(
            GET,
            ProstCodec::<FlightData, Ticket>::new(),
            move |_, sink| {
                for frame in &frames {
                    sink.send(frame).map_err(status)?;
                }
                sink.finish(Status::ok(""), trailers.clone())
                    .map_err(status)
            },
        )
    })
}

fn clean_trailers() -> Metadata {
    let mut trailers = Metadata::new();
    trailers
        .append_ascii("x-greptime-err-code", "0")
        .expect("code metadata");
    trailers
        .append_ascii("x-greptime-err-retry-hint", "false")
        .expect("hint metadata");
    trailers
}

fn error_trailers() -> Metadata {
    let mut trailers = Metadata::new();
    trailers
        .append_ascii(
            "inner_error_msg",
            r#"{"code":4003,"err_msg":"region busy"}"#,
        )
        .expect("diagnostic");
    trailers
        .append_ascii("x-greptime-err-retry-hint", "true")
        .expect("retry hint");
    trailers
}

fn projection() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![7, 11]))],
    )
    .expect("projection")
}

#[test]
fn integration_live_time_index_and_timestamp_free_projection_decode() -> TestResult {
    let live = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )
            .with_metadata(HashMap::from([(
                "greptime:time_index".into(),
                "true".into(),
            )])),
            Field::new("value", DataType::Int32, false),
        ])),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![100, 200])),
            Arc::new(Int32Array::from(vec![7, 11])),
        ],
    )?;
    for batch in [live, projection()] {
        let mut fixture = get_fixture(frames(&batch)?, clean_trailers())?;
        let mut query = fixture.query()?;
        let received = query.next_batch()?.expect("record batch");
        let decoded = decode_record_batch_to_columnar_batch(&received)?;
        assert_eq!(decoded.columns()[0].row_count(), 2);
        assert_eq!(decoded.columns().len(), batch.num_columns());
        if batch.num_columns() == 1 {
            assert!(decoded.encode_insert_request().is_err());
        }
        assert!(query.next_batch()?.is_none());
        drop(query);
        fixture.finish()?;
    }
    Ok(())
}

#[test]
fn integration_dictionary_ipc_stream_decodes_null_keys_and_values() -> TestResult {
    let values = Arc::new(StringArray::from(vec![Some("BTC"), None, Some("ETH")]));
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(2), None, Some(0), Some(1)]),
        values,
    )?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "symbol",
            dictionary.data_type().clone(),
            true,
        )])),
        vec![Arc::new(dictionary)],
    )?;
    let encoded = frames(&batch)?;
    assert_eq!(encoded.len(), 3, "schema, dictionary, record batch");
    let mut fixture = get_fixture(encoded, Metadata::new())?;
    let mut query = fixture.query()?;
    let decoded = decode_record_batch_to_columnar_batch(&query.next_batch()?.expect("record"))?;
    let column = &decoded.columns()[0];
    assert_eq!(column.row_count(), 4);
    let wire = column.to_proto_column();
    assert_eq!(wire.null_mask, vec![0b1010]);
    assert_eq!(
        wire.values.expect("string values").string_values,
        vec!["ETH", "BTC"]
    );
    assert!(query.next_batch()?.is_none());
    drop(query);
    fixture.finish()
}

#[test]
fn integration_nullable_timestamp_field_and_minimum_hour_partition() -> TestResult {
    let mut batch = ColumnarBatch::new("ticks");
    batch.add_timestamp_column("ts", TimestampUnit::Nanosecond)?;
    batch.add_field_column("event_time", ColumnDataType::TimestampMillisecond)?;
    batch.push_row(&[ValueRef::TimestampNanosecond(i64::MIN), ValueRef::Null])?;
    batch.push_row(&[
        ValueRef::TimestampNanosecond(0),
        ValueRef::TimestampMillisecond(17),
    ])?;
    let record = batch_to_record_batch(&batch)?;
    assert!(record.column(1).is_null(0));
    let partitions = partition_record_batch_by_hour(&record)?;
    assert_eq!(partitions.len(), 2);
    assert!(partitions[0].column(1).is_null(0));
    let decoded = decode_record_batch_to_columnar_batch(&partitions[0])?;
    assert_eq!(decoded.columns()[1].to_proto_column().null_mask, vec![1]);
    assert_eq!(
        partitions[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("nanosecond")
            .value(0),
        i64::MIN
    );
    Ok(())
}

#[test]
fn integration_dictionary_rejection_does_not_poison_encoder() -> TestResult {
    let dict = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0]),
        Arc::new(StringArray::from(vec!["BTC"])),
    )?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "symbol",
            dict.data_type().clone(),
            false,
        )])),
        vec![Arc::new(dict)],
    )?;
    let mut encoder = FlightIpcEncoder::new(FlightCompression::None)?;
    for _ in 0..2 {
        assert!(matches!(
            encoder.encode_schema(batch.schema().as_ref()),
            Err(Error::Encode(_))
        ));
        assert!(matches!(
            encoder.encode_batch(&batch),
            Err(Error::Encode(_))
        ));
    }
    let record = projection();
    encoder.encode_schema(record.schema().as_ref())?;
    assert!(!encoder.encode_batch(&record)?.data_header.is_empty());
    Ok(())
}

#[derive(Debug, Row)]
#[row(table = "ticks")]
struct Tick {
    #[greptime(timestamp)]
    ts: i64,
    price: f64,
}

fn ack(id: i64, rows: usize) -> PutResult {
    PutResult {
        app_metadata: sonic_rs::to_vec(&DoPutResponse {
            request_id: id,
            affected_rows: rows,
        })
        .expect("ack metadata"),
    }
}

#[derive(Clone, Copy)]
enum PutMode {
    Clean,
    Unknown,
    Overflow,
    RemoteError,
    Premature,
}

fn put_fixture(mode: PutMode) -> anyhow::Result<Fixture> {
    Fixture::controlled(
        matches!(mode, PutMode::RemoteError | PutMode::Premature),
        move |server| {
            server.register_bidi_streaming(
                PUT,
                ProstCodec::<PutResult, FlightData>::new(),
                move |mut incoming: RequestStream<FlightData>,
                      sink: &mut ResponseSink<PutResult>| {
                    incoming
                        .recv_next()
                        .map_err(status)?
                        .ok_or_else(|| Status::internal("missing schema"))?;
                    sink.send(&ack(0, 0)).map_err(status)?;
                    let mut count = 0;
                    while let Some(frame) = incoming.recv_next().map_err(status)? {
                        let metadata: DoPutMetadata = sonic_rs::from_slice(&frame.app_metadata)
                            .map_err(|e| Status::internal(e.to_string()))?;
                        count += 1;
                        match mode {
                            PutMode::Clean => {
                                sink.send(&ack(metadata.request_id, 1)).map_err(status)?
                            }
                            PutMode::Unknown => sink
                                .send(&ack(metadata.request_id + 100, 1))
                                .map_err(status)?,
                            PutMode::Overflow => sink
                                .send(&ack(
                                    metadata.request_id,
                                    if count == 1 { usize::MAX } else { 1 },
                                ))
                                .map_err(status)?,
                            PutMode::RemoteError => {
                                return sink
                                    .finish(Status::unavailable("retry later"), error_trailers())
                                    .map_err(status);
                            }
                            PutMode::Premature => {
                                return sink
                                    .finish(Status::ok(""), error_trailers())
                                    .map_err(status);
                            }
                        }
                    }
                    sink.finish(Status::ok(""), clean_trailers())
                        .map_err(status)
                },
            )
        },
    )
}

fn typed_batch() -> TickBatch {
    let mut batch = Tick::new_batch_with_capacity(1);
    TickRef { ts: 1, price: 12.0 }.push_into_batch(&mut batch);
    batch
}

fn open_writer(fixture: &mut Fixture, schema: &Schema) -> greptimedb::Result<BulkStreamWriter> {
    BulkStreamWriter::open(
        fixture.channel(),
        &options(),
        "ticks",
        schema,
        BulkWriteOptions {
            max_in_flight: 1,
            ..BulkWriteOptions::default()
        },
    )
}

#[test]
fn integration_generated_batch_and_unannotated_arrow_bulk_writes() -> TestResult {
    let batch = typed_batch();
    let record = batch_to_record_batch(&batch)?;
    let mut fixture = put_fixture(PutMode::Clean)?;
    let mut writer = open_writer(&mut fixture, record.schema().as_ref())?;
    let erased: &dyn BatchEncode = &batch;
    writer.write_batch(erased)?;
    assert_eq!(writer.finish()?.total_affected_rows, 1);
    fixture.finish()?;
    let record = projection();
    let mut fixture = put_fixture(PutMode::Clean)?;
    let mut writer = open_writer(&mut fixture, record.schema().as_ref())?;
    writer.write_record_batch(&record)?;
    assert_eq!(writer.finish()?.batches_sent, 1);
    fixture.finish()
}

#[test]
fn integration_bad_ack_latches_and_finish_cannot_report_success() -> TestResult {
    for mode in [PutMode::Unknown, PutMode::Overflow] {
        let record = batch_to_record_batch(&typed_batch())?;
        let mut fixture = put_fixture(mode)?;
        let mut writer = open_writer(&mut fixture, record.schema().as_ref())?;
        writer.write_record_batch(&record)?;
        if matches!(mode, PutMode::Overflow) {
            writer.write_record_batch(&record)?;
        }
        let error = writer
            .write_record_batch(&record)
            .expect_err("invalid ACK is terminal");
        assert!(matches!(error, Error::Protocol(_)));
        let text = error.to_string();
        assert_eq!(
            writer
                .write_record_batch(&record)
                .expect_err("sticky error")
                .to_string(),
            text
        );
        assert_eq!(
            writer
                .finish()
                .expect_err("finish must preserve failure")
                .to_string(),
            text
        );
        fixture.finish()?;
    }
    Ok(())
}

fn assert_region_error(error: Error) {
    assert!(
        matches!(error, Error::ServerStatus { raw_code: 4003, retry_hint: true, ref message, .. } if message == "region busy")
    );
}

#[test]
fn integration_bulk_structured_failure_is_preserved_until_finish() -> TestResult {
    for mode in [PutMode::RemoteError, PutMode::Premature] {
        let record = batch_to_record_batch(&typed_batch())?;
        let mut fixture = put_fixture(mode)?;
        let mut writer = open_writer(&mut fixture, record.schema().as_ref())?;
        writer.write_record_batch(&record)?;
        assert_region_error(
            writer
                .write_record_batch(&record)
                .expect_err("server rejection"),
        );
        assert_region_error(writer.finish().expect_err("latched server rejection"));
        assert!(
            fixture.terminal_observed.load(Ordering::Acquire),
            "terminal headers reached the controlled teardown gate"
        );
        fixture.finish()?;
    }
    Ok(())
}

#[test]
fn integration_doget_rejects_invalid_frame_sequences() -> TestResult {
    let encoded = frames(&projection())?;
    let mut mismatch = encoded[1].clone();
    mismatch.data_body.push(0);
    // A valid Message header with an unsupported header discriminator (Tensor=4).
    let mut unsupported = encoded[0].clone();
    let root = u32::from_le_bytes(unsupported.data_header[0..4].try_into()?) as usize;
    let back = i32::from_le_bytes(unsupported.data_header[root..root + 4].try_into()?);
    let vtable = (root as i64 - i64::from(back)) as usize;
    let field =
        u16::from_le_bytes(unsupported.data_header[vtable + 6..vtable + 8].try_into()?) as usize;
    unsupported.data_header[root + field] = 4;
    for frames in [
        vec![encoded[0].clone(), encoded[0].clone()],
        vec![encoded[1].clone()],
        vec![encoded[0].clone(), mismatch],
        vec![unsupported],
    ] {
        let mut fixture = get_fixture(frames, Metadata::new())?;
        let mut query = fixture.query()?;
        assert!(query.next_batch().is_err());
        assert!(query.next_batch().is_err(), "failed query stays failed");
        drop(query);
        fixture.finish()?;
    }
    Ok(())
}

#[test]
fn integration_clean_eof_metadata_does_not_fabricate_server_error() -> TestResult {
    let mut hint_only = Metadata::new();
    hint_only.append_ascii("x-greptime-err-retry-hint", "false")?;
    for trailers in [Metadata::new(), clean_trailers(), hint_only] {
        let mut fixture = get_fixture(Vec::new(), trailers)?;
        let query = fixture.query()?;
        let (_, metrics) = query.finish()?;
        assert!(metrics.is_empty());
        fixture.finish()?;
    }
    Ok(())
}

#[test]
fn integration_dictionary_keys_cover_all_integer_widths() -> TestResult {
    macro_rules! verify {
        ($key:ty, $array:ty) => {{
            let keys = <$array>::from(vec![Some(1), None, Some(0)]);
            let dictionary =
                DictionaryArray::<$key>::try_new(keys, Arc::new(Int32Array::from(vec![17, 29])))?;
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    dictionary.data_type().clone(),
                    true,
                )])),
                vec![Arc::new(dictionary)],
            )?;
            let decoded = decode_record_batch_to_columnar_batch(&batch)?;
            let column = decoded.columns()[0].to_proto_column();
            assert_eq!(column.null_mask, vec![2]);
            assert_eq!(
                column.values.expect("integer values").i32_values,
                vec![29, 17]
            );
        }};
    }
    verify!(arrow_array::types::Int8Type, arrow_array::Int8Array);
    verify!(arrow_array::types::Int16Type, arrow_array::Int16Array);
    verify!(arrow_array::types::Int32Type, arrow_array::Int32Array);
    verify!(arrow_array::types::Int64Type, arrow_array::Int64Array);
    verify!(arrow_array::types::UInt8Type, arrow_array::UInt8Array);
    verify!(arrow_array::types::UInt16Type, arrow_array::UInt16Array);
    verify!(arrow_array::types::UInt32Type, arrow_array::UInt32Array);
    verify!(arrow_array::types::UInt64Type, arrow_array::UInt64Array);
    Ok(())
}

#[test]
fn integration_arrow_views_and_time_index_validation() -> TestResult {
    let text = arrow_array::StringViewArray::from(vec![Some("BTC"), None]);
    let binary = arrow_array::BinaryViewArray::from(vec![Some(&b"{}"[..]), None]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8View, true),
            Field::new("document", DataType::BinaryView, true)
                .with_metadata(HashMap::from([("greptime:type".into(), "Json".into())])),
        ])),
        vec![Arc::new(text), Arc::new(binary)],
    )?;
    let decoded = decode_record_batch_to_columnar_batch(&batch)?;
    assert_eq!(decoded.columns()[0].data_type(), ColumnDataType::String);
    assert_eq!(decoded.columns()[1].data_type(), ColumnDataType::Json);
    assert_eq!(decoded.columns()[0].to_proto_column().null_mask, vec![2]);
    for field in [
        Field::new("ts", DataType::Int32, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![field.with_metadata(HashMap::from([(
            "greptime:time_index".into(),
            "true".into(),
        )]))]));
        let empty = RecordBatch::new_empty(schema);
        assert!(matches!(
            decode_record_batch_to_columnar_batch(&empty),
            Err(Error::SchemaViolation(_))
        ));
    }
    Ok(())
}

#[test]
fn integration_negative_decimal_scale_is_rejected() -> TestResult {
    let array =
        arrow_array::Decimal128Array::from(vec![123_i128]).with_precision_and_scale(10, -2)?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, -2),
            false,
        )])),
        vec![Arc::new(array)],
    )?;
    assert!(matches!(
        decode_record_batch_to_columnar_batch(&batch),
        Err(Error::SchemaViolation(_))
    ));
    let mut encoder = FlightIpcEncoder::new(FlightCompression::None)?;
    encoder.encode_batch(&batch)?;
    assert!(partition_record_batch_by_hour(&batch).is_err());
    Ok(())
}

#[test]
fn integration_finish_returns_dml_affected_rows_and_metrics() -> TestResult {
    use greptimedb_proto::v1::{AffectedRows, FlightMetadata, Metrics};
    use prost::Message;
    let metadata = FlightMetadata {
        affected_rows: Some(AffectedRows { value: 0 }),
        metrics: Some(Metrics {
            metrics: b"observed query metrics".to_vec(),
        }),
    };
    let frame = FlightData {
        data_header: vec![12, 0, 0, 0, 6, 0, 8, 0, 6, 0, 0, 0, 8, 0, 0, 0, 0, 0, 4, 0],
        app_metadata: metadata.encode_to_vec(),
        ..FlightData::default()
    };
    let mut fixture = get_fixture(vec![frame], clean_trailers())?;
    let query = fixture.query()?;
    let (rows, metrics) = query.finish()?;
    assert_eq!(rows, Some(0));
    assert_eq!(metrics, vec![b"observed query metrics".to_vec()]);
    fixture.finish()
}

#[test]
fn integration_schema_rejection_and_pre_ack_eof_preserve_trailers() -> TestResult {
    for grpc_ok in [false, true] {
        let mut fixture = Fixture::new(move |server| {
            server.register_bidi_streaming(
                PUT,
                ProstCodec::<PutResult, FlightData>::new(),
                move |mut incoming: RequestStream<FlightData>,
                      sink: &mut ResponseSink<PutResult>| {
                    incoming
                        .recv_next()
                        .map_err(status)?
                        .ok_or_else(|| Status::internal("schema missing"))?;
                    sink.finish(
                        if grpc_ok {
                            Status::ok("")
                        } else {
                            Status::unavailable("schema rejected")
                        },
                        error_trailers(),
                    )
                    .map_err(status)
                },
            )
        })?;
        let record = batch_to_record_batch(&typed_batch())?;
        let error = match open_writer(&mut fixture, record.schema().as_ref()) {
            Ok(_) => anyhow::bail!("schema rejection unexpectedly opened writer"),
            Err(error) => error,
        };
        assert_region_error(error);
        fixture.finish()?;
    }
    Ok(())
}

#[test]
fn integration_successful_grpc_trailers_preserve_nonzero_application_diagnostics() -> TestResult {
    for alias_message in [None, Some("quota exhausted")] {
        let mut trailers = Metadata::new();
        trailers.append_ascii("x-greptime-err-code", "4003")?;
        trailers.append_ascii("inner_error_msg", r#"{"code":0,"err_msg":"success"}"#)?;
        trailers.append_ascii("x-greptime-err-retry-hint", "true")?;
        trailers.append_ascii("x-greptime-err-retry-hint", "false")?;
        if let Some(message) = alias_message {
            trailers.append_ascii("x-greptime-err-msg", message)?;
        }
        let check = |error: Error| {
            assert!(
                matches!(error, Error::ServerStatus { raw_code: 4003, retry_hint: false, ref message, .. }
                if message == alias_message.unwrap_or_default())
            );
        };
        let mut fixture = get_fixture(Vec::new(), trailers.clone())?;
        let mut query = fixture.query()?;
        check(query.next_batch().expect_err("nonzero numeric diagnostic"));
        drop(query);
        fixture.finish()?;
        let mut fixture = Fixture::new(move |server| {
            server.register_bidi_streaming(
                PUT,
                ProstCodec::<PutResult, FlightData>::new(),
                move |mut incoming: RequestStream<FlightData>,
                      sink: &mut ResponseSink<PutResult>| {
                    incoming
                        .recv_next()
                        .map_err(status)?
                        .ok_or_else(|| Status::internal("schema missing"))?;
                    sink.finish(Status::ok(""), trailers.clone())
                        .map_err(status)
                },
            )
        })?;
        let record = batch_to_record_batch(&typed_batch())?;
        match open_writer(&mut fixture, record.schema().as_ref()) {
            Ok(_) => anyhow::bail!("nonzero application diagnostic accepted"),
            Err(error) => check(error),
        }
        fixture.finish()?;
    }
    Ok(())
}
