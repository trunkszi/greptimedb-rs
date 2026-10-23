//! Real HTTP/2 client-streaming writes against a controlled in-memory peer.

#[path = "support/timer.rs"]
mod timer;

use std::{cell::RefCell, rc::Rc, thread, time::Duration};

use greptimedb::{
    Client, Error,
    batch::{BatchEncode, ColumnarBatch, RowBatch},
    buffer_policy::MemoryBuffer,
    config::{Auth, ClientOptions, Endpoint},
    hints::WriteHints,
    stream_inserter::StreamInserter,
    types::{ColumnDataType, SemanticType, TimestampUnit, ValueRef},
};
use greptimedb_proto::v1::{
    self, AffectedRows, GreptimeRequest, GreptimeResponse, ResponseHeader,
    greptime_request::Request as Payload, greptime_response::Response as Reply,
};
use grpc::{
    Metadata, ProstCodec, Status,
    core::{Code, transport::MemoryPipe},
    server::SyncServer,
    stream::StreamError,
};

#[path = "support/signal.rs"]
mod signal;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const PATH: &str = "/greptime.v1.GreptimeDatabase/HandleRequests";
type ServerCodec = ProstCodec<GreptimeResponse, GreptimeRequest>;

#[derive(Clone, Copy)]
enum ReplyMode {
    Success,
    HeaderError,
    TrailersError,
    MissingAffectedRows,
    RejectFirst,
}

#[derive(Debug, PartialEq, Eq)]
enum Terminal {
    CleanEof,
    Status(Code),
}

#[derive(Debug)]
struct Capture {
    metadata: Metadata,
    requests: Vec<GreptimeRequest>,
    terminal: Option<Terminal>,
}

type ServerResult = Result<Vec<Capture>, Status>;
struct Harness {
    client: Client,
    worker: thread::JoinHandle<ServerResult>,
    received: signal::Receiver,
}
impl Harness {
    fn close(self) -> Result<Vec<Capture>, Box<dyn std::error::Error>> {
        drop(self.client);
        Ok(self.worker.join().expect("mock server must not panic")?)
    }
}

fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking receive returned WouldBlock"),
    }
}

fn options() -> ClientOptions {
    let mut options = ClientOptions::from_endpoint(
        Endpoint::parse("grpc://127.0.0.1:4001").expect("valid endpoint"),
    );
    options.catalog = "research".into();
    options.schema = "ticks".into();
    options.auth = Some(Auth::Token("test-secret".into()));
    options.request_timeout = Duration::from_secs(5);
    options
}

fn harness(mode: ReplyMode, options: ClientOptions) -> Result<Harness, Box<dyn std::error::Error>> {
    let (client_pipe, server_pipe) = MemoryPipe::pair();
    let (received_tx, received) = signal::channel();
    let worker =
        thread::spawn(move || {
            let captures = Rc::new(RefCell::new(Vec::new()));
            let observed = Rc::clone(&captures);
            let mut server = SyncServer::new(server_pipe)?;
            server.register_bidi_streaming(PATH, ServerCodec::new(), move |mut incoming, sink| {
            let metadata = incoming
                .initial_metadata()
                .map_err(stream_status)?
                .ok_or_else(|| Status::internal("request metadata missing"))?;
            let mut requests = Vec::new();
            loop {
                match incoming.recv_next() {
                    Ok(Some(request)) => {
                        requests.push(request);
                        received_tx.send(1).map_err(|error| Status::internal(error.to_string()))?;
                        if matches!(mode, ReplyMode::RejectFirst) && !matches!(
                            requests[0].request.as_ref(), Some(Payload::Inserts(inserts))
                                if inserts.inserts[0].table_name == "_probe"
                        ) {
                            observed.borrow_mut().push(Capture {
                                metadata,
                                requests,
                                terminal: None,
                            });
                            return sink
                                .finish(Status::unavailable("region unavailable"), Metadata::new())
                                .map_err(stream_status);
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let status = stream_status(error);
                        observed.borrow_mut().push(Capture {
                            metadata,
                            requests,
                            terminal: Some(Terminal::Status(status.code())),
                        });
                        return Err(status);
                    }
                }
            }
            let rows = requests
                .iter()
                .map(|request| {
                    payload_rows(request.request.as_ref().expect("write payload present"))
                })
                .sum();
            observed.borrow_mut().push(Capture {
                metadata,
                requests,
                terminal: Some(Terminal::CleanEof),
            });
            if matches!(mode, ReplyMode::TrailersError) {
                let mut trailers = Metadata::new();
                trailers
                    .append_ascii(
                        "inner_error_msg",
                        r#"{"code":4003,"err_msg":"stream region busy"}"#,
                    )
                    .map_err(|error| Status::internal(error.to_string()))?;
                trailers
                    .append_ascii("x-greptime-err-retry-hint", "true")
                    .map_err(|error| Status::internal(error.to_string()))?;
                return sink
                    .finish(Status::unavailable("retry later"), trailers)
                    .map_err(stream_status);
            }
            let response = match mode {
                ReplyMode::HeaderError => GreptimeResponse {
                    header: Some(ResponseHeader {
                        status: Some(v1::Status {
                            status_code: 3001,
                            err_msg: "destination absent".into(),
                            retry_hint: "false".into(),
                        }),
                    }),
                    response: Some(Reply::AffectedRows(AffectedRows { value: 999 })),
                },
                ReplyMode::MissingAffectedRows => GreptimeResponse {
                    header: Some(ResponseHeader {
                        status: Some(v1::Status::default()),
                    }),
                    response: None,
                },
                _ => affected(rows),
            };
            sink.send(&response).map_err(stream_status)
        })?;
            server.serve_connection()?;
            drop(server);
            Ok(Rc::try_unwrap(captures)
                .expect("handler dropped with server")
                .into_inner())
        });
    Ok(Harness {
        client: Client::from_memory_pipe(options, client_pipe)?,
        worker,
        received,
    })
}

fn affected(value: u32) -> GreptimeResponse {
    GreptimeResponse {
        header: Some(ResponseHeader {
            status: Some(v1::Status::default()),
        }),
        response: Some(Reply::AffectedRows(AffectedRows { value })),
    }
}

fn payload_rows(payload: &Payload) -> u32 {
    match payload {
        Payload::Inserts(requests) => requests.inserts.iter().map(|batch| batch.row_count).sum(),
        Payload::Deletes(requests) => requests.deletes.iter().map(|batch| batch.row_count).sum(),
        Payload::RowInserts(requests) => requests
            .inserts
            .iter()
            .map(|batch| {
                u32::try_from(batch.rows.as_ref().expect("rows present").rows.len())
                    .expect("small fixture")
            })
            .sum(),
        Payload::RowDeletes(requests) => requests
            .deletes
            .iter()
            .map(|batch| {
                u32::try_from(batch.rows.as_ref().expect("rows present").rows.len())
                    .expect("small fixture")
            })
            .sum(),
        _ => panic!("unexpected non-write payload"),
    }
}

fn columnar(table: &str, count: i64) -> greptimedb::Result<ColumnarBatch> {
    let mut batch = ColumnarBatch::with_capacity(table, 16);
    batch.add_timestamp_column("ts", TimestampUnit::Nanosecond)?;
    batch.add_tag_column("symbol", ColumnDataType::String)?;
    batch.add_field_column("price", ColumnDataType::Float64)?;
    for ts in 0..count {
        batch.push_row(&[
            ValueRef::TimestampNanosecond(ts),
            ValueRef::String("BTCUSDT"),
            ValueRef::Float64(123.5),
        ])?;
    }
    Ok(batch)
}

fn row_batch(table: &str) -> greptimedb::Result<RowBatch> {
    let mut batch = RowBatch::new(table);
    batch.add_column_schema(
        "ts",
        SemanticType::Timestamp,
        ColumnDataType::TimestampNanosecond,
    )?;
    batch.add_column_schema("symbol", SemanticType::Tag, ColumnDataType::String)?;
    batch.add_column_schema("price", SemanticType::Field, ColumnDataType::Float64)?;
    batch.push_row(&[
        ValueRef::TimestampNanosecond(7),
        ValueRef::String("ETHUSDT"),
        ValueRef::Float64(12.5),
    ])?;
    Ok(batch)
}

fn assert_insert(insert: &v1::InsertRequest, table: &str, timestamps: &[i64]) {
    assert_eq!(insert.table_name, table);
    assert_eq!(insert.row_count as usize, timestamps.len());
    assert_columns(&insert.columns, timestamps, false);
}

fn assert_columns(columns: &[v1::Column], timestamps: &[i64], keys_only: bool) {
    let mut expected = vec![
        v1::Column {
            column_name: "ts".into(),
            semantic_type: v1::SemanticType::Timestamp as i32,
            datatype: v1::ColumnDataType::TimestampNanosecond as i32,
            values: Some(v1::column::Values {
                timestamp_nanosecond_values: timestamps.to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        },
        v1::Column {
            column_name: "symbol".into(),
            semantic_type: v1::SemanticType::Tag as i32,
            datatype: v1::ColumnDataType::String as i32,
            values: Some(v1::column::Values {
                string_values: vec!["BTCUSDT".into(); timestamps.len()],
                ..Default::default()
            }),
            ..Default::default()
        },
    ];
    if !keys_only {
        expected.push(v1::Column {
            column_name: "price".into(),
            semantic_type: v1::SemanticType::Field as i32,
            datatype: v1::ColumnDataType::Float64 as i32,
            values: Some(v1::column::Values {
                f64_values: vec![123.5; timestamps.len()],
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    assert_eq!(columns, expected);
}

fn assert_rows(rows: &v1::Rows, keys_only: bool) {
    use v1::value::ValueData;
    let mut schema = vec![
        v1::ColumnSchema {
            column_name: "ts".into(),
            semantic_type: v1::SemanticType::Timestamp as i32,
            datatype: v1::ColumnDataType::TimestampNanosecond as i32,
            ..Default::default()
        },
        v1::ColumnSchema {
            column_name: "symbol".into(),
            semantic_type: v1::SemanticType::Tag as i32,
            datatype: v1::ColumnDataType::String as i32,
            ..Default::default()
        },
    ];
    let mut values = vec![
        ValueData::TimestampNanosecondValue(7).into(),
        ValueData::StringValue("ETHUSDT".into()).into(),
    ];
    if !keys_only {
        schema.push(v1::ColumnSchema {
            column_name: "price".into(),
            semantic_type: v1::SemanticType::Field as i32,
            datatype: v1::ColumnDataType::Float64 as i32,
            ..Default::default()
        });
        values.push(ValueData::F64Value(12.5).into());
    }
    assert_eq!(rows.schema, schema);
    assert_eq!(rows.rows, [v1::Row { values }]);
}

#[test]
fn integration_ten_batches_are_one_stream_with_exact_headers_and_values() -> TestResult {
    let mut defaults = options();
    defaults.write_hints = WriteHints::new().with_ttl("1d").with_append_mode(false);
    let mut harness = harness(ReplyMode::Success, defaults)?;
    let hints = WriteHints::new()
        .with_append_mode(true)
        .with_auto_create_table(false);
    let mut writer = harness.client.stream_inserter_with_hints(&hints)?;
    for ts in 0..10 {
        let mut batch = columnar("ticks", 0)?;
        batch.push_row(&[
            ValueRef::TimestampNanosecond(ts),
            ValueRef::String("BTCUSDT"),
            ValueRef::Float64(123.5),
        ])?;
        writer.send_batch(&batch)?;
    }
    assert_eq!(writer.batches_sent(), 10);
    assert_eq!(writer.rows_sent(), 10);
    assert_eq!(writer.finish()?, 10);
    let captures = harness.close()?;
    assert_eq!(captures.len(), 1);
    assert_eq!(captures[0].requests.len(), 10);
    let headers = captures[0].metadata.to_headers();
    assert!(headers.contains(&("x-greptime-db-name".into(), "research-ticks".into())));
    assert!(headers.contains(&("x-greptime-auth".into(), "Bearer test-secret".into())));
    let hints = &headers
        .iter()
        .find(|(key, _)| key == "x-greptime-hints")
        .expect("hints")
        .1;
    for expected in ["ttl=1d", "append_mode=true", "auto_create_table=false"] {
        assert!(hints.split(',').any(|hint| hint == expected), "{hints}");
    }
    for (index, frame) in captures[0].requests.iter().enumerate() {
        let header = frame.header.as_ref().expect("every frame needs a header");
        assert_eq!(header.catalog, "research");
        assert_eq!(header.schema, "ticks");
        assert_eq!(header.dbname, "research-ticks");
        assert!(
            matches!(header.authorization.as_ref().and_then(|auth| auth.auth_scheme.as_ref()),
            Some(v1::auth_header::AuthScheme::Token(token)) if token.token == "test-secret")
        );
        let Some(Payload::Inserts(requests)) = &frame.request else {
            panic!("insert frame")
        };
        assert_eq!(requests.inserts.len(), 1);
        assert_insert(&requests.inserts[0], "ticks", &[index as i64]);
    }
    Ok(())
}

#[test]
fn integration_all_write_shapes_and_multitable_routing_preserve_keys() -> TestResult {
    let mut harness = harness(ReplyMode::Success, options())?;
    let first = columnar("ticks", 2)?;
    let second = columnar("trades", 3)?;
    let rows = row_batch("row_ticks")?;
    let mut writer = harness.client.stream_inserter()?;
    writer.send_batch(&first)?;
    writer.send_batch_to("override_ticks", &first)?;
    writer.send_batches(&[&first, &second])?;
    writer.send_rows(&rows)?;
    writer.send_delete_batch(&first)?;
    writer.send_delete_rows(&rows)?;
    writer.send_request(Payload::Inserts(v1::InsertRequests {
        inserts: vec![first.encode_insert_request()?],
    }))?;
    assert_eq!(writer.batches_sent(), 7);
    assert_eq!(writer.rows_sent(), 15);
    assert_eq!(writer.finish()?, 15);
    let captures = harness.close()?;
    assert_eq!(captures.len(), 1);
    let requests = &captures[0].requests;
    assert_eq!(requests.len(), 7);
    for (index, tables) in [
        (0, vec![("ticks", 2)]),
        (1, vec![("override_ticks", 2)]),
        (2, vec![("ticks", 2), ("trades", 3)]),
        (6, vec![("ticks", 2)]),
    ] {
        let Some(Payload::Inserts(inserts)) = &requests[index].request else {
            panic!("insert frame {index}")
        };
        assert_eq!(inserts.inserts.len(), tables.len());
        for (insert, (table, count)) in inserts.inserts.iter().zip(tables) {
            assert_insert(insert, table, &(0..count).collect::<Vec<_>>());
        }
    }
    let Some(Payload::RowInserts(inserts)) = &requests[3].request else {
        panic!("row insert")
    };
    assert_eq!(inserts.inserts.len(), 1);
    assert_eq!(inserts.inserts[0].table_name, "row_ticks");
    assert_rows(inserts.inserts[0].rows.as_ref().expect("rows"), false);
    let Some(Payload::Deletes(deletes)) = &requests[4].request else {
        panic!("delete")
    };
    assert_eq!(deletes.deletes.len(), 1);
    let delete = &deletes.deletes[0];
    assert_eq!(delete.table_name, "ticks");
    assert_eq!(delete.row_count, 2);
    assert_columns(&delete.key_columns, &[0, 1], true);
    let Some(Payload::RowDeletes(deletes)) = &requests[5].request else {
        panic!("row delete")
    };
    assert_eq!(deletes.deletes.len(), 1);
    assert_eq!(deletes.deletes[0].table_name, "row_ticks");
    assert_rows(deletes.deletes[0].rows.as_ref().expect("row keys"), true);
    Ok(())
}

#[test]
fn integration_invalid_multitable_member_sends_nothing_and_keeps_counters() -> TestResult {
    let mut harness = harness(ReplyMode::Success, options())?;
    let valid = columnar("valid", 2)?;
    let invalid = ColumnarBatch::new("missing_time_index");
    let mut writer = harness.client.stream_inserter()?;
    assert!(matches!(
        writer.send_batches(&[&valid, &invalid]),
        Err(Error::SchemaViolation(message))
            if message == "a table must have exactly one timestamp column"
    ));
    assert_eq!(writer.batches_sent(), 0);
    assert_eq!(writer.rows_sent(), 0);
    assert!(matches!(
        writer.send_batch_to("", &valid),
        Err(Error::SchemaViolation(message)) if message == "table name must not be empty"
    ));
    assert_eq!(writer.batches_sent(), 0);
    writer.send_batch(&valid)?;
    assert_eq!(writer.finish()?, 2);
    let captures = harness.close()?;
    assert_eq!(captures[0].requests.len(), 1);
    Ok(())
}

#[test]
fn integration_write_conveniences_clear_only_successful_buffers() -> TestResult {
    let mut harness = harness(ReplyMode::Success, options())?;
    let mut batch = columnar("ticks", 2)?;
    let capacity = batch.row_capacity();
    let mut writer = harness.client.stream_inserter()?;
    writer.write(&mut batch)?;
    assert_eq!(batch.row_count(), 0);
    assert!(batch.row_capacity() >= capacity);
    batch.push_row(&[
        ValueRef::TimestampNanosecond(20),
        ValueRef::String("BTCUSDT"),
        ValueRef::Float64(9.0),
    ])?;
    writer.write_delete(&mut batch)?;
    assert_eq!(batch.row_count(), 0);
    let mut invalid = ColumnarBatch::new("invalid");
    invalid.add_field_column("price", ColumnDataType::Float64)?;
    invalid
        .column_mut(0)
        .expect("field")
        .push(ValueRef::Float64(8.0))?;
    assert!(matches!(
        writer.write(&mut invalid),
        Err(Error::SchemaViolation(message))
            if message == "a table must have exactly one timestamp column"
    ));
    assert_eq!(invalid.row_count(), 1);
    assert!(matches!(
        writer.write_delete(&mut invalid),
        Err(Error::SchemaViolation(message))
            if message == "a table must have exactly one timestamp column"
    ));
    assert_eq!(invalid.row_count(), 1);
    assert_eq!(writer.finish()?, 3);
    let captures = harness.close()?;
    assert_eq!(captures[0].requests.len(), 2);
    Ok(())
}

#[test]
fn integration_response_status_and_trailer_failures_are_not_affected_row_success() -> TestResult {
    for (mode, expected_code, expected_message, expected_retry) in [
        (ReplyMode::HeaderError, 3001, "destination absent", false),
        (ReplyMode::TrailersError, 4003, "stream region busy", true),
    ] {
        let mut harness = harness(mode, options())?;
        let mut writer = harness.client.stream_inserter()?;
        writer.send_batch(&columnar("ticks", 1)?)?;
        match writer.finish() {
            Err(Error::ServerStatus {
                raw_code,
                message,
                retry_hint,
                ..
            }) => {
                assert_eq!(raw_code, expected_code);
                assert_eq!(message, expected_message);
                assert_eq!(retry_hint, expected_retry);
            }
            other => panic!("expected structured error, got {other:?}"),
        }
        harness.close()?;
    }
    Ok(())
}

#[test]
fn integration_missing_affected_rows_is_a_protocol_failure() -> TestResult {
    let mut harness = harness(ReplyMode::MissingAffectedRows, options())?;
    let writer = harness.client.stream_inserter()?;
    assert!(matches!(writer.finish(), Err(Error::Protocol(_))));
    harness.close()?;
    Ok(())
}

#[test]
fn integration_midstream_unavailable_preserves_borrowed_batch() -> TestResult {
    {
        let mut harness = harness(ReplyMode::RejectFirst, options())?;
        let batch = columnar("ticks", 2)?;
        let snapshot = batch.encode_insert_request()?;
        let mut writer = harness.client.stream_inserter()?;
        writer.send_batch(&batch)?;
        let error = writer.finish().expect_err("peer rejected open upload");
        assert!(
            matches!(&error, Error::Grpc(status) if status.code() == Code::Unavailable),
            "{error:?}"
        );
        assert_eq!(batch.encode_insert_request()?, snapshot);
        harness.close()?;
    }
    let mut harness = harness(ReplyMode::RejectFirst, options())?;
    let mut batch = columnar("ticks", 2)?;
    let snapshot = batch.encode_insert_request()?;
    let mut writer = harness.client.stream_inserter()?;
    writer.send_batch(&batch)?;
    // Completing another RPC drives the shared connection through the peer's rejection.
    let mut probe = harness.client.stream_inserter()?;
    probe.send_batch_to("_probe", &batch)?;
    assert_eq!(probe.finish()?, 2);
    let error = writer
        .write(&mut batch)
        .expect_err("remote rejected open upload");
    assert!(
        matches!(&error, Error::Grpc(status)
            if status.code() == Code::FailedPrecondition
                && status.message() == "sending half already closed"),
        "{error:?}"
    );
    assert!(batch.row_count() > 0);
    assert_eq!(batch.encode_insert_request()?, snapshot);
    assert_eq!(writer.batches_sent(), 1);
    assert_eq!(writer.rows_sent(), 2);
    let error = writer
        .finish()
        .expect_err("rejected upload cannot finish successfully");
    assert!(
        matches!(&error, Error::Grpc(status)
            if status.code() == Code::FailedPrecondition
                && status.message() == "sending half already closed"),
        "{error:?}"
    );
    assert_eq!(batch.encode_insert_request()?, snapshot);
    harness.close()?;
    Ok(())
}

#[test]
fn integration_drop_and_explicit_cancel_recover_the_shared_channel() -> TestResult {
    let mut harness = harness(ReplyMode::Success, options())?;
    let batch = columnar("ticks", 2)?;
    for explicit in [false, true, false, true] {
        let mut writer = harness.client.stream_inserter()?;
        writer.send_batch_to(if explicit { "cancelled" } else { "dropped" }, &batch)?;
        harness.received.recv_timeout(Duration::from_secs(5))?;
        if explicit {
            writer.cancel()?;
        } else {
            drop(writer);
        }
        let mut next = harness.client.stream_inserter()?;
        next.send_batch_to("finished", &batch)?;
        assert_eq!(next.finish()?, 2);
        harness.received.recv_timeout(Duration::from_secs(5))?;
    }
    let captures = harness.close()?;
    assert_eq!(captures.len(), 8);
    for (table, expected_count, terminal) in [
        ("dropped", 2, Terminal::Status(Code::Cancelled)),
        ("cancelled", 2, Terminal::Status(Code::Cancelled)),
        ("finished", 4, Terminal::CleanEof),
    ] {
        let matching = captures.iter().filter(|capture| {
            matches!(capture.requests.first().and_then(|request| request.request.as_ref()), Some(Payload::Inserts(inserts))
                if inserts.inserts[0].table_name == table)
        }).collect::<Vec<_>>();
        assert_eq!(matching.len(), expected_count, "{table}");
        for capture in matching {
            assert_eq!(capture.requests.len(), 1);
            assert_eq!(capture.terminal.as_ref(), Some(&terminal), "{table}");
        }
    }
    Ok(())
}

#[test]
fn integration_expired_stream_write_keeps_buffer_and_counters() -> TestResult {
    let mut defaults = options();
    defaults.request_timeout = Duration::from_millis(10);
    let mut harness = harness(ReplyMode::Success, defaults)?;
    let mut batch = columnar("ticks", 2)?;
    let snapshot = batch.encode_insert_request()?;
    let mut writer = harness.client.stream_inserter()?;
    // This controlled deadline expires before the first write; no request can be committed.
    timer::pause(Duration::from_millis(25)).expect("fixture timer");
    let error = writer
        .write(&mut batch)
        .expect_err("expired write must fail");
    assert!(
        matches!(error, Error::Grpc(ref status)
        if status.code() == grpc::core::Code::DeadlineExceeded),
        "{error:?}"
    );
    assert_eq!(batch.encode_insert_request()?, snapshot);
    assert_eq!(writer.batches_sent(), 0);
    assert_eq!(writer.rows_sent(), 0);
    assert!(matches!(
        writer.write_delete(&mut batch),
        Err(Error::Grpc(status)) if status.code() == Code::DeadlineExceeded
    ));
    assert_eq!(batch.encode_insert_request()?, snapshot);
    drop(writer);
    harness.close()?;
    Ok(())
}

#[test]
fn unit_stream_inserter_is_not_send_or_sync() {
    trait AmbiguousSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousSend<()> for T {}
    struct SendWitness;
    impl<T: ?Sized + Send> AmbiguousSend<SendWitness> for T {}
    let _ = <StreamInserter as AmbiguousSend<_>>::check;
    trait AmbiguousSync<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousSync<()> for T {}
    struct SyncWitness;
    impl<T: ?Sized + Sync> AmbiguousSync<SyncWitness> for T {}
    let _ = <StreamInserter as AmbiguousSync<_>>::check;
}
