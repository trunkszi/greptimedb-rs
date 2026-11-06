//! Public unary operations against a controlled native HTTP/2 GreptimeDB peer.

use greptimedb::{
    batch::{ColumnarBatch, MultiTableBatch},
    client::Client,
    config::{Auth, ClientOptions, Endpoint},
    error::{Error, RetryableError},
    hints::WriteHints,
    row_batch::RowBatch,
    types::{ColumnDataType, SemanticType, TimestampUnit, ValueRef},
};
use greptimedb_proto::v1::{
    self as proto, ddl_request::Expr, greptime_request::Request as Operation,
};
use grpc::core::transport::MemoryPipe;
use grpc::{Metadata, ProstCodec, Request, Response, Status, SyncServer};
use std::{collections::HashMap, thread};

const HANDLE: &str = "/greptime.v1.GreptimeDatabase/Handle";
const PROM: &str = "/greptime.v1.PrometheusGateway/Handle";
const HEALTH: &str = "/greptime.v1.HealthCheck/HealthCheck";

// The fixture owns its server thread; SDK handles are created and used on one thread.
fn serve<F>(configure: F) -> (MemoryPipe, thread::JoinHandle<()>)
where
    F: FnOnce(&mut SyncServer<MemoryPipe>) + Send + 'static,
{
    let (client, peer) = MemoryPipe::pair();
    let worker = thread::spawn(move || {
        let mut server = SyncServer::new(peer).unwrap();
        configure(&mut server);
        server.serve_connection().unwrap();
    });
    (client, worker)
}
fn options() -> ClientOptions {
    ClientOptions::from_endpoint(Endpoint::parse("grpc://127.0.0.1:4001").unwrap())
        .with_catalog("trading")
        .with_schema("ticks")
}
fn response(rows: u32) -> proto::GreptimeResponse {
    proto::GreptimeResponse {
        header: Some(proto::ResponseHeader {
            status: Some(proto::Status {
                status_code: 0,
                err_msg: String::new(),
                retry_hint: String::new(),
            }),
        }),
        response: Some(proto::greptime_response::Response::AffectedRows(
            proto::AffectedRows { value: rows },
        )),
    }
}
fn columnar(table: &str) -> ColumnarBatch {
    let mut batch = ColumnarBatch::new(table);
    batch
        .add_tag_column("symbol", ColumnDataType::String)
        .unwrap();
    batch
        .add_timestamp_column("time", TimestampUnit::Millisecond)
        .unwrap();
    batch
        .add_field_column("price", ColumnDataType::Float64)
        .unwrap();
    batch
        .push_row(&[
            ValueRef::String("BTC"),
            ValueRef::TimestampMillisecond(1700000000000),
            ValueRef::Float64(42.5),
        ])
        .unwrap();
    batch
}
fn rows() -> RowBatch {
    let mut batch = RowBatch::new("ticks");
    batch
        .add_column_schema("symbol", SemanticType::Tag, ColumnDataType::String)
        .unwrap();
    batch
        .add_column_schema(
            "time",
            SemanticType::Timestamp,
            ColumnDataType::TimestampMillisecond,
        )
        .unwrap();
    batch
        .add_column_schema("price", SemanticType::Field, ColumnDataType::Float64)
        .unwrap();
    batch
        .push_row(&[
            ValueRef::String("BTC"),
            ValueRef::TimestampMillisecond(1700000000000),
            ValueRef::Float64(42.5),
        ])
        .unwrap();
    batch
}
fn assert_header(header: Option<&proto::RequestHeader>) {
    let header = header.expect("routing header");
    assert_eq!(
        (&*header.catalog, &*header.schema, &*header.dbname),
        ("trading", "ticks", "trading-ticks")
    );
}

#[test]
fn integration_scenario_ingestion_deletion_and_multi_table_single_rpc() {
    let (pipe, worker) = serve(|server| {
        let mut index = 0;
        server
            .register_unary(
                HANDLE,
                ProstCodec::<proto::GreptimeResponse, proto::GreptimeRequest>::new(),
                move |request: Request<proto::GreptimeRequest>| {
                    let (metadata, request) = request.into_parts();
                    assert_header(request.header.as_ref());
                    let tables = match request.request.expect("operation") {
                        Operation::Inserts(inserts) => {
                            assert!(index <= 4);
                            assert_eq!(inserts.inserts.len(), if index >= 3 { 2 } else { 1 });
                            for table in &inserts.inserts {
                                assert_eq!(table.row_count, 1);
                                assert_eq!(table.columns.len(), 3);
                                assert_eq!(
                                    table.columns[0].values.as_ref().unwrap().string_values,
                                    ["BTC"]
                                );
                                assert_eq!(
                                    table.columns[1]
                                        .values
                                        .as_ref()
                                        .unwrap()
                                        .timestamp_millisecond_values,
                                    [1700000000000]
                                );
                                assert_eq!(
                                    table.columns[2].values.as_ref().unwrap().f64_values,
                                    [42.5]
                                );
                            }
                            inserts
                                .inserts
                                .into_iter()
                                .map(|r| r.table_name)
                                .collect::<Vec<_>>()
                        }
                        Operation::RowInserts(inserts) => {
                            assert!(index == 5 || index == 6);
                            let rows = inserts.inserts[0].rows.as_ref().unwrap();
                            assert_eq!(rows.schema.len(), 3);
                            assert_eq!(rows.rows.len(), 1);
                            assert_eq!(rows.rows[0].values.len(), 3);
                            inserts.inserts.into_iter().map(|r| r.table_name).collect()
                        }
                        Operation::Deletes(deletes) => {
                            assert!(index == 7 || index == 8);
                            assert_eq!(
                                deletes.deletes[0]
                                    .key_columns
                                    .iter()
                                    .map(|c| c.column_name.as_str())
                                    .collect::<Vec<_>>(),
                                ["symbol", "time"]
                            );
                            deletes.deletes.into_iter().map(|r| r.table_name).collect()
                        }
                        Operation::RowDeletes(deletes) => {
                            assert_eq!(index, 9);
                            let rows = deletes.deletes[0].rows.as_ref().unwrap();
                            assert_eq!(
                                rows.schema
                                    .iter()
                                    .map(|c| c.column_name.as_str())
                                    .collect::<Vec<_>>(),
                                ["symbol", "time"]
                            );
                            assert_eq!(rows.rows[0].values.len(), 2);
                            deletes.deletes.into_iter().map(|r| r.table_name).collect()
                        }
                        other => panic!("unexpected operation {other:?}"),
                    };
                    let expected: &[&str] = match index {
                        2 | 6 | 8 => &["archive"],
                        3 | 4 => &["ticks", "trades"],
                        _ => &["ticks"],
                    };
                    assert_eq!(tables, expected);
                    if index == 1 {
                        let values: Vec<_> = metadata.get_all("x-greptime-hints").collect();
                        assert_eq!(
                            values,
                            [&grpc::core::MetadataValue::Ascii(
                                "auto_create_table=false,ttl=7d".into()
                            )]
                        );
                    }
                    index += 1;
                    Ok(Response::new(response(if expected.len() == 2 {
                        2
                    } else {
                        1
                    })))
                },
            )
            .unwrap();
    });
    let mut opts = options();
    opts.write_hints = WriteHints::new().with_auto_create_table(true);
    let mut client = Client::from_memory_pipe(opts, pipe).unwrap();
    let batch = columnar("ticks");
    let second = columnar("trades");
    assert_eq!(client.insert_batch(&batch).unwrap(), 1);
    assert_eq!(
        client
            .insert_batch_with_hints(
                &batch,
                &WriteHints::new()
                    .with_auto_create_table(false)
                    .with_ttl("7d")
            )
            .unwrap(),
        1
    );
    assert_eq!(client.insert_batch_to("archive", &batch).unwrap(), 1);
    assert_eq!(client.insert_batches(&[&batch, &second]).unwrap(), 2);
    let mut multi = MultiTableBatch::new();
    multi.add_batch(&batch).unwrap();
    multi.add_batch(&second).unwrap();
    assert_eq!(client.insert_multi_batch(&multi).unwrap(), 2);
    let row_batch = rows();
    assert_eq!(client.insert_rows(&row_batch).unwrap(), 1);
    assert_eq!(client.insert_rows_to("archive", &row_batch).unwrap(), 1);
    assert_eq!(client.delete_batch(&batch).unwrap(), 1);
    assert_eq!(client.delete_batch_from("archive", &batch).unwrap(), 1);
    assert_eq!(client.delete_rows(&row_batch).unwrap(), 1);
    drop(client);
    worker.join().unwrap();
}

#[test]
fn integration_scenario_all_ddl_variants_keep_scopes_and_expressions() {
    let create_table = proto::CreateTableExpr {
        table_name: "ticks".into(),
        catalog_name: "trading".into(),
        schema_name: "ticks".into(),
        engine: "mito".into(),
        ..Default::default()
    };
    let alter_database = proto::AlterDatabaseExpr {
        catalog_name: "foreign_catalog".into(),
        schema_name: "foreign_schema".into(),
        kind: Some(proto::alter_database_expr::Kind::UnsetDatabaseOptions(
            proto::UnsetDatabaseOptions {
                keys: vec!["ttl".into()],
            },
        )),
    };
    let alter_table = proto::AlterTableExpr {
        table_name: "ticks".into(),
        catalog_name: "foreign_catalog".into(),
        schema_name: "foreign_schema".into(),
        kind: Some(proto::alter_table_expr::Kind::RenameTable(
            proto::RenameTable {
                new_table_name: "renamed_ticks".into(),
            },
        )),
    };
    let create_view = proto::CreateViewExpr {
        view_name: "prices".into(),
        definition: "SELECT price FROM ticks".into(),
        ..Default::default()
    };
    let create_flow = proto::CreateFlowExpr {
        flow_name: "aggregate".into(),
        sql: "SELECT avg(price) FROM ticks".into(),
        ..Default::default()
    };
    let comment = proto::CommentOnExpr {
        catalog_name: "foreign_catalog".into(),
        schema_name: "foreign_schema".into(),
        object_name: "ticks".into(),
        comment: "Historical quotes in USD".into(),
        ..Default::default()
    };
    let expected = vec![
        Expr::CreateDatabase(proto::CreateDatabaseExpr {
            catalog_name: "trading".into(),
            schema_name: "archive".into(),
            create_if_not_exists: true,
            options: HashMap::from([("ttl".into(), "7d".into())]),
        }),
        Expr::AlterDatabase(alter_database.clone()),
        Expr::CreateTable(create_table.clone()),
        Expr::AlterTable(alter_table.clone()),
        Expr::DropTable(proto::DropTableExpr {
            catalog_name: "trading".into(),
            schema_name: "ticks".into(),
            table_name: "ticks".into(),
            drop_if_exists: true,
            ..Default::default()
        }),
        Expr::TruncateTable(proto::TruncateTableExpr {
            catalog_name: "trading".into(),
            schema_name: "ticks".into(),
            table_name: "ticks".into(),
            ..Default::default()
        }),
        Expr::CreateView(create_view.clone()),
        Expr::DropView(proto::DropViewExpr {
            catalog_name: "trading".into(),
            schema_name: "ticks".into(),
            view_name: "prices".into(),
            drop_if_exists: true,
            ..Default::default()
        }),
        Expr::CreateFlow(create_flow.clone()),
        Expr::DropFlow(proto::DropFlowExpr {
            catalog_name: "trading".into(),
            flow_name: "aggregate".into(),
            drop_if_exists: true,
            ..Default::default()
        }),
        Expr::CommentOn(comment.clone()),
        Expr::CreateTable(create_table.clone()),
    ];
    let (pipe, worker) = serve(move |server| {
        let mut expected = expected.into_iter();
        let mut count = 0;
        server
            .register_unary(
                HANDLE,
                ProstCodec::<proto::GreptimeResponse, proto::GreptimeRequest>::new(),
                move |request: Request<proto::GreptimeRequest>| {
                    let request = request.into_parts().1;
                    assert_header(request.header.as_ref());
                    match request.request.unwrap() {
                        Operation::Ddl(ddl) => assert_eq!(
                            ddl.expr,
                            Some(expected.next().expect("one DDL per invocation"))
                        ),
                        other => panic!("expected DDL, got {other:?}"),
                    }
                    count += 1;
                    Ok(Response::new(response(count)))
                },
            )
            .unwrap();
    });
    let mut client = Client::from_memory_pipe(options(), pipe).unwrap();
    assert_eq!(
        client
            .create_database(
                "archive",
                true,
                HashMap::from([("ttl".into(), "7d".into())])
            )
            .unwrap(),
        1
    );
    assert_eq!(client.alter_database(alter_database).unwrap(), 2);
    assert_eq!(client.create_table(create_table.clone()).unwrap(), 3);
    assert_eq!(client.alter_table(alter_table).unwrap(), 4);
    assert_eq!(client.drop_table("ticks", true).unwrap(), 5);
    assert_eq!(client.truncate_table("ticks").unwrap(), 6);
    assert_eq!(client.create_view(create_view).unwrap(), 7);
    assert_eq!(client.drop_view("prices", true).unwrap(), 8);
    assert_eq!(client.create_flow(create_flow).unwrap(), 9);
    assert_eq!(client.drop_flow("aggregate", true).unwrap(), 10);
    assert_eq!(client.comment_on(comment).unwrap(), 11);
    assert_eq!(
        client.execute_ddl(Expr::CreateTable(create_table)).unwrap(),
        12
    );
    drop(client);
    worker.join().unwrap();
}

#[test]
fn integration_scenario_health_promql_and_authenticated_metadata() {
    let (pipe, worker) = serve(|server| {
        server
            .register_unary(
                HEALTH,
                ProstCodec::<proto::HealthCheckResponse, proto::HealthCheckRequest>::new(),
                |request: Request<proto::HealthCheckRequest>| {
                    let (metadata, _) = request.into_parts();
                    assert_eq!(
                        metadata.get_all("x-greptime-db-name").next(),
                        Some(&grpc::core::MetadataValue::Ascii("trading-ticks".into()))
                    );
                    assert_eq!(
                        metadata.get_all("x-greptime-auth").next(),
                        Some(&grpc::core::MetadataValue::Ascii(
                            "Basic dXNlcjpwYXNz".into()
                        ))
                    );
                    Ok(Response::new(proto::HealthCheckResponse {}))
                },
            )
            .unwrap();
        let mut count = 0;
        server.register_unary(PROM, ProstCodec::<proto::PromqlResponse, proto::PromqlRequest>::new(), move |request: Request<proto::PromqlRequest>| {
            let request = request.into_parts().1;
            assert_header(request.header.as_ref());
            assert!(matches!(request.header.unwrap().authorization.unwrap().auth_scheme, Some(proto::auth_header::AuthScheme::Basic(proto::Basic { ref username, ref password })) if username == "user" && password == "pass"));
            match request.promql.unwrap() {
                proto::promql_request::Promql::InstantQuery(query) => {
                    assert_eq!(count, 0);
                    assert_eq!((query.query.as_str(), query.time.as_str(), query.lookback.as_str()), ("up{job=\"quotes\"}", "1700000000", "5m"));
                }
                proto::promql_request::Promql::RangeQuery(query) => {
                    assert_eq!(count, 1);
                    assert_eq!((query.query.as_str(), query.start.as_str(), query.end.as_str(), query.step.as_str(), query.lookback.as_str()), ("rate(trades[1m])", "1700000000", "1700000060", "5s", "10m"));
                }
            }
            count += 1;
            Ok(Response::new(proto::PromqlResponse { header: response(0).header, body: br#"{"status":"success","data":{"resultType":"vector","result":[]}}"#.to_vec() }))
        }).unwrap();
    });
    let mut opts = options();
    opts.auth = Some(Auth::basic("user", "pass"));
    let mut client = Client::from_memory_pipe(opts, pipe).unwrap();
    client.ping().unwrap();
    let report = client.probe_health().unwrap();
    assert!(report.healthy);
    assert_eq!(report.endpoint, "grpc://127.0.0.1:4001");
    assert!(report.rtt < std::time::Duration::from_secs(30));
    let instant = client
        .prom_instant_query("up{job=\"quotes\"}", "1700000000", "5m")
        .unwrap();
    assert_eq!(
        instant.as_json_str().unwrap(),
        r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#
    );
    assert_eq!(
        client
            .prom_range_query("rate(trades[1m])", "1700000000", "1700000060", "5s", "10m")
            .unwrap(),
        instant
    );
    drop(client);
    worker.join().unwrap();
    assert!(
        greptimedb::prom::PromQueryResult { body: vec![0xff] }
            .as_json_str()
            .is_err()
    );
}

fn stream_status(error: grpc::StreamError) -> Status {
    match error {
        grpc::StreamError::Status(status) => status,
        grpc::StreamError::WouldBlock => Status::internal("fixture unexpectedly pending"),
    }
}

#[test]
fn integration_scenario_layered_errors_and_unary_cardinality_never_succeed_silently() {
    let (pipe, worker) = serve(|server| {
        let mut call = 0;
        server
            .register_server_streaming(
                HANDLE,
                ProstCodec::<proto::GreptimeResponse, proto::GreptimeRequest>::new(),
                move |_request: Request<proto::GreptimeRequest>, sink| {
                    let mut result = response(0);
                    match call {
                        0 => {
                            result.header = Some(proto::ResponseHeader {
                                status: Some(proto::Status {
                                    status_code: 4000,
                                    err_msg: "storage unavailable".into(),
                                    retry_hint: "true".into(),
                                }),
                            });
                            sink.send(&result).map_err(stream_status)?;
                        }
                        1 => {
                            let mut trailers = Metadata::new();
                            trailers
                                .append_ascii(
                                    "inner_error_msg",
                                    r#"{"code":3001,"err_msg":"ticks missing"}"#,
                                )
                                .unwrap();
                            trailers
                                .append_ascii("x-greptime-err-retry-hint", "false")
                                .unwrap();
                            sink.finish(Status::unavailable("outer status"), trailers)
                                .map_err(stream_status)?;
                        }
                        2 => {
                            sink.send(&result).map_err(stream_status)?;
                            sink.send(&result).map_err(stream_status)?;
                        }
                        3 => {}
                        4 => {
                            result.response = None;
                            sink.send(&result).map_err(stream_status)?;
                        }
                        5 => {
                            sink.send(&result).map_err(stream_status)?;
                        }
                        _ => panic!("unexpected replay"),
                    }
                    call += 1;
                    Ok(())
                },
            )
            .unwrap();
    });
    let mut client = Client::from_memory_pipe(options(), pipe).unwrap();
    let batch = columnar("ticks");
    let error = client.insert_batch(&batch).unwrap_err();
    assert!(error.is_retryable());
    assert!(
        matches!(error, Error::ServerStatus { raw_code: 4000, ref message, .. } if message == "storage unavailable")
    );
    let error = client.insert_batch(&batch).unwrap_err();
    assert!(!error.is_retryable());
    assert!(
        matches!(error, Error::ServerStatus { raw_code: 3001, ref message, .. } if message == "ticks missing")
    );
    for _ in 0..3 {
        assert!(matches!(
            client.insert_batch(&batch),
            Err(Error::Protocol(_))
        ));
    }
    assert_eq!(client.insert_batch(&batch).unwrap(), 0);
    drop(client);
    worker.join().unwrap();
}

#[derive(greptimedb::Row)]
#[row(table = "typed_ticks")]
struct TypedTick {
    #[greptime(tag)]
    symbol: String,
    #[greptime(timestamp, unit = "ms")]
    time: i64,
    #[greptime(field)]
    price: f64,
}

#[test]
fn integration_scenario_typed_rows_use_columnar_wire_and_explicit_destination() {
    let (pipe, worker) = serve(|server| {
        let mut index = 0;
        server
            .register_unary(
                HANDLE,
                ProstCodec::<proto::GreptimeResponse, proto::GreptimeRequest>::new(),
                move |request: Request<proto::GreptimeRequest>| {
                    let Some(Operation::Inserts(inserts)) = request.into_parts().1.request else {
                        panic!("typed rows must use columnar inserts");
                    };
                    assert_eq!(inserts.inserts.len(), 1);
                    let insert = &inserts.inserts[0];
                    assert_eq!(
                        insert.table_name,
                        if index == 0 {
                            "typed_ticks"
                        } else {
                            "explicit_ticks"
                        }
                    );
                    assert_eq!(insert.row_count, 2);
                    assert_eq!(
                        insert.columns[0].values.as_ref().unwrap().string_values,
                        ["BTC", "ETH"]
                    );
                    assert_eq!(
                        insert.columns[1]
                            .values
                            .as_ref()
                            .unwrap()
                            .timestamp_millisecond_values,
                        [1700000000000, 1700000000001]
                    );
                    assert_eq!(
                        insert.columns[2].values.as_ref().unwrap().f64_values,
                        [42.5, 18.25]
                    );
                    index += 1;
                    Ok(Response::new(response(2)))
                },
            )
            .unwrap();
    });
    let mut client = Client::from_memory_pipe(options(), pipe).unwrap();
    let rows = [
        TypedTick {
            symbol: "BTC".into(),
            time: 1700000000000,
            price: 42.5,
        },
        TypedTick {
            symbol: "ETH".into(),
            time: 1700000000001,
            price: 18.25,
        },
    ];
    assert_eq!(client.insert(&rows).unwrap(), 2);
    assert_eq!(client.insert_to("explicit_ticks", &rows).unwrap(), 2);
    drop(client);
    worker.join().unwrap();
}

#[test]
fn integration_scenario_promql_errors_override_body_and_empty_fields_survive() {
    let (pipe, worker) = serve(|server| {
        let mut call = 0;
        server
            .register_server_streaming(
                PROM,
                ProstCodec::<proto::PromqlResponse, proto::PromqlRequest>::new(),
                move |request: Request<proto::PromqlRequest>, sink| {
                    let request = request.into_parts().1;
                    let mut resp = proto::PromqlResponse {
                        header: response(0).header,
                        body: br#"{"status":"success"}"#.to_vec(),
                    };
                    match call {
                        0 => {
                            resp.header = Some(proto::ResponseHeader {
                                status: Some(proto::Status {
                                    status_code: 2001,
                                    err_msg: "invalid plan".into(),
                                    retry_hint: "false".into(),
                                }),
                            });
                            sink.send(&resp).map_err(stream_status)?;
                        }
                        1 => {
                            sink.send(&resp).map_err(stream_status)?;
                            let mut trailers = Metadata::new();
                            trailers
                                .append_ascii(
                                    "inner_error_msg",
                                    r#"{"code":2002,"err_msg":"query execution failed"}"#,
                                )
                                .unwrap();
                            trailers
                                .append_ascii("x-greptime-err-retry-hint", "true")
                                .unwrap();
                            sink.finish(Status::internal("remote failure"), trailers)
                                .map_err(stream_status)?;
                        }
                        2 => {
                            assert_eq!(
                                request.promql,
                                Some(proto::promql_request::Promql::InstantQuery(
                                    proto::PromInstantQuery::default()
                                ))
                            );
                            resp.body.clear();
                            sink.send(&resp).map_err(stream_status)?;
                        }
                        3 => {
                            assert_eq!(
                                request.promql,
                                Some(proto::promql_request::Promql::RangeQuery(
                                    proto::PromRangeQuery::default()
                                ))
                            );
                            resp.body.clear();
                            sink.send(&resp).map_err(stream_status)?;
                        }
                        _ => panic!("unexpected replay"),
                    }
                    call += 1;
                    Ok(())
                },
            )
            .unwrap();
    });
    let mut client = Client::from_memory_pipe(options(), pipe).unwrap();
    assert!(matches!(
        client.prom_instant_query("up", "", ""),
        Err(Error::ServerStatus {
            raw_code: 2001,
            retry_hint: false,
            ..
        })
    ));
    assert!(matches!(
        client.prom_range_query("up", "", "", "", ""),
        Err(Error::ServerStatus {
            raw_code: 2002,
            retry_hint: true,
            ..
        })
    ));
    assert_eq!(
        client
            .prom_instant_query("", "", "")
            .unwrap()
            .as_json_str()
            .unwrap(),
        ""
    );
    assert!(
        client
            .prom_range_query("", "", "", "", "")
            .unwrap()
            .body
            .is_empty()
    );
    drop(client);
    worker.join().unwrap();
    assert!(matches!(
        greptimedb::prom::PromQueryResult { body: vec![0xff] }.as_json_str(),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn integration_scenario_second_batch_encoding_failure_sends_no_partial_rpc() {
    use greptimedb::batch::BatchEncode;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Reject;
    impl BatchEncode for Reject {
        fn table_name(&self) -> &str {
            "rejected"
        }
        fn row_count(&self) -> usize {
            1
        }
        fn validate_schema(&self) -> greptimedb::Result<()> {
            Err(Error::SchemaViolation("invalid second table".into()))
        }
        fn encode_insert_request_for(&self, _: &str) -> greptimedb::Result<proto::InsertRequest> {
            Err(Error::SchemaViolation("invalid second table".into()))
        }
        fn encode_delete_request_for(&self, _: &str) -> greptimedb::Result<proto::DeleteRequest> {
            Err(Error::SchemaViolation("invalid second table".into()))
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let server_calls = Arc::clone(&calls);
    let (pipe, worker) = serve(move |server| {
        server
            .register_unary(
                HANDLE,
                ProstCodec::<proto::GreptimeResponse, proto::GreptimeRequest>::new(),
                move |_: Request<proto::GreptimeRequest>| {
                    server_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(Response::new(response(1)))
                },
            )
            .unwrap();
        server
            .register_unary(
                HEALTH,
                ProstCodec::<proto::HealthCheckResponse, proto::HealthCheckRequest>::new(),
                |_: Request<proto::HealthCheckRequest>| {
                    Ok(Response::new(proto::HealthCheckResponse {}))
                },
            )
            .unwrap();
    });
    let mut client = Client::from_memory_pipe(options(), pipe).unwrap();
    assert!(matches!(
        client.insert_batches(&[&columnar("ticks"), &Reject]),
        Err(Error::SchemaViolation(_))
    ));
    client.ping().unwrap();
    drop(client);
    worker.join().unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

const _: fn() = || {
    struct Marker;
    trait IfSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> IfSend<()> for T {}
    impl<T: ?Sized + Send> IfSend<Marker> for T {}
    let _ = <Client as IfSend<_>>::check;
    trait IfSync<A> {
        fn check() {}
    }
    impl<T: ?Sized> IfSync<()> for T {}
    impl<T: ?Sized + Sync> IfSync<Marker> for T {}
    let _ = <Client as IfSync<_>>::check;
};
