//! Quickstart: unary health checks, DDL, and CRUD against GreptimeDB.
//!
//! The default run is deterministic and offline. An in-process `MemoryPipe`
//! server answers the health check and the `Handle` RPC, and records which
//! request kinds arrived so the example can assert the full sequence.
//!
//! ```text
//! cargo run -p greptimedb --example 01_quickstart_unary_ddl_and_crud
//! ```
//!
//! Setting `GREPTIMEDB_ENDPOINT` adds a live health probe after the offline
//! run. A bare `host:port` is treated as `grpc://`. The live path sends no DDL.
//!
//! ```text
//! GREPTIMEDB_ENDPOINT=127.0.0.1:4001 \
//!     cargo run -p greptimedb --example 01_quickstart_unary_ddl_and_crud
//! ```

use std::{cell::RefCell, error::Error, rc::Rc, thread};

use greptimedb::{
    batch::{ColumnarBatch, MultiTableBatch},
    client::Client,
    config::{Auth, ClientOptions, Endpoint, TransportScheme},
    hints::WriteHints,
    proto::v1::{self as proto, greptime_request::Request as Operation},
    row_batch::RowBatch,
    types::{ColumnDataType, SemanticType, TimestampUnit, ValueRef},
};
use grpc::{ProstCodec, Request, Response, SyncServer, core::transport::MemoryPipe};

const HANDLE_PATH: &str = "/greptime.v1.GreptimeDatabase/Handle";
const HEALTH_PATH: &str = "/greptime.v1.HealthCheck/HealthCheck";

fn main() -> Result<(), Box<dyn Error>> {
    demo_endpoints()?;
    demo_auth()?;

    let hints = WriteHints::new()
        .with_auto_create_table(true)
        .with_ttl("7d");
    let encoded_hints = hints.encode_header_value().unwrap_or_default();
    assert_eq!(encoded_hints, "auto_create_table=true,ttl=7d");
    println!("write hints: {encoded_hints}");

    demo_mock_round_trip(hints)?;
    live_probe()?;
    Ok(())
}

/// Parsing is local and performs no connection, so every scheme is safe here.
fn demo_endpoints() -> Result<(), Box<dyn Error>> {
    let grpc = Endpoint::parse("grpc://127.0.0.1:4001")?;
    let grpcs = Endpoint::parse("grpcs://db.example.com:4001")?;
    let unix = Endpoint::parse("unix:///tmp/greptimedb.sock")?;

    assert_eq!(grpc.scheme(), TransportScheme::Grpc);
    assert_eq!(grpcs.scheme(), TransportScheme::Grpcs);
    assert_eq!(unix.scheme(), TransportScheme::Unix);
    assert_eq!(grpc.socket_addr_str(), "127.0.0.1:4001");
    assert_eq!(grpcs.authority(), "db.example.com:4001");
    assert_eq!(unix.authority(), "localhost");

    let unix_path = unix
        .unix_path()
        .map_or_else(String::new, |path| path.display().to_string());
    assert_eq!(unix_path, "/tmp/greptimedb.sock");
    println!(
        "endpoints: grpc={} grpcs={} unix={unix_path}",
        grpc.socket_addr_str(),
        grpcs.authority()
    );
    Ok(())
}

/// `Auth`'s `Debug` output is safe to log; the SDK redacts both credentials.
fn demo_auth() -> Result<(), Box<dyn Error>> {
    let endpoint = Endpoint::parse("grpc://127.0.0.1:4001")?;
    let basic = Auth::basic("demo", "secret");
    let token = Auth::token("tok");

    let mut basic_options = ClientOptions::from_endpoint(endpoint.clone())
        .with_catalog("demo_catalog")
        .with_schema("demo_schema");
    basic_options.auth = Some(basic.clone());
    let mut token_options = ClientOptions::from_endpoint(endpoint)
        .with_catalog("demo_catalog")
        .with_schema("demo_schema");
    token_options.auth = Some(token.clone());

    assert_eq!(basic_options.dbname(), "demo_catalog-demo_schema");
    assert_eq!(token_options.dbname(), "demo_catalog-demo_schema");
    let basic_debug = format!("{basic_options:?}");
    let token_debug = format!("{token_options:?}");
    assert!(basic_debug.contains("<redacted>"));
    assert!(!basic_debug.contains("secret"));
    assert!(token_debug.contains("<redacted>"));
    assert!(!token_debug.contains("tok"));
    println!("auth: basic={basic:?} token={token:?}");
    Ok(())
}

/// Exercises every unary write path against one in-process server.
fn demo_mock_round_trip(hints: WriteHints) -> Result<(), Box<dyn Error>> {
    let (client_pipe, server_pipe) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<Vec<&'static str>, grpc::Status> {
        let server_requests = Rc::new(RefCell::new(Vec::new()));
        let mut server = SyncServer::new(server_pipe)?;
        let health_requests = Rc::clone(&server_requests);
        server.register_unary(
            HEALTH_PATH,
            ProstCodec::<proto::HealthCheckResponse, proto::HealthCheckRequest>::new(),
            move |_request: Request<proto::HealthCheckRequest>| {
                health_requests.borrow_mut().push("Health");
                Ok(Response::new(proto::HealthCheckResponse {}))
            },
        )?;
        let handle_requests = Rc::clone(&server_requests);
        server.register_unary(
            HANDLE_PATH,
            ProstCodec::<proto::GreptimeResponse, proto::GreptimeRequest>::new(),
            move |request: Request<proto::GreptimeRequest>| {
                let operation = request.into_parts().1.request;
                let kind = match operation.as_ref() {
                    Some(Operation::Inserts(inserts)) if inserts.inserts.len() > 1 => "MultiInsert",
                    Some(Operation::Inserts(_)) => "Insert",
                    Some(Operation::RowInserts(_)) => "RowInsert",
                    Some(Operation::Deletes(_)) => "Delete",
                    Some(Operation::Ddl(_)) => "Ddl",
                    _ => "Other",
                };
                handle_requests.borrow_mut().push(kind);
                Ok(Response::new(mock_response(1)))
            },
        )?;
        server.serve_connection()?;
        let recorded = server_requests.borrow().clone();
        Ok(recorded)
    });

    let mut options = ClientOptions::from_endpoint(Endpoint::parse("grpc://127.0.0.1:4001")?)
        .with_catalog("demo_catalog")
        .with_schema("demo_schema");
    options.auth = Some(Auth::basic("demo", "secret"));
    let mut client = Client::from_memory_pipe(options, client_pipe)?;

    let columnar = columnar_batch()?;
    let rows = row_batch()?;
    let mut multi = MultiTableBatch::new();
    multi.add_batch(&columnar)?;
    multi.add_batch_to("trades", &columnar)?;
    assert_eq!(multi.table_count(), 2);

    client.ping()?;
    let created = client.create_table(create_table_expr())?;
    let inserted = client.insert_batch_with_hints(&columnar, &hints)?;
    let row_inserted = client.insert_rows(&rows)?;
    let multi_inserted = client.insert_multi_batch(&multi)?;
    let deleted = client.delete_batch(&columnar)?;
    let altered = client.alter_table(alter_table_expr())?;
    let truncated = client.truncate_table("ticks")?;
    let dropped = client.drop_table("ticks", true)?;
    println!(
        "unary results: create_table={created} insert_batch={inserted} insert_rows={row_inserted} \
         multi_insert={multi_inserted} delete={deleted} alter={altered} truncate={truncated} \
         drop={dropped}"
    );

    drop(client);
    let recorded = worker.join().expect("mock server thread panicked")?;
    assert_eq!(
        recorded,
        [
            "Health",
            "Ddl",
            "Insert",
            "RowInsert",
            "MultiInsert",
            "Delete",
            "Ddl",
            "Ddl",
            "Ddl",
        ]
    );
    println!("mock requests: {recorded:?}");
    Ok(())
}

fn columnar_batch() -> greptimedb::Result<ColumnarBatch> {
    let mut batch = ColumnarBatch::new("ticks");
    batch.add_tag_column("symbol", ColumnDataType::String)?;
    batch.add_timestamp_column("ts", TimestampUnit::Millisecond)?;
    batch.add_field_column("price", ColumnDataType::Float64)?;
    batch.push_row(&[
        ValueRef::String("BTC"),
        ValueRef::TimestampMillisecond(1_700_000_000_000),
        ValueRef::Float64(42.5),
    ])?;
    Ok(batch)
}

fn row_batch() -> greptimedb::Result<RowBatch> {
    let mut batch = RowBatch::new("ticks");
    batch.add_column_schema("symbol", SemanticType::Tag, ColumnDataType::String)?;
    batch.add_column_schema(
        "ts",
        SemanticType::Timestamp,
        ColumnDataType::TimestampMillisecond,
    )?;
    batch.add_column_schema("price", SemanticType::Field, ColumnDataType::Float64)?;
    batch.push_row(&[
        ValueRef::String("BTC"),
        ValueRef::TimestampMillisecond(1_700_000_000_000),
        ValueRef::Float64(42.5),
    ])?;
    Ok(batch)
}

fn create_table_expr() -> proto::CreateTableExpr {
    proto::CreateTableExpr {
        catalog_name: "demo_catalog".into(),
        schema_name: "demo_schema".into(),
        table_name: "ticks".into(),
        column_defs: vec![proto::ColumnDef {
            name: "ts".into(),
            data_type: proto::ColumnDataType::TimestampMillisecond as i32,
            is_nullable: false,
            semantic_type: proto::SemanticType::Timestamp as i32,
            ..Default::default()
        }],
        time_index: "ts".into(),
        create_if_not_exists: true,
        engine: "mito".into(),
        ..Default::default()
    }
}

fn alter_table_expr() -> proto::AlterTableExpr {
    proto::AlterTableExpr {
        catalog_name: "demo_catalog".into(),
        schema_name: "demo_schema".into(),
        table_name: "ticks".into(),
        kind: Some(proto::alter_table_expr::Kind::RenameTable(
            proto::RenameTable {
                new_table_name: "ticks_v2".into(),
            },
        )),
    }
}

fn mock_response(affected_rows: u32) -> proto::GreptimeResponse {
    proto::GreptimeResponse {
        header: Some(proto::ResponseHeader {
            status: Some(proto::Status {
                status_code: 0,
                ..Default::default()
            }),
        }),
        response: Some(proto::greptime_response::Response::AffectedRows(
            proto::AffectedRows {
                value: affected_rows,
            },
        )),
    }
}

/// Health-only probe of a live server; no DDL is sent.
fn live_probe() -> Result<(), Box<dyn Error>> {
    let Ok(raw) = std::env::var("GREPTIMEDB_ENDPOINT") else {
        println!("live: skipped (GREPTIMEDB_ENDPOINT is unset)");
        return Ok(());
    };
    let endpoint = if raw.contains("://") {
        raw
    } else {
        format!("grpc://{raw}")
    };
    let options = ClientOptions::from_endpoint(Endpoint::parse(&endpoint)?)
        .with_catalog("greptime")
        .with_schema("public");
    let mut client = Client::connect(options)?;
    client.ping()?;
    let report = client.probe_health()?;
    assert!(report.healthy);
    println!(
        "live health: endpoint={} rtt={:?} healthy={}",
        report.endpoint, report.rtt, report.healthy
    );
    Ok(())
}
