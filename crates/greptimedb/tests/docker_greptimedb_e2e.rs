//! Live round trip against a GreptimeDB standalone process.
//!
//! `GREPTIMEDB_TEST_ENDPOINT` (`grpc://host:port` or `host:port`) wins.
//! Otherwise a container named `greptimedb-rs-e2e` is reused or started.
//! The stable container stays running unless `GREPTIMEDB_E2E_REMOVE_CONTAINER=1`.
//! `GREPTIMEDB_E2E_REQUIRED=1` turns missing Docker into a failure.
//! External endpoints are never removed.

#[path = "support/timer.rs"]
mod timer;

use std::collections::HashMap;
use std::env;
use std::process::{Command, Output};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use greptimedb::proto::v1::{self, ColumnDataType, ColumnDef, CreateTableExpr, SemanticType};
use greptimedb::{
    Client, ClientOptions, ColumnDataType as SdkType, ColumnarBatch, Endpoint, MultiTableBatch,
    RowBatch, SemanticType as SdkRole, ThreadLocalClient, ValueRef, WorkerClient, WorkerConfig,
    WriteHints,
};

const CONTAINER_NAME: &str = "greptimedb-rs-e2e";
const IMAGE: &str = "greptime/greptimedb:latest";
const READY_DEADLINE: Duration = Duration::from_secs(90);
const RETRY_PAUSE: Duration = Duration::from_millis(200);

#[test]
fn e2e_greptimedb_standalone_round_trip() {
    let session = match prepare_session() {
        Prepare::Skip(reason) => {
            if env::var("GREPTIMEDB_E2E_REQUIRED").as_deref() == Ok("1") {
                panic!(
                    "GREPTIMEDB_E2E_REQUIRED=1 is set, but Docker / GreptimeDB endpoint is unavailable: {reason}"
                );
            }
            eprintln!("{reason}");
            return;
        }
        Prepare::Ready(session) => session,
    };
    round_trip(&session.endpoint).unwrap_or_else(|error| {
        if env::var("GREPTIMEDB_E2E_REQUIRED").as_deref() == Ok("1") {
            panic!("GREPTIMEDB_E2E_REQUIRED=1 is set, but Docker / GreptimeDB endpoint is unavailable or the round trip failed: {error}");
        }
        panic!("greptimedb standalone round trip failed: {error}");
    });
}

struct Session {
    endpoint: String,
    _lease: Option<ContainerLease>,
}

enum Prepare {
    Skip(String),
    Ready(Session),
}

/// Removal is an explicit opt-in, including when the stable container is reused.
struct ContainerLease {
    name: String,
    remove_on_drop: bool,
}

impl ContainerLease {
    fn stable(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            remove_on_drop: env::var("GREPTIMEDB_E2E_REMOVE_CONTAINER").as_deref() == Ok("1"),
        }
    }
}

impl Drop for ContainerLease {
    fn drop(&mut self) {
        if !self.remove_on_drop {
            return;
        }
        match Command::new("docker")
            .args(["rm", "-f", &self.name])
            .status()
        {
            Ok(status) if status.success() => {}
            Ok(status) => eprintln!("container cleanup rejected: {status}"),
            Err(error) => eprintln!("container cleanup failed: {error}"),
        }
    }
}

fn prepare_session() -> Prepare {
    if let Ok(raw) = env::var("GREPTIMEDB_TEST_ENDPOINT") {
        return Prepare::Ready(Session {
            endpoint: normalize_endpoint(&raw),
            _lease: None,
        });
    }
    if !docker_on_path() {
        return Prepare::Skip(
            "skipping e2e_greptimedb_standalone_round_trip: GREPTIMEDB_TEST_ENDPOINT is unset and docker is not on PATH"
                .to_owned(),
        );
    }
    let daemon = docker_output(&["info", "--format", "{{.ServerVersion}}"]);
    if !daemon.status.success() {
        return Prepare::Skip(format!(
            "Docker daemon is unavailable: {}",
            String::from_utf8_lossy(&daemon.stderr),
        ));
    }
    let lease = ContainerLease::stable(CONTAINER_NAME);
    let port = ensure_container();
    wait_until_grpc_ready(port);
    Prepare::Ready(Session {
        endpoint: format!("grpc://127.0.0.1:{port}"),
        _lease: Some(lease),
    })
}

fn normalize_endpoint(raw: &str) -> String {
    let raw = raw.trim();
    if raw.contains("://") {
        raw.to_owned()
    } else {
        format!("grpc://{raw}")
    }
}

fn docker_on_path() -> bool {
    env::var_os("PATH")
        .is_some_and(|path| env::split_paths(&path).any(|dir| dir.join("docker").is_file()))
}

fn ensure_container() -> u16 {
    match inspect_running(CONTAINER_NAME) {
        Some(true) => published_grpc_port(),
        Some(false) => {
            must_succeed(&["start", CONTAINER_NAME], "docker start");
            published_grpc_port()
        }
        None => {
            let output = docker_output(&[
                "run",
                "-d",
                "--name",
                CONTAINER_NAME,
                "-p",
                "127.0.0.1::4001",
                "-p",
                "127.0.0.1::4000",
                IMAGE,
                "standalone",
                "start",
                "--grpc-bind-addr",
                "0.0.0.0:4001",
            ]);
            if !output.status.success() {
                panic!(
                    "docker run failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            published_grpc_port()
        }
    }
}

fn inspect_running(name: &str) -> Option<bool> {
    let output = docker_output(&["inspect", "-f", "{{.State.Running}}", name]);
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.to_ascii_lowercase().contains("no such object") {
            return None;
        }
        panic!("docker inspect failed: {stderr}");
    }
    match String::from_utf8_lossy(&output.stdout).trim() {
        "true" => Some(true),
        "false" => Some(false),
        other => panic!("unexpected docker inspect state: {other}"),
    }
}

fn published_grpc_port() -> u16 {
    let output = docker_output(&["port", CONTAINER_NAME, "4001/tcp"]);
    if !output.status.success() {
        panic!(
            "docker port failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    parse_published_port(&String::from_utf8_lossy(&output.stdout))
}

fn parse_published_port(text: &str) -> u16 {
    let mut fallback = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((host, port_text)) = line.rsplit_once(':') else {
            continue;
        };
        let Ok(port) = port_text.parse::<u16>() else {
            continue;
        };
        if port == 0 {
            continue;
        }
        if host.ends_with("127.0.0.1") {
            return port;
        }
        if fallback.is_none() {
            fallback = Some(port);
        }
    }
    fallback.unwrap_or_else(|| panic!("docker port did not publish 4001/tcp: {text}"))
}

/// The container's port mapping accepts TCP as soon as docker-proxy starts,
/// before GreptimeDB has bound its gRPC listener. Probe the health RPC so a
/// cold start cannot race the first request.
fn wait_until_grpc_ready(port: u16) {
    let deadline = Instant::now() + READY_DEADLINE;
    let endpoint = format!("grpc://127.0.0.1:{port}");
    loop {
        let attempt = Endpoint::parse(&endpoint).and_then(|endpoint| {
            let mut options = ClientOptions::from_endpoint(endpoint);
            options.connect_timeout = Duration::from_millis(500);
            options.request_timeout = Duration::from_secs(3);
            let mut client = Client::connect(options)?;
            client.ping()
        });
        if attempt.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            let logs = docker_output(&["logs", "--tail", "40", CONTAINER_NAME]);
            panic!(
                "timed out after 90s waiting for 127.0.0.1:{port} to answer health checks\n{}{}",
                String::from_utf8_lossy(&logs.stdout),
                String::from_utf8_lossy(&logs.stderr)
            );
        }
        timer::pause(RETRY_PAUSE).expect("fixture timer");
    }
}

fn docker_output(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|error| {
            panic!("failed to spawn docker {}: {error}", args.join(" "));
        })
}

fn must_succeed(args: &[&str], action: &str) {
    let output = docker_output(args);
    if !output.status.success() {
        panic!(
            "{action} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn round_trip(endpoint_str: &str) -> greptimedb::Result<()> {
    let endpoint = Endpoint::parse(endpoint_str)?;
    let options = ClientOptions::from_endpoint(endpoint)
        .with_catalog("greptime")
        .with_schema("public");
    let mut client = Client::connect(options)?;
    client.ping()?;
    let report = client.probe_health()?;
    assert!(report.healthy);
    assert!(!report.endpoint.is_empty());

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let schema = format!("e2e_{}_{suffix}", std::process::id());
    client.create_database(&schema, true, HashMap::new())?;
    let options = ClientOptions::from_endpoint(Endpoint::parse(endpoint_str)?)
        .with_catalog("greptime")
        .with_schema(&schema);
    let mut client = Client::connect(options)?;

    let table = "cpu_ticks";
    let expr = CreateTableExpr {
        catalog_name: "greptime".into(),
        schema_name: schema.clone(),
        table_name: table.into(),
        desc: String::new(),
        column_defs: vec![
            ColumnDef {
                name: "host".into(),
                data_type: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: Vec::new(),
                semantic_type: SemanticType::Tag as i32,
                comment: String::new(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: "ts".into(),
                data_type: ColumnDataType::TimestampMillisecond as i32,
                is_nullable: false,
                default_constraint: Vec::new(),
                semantic_type: SemanticType::Timestamp as i32,
                comment: String::new(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: "value".into(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: Vec::new(),
                semantic_type: SemanticType::Field as i32,
                comment: String::new(),
                datatype_extension: None,
                options: None,
            },
        ],
        time_index: "ts".into(),
        primary_keys: vec!["host".into()],
        create_if_not_exists: true,
        table_options: HashMap::new(),
        table_id: None,
        engine: "mito".into(),
    };
    client.create_table(expr.clone())?;
    let mut typed_expr = expr;
    typed_expr.table_name = "typed_ticks".into();
    client.create_table(typed_expr)?;

    let mut batch = ColumnarBatch::new(table);
    batch.add_tag_column("host", SdkType::String)?;
    batch.add_timestamp_column("ts", greptimedb::TimestampUnit::Millisecond)?;
    batch.add_field_column("value", SdkType::Float64)?;
    batch.push_row(&[
        ValueRef::String("a"),
        ValueRef::TimestampMillisecond(1_700_000_000_000),
        ValueRef::Float64(1.5),
    ])?;
    let hints = WriteHints::new().with_auto_create_table(true);
    let inserted = client.insert_batch_with_hints(&batch, &hints)?;
    assert_eq!(inserted, 1);

    let mut rows = RowBatch::new(table);
    rows.add_column_schema("host", SdkRole::Tag, SdkType::String)?;
    rows.add_column_schema("ts", SdkRole::Timestamp, SdkType::TimestampMillisecond)?;
    rows.add_column_schema("value", SdkRole::Field, SdkType::Float64)?;
    rows.push_row(&[
        ValueRef::String("a"),
        ValueRef::TimestampMillisecond(1_700_000_000_001),
        ValueRef::Float64(2.5),
    ])?;
    let row_inserted = client.insert_rows(&rows)?;
    assert_eq!(row_inserted, 1);

    let mut stream = client.stream_inserter()?;
    stream.send_batch(&batch)?;
    let streamed = stream.finish()?;
    assert_eq!(streamed, 1);

    let routed = client.options().clone();
    let record = greptimedb::bulk::batch_to_record_batch(&batch)?;
    let mut bulk = greptimedb::bulk::BulkStreamWriter::open(
        client.channel_mut()?,
        &routed,
        table,
        record.schema().as_ref(),
        greptimedb::bulk::BulkWriteOptions::default(),
    )?;
    bulk.write_batch(&batch)?;
    let bulk_rows = bulk.finish()?;
    assert_eq!(bulk_rows.total_affected_rows, 1);

    assert_eq!(
        read_ticks(&mut client, table)?,
        vec![("a".into(), TS, 1.5), ("a".into(), TS + 1, 2.5)]
    );

    assert_eq!(
        client.insert(&[TypedTick {
            host: "typed".into(),
            ts: TS + 2,
            value: 3.5,
        }])?,
        1
    );
    let mut multi = MultiTableBatch::new();
    multi.add_batch(&tick_batch(table, "multi", TS + 3, 4.5)?)?;
    multi.add_batch(&tick_batch("typed_ticks", "multi", TS + 4, 5.5)?)?;
    assert_eq!(client.insert_multi_batch(&multi)?, 2);

    let local = Arc::new(ThreadLocalClient::new(client.options().clone())?);
    let worker = Arc::new(WorkerClient::new(
        client.options().clone(),
        WorkerConfig::default(),
    )?);
    let local_thread = thread::spawn(move || {
        local.with_client(|client| client.insert_batch(&tick_batch(table, "local", TS + 5, 6.5)?))
    });
    let worker_thread = thread::spawn(move || {
        worker.with_client(|client| client.insert_batch(&tick_batch(table, "worker", TS + 6, 7.5)?))
    });
    assert_eq!(local_thread.join().expect("thread-local writer")?, 1);
    assert_eq!(worker_thread.join().expect("worker writer")?, 1);
    assert_eq!(
        read_ticks(&mut client, "typed_ticks")?,
        vec![("typed".into(), TS + 2, 3.5), ("multi".into(), TS + 4, 5.5),]
    );
    let mut expected = vec![
        ("a".into(), TS, 1.5),
        ("a".into(), TS + 1, 2.5),
        ("multi".into(), TS + 3, 4.5),
        ("local".into(), TS + 5, 6.5),
        ("worker".into(), TS + 6, 7.5),
    ];
    assert_eq!(read_ticks(&mut client, table)?, expected);

    let prom = client.prom_instant_query(table, "1700000000.006", "5m")?;
    let json: sonic_rs::Value =
        sonic_rs::from_str(prom.as_json_str()?).expect("valid Prometheus JSON");
    assert_eq!(json["status"], "success", "Prometheus response: {json}");

    client.alter_table(v1::AlterTableExpr {
        catalog_name: "greptime".into(),
        schema_name: schema.clone(),
        table_name: table.into(),
        kind: Some(v1::alter_table_expr::Kind::AddColumns(v1::AddColumns {
            add_columns: vec![v1::AddColumn {
                column_def: Some(ColumnDef {
                    name: "note".into(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: true,
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                }),
                location: None,
                add_if_not_exists: false,
            }],
        })),
    })?;
    let mut altered = greptimedb::query::QueryStream::sql(
        client.channel_mut()?,
        &routed,
        "SELECT note FROM cpu_ticks ORDER BY ts",
    )?;
    let mut null_rows = 0;
    while let Some(batch) = altered.next_batch()? {
        assert_eq!(batch.schema().field(0).name(), "note");
        assert_eq!(batch.column(0).null_count(), batch.num_rows());
        null_rows += batch.num_rows();
    }
    assert_eq!(null_rows, expected.len());

    let mut keys = ColumnarBatch::new(table);
    keys.add_tag_column("host", SdkType::String)?;
    keys.add_timestamp_column("ts", greptimedb::TimestampUnit::Millisecond)?;
    keys.push_row(&[
        ValueRef::String("a"),
        ValueRef::TimestampMillisecond(1_700_000_000_000),
    ])?;
    assert_eq!(client.delete_batch(&keys)?, 1);
    expected.remove(0);
    assert_eq!(read_ticks(&mut client, table)?, expected);
    client.truncate_table(table)?;
    assert!(read_ticks(&mut client, table)?.is_empty());
    client.drop_table(table, false)?;
    let missing = read_ticks(&mut client, table).expect_err("dropped table must fail");
    assert!(
        matches!(missing, greptimedb::Error::ServerStatus { .. }),
        "expected missing-table server status, got {missing:?}"
    );
    let cleanup_options = ClientOptions::from_endpoint(Endpoint::parse(endpoint_str)?)
        .with_catalog("greptime")
        .with_schema("public");
    let mut cleanup_client = Client::connect(cleanup_options.clone())?;
    let mut drop_database = greptimedb::query::QueryStream::sql(
        cleanup_client.channel_mut()?,
        &cleanup_options,
        &format!("DROP DATABASE {schema}"),
    )?;
    while let Some(batch) = drop_database.next_batch()? {
        assert_eq!(batch.num_rows(), 0);
    }
    Ok(())
}

const TS: i64 = 1_700_000_000_000;

#[derive(greptimedb::Row)]
#[row(table = "typed_ticks")]
struct TypedTick {
    #[greptime(tag)]
    host: String,
    #[greptime(timestamp, unit = "ms")]
    ts: i64,
    #[greptime(field)]
    value: f64,
}

fn tick_batch(table: &str, host: &str, ts: i64, value: f64) -> greptimedb::Result<ColumnarBatch> {
    let mut batch = ColumnarBatch::new(table);
    batch.add_tag_column("host", SdkType::String)?;
    batch.add_timestamp_column("ts", greptimedb::TimestampUnit::Millisecond)?;
    batch.add_field_column("value", SdkType::Float64)?;
    batch.push_row(&[
        ValueRef::String(host),
        ValueRef::TimestampMillisecond(ts),
        ValueRef::Float64(value),
    ])?;
    Ok(batch)
}

fn read_ticks(client: &mut Client, table: &str) -> greptimedb::Result<Vec<(String, i64, f64)>> {
    let options = client.options().clone();
    let mut query = greptimedb::query::QueryStream::sql(
        client.channel_mut()?,
        &options,
        &format!("SELECT host, ts, value FROM {table} ORDER BY ts"),
    )?;
    let mut tuples = Vec::new();
    while let Some(batch) = query.next_batch()? {
        assert_eq!(batch.num_columns(), 3);
        let hosts = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("host string column");
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("millisecond timestamp column");
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 value column");
        for index in 0..batch.num_rows() {
            assert!(!hosts.is_null(index) && !times.is_null(index) && !values.is_null(index));
            tuples.push((
                hosts.value(index).to_owned(),
                times.value(index),
                values.value(index),
            ));
        }
    }
    Ok(tuples)
}
