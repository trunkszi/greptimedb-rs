//! Retention and reclamation behavior of reusable buffers under `BufferPolicy`.
//!
//! Each checkpoint prints the measured `row_capacity` and `allocated_bytes`,
//! and every retention or reclamation claim is asserted against those values
//! rather than an assumed allocator size. Scalar buffers keep one value vector;
//! variable-width buffers pool cleared payloads, so a warm clear releases
//! nothing and can add the spare-list allocation itself.

use greptimedb::{
    BatchEncode, ColumnDataType, ColumnarBatch, MultiTableBatch, Row, RowBatch, SemanticType,
    TimestampUnit, ValueRef,
    buffer_policy::{BufferPolicy, MemoryBuffer},
    client::Client,
    column_buf::TypedColumnBuf,
    config::{ClientOptions, Endpoint},
    types::TimestampNanosecond,
};

/// A fixed-width row used to show derive-generated batch reuse.
///
/// Every field maps to a `ScalarBuffer`, so a clear either keeps the value
/// vectors (unbounded) or drops them entirely (zero).
#[derive(Row)]
struct SensorReading {
    #[greptime(tag)]
    rack: u32,
    #[greptime(timestamp)]
    ts: TimestampNanosecond,
    #[greptime(field)]
    value: f64,
}

fn tick<'a>(symbol: &'a str, time: i64, price: f64) -> [ValueRef<'a>; 3] {
    [
        ValueRef::String(symbol),
        ValueRef::TimestampMillisecond(time),
        ValueRef::Float64(price),
    ]
}

fn text_value<'a>(data_type: ColumnDataType, text: &'a str) -> ValueRef<'a> {
    match data_type {
        ColumnDataType::String => ValueRef::String(text),
        _ => ValueRef::Binary(text.as_bytes()),
    }
}

fn reading(step: i64) -> SensorReading {
    SensorReading {
        rack: 7,
        ts: TimestampNanosecond(step),
        value: step as f64,
    }
}

fn snapshot(section: &str, subject: &str, rows: usize, buffer: &impl MemoryBuffer) {
    println!(
        "{section:<8} {subject:<42} {rows:>5} {:>12} {:>15}",
        buffer.row_capacity(),
        buffer.allocated_bytes()
    );
}

fn policy_row(subject: &str, policy: BufferPolicy) {
    println!(
        "{:<8} {:<42} {:>5} {:>12} {:>15}",
        "policy", subject, "-", policy.max_retained_rows, policy.max_retained_bytes
    );
}

fn multi_row(subject: &str, multi: &MultiTableBatch) {
    println!(
        "{:<8} {subject:<42} total_rows={} table_count={} (no MemoryBuffer)",
        "multi",
        multi.total_rows(),
        multi.table_count()
    );
}

fn usage(buffer: &impl MemoryBuffer) -> (usize, usize) {
    (buffer.row_capacity(), buffer.allocated_bytes())
}

fn columnar_table() -> ColumnarBatch {
    let mut batch = ColumnarBatch::with_capacity("ticks", 256);
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
}

fn row_table() -> RowBatch {
    let mut batch = RowBatch::with_capacity("ticks", 256);
    let timestamp = ColumnDataType::TimestampMillisecond;
    batch
        .add_column_schema("symbol", SemanticType::Tag, ColumnDataType::String)
        .unwrap();
    batch
        .add_column_schema("time", SemanticType::Timestamp, timestamp)
        .unwrap();
    batch
        .add_column_schema("price", SemanticType::Field, ColumnDataType::Float64)
        .unwrap();
    batch
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!(
        "{:<8} {:<42} {:>5} {:>12} {:>15}",
        "section", "subject", "rows", "row_capacity", "allocated_bytes"
    );

    // Watermarks are inclusive: a buffer exactly at its limit is retained.
    const RELEASE_ALL: BufferPolicy = BufferPolicy::zero();
    let retain_all = BufferPolicy::unbounded();
    let defaults = BufferPolicy::default();
    let tight = BufferPolicy::new(16, 1_024);
    assert_eq!(defaults.max_retained_rows, 65_536);
    assert_eq!(defaults.max_retained_bytes, 8 * 1024 * 1024);
    assert!(!tight.should_shrink(16, 1_024));
    assert!(tight.should_shrink(17, 1_024));
    assert!(tight.should_shrink(16, 1_025));
    assert!(!retain_all.should_shrink(usize::MAX, usize::MAX));
    assert!(!RELEASE_ALL.should_shrink(0, 0));
    assert!(RELEASE_ALL.should_shrink(0, 1));
    policy_row("default: max_retained_rows / bytes", defaults);
    policy_row("tight: new(16, 1024), inclusive", tight);

    // Fixed-width columns clear in place: the value vector and null mask keep
    // their capacity, so a warmed ingestion cycle never reallocates.
    let mut ints =
        TypedColumnBuf::with_capacity("value", SemanticType::Field, ColumnDataType::Int64, 4_096);
    for value in 0..100 {
        ints.push(ValueRef::Int64(value)).unwrap();
    }
    assert_eq!(ints.row_count(), 100);
    let (steady_capacity, steady_bytes) = usage(&ints);
    assert!(steady_capacity >= 100);
    ints.clear_with_policy(retain_all);
    assert_eq!(ints.row_count(), 0);
    assert_eq!(usage(&ints), (steady_capacity, steady_bytes));
    snapshot("typed", "Int64 clear(unbounded): retained", 0, &ints);

    for value in 0..100 {
        ints.push(ValueRef::Int64(value)).unwrap();
    }
    let before_shrink = usage(&ints).1;
    ints.shrink_to_fit();
    assert_eq!(ints.row_count(), 100);
    assert!(usage(&ints).0 < steady_capacity);
    assert!(usage(&ints).1 < before_shrink);
    snapshot("typed", "Int64 shrink_to_fit: rows kept", 100, &ints);
    ints.clear_with_policy(RELEASE_ALL);
    assert_eq!(ints.row_count(), 0);
    assert_eq!(usage(&ints), (0, 0));
    snapshot("typed", "Int64 clear(zero): released", 0, &ints);

    // Variable-width columns pool cleared payloads, so a warm clear keeps the
    // row slots plus every nested String or Vec<u8> allocation.
    let text = "x".repeat(128);
    let kinds = [
        ("String", ColumnDataType::String),
        ("Binary", ColumnDataType::Binary),
    ];
    for (label, data_type) in kinds {
        let mut column = TypedColumnBuf::with_capacity(label, SemanticType::Field, data_type, 256);
        for _ in 0..64 {
            column.push(text_value(data_type, &text)).unwrap();
        }
        let (steady_capacity, steady_bytes) = usage(&column);
        assert!(steady_bytes >= 64 * text.len());
        column.clear_with_policy(retain_all);
        assert_eq!(column.row_count(), 0);
        assert_eq!(usage(&column).0, steady_capacity);
        // Pooling moves payloads into the spare list, so bytes can grow even
        // though nothing was released; the invariant is "not less than before".
        assert!(usage(&column).1 >= steady_bytes);
        snapshot(
            "typed",
            &format!("{label} clear(unbounded): pooled"),
            0,
            &column,
        );

        for _ in 0..64 {
            column.push(text_value(data_type, &text)).unwrap();
        }
        let before_shrink = usage(&column).1;
        column.shrink_to_fit();
        assert_eq!(column.row_count(), 64);
        assert!(usage(&column).0 < steady_capacity);
        assert!(usage(&column).1 < before_shrink);
        column.clear_with_policy(RELEASE_ALL);
        assert_eq!(column.row_count(), 0);
        assert_eq!(usage(&column), (0, 0));
        snapshot(
            "typed",
            &format!("{label} shrink then clear(zero)"),
            0,
            &column,
        );
    }

    // A batch-wide clear applies the same policy per column. The tag column
    // pools its String payloads, so retained bytes may exceed the pre-clear
    // measurement even though nothing was released.
    let mut columnar = columnar_table();
    for row in 0..64 {
        columnar.push_row(&tick("BTC", row, row as f64)).unwrap();
    }
    assert_eq!(BatchEncode::row_count(&columnar), 64);
    let (steady_capacity, steady_bytes) = usage(&columnar);
    columnar.clear_with_policy(retain_all);
    assert_eq!(BatchEncode::row_count(&columnar), 0);
    assert_eq!(columnar.row_capacity(), steady_capacity);
    assert!(columnar.allocated_bytes() >= steady_bytes);
    snapshot("columnar", "clear(unbounded): capacity kept", 0, &columnar);

    for row in 0..64 {
        columnar.push_row(&tick("BTC", row, row as f64)).unwrap();
    }
    let capacity_before_shrink = usage(&columnar).0;
    let bytes_before_shrink = usage(&columnar).1;
    columnar.shrink_to_fit();
    assert_eq!(BatchEncode::row_count(&columnar), 64);
    assert!(usage(&columnar).0 < capacity_before_shrink);
    assert!(usage(&columnar).1 < bytes_before_shrink);
    snapshot("columnar", "shrink_to_fit: rows kept", 64, &columnar);

    let burst = "y".repeat(256);
    for row in 0..2_048 {
        columnar.push_row(&tick(&burst, row, row as f64)).unwrap();
    }
    assert!(usage(&columnar).0 >= 2_048);
    assert!(usage(&columnar).1 > bytes_before_shrink);
    columnar.clear_with_policy(BufferPolicy::new(1_024, usize::MAX));
    assert_eq!(BatchEncode::row_count(&columnar), 0);
    assert_eq!(usage(&columnar), (0, 0));
    snapshot(
        "columnar",
        "burst clear(rows>1024): reclaimed",
        0,
        &columnar,
    );
    columnar.push_row(&tick("BTC", 1, 1.0)).unwrap();
    assert_eq!(BatchEncode::row_count(&columnar), 1);

    // The row path stores owned protobuf values, so its accounting covers every
    // String and Binary value capacity as well as the row slots.
    let mut row_batch = row_table();
    for row in 0..64 {
        row_batch.push_row(&tick("BTC", row, row as f64)).unwrap();
    }
    assert_eq!(row_batch.row_count(), 64);
    let (steady_capacity, steady_bytes) = usage(&row_batch);
    row_batch.clear_unbounded();
    assert_eq!(row_batch.row_count(), 0);
    assert_eq!(usage(&row_batch), (steady_capacity, steady_bytes));
    snapshot("row", "clear(unbounded): slots retained", 0, &row_batch);

    // Same row count and row slots, longer payloads: only nested capacities
    // changed, so the accounting must grow.
    let wide = "z".repeat(256);
    for _ in 0..64 {
        row_batch.push_row(&tick(&wide, 1, 1.0)).unwrap();
    }
    assert!(usage(&row_batch).1 > steady_bytes);

    for row in 0..4_096 {
        row_batch.push_row(&tick(&wide, row, row as f64)).unwrap();
    }
    assert!(usage(&row_batch).0 >= 4_096);
    assert!(usage(&row_batch).1 > steady_bytes);
    row_batch.clear_with_policy(BufferPolicy::new(64, 64 * 1024));
    assert_eq!(row_batch.row_count(), 0);
    assert_eq!(usage(&row_batch), (0, 0));
    snapshot("row", "burst clear(rows>64): reclaimed", 0, &row_batch);
    row_batch.push_row(&tick("BTC", 2, 2.0)).unwrap();
    assert_eq!(row_batch.row_count(), 1);

    // `MultiTableBatch` stores encoded request snapshots and does not implement
    // `MemoryBuffer`; reuse means clearing the snapshot list and adding again.
    let mut multi = MultiTableBatch::new();
    multi.add_batch(&columnar).unwrap();
    multi.add_batch_to("ticks_archive", &columnar).unwrap();
    assert_eq!((multi.total_rows(), multi.table_count()), (2, 2));
    multi.clear();
    assert_eq!((multi.total_rows(), multi.table_count()), (0, 0));
    multi.add_batch(&columnar).unwrap();
    assert_eq!((multi.total_rows(), multi.table_count()), (1, 1));
    multi_row("clear then re-add: counts restart", &multi);

    // The derive-generated batch exposes the same `MemoryBuffer` contract over
    // its typed column buffers; `with_policy` selects the policy `clear` uses.
    let mut sensors = SensorReadingBatch::with_capacity(256);
    for step in 0..64 {
        sensors.push(&reading(step));
    }
    assert_eq!(sensors.row_count(), 64);
    let (capacity, bytes) = usage(&sensors);
    sensors.clear_with_policy(retain_all);
    assert_eq!(sensors.row_count(), 0);
    assert_eq!(usage(&sensors), (capacity, bytes));
    snapshot("derive", "clear(unbounded): retained", 0, &sensors);

    for step in 0..4_096 {
        sensors.push(&reading(step));
    }
    assert!(usage(&sensors).0 > capacity);
    assert!(usage(&sensors).1 > bytes);
    sensors.clear_with_policy(RELEASE_ALL);
    assert_eq!(sensors.row_count(), 0);
    assert_eq!(usage(&sensors), (0, 0));
    snapshot("derive", "burst clear(zero): released", 0, &sensors);

    let mut configured = SensorReadingBatch::with_policy(0, RELEASE_ALL);
    configured.push(&reading(1));
    assert!(usage(&configured).1 > 0);
    configured.clear();
    assert_eq!(configured.row_count(), 0);
    assert_eq!(usage(&configured), (0, 0));
    snapshot("derive", "with_policy + clear(): released", 0, &configured);
    live_probe()?;
    Ok(())
}

/// Probes a live server when `GREPTIMEDB_ENDPOINT` is set; no data is written.
fn live_probe() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let Ok(raw) = std::env::var("GREPTIMEDB_ENDPOINT") else {
        println!("live: skipped (GREPTIMEDB_ENDPOINT is unset)");
        return Ok(());
    };
    let url = if raw.contains("://") {
        raw
    } else {
        format!("grpc://{raw}")
    };
    let mut client = Client::connect(ClientOptions::from_endpoint(Endpoint::parse(&url)?))?;
    client.ping()?;
    println!("live: ping ok");
    Ok(())
}
