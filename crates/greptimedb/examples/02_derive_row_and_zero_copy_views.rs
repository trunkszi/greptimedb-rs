//! Derive a typed row schema, read it through borrowed views, and round-trip it
//! through Arrow.
//!
//! The derived batch stores rows in typed column buffers. `OrderTickView` reads
//! one row without copying variable-size values, and `OrderTickRef` pushes a row
//! whose strings and bytes borrow caller-owned storage. `batch_to_record_batch`
//! produces the Arrow form used by Flight ingestion, and `RowDecode` rebuilds the
//! typed batch by matching Arrow columns by name rather than by position.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use greptimedb::{
    Row,
    bulk::{
        batch_to_record_batch,
        schema::{JSON_TYPE_VALUE, SEMANTIC_TYPE_KEY, TYPE_EXTENSION_KEY},
    },
    client::Client,
    config::{ClientOptions, Endpoint},
    row::{RowDecode, decode::Json},
    types::{ColumnDataType, Decimal128, SemanticType},
};

/// One order-book tick stored in `order_book_ticks`.
///
/// The derive has no `default` attribute: a missing measured value is expressed
/// as `Option::None`, which becomes a null in the nullable `size` column.
#[derive(Clone, Debug, PartialEq, Row)]
#[row(table = "order_book_ticks", rename_all = "snake_case")]
struct OrderTick {
    #[greptime(tag)]
    symbol: String,
    #[greptime(timestamp, unit = "ns")]
    ts: i64,
    #[greptime(field)]
    best_bid: f64,
    /// `None` is the only absence representation the derive supports.
    #[greptime(field)]
    size: Option<f64>,
    #[greptime(json)]
    payload: String,
    #[greptime(decimal(precision = 18, scale = 4))]
    notional: Decimal128,
    #[greptime(field)]
    blob: Vec<u8>,
}

struct BorrowedOrderTick<'a> {
    symbol: &'a str,
    blob: &'a [u8],
    payload: Json<'a>,
    notional: Decimal128,
    size: Option<f64>,
}

impl<'a> BorrowedOrderTick<'a> {
    /// Reinterprets a derived view, tagging the payload text as JSON in place.
    fn from_view(view: OrderTickView<'a>) -> Self {
        Self {
            symbol: view.symbol,
            blob: view.blob,
            payload: Json(view.payload),
            notional: view.notional,
            size: view.size,
        }
    }
}

fn arrow_field<'a>(schema: &'a Schema, name: &str) -> &'a Field {
    schema.field_with_name(name).expect("column is declared")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows = [
        OrderTick {
            symbol: "BTCUSDT".into(),
            ts: 1_700_000_000_000_000_000,
            best_bid: 100.25,
            size: Some(1.5),
            payload: "{\"bid\":100.25,\"ask\":100.3}".into(),
            notional: Decimal128::new(123_456, 18, 4),
            blob: vec![0, 1, 2],
        },
        OrderTick {
            symbol: "ETHUSDT".into(),
            ts: 1_700_000_000_000_000_001,
            best_bid: 2_000.5,
            size: None,
            payload: "{\"bid\":2000.5}".into(),
            notional: Decimal128::new(-987_654, 18, 4),
            blob: vec![0xff, 0x00],
        },
        OrderTick {
            symbol: "WLDUSDT".into(),
            ts: 1_700_000_000_000_000_002,
            best_bid: 1.25,
            size: Some(0.0),
            payload: "{}".into(),
            notional: Decimal128::new(0, 18, 4),
            blob: Vec::new(),
        },
    ];

    assert_eq!(OrderTick::TABLE_NAME, "order_book_ticks");
    let declared: Vec<_> = OrderTick::COLUMNS
        .iter()
        .map(|column| {
            (
                column.name,
                column.semantic_type,
                column.data_type,
                column.nullable,
            )
        })
        .collect();
    assert_eq!(
        declared,
        [
            ("symbol", SemanticType::Tag, ColumnDataType::String, false),
            (
                "ts",
                SemanticType::Timestamp,
                ColumnDataType::TimestampNanosecond,
                false
            ),
            (
                "best_bid",
                SemanticType::Field,
                ColumnDataType::Float64,
                false
            ),
            ("size", SemanticType::Field, ColumnDataType::Float64, true),
            ("payload", SemanticType::Field, ColumnDataType::Json, false),
            (
                "notional",
                SemanticType::Field,
                ColumnDataType::Decimal128 {
                    precision: 18,
                    scale: 4
                },
                false
            ),
            ("blob", SemanticType::Field, ColumnDataType::Binary, false),
        ]
    );
    for column in OrderTick::COLUMNS {
        println!(
            "column name={} role={:?} type={:?} nullable={}",
            column.name, column.semantic_type, column.data_type, column.nullable
        );
    }

    let mut batch = OrderTickBatch::with_capacity(rows.len());
    let warmed_capacity = batch.row_capacity();
    let warmed_bytes = batch.allocated_bytes();
    assert!(batch.is_empty());
    assert!(warmed_capacity >= rows.len());
    assert!(warmed_bytes > 0);

    // The owned path copies the row into the typed buffers.
    batch.push(&rows[0]);
    // The borrowed path copies only flat bytes; no String or Vec is allocated.
    for row in &rows[1..] {
        batch.push_ref(&OrderTickRef {
            symbol: row.symbol.as_str(),
            ts: row.ts,
            best_bid: row.best_bid,
            size: row.size,
            payload: row.payload.as_str(),
            notional: row.notional,
            blob: row.blob.as_slice(),
        });
    }

    assert_eq!(batch.row_count(), rows.len());
    assert_eq!(batch.len(), rows.len());
    assert_eq!(batch.row_capacity(), warmed_capacity);
    assert_eq!(batch.allocated_bytes(), warmed_bytes);
    println!(
        "batch rows={} row_capacity={} allocated_bytes={}",
        batch.row_count(),
        batch.row_capacity(),
        batch.allocated_bytes()
    );

    let expected = rows.to_vec();
    assert_eq!(batch.to_owned_rows(), expected);
    assert!(batch.get_view(rows.len()).is_none());

    let view = batch.get_view(0).expect("row 0 is present");
    assert_eq!(view.symbol, "BTCUSDT");
    assert_eq!(view.ts, rows[0].ts);
    assert_eq!(view.best_bid, 100.25);
    assert_eq!(view.size, Some(1.5));
    assert_eq!(view.payload, rows[0].payload);
    assert_eq!(view.notional, Decimal128::new(123_456, 18, 4));
    assert_eq!(view.blob, rows[0].blob.as_slice());
    assert_eq!(view.to_owned_row(), rows[0]);

    let borrowed = BorrowedOrderTick::from_view(view);
    assert_eq!(borrowed.symbol, "BTCUSDT");
    assert_eq!(borrowed.payload.0, rows[0].payload);
    // The wrapper and the view point at the same bytes in the batch.
    assert_eq!(borrowed.payload.0.as_ptr(), view.payload.as_ptr());
    assert_eq!(borrowed.blob.as_ptr(), view.blob.as_ptr());
    assert_eq!(borrowed.notional, Decimal128::new(123_456, 18, 4));
    assert_eq!(borrowed.size, Some(1.5));

    let timestamps: Vec<i64> = batch.iter_views().map(|row| row.ts).collect();
    assert_eq!(
        timestamps,
        rows.iter().map(|row| row.ts).collect::<Vec<_>>()
    );
    assert_eq!(batch.iter_views().len(), rows.len());
    assert_eq!(
        batch.iter_views().map(|row| row.size).collect::<Vec<_>>(),
        [Some(1.5), None, Some(0.0)]
    );

    let record = batch_to_record_batch(&batch).expect("typed batch converts to Arrow");
    assert_eq!(record.num_columns(), OrderTick::COLUMNS.len());
    assert_eq!(record.num_rows(), rows.len());

    let schema = record.schema();
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(
        names,
        [
            "symbol", "ts", "best_bid", "size", "payload", "notional", "blob"
        ]
    );
    assert_eq!(arrow_field(&schema, "symbol").data_type(), &DataType::Utf8);
    assert!(arrow_field(&schema, "symbol").is_nullable());
    assert_eq!(
        arrow_field(&schema, "ts").data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert!(!arrow_field(&schema, "ts").is_nullable());
    assert_eq!(
        arrow_field(&schema, "best_bid").data_type(),
        &DataType::Float64
    );
    assert_eq!(arrow_field(&schema, "size").data_type(), &DataType::Float64);
    assert_eq!(
        arrow_field(&schema, "payload").data_type(),
        &DataType::Binary
    );
    assert_eq!(
        arrow_field(&schema, "notional").data_type(),
        &DataType::Decimal128(18, 4)
    );
    assert_eq!(arrow_field(&schema, "blob").data_type(), &DataType::Binary);

    assert_eq!(
        arrow_field(&schema, "ts")
            .metadata()
            .get(SEMANTIC_TYPE_KEY)
            .map(String::as_str),
        Some("timestamp")
    );
    assert_eq!(
        arrow_field(&schema, "best_bid")
            .metadata()
            .get(SEMANTIC_TYPE_KEY)
            .map(String::as_str),
        Some("field")
    );
    assert_eq!(
        arrow_field(&schema, "payload")
            .metadata()
            .get(TYPE_EXTENSION_KEY)
            .map(String::as_str),
        Some(JSON_TYPE_VALUE)
    );
    assert!(
        arrow_field(&schema, "blob")
            .metadata()
            .get(TYPE_EXTENSION_KEY)
            .is_none()
    );
    println!(
        "arrow rows={} columns={} json_extension={:?}",
        record.num_rows(),
        record.num_columns(),
        arrow_field(&schema, "payload")
            .metadata()
            .get(TYPE_EXTENSION_KEY)
    );

    let decoded = OrderTickBatch::from_record_batch(&record).expect("decode by column name");
    assert_eq!(decoded.row_count(), rows.len());
    assert_eq!(decoded.to_owned_rows(), expected);
    assert_eq!(decoded.get_view(1).expect("row 1 is present").size, None);
    assert_eq!(
        decoded.get_view(0).expect("row 0 is present").payload,
        rows[0].payload
    );

    let through_trait =
        <OrderTick as RowDecode>::decode_record_batch(&record).expect("trait decode");
    assert_eq!(through_trait.to_owned_rows(), expected);

    // Reverse the physical column order: name-bound decoding must not care.
    let order = [6usize, 5, 4, 3, 2, 1, 0];
    let fields: Vec<Field> = order
        .iter()
        .map(|&index| schema.field(index).clone())
        .collect();
    let columns: Vec<ArrayRef> = order
        .iter()
        .map(|&index| Arc::clone(record.column(index)))
        .collect();
    let reordered = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("reordered batch keeps the original column types");
    assert_eq!(reordered.schema().field(0).name(), "blob");
    assert_eq!(reordered.schema().field(6).name(), "symbol");

    let by_name = <OrderTick as RowDecode>::decode_record_batch(&reordered)
        .expect("name-bound decode ignores physical order");
    assert_eq!(by_name.to_owned_rows(), expected);
    println!(
        "reordered decode rows={} first_column={}",
        by_name.row_count(),
        reordered.schema().field(0).name()
    );
    live_probe()?;
    Ok(())
}

fn live_probe() -> Result<(), Box<dyn std::error::Error>> {
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
