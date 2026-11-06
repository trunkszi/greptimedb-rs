//! Example 04: the full GreptimeDB `ColumnDataType` matrix.
//!
//! One `ColumnarBatch` carries every supported column type exactly once: a
//! `String` tag, the single `Timestamp` role, and 19 `Field` columns. The three
//! timestamp-typed columns that are not the time index stay `Field`, because a
//! table has exactly one `Timestamp` role and that role rejects nulls.
//!
//! The batch is encoded as a GreptimeDB insert request, converted to an Arrow
//! `RecordBatch`, decoded back with the query decoder, and re-encoded to prove
//! that every value, null mask, and type extension survives the round trip.
//!
//! Run with `cargo run -p greptimedb --example 04_full_column_type_matrix`.

use std::collections::HashSet;

use arrow_array::Array;
use greptimedb::batch::BatchEncode;
use greptimedb::bulk::schema::{JSON_TYPE_VALUE, SEMANTIC_TYPE_KEY, TYPE_EXTENSION_KEY};
use greptimedb::bulk::{DataType, batch_to_record_batch};
use greptimedb::proto::v1::Column;
use greptimedb::proto::v1::column_data_type_extension::TypeExt;
use greptimedb::query::decode_record_batch_to_columnar_batch;
use greptimedb::{
    ColumnDataType, ColumnarBatch, Date, DateTime, Decimal128, RecordBatch, Result, SemanticType,
    ValueRef,
    client::Client,
    config::{ClientOptions, Endpoint},
};

const TABLE: &str = "type_matrix";
const COLUMN_COUNT: usize = 21;
/// The only column allowed to carry `SemanticType::Timestamp`.
const TIME_INDEX: usize = 1;
const JSON_INDEX: usize = 14;
const DECIMAL_INDEX: usize = 20;
const DECIMAL_PRECISION: u32 = 38;
const DECIMAL_SCALE: u32 = 9;
const DECIMAL_TYPE: ColumnDataType = ColumnDataType::Decimal128 {
    precision: DECIMAL_PRECISION,
    scale: DECIMAL_SCALE,
};

const TAG_VALUE: &str = "BTCUSDT";
const JSON_DOC: &str = r#"{"venue":"bitget","leg":1}"#;
const BLOB: [u8; 4] = [0xde, 0xad, 0xbe, 0xef];
const DECIMAL_VALUE: i128 = 123_456_789_012_345_678_901_234_567_890;

const SCHEMA: [(&str, SemanticType, ColumnDataType); COLUMN_COUNT] = [
    ("symbol", SemanticType::Tag, ColumnDataType::String),
    (
        "ts",
        SemanticType::Timestamp,
        ColumnDataType::TimestampMillisecond,
    ),
    ("flag", SemanticType::Field, ColumnDataType::Boolean),
    ("n_i8", SemanticType::Field, ColumnDataType::Int8),
    ("n_i16", SemanticType::Field, ColumnDataType::Int16),
    ("n_i32", SemanticType::Field, ColumnDataType::Int32),
    ("n_i64", SemanticType::Field, ColumnDataType::Int64),
    ("n_u8", SemanticType::Field, ColumnDataType::Uint8),
    ("n_u16", SemanticType::Field, ColumnDataType::Uint16),
    ("n_u32", SemanticType::Field, ColumnDataType::Uint32),
    ("n_u64", SemanticType::Field, ColumnDataType::Uint64),
    ("n_f32", SemanticType::Field, ColumnDataType::Float32),
    ("n_f64", SemanticType::Field, ColumnDataType::Float64),
    ("blob", SemanticType::Field, ColumnDataType::Binary),
    ("doc", SemanticType::Field, ColumnDataType::Json),
    ("day", SemanticType::Field, ColumnDataType::Date),
    ("at", SemanticType::Field, ColumnDataType::DateTime),
    (
        "ts_sec",
        SemanticType::Field,
        ColumnDataType::TimestampSecond,
    ),
    (
        "ts_micro",
        SemanticType::Field,
        ColumnDataType::TimestampMicrosecond,
    ),
    (
        "ts_nano",
        SemanticType::Field,
        ColumnDataType::TimestampNanosecond,
    ),
    ("amount", SemanticType::Field, DECIMAL_TYPE),
];

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let batch = build_batch()?;

    println!("schema: {COLUMN_COUNT} columns, one per ColumnDataType variant");
    for column in batch.columns() {
        let group = type_group(column.data_type());
        let (name, data_type, role) = (column.name(), column.data_type(), column.semantic_type());
        println!("{group:<16} {name:<9} {data_type:?} as {role:?}");
    }

    // `type_group` matches every ColumnDataType variant exhaustively, so 21
    // distinct column types here means all 21 variants are covered.
    let covered: HashSet<ColumnDataType> = batch.columns().iter().map(|c| c.data_type()).collect();
    assert_eq!(batch.columns().len(), COLUMN_COUNT);
    assert_eq!(covered.len(), COLUMN_COUNT, "duplicate column types");

    let request = batch.encode_insert_request()?;
    assert_eq!(request.table_name, TABLE);
    assert_eq!(request.columns.len(), COLUMN_COUNT);
    assert_eq!(request.row_count, 2);

    // Row 0 is fully non-null; row 1 is null in every nullable column. The time
    // index is the one column whose buffer rejects nulls, so it holds values in
    // both rows.
    for (index, column) in request.columns.iter().enumerate() {
        assert!(!is_null(column, 0), "column {} row 0", column.column_name);
        if index == TIME_INDEX {
            assert!(
                column.null_mask.is_empty(),
                "the time index has a null mask"
            );
            assert!(!is_null(column, 1), "the time index row 1 is null");
        } else {
            assert_eq!(
                column.null_mask,
                [0b0000_0010],
                "column {} null row",
                column.column_name
            );
        }
    }
    assert_eq!(
        request.columns[0].semantic_type,
        SemanticType::Tag.to_proto()
    );
    assert_eq!(
        request.columns[TIME_INDEX].semantic_type,
        SemanticType::Timestamp.to_proto()
    );
    // Only one Timestamp role is allowed; the other timestamp-typed columns are fields.
    for column in &request.columns[TIME_INDEX + 1..] {
        assert_eq!(column.semantic_type, SemanticType::Field.to_proto());
    }

    let json_column = &request.columns[JSON_INDEX];
    let Some(TypeExt::JsonType(kind)) = type_extension(json_column) else {
        panic!("the JSON column is missing its type extension");
    };
    assert_eq!(
        *kind,
        greptimedb::proto::v1::JsonTypeExtension::JsonBinary as i32
    );
    assert_eq!(
        json_column.datatype,
        ColumnDataType::Json.to_proto_datatype()
    );

    let decimal_column = &request.columns[DECIMAL_INDEX];
    let Some(TypeExt::DecimalType(decimal)) = type_extension(decimal_column) else {
        panic!("the decimal column is missing its precision and scale");
    };
    assert_eq!(
        (decimal.precision, decimal.scale),
        (DECIMAL_PRECISION as i32, DECIMAL_SCALE as i32)
    );

    let record = batch_to_record_batch(&batch)?;
    assert_eq!(record.num_columns(), COLUMN_COUNT);
    assert_eq!(record.num_rows(), 2);
    let fields = record.schema();
    for (index, field) in fields.fields().iter().enumerate() {
        assert_eq!(field.name(), SCHEMA[index].0);
        assert_eq!(field.is_nullable(), index != TIME_INDEX);
        let role = field.metadata().get(SEMANTIC_TYPE_KEY).map(String::as_str);
        assert_eq!(role, Some(SCHEMA[index].1.as_flight_str()));
        assert!(!record.column(index).is_null(0));
        assert_eq!(record.column(index).is_null(1), index != TIME_INDEX);
    }
    let json_field = fields.field(JSON_INDEX);
    let json_type = json_field
        .metadata()
        .get(TYPE_EXTENSION_KEY)
        .map(String::as_str);
    assert_eq!(json_type, Some(JSON_TYPE_VALUE));
    assert_eq!(json_field.data_type(), &DataType::Binary);
    assert_eq!(
        fields.field(DECIMAL_INDEX).data_type(),
        &DataType::Decimal128(DECIMAL_PRECISION as u8, DECIMAL_SCALE as i8)
    );

    let decoded = decode_record_batch_to_columnar_batch(&record)?;
    assert_eq!(decoded.columns().len(), COLUMN_COUNT);
    assert_eq!(
        decoded.columns()[TIME_INDEX].semantic_type(),
        SemanticType::Timestamp
    );

    let decoded = record_batch_to_columnar_batch(&record)?;
    // A decoded schema does not carry `greptime:table`, so compare under the
    // original destination instead of the empty decoded table name.
    let reencoded = decoded.encode_insert_request_for(&request.table_name)?;
    assert_eq!(reencoded.columns.len(), request.columns.len());

    let mut verified: HashSet<ColumnDataType> = HashSet::new();
    for (index, (original, round_tripped)) in
        request.columns.iter().zip(&reencoded.columns).enumerate()
    {
        assert_eq!(round_tripped.column_name, original.column_name);
        assert_eq!(round_tripped.datatype, original.datatype);
        assert_eq!(
            round_tripped.datatype_extension,
            original.datatype_extension
        );
        // Full column equality covers values, null mask, and semantic role.
        assert_eq!(round_tripped, original, "column {index} changed");
        verified.insert(decoded.columns()[index].data_type());
    }
    for index in [JSON_INDEX, DECIMAL_INDEX] {
        let (round_tripped, original) = (&reencoded.columns[index], &request.columns[index]);
        assert_eq!(
            round_tripped.datatype_extension,
            original.datatype_extension
        );
    }
    assert_eq!(verified, covered, "round-trip type coverage");

    println!(
        "json: datatype=binary greptime:type={JSON_TYPE_VALUE}; decimal: precision={DECIMAL_PRECISION} scale={DECIMAL_SCALE}"
    );
    println!(
        "{}/{} types round-tripped: values, null masks, names, datatypes, and extension metadata identical",
        verified.len(),
        COLUMN_COUNT
    );
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

/// Build the two-row matrix: one fully non-null row, one all-null row.
///
/// # Errors
/// Propagates column schema and row validation errors.
fn build_batch() -> Result<ColumnarBatch> {
    let mut batch = ColumnarBatch::new(TABLE);
    for (name, semantic_type, data_type) in SCHEMA {
        batch.add_column(name, semantic_type, data_type)?;
    }
    batch.push_row(&non_null_row())?;
    let mut null_row: [ValueRef<'_>; COLUMN_COUNT] = std::array::from_fn(|_| ValueRef::Null);
    null_row[TIME_INDEX] = ValueRef::TimestampMillisecond(1_700_000_000_124);
    batch.push_row(&null_row)?;
    Ok(batch)
}

fn non_null_row() -> [ValueRef<'static>; COLUMN_COUNT] {
    [
        ValueRef::String(TAG_VALUE),
        ValueRef::TimestampMillisecond(1_700_000_000_123),
        ValueRef::Boolean(true),
        ValueRef::Int8(-8),
        ValueRef::Int16(-1_600),
        ValueRef::Int32(-320_000),
        ValueRef::Int64(-6_400_000_000),
        ValueRef::Uint8(8),
        ValueRef::Uint16(1_600),
        ValueRef::Uint32(3_200_000),
        ValueRef::Uint64(6_400_000_000),
        ValueRef::Float32(1.5),
        ValueRef::Float64(-2.25),
        ValueRef::Binary(&BLOB),
        ValueRef::Json(JSON_DOC),
        ValueRef::Date(Date(19_700)),
        ValueRef::DateTime(DateTime(1_700_000_000_123)),
        ValueRef::TimestampSecond(1_700_000_000),
        ValueRef::TimestampMicrosecond(1_700_000_000_123_456),
        ValueRef::TimestampNanosecond(1_700_000_000_123_456_789),
        ValueRef::Decimal128(Decimal128::new(
            DECIMAL_VALUE,
            DECIMAL_PRECISION,
            DECIMAL_SCALE,
        )),
    ]
}

/// Local alias for the public decoder. The public API spells this conversion
/// `decode_record_batch_to_columnar_batch`; this helper names the Arrow-to-
/// columnar direction the round-trip check reads and delegates to it.
///
/// # Errors
/// Propagates the decoder's schema errors.
fn record_batch_to_columnar_batch(batch: &RecordBatch) -> Result<ColumnarBatch> {
    decode_record_batch_to_columnar_batch(batch)
}

/// Read one row's wire null bit. GreptimeDB packs the bits little-endian.
fn is_null(column: &Column, row: usize) -> bool {
    column
        .null_mask
        .get(row / 8)
        .is_some_and(|byte| byte & (1 << (row % 8)) != 0)
}

fn type_extension(column: &Column) -> Option<&TypeExt> {
    column.datatype_extension.as_ref()?.type_ext.as_ref()
}

fn type_group(data_type: ColumnDataType) -> &'static str {
    match data_type {
        ColumnDataType::Boolean
        | ColumnDataType::Int8
        | ColumnDataType::Int16
        | ColumnDataType::Int32
        | ColumnDataType::Int64
        | ColumnDataType::Uint8
        | ColumnDataType::Uint16
        | ColumnDataType::Uint32
        | ColumnDataType::Uint64 => "boolean/integer",
        ColumnDataType::Float32 | ColumnDataType::Float64 => "float",
        ColumnDataType::String | ColumnDataType::Binary | ColumnDataType::Json => {
            "text/binary/json"
        }
        ColumnDataType::Date
        | ColumnDataType::DateTime
        | ColumnDataType::TimestampSecond
        | ColumnDataType::TimestampMillisecond
        | ColumnDataType::TimestampMicrosecond
        | ColumnDataType::TimestampNanosecond => "temporal",
        ColumnDataType::Decimal128 { .. } => "decimal",
    }
}
