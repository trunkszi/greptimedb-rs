//! Typed columnar ingestion and borrowed view integration.
use greptimedb::{BorrowedRow, Row, batch::BatchEncode, buffer_policy::BufferPolicy, types::*};
#[derive(Debug, PartialEq, Row)]
#[row(table = "ticks", rename_all = "camelCase")]
struct TradeTick {
    #[greptime(tag, column = "symbol")]
    instrument_name: String,
    #[greptime(timestamp)]
    ts: i64,
    price: Option<f64>,
    payload: Option<Vec<u8>>,
    #[greptime(json)]
    document: String,
    #[greptime(decimal(precision = 38, scale = 10))]
    coefficient: i128,
}
#[test]
fn integration_borrowed_views_and_direct_wire_columns() {
    let mut batch = TradeTick::new_batch_with_capacity(16);
    for index in 0..16 {
        TradeTickRef {
            instrument_name: "BTC-USDT",
            ts: index,
            price: (!matches!(index, 0 | 7 | 8 | 15)).then_some(12.5),
            payload: (index % 2 == 0).then_some(&b"\0\xff"[..]),
            document: "{\"x\":1}",
            coefficient: -123,
        }
        .push_into_batch(&mut batch);
    }
    let arrow = greptimedb::bulk::batch_to_record_batch(&batch).unwrap();
    let decoded = TradeTickBatch::from_record_batch(&arrow).unwrap();
    assert_eq!(decoded.to_owned_rows(), batch.to_owned_rows());
    assert_eq!(batch.len(), 16);
    assert!(batch.get_view(16).is_none());
    let view = batch.get_view(0).unwrap();
    assert_eq!(view.payload, Some(&b"\0\xff"[..]));
    assert_eq!(view.price, None);
    assert_eq!(view.to_owned_row().instrument_name, "BTC-USDT");
    assert_eq!(
        batch.iter_views().map(|v| v.ts).collect::<Vec<_>>(),
        (0..16).collect::<Vec<_>>()
    );
    let request = batch.encode_insert_request_for("ticks_copy").unwrap();
    assert_eq!(request.table_name, "ticks_copy");
    assert_eq!(request.row_count, 16);
    assert_eq!(request.columns[2].null_mask, [0x81, 0x81]);
    assert_eq!(
        request.columns[2].values.as_ref().unwrap().f64_values,
        vec![12.5; 12]
    );
    assert_eq!(
        request.columns[4].datatype,
        ColumnDataType::Json.to_proto_datatype()
    );
    assert_eq!(
        request.columns[4].datatype_extension,
        ColumnDataType::Json.to_proto_extension()
    );
    assert_eq!(
        request.columns[4].values.as_ref().unwrap().binary_values[0],
        b"{\"x\":1}"
    );
    let decimal = &request.columns[5]
        .values
        .as_ref()
        .unwrap()
        .decimal128_values[0];
    assert_eq!((decimal.hi, decimal.lo), (-1, -123));
    let delete = batch.encode_delete_request().unwrap();
    assert_eq!(
        delete
            .key_columns
            .iter()
            .map(|c| c.column_name.as_str())
            .collect::<Vec<_>>(),
        ["symbol", "ts"]
    );
    assert!(batch.encode_insert_request_for("").is_err());
    assert!(TradeTick::COLUMNS[2].nullable);
}
#[derive(Debug, PartialEq, Row)]
struct ScalarOnly {
    #[greptime(timestamp, unit = "us")]
    ts: i64,
    flag: bool,
    small: i8,
    medium: i16,
    integer: i32,
    unsigned: u64,
    float: f32,
    day: Date,
    datetime: DateTime,
}
#[test]
fn integration_scalar_only_ref_and_logical_types() {
    let mut batch = ScalarOnlyBatch::new();
    batch.push_ref(&ScalarOnlyRef {
        ts: -5,
        flag: true,
        small: -8,
        medium: -16,
        integer: -32,
        unsigned: u64::MAX,
        float: 1.25,
        day: Date(-1),
        datetime: DateTime(-2),
    });
    let arrow = greptimedb::bulk::batch_to_record_batch(&batch).unwrap();
    let decoded = ScalarOnlyBatch::from_record_batch(&arrow).unwrap();
    assert_eq!(decoded.to_owned_rows(), batch.to_owned_rows());
    assert_eq!(batch.get_view(0).unwrap().to_owned_row().ts, -5);
    assert_eq!(ScalarOnly::TABLE_NAME, "scalar_only");
    let wire = batch.encode_insert_request().unwrap();
    assert_eq!(
        wire.columns[0]
            .values
            .as_ref()
            .unwrap()
            .timestamp_microsecond_values,
        [-5]
    );
    assert_eq!(wire.columns[2].values.as_ref().unwrap().i8_values, [-8]);
    assert_eq!(
        wire.columns[5].values.as_ref().unwrap().u64_values,
        [u64::MAX]
    );
    assert_eq!(wire.columns[7].values.as_ref().unwrap().date_values, [-1]);
}
#[derive(Row)]
struct BorrowedDocument<'a> {
    #[greptime(timestamp)]
    ts: TimestampNanosecond,
    #[greptime(json)]
    document: &'a str,
}
#[test]
fn integration_borrowed_source_and_reusable_storage() {
    let mut batch = BorrowedDocumentBatch::with_policy(4, BufferPolicy::unbounded());
    let mut source = String::from("{\"n\":1}");
    batch.extend_rows([&BorrowedDocument {
        ts: TimestampNanosecond(9),
        document: &source,
    }]);
    source.clear();
    assert_eq!(batch.get_view(0).unwrap().document, "{\"n\":1}");
    let bytes = batch.allocated_bytes();
    let ptr = batch.get_view(0).unwrap().document.as_ptr();
    batch.clear();
    batch.push_ref(&BorrowedDocumentRef {
        ts: TimestampNanosecond(10),
        document: "{\"n\":2}",
    });
    assert_eq!(batch.allocated_bytes(), bytes);
    assert_eq!(batch.get_view(0).unwrap().document.as_ptr(), ptr);
    assert_eq!(
        batch.get_view(0).unwrap().to_owned_row().ts,
        TimestampNanosecond(10)
    );
    batch.clear_with_policy(BufferPolicy::zero());
    assert_eq!(batch.allocated_bytes(), 0);
}
#[derive(Row)]
struct DecimalRow {
    #[greptime(timestamp)]
    ts: TimestampSecond,
    #[greptime(decimal(precision = 5, scale = 2))]
    value: Option<Decimal128>,
}
#[test]
fn integration_decimal_validation_rejects_inconsistent_values() {
    let mut batch = DecimalRowBatch::new();
    batch.push(&DecimalRow {
        ts: TimestampSecond(1),
        value: Some(Decimal128 {
            value: 123,
            precision: 5,
            scale: 2,
        }),
    });
    assert!(batch.encode_insert_request().is_ok());
    batch.push(&DecimalRow {
        ts: TimestampSecond(2),
        value: Some(Decimal128 {
            value: 123,
            precision: 6,
            scale: 2,
        }),
    });
    assert!(batch.validate_schema().is_err());
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.get_view(0).unwrap().value.unwrap().value, 123);
}
macro_rules! naming {
    ($name:ident, $rename:literal, $expected:literal) => {
        #[derive(Row)]
        #[row(rename_all = $rename)]
        struct $name {
            #[greptime(timestamp)]
            event_time: i64,
        }
        assert_eq!($name::COLUMNS[0].name, $expected);
    };
}
#[test]
fn unit_column_naming_conventions() {
    naming!(Snake, "snake_case", "event_time");
    naming!(Camel, "camelCase", "eventTime");
    naming!(Pascal, "PascalCase", "EventTime");
    naming!(Screaming, "SCREAMING_SNAKE_CASE", "EVENT_TIME");
    naming!(Kebab, "kebab-case", "event-time");
    naming!(Lower, "lowercase", "event_time");
    naming!(Upper, "UPPERCASE", "EVENT_TIME");
}

#[derive(Row)]
#[row(crate_path = "sdk")]
struct HTTPTradeTick {
    #[greptime(timestamp)]
    time: TimestampMillisecond,
    micros: TimestampMicrosecond,
    nanos: TimestampNanosecond,
    seconds: TimestampSecond,
    byte: u8,
    short: u16,
    int: u32,
    text: Option<String>,
}
use greptimedb as sdk;
#[test]
fn integration_wrapper_units_alias_and_nullable_text() {
    let mut batch = HTTPTradeTick::new_batch();
    batch.extend_rows([&HTTPTradeTick {
        time: TimestampMillisecond(-1),
        micros: TimestampMicrosecond(2),
        nanos: TimestampNanosecond(3),
        seconds: TimestampSecond(4),
        byte: 255,
        short: 65535,
        int: u32::MAX,
        text: Some("\u{e9}lan".into()),
    }]);
    batch.push_ref(&HTTPTradeTickRef {
        time: TimestampMillisecond(0),
        micros: TimestampMicrosecond(0),
        nanos: TimestampNanosecond(0),
        seconds: TimestampSecond(0),
        byte: 0,
        short: 0,
        int: 0,
        text: None,
    });
    let arrow = greptimedb::bulk::batch_to_record_batch(&batch).unwrap();
    let decoded = HTTPTradeTickBatch::from_record_batch(&arrow).unwrap();
    assert_eq!(
        decoded.encode_insert_request().unwrap(),
        batch.encode_insert_request().unwrap()
    );
    assert_eq!(HTTPTradeTick::TABLE_NAME, "http_trade_tick");
    assert_eq!(batch.get_view(0).unwrap().text, Some("\u{e9}lan"));
    assert_eq!(batch.get_view(1).unwrap().text, None);
    let request = batch.encode_insert_request().unwrap();
    assert_eq!(
        request.columns[0]
            .values
            .as_ref()
            .unwrap()
            .timestamp_millisecond_values,
        [-1, 0]
    );
    assert_eq!(
        request.columns[1]
            .values
            .as_ref()
            .unwrap()
            .timestamp_microsecond_values,
        [2, 0]
    );
    assert_eq!(
        request.columns[2]
            .values
            .as_ref()
            .unwrap()
            .timestamp_nanosecond_values,
        [3, 0]
    );
    assert_eq!(
        request.columns[3]
            .values
            .as_ref()
            .unwrap()
            .timestamp_second_values,
        [4, 0]
    );
    assert_eq!(
        request.columns[4].values.as_ref().unwrap().u8_values,
        [255, 0]
    );
    assert_eq!(
        request.columns[5].values.as_ref().unwrap().u16_values,
        [65535, 0]
    );
    assert_eq!(
        request.columns[6].values.as_ref().unwrap().u32_values,
        [u32::MAX, 0]
    );
    assert_eq!(request.columns[7].null_mask, [2]);
    assert_eq!(
        request.columns[7].values.as_ref().unwrap().string_values,
        ["\u{e9}lan"]
    );
    assert_eq!(
        TradeTick::COLUMNS[1].data_type,
        ColumnDataType::TimestampNanosecond
    );
}

#[test]
fn unit_batch_is_neither_send_nor_sync() {
    trait AmbiguousSend<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousSend<()> for T {}
    struct SendMarker;
    impl<T: ?Sized + Send> AmbiguousSend<SendMarker> for T {}
    let _ = <TradeTickBatch as AmbiguousSend<_>>::marker;
    trait AmbiguousSync<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousSync<()> for T {}
    struct SyncMarker;
    impl<T: ?Sized + Sync> AmbiguousSync<SyncMarker> for T {}
    let _ = <TradeTickBatch as AmbiguousSync<_>>::marker;
}

#[test]
fn lifecycle_retention_budget_is_aggregate_and_views_are_empty_after_clear() {
    let mut batch = TradeTick::new_batch_with_policy(1024, BufferPolicy::new(2, 512));
    let row = TradeTick {
        instrument_name: "x".repeat(1000),
        ts: 1,
        price: None,
        payload: Some(vec![1; 1000]),
        document: "{}".into(),
        coefficient: 0,
    };
    batch.extend_rows([&row, &row]);
    assert!(batch.allocated_bytes() > 512);
    batch.clear();
    assert!(batch.allocated_bytes() <= 512);
    assert!(batch.row_capacity() <= 2);
    assert!(batch.get_view(0).is_none());
    assert_eq!(batch.iter_views().count(), 0);
    batch.push(&row);
    assert_eq!(batch.get_view(0).unwrap().to_owned_row(), row);
    batch.shrink_to_fit();
    assert_eq!(batch.len(), 1);
}

#[test]
fn integration_lifecycle_nullable_mask_capacity_obeys_non_byte_aligned_row_limits() {
    let row = TradeTickRef {
        instrument_name: "ETH",
        ts: 1,
        price: None,
        payload: None,
        document: "{}",
        coefficient: 0,
    };
    for rows in [1_usize, 9] {
        let mut batch = TradeTickBatch::with_policy(rows, BufferPolicy::new(rows, usize::MAX));
        assert!(batch.row_capacity() > rows);
        batch.push_ref(&row);
        let allocated = batch.allocated_bytes();
        batch.clear();
        assert!(batch.row_capacity() <= rows);
        assert!(batch.allocated_bytes() < allocated);
        assert!(batch.get_view(0).is_none());
        batch.push_ref(&row);
        let request = batch.encode_insert_request().unwrap();
        assert_eq!(request.row_count, 1);
        assert_eq!(request.columns[2].null_mask, [1]);
        assert_eq!(request.columns[3].null_mask, [1]);
    }
}

#[test]
fn integration_scenario_insert_and_delete_reject_blank_table_overrides() {
    let batch = ScalarOnlyBatch::new();
    assert!(batch.encode_insert_request_for("ticks").is_ok());
    assert!(batch.encode_delete_request_for("ticks").is_ok());
    for table in ["", " ", "\t\n"] {
        assert!(batch.encode_insert_request_for(table).is_err());
        assert!(batch.encode_delete_request_for(table).is_err());
    }
}

struct CountingAllocator;
thread_local! {
    static ALLOCATIONS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}
// SAFETY: All operations forward the identical pointer, size and alignment to
// System; the thread-local counter never owns or changes allocated memory.
unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get().map(|n| n + 1)));
        // SAFETY: The caller supplies the valid layout required by GlobalAlloc.
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: The pointer and layout are passed through from the caller.
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get().map(|n| n + 1)));
        // SAFETY: System receives the original allocation and requested size.
        unsafe { std::alloc::System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn lifecycle_warmed_borrowed_push_and_view_iteration_allocate_nothing() {
    let mut batch = TradeTickBatch::with_capacity(32);
    let row = TradeTickRef {
        instrument_name: "ETH",
        ts: 1,
        price: Some(1.5),
        payload: Some(b"bytes"),
        document: "{}",
        coefficient: 123,
    };
    for _ in 0..32 {
        batch.push_ref(&row);
    }
    batch.clear_unbounded();
    ALLOCATIONS.with(|count| count.set(Some(0)));
    for _ in 0..32 {
        batch.push_ref(&row);
    }
    let sum: i64 = batch.iter_views().map(|view| view.ts).sum();
    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(sum, 32);
    assert_eq!(count, 0);
}

mod arrow_decode {
    use super::*;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, Decimal128Array, DictionaryArray,
        Int32Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, StringViewArray,
        TimestampNanosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use greptimedb::{error::Error, row::RowDecode};
    use std::{collections::HashMap, sync::Arc};

    fn record(columns: Vec<(&str, ArrayRef, HashMap<String, String>)>) -> RecordBatch {
        let fields: Vec<_> = columns
            .iter()
            .map(|(name, array, metadata)| {
                Field::new(*name, array.data_type().clone(), true).with_metadata(metadata.clone())
            })
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns.into_iter().map(|(_, a, _)| a).collect(),
        )
        .unwrap()
    }
    fn meta(key: &str, value: &str) -> HashMap<String, String> {
        HashMap::from([(key.into(), value.into())])
    }
    fn timestamp() -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(vec![1, 2]))
    }
    #[derive(Debug, PartialEq, Row)]
    struct Decoded {
        #[greptime(timestamp)]
        ts: TimestampNanosecond,
        #[greptime(tag)]
        label: String,
        value: Option<i32>,
    }
    fn simple(label: ArrayRef) -> RecordBatch {
        record(vec![
            (
                "value",
                Arc::new(Int32Array::from(vec![Some(7), None])),
                HashMap::new(),
            ),
            ("label", label, HashMap::new()),
            ("ts", timestamp(), meta("greptime:time_index", "true")),
        ])
    }
    #[test]
    fn integration_reordered_live_arrow_to_owned_rows_and_borrowed_views() {
        let arrow = simple(Arc::new(StringArray::from(vec!["a", "b"])));
        let mut decoded =
            Decoded::decode_record_batch_with_policy(&arrow, BufferPolicy::zero()).unwrap();
        assert_eq!(
            decoded
                .iter_views()
                .map(|v| (v.ts.0, v.label, v.value))
                .collect::<Vec<_>>(),
            [(1, "a", Some(7)), (2, "b", None)]
        );
        assert_eq!(
            decoded.to_owned_rows(),
            [
                Decoded {
                    ts: TimestampNanosecond(1),
                    label: "a".into(),
                    value: Some(7)
                },
                Decoded {
                    ts: TimestampNanosecond(2),
                    label: "b".into(),
                    value: None
                }
            ]
        );
        let first = decoded.get_view(0).unwrap();
        assert_eq!(
            first.label.as_ptr(),
            decoded.get_view(0).unwrap().label.as_ptr()
        );
        assert_eq!(first.to_owned_row().label, "a");
        assert!(decoded.get_view(2).is_none());
        let wire = decoded.encode_insert_request().unwrap();
        assert_eq!(wire.columns[2].null_mask, [2]);
        assert_eq!(DecodedBatch::from_record_batch(&arrow).unwrap().len(), 2);
        assert_eq!(Decoded::decode_record_batch(&arrow).unwrap().len(), 2);
        decoded.clear();
        assert_eq!(decoded.allocated_bytes(), 0);
    }
    #[test]
    fn integration_dictionary_integer_keys_and_string_representations() {
        macro_rules! dictionary {
            ($key:ty) => {{
                let dict = DictionaryArray::<$key>::from_iter([Some("a"), Some("b")]);
                let decoded = DecodedBatch::from_record_batch(&simple(Arc::new(dict))).unwrap();
                assert_eq!(
                    decoded.iter_views().map(|v| v.label).collect::<Vec<_>>(),
                    ["a", "b"]
                );
            }};
        }
        use arrow_array::types::*;
        dictionary!(Int8Type);
        dictionary!(Int16Type);
        dictionary!(Int32Type);
        dictionary!(Int64Type);
        dictionary!(UInt8Type);
        dictionary!(UInt16Type);
        dictionary!(UInt32Type);
        dictionary!(UInt64Type);
        for labels in [
            Arc::new(LargeStringArray::from(vec!["a", "b"])) as ArrayRef,
            Arc::new(StringViewArray::from(vec!["a", "b"])),
        ] {
            assert_eq!(
                DecodedBatch::from_record_batch(&simple(labels))
                    .unwrap()
                    .get_view(1)
                    .unwrap()
                    .label,
                "b"
            );
        }
    }
    #[derive(Debug, PartialEq, Row)]
    struct BorrowedArrow<'a> {
        #[greptime(timestamp)]
        ts: TimestampNanosecond,
        text: &'a str,
        payload: &'a [u8],
        #[greptime(json)]
        document: &'a str,
        #[greptime(decimal(precision = 5, scale = 2))]
        decimal: Option<Decimal128>,
    }
    #[test]
    fn integration_borrowed_decimal_json_and_large_view_arrays() {
        for (text, payload, json) in [
            (
                Arc::new(LargeStringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(LargeBinaryArray::from(vec![
                    b"x".as_slice(),
                    b"y".as_slice(),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec!["{}", "[]"])) as ArrayRef,
            ),
            (
                Arc::new(StringViewArray::from(vec!["a", "b"])),
                Arc::new(BinaryViewArray::from(vec![
                    b"x".as_slice(),
                    b"y".as_slice(),
                ])),
                Arc::new(BinaryArray::from(vec![b"{}".as_slice(), b"[]".as_slice()])),
            ),
        ] {
            let decimals = Decimal128Array::from(vec![Some(-123), None])
                .with_precision_and_scale(5, 2)
                .unwrap();
            let arrow = record(vec![
                ("document", json, meta("greptime:type", "Json")),
                ("decimal", Arc::new(decimals), HashMap::new()),
                ("payload", payload, HashMap::new()),
                ("text", text, HashMap::new()),
                ("ts", timestamp(), HashMap::new()),
            ]);
            let decoded = BorrowedArrowBatch::from_record_batch(&arrow).unwrap();
            drop(arrow);
            let view = decoded.get_view(0).unwrap();
            assert_eq!(
                (view.text, view.payload, view.document),
                ("a", b"x".as_slice(), "{}")
            );
            assert_eq!(view.decimal, Some(Decimal128::new(-123, 5, 2)));
            let owned = decoded.to_owned_rows();
            assert_eq!(
                owned[1],
                BorrowedArrow {
                    ts: TimestampNanosecond(2),
                    text: "b",
                    payload: b"y",
                    document: "[]",
                    decimal: None
                }
            );
            assert_eq!(view.to_owned_row().ts, TimestampNanosecond(1));
        }
    }
    #[test]
    fn integration_scenario_arrow_schema_and_nullability_fail_closed() {
        let labels = || Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let missing = record(vec![("ts", timestamp(), HashMap::new())]);
        assert!(matches!(
            DecodedBatch::from_record_batch(&missing),
            Err(Error::SchemaViolation(_))
        ));
        let duplicate = record(vec![
            ("ts", timestamp(), HashMap::new()),
            ("label", labels(), HashMap::new()),
            ("label", labels(), HashMap::new()),
            (
                "value",
                Arc::new(Int32Array::from(vec![1, 2])),
                HashMap::new(),
            ),
        ]);
        assert!(matches!(
            DecodedBatch::from_record_batch(&duplicate),
            Err(Error::SchemaViolation(_))
        ));
        let wrong = simple(Arc::new(Int32Array::from(vec![1, 2])));
        assert!(matches!(
            DecodedBatch::from_record_batch(&wrong),
            Err(Error::SchemaViolation(_))
        ));
        let null = simple(Arc::new(StringArray::from(vec![Some("a"), None])));
        assert!(matches!(
            DecodedBatch::from_record_batch(&null),
            Err(Error::SchemaViolation(_))
        ));
        let null_timestamp = record(vec![
            (
                "ts",
                Arc::new(TimestampNanosecondArray::from(vec![Some(1), None])),
                HashMap::new(),
            ),
            ("label", labels(), HashMap::new()),
            (
                "value",
                Arc::new(Int32Array::from(vec![1, 2])),
                HashMap::new(),
            ),
        ]);
        assert!(matches!(
            DecodedBatch::from_record_batch(&null_timestamp),
            Err(Error::SchemaViolation(_))
        ));
        let role = record(vec![
            ("ts", timestamp(), meta("greptime:semantic_type", "field")),
            ("label", labels(), HashMap::new()),
            (
                "value",
                Arc::new(Int32Array::from(vec![1, 2])),
                HashMap::new(),
            ),
        ]);
        assert!(matches!(
            DecodedBatch::from_record_batch(&role),
            Err(Error::SchemaViolation(_))
        ));
        let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("label", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ])));
        assert!(DecodedBatch::from_record_batch(&empty).unwrap().is_empty());
    }
    #[test]
    fn integration_scenario_json_extension_and_decimal_metadata_are_validated() {
        for metadata in [HashMap::new(), meta("greptime:type", "NotJson")] {
            let arrow = record(vec![
                ("ts", timestamp(), HashMap::new()),
                (
                    "document",
                    Arc::new(StringArray::from(vec!["{}", "[]"])),
                    metadata,
                ),
            ]);
            assert!(matches!(
                BorrowedDocumentBatch::from_record_batch(&arrow),
                Err(Error::SchemaViolation(_))
            ));
        }
        let arrow = record(vec![
            (
                "ts",
                Arc::new(arrow_array::TimestampSecondArray::from(vec![1, 2])),
                HashMap::new(),
            ),
            (
                "value",
                Arc::new(
                    Decimal128Array::from(vec![Some(123), None])
                        .with_precision_and_scale(6, 2)
                        .unwrap(),
                ),
                HashMap::new(),
            ),
        ]);
        assert!(matches!(
            DecimalRowBatch::from_record_batch(&arrow),
            Err(Error::SchemaViolation(_))
        ));
    }
    mod shadow {
        use greptimedb::Row;
        macro_rules! vec {
            () => {
                0
            };
            ($($tt:tt)*) => {
                compile_error!("caller macro must not be used")
            };
        }
        struct Option;
        struct Result;
        #[derive(Row)]
        struct Shadow {
            #[greptime(timestamp)]
            ts: i64,
            label: String,
        }
        #[test]
        fn integration_scenario_caller_names_cannot_shadow_codegen() {
            let _ = (Option, Result);
            assert_eq!(vec![], 0);
            let mut batch = ShadowBatch::new();
            batch.push(&Shadow {
                ts: 1,
                label: "a".into(),
            });
            assert_eq!(batch.encode_insert_request().unwrap().columns.len(), 2);
            assert_eq!(batch.encode_delete_request().unwrap().key_columns.len(), 1);
        }
    }
}
