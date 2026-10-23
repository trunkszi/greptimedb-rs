//! Batch schema admission, atomic appends, projections, and insert/delete encoding.

use greptimedb::{
    BatchEncode, ColumnDataType, ColumnarBatch, MultiTableBatch, RowBatch, SemanticType,
    TimestampUnit, ValueRef,
};

// The second implementation makes inference ambiguous if a batch gains either auto trait.
const _: fn() = || {
    struct Marker;
    trait AmbiguousIfSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfSend<Marker> for T {}
    let _ = <ColumnarBatch as AmbiguousIfSend<_>>::check;
    let _ = <RowBatch as AmbiguousIfSend<_>>::check;
    let _ = <MultiTableBatch as AmbiguousIfSend<_>>::check;
    trait AmbiguousIfSync<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<Marker> for T {}
    let _ = <ColumnarBatch as AmbiguousIfSync<_>>::check;
    let _ = <RowBatch as AmbiguousIfSync<_>>::check;
    let _ = <MultiTableBatch as AmbiguousIfSync<_>>::check;
};

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
}

fn row_batch(table: &str) -> RowBatch {
    let mut batch = RowBatch::new(table);
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
}

use anyhow::Result;

#[test]
fn unit_scenario_invalid_table_overrides_do_not_change_valid_encoding() {
    let batch = columnar("ticks");
    assert!(batch.encode_insert_request_for("").is_err());
    assert!(batch.encode_delete_request_for("").is_err());
    assert_eq!(
        batch
            .encode_insert_request_for("archive")
            .unwrap()
            .table_name,
        "archive"
    );
    assert_eq!(batch.encode_insert_request().unwrap().table_name, "ticks");
    assert!(columnar("").encode_insert_request().is_err());

    let rows = row_batch("ticks");
    assert!(rows.encode_row_insert_request_for("").is_err());
    assert!(rows.encode_row_delete_request_for("").is_err());
    assert_eq!(
        rows.encode_row_insert_request_for("archive")
            .unwrap()
            .table_name,
        "archive"
    );
    assert_eq!(
        rows.encode_row_insert_request().unwrap().table_name,
        "ticks"
    );
    assert!(row_batch("").encode_row_insert_request().is_err());
}

#[test]
fn unit_scenario_timestamp_schema_is_mandatory_and_unique() {
    let mut columns = ColumnarBatch::new("ticks");
    columns
        .add_field_column("price", ColumnDataType::Float64)
        .unwrap();
    assert!(columns.validate_schema().is_err());
    columns
        .add_timestamp_column("time", TimestampUnit::Millisecond)
        .unwrap();
    assert!(columns.validate_schema().is_ok());
    assert!(
        columns
            .add_timestamp_column("other_time", TimestampUnit::Second)
            .is_err()
    );
    assert!(columns.validate_schema().is_ok());

    let mut rows = RowBatch::new("ticks");
    rows.add_column_schema("price", SemanticType::Field, ColumnDataType::Float64)
        .unwrap();
    assert!(rows.validate_schema().is_err());
    rows.add_column_schema(
        "time",
        SemanticType::Timestamp,
        ColumnDataType::TimestampMillisecond,
    )
    .unwrap();
    assert!(rows.validate_schema().is_ok());
    assert!(
        rows.add_column_schema(
            "other_time",
            SemanticType::Timestamp,
            ColumnDataType::TimestampSecond
        )
        .is_err()
    );
    assert!(rows.validate_schema().is_ok());
}

#[test]
fn unit_scenario_duplicate_and_empty_column_names_leave_schema_unchanged() {
    let mut columns = columnar("ticks");
    let before = columns.encode_insert_request().unwrap();
    assert!(
        columns
            .add_field_column("symbol", ColumnDataType::Float64)
            .is_err()
    );
    assert!(
        columns
            .add_field_column("", ColumnDataType::Float64)
            .is_err()
    );
    assert_eq!(columns.encode_insert_request().unwrap(), before);

    let mut rows = row_batch("ticks");
    let before = rows.encode_row_insert_request().unwrap();
    assert!(
        rows.add_column_schema("symbol", SemanticType::Field, ColumnDataType::Float64)
            .is_err()
    );
    assert!(
        rows.add_column_schema("", SemanticType::Field, ColumnDataType::Float64)
            .is_err()
    );
    assert_eq!(rows.encode_row_insert_request().unwrap(), before);
}

#[test]
fn unit_scenario_columnar_nullmask_crosses_byte_boundary_and_delete_filters_fields() {
    let mut batch = columnar("ticks");
    batch
        .add_field_column("payload", ColumnDataType::Binary)
        .unwrap();
    for row in 0..10 {
        batch
            .push_row(&[
                if row == 1 || row == 8 {
                    ValueRef::Null
                } else {
                    ValueRef::String("BTC")
                },
                ValueRef::TimestampMillisecond(1_700_000_000_000 + row),
                if row == 0 || row == 7 || row == 9 {
                    ValueRef::Null
                } else {
                    ValueRef::Float64(42.5)
                },
                if row == 2 {
                    ValueRef::Binary(&[0, 255])
                } else {
                    ValueRef::Null
                },
            ])
            .unwrap();
    }
    let insert = batch.encode_insert_request().unwrap();
    assert_eq!(insert.table_name, "ticks");
    assert_eq!(insert.row_count, 10);
    assert_eq!(
        insert
            .columns
            .iter()
            .map(|c| c.column_name.as_str())
            .collect::<Vec<_>>(),
        ["symbol", "time", "price", "payload"]
    );
    assert_eq!(insert.columns[0].semantic_type, 0);
    assert_eq!(insert.columns[1].semantic_type, 2);
    assert_eq!(insert.columns[2].semantic_type, 1);
    assert_eq!(insert.columns[0].null_mask, [0b0000_0010, 0b0000_0001]);
    assert_eq!(
        insert.columns[0].values.as_ref().unwrap().string_values,
        ["BTC"; 8]
    );
    assert!(insert.columns[1].null_mask.is_empty());
    assert_eq!(
        insert.columns[1]
            .values
            .as_ref()
            .unwrap()
            .timestamp_millisecond_values,
        [
            1_700_000_000_000,
            1_700_000_000_001,
            1_700_000_000_002,
            1_700_000_000_003,
            1_700_000_000_004,
            1_700_000_000_005,
            1_700_000_000_006,
            1_700_000_000_007,
            1_700_000_000_008,
            1_700_000_000_009
        ]
    );
    assert_eq!(insert.columns[2].null_mask, [0b1000_0001, 0b0000_0010]);
    assert_eq!(
        insert.columns[2].values.as_ref().unwrap().f64_values,
        [42.5; 7]
    );
    assert_eq!(insert.columns[3].null_mask, [0b1111_1011, 0b0000_0011]);
    assert_eq!(
        insert.columns[3].values.as_ref().unwrap().binary_values,
        [vec![0, 255]]
    );
    let delete = batch.encode_delete_request().unwrap();
    assert_eq!(delete.table_name, "ticks");
    assert_eq!(delete.row_count, 10);
    assert_eq!(delete.key_columns, insert.columns[..2]);
}

#[test]
fn unit_scenario_row_wire_values_and_delete_keys_preserve_schema_order() {
    use greptimedb_proto::v1::value::ValueData;
    let mut batch = RowBatch::new("ticks");
    batch
        .add_column_schema("price", SemanticType::Field, ColumnDataType::Float64)
        .unwrap();
    batch
        .add_column_schema(
            "time",
            SemanticType::Timestamp,
            ColumnDataType::TimestampMillisecond,
        )
        .unwrap();
    batch
        .add_column_schema("symbol", SemanticType::Tag, ColumnDataType::String)
        .unwrap();
    batch
        .push_row(&[
            ValueRef::Float64(42.5),
            ValueRef::TimestampMillisecond(1234),
            ValueRef::String("BTC"),
        ])
        .unwrap();
    batch
        .push_row(&[
            ValueRef::Null,
            ValueRef::TimestampMillisecond(1235),
            ValueRef::Null,
        ])
        .unwrap();
    let insert = batch.encode_row_insert_request().unwrap();
    assert_eq!(insert.table_name, "ticks");
    let rows = insert.rows.unwrap();
    assert_eq!(
        rows.schema
            .iter()
            .map(|c| c.column_name.as_str())
            .collect::<Vec<_>>(),
        ["price", "time", "symbol"]
    );
    assert_eq!(
        rows.schema
            .iter()
            .map(|c| c.semantic_type)
            .collect::<Vec<_>>(),
        [1, 2, 0]
    );
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(
        rows.rows[0]
            .values
            .iter()
            .map(|v| v.value_data.clone())
            .collect::<Vec<_>>(),
        [
            Some(ValueData::F64Value(42.5)),
            Some(ValueData::TimestampMillisecondValue(1234)),
            Some(ValueData::StringValue("BTC".into()))
        ]
    );
    assert_eq!(
        rows.rows[1]
            .values
            .iter()
            .map(|v| v.value_data.clone())
            .collect::<Vec<_>>(),
        [None, Some(ValueData::TimestampMillisecondValue(1235)), None]
    );
    let delete = batch.encode_row_delete_request().unwrap();
    assert_eq!(delete.table_name, "ticks");
    let keys = delete.rows.unwrap();
    assert_eq!(keys.schema, rows.schema[1..]);
    assert_eq!(keys.rows[0].values, rows.rows[0].values[1..]);
    assert_eq!(keys.rows[1].values, rows.rows[1].values[1..]);
}

#[test]
fn unit_scenario_failed_rows_are_atomic_for_both_builders() {
    let mut columns = columnar("ticks");
    let mut rows = row_batch("ticks");
    let good = [
        ValueRef::String("BTC"),
        ValueRef::TimestampMillisecond(1234),
        ValueRef::Float64(42.5),
    ];
    columns.push_row(&good).unwrap();
    rows.push_row(&good).unwrap();
    let column_before = columns.encode_insert_request().unwrap();
    let row_before = rows.encode_row_insert_request().unwrap();
    for invalid in [
        vec![
            ValueRef::String("ETH"),
            ValueRef::TimestampMillisecond(1235),
        ],
        vec![
            ValueRef::String("ETH"),
            ValueRef::TimestampMillisecond(1235),
            ValueRef::String("wrong last column"),
        ],
        vec![
            ValueRef::String("ETH"),
            ValueRef::Null,
            ValueRef::Float64(43.0),
        ],
        vec![
            ValueRef::String("ETH"),
            ValueRef::TimestampSecond(1235),
            ValueRef::Float64(43.0),
        ],
        vec![
            ValueRef::String("ETH"),
            ValueRef::TimestampMillisecond(1235),
            ValueRef::Float64(43.0),
            ValueRef::Null,
        ],
    ] {
        assert!(columns.push_row(&invalid).is_err());
        assert!(rows.push_row(&invalid).is_err());
        assert_eq!(columns.encode_insert_request().unwrap(), column_before);
        assert_eq!(rows.encode_row_insert_request().unwrap(), row_before);
    }
    assert!(
        columns
            .add_field_column("late", ColumnDataType::Int64)
            .is_err()
    );
    assert!(
        rows.add_column_schema("late", SemanticType::Field, ColumnDataType::Int64)
            .is_err()
    );
    assert_eq!(columns.encode_insert_request().unwrap(), column_before);
    assert_eq!(rows.encode_row_insert_request().unwrap(), row_before);
    columns.push_row(&good).unwrap();
    rows.push_row(&good).unwrap();
    assert_eq!(columns.row_count(), 2);
    assert_eq!(rows.row_count(), 2);
}

#[test]
fn unit_scenario_unequal_column_lengths_fail_before_encoding_and_can_be_completed() {
    let mut batch = columnar("ticks");
    batch
        .column_mut(0)
        .unwrap()
        .push(ValueRef::String("BTC"))
        .unwrap();
    assert!(batch.encode_insert_request().is_err());
    assert!(batch.encode_delete_request().is_err());
    assert!(
        batch
            .push_row(&[
                ValueRef::String("ETH"),
                ValueRef::TimestampMillisecond(2),
                ValueRef::Null
            ])
            .is_err()
    );
    batch
        .column_mut(1)
        .unwrap()
        .push(ValueRef::TimestampMillisecond(1))
        .unwrap();
    batch
        .column_mut(2)
        .unwrap()
        .push(ValueRef::Float64(42.5))
        .unwrap();
    let insert = batch.encode_insert_request().unwrap();
    assert_eq!(insert.row_count, 1);
    assert_eq!(
        insert.columns[0].values.as_ref().unwrap().string_values,
        ["BTC"]
    );
}

#[test]
fn unit_scenario_timestamp_semantic_rejects_non_temporal_types() {
    let mut columns = ColumnarBatch::new("ticks");
    assert!(
        columns
            .add_column("time", SemanticType::Timestamp, ColumnDataType::Int64)
            .is_err()
    );
    columns
        .add_timestamp_column("time", TimestampUnit::Nanosecond)
        .unwrap();
    columns
        .push_row(&[ValueRef::TimestampNanosecond(-1)])
        .unwrap();
    assert_eq!(
        columns.encode_insert_request().unwrap().columns[0]
            .values
            .as_ref()
            .unwrap()
            .timestamp_nanosecond_values,
        [-1]
    );
    let mut rows = RowBatch::new("ticks");
    assert!(
        rows.add_column_schema("time", SemanticType::Timestamp, ColumnDataType::String)
            .is_err()
    );
    rows.add_column_schema(
        "time",
        SemanticType::Timestamp,
        ColumnDataType::TimestampSecond,
    )
    .unwrap();
    rows.push_row(&[ValueRef::TimestampSecond(-1)]).unwrap();
    assert_eq!(rows.row_count(), 1);
}

#[test]
fn unit_scenario_multi_table_encoding_keeps_order_and_failed_add_is_atomic() {
    let mut ticks = columnar("ticks");
    ticks
        .push_row(&[
            ValueRef::String("BTC"),
            ValueRef::TimestampMillisecond(1),
            ValueRef::Float64(42.5),
        ])
        .unwrap();
    let mut trades = columnar("trades");
    trades
        .push_row(&[
            ValueRef::String("ETH"),
            ValueRef::TimestampMillisecond(2),
            ValueRef::Float64(43.0),
        ])
        .unwrap();
    let mut multi = MultiTableBatch::new();
    multi.add_batch(&ticks).unwrap();
    multi.add_batch_to("archive", &trades).unwrap();
    let before = multi.to_insert_requests();
    assert_eq!(multi.total_rows(), 2);
    assert_eq!(multi.table_count(), 2);
    assert!(multi.add_batch_to("", &ticks).is_err());
    assert!(multi.add_batch(&ColumnarBatch::new("broken")).is_err());
    assert_eq!(multi.to_insert_requests(), before);
    assert_eq!(
        before
            .inserts
            .iter()
            .map(|r| r.table_name.as_str())
            .collect::<Vec<_>>(),
        ["ticks", "archive"]
    );
    assert_eq!(before.inserts[0], ticks.encode_insert_request().unwrap());
    assert_eq!(
        before.inserts[1],
        trades.encode_insert_request_for("archive").unwrap()
    );
    assert_eq!(multi.into_insert_requests(), before);
}

#[test]
fn unit_scenario_decimal_json_and_binary_keep_wire_metadata_and_owned_values() {
    use greptimedb::{Date, Decimal128};
    use greptimedb_proto::v1::{column_data_type_extension::TypeExt, value::ValueData};
    let schema = [
        (
            "time",
            SemanticType::Timestamp,
            ColumnDataType::TimestampMicrosecond,
        ),
        (
            "amount",
            SemanticType::Field,
            ColumnDataType::Decimal128 {
                precision: 12,
                scale: 4,
            },
        ),
        ("meta", SemanticType::Field, ColumnDataType::Json),
        ("raw", SemanticType::Field, ColumnDataType::Binary),
        ("day", SemanticType::Field, ColumnDataType::Date),
    ];
    let mut columns = ColumnarBatch::new("trades");
    let mut rows = RowBatch::new("trades");
    for (name, role, datatype) in schema {
        columns.add_column(name, role, datatype).unwrap();
        rows.add_column_schema(name, role, datatype).unwrap();
    }
    {
        let json = String::from("{\"venue\":\"X\"}");
        let binary = vec![0, 255, 3];
        let values = [
            ValueRef::TimestampMicrosecond(5),
            ValueRef::Decimal128(Decimal128::new(-12345, 12, 4)),
            ValueRef::Json(&json),
            ValueRef::Binary(&binary),
            ValueRef::Date(Date(20_000)),
        ];
        columns.push_row(&values).unwrap();
        rows.push_row(&values).unwrap();
    }
    let column_before = columns.encode_insert_request().unwrap();
    let row_before = rows.encode_row_insert_request().unwrap();
    let invalid = [
        ValueRef::TimestampMicrosecond(6),
        ValueRef::Decimal128(Decimal128::new(1, 12, 3)),
        ValueRef::Null,
        ValueRef::Null,
        ValueRef::Null,
    ];
    assert!(columns.push_row(&invalid).is_err());
    assert!(rows.push_row(&invalid).is_err());
    assert_eq!(columns.encode_insert_request().unwrap(), column_before);
    assert_eq!(rows.encode_row_insert_request().unwrap(), row_before);
    let extension = column_before.columns[1]
        .datatype_extension
        .as_ref()
        .unwrap();
    let Some(TypeExt::DecimalType(decimal)) = extension.type_ext.as_ref() else {
        panic!("decimal extension missing")
    };
    assert_eq!((decimal.precision, decimal.scale), (12, 4));
    let amount = &column_before.columns[1]
        .values
        .as_ref()
        .unwrap()
        .decimal128_values[0];
    assert_eq!((amount.hi, amount.lo), (-1, -12345));
    assert_eq!(
        column_before.columns[2]
            .values
            .as_ref()
            .unwrap()
            .binary_values,
        [b"{\"venue\":\"X\"}".to_vec()]
    );
    assert_eq!(
        column_before.columns[3]
            .values
            .as_ref()
            .unwrap()
            .binary_values,
        [vec![0, 255, 3]]
    );
    assert_eq!(
        column_before.columns[4]
            .values
            .as_ref()
            .unwrap()
            .date_values,
        [20_000]
    );
    let encoded_rows = row_before.rows.unwrap();
    assert_eq!(
        encoded_rows.schema[1].datatype_extension,
        column_before.columns[1].datatype_extension
    );
    assert_eq!(
        encoded_rows.schema[2].datatype_extension,
        column_before.columns[2].datatype_extension
    );
    assert_eq!(
        encoded_rows.rows[0].values[2].value_data,
        Some(ValueData::BinaryValue(b"{\"venue\":\"X\"}".to_vec()))
    );
    assert_eq!(
        encoded_rows.rows[0].values[3].value_data,
        Some(ValueData::BinaryValue(vec![0, 255, 3]))
    );
    assert_eq!(
        encoded_rows.rows[0].values[4].value_data,
        Some(ValueData::DateValue(20_000))
    );
}

#[test]
fn unit_scenario_mutable_column_replacement_cannot_bypass_schema_checks() {
    use greptimedb::column_buf::TypedColumnBuf;
    let mut batch = columnar("ticks");
    for replacement in [
        TypedColumnBuf::new("symbol", SemanticType::Field, ColumnDataType::Float64),
        TypedColumnBuf::new(" ", SemanticType::Field, ColumnDataType::Float64),
        TypedColumnBuf::new(
            "other_time",
            SemanticType::Timestamp,
            ColumnDataType::TimestampSecond,
        ),
        TypedColumnBuf::new("other_time", SemanticType::Timestamp, ColumnDataType::Int64),
        TypedColumnBuf::new(
            "amount",
            SemanticType::Field,
            ColumnDataType::Decimal128 {
                precision: 0,
                scale: 0,
            },
        ),
    ] {
        let original = std::mem::replace(batch.column_mut(2).unwrap(), replacement);
        assert!(batch.encode_insert_request().is_err());
        assert!(batch.encode_delete_request().is_err());
        assert!(
            batch
                .push_row(&[
                    ValueRef::String("BTC"),
                    ValueRef::TimestampMillisecond(1),
                    ValueRef::Null
                ])
                .is_err()
        );
        assert!(batch.columns().iter().all(|column| column.row_count() == 0));
        *batch.column_mut(2).unwrap() = original;
        assert!(batch.validate_schema().is_ok());
    }
}

#[test]
fn unit_scenario_direct_decimal_appends_enforce_coefficient_precision() {
    use greptimedb::Decimal128;
    let mut batch = ColumnarBatch::new("trades");
    batch
        .add_timestamp_column("time", TimestampUnit::Second)
        .unwrap();
    batch
        .add_field_column(
            "amount",
            ColumnDataType::Decimal128 {
                precision: 2,
                scale: 0,
            },
        )
        .unwrap();
    batch
        .column_mut(0)
        .unwrap()
        .push(ValueRef::TimestampSecond(1))
        .unwrap();
    let append = batch
        .column_mut(1)
        .unwrap()
        .push(ValueRef::Decimal128(Decimal128::new(100, 2, 0)));
    assert!(
        append.is_err() || batch.encode_insert_request().is_err(),
        "100 cannot be represented by decimal precision 2"
    );
    assert!(batch.encode_delete_request().is_err());
}

#[test]
fn projected_result_rows_are_valid_but_cannot_be_inserted_without_a_time_index() -> Result<()> {
    let mut batch = ColumnarBatch::new("projection");
    batch.add_field_column("value", ColumnDataType::Int64)?;
    batch.add_field_column("label", ColumnDataType::String)?;
    batch.push_result_row(&[ValueRef::Int64(42), ValueRef::String("a")])?;
    batch.push_result_row(&[ValueRef::Null, ValueRef::String("b")])?;
    batch.validate_result_shape()?;
    assert!(batch.validate_schema().is_err());
    assert!(batch.encode_insert_request().is_err());
    assert!(batch.encode_delete_request().is_err());
    assert!(batch.push_result_row(&[ValueRef::Int64(1)]).is_err());
    assert!(
        batch
            .push_result_row(&[ValueRef::String("wrong"), ValueRef::String("c")])
            .is_err()
    );
    assert_eq!(batch.row_count(), 2);
    assert!(batch.columns().iter().all(|column| column.row_count() == 2));
    batch
        .column_mut(0)
        .expect("value")
        .push(ValueRef::Int64(9))?;
    assert!(batch.validate_result_shape().is_err());
    Ok(())
}

#[test]
fn result_time_index_remains_non_null_and_row_append_is_atomic() -> Result<()> {
    let mut batch = ColumnarBatch::new("metrics");
    batch.add_field_column("value", ColumnDataType::Int64)?;
    batch.add_timestamp_column("ts", TimestampUnit::Millisecond)?;
    assert!(
        batch
            .push_result_row(&[ValueRef::Int64(1), ValueRef::Null])
            .is_err()
    );
    assert_eq!(batch.row_count(), 0);
    assert!(batch.columns().iter().all(|column| column.row_count() == 0));
    batch.push_result_row(&[ValueRef::Int64(1), ValueRef::TimestampMillisecond(10)])?;
    batch.validate_result_shape()?;
    batch.encode_insert_request()?;
    Ok(())
}
