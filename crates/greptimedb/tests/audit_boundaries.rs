//! Adversarial schema and bitmap contracts through public SDK interfaces.
use arrow_array::{
    Array, DictionaryArray, Int32Array, RecordBatch, StringArray, StructArray, types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema};
use greptimedb::{
    bulk::{FlightCompression, FlightIpcEncoder},
    error::Error,
};
use std::sync::Arc;

/// Rejected nested dictionaries cannot become accepted on a later encode attempt.
#[test]
fn nested_dictionary_retries_stay_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0]),
        Arc::new(StringArray::from(vec!["value"])),
    )?;
    let field = Arc::new(Field::new("encoded", dictionary.data_type().clone(), false));
    let nested = StructArray::from(vec![(field, Arc::new(dictionary) as arrow_array::ArrayRef)]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        nested.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(nested)])?;
    let mut encoder = FlightIpcEncoder::new(FlightCompression::None)?;
    let _schema_result = encoder.encode_schema(&schema);
    for attempt in 0..2 {
        assert!(
            matches!(encoder.encode_batch(&batch), Err(Error::Encode(_))),
            "nested dictionary accepted at attempt {attempt}"
        );
    }
    let plain = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )?;
    encoder.encode_schema(plain.schema().as_ref())?;
    assert!(!encoder.encode_batch(&plain)?.data_header.is_empty());
    Ok(())
}

/// Semantic annotations cannot bypass a nullable time-index schema rejection.
#[test]
fn nullable_time_index_annotations_rejected() -> Result<(), Box<dyn std::error::Error>> {
    use arrow_array::TimestampNanosecondArray;
    use arrow_schema::TimeUnit;
    use std::collections::HashMap;
    for metadata in [
        HashMap::from([("greptime:semantic_type".into(), "timestamp".into())]),
        HashMap::from([("greptime:time_index".into(), "true".into())]),
        HashMap::from([
            ("greptime:semantic_type".into(), "timestamp".into()),
            ("greptime:time_index".into(), "true".into()),
        ]),
    ] {
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                .with_metadata(metadata),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(TimestampNanosecondArray::from(vec![1]))],
        )?;
        assert!(matches!(
            greptimedb::query::decode_record_batch_to_columnar_batch(&batch),
            Err(Error::SchemaViolation(_))
        ));
    }
    Ok(())
}

/// Hour partitioning rejects nullable timestamp schemas even with non-null rows.
#[test]
fn nullable_partition_time_index_rejected() -> Result<(), Box<dyn std::error::Error>> {
    use arrow_array::TimestampNanosecondArray;
    use arrow_schema::TimeUnit;
    use std::collections::HashMap;
    let schema = Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true).with_metadata(
            HashMap::from([("greptime:semantic_type".into(), "timestamp".into())]),
        ),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(TimestampNanosecondArray::from(vec![1]))],
    )?;
    assert!(matches!(
        greptimedb::bulk::partition_record_batch_by_hour(&batch),
        Err(Error::SchemaViolation(_))
    ));
    Ok(())
}

/// Every Arrow container rejects a dictionary before mutating encoder state.
#[test]
fn dictionary_nesting_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    use arrow_schema::{UnionFields, UnionMode};
    let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let item = Arc::new(Field::new("item", dictionary, true));
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::clone(&item),
            ]
            .into(),
        ),
        false,
    ));
    let types = [
        DataType::List(Arc::clone(&item)),
        DataType::LargeList(Arc::clone(&item)),
        DataType::FixedSizeList(Arc::clone(&item), 1),
        DataType::ListView(Arc::clone(&item)),
        DataType::LargeListView(Arc::clone(&item)),
        DataType::Struct(vec![Arc::clone(&item)].into()),
        DataType::Map(entries, false),
        DataType::Union(
            UnionFields::try_new(vec![0], vec![Arc::clone(&item)])?,
            UnionMode::Sparse,
        ),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            item,
        ),
    ];
    for data_type in types {
        let schema = Schema::new(vec![Field::new("nested", data_type, true)]);
        let mut encoder = FlightIpcEncoder::new(FlightCompression::None)?;
        assert!(matches!(
            encoder.encode_schema(&schema),
            Err(Error::Encode(_))
        ));
    }
    let plain = StructArray::from(vec![(
        Arc::new(Field::new("plain", DataType::Int32, false)),
        Arc::new(Int32Array::from(vec![7])) as arrow_array::ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        plain.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(plain)])?;
    let mut encoder = FlightIpcEncoder::new(FlightCompression::None)?;
    encoder.encode_schema(&schema)?;
    assert!(!encoder.encode_batch(&batch)?.data_header.is_empty());
    Ok(())
}

/// Wide schemas and multi-word null masks retain every field and row.
#[test]
fn wide_schema_null_masks_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    use greptimedb::{BatchEncode, ColumnDataType, ColumnarBatch, TimestampUnit, ValueRef};
    let mut source = ColumnarBatch::with_capacity("wide", 145);
    source.add_timestamp_column("ts", TimestampUnit::Nanosecond)?;
    for column in 0..70 {
        source.add_field_column(format!("field{column}"), ColumnDataType::Int64)?;
    }
    for row in 0..145 {
        let mut values = vec![ValueRef::TimestampNanosecond(row)];
        for column in 0..70 {
            values.push(if [0, 63, 64, 127, 144].contains(&row) {
                ValueRef::Null
            } else {
                ValueRef::Int64(column * 1000 + row)
            });
        }
        source.push_row(&values)?;
    }
    let arrow = greptimedb::bulk::batch_to_record_batch(&source)?;
    let restored = greptimedb::query::decode_record_batch_to_columnar_batch(&arrow)?;
    assert_eq!(
        restored.encode_insert_request_for("wide")?,
        source.encode_insert_request()?
    );
    assert_eq!(restored.columns().len(), 71);
    for column in &restored.columns()[1..] {
        assert_eq!(
            column.null_mask().null_bitmap(0).raw(),
            0x8000_0000_0000_0001
        );
        assert_eq!(
            column.null_mask().null_bitmap(64).raw(),
            0x8000_0000_0000_0001
        );
        assert_eq!(column.null_mask().null_bitmap(128).raw(), 1 << 16);
        assert_eq!(column.null_mask().null_bitmap(usize::MAX).raw(), 0);
    }
    Ok(())
}
