use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, TimestampNanosecondArray, types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use greptimedb::{
    BatchEncode, ColumnDataType, ColumnarBatch, Decimal128, RecordBatch, Row, SemanticType,
    TimestampUnit, ValueRef, query::decode_record_batch_to_columnar_batch, row::RowDecode,
};

type TestResult = anyhow::Result<()>;

#[derive(Row)]
struct TimestampRow {
    #[greptime(timestamp)]
    ts: i64,
}

#[test]
fn dictionary_time_index_decodes() -> TestResult {
    let values: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![10, 20]));
    let array: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![1, 0]),
        values,
    )?);
    let field =
        Field::new("ts", array.data_type().clone(), false).with_metadata(HashMap::from([(
            "greptime:semantic_type".into(),
            "timestamp".into(),
        )]));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array])?;
    let typed = TimestampRow::decode_record_batch(&batch)?;
    assert_eq!(
        typed
            .to_owned_rows()
            .iter()
            .map(|row| row.ts)
            .collect::<Vec<_>>(),
        [20, 10]
    );
    let decoded = decode_record_batch_to_columnar_batch(&batch)?;
    assert_eq!(decoded.column(0).map(|c| c.row_count()), Some(2));
    assert_eq!(
        decoded.column(0).map(|c| c.semantic_type()),
        Some(SemanticType::Timestamp)
    );
    assert_eq!(
        decoded.column(0).map(|c| c
            .to_proto_column()
            .values
            .map(|v| v.timestamp_nanosecond_values)),
        Some(Some(vec![20, 10]))
    );
    let field = batch
        .schema()
        .field(0)
        .clone()
        .with_metadata(HashMap::from([(
            "greptime:time_index".into(),
            "true".into(),
        )]));
    let marked =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), batch.columns().to_vec())?;
    let decoded = decode_record_batch_to_columnar_batch(&marked)?;
    assert_eq!(
        decoded.column(0).map(|c| c.semantic_type()),
        Some(SemanticType::Timestamp)
    );
    Ok(())
}

#[test]
fn direct_decimal_validates_before_encoding() -> TestResult {
    let mut batch = ColumnarBatch::new("samples");
    batch.add_timestamp_column("ts", TimestampUnit::Nanosecond)?;
    batch.add_field_column(
        "value",
        ColumnDataType::Decimal128 {
            precision: 2,
            scale: 0,
        },
    )?;
    batch
        .column_mut(0)
        .ok_or_else(|| anyhow::anyhow!("timestamp"))?
        .push(ValueRef::TimestampNanosecond(10))?;
    batch
        .column_mut(1)
        .ok_or_else(|| anyhow::anyhow!("decimal"))?
        .push(ValueRef::Decimal128(Decimal128::new(100, 2, 0)))?;
    assert!(batch.encode_insert_request().is_err());
    Ok(())
}

#[test]
fn invalid_time_marker_is_rejected() -> TestResult {
    let field =
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false).with_metadata(
            HashMap::from([("greptime:time_index".into(), "yes".into())]),
        );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(TimestampNanosecondArray::from(vec![10]))],
    )?;
    let result = decode_record_batch_to_columnar_batch(&batch);
    assert!(
        result.is_err(),
        "invalid time-index marker silently accepted: {result:?}"
    );
    Ok(())
}

#[test]
fn conflicting_time_marker_is_rejected() -> TestResult {
    for (role, marker) in [("timestamp", "false"), ("field", "true")] {
        let field = Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false)
            .with_metadata(HashMap::from([
                ("greptime:semantic_type".into(), role.into()),
                ("greptime:time_index".into(), marker.into()),
            ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(TimestampNanosecondArray::from(vec![10]))],
        )?;
        assert!(
            decode_record_batch_to_columnar_batch(&batch).is_err(),
            "conflicting role={role}, marker={marker}"
        );
    }
    Ok(())
}

#[test]
fn valid_false_time_marker_is_field() -> TestResult {
    let field =
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false).with_metadata(
            HashMap::from([("greptime:time_index".into(), "false".into())]),
        );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(TimestampNanosecondArray::from(vec![10]))],
    )?;
    let decoded = decode_record_batch_to_columnar_batch(&batch)?;
    assert_eq!(
        decoded.column(0).map(|c| c.semantic_type()),
        Some(SemanticType::Field)
    );
    Ok(())
}

#[test]
fn dictionary_null_time_index_is_rejected() -> TestResult {
    let values: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(10), None]));
    let array: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![1]),
        values,
    )?);
    let field =
        Field::new("ts", array.data_type().clone(), false).with_metadata(HashMap::from([(
            "greptime:semantic_type".into(),
            "timestamp".into(),
        )]));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array])?;
    assert!(TimestampRow::decode_record_batch(&batch).is_err());
    assert!(decode_record_batch_to_columnar_batch(&batch).is_err());
    Ok(())
}

#[test]
fn dictionary_timezones_are_rejected() -> TestResult {
    let values: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![10]).with_timezone("UTC"));
    let array: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0]),
        values,
    )?);
    let field =
        Field::new("ts", array.data_type().clone(), false).with_metadata(HashMap::from([(
            "greptime:semantic_type".into(),
            "timestamp".into(),
        )]));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array])?;
    assert!(decode_record_batch_to_columnar_batch(&batch).is_err());
    Ok(())
}
