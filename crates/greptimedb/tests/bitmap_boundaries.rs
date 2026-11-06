//! Chunked null-mask and Arrow validity boundaries.
use greptimedb::{MemoryBuffer, NullMask};

/// Null chunks retain little-endian bit order across byte and word boundaries.
#[test]
fn null_words_cross_boundaries() {
    let mut mask = NullMask::with_capacity(140);
    for row in 0..140 {
        mask.push([0, 7, 63, 64, 65, 127, 139].contains(&row));
    }
    assert_eq!(mask.null_bitmap(0).raw(), 0x8000_0000_0000_0081);
    assert_eq!(mask.null_bitmap(64).raw(), 0x8000_0000_0000_0003);
    assert_eq!(mask.null_bitmap(127).raw(), 0x1001);
    assert_eq!(mask.null_bitmap(140).raw(), 0);
    assert_eq!(mask.null_count(), 7);
    mask.clear_unbounded();
    mask.push(false);
    assert_eq!(mask.null_bitmap(0).raw(), 0);
}

#[derive(greptimedb::Row)]
struct Sample {
    #[greptime(timestamp)]
    ts: greptimedb::types::TimestampNanosecond,
    value: Option<i64>,
}

/// Sliced Arrow validity and full-null words survive typed and wire round trips.
#[test]
fn sliced_validity_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    use arrow_array::{Array, Int64Array, RecordBatch, TimestampNanosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use greptimedb::{
        bulk::batch_to_record_batch, query::decode_record_batch_to_columnar_batch, row::RowDecode,
    };
    use std::{collections::HashMap, sync::Arc};
    let values = (0..150)
        .map(|row| {
            if row < 69 || row == 132 {
                None
            } else {
                Some(row * 2)
            }
        })
        .collect::<Vec<_>>();
    let schema = Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false).with_metadata(
            HashMap::from([("greptime:time_index".into(), "true".into())]),
        ),
        Field::new("value", DataType::Int64, true),
    ])
    .with_metadata(HashMap::from([(
        "greptime:table".into(),
        "bitmap_rows".into(),
    )]));
    let source = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(TimestampNanosecondArray::from((0..150).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    let sliced = source.slice(5, 137);
    let typed = Sample::decode_record_batch(&sliced)?;
    assert_eq!(typed.len(), 137);
    let dynamic = decode_record_batch_to_columnar_batch(&sliced)?;
    let round_trip = batch_to_record_batch(&dynamic)?;
    let column = round_trip
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for (row, view) in typed.iter_views().enumerate() {
        let expected = if row < 64 || row == 127 {
            None
        } else {
            Some((row as i64 + 5) * 2)
        };
        assert_eq!(view.ts.0, row as i64 + 5);
        assert_eq!(view.value, expected);
        assert_eq!(column.is_null(row), expected.is_none());
        if let Some(value) = expected {
            assert_eq!(column.value(row), value);
        }
    }
    assert_eq!(
        dynamic.columns()[1].null_mask().null_bitmap(0).raw(),
        u64::MAX
    );
    Ok(())
}

/// Compact storage preserves every logical variant and full decimal metadata.
#[test]
fn column_descriptor_keeps_metadata() {
    use greptimedb::{ColumnDataType::*, SemanticType, TypedColumnBuf};
    for kind in [
        Boolean,
        Int8,
        Int16,
        Int32,
        Int64,
        Uint8,
        Uint16,
        Uint32,
        Uint64,
        Float32,
        Float64,
        Binary,
        String,
        Date,
        DateTime,
        TimestampSecond,
        TimestampMillisecond,
        TimestampMicrosecond,
        TimestampNanosecond,
        Json,
        Decimal128 {
            precision: u32::MAX,
            scale: u32::MAX - 1,
        },
    ] {
        let column = TypedColumnBuf::new("value", SemanticType::Field, kind);
        assert_eq!(column.data_type(), kind);
    }
}

/// Hour partitioning floors negative timestamps and handles both signed extremes.
#[test]
fn hour_windows_keep_signed_boundaries() -> Result<(), Box<dyn std::error::Error>> {
    use arrow_array::{RecordBatch, TimestampSecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::{collections::HashMap, sync::Arc};
    let timestamps = vec![i64::MAX, 3600, -1, 0, -3600, 3599, -3601, i64::MIN];
    let schema = Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false).with_metadata(
            HashMap::from([("greptime:semantic_type".into(), "timestamp".into())]),
        ),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(TimestampSecondArray::from(timestamps))],
    )?;
    let parts = greptimedb::bulk::partition_record_batch_by_hour(&batch)?;
    let groups = parts
        .iter()
        .map(|part| {
            part.column(0)
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        groups,
        [
            vec![i64::MIN],
            vec![-3601],
            vec![-1, -3600],
            vec![0, 3599],
            vec![3600],
            vec![i64::MAX]
        ]
    );
    Ok(())
}
