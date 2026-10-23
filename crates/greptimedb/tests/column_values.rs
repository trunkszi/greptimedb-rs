//! Typed column values, null masks, validation, and protobuf wire encoding.

use greptimedb::column_buf::NullMask;
use greptimedb::{
    BufferPolicy, ColumnDataType, Date, DateTime, Decimal128, MemoryBuffer, SemanticType,
    TimestampUnit, TypedColumnBuf, ValueRef,
};
use greptimedb_proto::v1::{Column, column::Values, column_data_type_extension::TypeExt};

macro_rules! values {
    ($field:ident, $values:expr) => {
        Values {
            $field: $values,
            ..Values::default()
        }
    };
}

fn column(data_type: ColumnDataType) -> TypedColumnBuf {
    TypedColumnBuf::new("value", SemanticType::Field, data_type)
}

fn assert_wire(data_type: ColumnDataType, value: ValueRef<'_>, datatype: i32, expected: Values) {
    let mut buf = column(data_type);
    buf.push(ValueRef::Null).expect("field accepts null");
    buf.push(value).expect("matching non-null value");
    buf.push(ValueRef::Null).expect("field accepts null");
    assert_eq!(buf.row_count(), 3);
    assert_eq!(buf.non_null_len(), 1);
    assert_eq!(buf.null_mask().null_count(), 2);
    assert_eq!(buf.name(), "value");
    assert_eq!(buf.semantic_type(), SemanticType::Field);
    assert_eq!(buf.data_type(), data_type);
    assert_eq!(
        buf.to_proto_column(),
        Column {
            column_name: "value".to_owned(),
            semantic_type: 1,
            datatype,
            values: Some(expected),
            null_mask: vec![0b101],
            datatype_extension: data_type.to_proto_extension(),
            ..Column::default()
        }
    );
}

#[test]
fn null_mask_uses_little_endian_bits_across_byte_boundaries() {
    let mut mask = NullMask::with_capacity(24);
    for row in 0..18 {
        mask.push(matches!(row, 0 | 7 | 8 | 17));
    }
    assert_eq!(mask.row_count(), 18);
    assert_eq!(mask.null_count(), 4);
    assert_eq!(mask.non_null_count(), 14);
    assert!(mask.has_nulls());
    assert_eq!(mask.as_bytes(), &[0x81, 0x01, 0x02]);
    assert_eq!(mask.to_proto_bytes(), vec![0x81, 0x01, 0x02]);
    for row in 0..18 {
        assert_eq!(mask.is_null(row), matches!(row, 0 | 7 | 8 | 17));
    }
    assert!(!mask.is_null(18));
    assert!(!mask.is_null(usize::MAX));
}

#[test]
fn no_nulls_omit_wire_mask_and_clear_resets_stale_bits() {
    let mut mask = NullMask::with_capacity(64);
    assert!(mask.to_proto_bytes().is_empty());
    for _ in 0..17 {
        mask.push(true);
    }
    assert_eq!(mask.to_proto_bytes(), vec![0xff, 0xff, 0x01]);
    let capacity = mask.row_capacity();
    let bytes = mask.allocated_bytes();
    mask.clear_unbounded();
    assert_eq!(mask.row_count(), 0);
    assert_eq!(mask.null_count(), 0);
    assert_eq!(mask.row_capacity(), capacity);
    assert_eq!(mask.allocated_bytes(), bytes);
    for _ in 0..17 {
        mask.push(false);
    }
    assert_eq!(mask.as_bytes(), &[0, 0, 0]);
    assert_eq!(mask.non_null_count(), 17);
    assert!(!mask.has_nulls());
    assert!(mask.to_proto_bytes().is_empty());
    mask.clear_with_policy(BufferPolicy::zero());
    assert_eq!(mask.row_capacity(), 0);
    assert_eq!(mask.allocated_bytes(), 0);
}

#[test]
fn mask_shrink_preserves_rows_and_only_clear_discards_them() {
    let mut mask = NullMask::with_capacity(4096);
    for row in 0..9 {
        mask.push(row == 8);
    }
    mask.shrink_to_fit();
    assert_eq!(mask.row_count(), 9);
    assert_eq!(mask.to_proto_bytes(), vec![0, 1]);
    assert!(mask.row_capacity() >= 9);
    assert!(mask.row_capacity() < 4096);
    mask.clear_with_policy(BufferPolicy::new(0, usize::MAX));
    assert_eq!(mask.row_capacity(), 0);
}

#[test]
fn fixed_width_scalars_encode_into_their_distinct_wire_arrays() {
    macro_rules! scalar {
        ($kind:ident, $value:expr, $datatype:literal, $field:ident) => {
            (
                ColumnDataType::$kind,
                ValueRef::$kind($value),
                $datatype,
                values!($field, vec![$value]),
            )
        };
    }
    let fixtures = [
        scalar!(Boolean, true, 0, bool_values),
        scalar!(Int8, -128, 1, i8_values),
        scalar!(Int16, -32_768, 2, i16_values),
        scalar!(Int32, i32::MIN, 3, i32_values),
        scalar!(Int64, i64::MIN, 4, i64_values),
        scalar!(Uint8, 255, 5, u8_values),
        scalar!(Uint16, 65_535, 6, u16_values),
        scalar!(Uint32, u32::MAX, 7, u32_values),
        scalar!(Uint64, u64::MAX, 8, u64_values),
        scalar!(Float32, -1.5, 9, f32_values),
        scalar!(Float64, 0.125, 10, f64_values),
    ];
    for (data_type, value, datatype, values) in fixtures {
        assert_wire(data_type, value, datatype, values);
    }
}

#[test]
fn strings_and_binary_preserve_empty_and_borrowed_values() {
    assert_wire(
        ColumnDataType::String,
        ValueRef::String("tokyo\0"),
        12,
        values!(string_values, vec!["tokyo\0".to_owned()]),
    );
    assert_wire(
        ColumnDataType::Binary,
        ValueRef::Binary(&[0, 255, 128]),
        11,
        values!(binary_values, vec![vec![0, 255, 128]]),
    );
    let mut buf = column(ColumnDataType::String);
    let input = String::from("original");
    buf.push(ValueRef::String(&input)).expect("string");
    drop(input);
    buf.push(ValueRef::String("")).expect("empty string");
    let values = buf.to_proto_column().values.expect("values");
    assert_eq!(values.string_values, vec!["original", ""]);
    assert!(buf.to_proto_column().null_mask.is_empty());
}

#[test]
fn mismatch_and_timestamp_null_rejection_leave_buffers_unchanged() {
    let mut timestamp = TypedColumnBuf::new(
        "ts",
        SemanticType::Timestamp,
        ColumnDataType::TimestampNanosecond,
    );
    timestamp
        .push(ValueRef::TimestampNanosecond(42))
        .expect("timestamp");
    let before = timestamp.to_proto_column();
    assert!(timestamp.push(ValueRef::Null).is_err());
    assert!(timestamp.push(ValueRef::Int64(42)).is_err());
    assert_eq!(timestamp.to_proto_column(), before);
    assert_eq!(timestamp.row_count(), 1);
    let mut integer = column(ColumnDataType::Int8);
    integer.push(ValueRef::Null).expect("nullable field");
    let before = integer.to_proto_column();
    assert!(integer.push(ValueRef::Int16(127)).is_err());
    assert!(integer.push(ValueRef::String("127")).is_err());
    assert_eq!(integer.to_proto_column(), before);
    assert_eq!(integer.non_null_len(), 0);
}

#[test]
fn null_only_columns_do_not_create_placeholder_values() {
    let mut buf = column(ColumnDataType::Int64);
    for _ in 0..9 {
        buf.push(ValueRef::Null).expect("null field");
    }
    let proto = buf.to_proto_column();
    assert_eq!(proto.null_mask, vec![255, 1]);
    assert_eq!(proto.values, Some(Values::default()));
    assert_eq!(buf.non_null_len(), 0);
    assert_eq!(buf.row_count(), 9);
}

#[test]
fn decimal_split_round_trips_signed_extremes_and_enforces_schema() {
    for value in [i128::MIN, -1, 0, 1, i128::MAX] {
        let decimal = Decimal128::new(value, 38, 9);
        let proto = decimal.to_proto();
        let (hi, lo) = match value {
            i128::MIN => (i64::MIN, 0),
            -1 => (-1, u64::MAX),
            0 => (0, 0),
            1 => (0, 1),
            _ => (i64::MAX, u64::MAX),
        };
        assert_eq!(decimal.hi(), hi);
        assert_eq!(decimal.lo(), lo);
        assert_eq!(proto.hi, hi);
        assert_eq!(proto.lo as u64, lo);
        assert_eq!(Decimal128::from_proto(proto, 38, 9), decimal);
    }
    let mut buf = column(ColumnDataType::Decimal128 {
        precision: 38,
        scale: 9,
    });
    buf.push(ValueRef::Decimal128(Decimal128::new(-42, 38, 9)))
        .expect("decimal");
    let before = buf.to_proto_column();
    assert!(
        buf.push(ValueRef::Decimal128(Decimal128::new(-42, 37, 9)))
            .is_err()
    );
    assert!(
        buf.push(ValueRef::Decimal128(Decimal128::new(-42, 38, 8)))
            .is_err()
    );
    assert_eq!(buf.to_proto_column(), before);
}

#[test]
fn temporal_types_keep_units_and_signed_ticks_on_wire() {
    let fixtures = [
        (
            ColumnDataType::Date,
            ValueRef::Date(Date(-1)),
            13,
            values!(date_values, vec![-1]),
        ),
        (
            ColumnDataType::DateTime,
            ValueRef::DateTime(DateTime(-2)),
            14,
            values!(datetime_values, vec![-2]),
        ),
        (
            ColumnDataType::TimestampSecond,
            ValueRef::TimestampSecond(-3),
            15,
            values!(timestamp_second_values, vec![-3]),
        ),
        (
            ColumnDataType::TimestampMillisecond,
            ValueRef::TimestampMillisecond(-4),
            16,
            values!(timestamp_millisecond_values, vec![-4]),
        ),
        (
            ColumnDataType::TimestampMicrosecond,
            ValueRef::TimestampMicrosecond(-5),
            17,
            values!(timestamp_microsecond_values, vec![-5]),
        ),
        (
            ColumnDataType::TimestampNanosecond,
            ValueRef::TimestampNanosecond(-6),
            18,
            values!(timestamp_nanosecond_values, vec![-6]),
        ),
    ];
    for (data_type, value, datatype, values) in fixtures {
        assert_wire(data_type, value, datatype, values);
    }
    let units = [
        (
            TimestampUnit::Second,
            ColumnDataType::TimestampSecond,
            3_600,
        ),
        (
            TimestampUnit::Millisecond,
            ColumnDataType::TimestampMillisecond,
            3_600_000,
        ),
        (
            TimestampUnit::Microsecond,
            ColumnDataType::TimestampMicrosecond,
            3_600_000_000,
        ),
        (
            TimestampUnit::Nanosecond,
            ColumnDataType::TimestampNanosecond,
            3_600_000_000_000,
        ),
    ];
    assert_eq!(TimestampUnit::default(), TimestampUnit::Nanosecond);
    for (unit, data_type, hour_ticks) in units {
        assert_eq!(unit.to_column_data_type(), data_type);
        assert_eq!(unit.one_hour_ticks(), hour_ticks);
    }
}

#[test]
fn json_and_decimal_extensions_are_independent_wire_fixtures() {
    use greptimedb_proto::v1::{
        ColumnDataTypeExtension, DecimalTypeExtension, column_data_type_extension::TypeExt,
    };
    let mut json = column(ColumnDataType::Json);
    json.push(ValueRef::Json("{\"city\":\"tokyo\"}"))
        .expect("JSON");
    let proto = json.to_proto_column();
    assert_eq!(proto.datatype, 11);
    assert_eq!(
        proto.values.expect("values").binary_values,
        vec!["{\"city\":\"tokyo\"}".as_bytes().to_vec()]
    );
    assert_eq!(
        proto.datatype_extension,
        Some(ColumnDataTypeExtension {
            type_ext: Some(TypeExt::JsonType(0))
        })
    );
    let mut decimal = column(ColumnDataType::Decimal128 {
        precision: 38,
        scale: 9,
    });
    decimal
        .push(ValueRef::Decimal128(Decimal128::new(-1, 38, 9)))
        .expect("decimal");
    let proto = decimal.to_proto_column();
    assert_eq!(proto.datatype, 30);
    assert_eq!(
        proto.values.expect("values").decimal128_values,
        vec![greptimedb_proto::v1::Decimal128 { hi: -1, lo: -1 }]
    );
    assert_eq!(
        proto.datatype_extension,
        Some(ColumnDataTypeExtension {
            type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                precision: 38,
                scale: 9
            }))
        })
    );
}

#[test]
fn semantics_and_row_values_match_proto_variants() {
    use greptimedb_proto::v1::value::ValueData;
    for (semantic, number, flight) in [
        (SemanticType::Tag, 0, "tag"),
        (SemanticType::Field, 1, "field"),
        (SemanticType::Timestamp, 2, "timestamp"),
    ] {
        assert_eq!(semantic.to_proto(), number);
        assert_eq!(semantic.as_flight_str(), flight);
    }
    let fixtures = [
        (ValueRef::Null, None),
        (ValueRef::Boolean(true), Some(ValueData::BoolValue(true))),
        (ValueRef::Int8(-128), Some(ValueData::I8Value(-128))),
        (ValueRef::Int16(-32_768), Some(ValueData::I16Value(-32_768))),
        (
            ValueRef::Int32(i32::MIN),
            Some(ValueData::I32Value(i32::MIN)),
        ),
        (
            ValueRef::Int64(i64::MIN),
            Some(ValueData::I64Value(i64::MIN)),
        ),
        (ValueRef::Uint8(255), Some(ValueData::U8Value(255))),
        (ValueRef::Uint16(65_535), Some(ValueData::U16Value(65_535))),
        (
            ValueRef::Uint32(u32::MAX),
            Some(ValueData::U32Value(u32::MAX)),
        ),
        (
            ValueRef::Uint64(u64::MAX),
            Some(ValueData::U64Value(u64::MAX)),
        ),
        (ValueRef::Float32(1.5), Some(ValueData::F32Value(1.5))),
        (ValueRef::Float64(-0.5), Some(ValueData::F64Value(-0.5))),
        (
            ValueRef::String("a"),
            Some(ValueData::StringValue("a".to_owned())),
        ),
        (
            ValueRef::Binary(&[0, 255]),
            Some(ValueData::BinaryValue(vec![0, 255])),
        ),
        (ValueRef::Date(Date(-1)), Some(ValueData::DateValue(-1))),
        (
            ValueRef::DateTime(DateTime(-2)),
            Some(ValueData::DatetimeValue(-2)),
        ),
        (
            ValueRef::TimestampSecond(-3),
            Some(ValueData::TimestampSecondValue(-3)),
        ),
        (
            ValueRef::TimestampMillisecond(-4),
            Some(ValueData::TimestampMillisecondValue(-4)),
        ),
        (
            ValueRef::TimestampMicrosecond(-5),
            Some(ValueData::TimestampMicrosecondValue(-5)),
        ),
        (
            ValueRef::TimestampNanosecond(-6),
            Some(ValueData::TimestampNanosecondValue(-6)),
        ),
        (
            ValueRef::Decimal128(Decimal128::new(-1, 38, 9)),
            Some(ValueData::Decimal128Value(
                greptimedb_proto::v1::Decimal128 { hi: -1, lo: -1 },
            )),
        ),
        (
            ValueRef::Json("{}"),
            Some(ValueData::BinaryValue(b"{}".to_vec())),
        ),
    ];
    for (value, expected) in fixtures {
        assert_eq!(value.to_proto_value().value_data, expected);
    }
}

#[test]
fn typed_temporal_wrappers_have_value_semantics_and_zero_defaults() {
    use greptimedb::types::{
        TimestampMicrosecond, TimestampMillisecond, TimestampNanosecond, TimestampSecond,
    };
    use std::collections::HashSet;
    fn assert_wrapper<T: Copy + Ord + std::hash::Hash + std::fmt::Debug + Default>(
        zero: T,
        low: T,
        high: T,
    ) {
        assert_eq!(T::default(), zero);
        assert!(low < high);
        let mut set = HashSet::new();
        set.insert(low);
        set.insert(high);
        set.insert(low);
        assert_eq!(set.len(), 2);
    }
    assert_wrapper(TimestampSecond(0), TimestampSecond(-1), TimestampSecond(1));
    assert_wrapper(
        TimestampMillisecond(0),
        TimestampMillisecond(-1),
        TimestampMillisecond(1),
    );
    assert_wrapper(
        TimestampMicrosecond(0),
        TimestampMicrosecond(-1),
        TimestampMicrosecond(1),
    );
    assert_wrapper(
        TimestampNanosecond(0),
        TimestampNanosecond(-1),
        TimestampNanosecond(1),
    );
    assert_wrapper(Date(0), Date(-1), Date(1));
    assert_wrapper(DateTime(0), DateTime(-1), DateTime(1));
}

#[test]
fn every_timestamp_unit_rejects_null_before_and_after_values() {
    for (data_type, value) in [
        (
            ColumnDataType::TimestampSecond,
            ValueRef::TimestampSecond(1),
        ),
        (
            ColumnDataType::TimestampMillisecond,
            ValueRef::TimestampMillisecond(1),
        ),
        (
            ColumnDataType::TimestampMicrosecond,
            ValueRef::TimestampMicrosecond(1),
        ),
        (
            ColumnDataType::TimestampNanosecond,
            ValueRef::TimestampNanosecond(1),
        ),
    ] {
        let mut buf = TypedColumnBuf::new("ts", SemanticType::Timestamp, data_type);
        assert!(buf.push(ValueRef::Null).is_err());
        assert_eq!(buf.row_count(), 0);
        buf.push(value).expect("timestamp");
        assert!(buf.push(ValueRef::Null).is_err());
        assert_eq!(buf.row_count(), 1);
        assert_eq!(buf.non_null_len(), 1);
        assert!(buf.to_proto_column().null_mask.is_empty());
    }
}

#[test]
fn floats_preserve_nan_infinity_and_negative_zero_bits() {
    let mut f32_column = column(ColumnDataType::Float32);
    let f32_values = [f32::from_bits(0x7fc0_1234), f32::INFINITY, -0.0];
    for value in f32_values {
        f32_column.push(ValueRef::Float32(value)).expect("float32");
    }
    let wire = f32_column.to_proto_column().values.expect("values");
    assert_eq!(
        wire.f32_values
            .into_iter()
            .map(f32::to_bits)
            .collect::<Vec<_>>(),
        vec![0x7fc0_1234, 0x7f80_0000, 0x8000_0000]
    );
    let mut f64_column = column(ColumnDataType::Float64);
    for value in [
        f64::from_bits(0x7ff8_0000_0000_1234),
        f64::NEG_INFINITY,
        -0.0,
    ] {
        f64_column.push(ValueRef::Float64(value)).expect("float64");
    }
    let wire = f64_column.to_proto_column().values.expect("values");
    assert_eq!(
        wire.f64_values
            .into_iter()
            .map(f64::to_bits)
            .collect::<Vec<_>>(),
        vec![
            0x7ff8_0000_0000_1234,
            0xfff0_0000_0000_0000,
            0x8000_0000_0000_0000
        ]
    );
}

#[test]
fn boundary_masks_and_non_null_values_stay_in_lockstep() {
    for rows in [8_usize, 9, 16, 17, 64, 65] {
        for sparse in [false, true] {
            let mut buf = column(ColumnDataType::Int64);
            let expected_mask = match (rows, sparse) {
                (8, false) => vec![0x55],
                (9, false) => vec![0x55, 1],
                (16, false) => vec![0x55; 2],
                (17, false) => vec![0x55, 0x55, 1],
                (64, false) => vec![0x55; 8],
                (65, false) => {
                    let mut v = vec![0x55; 8];
                    v.push(1);
                    v
                }
                (8, true) => vec![0x80],
                (9, true) => vec![0x80, 1],
                (16, true) => vec![0x80, 0x80],
                (17, true) => vec![0x80, 0, 1],
                (64, true) => vec![0x80, 0, 0, 0, 0, 0, 0, 0x80],
                (65, true) => vec![0x80, 0, 0, 0, 0, 0, 0, 0, 1],
                _ => unreachable!("fixture rows"),
            };
            let mut expected_values = Vec::new();
            for row in 0..rows {
                let null = if sparse {
                    row == 7 || row == rows - 1
                } else {
                    row % 2 == 0
                };
                if null {
                    buf.push(ValueRef::Null).expect("null field");
                } else {
                    expected_values.push(row as i64);
                    buf.push(ValueRef::Int64(row as i64)).expect("integer");
                }
                assert_eq!(buf.null_mask().is_null(row), null);
            }
            let wire = buf.to_proto_column();
            assert_eq!(buf.row_count(), rows);
            assert_eq!(buf.non_null_len(), expected_values.len());
            assert_eq!(wire.null_mask, expected_mask);
            assert_eq!(wire.values, Some(values!(i64_values, expected_values)));
        }
    }
}

#[test]
fn rejected_values_then_valid_appends_preserve_exact_wire_state() {
    let mut integer = column(ColumnDataType::Int8);
    integer.push(ValueRef::Null).expect("nullable field");
    assert!(integer.push(ValueRef::Int16(42)).is_err());
    integer
        .push(ValueRef::Int8(42))
        .expect("valid after mismatch");
    assert_eq!(integer.row_count(), 2);
    assert_eq!(integer.to_proto_column().null_mask, vec![1]);
    assert_eq!(
        integer.to_proto_column().values,
        Some(values!(i8_values, vec![42]))
    );
    let mut timestamp = TypedColumnBuf::new(
        "ts",
        SemanticType::Timestamp,
        ColumnDataType::TimestampMillisecond,
    );
    assert!(timestamp.push(ValueRef::Null).is_err());
    timestamp
        .push(ValueRef::TimestampMillisecond(42))
        .expect("valid after null");
    assert_eq!(timestamp.row_count(), 1);
    assert!(timestamp.to_proto_column().null_mask.is_empty());
    assert_eq!(
        timestamp.to_proto_column().values,
        Some(values!(timestamp_millisecond_values, vec![42]))
    );
    let mut decimal = column(ColumnDataType::Decimal128 {
        precision: 38,
        scale: 9,
    });
    decimal.push(ValueRef::Null).expect("nullable decimal");
    for (precision, scale) in [(0, 0), (39, 0), (38, 39), (38, u32::MAX), (37, 9), (38, 8)] {
        let before = decimal.to_proto_column();
        assert!(
            decimal
                .push(ValueRef::Decimal128(Decimal128::new(1, precision, scale)))
                .is_err()
        );
        assert_eq!(decimal.to_proto_column(), before);
        decimal
            .push(ValueRef::Decimal128(Decimal128::new(-1, 38, 9)))
            .expect("valid after rejection");
    }
    assert_eq!(decimal.row_count(), 7);
    assert_eq!(decimal.to_proto_column().null_mask, vec![1]);
    assert_eq!(
        decimal
            .to_proto_column()
            .values
            .expect("values")
            .decimal128_values,
        vec![greptimedb_proto::v1::Decimal128 { hi: -1, lo: -1 }; 6]
    );
    for (precision, scale) in [(0, 0), (39, 0), (u32::MAX, 0), (1, 2), (38, u32::MAX)] {
        let mut invalid = column(ColumnDataType::Decimal128 { precision, scale });
        for value in [
            ValueRef::Null,
            ValueRef::Decimal128(Decimal128::new(1, precision, scale)),
        ] {
            assert!(invalid.push(value).is_err());
            assert_eq!(invalid.row_count(), 0);
            assert_eq!(invalid.to_proto_column().values, Some(Values::default()));
            assert!(invalid.to_proto_column().null_mask.is_empty());
        }
    }
}

#[test]
fn decimal_metadata_clamps_unsigned_fields_without_sign_wrap() {
    let data_type = ColumnDataType::Decimal128 {
        precision: u32::MAX,
        scale: u32::MAX,
    };
    for column in [
        TypedColumnBuf::new("d", SemanticType::Field, data_type),
        TypedColumnBuf::with_capacity("d", SemanticType::Field, data_type, 16),
    ] {
        let extension = column
            .to_proto_column()
            .datatype_extension
            .expect("decimal extension");
        assert!(
            matches!(extension.type_ext, Some(TypeExt::DecimalType(value)) if value.precision == i32::MAX && value.scale == i32::MAX)
        );
    }
}
