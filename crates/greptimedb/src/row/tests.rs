//! Typed row storage contracts.
use super::*;

fn meta(data_type: ColumnDataType, nullable: bool) -> ColumnMeta {
    ColumnMeta {
        name: "value",
        semantic_type: SemanticType::Field,
        data_type,
        nullable,
    }
}

#[test]
fn scenario_nullable_text_preserves_dense_values_and_cross_byte_null_positions() {
    type Text = Option<String>;
    let mut buffer = Text::with_capacity(9);
    let rows = [
        Some("tokyo"),
        None,
        Some(""),
        None,
        Some("bid"),
        Some("ask"),
        Some("strategy"),
        Some("done"),
        None,
    ];
    for row in rows {
        Text::push(&mut buffer, row);
    }
    assert_eq!(Text::len(&buffer), 9);
    for (index, expected) in rows.iter().enumerate() {
        assert_eq!(Text::get(&buffer, index), Some(*expected));
    }
    assert_eq!(Text::get(&buffer, 9), None);
    let column = Text::to_column(&buffer, &meta(ColumnDataType::String, true)).unwrap();
    assert_eq!(column.null_mask, [0x0a, 0x01]);
    assert_eq!(
        column.values.unwrap().string_values,
        ["tokyo", "", "bid", "ask", "strategy", "done"]
    );
    assert!(Text::validate(&buffer, &meta(ColumnDataType::String, false)).is_err());
}

#[test]
fn lifecycle_nullable_binary_reuses_both_arenas_and_applies_aggregate_budget() {
    type Binary = Option<Vec<u8>>;
    let mut buffer = Binary::with_capacity(4);
    let bytes = [0, 255, 128, 0];
    for value in [Some(bytes.as_slice()), None, Some(&[])] {
        Binary::push(&mut buffer, value);
    }
    let warmed = buffer.allocated_bytes();
    buffer.clear_unbounded();
    assert_eq!(Binary::len(&buffer), 0);
    assert_eq!(buffer.allocated_bytes(), warmed);
    for value in [None, Some(bytes.as_slice()), Some(&[])] {
        Binary::push(&mut buffer, value);
    }
    assert_eq!(buffer.allocated_bytes(), warmed);
    let encoded = Binary::to_column(&buffer, &meta(ColumnDataType::Binary, true)).unwrap();
    assert_eq!(encoded.null_mask, [1]);
    assert_eq!(
        encoded.values.unwrap().binary_values,
        [bytes.to_vec(), Vec::new()]
    );
    // The total exceeds the budget even though the largest child does not.
    let budget = warmed - 1;
    assert!(buffer.values.allocated_bytes() <= budget);
    buffer.clear_with_policy(BufferPolicy::new(usize::MAX, budget));
    assert_eq!(buffer.allocated_bytes(), 0);
    assert_eq!(Binary::len(&buffer), 0);
    Binary::push(&mut buffer, Some(&bytes));
    assert_eq!(Binary::get(&buffer, 0), Some(Some(bytes.as_slice())));
}

#[test]
fn scenario_decimal_validation_rejects_bad_schema_empty_values_and_coefficient_overflow() {
    let mut coefficients = i128::with_capacity(2);
    let schema = meta(
        ColumnDataType::Decimal128 {
            precision: 4,
            scale: 2,
        },
        false,
    );
    for bad in [
        ColumnDataType::Decimal128 {
            precision: 0,
            scale: 0,
        },
        ColumnDataType::Decimal128 {
            precision: 39,
            scale: 0,
        },
        ColumnDataType::Decimal128 {
            precision: 4,
            scale: 5,
        },
    ] {
        assert!(i128::validate(&coefficients, &meta(bad, false)).is_err());
    }
    i128::push(&mut coefficients, -9999);
    let encoded = i128::to_column(&coefficients, &schema)
        .unwrap()
        .values
        .unwrap();
    assert_eq!(encoded.decimal128_values[0].hi, -1);
    assert_eq!(encoded.decimal128_values[0].lo, -9999);
    i128::push(&mut coefficients, 10_000);
    assert!(i128::validate(&coefficients, &schema).is_err());
    assert!(i128::to_column(&coefficients, &schema).is_err());
    assert_eq!(i128::get(&coefficients, 0), Some(-9999));
    coefficients.clear_unbounded();
    i128::push(&mut coefficients, i128::MIN);
    assert!(i128::validate(&coefficients, &schema).is_err());

    let mut decimals = types::Decimal128::with_capacity(1);
    types::Decimal128::push(&mut decimals, types::Decimal128::new(12, 4, 3));
    assert!(types::Decimal128::validate(&decimals, &schema).is_err());
}

#[test]
fn scenario_time_index_requires_non_null_timestamp_storage_and_matching_units() {
    let mut buffer = i64::with_capacity(1);
    i64::push(&mut buffer, -42);
    let timestamp = ColumnMeta {
        semantic_type: SemanticType::Timestamp,
        ..meta(ColumnDataType::TimestampNanosecond, false)
    };
    let encoded = i64::to_column(&buffer, &timestamp).unwrap();
    assert_eq!(encoded.values.unwrap().timestamp_nanosecond_values, [-42]);
    assert!(
        i64::validate(
            &buffer,
            &ColumnMeta {
                data_type: ColumnDataType::Int64,
                ..timestamp
            }
        )
        .is_err()
    );
    let nullable = Option::<i64>::with_capacity(1);
    assert!(
        Option::<i64>::validate(
            &nullable,
            &ColumnMeta {
                nullable: true,
                ..timestamp
            }
        )
        .is_err()
    );
    let mut seconds = types::TimestampSecond::with_capacity(1);
    types::TimestampSecond::push(&mut seconds, types::TimestampSecond(1));
    assert!(types::TimestampSecond::validate(&seconds, &timestamp).is_err());
}

#[test]
fn scenario_shared_flat_storage_rejects_invalid_text_without_losing_binary_bytes() {
    let mut buffer = <Vec<u8> as Field>::with_capacity(1);
    // A flat buffer can be used through either supported field contract.
    <Vec<u8> as Field>::push(&mut buffer, &[255, 0]);
    assert!(String::validate(&buffer, &meta(ColumnDataType::String, false)).is_err());
    assert!(String::to_column(&buffer, &meta(ColumnDataType::Json, false)).is_err());
    assert_eq!(
        <Vec<u8> as Field>::get(&buffer, 0),
        Some([255, 0].as_slice())
    );
    assert!(<Vec<u8> as Field>::to_column(&buffer, &meta(ColumnDataType::String, false)).is_err());
}
