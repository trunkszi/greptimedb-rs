//! Typed row schemas and reusable column buffers used by `derive(Row)`.

pub mod decode;
pub use decode::{ArrowField, PreparedRecordBatch, RowDecode};

use std::{error::Error, fmt};

use crate::{
    batch::BatchEncode,
    buffer_policy::{BufferPolicy, MemoryBuffer},
    column_buf::NullMask,
    types::{self, ColumnDataType, SemanticType},
};
use greptimedb_proto::v1::{Column, column::Values};

/// A field's column name, role, logical type and nullability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: &'static str,
    pub semantic_type: SemanticType,
    pub data_type: ColumnDataType,
    pub nullable: bool,
}

/// A table schema with an associated reusable typed batch.
pub trait Row: Sized {
    const TABLE_NAME: &'static str;
    const COLUMNS: &'static [ColumnMeta];
    type Batch: BatchEncode + MemoryBuffer + Default;
    fn new_batch() -> Self::Batch;
    fn new_batch_with_capacity(row_capacity: usize) -> Self::Batch;
    fn new_batch_with_policy(row_capacity: usize, policy: BufferPolicy) -> Self::Batch;
    fn push_into_batch(&self, batch: &mut Self::Batch);
}

/// An ingestion row whose variable-size fields borrow caller-owned data.
pub trait BorrowedRow<'a, R: Row> {
    fn push_into_batch(&self, batch: &mut R::Batch);
}

/// A typed buffer cannot encode values under incompatible column metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowError(pub &'static str);
impl fmt::Display for RowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}
impl Error for RowError {}

/// Converts a field encoding failure to the batch schema error contract.
pub fn field_error(error: RowError) -> crate::error::Error {
    crate::error::Error::SchemaViolation(error.to_string())
}
/// Constructs a schema error for an invalid generated batch shape.
pub fn schema_error(message: &'static str) -> crate::error::Error {
    crate::error::Error::SchemaViolation(message.into())
}

/// Storage and direct protobuf encoding for one Rust field type.
///
/// The borrowed input and inspected value share the same representation. Only
/// protobuf construction creates owned per-value strings or byte vectors.
pub trait Field {
    type Buffer: MemoryBuffer + Default;
    type Input<'a>: Copy;
    fn with_capacity(rows: usize) -> Self::Buffer;
    fn len(buffer: &Self::Buffer) -> usize;
    fn push(buffer: &mut Self::Buffer, value: Self::Input<'_>);
    fn get<'a>(buffer: &'a Self::Buffer, index: usize) -> Option<Self::Input<'a>>;
    fn accepts_data_type(data_type: ColumnDataType) -> bool;
    /// Checks column compatibility and value constraints without allocation.
    /// # Errors
    /// Returns an error for incompatible schema metadata or invalid values.
    fn validate(buffer: &Self::Buffer, meta: &ColumnMeta) -> Result<(), RowError> {
        if meta.nullable || !Self::accepts_data_type(meta.data_type) {
            return Err(RowError("field storage and column metadata differ"));
        }
        if meta.semantic_type == SemanticType::Timestamp
            && !matches!(
                meta.data_type,
                ColumnDataType::TimestampSecond
                    | ColumnDataType::TimestampMillisecond
                    | ColumnDataType::TimestampMicrosecond
                    | ColumnDataType::TimestampNanosecond
            )
        {
            return Err(RowError("time-index column requires a timestamp datatype"));
        }
        Self::validate_values(buffer, meta)
    }
    /// Checks representation-specific values after schema validation.
    /// # Errors
    /// Returns an error when a value violates the column's contract.
    fn validate_values(_buffer: &Self::Buffer, _meta: &ColumnMeta) -> Result<(), RowError> {
        Ok(())
    }
    /// Copies contiguous non-null values directly into protobuf column arrays.
    /// # Errors
    /// Returns an error for incompatible metadata or invalid decimal values.
    fn to_column(buffer: &Self::Buffer, meta: &ColumnMeta) -> Result<Column, RowError>;
}

fn column(meta: &ColumnMeta, values: Values) -> Column {
    Column {
        column_name: meta.name.into(),
        semantic_type: meta.semantic_type.to_proto(),
        datatype: meta.data_type.to_proto_datatype(),
        datatype_extension: meta.data_type.to_proto_extension(),
        values: Some(values),
        ..Column::default()
    }
}

/// Contiguous fixed-size non-null field values.
pub struct ScalarBuffer<T> {
    values: Vec<T>,
}
impl<T> Default for ScalarBuffer<T> {
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}
impl<T> MemoryBuffer for ScalarBuffer<T> {
    fn allocated_bytes(&self) -> usize {
        self.values.capacity().saturating_mul(size_of::<T>())
    }
    fn row_capacity(&self) -> usize {
        self.values.capacity()
    }
    fn clear_with_policy(&mut self, policy: BufferPolicy) {
        let shrink = policy.should_shrink(self.row_capacity(), self.allocated_bytes());
        self.values.clear();
        if shrink {
            self.values = Vec::new();
        }
    }
    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
    }
}

/// Flat variable-size data, indexed by the end offset of each non-null value.
#[derive(Default)]
pub struct FlatBuffer {
    bytes: Vec<u8>,
    ends: Vec<usize>,
}
impl FlatBuffer {
    fn with_capacity(rows: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(rows.saturating_mul(64)),
            ends: Vec::with_capacity(rows),
        }
    }
    fn push(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
        self.ends.push(self.bytes.len());
    }
    fn get(&self, index: usize) -> Option<&[u8]> {
        let end = *self.ends.get(index)?;
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        Some(&self.bytes[start..end])
    }
    /// Reserves additional variable payload bytes without adding rows.
    pub fn reserve_bytes(&mut self, additional: usize) {
        self.bytes.reserve(additional);
    }
}
impl MemoryBuffer for FlatBuffer {
    fn allocated_bytes(&self) -> usize {
        self.bytes
            .capacity()
            .saturating_add(self.ends.capacity().saturating_mul(size_of::<usize>()))
    }
    fn row_capacity(&self) -> usize {
        self.ends.capacity()
    }
    fn clear_with_policy(&mut self, policy: BufferPolicy) {
        let shrink = policy.should_shrink(self.row_capacity(), self.allocated_bytes());
        self.bytes.clear();
        self.ends.clear();
        if shrink {
            self.bytes = Vec::new();
            self.ends = Vec::new();
        }
    }
    fn shrink_to_fit(&mut self) {
        self.bytes.shrink_to_fit();
        self.ends.shrink_to_fit();
    }
}

/// Compact non-null values with null bits and constant-time row lookup.
pub struct NullableBuffer<F: Field> {
    values: F::Buffer,
    nulls: NullMask,
    positions: Vec<usize>,
}
impl<F: Field> Default for NullableBuffer<F> {
    fn default() -> Self {
        Self {
            values: F::Buffer::default(),
            nulls: NullMask::default(),
            positions: Vec::new(),
        }
    }
}
impl<F: Field> MemoryBuffer for NullableBuffer<F> {
    fn allocated_bytes(&self) -> usize {
        self.values
            .allocated_bytes()
            .saturating_add(self.nulls.allocated_bytes())
            .saturating_add(self.positions.capacity().saturating_mul(size_of::<usize>()))
    }
    fn row_capacity(&self) -> usize {
        self.values
            .row_capacity()
            .max(self.nulls.row_capacity())
            .max(self.positions.capacity())
    }
    fn clear_with_policy(&mut self, policy: BufferPolicy) {
        let shrink = policy.should_shrink(self.row_capacity(), self.allocated_bytes());
        let retention = if shrink {
            BufferPolicy::zero()
        } else {
            BufferPolicy::unbounded()
        };
        self.values.clear_with_policy(retention);
        self.nulls.clear_with_policy(retention);
        self.positions.clear();
        if shrink {
            self.positions = Vec::new();
        }
    }
    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.nulls.shrink_to_fit();
        self.positions.shrink_to_fit();
    }
}
impl<F: Field> Field for Option<F> {
    type Buffer = NullableBuffer<F>;
    type Input<'a> = Option<F::Input<'a>>;
    fn with_capacity(rows: usize) -> Self::Buffer {
        NullableBuffer {
            values: F::with_capacity(rows),
            nulls: NullMask::with_capacity(rows),
            positions: Vec::with_capacity(rows),
        }
    }
    fn len(buffer: &Self::Buffer) -> usize {
        buffer.positions.len()
    }
    fn push(buffer: &mut Self::Buffer, value: Self::Input<'_>) {
        let index = F::len(&buffer.values);
        if let Some(value) = value {
            F::push(&mut buffer.values, value);
        }
        buffer.nulls.push(value.is_none());
        buffer.positions.push(index);
    }
    fn get<'a>(buffer: &'a Self::Buffer, index: usize) -> Option<Self::Input<'a>> {
        let position = *buffer.positions.get(index)?;
        if buffer.nulls.is_null(index) {
            Some(None)
        } else {
            Some(Some(F::get(&buffer.values, position)?))
        }
    }
    fn accepts_data_type(data_type: ColumnDataType) -> bool {
        F::accepts_data_type(data_type)
    }
    fn validate(buffer: &Self::Buffer, meta: &ColumnMeta) -> Result<(), RowError> {
        if !meta.nullable || meta.semantic_type == SemanticType::Timestamp {
            return Err(RowError(
                "nullable storage requires a nullable non-timestamp column",
            ));
        }
        if buffer.nulls.row_count() != buffer.positions.len()
            || buffer.nulls.non_null_count() != F::len(&buffer.values)
        {
            return Err(RowError(
                "nullable values and null mask have inconsistent lengths",
            ));
        }
        F::validate(
            &buffer.values,
            &ColumnMeta {
                nullable: false,
                ..*meta
            },
        )
    }
    fn to_column(buffer: &Self::Buffer, meta: &ColumnMeta) -> Result<Column, RowError> {
        Self::validate(buffer, meta)?;
        let mut output = F::to_column(
            &buffer.values,
            &ColumnMeta {
                nullable: false,
                ..*meta
            },
        )?;
        output.null_mask = buffer.nulls.to_proto_bytes();
        Ok(output)
    }
}

macro_rules! scalar_field {
    ($ty:ty, $values:ident, $data:ident, $pattern:pat, $body:block $(, $validation:block)?) => {
        impl Field for $ty {
            type Buffer = ScalarBuffer<Self>;
            type Input<'a> = Self;
            fn with_capacity(rows: usize) -> Self::Buffer { ScalarBuffer { values: Vec::with_capacity(rows) } }
            fn len(buffer: &Self::Buffer) -> usize { buffer.values.len() }
            fn push(buffer: &mut Self::Buffer, value: Self) { buffer.values.push(value); }
            fn get<'a>(buffer: &'a Self::Buffer, index: usize) -> Option<Self::Input<'a>> { buffer.values.get(index).copied() }
            fn accepts_data_type(data_type: ColumnDataType) -> bool { matches!(data_type, $pattern) }
            $(fn validate_values(buffer: &Self::Buffer, meta: &ColumnMeta) -> Result<(), RowError> {
                let $values = &buffer.values;
                let $data = meta.data_type;
                $validation
            })?
            fn to_column(buffer: &Self::Buffer, meta: &ColumnMeta) -> Result<Column, RowError> {
                Self::validate(buffer, meta)?;
                let $values = &buffer.values;
                let $data = meta.data_type;
                let encoded: Values = $body;
                Ok(column(meta, encoded))
            }
        }
    };
}
macro_rules! primitive_field {
    ($ty:ty, $variant:ident, $member:ident, $convert:expr) => {
        scalar_field!($ty, values, data, ColumnDataType::$variant, {
            if data != ColumnDataType::$variant {
                return Err(RowError("field and column type differ"));
            }
            Values {
                $member: values.iter().copied().map($convert).collect(),
                ..Values::default()
            }
        });
    };
}
primitive_field!(bool, Boolean, bool_values, |value| value);
primitive_field!(i8, Int8, i8_values, i32::from);
primitive_field!(i16, Int16, i16_values, i32::from);
primitive_field!(u8, Uint8, u8_values, u32::from);
primitive_field!(u16, Uint16, u16_values, u32::from);
primitive_field!(u32, Uint32, u32_values, |value| value);
primitive_field!(u64, Uint64, u64_values, |value| value);
primitive_field!(f32, Float32, f32_values, |value| value);
primitive_field!(f64, Float64, f64_values, |value| value);
scalar_field!(
    i32,
    source,
    data,
    ColumnDataType::Int32 | ColumnDataType::Date,
    {
        let mut values = Values::default();
        match data {
            ColumnDataType::Int32 => values.i32_values = source.clone(),
            ColumnDataType::Date => values.date_values = source.clone(),
            _ => return Err(RowError("i32 field requires an Int32 or Date column")),
        }
        values
    }
);
scalar_field!(
    i64,
    source,
    data,
    ColumnDataType::Int64
        | ColumnDataType::DateTime
        | ColumnDataType::TimestampSecond
        | ColumnDataType::TimestampMillisecond
        | ColumnDataType::TimestampMicrosecond
        | ColumnDataType::TimestampNanosecond,
    {
        let mut values = Values::default();
        match data {
            ColumnDataType::Int64 => values.i64_values = source.clone(),
            ColumnDataType::DateTime => values.datetime_values = source.clone(),
            ColumnDataType::TimestampSecond => values.timestamp_second_values = source.clone(),
            ColumnDataType::TimestampMillisecond => {
                values.timestamp_millisecond_values = source.clone()
            }
            ColumnDataType::TimestampMicrosecond => {
                values.timestamp_microsecond_values = source.clone()
            }
            ColumnDataType::TimestampNanosecond => {
                values.timestamp_nanosecond_values = source.clone()
            }
            _ => return Err(RowError("i64 field requires an integer or temporal column")),
        }
        values
    }
);
macro_rules! temporal_field {
    ($ty:ident, $member:ident) => {
        scalar_field!(types::$ty, source, data, ColumnDataType::$ty, {
            if data != ColumnDataType::$ty {
                return Err(RowError("temporal field and column unit differ"));
            }
            Values {
                $member: source.iter().map(|value| value.0).collect(),
                ..Values::default()
            }
        });
    };
}
temporal_field!(Date, date_values);
temporal_field!(DateTime, datetime_values);
temporal_field!(TimestampSecond, timestamp_second_values);
temporal_field!(TimestampMillisecond, timestamp_millisecond_values);
temporal_field!(TimestampMicrosecond, timestamp_microsecond_values);
temporal_field!(TimestampNanosecond, timestamp_nanosecond_values);

fn decimal(
    value: i128,
    data: ColumnDataType,
) -> Result<greptimedb_proto::v1::Decimal128, RowError> {
    let ColumnDataType::Decimal128 { precision, scale } = data else {
        return Err(RowError("decimal field requires decimal column metadata"));
    };
    if !(1..=38).contains(&precision)
        || scale > precision
        || value.unsigned_abs() >= 10_u128.pow(precision)
    {
        return Err(RowError(
            "decimal coefficient or precision/scale is outside the column range",
        ));
    }
    Ok(types::Decimal128::new(value, precision, scale).to_proto())
}
scalar_field!(
    i128,
    source,
    data,
    ColumnDataType::Decimal128 { .. },
    {
        Values {
            decimal128_values: source
                .iter()
                .map(|value| decimal(*value, data))
                .collect::<Result<_, _>>()?,
            ..Values::default()
        }
    },
    {
        for value in source {
            decimal(*value, data)?;
        }
        // Empty columns must still reject invalid decimal metadata.
        decimal(0, data)?;
        Ok(())
    }
);
scalar_field!(
    types::Decimal128,
    source,
    data,
    ColumnDataType::Decimal128 { .. },
    {
        Values {
            decimal128_values: source
                .iter()
                .map(|value| {
                    if data
                        != (ColumnDataType::Decimal128 {
                            precision: value.precision,
                            scale: value.scale,
                        })
                    {
                        return Err(RowError(
                            "decimal value precision/scale differs from column metadata",
                        ));
                    }
                    decimal(value.value, data)
                })
                .collect::<Result<_, _>>()?,
            ..Values::default()
        }
    },
    {
        decimal(0, data)?;
        for value in source {
            if data
                != (ColumnDataType::Decimal128 {
                    precision: value.precision,
                    scale: value.scale,
                })
            {
                return Err(RowError(
                    "decimal value precision/scale differs from column metadata",
                ));
            }
            decimal(value.value, data)?;
        }
        Ok(())
    }
);

impl Field for String {
    type Buffer = FlatBuffer;
    type Input<'a> = &'a str;
    fn with_capacity(rows: usize) -> FlatBuffer {
        FlatBuffer::with_capacity(rows)
    }
    fn len(buffer: &FlatBuffer) -> usize {
        buffer.ends.len()
    }
    fn push(buffer: &mut FlatBuffer, value: &str) {
        buffer.push(value.as_bytes());
    }
    fn get(buffer: &FlatBuffer, index: usize) -> Option<&str> {
        std::str::from_utf8(buffer.get(index)?).ok()
    }
    fn accepts_data_type(data_type: ColumnDataType) -> bool {
        matches!(data_type, ColumnDataType::String | ColumnDataType::Json)
    }
    fn validate_values(buffer: &FlatBuffer, _: &ColumnMeta) -> Result<(), RowError> {
        for index in 0..buffer.ends.len() {
            Self::get(buffer, index).ok_or(RowError("string buffer contains invalid UTF-8"))?;
        }
        Ok(())
    }
    fn to_column(buffer: &FlatBuffer, meta: &ColumnMeta) -> Result<Column, RowError> {
        Self::validate(buffer, meta)?;
        let mut values = Values::default();
        match meta.data_type {
            ColumnDataType::String => {
                values.string_values = (0..buffer.ends.len())
                    .map(|index| {
                        Self::get(buffer, index)
                            .map(str::to_owned)
                            .ok_or(RowError("string buffer contains invalid UTF-8"))
                    })
                    .collect::<Result<_, _>>()?;
            }
            ColumnDataType::Json => {
                values.binary_values = (0..buffer.ends.len())
                    .map(|index| {
                        buffer
                            .get(index)
                            .map(<[u8]>::to_vec)
                            .ok_or(RowError("JSON buffer row is missing"))
                    })
                    .collect::<Result<_, _>>()?;
            }
            _ => return Err(RowError("string field requires a String or Json column")),
        }
        Ok(column(meta, values))
    }
}
impl Field for Vec<u8> {
    type Buffer = FlatBuffer;
    type Input<'a> = &'a [u8];
    fn with_capacity(rows: usize) -> FlatBuffer {
        FlatBuffer::with_capacity(rows)
    }
    fn len(buffer: &FlatBuffer) -> usize {
        buffer.ends.len()
    }
    fn push(buffer: &mut FlatBuffer, value: &[u8]) {
        buffer.push(value);
    }
    fn get(buffer: &FlatBuffer, index: usize) -> Option<&[u8]> {
        buffer.get(index)
    }
    fn accepts_data_type(data_type: ColumnDataType) -> bool {
        data_type == ColumnDataType::Binary
    }
    fn to_column(buffer: &FlatBuffer, meta: &ColumnMeta) -> Result<Column, RowError> {
        Self::validate(buffer, meta)?;
        if meta.data_type != ColumnDataType::Binary {
            return Err(RowError("binary field requires a Binary column"));
        }
        Ok(column(
            meta,
            Values {
                binary_values: (0..buffer.ends.len())
                    .map(|index| {
                        buffer
                            .get(index)
                            .map(<[u8]>::to_vec)
                            .ok_or(RowError("binary buffer row is missing"))
                    })
                    .collect::<Result<_, _>>()?,
                ..Values::default()
            },
        ))
    }
}

#[cfg(test)]
mod unit {
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
        assert!(
            <Vec<u8> as Field>::to_column(&buffer, &meta(ColumnDataType::String, false)).is_err()
        );
    }
}
