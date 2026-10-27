//! Reusable typed non-null values paired with GreptimeDB null bitmasks.
use crate::buffer_policy::{BufferPolicy, MemoryBuffer};
use crate::types::{ColumnDataType, Decimal128, SemanticType, ValueRef};
use greptimedb_proto::v1::{Column, column::Values};
use std::{error::Error, fmt};

/// Set bit for each position inside a null-mask byte.
const NULL_BIT: [u8; 8] = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80];

/// Little-endian null bits: a set bit represents a null row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NullMask {
    bytes: Vec<u8>,
    row_count: usize,
    null_count: usize,
}

impl NullMask {
    pub fn with_capacity(rows: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(rows.div_ceil(8)),
            ..Self::default()
        }
    }

    #[inline(always)]
    pub fn push(&mut self, is_null: bool) {
        let row = self.row_count;
        let byte_index = row >> 3;
        if byte_index == self.bytes.len() {
            self.reserve_mask_byte();
        }
        if is_null {
            self.bytes[byte_index] |= NULL_BIT[row & 7];
            self.null_count += 1;
        }
        self.row_count = row + 1;
    }

    /// Grow the mask by a cache-line chunk when the next byte has no spare capacity.
    #[inline(never)]
    fn reserve_mask_byte(&mut self) {
        if self.bytes.len() == self.bytes.capacity() {
            let grow = self.bytes.capacity().max(64);
            self.bytes.reserve(grow);
        }
        self.bytes.push(0);
    }

    /// Returns false for an index outside the buffered rows.
    #[inline(always)]
    pub fn is_null(&self, row: usize) -> bool {
        row < self.row_count && self.bytes[row >> 3] & NULL_BIT[row & 7] != 0
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }
    pub fn null_count(&self) -> usize {
        self.null_count
    }
    pub fn non_null_count(&self) -> usize {
        self.row_count - self.null_count
    }
    pub fn has_nulls(&self) -> bool {
        self.null_count != 0
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// GreptimeDB omits the entire mask when every row is non-null.
    pub fn to_proto_bytes(&self) -> Vec<u8> {
        if self.has_nulls() {
            self.bytes.clone()
        } else {
            Vec::new()
        }
    }
}

impl MemoryBuffer for NullMask {
    fn allocated_bytes(&self) -> usize {
        self.bytes.capacity()
    }
    fn row_capacity(&self) -> usize {
        self.bytes.capacity().saturating_mul(8)
    }
    fn clear_with_policy(&mut self, policy: BufferPolicy) {
        let shrink = policy.should_shrink(self.row_capacity(), self.allocated_bytes());
        self.bytes.clear();
        self.row_count = 0;
        self.null_count = 0;
        if shrink {
            self.bytes = Vec::new();
        }
    }
    fn shrink_to_fit(&mut self) {
        self.bytes.shrink_to_fit();
    }
}

#[derive(Clone, Debug)]
enum TypedValues {
    Bools(Vec<bool>),
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Binary(Vec<Vec<u8>>),
    Strings(Vec<String>),
    Decimal128(Vec<Decimal128>),
}

macro_rules! with_values {
    ($values:expr, $v:ident => $body:expr) => {
        match $values {
            TypedValues::Bools($v) => $body,
            TypedValues::I8($v) => $body,
            TypedValues::I16($v) => $body,
            TypedValues::I32($v) => $body,
            TypedValues::I64($v) => $body,
            TypedValues::U8($v) => $body,
            TypedValues::U16($v) => $body,
            TypedValues::U32($v) => $body,
            TypedValues::U64($v) => $body,
            TypedValues::F32($v) => $body,
            TypedValues::F64($v) => $body,
            TypedValues::Binary($v) => $body,
            TypedValues::Strings($v) => $body,
            TypedValues::Decimal128($v) => $body,
        }
    };
}

impl TypedValues {
    fn new(data_type: ColumnDataType, rows: usize) -> Self {
        match data_type {
            ColumnDataType::Boolean => Self::Bools(Vec::with_capacity(rows)),
            ColumnDataType::Int8 => Self::I8(Vec::with_capacity(rows)),
            ColumnDataType::Int16 => Self::I16(Vec::with_capacity(rows)),
            ColumnDataType::Int32 | ColumnDataType::Date => Self::I32(Vec::with_capacity(rows)),
            ColumnDataType::Int64
            | ColumnDataType::DateTime
            | ColumnDataType::TimestampSecond
            | ColumnDataType::TimestampMillisecond
            | ColumnDataType::TimestampMicrosecond
            | ColumnDataType::TimestampNanosecond => Self::I64(Vec::with_capacity(rows)),
            ColumnDataType::Uint8 => Self::U8(Vec::with_capacity(rows)),
            ColumnDataType::Uint16 => Self::U16(Vec::with_capacity(rows)),
            ColumnDataType::Uint32 => Self::U32(Vec::with_capacity(rows)),
            ColumnDataType::Uint64 => Self::U64(Vec::with_capacity(rows)),
            ColumnDataType::Float32 => Self::F32(Vec::with_capacity(rows)),
            ColumnDataType::Float64 => Self::F64(Vec::with_capacity(rows)),
            ColumnDataType::Binary | ColumnDataType::Json => Self::Binary(Vec::with_capacity(rows)),
            ColumnDataType::String => Self::Strings(Vec::with_capacity(rows)),
            ColumnDataType::Decimal128 { .. } => Self::Decimal128(Vec::with_capacity(rows)),
        }
    }
    fn len(&self) -> usize {
        with_values!(self, v => v.len())
    }
    fn capacity(&self) -> usize {
        with_values!(self, v => v.capacity())
    }
    fn allocated_bytes(&self) -> usize {
        let outer = with_values!(self, v => v.capacity().saturating_mul(size_of_val_element(v)));
        outer.saturating_add(match self {
            Self::Binary(v) => v.iter().map(Vec::capacity).sum(),
            Self::Strings(v) => v.iter().map(String::capacity).sum(),
            _ => 0,
        })
    }
}
fn size_of_val_element<T>(_: &[T]) -> usize {
    size_of::<T>()
}

/// Append one fixed-width value into spare capacity without calling the reallocating push path.
#[inline(always)]
fn push_copy<T: Copy>(values: &mut Vec<T>, value: T) {
    if let Some(slot) = values.spare_capacity_mut().first_mut() {
        slot.write(value);
        // SAFETY: `slot` is the next element of this allocation, and `write` initialized it.
        // The updated length remains within capacity.
        unsafe { values.set_len(values.len() + 1) };
    } else {
        values.push(value);
    }
}

/// A failed append leaves both values and null-mask rows unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnBufError {
    NullTimestamp,
    InvalidDecimal { precision: u32, scale: u32 },
    TypeMismatch { expected: ColumnDataType },
}
impl fmt::Display for ColumnBufError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NullTimestamp => f.write_str("timestamp columns cannot contain nulls"),
            Self::InvalidDecimal { precision, scale } => write!(
                f,
                "invalid decimal precision {precision} and scale {scale}: precision must be 1..=38 and scale must not exceed precision"
            ),
            Self::TypeMismatch { expected } => {
                write!(f, "value does not match column type {expected:?}")
            }
        }
    }
}
impl Error for ColumnBufError {}

/// A typed column containing only non-null values plus a mask for all rows.
/// Memory accounting includes all value and mask allocations, excluding the
/// schema name and allocator bookkeeping. Cleared variable-length payloads are
/// pooled so warmed ingestion cycles reuse their allocations.
#[derive(Clone, Debug)]
pub struct TypedColumnBuf {
    name: String,
    semantic_type: SemanticType,
    data_type: ColumnDataType,
    null_mask: NullMask,
    values: TypedValues,
    spare_strings: Vec<String>,
    spare_binary: Vec<Vec<u8>>,
}
impl TypedColumnBuf {
    pub fn new(
        name: impl Into<String>,
        semantic_type: SemanticType,
        data_type: ColumnDataType,
    ) -> Self {
        Self::with_capacity(name, semantic_type, data_type, 0)
    }
    pub fn with_capacity(
        name: impl Into<String>,
        semantic_type: SemanticType,
        data_type: ColumnDataType,
        rows: usize,
    ) -> Self {
        Self {
            name: name.into(),
            semantic_type,
            data_type,
            null_mask: NullMask::with_capacity(rows),
            values: TypedValues::new(data_type, rows),
            spare_strings: Vec::new(),
            spare_binary: Vec::new(),
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn semantic_type(&self) -> SemanticType {
        self.semantic_type
    }
    pub fn data_type(&self) -> ColumnDataType {
        self.data_type
    }
    pub fn null_mask(&self) -> &NullMask {
        &self.null_mask
    }
    pub fn row_count(&self) -> usize {
        self.null_mask.row_count()
    }
    pub fn non_null_len(&self) -> usize {
        self.values.len()
    }

    /// Append one row without changing the column on a type error.
    ///
    /// # Errors
    /// Returns an error for null timestamps, decimal metadata outside
    /// `1 <= precision <= 38` and `scale <= precision`, or a value type
    /// (including decimal precision and scale) that differs from the schema.
    pub fn push(&mut self, value: ValueRef<'_>) -> Result<(), ColumnBufError> {
        if let ColumnDataType::Decimal128 { precision, scale } = self.data_type {
            validate_decimal(precision, scale)?;
        }
        if let ValueRef::Decimal128(decimal) = &value {
            validate_decimal(decimal.precision, decimal.scale)?;
        }
        if value == ValueRef::Null {
            if self.semantic_type == SemanticType::Timestamp {
                return Err(ColumnBufError::NullTimestamp);
            }
            self.null_mask.push(true);
            return Ok(());
        }
        match (&mut self.values, self.data_type, value) {
            (TypedValues::Bools(v), ColumnDataType::Boolean, ValueRef::Boolean(x)) => {
                push_copy(v, x)
            }
            (TypedValues::I8(v), ColumnDataType::Int8, ValueRef::Int8(x)) => push_copy(v, x),
            (TypedValues::I16(v), ColumnDataType::Int16, ValueRef::Int16(x)) => push_copy(v, x),
            (TypedValues::I32(v), ColumnDataType::Int32, ValueRef::Int32(x)) => push_copy(v, x),
            (TypedValues::I64(v), ColumnDataType::Int64, ValueRef::Int64(x)) => push_copy(v, x),
            (TypedValues::U8(v), ColumnDataType::Uint8, ValueRef::Uint8(x)) => push_copy(v, x),
            (TypedValues::U16(v), ColumnDataType::Uint16, ValueRef::Uint16(x)) => push_copy(v, x),
            (TypedValues::U32(v), ColumnDataType::Uint32, ValueRef::Uint32(x)) => push_copy(v, x),
            (TypedValues::U64(v), ColumnDataType::Uint64, ValueRef::Uint64(x)) => push_copy(v, x),
            (TypedValues::F32(v), ColumnDataType::Float32, ValueRef::Float32(x)) => push_copy(v, x),
            (TypedValues::F64(v), ColumnDataType::Float64, ValueRef::Float64(x)) => push_copy(v, x),
            (TypedValues::I32(v), ColumnDataType::Date, ValueRef::Date(x)) => push_copy(v, x.0),
            (TypedValues::I64(v), ColumnDataType::DateTime, ValueRef::DateTime(x)) => {
                push_copy(v, x.0)
            }
            (
                TypedValues::I64(v),
                ColumnDataType::TimestampSecond,
                ValueRef::TimestampSecond(x),
            )
            | (
                TypedValues::I64(v),
                ColumnDataType::TimestampMillisecond,
                ValueRef::TimestampMillisecond(x),
            )
            | (
                TypedValues::I64(v),
                ColumnDataType::TimestampMicrosecond,
                ValueRef::TimestampMicrosecond(x),
            )
            | (
                TypedValues::I64(v),
                ColumnDataType::TimestampNanosecond,
                ValueRef::TimestampNanosecond(x),
            ) => push_copy(v, x),
            (TypedValues::Strings(v), ColumnDataType::String, ValueRef::String(x)) => {
                let mut owned = self.spare_strings.pop().unwrap_or_default();
                owned.push_str(x);
                v.push(owned);
            }
            (TypedValues::Binary(v), ColumnDataType::Binary, ValueRef::Binary(x)) => {
                let mut owned = self.spare_binary.pop().unwrap_or_default();
                owned.extend_from_slice(x);
                v.push(owned);
            }
            (TypedValues::Binary(v), ColumnDataType::Json, ValueRef::Json(x)) => {
                let mut owned = self.spare_binary.pop().unwrap_or_default();
                owned.extend_from_slice(x.as_bytes());
                v.push(owned);
            }
            (
                TypedValues::Decimal128(v),
                ColumnDataType::Decimal128 { precision, scale },
                ValueRef::Decimal128(x),
            ) if x.precision == precision && x.scale == scale => push_copy(v, x),
            _ => {
                return Err(ColumnBufError::TypeMismatch {
                    expected: self.data_type,
                });
            }
        }
        self.null_mask.push(false);
        debug_assert_eq!(
            self.non_null_len() + self.null_mask.null_count(),
            self.row_count()
        );
        Ok(())
    }

    pub fn to_proto_column(&self) -> Column {
        let mut values = Values::default();
        match &self.values {
            TypedValues::Bools(v) => values.bool_values = v.clone(),
            TypedValues::I8(v) => values.i8_values = v.iter().copied().map(i32::from).collect(),
            TypedValues::I16(v) => values.i16_values = v.iter().copied().map(i32::from).collect(),
            TypedValues::I32(v) => match self.data_type {
                ColumnDataType::Date => values.date_values = v.clone(),
                _ => values.i32_values = v.clone(),
            },
            TypedValues::I64(v) => match self.data_type {
                ColumnDataType::DateTime => values.datetime_values = v.clone(),
                ColumnDataType::TimestampSecond => values.timestamp_second_values = v.clone(),
                ColumnDataType::TimestampMillisecond => {
                    values.timestamp_millisecond_values = v.clone()
                }
                ColumnDataType::TimestampMicrosecond => {
                    values.timestamp_microsecond_values = v.clone()
                }
                ColumnDataType::TimestampNanosecond => {
                    values.timestamp_nanosecond_values = v.clone()
                }
                _ => values.i64_values = v.clone(),
            },
            TypedValues::U8(v) => values.u8_values = v.iter().copied().map(u32::from).collect(),
            TypedValues::U16(v) => values.u16_values = v.iter().copied().map(u32::from).collect(),
            TypedValues::U32(v) => values.u32_values = v.clone(),
            TypedValues::U64(v) => values.u64_values = v.clone(),
            TypedValues::F32(v) => values.f32_values = v.clone(),
            TypedValues::F64(v) => values.f64_values = v.clone(),
            TypedValues::Binary(v) => values.binary_values = v.clone(),
            TypedValues::Strings(v) => values.string_values = v.clone(),
            TypedValues::Decimal128(v) => {
                values.decimal128_values = v.iter().copied().map(Decimal128::to_proto).collect()
            }
        }
        Column {
            column_name: self.name.clone(),
            semantic_type: self.semantic_type.to_proto(),
            values: Some(values),
            null_mask: self.null_mask.to_proto_bytes(),
            datatype: self.data_type.to_proto_datatype(),
            datatype_extension: self.data_type.to_proto_extension(),
            ..Column::default()
        }
    }
}
fn validate_decimal(precision: u32, scale: u32) -> Result<(), ColumnBufError> {
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(ColumnBufError::InvalidDecimal { precision, scale });
    }
    Ok(())
}
impl MemoryBuffer for TypedColumnBuf {
    fn allocated_bytes(&self) -> usize {
        self.values
            .allocated_bytes()
            .saturating_add(self.null_mask.allocated_bytes())
            .saturating_add(
                self.spare_strings
                    .capacity()
                    .saturating_mul(size_of::<String>()),
            )
            .saturating_add(
                self.spare_binary
                    .capacity()
                    .saturating_mul(size_of::<Vec<u8>>()),
            )
            .saturating_add(
                self.spare_strings
                    .iter()
                    .map(String::capacity)
                    .sum::<usize>(),
            )
            .saturating_add(self.spare_binary.iter().map(Vec::capacity).sum::<usize>())
    }
    fn row_capacity(&self) -> usize {
        self.values
            .capacity()
            .max(self.null_mask.row_capacity())
            .max(self.spare_strings.capacity())
            .max(self.spare_binary.capacity())
    }
    fn clear_with_policy(&mut self, policy: BufferPolicy) {
        if policy.max_retained_rows == 0
            || policy.max_retained_bytes == 0
            || policy.should_shrink(self.row_capacity(), self.allocated_bytes())
        {
            self.values = TypedValues::new(self.data_type, 0);
            self.spare_strings = Vec::new();
            self.spare_binary = Vec::new();
            self.null_mask.clear_with_policy(BufferPolicy::zero());
            return;
        }
        match &mut self.values {
            TypedValues::Strings(values) => {
                for value in values.iter_mut() {
                    value.clear();
                }
                self.spare_strings.append(values);
            }
            TypedValues::Binary(values) => {
                for value in values.iter_mut() {
                    value.clear();
                }
                self.spare_binary.append(values);
            }
            _ => with_values!(&mut self.values, v => v.clear()),
        }
        self.null_mask.clear_unbounded();
        if policy.should_shrink(self.row_capacity(), self.allocated_bytes()) {
            self.values = TypedValues::new(self.data_type, 0);
            self.spare_strings = Vec::new();
            self.spare_binary = Vec::new();
            self.null_mask.clear_with_policy(BufferPolicy::zero());
        }
    }
    fn shrink_to_fit(&mut self) {
        match &mut self.values {
            TypedValues::Strings(values) => {
                for value in values {
                    value.shrink_to_fit();
                }
            }
            TypedValues::Binary(values) => {
                for value in values {
                    value.shrink_to_fit();
                }
            }
            _ => {}
        }
        with_values!(&mut self.values, v => v.shrink_to_fit());
        self.spare_strings = Vec::new();
        self.spare_binary = Vec::new();
        self.null_mask.shrink_to_fit();
    }
}
