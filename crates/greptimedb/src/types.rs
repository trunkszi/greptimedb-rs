//! GreptimeDB scalar types and their protobuf representations.

use greptimedb_proto::v1::{self as proto, column_data_type_extension::TypeExt, value::ValueData};

/// The role of a column in a GreptimeDB table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum SemanticType {
    /// Primary key or indexed dimension.
    Tag = 0,
    /// A measured value.
    Field = 1,
    /// The table's non-null time index.
    Timestamp = 2,
}

impl SemanticType {
    /// Returns the protobuf enum discriminant.
    pub const fn to_proto(self) -> i32 {
        self as i32
    }

    /// Returns the Arrow Flight `greptime:semantic_type` metadata value.
    pub const fn as_flight_str(self) -> &'static str {
        match self {
            Self::Tag => "tag",
            Self::Field => "field",
            Self::Timestamp => "timestamp",
        }
    }
}

/// The resolution of a timestamp measured from the Unix epoch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum TimestampUnit {
    /// Seconds.
    Second,
    /// Milliseconds.
    Millisecond,
    /// Microseconds.
    Microsecond,
    /// Nanoseconds.
    #[default]
    Nanosecond,
}

impl TimestampUnit {
    /// Returns the corresponding column type.
    pub const fn to_column_data_type(self) -> ColumnDataType {
        match self {
            Self::Second => ColumnDataType::TimestampSecond,
            Self::Millisecond => ColumnDataType::TimestampMillisecond,
            Self::Microsecond => ColumnDataType::TimestampMicrosecond,
            Self::Nanosecond => ColumnDataType::TimestampNanosecond,
        }
    }

    /// Returns the number of ticks in one hour at this resolution.
    pub const fn one_hour_ticks(self) -> i64 {
        match self {
            Self::Second => 3_600,
            Self::Millisecond => 3_600_000,
            Self::Microsecond => 3_600_000_000,
            Self::Nanosecond => 3_600_000_000_000,
        }
    }
}

macro_rules! temporal_type {
    ($name:ident, $storage:ty, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name(pub $storage);
    };
}

temporal_type!(TimestampSecond, i64, "Unix timestamp in seconds.");
temporal_type!(TimestampMillisecond, i64, "Unix timestamp in milliseconds.");
temporal_type!(TimestampMicrosecond, i64, "Unix timestamp in microseconds.");
temporal_type!(TimestampNanosecond, i64, "Unix timestamp in nanoseconds.");
temporal_type!(Date, i32, "Days since the Unix epoch.");
temporal_type!(DateTime, i64, "An i64 GreptimeDB DATETIME wire value.");

/// A signed decimal coefficient with explicit precision and scale.
///
/// The represented value is `value * 10^(-scale)`. Construction preserves the
/// supplied metadata; column buffers require precision in `1..=38`, scale no
/// greater than precision, and metadata matching their declared type on append.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Decimal128 {
    /// The unscaled signed coefficient.
    pub value: i128,
    /// Maximum number of decimal digits.
    pub precision: u32,
    /// Number of fractional decimal digits.
    pub scale: u32,
}

impl Decimal128 {
    /// Constructs a coefficient with the supplied precision and scale.
    pub const fn new(value: i128, precision: u32, scale: u32) -> Self {
        Self {
            value,
            precision,
            scale,
        }
    }

    /// Returns the signed high 64 bits of the coefficient.
    pub const fn hi(self) -> i64 {
        (self.value >> 64) as i64
    }

    /// Returns the low 64 bits of the coefficient.
    pub const fn lo(self) -> u64 {
        self.value as u64
    }

    /// Encodes the coefficient; precision and scale belong to the column type.
    pub fn to_proto(self) -> proto::Decimal128 {
        proto::Decimal128 {
            hi: self.hi(),
            lo: self.lo() as i64,
        }
    }

    /// Reconstructs a signed coefficient without sign-extending its low half.
    pub fn from_proto(proto: proto::Decimal128, precision: u32, scale: u32) -> Self {
        let value = (i128::from(proto.hi) << 64) | i128::from(proto.lo as u64);
        Self::new(value, precision, scale)
    }
}

/// A supported column's logical scalar type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ColumnDataType {
    /// Boolean.
    Boolean,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 8-bit integer.
    Uint8,
    /// Unsigned 16-bit integer.
    Uint16,
    /// Unsigned 32-bit integer.
    Uint32,
    /// Unsigned 64-bit integer.
    Uint64,
    /// IEEE 754 single precision float.
    Float32,
    /// IEEE 754 double precision float.
    Float64,
    /// Arbitrary bytes.
    Binary,
    /// UTF-8 text.
    String,
    /// Days since the Unix epoch.
    Date,
    /// An i64 GreptimeDB DATETIME wire value.
    DateTime,
    /// Unix timestamp in seconds.
    TimestampSecond,
    /// Unix timestamp in milliseconds.
    TimestampMillisecond,
    /// Unix timestamp in microseconds.
    TimestampMicrosecond,
    /// Unix timestamp in nanoseconds.
    TimestampNanosecond,
    /// A signed decimal coefficient with column-level precision and scale.
    Decimal128 {
        /// Maximum number of decimal digits.
        precision: u32,
        /// Number of fractional decimal digits.
        scale: u32,
    },
    /// UTF-8 JSON encoded as binary with a JSON type extension.
    Json,
}

impl ColumnDataType {
    /// Returns the physical protobuf datatype discriminant.
    pub const fn to_proto_datatype(self) -> i32 {
        match self {
            Self::Boolean => proto::ColumnDataType::Boolean as i32,
            Self::Int8 => proto::ColumnDataType::Int8 as i32,
            Self::Int16 => proto::ColumnDataType::Int16 as i32,
            Self::Int32 => proto::ColumnDataType::Int32 as i32,
            Self::Int64 => proto::ColumnDataType::Int64 as i32,
            Self::Uint8 => proto::ColumnDataType::Uint8 as i32,
            Self::Uint16 => proto::ColumnDataType::Uint16 as i32,
            Self::Uint32 => proto::ColumnDataType::Uint32 as i32,
            Self::Uint64 => proto::ColumnDataType::Uint64 as i32,
            Self::Float32 => proto::ColumnDataType::Float32 as i32,
            Self::Float64 => proto::ColumnDataType::Float64 as i32,
            Self::Binary | Self::Json => proto::ColumnDataType::Binary as i32,
            Self::String => proto::ColumnDataType::String as i32,
            Self::Date => proto::ColumnDataType::Date as i32,
            Self::DateTime => proto::ColumnDataType::Datetime as i32,
            Self::TimestampSecond => proto::ColumnDataType::TimestampSecond as i32,
            Self::TimestampMillisecond => proto::ColumnDataType::TimestampMillisecond as i32,
            Self::TimestampMicrosecond => proto::ColumnDataType::TimestampMicrosecond as i32,
            Self::TimestampNanosecond => proto::ColumnDataType::TimestampNanosecond as i32,
            Self::Decimal128 { .. } => proto::ColumnDataType::Decimal128 as i32,
        }
    }

    /// Returns metadata required to interpret decimal or JSON physical values.
    pub fn to_proto_extension(self) -> Option<proto::ColumnDataTypeExtension> {
        let type_ext = match self {
            Self::Decimal128 { precision, scale } => {
                TypeExt::DecimalType(proto::DecimalTypeExtension {
                    precision: precision.min(i32::MAX as u32) as i32,
                    scale: scale.min(i32::MAX as u32) as i32,
                })
            }
            Self::Json => TypeExt::JsonType(proto::JsonTypeExtension::JsonBinary as i32),
            _ => return None,
        };
        Some(proto::ColumnDataTypeExtension {
            type_ext: Some(type_ext),
        })
    }
}

/// A borrowed scalar value accepted by a dynamic column buffer.
#[derive(Clone, Debug, PartialEq)]
pub enum ValueRef<'a> {
    /// SQL NULL.
    Null,
    /// Boolean.
    Boolean(bool),
    /// Signed 8-bit integer.
    Int8(i8),
    /// Signed 16-bit integer.
    Int16(i16),
    /// Signed 32-bit integer.
    Int32(i32),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 8-bit integer.
    Uint8(u8),
    /// Unsigned 16-bit integer.
    Uint16(u16),
    /// Unsigned 32-bit integer.
    Uint32(u32),
    /// Unsigned 64-bit integer.
    Uint64(u64),
    /// IEEE 754 single precision float.
    Float32(f32),
    /// IEEE 754 double precision float.
    Float64(f64),
    /// Arbitrary borrowed bytes.
    Binary(&'a [u8]),
    /// Borrowed UTF-8 text.
    String(&'a str),
    /// Days since the Unix epoch.
    Date(Date),
    /// An i64 GreptimeDB DATETIME wire value.
    DateTime(DateTime),
    /// Unix timestamp in seconds.
    TimestampSecond(i64),
    /// Unix timestamp in milliseconds.
    TimestampMillisecond(i64),
    /// Unix timestamp in microseconds.
    TimestampMicrosecond(i64),
    /// Unix timestamp in nanoseconds.
    TimestampNanosecond(i64),
    /// Signed decimal coefficient with precision and scale.
    Decimal128(Decimal128),
    /// Borrowed UTF-8 JSON.
    Json(&'a str),
}

impl ValueRef<'_> {
    /// Copies this value into an owned protobuf row value.
    pub fn to_proto_value(&self) -> proto::Value {
        let value_data = match self {
            Self::Null => None,
            Self::Boolean(value) => Some(ValueData::BoolValue(*value)),
            Self::Int8(value) => Some(ValueData::I8Value(i32::from(*value))),
            Self::Int16(value) => Some(ValueData::I16Value(i32::from(*value))),
            Self::Int32(value) => Some(ValueData::I32Value(*value)),
            Self::Int64(value) => Some(ValueData::I64Value(*value)),
            Self::Uint8(value) => Some(ValueData::U8Value(u32::from(*value))),
            Self::Uint16(value) => Some(ValueData::U16Value(u32::from(*value))),
            Self::Uint32(value) => Some(ValueData::U32Value(*value)),
            Self::Uint64(value) => Some(ValueData::U64Value(*value)),
            Self::Float32(value) => Some(ValueData::F32Value(*value)),
            Self::Float64(value) => Some(ValueData::F64Value(*value)),
            Self::Binary(value) => Some(ValueData::BinaryValue(value.to_vec())),
            Self::String(value) => Some(ValueData::StringValue((*value).to_owned())),
            Self::Date(value) => Some(ValueData::DateValue(value.0)),
            Self::DateTime(value) => Some(ValueData::DatetimeValue(value.0)),
            Self::TimestampSecond(value) => Some(ValueData::TimestampSecondValue(*value)),
            Self::TimestampMillisecond(value) => Some(ValueData::TimestampMillisecondValue(*value)),
            Self::TimestampMicrosecond(value) => Some(ValueData::TimestampMicrosecondValue(*value)),
            Self::TimestampNanosecond(value) => Some(ValueData::TimestampNanosecondValue(*value)),
            Self::Decimal128(value) => Some(ValueData::Decimal128Value(value.to_proto())),
            Self::Json(value) => Some(ValueData::BinaryValue(value.as_bytes().to_vec())),
        };
        proto::Value { value_data }
    }
}
