//! Name-bound Arrow query decoding into reusable typed row storage.

use std::collections::HashMap;

pub use arrow_array::RecordBatch;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, DictionaryArray, LargeBinaryArray,
    LargeStringArray, PrimitiveArray, StringArray, StringViewArray, types::*,
};
use arrow_schema::{DataType, TimeUnit};

use super::{ColumnMeta, Field, Row, field_error};
use crate::{
    buffer_policy::BufferPolicy,
    error::{Error, Result},
    types::{self, ColumnDataType, SemanticType},
};

/// Decodes query columns into a derived row's owned columnar storage.
pub trait RowDecode: Row {
    /// Builds a batch under the default memory-retention policy.
    /// # Errors
    /// Rejects incompatible columns, semantic annotations, or nullability.
    fn decode_record_batch(batch: &RecordBatch) -> Result<Self::Batch> {
        Self::decode_record_batch_with_policy(batch, BufferPolicy::default())
    }
    /// Builds a batch under the supplied memory-retention policy.
    /// # Errors
    /// Rejects incompatible columns, semantic annotations, or nullability.
    fn decode_record_batch_with_policy(
        batch: &RecordBatch,
        policy: BufferPolicy,
    ) -> Result<Self::Batch>;
}

/// Columns resolved once by name in the derived schema's declared order.
#[doc(hidden)]
pub struct PreparedRecordBatch {
    columns: Vec<ArrayRef>,
}
impl PreparedRecordBatch {
    /// Binds and validates every requested column without reading row values.
    /// # Errors
    /// Rejects duplicate names, missing columns, or incompatible metadata.
    pub fn new(batch: &RecordBatch, expected: &[ColumnMeta]) -> Result<Self> {
        let schema = batch.schema();
        let mut names = HashMap::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            if names.insert(field.name().as_str(), index).is_some() {
                return Err(schema_error(field.name(), "duplicate Arrow column name"));
            }
        }
        let mut columns = Vec::with_capacity(expected.len());
        for meta in expected {
            let index = *names
                .get(meta.name)
                .ok_or_else(|| schema_error(meta.name, "missing Arrow column"))?;
            let field = schema.field(index);
            if let Some(role) = field.metadata().get("greptime:semantic_type")
                && role != meta.semantic_type.as_flight_str()
            {
                return Err(schema_error(meta.name, "conflicting semantic type"));
            }
            if let Some(value) = field.metadata().get("greptime:time_index") {
                match value.as_str() {
                    "true" if meta.semantic_type == SemanticType::Timestamp => {}
                    "false" if meta.semantic_type != SemanticType::Timestamp => {}
                    _ => return Err(schema_error(meta.name, "conflicting time-index annotation")),
                }
            }
            let json = field.metadata().get("greptime:type").map(String::as_str);
            if (meta.data_type == ColumnDataType::Json && json != Some("Json"))
                || (meta.data_type != ColumnDataType::Json && json == Some("Json"))
            {
                return Err(schema_error(
                    meta.name,
                    "conflicting or missing JSON annotation",
                ));
            }
            validate_type(field.data_type(), meta)?;
            columns.push(ArrayRef::clone(batch.column(index)));
        }
        Ok(Self { columns })
    }
    /// Returns a bound column in the requested schema order.
    /// # Errors
    /// Rejects an index outside the derived schema.
    pub fn column(&self, index: usize) -> Result<&ArrayRef> {
        self.columns
            .get(index)
            .ok_or_else(|| Error::Decode("bound Arrow column index is out of range".into()))
    }
}

fn schema_error(name: &str, reason: &str) -> Error {
    Error::SchemaViolation(format!("column '{name}': {reason}"))
}
fn dictionary_value_type(mut data_type: &DataType) -> Result<&DataType> {
    while let DataType::Dictionary(key, values) = data_type {
        if !matches!(
            key.as_ref(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ) {
            return Err(Error::Decode(
                "unsupported Arrow dictionary key type".into(),
            ));
        }
        data_type = values;
    }
    Ok(data_type)
}
fn validate_type(data_type: &DataType, meta: &ColumnMeta) -> Result<()> {
    let data_type = dictionary_value_type(data_type)?;
    let compatible = match meta.data_type {
        ColumnDataType::Boolean => *data_type == DataType::Boolean,
        ColumnDataType::Int8 => *data_type == DataType::Int8,
        ColumnDataType::Int16 => *data_type == DataType::Int16,
        ColumnDataType::Int32 => *data_type == DataType::Int32,
        ColumnDataType::Int64 => *data_type == DataType::Int64,
        ColumnDataType::Uint8 => *data_type == DataType::UInt8,
        ColumnDataType::Uint16 => *data_type == DataType::UInt16,
        ColumnDataType::Uint32 => *data_type == DataType::UInt32,
        ColumnDataType::Uint64 => *data_type == DataType::UInt64,
        ColumnDataType::Float32 => *data_type == DataType::Float32,
        ColumnDataType::Float64 => *data_type == DataType::Float64,
        ColumnDataType::String => matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        ColumnDataType::Binary => matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        ),
        ColumnDataType::Json => matches!(
            data_type,
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        ),
        ColumnDataType::Date => *data_type == DataType::Date32,
        ColumnDataType::DateTime => *data_type == DataType::Date64,
        ColumnDataType::TimestampSecond => {
            matches!(data_type, DataType::Timestamp(TimeUnit::Second, None))
        }
        ColumnDataType::TimestampMillisecond => {
            matches!(data_type, DataType::Timestamp(TimeUnit::Millisecond, None))
        }
        ColumnDataType::TimestampMicrosecond => {
            matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        ColumnDataType::TimestampNanosecond => {
            matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        ColumnDataType::Decimal128 { precision, scale } => {
            matches!(data_type, DataType::Decimal128(p, s)
            if u32::from(*p) == precision && u32::try_from(*s) == Ok(scale))
        }
    };
    if compatible {
        Ok(())
    } else {
        Err(schema_error(
            meta.name,
            "Arrow type does not match derived field",
        ))
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::Decode("Arrow array representation does not match datatype".into()))
}
fn resolved(mut array: &dyn Array, mut index: usize) -> Result<Option<(&dyn Array, usize)>> {
    loop {
        if index >= array.len() {
            return Err(Error::Decode(
                "Arrow dictionary key or row index is out of range".into(),
            ));
        }
        if array.is_null(index) {
            return Ok(None);
        }
        macro_rules! key {
            ($ty:ty) => {{
                let dict = downcast::<DictionaryArray<$ty>>(array)?;
                let key = usize::try_from(dict.keys().value(index)).map_err(|_| {
                    Error::Decode("Arrow dictionary key is outside usize range".into())
                })?;
                (dict.values().as_ref(), key)
            }};
        }
        let DataType::Dictionary(key, _) = array.data_type() else {
            return Ok(Some((array, index)));
        };
        (array, index) = match key.as_ref() {
            DataType::Int8 => key!(Int8Type),
            DataType::Int16 => key!(Int16Type),
            DataType::Int32 => key!(Int32Type),
            DataType::Int64 => key!(Int64Type),
            DataType::UInt8 => key!(UInt8Type),
            DataType::UInt16 => key!(UInt16Type),
            DataType::UInt32 => key!(UInt32Type),
            DataType::UInt64 => key!(UInt64Type),
            _ => {
                return Err(Error::Decode(
                    "unsupported Arrow dictionary key type".into(),
                ));
            }
        };
    }
}

fn non_null<'a>(
    array: &'a dyn Array,
    index: usize,
    meta: &ColumnMeta,
) -> Result<(&'a dyn Array, usize)> {
    resolved(array, index)?
        .ok_or_else(|| schema_error(meta.name, "unexpected null in non-nullable field"))
}

/// Converts one Arrow column directly into the corresponding typed SoA buffer.
#[doc(hidden)]
pub trait ArrowField: Field {
    /// Reads a value at a checked, non-null Arrow row.
    /// # Errors
    /// Rejects incompatible representations or values.
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>>;
    /// Populates owned typed storage column-wise, including null positions.
    /// # Errors
    /// Rejects invalid types, invalid values, or unexpected nulls.
    fn decode_column(array: &ArrayRef, meta: &ColumnMeta) -> Result<Self::Buffer> {
        validate_type(array.data_type(), meta)?;
        let mut buffer = Self::with_capacity(array.len());
        for index in 0..array.len() {
            Self::push(&mut buffer, Self::read(array.as_ref(), index, meta)?);
        }
        Self::validate(&buffer, meta).map_err(field_error)?;
        Ok(buffer)
    }
}
macro_rules! primitive {
    ($rust:ty, $arrow:ty, $map:expr) => {
        impl ArrowField for $rust {
            fn read<'a>(
                array: &'a dyn Array,
                index: usize,
                meta: &ColumnMeta,
            ) -> Result<Self::Input<'a>> {
                let (array, index) = non_null(array, index, meta)?;
                let value = downcast::<PrimitiveArray<$arrow>>(array)?.value(index);
                Ok(($map)(value))
            }
        }
    };
}
primitive!(i8, Int8Type, |v| v);
primitive!(i16, Int16Type, |v| v);
primitive!(u8, UInt8Type, |v| v);
primitive!(u16, UInt16Type, |v| v);
primitive!(u32, UInt32Type, |v| v);
primitive!(u64, UInt64Type, |v| v);
primitive!(f32, Float32Type, |v| v);
primitive!(f64, Float64Type, |v| v);
primitive!(i128, Decimal128Type, |v| v);
primitive!(types::Date, Date32Type, types::Date);
primitive!(types::DateTime, Date64Type, types::DateTime);
primitive!(
    types::TimestampSecond,
    TimestampSecondType,
    types::TimestampSecond
);
primitive!(
    types::TimestampMillisecond,
    TimestampMillisecondType,
    types::TimestampMillisecond
);
primitive!(
    types::TimestampMicrosecond,
    TimestampMicrosecondType,
    types::TimestampMicrosecond
);
primitive!(
    types::TimestampNanosecond,
    TimestampNanosecondType,
    types::TimestampNanosecond
);
impl ArrowField for bool {
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>> {
        let (array, index) = non_null(array, index, meta)?;
        Ok(downcast::<BooleanArray>(array)?.value(index))
    }
}
impl ArrowField for i32 {
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>> {
        let (array, index) = non_null(array, index, meta)?;
        match array.data_type() {
            DataType::Date32 => Ok(downcast::<PrimitiveArray<Date32Type>>(array)?.value(index)),
            _ => Ok(downcast::<PrimitiveArray<Int32Type>>(array)?.value(index)),
        }
    }
}
impl ArrowField for i64 {
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>> {
        let (array, index) = non_null(array, index, meta)?;
        macro_rules! value {
            ($ty:ty) => {
                Ok(downcast::<PrimitiveArray<$ty>>(array)?.value(index))
            };
        }
        match array.data_type() {
            DataType::Date64 => value!(Date64Type),
            DataType::Timestamp(TimeUnit::Second, _) => value!(TimestampSecondType),
            DataType::Timestamp(TimeUnit::Millisecond, _) => value!(TimestampMillisecondType),
            DataType::Timestamp(TimeUnit::Microsecond, _) => value!(TimestampMicrosecondType),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => value!(TimestampNanosecondType),
            _ => value!(Int64Type),
        }
    }
}
impl ArrowField for types::Decimal128 {
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>> {
        let value = <i128 as ArrowField>::read(array, index, meta)?;
        let ColumnDataType::Decimal128 { precision, scale } = meta.data_type else {
            return Err(schema_error(meta.name, "decimal metadata is required"));
        };
        Ok(types::Decimal128::new(value, precision, scale))
    }
}
fn bytes(array: &dyn Array, index: usize) -> Result<&[u8]> {
    match array.data_type() {
        DataType::Binary => Ok(downcast::<BinaryArray>(array)?.value(index)),
        DataType::LargeBinary => Ok(downcast::<LargeBinaryArray>(array)?.value(index)),
        DataType::BinaryView => Ok(downcast::<BinaryViewArray>(array)?.value(index)),
        DataType::Utf8 => Ok(downcast::<StringArray>(array)?.value(index).as_bytes()),
        DataType::LargeUtf8 => Ok(downcast::<LargeStringArray>(array)?.value(index).as_bytes()),
        DataType::Utf8View => Ok(downcast::<StringViewArray>(array)?.value(index).as_bytes()),
        _ => Err(Error::Decode("Arrow value is not text or binary".into())),
    }
}
impl ArrowField for String {
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>> {
        let (array, index) = non_null(array, index, meta)?;
        std::str::from_utf8(bytes(array, index)?)
            .map_err(|error| Error::Decode(format!("invalid JSON/string UTF-8: {error}")))
    }
}
impl ArrowField for Vec<u8> {
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>> {
        let (array, index) = non_null(array, index, meta)?;
        bytes(array, index)
    }
}
impl<T: ArrowField> ArrowField for Option<T> {
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>> {
        match resolved(array, index)? {
            Some((array, index)) => T::read(array, index, meta).map(Some),
            None if meta.semantic_type != SemanticType::Timestamp => Ok(None),
            None => Err(schema_error(meta.name, "unexpected null time index")),
        }
    }
}

macro_rules! borrowed_field {
    ($borrowed:ty, $owned:ty) => {
        impl Field for $borrowed {
            type Buffer = <$owned as Field>::Buffer;
            type Input<'a> = <$owned as Field>::Input<'a>;
            fn with_capacity(rows: usize) -> Self::Buffer {
                <$owned as Field>::with_capacity(rows)
            }
            fn len(buffer: &Self::Buffer) -> usize {
                <$owned as Field>::len(buffer)
            }
            fn push(buffer: &mut Self::Buffer, value: Self::Input<'_>) {
                <$owned as Field>::push(buffer, value);
            }
            fn get<'a>(buffer: &'a Self::Buffer, index: usize) -> Option<Self::Input<'a>> {
                <$owned as Field>::get(buffer, index)
            }
            fn accepts_data_type(data: ColumnDataType) -> bool {
                <$owned as Field>::accepts_data_type(data)
            }
            fn validate(
                buffer: &Self::Buffer,
                meta: &ColumnMeta,
            ) -> std::result::Result<(), super::RowError> {
                <$owned as Field>::validate(buffer, meta)
            }
            fn to_column(
                buffer: &Self::Buffer,
                meta: &ColumnMeta,
            ) -> std::result::Result<greptimedb_proto::v1::Column, super::RowError> {
                <$owned as Field>::to_column(buffer, meta)
            }
        }
        impl ArrowField for $borrowed {
            fn read<'a>(
                array: &'a dyn Array,
                index: usize,
                meta: &ColumnMeta,
            ) -> Result<Self::Input<'a>> {
                <$owned as ArrowField>::read(array, index, meta)
            }
        }
    };
}
borrowed_field!(&str, String);
borrowed_field!(&[u8], Vec<u8>);

/// Borrowed JSON text stored in an owned flat buffer during query decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Json<'a>(pub &'a str);
impl Field for Json<'_> {
    type Buffer = <String as Field>::Buffer;
    type Input<'a> = Json<'a>;
    fn with_capacity(rows: usize) -> Self::Buffer {
        <String as Field>::with_capacity(rows)
    }
    fn len(buffer: &Self::Buffer) -> usize {
        <String as Field>::len(buffer)
    }
    fn push(buffer: &mut Self::Buffer, value: Self::Input<'_>) {
        <String as Field>::push(buffer, value.0);
    }
    fn get<'a>(buffer: &'a Self::Buffer, index: usize) -> Option<Self::Input<'a>> {
        <String as Field>::get(buffer, index).map(Json)
    }
    fn accepts_data_type(data: ColumnDataType) -> bool {
        data == ColumnDataType::Json
    }
    fn validate_values(
        buffer: &Self::Buffer,
        meta: &ColumnMeta,
    ) -> std::result::Result<(), super::RowError> {
        <String as Field>::validate_values(buffer, meta)
    }
    fn to_column(
        buffer: &Self::Buffer,
        meta: &ColumnMeta,
    ) -> std::result::Result<greptimedb_proto::v1::Column, super::RowError> {
        Self::validate(buffer, meta)?;
        <String as Field>::to_column(buffer, meta)
    }
}
impl ArrowField for Json<'_> {
    fn read<'a>(array: &'a dyn Array, index: usize, meta: &ColumnMeta) -> Result<Self::Input<'a>> {
        if meta.data_type != ColumnDataType::Json {
            return Err(schema_error(meta.name, "JSON metadata required"));
        }
        <String as ArrowField>::read(array, index, meta).map(Json)
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    use crate::buffer_policy::MemoryBuffer;
    use arrow_schema::{Field as ArrowColumn, Schema};
    use std::sync::Arc;

    fn meta(data_type: ColumnDataType, nullable: bool) -> ColumnMeta {
        ColumnMeta {
            name: "value",
            semantic_type: SemanticType::Field,
            data_type,
            nullable,
        }
    }

    #[test]
    fn scenario_dictionary_all_integer_keys_preserve_value_nulls_and_key_nulls() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("first"), None]));
        let meta = meta(ColumnDataType::String, true);
        macro_rules! check {
            ($ty:ty) => {{
                let keys = PrimitiveArray::<$ty>::from(vec![Some(0), Some(1), None, Some(0)]);
                let array: ArrayRef = Arc::new(
                    DictionaryArray::<$ty>::try_new(keys, Arc::clone(&values))
                        .map_err(|e| Error::Decode(e.to_string()))?,
                );
                let buffer = <Option<String> as ArrowField>::decode_column(&array, &meta)?;
                assert_eq!(
                    <Option<String> as Field>::get(&buffer, 0),
                    Some(Some("first"))
                );
                assert_eq!(<Option<String> as Field>::get(&buffer, 1), Some(None));
                assert_eq!(<Option<String> as Field>::get(&buffer, 2), Some(None));
                assert_eq!(
                    <Option<String> as Field>::get(&buffer, 3),
                    Some(Some("first"))
                );
                assert!(matches!(resolved(array.as_ref(), 4), Err(Error::Decode(_))));
                assert!(matches!(
                    <String as ArrowField>::decode_column(
                        &array,
                        &ColumnMeta {
                            nullable: false,
                            ..meta
                        }
                    ),
                    Err(Error::SchemaViolation(_))
                ));
            }};
        }
        check!(Int8Type);
        check!(Int16Type);
        check!(Int32Type);
        check!(Int64Type);
        check!(UInt8Type);
        check!(UInt16Type);
        check!(UInt32Type);
        check!(UInt64Type);
        Ok(())
    }

    #[test]
    fn scenario_timestamp_timezone_and_null_dictionary_value_are_rejected() -> Result<()> {
        let meta = ColumnMeta {
            semantic_type: SemanticType::Timestamp,
            ..meta(ColumnDataType::TimestampNanosecond, false)
        };
        let array: ArrayRef =
            Arc::new(PrimitiveArray::<TimestampNanosecondType>::from(vec![1]).with_timezone("UTC"));
        assert!(matches!(
            <i64 as ArrowField>::decode_column(&array, &meta),
            Err(Error::SchemaViolation(_))
        ));
        let values: ArrayRef = Arc::new(PrimitiveArray::<TimestampNanosecondType>::from(vec![
            Some(1),
            None,
        ]));
        let array: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                PrimitiveArray::<Int32Type>::from(vec![1]),
                values,
            )
            .map_err(|e| Error::Decode(e.to_string()))?,
        );
        assert!(matches!(
            <i64 as ArrowField>::decode_column(&array, &meta),
            Err(Error::SchemaViolation(_))
        ));
        Ok(())
    }

    #[test]
    fn scenario_json_and_borrowed_buffers_own_decoded_values_and_obey_retention() -> Result<()> {
        let json_meta = meta(ColumnDataType::Json, false);
        let array: ArrayRef =
            Arc::new(BinaryViewArray::from_iter_values([b"{\"a\":1}".as_slice()]));
        let mut buffer = <Json<'_> as ArrowField>::decode_column(&array, &json_meta)?;
        drop(array);
        assert_eq!(
            <Json<'_> as Field>::get(&buffer, 0),
            Some(Json("{\"a\":1}"))
        );
        buffer.clear_with_policy(BufferPolicy::zero());
        assert_eq!(buffer.allocated_bytes(), 0);
        let text: ArrayRef = Arc::new(LargeStringArray::from(vec!["tokyo"]));
        let text_buffer =
            <&str as ArrowField>::decode_column(&text, &meta(ColumnDataType::String, false))?;
        assert_eq!(<&str as Field>::get(&text_buffer, 0), Some("tokyo"));
        let binary: ArrayRef =
            Arc::new(LargeBinaryArray::from_iter_values([b"\x00\xff".as_slice()]));
        let binary_buffer =
            <&[u8] as ArrowField>::decode_column(&binary, &meta(ColumnDataType::Binary, false))?;
        assert_eq!(
            <&[u8] as Field>::get(&binary_buffer, 0),
            Some(b"\x00\xff".as_slice())
        );
        let invalid: ArrayRef = Arc::new(BinaryArray::from_iter_values([b"\xff".as_slice()]));
        assert!(matches!(
            <Json<'_> as ArrowField>::decode_column(&invalid, &json_meta),
            Err(Error::Decode(_))
        ));
        Ok(())
    }

    #[test]
    fn scenario_prepared_schema_checks_annotations_and_empty_physical_types() -> Result<()> {
        let column: ArrayRef = Arc::new(StringViewArray::from_iter_values(["{}"]));
        let expected = meta(ColumnDataType::Json, false);
        let make = |metadata| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    ArrowColumn::new("value", DataType::Utf8View, false).with_metadata(metadata),
                ])),
                vec![Arc::clone(&column)],
            )
            .map_err(|e| Error::Decode(e.to_string()))
        };
        assert!(matches!(
            PreparedRecordBatch::new(&make(HashMap::new())?, &[expected]),
            Err(Error::SchemaViolation(_))
        ));
        let mut annotations = HashMap::from([("greptime:type".into(), "Json".into())]);
        let prepared = PreparedRecordBatch::new(&make(annotations.clone())?, &[expected])?;
        let buffer = <String as ArrowField>::decode_column(prepared.column(0)?, &expected)?;
        assert_eq!(<String as Field>::get(&buffer, 0), Some("{}"));
        annotations.insert("greptime:semantic_type".into(), "tag".into());
        assert!(matches!(
            PreparedRecordBatch::new(&make(annotations)?, &[expected]),
            Err(Error::SchemaViolation(_))
        ));
        let empty: ArrayRef = Arc::new(PrimitiveArray::<Int64Type>::from(Vec::<i64>::new()));
        assert!(matches!(
            <i32 as ArrowField>::decode_column(&empty, &meta(ColumnDataType::Int32, false)),
            Err(Error::SchemaViolation(_))
        ));
        Ok(())
    }
}
