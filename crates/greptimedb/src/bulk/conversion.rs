//! Conversion between GreptimeDB columnar wire values and Arrow arrays.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, RecordBatch,
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
        Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
        StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder, TimestampSecondBuilder, UInt8Builder, UInt16Builder,
        UInt32Builder, UInt64Builder,
    },
    cast::AsArray,
    types::{
        Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use greptimedb_proto::v1::{self, column_data_type_extension};

use crate::error::{Error, Result};

use super::schema::{JSON_TYPE_VALUE, SEMANTIC_TYPE_KEY, TYPE_EXTENSION_KEY};

pub(super) fn record_batch_from_columns(
    columns: &[v1::Column],
    rows: usize,
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for column in columns {
        let (field, array) = column_to_arrow(column, rows)?;
        fields.push(field);
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(|error| Error::SchemaViolation(error.to_string()))
}

fn column_to_arrow(column: &v1::Column, rows: usize) -> Result<(Field, ArrayRef)> {
    if column.column_name.is_empty() {
        return Err(Error::SchemaViolation(
            "column name must be nonempty".into(),
        ));
    }
    let semantic = v1::SemanticType::try_from(column.semantic_type).map_err(|_| {
        Error::SchemaViolation(format!(
            "unsupported semantic type {}",
            column.semantic_type
        ))
    })?;
    let (role, nullable) = match semantic {
        v1::SemanticType::Timestamp => ("timestamp", false),
        v1::SemanticType::Tag => ("tag", true),
        v1::SemanticType::Field => ("field", true),
    };
    let datatype = v1::ColumnDataType::try_from(column.datatype)
        .map_err(|_| Error::SchemaViolation(format!("unsupported datatype {}", column.datatype)))?;
    let json = is_json(column, datatype);
    if json
        && !matches!(
            datatype,
            v1::ColumnDataType::Binary | v1::ColumnDataType::Json
        )
    {
        return Err(Error::SchemaViolation(
            "JSON columns must use a binary physical type".into(),
        ));
    }
    let arrow_type = arrow_type(column, datatype)?;
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(SEMANTIC_TYPE_KEY.to_owned(), role.to_owned());
    if json {
        metadata.insert(TYPE_EXTENSION_KEY.to_owned(), JSON_TYPE_VALUE.to_owned());
    }
    let empty = v1::column::Values::default();
    let values = match column.values.as_ref() {
        Some(values) => values,
        None if rows == 0 => &empty,
        None => return Err(Error::SchemaViolation("column is missing values".into())),
    };
    let array = build_array(
        datatype,
        &arrow_type,
        rows,
        &column.null_mask,
        values,
        role == "timestamp",
    )?;
    let field = Field::new(&column.column_name, arrow_type, nullable).with_metadata(metadata);
    Ok((field, array))
}

fn is_json(column: &v1::Column, datatype: v1::ColumnDataType) -> bool {
    datatype == v1::ColumnDataType::Json
        || matches!(
            column
                .datatype_extension
                .as_ref()
                .and_then(|ext| ext.type_ext.as_ref()),
            Some(column_data_type_extension::TypeExt::JsonType(_))
                | Some(column_data_type_extension::TypeExt::JsonNativeType(_))
        )
}

fn arrow_type(column: &v1::Column, datatype: v1::ColumnDataType) -> Result<DataType> {
    Ok(match datatype {
        v1::ColumnDataType::Boolean => DataType::Boolean,
        v1::ColumnDataType::Int8 => DataType::Int8,
        v1::ColumnDataType::Int16 => DataType::Int16,
        v1::ColumnDataType::Int32 => DataType::Int32,
        v1::ColumnDataType::Int64 => DataType::Int64,
        v1::ColumnDataType::Uint8 => DataType::UInt8,
        v1::ColumnDataType::Uint16 => DataType::UInt16,
        v1::ColumnDataType::Uint32 => DataType::UInt32,
        v1::ColumnDataType::Uint64 => DataType::UInt64,
        v1::ColumnDataType::Float32 => DataType::Float32,
        v1::ColumnDataType::Float64 => DataType::Float64,
        v1::ColumnDataType::Binary | v1::ColumnDataType::Json => DataType::Binary,
        v1::ColumnDataType::String => DataType::Utf8,
        v1::ColumnDataType::Date => DataType::Date32,
        v1::ColumnDataType::Datetime => DataType::Date64,
        v1::ColumnDataType::TimestampSecond => DataType::Timestamp(TimeUnit::Second, None),
        v1::ColumnDataType::TimestampMillisecond => {
            DataType::Timestamp(TimeUnit::Millisecond, None)
        }
        v1::ColumnDataType::TimestampMicrosecond => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        v1::ColumnDataType::TimestampNanosecond => DataType::Timestamp(TimeUnit::Nanosecond, None),
        v1::ColumnDataType::Decimal128 => {
            let (precision, scale) = decimal_parts(column)?;
            DataType::Decimal128(precision, scale)
        }
        other => {
            return Err(Error::SchemaViolation(format!(
                "column datatype {other:?} has no Arrow mapping"
            )));
        }
    })
}

fn decimal_parts(column: &v1::Column) -> Result<(u8, i8)> {
    let Some(column_data_type_extension::TypeExt::DecimalType(extension)) = column
        .datatype_extension
        .as_ref()
        .and_then(|extension| extension.type_ext.as_ref())
    else {
        return Err(Error::SchemaViolation(
            "decimal column is missing precision and scale".into(),
        ));
    };
    let precision = u8::try_from(extension.precision).map_err(|_| {
        Error::SchemaViolation("decimal precision does not fit an Arrow decimal".into())
    })?;
    let scale = i8::try_from(extension.scale).map_err(|_| {
        Error::SchemaViolation("decimal scale does not fit an Arrow decimal".into())
    })?;
    if scale < 0 {
        return Err(Error::SchemaViolation(
            "decimal scale cannot be negative".into(),
        ));
    }
    Ok((precision, scale))
}

fn null_at(mask: &[u8], row: usize) -> bool {
    mask.get(row / 8)
        .is_some_and(|byte| byte & (1 << (row % 8)) != 0)
}

fn short_column() -> Error {
    Error::SchemaViolation("column values are shorter than the non-null row count".into())
}

fn long_column() -> Error {
    Error::SchemaViolation("column values are longer than the non-null row count".into())
}

fn narrow_error() -> Error {
    Error::SchemaViolation("column value does not fit its Arrow physical type".into())
}

macro_rules! fill_primitive {
    ($rows:expr, $mask:expr, $values:expr, $builder:expr, $convert:expr, $reject_null:expr) => {{
        let mut builder = $builder;
        let mut next = 0usize;
        for row in 0..$rows {
            if null_at($mask, row) {
                if $reject_null {
                    return Err(Error::SchemaViolation(
                        "timestamp columns cannot contain nulls".into(),
                    ));
                }
                builder.append_null();
            } else {
                let raw = $values.get(next).copied().ok_or_else(short_column)?;
                next += 1;
                builder.append_value($convert(raw)?);
            }
        }
        if next != $values.len() {
            return Err(long_column());
        }
        Arc::new(builder.finish()) as ArrayRef
    }};
}

fn identity<T>(value: T) -> Result<T> {
    Ok(value)
}

fn build_array(
    datatype: v1::ColumnDataType,
    arrow_type: &DataType,
    rows: usize,
    mask: &[u8],
    values: &v1::column::Values,
    reject_null: bool,
) -> Result<ArrayRef> {
    Ok(match datatype {
        v1::ColumnDataType::Boolean => fill_primitive!(
            rows,
            mask,
            &values.bool_values,
            BooleanBuilder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::Int8 => fill_primitive!(
            rows,
            mask,
            &values.i8_values,
            Int8Builder::with_capacity(rows),
            |value: i32| i8::try_from(value).map_err(|_| narrow_error()),
            reject_null
        ),
        v1::ColumnDataType::Int16 => fill_primitive!(
            rows,
            mask,
            &values.i16_values,
            Int16Builder::with_capacity(rows),
            |value: i32| i16::try_from(value).map_err(|_| narrow_error()),
            reject_null
        ),
        v1::ColumnDataType::Int32 => fill_primitive!(
            rows,
            mask,
            &values.i32_values,
            Int32Builder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::Int64 => fill_primitive!(
            rows,
            mask,
            &values.i64_values,
            Int64Builder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::Uint8 => fill_primitive!(
            rows,
            mask,
            &values.u8_values,
            UInt8Builder::with_capacity(rows),
            |value: u32| u8::try_from(value).map_err(|_| narrow_error()),
            reject_null
        ),
        v1::ColumnDataType::Uint16 => fill_primitive!(
            rows,
            mask,
            &values.u16_values,
            UInt16Builder::with_capacity(rows),
            |value: u32| u16::try_from(value).map_err(|_| narrow_error()),
            reject_null
        ),
        v1::ColumnDataType::Uint32 => fill_primitive!(
            rows,
            mask,
            &values.u32_values,
            UInt32Builder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::Uint64 => fill_primitive!(
            rows,
            mask,
            &values.u64_values,
            UInt64Builder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::Float32 => fill_primitive!(
            rows,
            mask,
            &values.f32_values,
            Float32Builder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::Float64 => fill_primitive!(
            rows,
            mask,
            &values.f64_values,
            Float64Builder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::Date => fill_primitive!(
            rows,
            mask,
            &values.date_values,
            Date32Builder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::Datetime => fill_primitive!(
            rows,
            mask,
            &values.datetime_values,
            Date64Builder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::TimestampSecond => fill_primitive!(
            rows,
            mask,
            &values.timestamp_second_values,
            TimestampSecondBuilder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::TimestampMillisecond => fill_primitive!(
            rows,
            mask,
            &values.timestamp_millisecond_values,
            TimestampMillisecondBuilder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::TimestampMicrosecond => fill_primitive!(
            rows,
            mask,
            &values.timestamp_microsecond_values,
            TimestampMicrosecondBuilder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::TimestampNanosecond => fill_primitive!(
            rows,
            mask,
            &values.timestamp_nanosecond_values,
            TimestampNanosecondBuilder::with_capacity(rows),
            identity,
            reject_null
        ),
        v1::ColumnDataType::String => fill_text(rows, mask, &values.string_values)?,
        v1::ColumnDataType::Binary | v1::ColumnDataType::Json => {
            fill_binary(rows, mask, &values.binary_values)?
        }
        v1::ColumnDataType::Decimal128 => {
            let DataType::Decimal128(precision, scale) = arrow_type else {
                return Err(Error::SchemaViolation(
                    "decimal Arrow type is inconsistent".into(),
                ));
            };
            let builder = Decimal128Builder::with_capacity(rows)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|error| Error::SchemaViolation(error.to_string()))?;
            fill_primitive!(
                rows,
                mask,
                &values.decimal128_values,
                builder,
                |value: v1::Decimal128| Ok::<i128, Error>(decimal_i128(value)),
                reject_null
            )
        }
        other => {
            return Err(Error::SchemaViolation(format!(
                "column datatype {other:?} has no Arrow mapping"
            )));
        }
    })
}

fn decimal_i128(value: v1::Decimal128) -> i128 {
    (i128::from(value.hi) << 64) | i128::from(value.lo as u64)
}

fn fill_text(rows: usize, mask: &[u8], values: &[String]) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(rows, 0);
    let mut next = 0usize;
    let mut total_bytes = 0usize;
    for row in 0..rows {
        if null_at(mask, row) {
            builder.append_null();
        } else {
            let value = values.get(next).ok_or_else(short_column)?;
            next += 1;
            total_bytes = total_bytes
                .checked_add(value.len())
                .filter(|total| *total <= i32::MAX as usize)
                .ok_or_else(|| {
                    Error::SchemaViolation(
                        "column byte payload exceeds 2 GiB Arrow offset limit".into(),
                    )
                })?;
            builder.append_value(value);
        }
    }
    if next != values.len() {
        return Err(long_column());
    }
    Ok(Arc::new(builder.finish()))
}

fn fill_binary(rows: usize, mask: &[u8], values: &[Vec<u8>]) -> Result<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(rows, 0);
    let mut next = 0usize;
    let mut total_bytes = 0usize;
    for row in 0..rows {
        if null_at(mask, row) {
            builder.append_null();
        } else {
            let value = values.get(next).ok_or_else(short_column)?;
            next += 1;
            total_bytes = total_bytes
                .checked_add(value.len())
                .filter(|total| *total <= i32::MAX as usize)
                .ok_or_else(|| {
                    Error::SchemaViolation(
                        "column byte payload exceeds 2 GiB Arrow offset limit".into(),
                    )
                })?;
            builder.append_value(value);
        }
    }
    if next != values.len() {
        return Err(long_column());
    }
    Ok(Arc::new(builder.finish()))
}

pub(super) fn timestamp_column(schema: &Schema) -> Result<(usize, TimeUnit)> {
    let mut found = None;
    for (index, field) in schema.fields().iter().enumerate() {
        if field.metadata().get(SEMANTIC_TYPE_KEY).map(String::as_str) != Some("timestamp") {
            continue;
        }
        if found.is_some() {
            return Err(Error::SchemaViolation(
                "a table must have exactly one timestamp column".into(),
            ));
        }
        let DataType::Timestamp(unit, timezone) = field.data_type() else {
            return Err(Error::SchemaViolation(
                "timestamp column must use an Arrow timestamp type".into(),
            ));
        };
        if timezone.is_some() {
            return Err(Error::SchemaViolation(
                "timestamp columns must not carry a timezone".into(),
            ));
        }
        found = Some((index, *unit));
    }
    found.ok_or_else(|| Error::SchemaViolation("batch is missing a timestamp column".into()))
}

pub(super) fn hour_ticks(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 3_600,
        TimeUnit::Millisecond => 3_600_000,
        TimeUnit::Microsecond => 3_600_000_000,
        TimeUnit::Nanosecond => 3_600_000_000_000,
    }
}

pub(super) fn timestamp_at(column: &ArrayRef, row: usize) -> Result<i64> {
    if column.is_null(row) {
        return Err(Error::SchemaViolation(
            "timestamp columns cannot contain nulls".into(),
        ));
    }
    Ok(match column.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            column.as_primitive::<TimestampSecondType>().value(row)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            column.as_primitive::<TimestampMillisecondType>().value(row)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            column.as_primitive::<TimestampMicrosecondType>().value(row)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            column.as_primitive::<TimestampNanosecondType>().value(row)
        }
        _ => {
            return Err(Error::SchemaViolation(
                "timestamp column must use an Arrow timestamp type".into(),
            ));
        }
    })
}

pub(super) fn take_rows(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    let mut arrays = Vec::with_capacity(batch.num_columns());
    for (index, field) in batch.schema().fields().iter().enumerate() {
        arrays.push(take_column(batch.column(index), field, indices)?);
    }
    RecordBatch::try_new(batch.schema(), arrays)
        .map_err(|error| Error::SchemaViolation(error.to_string()))
}

fn take_column(column: &ArrayRef, field: &Field, indices: &[usize]) -> Result<ArrayRef> {
    let rows = indices.len();
    let reject_null =
        field.metadata().get(SEMANTIC_TYPE_KEY).map(String::as_str) == Some("timestamp");
    match field.data_type() {
        DataType::Boolean => {
            let source = column.as_boolean();
            let mut builder = BooleanBuilder::with_capacity(rows);
            for index in indices {
                append_copied(
                    &mut builder,
                    source.is_null(*index),
                    source.value(*index),
                    reject_null,
                )?;
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => take_primitive::<Int8Type, _>(
            column,
            indices,
            reject_null,
            Int8Builder::with_capacity(rows),
        ),
        DataType::Int16 => take_primitive::<Int16Type, _>(
            column,
            indices,
            reject_null,
            Int16Builder::with_capacity(rows),
        ),
        DataType::Int32 => take_primitive::<Int32Type, _>(
            column,
            indices,
            reject_null,
            Int32Builder::with_capacity(rows),
        ),
        DataType::Int64 => take_primitive::<Int64Type, _>(
            column,
            indices,
            reject_null,
            Int64Builder::with_capacity(rows),
        ),
        DataType::UInt8 => take_primitive::<UInt8Type, _>(
            column,
            indices,
            reject_null,
            UInt8Builder::with_capacity(rows),
        ),
        DataType::UInt16 => take_primitive::<UInt16Type, _>(
            column,
            indices,
            reject_null,
            UInt16Builder::with_capacity(rows),
        ),
        DataType::UInt32 => take_primitive::<UInt32Type, _>(
            column,
            indices,
            reject_null,
            UInt32Builder::with_capacity(rows),
        ),
        DataType::UInt64 => take_primitive::<UInt64Type, _>(
            column,
            indices,
            reject_null,
            UInt64Builder::with_capacity(rows),
        ),
        DataType::Float32 => take_primitive::<Float32Type, _>(
            column,
            indices,
            reject_null,
            Float32Builder::with_capacity(rows),
        ),
        DataType::Float64 => take_primitive::<Float64Type, _>(
            column,
            indices,
            reject_null,
            Float64Builder::with_capacity(rows),
        ),
        DataType::Date32 => take_primitive::<arrow_array::types::Date32Type, _>(
            column,
            indices,
            reject_null,
            Date32Builder::with_capacity(rows),
        ),
        DataType::Date64 => take_primitive::<arrow_array::types::Date64Type, _>(
            column,
            indices,
            reject_null,
            Date64Builder::with_capacity(rows),
        ),
        DataType::Timestamp(TimeUnit::Second, None) => take_primitive::<TimestampSecondType, _>(
            column,
            indices,
            reject_null,
            TimestampSecondBuilder::with_capacity(rows),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            take_primitive::<TimestampMillisecondType, _>(
                column,
                indices,
                reject_null,
                TimestampMillisecondBuilder::with_capacity(rows),
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            take_primitive::<TimestampMicrosecondType, _>(
                column,
                indices,
                reject_null,
                TimestampMicrosecondBuilder::with_capacity(rows),
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            take_primitive::<TimestampNanosecondType, _>(
                column,
                indices,
                reject_null,
                TimestampNanosecondBuilder::with_capacity(rows),
            )
        }
        DataType::Utf8 => {
            let source = column.as_string::<i32>();
            let mut builder = StringBuilder::with_capacity(rows, 0);
            for index in indices {
                if source.is_null(*index) {
                    builder.append_null();
                } else {
                    builder.append_value(source.value(*index));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let source = column.as_binary::<i32>();
            let mut builder = BinaryBuilder::with_capacity(rows, 0);
            for index in indices {
                if source.is_null(*index) {
                    builder.append_null();
                } else {
                    builder.append_value(source.value(*index));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let source = column.as_primitive::<Decimal128Type>();
            let mut builder = Decimal128Builder::with_capacity(rows)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|error| Error::SchemaViolation(error.to_string()))?;
            for index in indices {
                append_copied(
                    &mut builder,
                    source.is_null(*index),
                    source.value(*index),
                    reject_null,
                )?;
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(Error::SchemaViolation(format!(
            "cannot partition Arrow type {other}"
        ))),
    }
}

fn take_primitive<T, B>(
    column: &ArrayRef,
    indices: &[usize],
    reject_null: bool,
    mut builder: B,
) -> Result<ArrayRef>
where
    T: arrow_array::types::ArrowPrimitiveType,
    B: PrimitiveAppend<T::Native>,
{
    let source = column.as_primitive::<T>();
    for index in indices {
        append_copied(
            &mut builder,
            source.is_null(*index),
            source.value(*index),
            reject_null,
        )?;
    }
    Ok(builder.finish_array())
}

fn append_copied<B, T>(builder: &mut B, is_null: bool, value: T, reject_null: bool) -> Result<()>
where
    B: PrimitiveAppend<T>,
{
    if is_null {
        if reject_null {
            return Err(Error::SchemaViolation(
                "timestamp columns cannot contain nulls".into(),
            ));
        }
        builder.append_null_value();
    } else {
        builder.append_copied(value);
    }
    Ok(())
}

trait PrimitiveAppend<T> {
    fn append_copied(&mut self, value: T);
    fn append_null_value(&mut self);
    fn finish_array(self) -> ArrayRef;
}

macro_rules! primitive_append {
    ($builder:ty, $native:ty) => {
        impl PrimitiveAppend<$native> for $builder {
            fn append_copied(&mut self, value: $native) {
                self.append_value(value);
            }
            fn append_null_value(&mut self) {
                self.append_null();
            }
            fn finish_array(mut self) -> ArrayRef {
                Arc::new(self.finish())
            }
        }
    };
}

primitive_append!(BooleanBuilder, bool);
primitive_append!(Int8Builder, i8);
primitive_append!(Int16Builder, i16);
primitive_append!(Int32Builder, i32);
primitive_append!(Int64Builder, i64);
primitive_append!(UInt8Builder, u8);
primitive_append!(UInt16Builder, u16);
primitive_append!(UInt32Builder, u32);
primitive_append!(UInt64Builder, u64);
primitive_append!(Float32Builder, f32);
primitive_append!(Float64Builder, f64);
primitive_append!(Date32Builder, i32);
primitive_append!(Date64Builder, i64);
primitive_append!(TimestampSecondBuilder, i64);
primitive_append!(TimestampMillisecondBuilder, i64);
primitive_append!(TimestampMicrosecondBuilder, i64);
primitive_append!(TimestampNanosecondBuilder, i64);
primitive_append!(Decimal128Builder, i128);
