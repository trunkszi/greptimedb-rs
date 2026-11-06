//! Row gathering for time-window partitions.
use super::*;

pub(crate) fn take_rows(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    let mut arrays = Vec::with_capacity(batch.num_columns());
    for (index, field) in batch.schema().fields().iter().enumerate() {
        arrays.push(take_column(batch.column(index), field, indices)?);
    }
    RecordBatch::try_new(batch.schema(), arrays)
        .map_err(|error| Error::schema_with(|| error.to_string()))
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
                .map_err(|error| Error::schema_with(|| error.to_string()))?;
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
        other => Err(Error::schema_with(|| {
            format!("cannot partition Arrow type {other}")
        })),
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

#[hot_inline]
fn append_copied<B, T>(builder: &mut B, is_null: bool, value: T, reject_null: bool) -> Result<()>
where
    B: PrimitiveAppend<T>,
{
    if is_null {
        if reject_null {
            return Err(Error::schema_with(|| {
                "timestamp columns cannot contain nulls".into()
            }));
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
