//! Reusable structure-of-arrays storage and columnar wire encoding.
use super::*;
use crate::column_buf::TypedColumnBuf;
use crate::types::TimestampUnit;

/// Reusable structure-of-arrays rows for one table.
#[derive(Debug)]
pub struct ColumnarBatch {
    table_name: String,
    columns: Vec<TypedColumnBuf>,
    policy: BufferPolicy,
    initial_row_capacity: usize,
    thread_affinity: PhantomData<Rc<()>>,
}

impl ColumnarBatch {
    /// Create an empty schema with default retention limits.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self::with_capacity(table_name, 0)
    }

    /// Reserve this many rows when each column is defined.
    pub fn with_capacity(table_name: impl Into<String>, row_capacity: usize) -> Self {
        Self::with_policy(table_name, row_capacity, BufferPolicy::default())
    }

    /// Create a schema with an initial row capacity and clear-time limits.
    pub fn with_policy(
        table_name: impl Into<String>,
        row_capacity: usize,
        policy: BufferPolicy,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
            policy,
            initial_row_capacity: row_capacity,
            thread_affinity: PhantomData,
        }
    }

    /// Append a column definition and return its stable index.
    ///
    /// # Errors
    /// Rejects empty or duplicate names, schema changes with buffered rows,
    /// duplicate time indexes, and non-timestamp types used as time indexes.
    pub fn add_column(
        &mut self,
        name: impl Into<String>,
        semantic_type: SemanticType,
        data_type: ColumnDataType,
    ) -> Result<usize> {
        let name = name.into();
        validate_data_type(data_type)?;
        if name.trim().is_empty() || self.columns.iter().any(|column| column.name() == name) {
            return Err(schema_error("column names must be nonempty and unique"));
        }
        if self.columns.iter().any(|column| column.row_count() != 0) {
            return Err(schema_error("cannot change schema while rows are buffered"));
        }
        if semantic_type == SemanticType::Timestamp {
            if !is_timestamp(data_type) {
                return Err(schema_error("the time index requires a timestamp datatype"));
            }
            if self
                .columns
                .iter()
                .any(|column| column.semantic_type() == SemanticType::Timestamp)
            {
                return Err(schema_error(
                    "a table must have exactly one timestamp column",
                ));
            }
        }
        let index = self.columns.len();
        self.columns.push(TypedColumnBuf::with_capacity(
            name,
            semantic_type,
            data_type,
            self.initial_row_capacity,
        ));
        Ok(index)
    }

    /// Define a tag column.
    ///
    /// # Errors
    /// Returns the schema-definition errors described by [`Self::add_column`].
    pub fn add_tag_column(
        &mut self,
        name: impl Into<String>,
        data_type: ColumnDataType,
    ) -> Result<usize> {
        self.add_column(name, SemanticType::Tag, data_type)
    }

    /// Define the table's non-null time index.
    ///
    /// # Errors
    /// Returns the schema-definition errors described by [`Self::add_column`].
    pub fn add_timestamp_column(
        &mut self,
        name: impl Into<String>,
        unit: TimestampUnit,
    ) -> Result<usize> {
        self.add_column(name, SemanticType::Timestamp, unit.to_column_data_type())
    }

    /// Define a measured field column.
    ///
    /// # Errors
    /// Returns the schema-definition errors described by [`Self::add_column`].
    pub fn add_field_column(
        &mut self,
        name: impl Into<String>,
        data_type: ColumnDataType,
    ) -> Result<usize> {
        self.add_column(name, SemanticType::Field, data_type)
    }

    /// Look up a column by index.
    pub fn column(&self, index: usize) -> Option<&TypedColumnBuf> {
        self.columns.get(index)
    }

    /// Borrow a column for direct appends. Encoding checks equal row counts.
    pub fn column_mut(&mut self, index: usize) -> Option<&mut TypedColumnBuf> {
        self.columns.get_mut(index)
    }

    /// Look up a column by its schema name.
    pub fn column_by_name(&self, name: &str) -> Option<&TypedColumnBuf> {
        self.columns.iter().find(|column| column.name() == name)
    }

    /// Borrow a named column for direct appends.
    pub fn column_by_name_mut(&mut self, name: &str) -> Option<&mut TypedColumnBuf> {
        self.columns.iter_mut().find(|column| column.name() == name)
    }

    /// Return columns in schema order.
    pub fn columns(&self) -> &[TypedColumnBuf] {
        &self.columns
    }

    /// Append a row after validating every value without changing the buffers.
    ///
    /// # Errors
    /// Rejects invalid schemas, ragged columns, a wrong row width, type
    /// mismatches, and null time indexes. Rejected rows leave all columns intact.
    pub fn push_row<'a>(&mut self, values: &[ValueRef<'a>]) -> Result<()> {
        self.validate_schema()?;
        self.append_validated_row(values)
    }

    /// Append a query-result row, allowing projections without a time index.
    ///
    /// # Errors
    /// Rejects invalid result shapes, wrong row widths, type mismatches, and
    /// null time indexes. Rejected rows leave all columns intact.
    pub fn push_result_row(&mut self, values: &[ValueRef<'_>]) -> Result<()> {
        self.validate_result_shape()?;
        self.append_validated_row(values)
    }

    fn append_validated_row(&mut self, values: &[ValueRef<'_>]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(schema_error("row width does not match the column count"));
        }
        if self.row_count() >= u32::MAX as usize {
            return Err(schema_error("row count exceeds the protobuf u32 limit"));
        }
        for (column, value) in self.columns.iter().zip(values) {
            validate_value(column.semantic_type(), column.data_type(), value)?;
        }
        for (column, value) in self.columns.iter_mut().zip(values) {
            column
                .push(value.clone())
                .map_err(|error| schema_error(error.to_string()))?;
        }
        Ok(())
    }

    /// Validate a query-result schema and its buffered column lengths.
    ///
    /// Query projections may omit the time index. Any time index present must
    /// be the only one, have a timestamp datatype, and contain no nulls.
    ///
    /// # Errors
    /// Rejects empty or duplicate column names, invalid decimal metadata,
    /// ragged columns, invalid time indexes, and row counts exceeding u32.
    pub fn validate_result_shape(&self) -> Result<()> {
        let mut timestamp_count = 0;
        for (index, column) in self.columns.iter().enumerate() {
            validate_data_type(column.data_type())?;
            if column.name().trim().is_empty()
                || self.columns[..index]
                    .iter()
                    .any(|other| other.name() == column.name())
            {
                return Err(schema_error("column names must be nonempty and unique"));
            }
            if column.row_count() != self.row_count() {
                return Err(schema_error("all columns must have identical row counts"));
            }
            if column.semantic_type() == SemanticType::Timestamp {
                timestamp_count += 1;
                if !is_timestamp(column.data_type()) || column.null_mask().has_nulls() {
                    return Err(schema_error(
                        "the time index must contain non-null timestamps",
                    ));
                }
            }
        }
        if timestamp_count > 1 {
            return Err(schema_error(
                "a result may have at most one timestamp column",
            ));
        }
        u32::try_from(self.row_count())
            .map_err(|_| schema_error("row count exceeds the protobuf u32 limit"))?;
        Ok(())
    }
}

impl BatchEncode for ColumnarBatch {
    fn table_name(&self) -> &str {
        &self.table_name
    }
    fn row_count(&self) -> usize {
        self.columns.first().map_or(0, TypedColumnBuf::row_count)
    }
    fn validate_schema(&self) -> Result<()> {
        self.validate_result_shape()?;
        if !self
            .columns
            .iter()
            .any(|column| column.semantic_type() == SemanticType::Timestamp)
        {
            return Err(schema_error(
                "a table must have exactly one timestamp column",
            ));
        }
        Ok(())
    }
    fn encode_insert_request_for(&self, table_name: &str) -> Result<proto::v1::InsertRequest> {
        self.validate_schema()?;
        validate_table_name(table_name)?;
        let row_count = u32::try_from(self.row_count())
            .map_err(|_| schema_error("row count exceeds the protobuf u32 limit"))?;
        Ok(proto::v1::InsertRequest {
            table_name: table_name.to_owned(),
            columns: self
                .columns
                .iter()
                .map(encode_column)
                .collect::<Result<Vec<_>>>()?,
            row_count,
        })
    }
    fn encode_delete_request_for(&self, table_name: &str) -> Result<proto::v1::DeleteRequest> {
        self.validate_schema()?;
        validate_table_name(table_name)?;
        let row_count = u32::try_from(self.row_count())
            .map_err(|_| schema_error("row count exceeds the protobuf u32 limit"))?;
        let mut key_columns = self
            .columns
            .iter()
            .map(encode_column)
            .collect::<Result<Vec<_>>>()?;
        key_columns.retain(|column| column.semantic_type != SemanticType::Field.to_proto());
        Ok(proto::v1::DeleteRequest {
            table_name: table_name.to_owned(),
            key_columns,
            row_count,
        })
    }
}

impl MemoryBuffer for ColumnarBatch {
    fn allocated_bytes(&self) -> usize {
        self.columns.iter().fold(0usize, |bytes, column| {
            bytes.saturating_add(column.allocated_bytes())
        })
    }
    fn row_capacity(&self) -> usize {
        self.columns
            .iter()
            .map(MemoryBuffer::row_capacity)
            .max()
            .unwrap_or(0)
    }
    fn clear(&mut self) {
        self.clear_with_policy(self.policy);
    }
    fn clear_with_policy(&mut self, policy: BufferPolicy) {
        let mut remaining = policy.max_retained_bytes;
        for column in &mut self.columns {
            column.clear_with_policy(BufferPolicy::new(policy.max_retained_rows, remaining));
            remaining = remaining.saturating_sub(column.allocated_bytes());
        }
    }
    fn shrink_to_fit(&mut self) {
        for column in &mut self.columns {
            column.shrink_to_fit();
        }
    }
}

fn encode_column(column: &TypedColumnBuf) -> Result<proto::v1::Column> {
    let encoded = column.to_proto_column();
    if let ColumnDataType::Decimal128 { precision, .. } = column.data_type()
        && let Some(values) = &encoded.values
        && values.decimal128_values.iter().any(|value| {
            let coefficient = (i128::from(value.hi) << 64) | i128::from(value.lo as u64);
            coefficient.unsigned_abs() >= 10u128.pow(precision)
        })
    {
        return Err(schema_error(
            "decimal coefficient exceeds declared precision",
        ));
    }
    Ok(encoded)
}
