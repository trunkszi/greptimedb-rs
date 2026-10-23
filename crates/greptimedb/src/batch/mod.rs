//! Thread-affine columnar and row-oriented ingestion batches.
use crate::buffer_policy::{BufferPolicy, MemoryBuffer};
use crate::error::{Error, Result};
use crate::types::{ColumnDataType, SemanticType, ValueRef};
use greptimedb_proto as proto;
use std::{marker::PhantomData, rc::Rc};
mod columnar;
pub use columnar::ColumnarBatch;
pub use rows::RowBatch;

/// A single-table source of columnar insert and key-only delete requests.
pub trait BatchEncode {
    /// Return the default destination table.
    fn table_name(&self) -> &str;
    /// Return the number of buffered rows.
    fn row_count(&self) -> usize;
    /// Whether the batch contains no rows.
    fn is_empty(&self) -> bool {
        self.row_count() == 0
    }
    /// Validate column roles, lengths, and the non-null time index.
    ///
    /// # Errors
    /// Returns an error for an invalid schema or inconsistent column lengths.
    fn validate_schema(&self) -> Result<()>;
    /// Encode an insert for the default destination.
    ///
    /// # Errors
    /// Returns an error if the schema or wire row count is invalid.
    fn encode_insert_request(&self) -> Result<proto::v1::InsertRequest> {
        self.encode_insert_request_for(self.table_name())
    }
    /// Encode an insert for an explicit destination.
    ///
    /// # Errors
    /// Returns an error if the schema, destination, or wire row count is invalid.
    fn encode_insert_request_for(&self, table_name: &str) -> Result<proto::v1::InsertRequest>;
    /// Encode a delete using the default destination's tag and timestamp keys.
    ///
    /// # Errors
    /// Returns an error if the schema or wire row count is invalid.
    fn encode_delete_request(&self) -> Result<proto::v1::DeleteRequest> {
        self.encode_delete_request_for(self.table_name())
    }
    /// Encode a key-only delete for an explicit destination.
    ///
    /// # Errors
    /// Returns an error if the schema, destination, or wire row count is invalid.
    fn encode_delete_request_for(&self, table_name: &str) -> Result<proto::v1::DeleteRequest>;
}

fn is_timestamp(data_type: ColumnDataType) -> bool {
    matches!(
        data_type,
        ColumnDataType::TimestampSecond
            | ColumnDataType::TimestampMillisecond
            | ColumnDataType::TimestampMicrosecond
            | ColumnDataType::TimestampNanosecond
    )
}

fn schema_error(message: impl Into<String>) -> Error {
    Error::SchemaViolation(message.into())
}

pub(crate) fn validate_data_type(data_type: ColumnDataType) -> Result<()> {
    if let ColumnDataType::Decimal128 { precision, scale } = data_type
        && (!(1..=38).contains(&precision) || scale > precision)
    {
        return Err(schema_error(
            "decimal precision must be 1..=38 and scale must not exceed precision",
        ));
    }
    Ok(())
}

pub(crate) fn validate_value(
    semantic_type: SemanticType,
    data_type: ColumnDataType,
    value: &ValueRef<'_>,
) -> Result<()> {
    validate_data_type(data_type)?;
    if matches!(value, ValueRef::Null) {
        return if semantic_type == SemanticType::Timestamp {
            Err(schema_error("timestamp columns cannot contain nulls"))
        } else {
            Ok(())
        };
    }
    let matches = match (data_type, value) {
        (ColumnDataType::Boolean, ValueRef::Boolean(_))
        | (ColumnDataType::Int8, ValueRef::Int8(_))
        | (ColumnDataType::Int16, ValueRef::Int16(_))
        | (ColumnDataType::Int32, ValueRef::Int32(_))
        | (ColumnDataType::Int64, ValueRef::Int64(_))
        | (ColumnDataType::Uint8, ValueRef::Uint8(_))
        | (ColumnDataType::Uint16, ValueRef::Uint16(_))
        | (ColumnDataType::Uint32, ValueRef::Uint32(_))
        | (ColumnDataType::Uint64, ValueRef::Uint64(_))
        | (ColumnDataType::Float32, ValueRef::Float32(_))
        | (ColumnDataType::Float64, ValueRef::Float64(_))
        | (ColumnDataType::String, ValueRef::String(_))
        | (ColumnDataType::Binary, ValueRef::Binary(_))
        | (ColumnDataType::Json, ValueRef::Json(_))
        | (ColumnDataType::Date, ValueRef::Date(_))
        | (ColumnDataType::DateTime, ValueRef::DateTime(_))
        | (ColumnDataType::TimestampSecond, ValueRef::TimestampSecond(_))
        | (ColumnDataType::TimestampMillisecond, ValueRef::TimestampMillisecond(_))
        | (ColumnDataType::TimestampMicrosecond, ValueRef::TimestampMicrosecond(_))
        | (ColumnDataType::TimestampNanosecond, ValueRef::TimestampNanosecond(_)) => true,
        (ColumnDataType::Decimal128 { precision, scale }, ValueRef::Decimal128(value)) => {
            value.precision == precision
                && value.scale == scale
                && value.value.unsigned_abs() < 10u128.pow(precision)
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(schema_error(
            "value does not match the declared column datatype",
        ))
    }
}

fn validate_table_name(table_name: &str) -> Result<()> {
    if table_name.trim().is_empty() {
        return Err(schema_error("table name must not be empty"));
    }
    Ok(())
}

/// Owned request snapshots for inserting several tables in one RPC.
#[derive(Debug, Default)]
pub struct MultiTableBatch {
    inserts: Vec<proto::v1::InsertRequest>,
    thread_affinity: PhantomData<Rc<()>>,
}

impl MultiTableBatch {
    /// Create an empty request group.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot a batch using its default destination.
    ///
    /// # Errors
    /// Returns the batch encoding error without changing this group.
    pub fn add_batch(&mut self, batch: &impl BatchEncode) -> Result<()> {
        let request = batch.encode_insert_request()?;
        self.push_request(request)
    }

    /// Snapshot a batch using an explicit destination.
    ///
    /// # Errors
    /// Returns the batch encoding error without changing this group.
    pub fn add_batch_to(&mut self, table_name: &str, batch: &impl BatchEncode) -> Result<()> {
        let request = batch.encode_insert_request_for(table_name)?;
        self.push_request(request)
    }

    fn push_request(&mut self, request: proto::v1::InsertRequest) -> Result<()> {
        self.total_rows()
            .checked_add(request.row_count as usize)
            .ok_or_else(|| schema_error("multi-table row count exceeds usize"))?;
        self.inserts.push(request);
        Ok(())
    }

    /// Return the sum of rows in all request snapshots.
    pub fn total_rows(&self) -> usize {
        self.inserts
            .iter()
            .map(|request| request.row_count as usize)
            .sum()
    }

    /// Return the number of table request snapshots, including repeated tables.
    pub fn table_count(&self) -> usize {
        self.inserts.len()
    }

    /// Consume the group into the wire request container.
    pub fn into_insert_requests(self) -> proto::v1::InsertRequests {
        proto::v1::InsertRequests {
            inserts: self.inserts,
        }
    }

    /// Clone the request snapshots into the wire request container.
    pub fn to_insert_requests(&self) -> proto::v1::InsertRequests {
        proto::v1::InsertRequests {
            inserts: self.inserts.clone(),
        }
    }

    /// Discard snapshots while retaining the request-list allocation.
    pub fn clear(&mut self) {
        self.inserts.clear();
    }
}

mod rows {

    use std::{marker::PhantomData, mem::size_of, rc::Rc};

    use greptimedb_proto::v1::{self as proto, value::ValueData};

    use crate::{
        batch::{validate_data_type, validate_value},
        buffer_policy::{BufferPolicy, MemoryBuffer},
        error::{Error, Result},
        types::{ColumnDataType, SemanticType, ValueRef},
    };

    /// Thread-affine rows sharing one ordered GreptimeDB schema.
    ///
    /// Appending validates the complete row before modifying any value. Clearing
    /// retains row slots and their nested payload allocations within the selected
    /// buffer policy. Encoding produces an independent owned protobuf request.
    #[derive(Debug)]
    pub struct RowBatch {
        table_name: String,
        schema: Vec<(String, SemanticType, ColumnDataType)>,
        rows: Vec<proto::Row>,
        row_count: usize,
        thread_affine: PhantomData<Rc<()>>,
    }

    impl RowBatch {
        /// Creates an empty row batch for the supplied table.
        pub fn new(table_name: impl Into<String>) -> Self {
            Self::with_capacity(table_name, 0)
        }

        /// Reserves row slots; schemas and variable-size payloads grow as needed.
        pub fn with_capacity(table_name: impl Into<String>, row_capacity: usize) -> Self {
            Self {
                table_name: table_name.into(),
                schema: Vec::new(),
                rows: Vec::with_capacity(row_capacity),
                row_count: 0,
                thread_affine: PhantomData,
            }
        }

        /// Adds a column and returns its ordered row position.
        ///
        /// # Errors
        /// Rejects empty or repeated names, invalid decimal metadata, a second time
        /// index, non-timestamp time-index types, or schema changes while rows exist.
        pub fn add_column_schema(
            &mut self,
            name: impl Into<String>,
            semantic_type: SemanticType,
            data_type: ColumnDataType,
        ) -> Result<usize> {
            let name = name.into();
            if self.row_count != 0 {
                return Err(schema_error("cannot change a populated row schema"));
            }
            if name.trim().is_empty() || self.schema.iter().any(|column| column.0 == name) {
                return Err(schema_error("column names must be nonempty and unique"));
            }
            validate_data_type(data_type)?;
            if semantic_type == SemanticType::Timestamp {
                if !is_timestamp(data_type) {
                    return Err(schema_error("the time index requires a timestamp datatype"));
                }
                if self
                    .schema
                    .iter()
                    .any(|column| column.1 == SemanticType::Timestamp)
                {
                    return Err(schema_error("a row schema requires exactly one time index"));
                }
            }
            let index = self.schema.len();
            self.schema.push((name, semantic_type, data_type));
            Ok(index)
        }

        /// Appends one complete row after checking every value against its schema.
        ///
        /// # Errors
        /// Rejects invalid schemas, unequal row width, null timestamps, mismatched
        /// scalar types, or decimal metadata differing from the declared column.
        /// Every error leaves all stored rows unchanged.
        pub fn push_row<'a>(&mut self, values: &[ValueRef<'a>]) -> Result<()> {
            self.timestamp_index()?;
            if values.len() != self.schema.len() {
                return Err(schema_error("row width must equal the schema width"));
            }
            for ((_, semantic, datatype), value) in self.schema.iter().zip(values) {
                validate_value(*semantic, *datatype, value)?;
            }
            if self.row_count == self.rows.len() {
                self.rows.push(proto::Row {
                    values: Vec::with_capacity(values.len()),
                });
            }
            let row = &mut self.rows[self.row_count];
            row.values.resize_with(values.len(), proto::Value::default);
            for (slot, value) in row.values.iter_mut().zip(values) {
                assign_value(slot, value);
            }
            self.row_count += 1;
            Ok(())
        }

        /// Returns the number of currently buffered rows.
        pub fn row_count(&self) -> usize {
            self.row_count
        }

        /// Returns the default destination table.
        pub fn table_name(&self) -> &str {
            &self.table_name
        }

        /// Checks the time-index and row-width invariants.
        ///
        /// # Errors
        /// Rejects missing or repeated time indexes, non-timestamp index types,
        /// unequal row widths, or null time-index values.
        pub fn validate_schema(&self) -> Result<()> {
            let timestamp = self.timestamp_index()?;
            for row in &self.rows[..self.row_count] {
                if row.values.len() != self.schema.len() {
                    return Err(schema_error("row width must equal the schema width"));
                }
                if row.values[timestamp].value_data.is_none() {
                    return Err(schema_error("timestamp values cannot be null"));
                }
            }
            Ok(())
        }

        /// Encodes the current rows for the default destination.
        ///
        /// # Errors
        /// Rejects an empty table name or an invalid row schema.
        pub fn encode_row_insert_request(&self) -> Result<proto::RowInsertRequest> {
            self.encode_row_insert_request_for(&self.table_name)
        }

        /// Encodes the current rows for a supplied destination without changing it.
        ///
        /// # Errors
        /// Rejects an empty table name or an invalid row schema.
        pub fn encode_row_insert_request_for(
            &self,
            table_name: &str,
        ) -> Result<proto::RowInsertRequest> {
            self.validate_destination(table_name)?;
            Ok(proto::RowInsertRequest {
                table_name: table_name.to_owned(),
                rows: Some(self.encode_rows(false)),
            })
        }

        /// Encodes only tag and timestamp columns for the default destination.
        ///
        /// # Errors
        /// Rejects an empty table name or an invalid row schema.
        pub fn encode_row_delete_request(&self) -> Result<proto::RowDeleteRequest> {
            self.encode_row_delete_request_for(&self.table_name)
        }

        /// Encodes tag and timestamp keys for a supplied destination.
        ///
        /// # Errors
        /// Rejects an empty table name or an invalid row schema.
        pub fn encode_row_delete_request_for(
            &self,
            table_name: &str,
        ) -> Result<proto::RowDeleteRequest> {
            self.validate_destination(table_name)?;
            Ok(proto::RowDeleteRequest {
                table_name: table_name.to_owned(),
                rows: Some(self.encode_rows(true)),
            })
        }

        fn timestamp_index(&self) -> Result<usize> {
            let mut timestamp = None;
            for (index, (_, semantic, datatype)) in self.schema.iter().enumerate() {
                if *semantic == SemanticType::Timestamp {
                    if timestamp.is_some() || !is_timestamp(*datatype) {
                        return Err(schema_error(
                            "a row schema requires exactly one timestamp time index",
                        ));
                    }
                    timestamp = Some(index);
                }
            }
            timestamp.ok_or_else(|| schema_error("a row schema requires a time index"))
        }

        fn validate_destination(&self, table_name: &str) -> Result<()> {
            if table_name.trim().is_empty() {
                return Err(schema_error("table name must be nonempty"));
            }
            self.validate_schema()
        }

        fn encode_rows(&self, keys_only: bool) -> proto::Rows {
            let selected = |semantic: SemanticType| !keys_only || semantic != SemanticType::Field;
            let schema = self
                .schema
                .iter()
                .filter(|column| selected(column.1))
                .map(|(name, semantic, datatype)| proto::ColumnSchema {
                    column_name: name.clone(),
                    datatype: datatype.to_proto_datatype(),
                    semantic_type: semantic.to_proto(),
                    datatype_extension: datatype.to_proto_extension(),
                    ..Default::default()
                })
                .collect();
            let rows = self.rows[..self.row_count]
                .iter()
                .map(|row| proto::Row {
                    values: row
                        .values
                        .iter()
                        .zip(&self.schema)
                        .filter(|(_, column)| selected(column.1))
                        .map(|(value, _)| value.clone())
                        .collect(),
                })
                .collect();
            proto::Rows { schema, rows }
        }
    }

    impl MemoryBuffer for RowBatch {
        fn allocated_bytes(&self) -> usize {
            self.rows.capacity() * size_of::<proto::Row>()
                + self
                    .rows
                    .iter()
                    .map(|row| {
                        row.values.capacity() * size_of::<proto::Value>()
                            + row
                                .values
                                .iter()
                                .map(|value| match &value.value_data {
                                    Some(ValueData::StringValue(value)) => value.capacity(),
                                    Some(ValueData::BinaryValue(value)) => value.capacity(),
                                    _ => 0,
                                })
                                .sum::<usize>()
                    })
                    .sum::<usize>()
        }

        fn row_capacity(&self) -> usize {
            self.rows.capacity()
        }

        fn clear_with_policy(&mut self, policy: BufferPolicy) {
            self.row_count = 0;
            if policy.should_shrink(self.row_capacity(), self.allocated_bytes()) {
                self.rows = Vec::new();
            }
        }

        fn shrink_to_fit(&mut self) {
            self.rows.truncate(self.row_count);
            for row in &mut self.rows {
                for value in &mut row.values {
                    match &mut value.value_data {
                        Some(ValueData::StringValue(value)) => value.shrink_to_fit(),
                        Some(ValueData::BinaryValue(value)) => value.shrink_to_fit(),
                        _ => {}
                    }
                }
                row.values.shrink_to_fit();
            }
            self.rows.shrink_to_fit();
        }
    }

    fn schema_error(message: impl Into<String>) -> Error {
        Error::SchemaViolation(message.into())
    }

    fn is_timestamp(datatype: ColumnDataType) -> bool {
        matches!(
            datatype,
            ColumnDataType::TimestampSecond
                | ColumnDataType::TimestampMillisecond
                | ColumnDataType::TimestampMicrosecond
                | ColumnDataType::TimestampNanosecond
        )
    }

    fn assign_value(slot: &mut proto::Value, value: &ValueRef<'_>) {
        match (&mut slot.value_data, value) {
            (Some(ValueData::StringValue(target)), ValueRef::String(value)) => {
                target.clear();
                target.push_str(value);
            }
            (Some(ValueData::BinaryValue(target)), ValueRef::Binary(value)) => {
                target.clear();
                target.extend_from_slice(value);
            }
            (Some(ValueData::BinaryValue(target)), ValueRef::Json(value)) => {
                target.clear();
                target.extend_from_slice(value.as_bytes());
            }
            _ => *slot = value.to_proto_value(),
        }
    }
}
