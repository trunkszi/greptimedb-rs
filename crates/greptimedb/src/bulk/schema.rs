//! GreptimeDB Arrow schema mapping and Flight IPC encoding.
//!
//! Timestamp columns are non-null. Tag and field columns are nullable. JSON columns
//! use Arrow `Binary` plus `greptime:type = Json`. IPC headers are raw FlatBuffers
//! messages, not length-prefixed stream frames.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow_schema::{DataType, Schema};

use crate::batch::BatchEncode;
use crate::error::{Error, Result};

use super::conversion::{
    hour_ticks, record_batch_from_columns, take_rows, timestamp_at, timestamp_column,
};

/// Arrow field metadata naming the GreptimeDB column role.
pub const SEMANTIC_TYPE_KEY: &str = "greptime:semantic_type";
/// Arrow field metadata marking a binary column as JSON.
pub const TYPE_EXTENSION_KEY: &str = "greptime:type";
/// Metadata value for a JSON column stored as Arrow `Binary`.
pub const JSON_TYPE_VALUE: &str = "Json";

/// IPC body compression selected for one bulk stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FlightCompression {
    /// Uncompressed Arrow buffers.
    #[default]
    None,
    /// LZ4 frame compression.
    Lz4Frame,
    /// Zstandard compression.
    Zstd,
}

/// Arrow IPC header and body for one Flight data frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedFlightBody {
    /// Raw FlatBuffers `Message` bytes.
    pub data_header: Vec<u8>,
    /// Arrow body buffers, including tail padding.
    pub data_body: Vec<u8>,
}

/// Encoder for the schema frame and the record batches that follow it.
///
/// One encoder belongs to one Flight stream. Dictionary columns are rejected
/// because GreptimeDB bulk ingestion does not accept dictionary batches.
pub struct FlightIpcEncoder {
    generator: IpcDataGenerator,
    tracker: DictionaryTracker,
    options: IpcWriteOptions,
    context: IpcWriteContext,
}

impl FlightIpcEncoder {
    /// Build an encoder for the selected IPC compression.
    ///
    /// # Errors
    /// Returns an encoding error when the compression setting is rejected by
    /// the IPC writer.
    pub fn new(compression: FlightCompression) -> Result<Self> {
        let kind = match compression {
            FlightCompression::None => None,
            FlightCompression::Lz4Frame => Some(arrow_ipc::CompressionType::LZ4_FRAME),
            FlightCompression::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
        };
        let options = IpcWriteOptions::default()
            .try_with_compression(kind)
            .map_err(|error| Error::Encode(error.to_string()))?;
        Ok(Self {
            generator: IpcDataGenerator::default(),
            tracker: DictionaryTracker::new(false),
            options,
            context: IpcWriteContext::default(),
        })
    }

    /// Encode an Arrow schema message.
    ///
    /// # Errors
    /// Reserved for encoder failures. Schema encoding itself does not allocate a
    /// fallible IPC result today.
    pub fn encode_schema(&mut self, schema: &Schema) -> Result<EncodedFlightBody> {
        reject_dictionaries(schema)?;
        let encoded = self.generator.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut self.tracker,
            &self.options,
        );
        Ok(EncodedFlightBody {
            data_header: encoded.ipc_message,
            data_body: encoded.arrow_data,
        })
    }

    /// Encode one record batch, rejecting dictionary columns.
    ///
    /// # Errors
    /// Returns an encoding error when IPC encoding fails or the batch contains
    /// a dictionary array.
    pub fn encode_batch(&mut self, batch: &RecordBatch) -> Result<EncodedFlightBody> {
        reject_dictionaries(batch.schema().as_ref())?;
        let (dictionaries, encoded) = self
            .generator
            .encode(batch, &mut self.tracker, &self.options, &mut self.context)
            .map_err(|error| Error::Encode(error.to_string()))?;
        if !dictionaries.is_empty() {
            return Err(Error::Encode(
                "dictionary columns are not supported on Arrow Flight streams".into(),
            ));
        }
        Ok(EncodedFlightBody {
            data_header: encoded.ipc_message,
            data_body: encoded.arrow_data,
        })
    }
}

/// Convert any columnar insert source into an Arrow record batch.
///
/// The Arrow schema carries GreptimeDB semantic metadata. JSON columns remain
/// binary values and record `greptime:type = Json`.
///
/// # Errors
/// Returns a schema error when the batch is invalid or a column cannot be
/// represented as an Arrow array.
pub fn batch_to_record_batch<B: BatchEncode + ?Sized>(batch: &B) -> Result<RecordBatch> {
    let request = batch.encode_insert_request()?;
    let rows = usize::try_from(request.row_count)
        .map_err(|_| Error::SchemaViolation("row count does not fit usize".into()))?;
    record_batch_from_columns(&request.columns, rows)
}

/// Inclusive timestamp bounds in the column's native unit.
///
/// An empty batch yields `(None, None)`.
///
/// # Errors
/// Returns a schema error when the batch does not contain exactly one
/// non-null timestamp column.
pub fn timestamp_bounds(batch: &RecordBatch) -> Result<(Option<i64>, Option<i64>)> {
    if batch.num_rows() == 0 {
        return Ok((None, None));
    }
    let (index, _) = timestamp_column(batch.schema().as_ref())?;
    let column = batch.column(index);
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for row in 0..batch.num_rows() {
        let value = timestamp_at(column, row)?;
        min = min.min(value);
        max = max.max(value);
    }
    Ok((Some(min), Some(max)))
}

/// Split a batch into contiguous one-hour timestamp windows.
///
/// Rows keep their original order inside each window. Windows are emitted from
/// earliest to latest. An empty batch is returned unchanged.
///
/// # Errors
/// Returns a schema error when the timestamp column is missing, nullable, or
/// not an Arrow timestamp.
pub fn partition_record_batch_by_hour(batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
    if batch.num_rows() == 0 {
        return Ok(vec![batch.clone()]);
    }
    let (index, unit) = timestamp_column(batch.schema().as_ref())?;
    let ticks = hour_ticks(unit);
    let column = batch.column(index);
    let mut windows: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        let value = timestamp_at(column, row)?;
        let start = value
            .checked_div_euclid(ticks)
            .ok_or_else(|| Error::SchemaViolation("timestamp hour window overflowed".into()))?;
        windows.entry(start).or_default().push(row);
    }
    let mut parts = Vec::with_capacity(windows.len());
    for indices in windows.into_values() {
        parts.push(take_rows(batch, &indices)?);
    }
    Ok(parts)
}

fn reject_dictionaries(schema: &Schema) -> Result<()> {
    if schema
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Dictionary(_, _)))
    {
        return Err(Error::Encode(
            "dictionary columns are not supported on Arrow Flight streams".into(),
        ));
    }
    Ok(())
}
