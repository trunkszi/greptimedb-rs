//! Synchronous Arrow Flight bulk ingestion.
//!
//! [`BulkStreamWriter`] sends GreptimeDB `DoPut` frames. Schema and record-batch
//! payloads are Arrow IPC bytes carried in `greptimedb_proto::flight::FlightData`.

mod conversion;
pub mod schema;
pub mod writer;

pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
pub use schema::{
    EncodedFlightBody, FlightCompression, FlightIpcEncoder, batch_to_record_batch,
    partition_record_batch_by_hour, timestamp_bounds,
};
pub use writer::{
    BulkStreamWriter, BulkWriteOptions, BulkWriteStats, DoPutMetadata, DoPutResponse,
};
