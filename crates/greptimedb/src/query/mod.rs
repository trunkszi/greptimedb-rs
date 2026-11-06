//! Synchronous Arrow Flight `DoGet` queries.
//!
//! [`QueryStream`] submits a `GreptimeRequest` inside a Flight ticket and decodes
//! the returned IPC stream into record batches and columnar rows.

pub mod decode;
pub mod stream;

pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema, SchemaRef};
pub use decode::{
    decode_flight_record_batch, decode_flight_schema, decode_record_batch_to_columnar_batch,
};
pub use stream::QueryStream;
