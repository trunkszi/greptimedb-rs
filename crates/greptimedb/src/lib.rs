//! Synchronous, completion-driven, thread-affine Rust SDK for GreptimeDB.

pub mod batch;
pub mod buffer_policy;
pub mod bulk;
pub mod client;
pub mod column_buf;
pub mod config;
pub mod ddl;
pub mod error;
pub mod failover;
pub mod hints;
pub mod prom;
pub mod query;
pub mod row;
pub mod stream_inserter;
pub mod threading;
#[cfg(feature = "tls")]
pub mod tls;
pub mod transport;
pub mod types;

pub mod row_batch {
    //! Row-oriented batch path used by client and streaming sessions.
    pub use crate::batch::RowBatch;
}

pub use batch::{BatchEncode, ColumnarBatch, MultiTableBatch, RowBatch};
pub use buffer_policy::{BufferPolicy, BufferRetentionPolicy, MemoryBuffer};
pub use bulk::{
    BulkStreamWriter, BulkWriteOptions, BulkWriteStats, DataType, DoPutMetadata, DoPutResponse,
    Field, FlightCompression, FlightIpcEncoder, RecordBatch, Schema, SchemaRef, TimeUnit,
};
pub use client::{Client, HealthReport};
pub use column_buf::{NullMask, TypedColumnBuf};
pub use config::{Auth, ClientOptions, Compression, Endpoint, TransportScheme};
pub use error::{Error, ErrorCode, Result, RetryableError};
pub use failover::{
    EndpointHealthState, EndpointSelection, FailoverConfig, FailoverEvent, FailoverPolicy,
};
pub use greptimedb_macros as macros;
pub use greptimedb_macros::Row;
pub use greptimedb_proto as proto;
pub use greptimedb_proto::v1::ddl_request::Expr as DdlExpr;
pub use greptimedb_proto::v1::{IntervalMonthDayNano, PromInstantQuery, PromRangeQuery, Value};
pub use hints::{MergeMode, WriteHints};
pub use prom::PromQueryResult;
pub use query::QueryStream;
pub use row::{BorrowedRow, ColumnMeta, Row, RowError};
pub use threading::{LeaseGuard, ThreadLocalClient, WorkerBalance, WorkerClient, WorkerConfig};
#[cfg(feature = "tls")]
pub use tls::TlsOptions;
pub use transport::EndpointTransport;
pub use types::{
    ColumnDataType, Date, DateTime, Decimal128, SemanticType, TimestampUnit, ValueRef,
};
