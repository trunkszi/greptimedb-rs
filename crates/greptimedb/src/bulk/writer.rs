//! Caller-thread Arrow Flight `DoPut` bulk writer.
//!
//! The writer keeps a bounded number of record batches in flight. Dropping an
//! unfinished writer cancels the HTTP/2 stream.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use greptimedb_proto::flight::{self, FlightData, FlightDescriptor, PutResult};
use serde::{Deserialize, Serialize};
use sonic_rs::JsonValueTrait;

use crate::batch::BatchEncode;
use crate::client::Client;
use crate::config::ClientOptions;
use crate::error::{Error, Result};
use crate::hints::WriteHints;
use crate::transport::EndpointTransport;

use super::schema::{
    FlightCompression, FlightIpcEncoder, batch_to_record_batch, partition_record_batch_by_hour,
    timestamp_bounds,
};

const DO_PUT_PATH: &str = "/arrow.flight.protocol.FlightService/DoPut";

/// Per-batch metadata carried in `FlightData.app_metadata`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DoPutMetadata {
    /// Client-assigned identifier. The schema frame uses `0`.
    pub request_id: i64,
    /// Smallest timestamp in the batch, in the column's native unit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_timestamp: Option<i64>,
    /// Largest timestamp in the batch, in the column's native unit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_timestamp: Option<i64>,
}

/// Server acknowledgement carried in `PutResult.app_metadata`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DoPutResponse {
    /// Identifier of the request being acknowledged.
    pub request_id: i64,
    /// Rows the server accepted for that request.
    pub affected_rows: usize,
}

/// Limits and encoding options for one bulk stream.
#[derive(Clone, Debug)]
pub struct BulkWriteOptions {
    /// Maximum record-batch frames waiting for a `PutResult`.
    pub max_in_flight: usize,
    /// Arrow IPC body compression.
    pub compression: FlightCompression,
    /// Split each write on one-hour timestamp windows before sending.
    pub partition_by_time_window: bool,
    /// Ingestion directives merged over the client defaults for this stream.
    pub write_hints: Option<WriteHints>,
}

impl Default for BulkWriteOptions {
    fn default() -> Self {
        Self {
            max_in_flight: 8,
            compression: FlightCompression::None,
            partition_by_time_window: false,
            write_hints: None,
        }
    }
}

/// Rows and frames accepted by a finished bulk stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BulkWriteStats {
    /// Sum of `affected_rows` from data-frame acknowledgements.
    pub total_affected_rows: usize,
    /// Record-batch frames successfully submitted.
    pub batches_sent: usize,
}

/// Thread-affine `DoPut` writer.
///
/// The schema frame is acknowledged before any data frame is sent. `request_id`
/// values then increase from 1. The writer is confined to the thread that opened
/// it.
pub struct BulkStreamWriter {
    call: Option<grpc::BidiStreamingCall<FlightData, PutResult>>,
    encoder: FlightIpcEncoder,
    schema: Arc<Schema>,
    max_in_flight: usize,
    partition_by_time_window: bool,
    next_request_id: i64,
    pending: VecDeque<i64>,
    affected_rows: usize,
    batches_sent: usize,
    finished: bool,
    terminal_error: Option<TerminalFailure>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl BulkStreamWriter {
    /// Open a bulk stream and wait for the server to accept the Arrow schema.
    ///
    /// # Errors
    /// Returns a configuration, transport, encoding, or protocol error. A failure
    /// after the RPC starts cancels the stream when this value is dropped.
    pub fn open(
        channel: &mut grpc::SyncChannel<EndpointTransport>,
        client_options: &ClientOptions,
        table_name: &str,
        schema: &Schema,
        bulk_options: BulkWriteOptions,
    ) -> Result<Self> {
        if table_name.is_empty() {
            return Err(Error::InvalidConfig("table name must be nonempty".into()));
        }
        if bulk_options.max_in_flight == 0 {
            return Err(Error::InvalidConfig(
                "max_in_flight must be at least 1".into(),
            ));
        }
        let call_options =
            client_options.try_build_call_options(bulk_options.write_hints.as_ref())?;
        let call = channel
            .bidi_streaming(
                DO_PUT_PATH,
                grpc::ProstCodec::<FlightData, PutResult>::new(),
                call_options,
            )
            .map_err(Error::from)?;
        let mut writer = Self {
            call: Some(call),
            encoder: FlightIpcEncoder::new(bulk_options.compression)?,
            schema: Arc::new(schema.clone()),
            max_in_flight: bulk_options.max_in_flight,
            partition_by_time_window: bulk_options.partition_by_time_window,
            next_request_id: 1,
            pending: VecDeque::new(),
            affected_rows: 0,
            batches_sent: 0,
            finished: false,
            terminal_error: None,
            _not_send_sync: PhantomData,
        };
        writer.send_schema(table_name)?;
        Ok(writer)
    }

    /// Encode and send a record batch, applying backpressure.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or protocol errors. Local validation
    /// failures leave the stream usable; remote and acknowledgement failures are
    /// terminal and are returned again by later writes or finish. Earlier partitions
    /// may already have been sent when a partitioned write fails.
    pub fn write_record_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_active()?;
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(Error::SchemaViolation(
                "record batch schema does not match the Flight stream".into(),
            ));
        }
        if self.partition_by_time_window {
            for part in partition_record_batch_by_hour(batch)? {
                self.send_batch(&part)?;
            }
        } else {
            self.send_batch(batch)?;
        }
        Ok(())
    }

    /// Convert a columnar batch and send it as one or more Flight frames.
    ///
    /// # Errors
    /// Returns the schema and transport errors of [`Self::write_record_batch`].
    pub fn write_batch<B: BatchEncode + ?Sized>(&mut self, batch: &B) -> Result<()> {
        self.ensure_active()?;
        let record = batch_to_record_batch(batch)?;
        self.write_record_batch(&record)
    }

    /// Half-close the request stream and collect every outstanding acknowledgement.
    ///
    /// # Errors
    /// Returns a transport or protocol error, including a missing acknowledgement
    /// or a GreptimeDB trailer status.
    pub fn finish(mut self) -> Result<BulkWriteStats> {
        self.ensure_active()?;
        self.close_send()?;
        loop {
            let received = {
                let call = self.call_mut()?;
                call.recv_next()
            };
            match received {
                Ok(Some(result)) => self.accept_result(result)?,
                Ok(None) => break,
                Err(error) => return Err(self.map_error(error)),
            }
        }
        if !self.pending.is_empty() {
            self.check_trailers()?;
            return Err(self.latch(Error::Protocol(
                "DoPut ended before every data frame was acknowledged".into(),
            )));
        }
        self.check_trailers()?;
        self.finished = true;
        Ok(BulkWriteStats {
            total_affected_rows: self.affected_rows,
            batches_sent: self.batches_sent,
        })
    }

    fn send_schema(&mut self, table_name: &str) -> Result<()> {
        let encoded = self.encoder.encode_schema(self.schema.as_ref())?;
        let metadata = DoPutMetadata {
            request_id: 0,
            min_timestamp: None,
            max_timestamp: None,
        };
        let frame = FlightData {
            flight_descriptor: Some(FlightDescriptor {
                r#type: flight::flight_descriptor::DescriptorType::Path as i32,
                cmd: Vec::new(),
                path: vec![table_name.to_owned()],
            }),
            data_header: encoded.data_header,
            app_metadata: sonic_rs::to_vec(&metadata)
                .map_err(|error| Error::Encode(error.to_string()))?,
            data_body: encoded.data_body,
        };
        let sent = self.call_mut()?.send(&frame);
        if let Err(error) = sent {
            return Err(self.map_error(error));
        }
        let received = {
            let call = self.call_mut()?;
            call.recv_next()
        };
        match received {
            Ok(Some(result)) => self.accept_schema_ack(result),
            Ok(None) => {
                self.check_trailers()?;
                Err(self.latch(Error::Protocol(
                    "DoPut closed before the schema acknowledgement".into(),
                )))
            }
            Err(error) => Err(self.map_error(error)),
        }
    }

    fn accept_schema_ack(&self, result: PutResult) -> Result<()> {
        let response = decode_response(&result)?;
        if response.request_id != 0 {
            return Err(Error::Protocol(format!(
                "schema acknowledgement used request_id {}",
                response.request_id
            )));
        }
        Ok(())
    }

    fn send_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.drain_ready()?;
        self.wait_for_capacity()?;
        let request_id = self.next_request_id;
        let next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| Error::Encode("DoPut request_id overflowed".into()))?;
        let next_batches_sent = self
            .batches_sent
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("bulk batch counter overflowed".into()))?;
        let has_timestamp = batch.schema().fields().iter().any(|field| {
            field
                .metadata()
                .get(super::schema::SEMANTIC_TYPE_KEY)
                .map(String::as_str)
                == Some("timestamp")
        });
        let (min_timestamp, max_timestamp) = if self.partition_by_time_window || has_timestamp {
            timestamp_bounds(batch)?
        } else {
            (None, None)
        };
        let encoded = self.encoder.encode_batch(batch)?;
        let metadata = DoPutMetadata {
            request_id,
            min_timestamp,
            max_timestamp,
        };
        let frame = FlightData {
            flight_descriptor: None,
            data_header: encoded.data_header,
            app_metadata: sonic_rs::to_vec(&metadata)
                .map_err(|error| Error::Encode(error.to_string()))?,
            data_body: encoded.data_body,
        };
        let sent = self.call_mut()?.send(&frame);
        if let Err(error) = sent {
            return Err(self.map_error(error));
        }
        self.pending.push_back(request_id);
        self.batches_sent = next_batches_sent;
        self.next_request_id = next_request_id;
        Ok(())
    }

    fn drain_ready(&mut self) -> Result<()> {
        loop {
            let received = {
                let call = self.call_mut()?;
                call.try_recv_next()
            };
            match received {
                Ok(Some(result)) => self.accept_result(result)?,
                Ok(None) => {
                    self.check_trailers()?;
                    return Err(self.latch(Error::Protocol(
                        "DoPut response stream closed before requests were half-closed".into(),
                    )));
                }
                Err(grpc::StreamError::WouldBlock) => return Ok(()),
                Err(error) => return Err(self.map_error(error)),
            }
        }
    }

    fn wait_for_capacity(&mut self) -> Result<()> {
        while self.pending.len() >= self.max_in_flight {
            let received = {
                let call = self.call_mut()?;
                call.recv_next()
            };
            match received {
                Ok(Some(result)) => self.accept_result(result)?,
                Ok(None) => {
                    self.check_trailers()?;
                    return Err(self.latch(Error::Protocol(
                        "DoPut response stream closed while batches were in flight".into(),
                    )));
                }
                Err(error) => return Err(self.map_error(error)),
            }
        }
        Ok(())
    }

    fn accept_result(&mut self, result: PutResult) -> Result<()> {
        let response = decode_response(&result).map_err(|error| self.latch(error))?;
        let Some(position) = self
            .pending
            .iter()
            .position(|id| *id == response.request_id)
        else {
            return Err(self.latch(Error::Protocol(format!(
                "unexpected DoPut request_id {}",
                response.request_id
            ))));
        };
        let Some(next_total) = self.affected_rows.checked_add(response.affected_rows) else {
            return Err(self.latch(Error::Protocol("affected row count overflowed".into())));
        };
        self.pending.remove(position);
        self.affected_rows = next_total;
        Ok(())
    }

    fn close_send(&mut self) -> Result<()> {
        let result = self.call_mut()?.half_close();
        result.map_err(|error| self.map_error(error))
    }

    fn check_trailers(&mut self) -> Result<()> {
        let result = self
            .call
            .as_ref()
            .ok_or_else(|| Error::Protocol("bulk stream is closed".into()))?
            .trailing_metadata();
        match result {
            Ok(Some(metadata)) => {
                if let Some(error) = application_error_from_trailers(&metadata) {
                    return Err(self.latch(error));
                }
                Ok(())
            }
            Ok(None) => Err(self.latch(Error::Protocol(
                "Flight stream ended without trailers".into(),
            ))),
            Err(error) => Err(self.map_error(error)),
        }
    }

    fn ensure_active(&self) -> Result<()> {
        match &self.terminal_error {
            Some(error) => Err(error.restore()),
            None => Ok(()),
        }
    }

    fn latch(&mut self, error: Error) -> Error {
        let terminal = self
            .terminal_error
            .get_or_insert_with(|| TerminalFailure::capture(&error));
        terminal.restore()
    }

    fn call_mut(&mut self) -> Result<&mut grpc::BidiStreamingCall<FlightData, PutResult>> {
        self.call
            .as_mut()
            .ok_or_else(|| Error::Protocol("bulk stream is closed".into()))
    }

    fn map_error(&mut self, error: grpc::StreamError) -> Error {
        let error = match error {
            grpc::StreamError::WouldBlock => {
                Error::Stream("blocking Flight call returned pending".into())
            }
            grpc::StreamError::Status(status) => {
                let trailers = match self.call.as_ref().map(|call| call.trailing_metadata()) {
                    Some(Ok(metadata)) => metadata,
                    Some(Err(_)) | None => None,
                };
                Error::from_grpc_status_and_trailers(status, trailers.as_ref())
            }
        };
        self.latch(error)
    }
}

impl Client {
    /// Open a `DoPut` bulk stream using this client's routing and RPC defaults.
    ///
    /// # Errors
    /// Returns missing-channel, configuration, transport, encoding, or protocol
    /// errors. A failed handshake cancels the RPC when the error is returned.
    pub fn bulk_writer(
        &mut self,
        table_name: &str,
        schema: &Schema,
        options: BulkWriteOptions,
    ) -> Result<BulkStreamWriter> {
        let client_options = self.options().clone();
        BulkStreamWriter::open(
            self.channel_mut()?,
            &client_options,
            table_name,
            schema,
            options,
        )
    }
}

impl Drop for BulkStreamWriter {
    fn drop(&mut self) {
        if !self.finished
            && let Some(mut call) = self.call.take()
        {
            #[allow(clippy::let_underscore_must_use)]
            {
                let _ = call.cancel();
            }
        }
    }
}

fn decode_response(result: &PutResult) -> Result<DoPutResponse> {
    sonic_rs::from_slice(&result.app_metadata).map_err(|error| Error::Decode(error.to_string()))
}

#[derive(Clone)]
enum TerminalFailure {
    Server {
        code: crate::error::ErrorCode,
        raw_code: u32,
        message: String,
        retry_hint: bool,
    },
    Grpc(grpc::Status),
    Protocol(String),
    Decode(String),
    Stream(String),
}

impl TerminalFailure {
    fn capture(error: &Error) -> Self {
        match error {
            Error::ServerStatus {
                code,
                raw_code,
                message,
                retry_hint,
            } => Self::Server {
                code: *code,
                raw_code: *raw_code,
                message: message.clone(),
                retry_hint: *retry_hint,
            },
            Error::Grpc(status) => Self::Grpc(status.clone()),
            Error::Protocol(message) => Self::Protocol(message.clone()),
            Error::Decode(message) => Self::Decode(message.clone()),
            Error::Stream(message) => Self::Stream(message.clone()),
            other => Self::Protocol(other.to_string()),
        }
    }

    fn restore(&self) -> Error {
        match self {
            Self::Server {
                code,
                raw_code,
                message,
                retry_hint,
            } => Error::ServerStatus {
                code: *code,
                raw_code: *raw_code,
                message: message.clone(),
                retry_hint: *retry_hint,
            },
            Self::Grpc(status) => Error::Grpc(status.clone()),
            Self::Protocol(message) => Error::Protocol(message.clone()),
            Self::Decode(message) => Error::Decode(message.clone()),
            Self::Stream(message) => Error::Stream(message.clone()),
        }
    }
}

/// Interpret application failures on an otherwise successful Flight stream.
pub(crate) fn application_error_from_trailers(metadata: &grpc::Metadata) -> Option<Error> {
    let mut inner = None;
    let mut alias = None;
    let mut alias_message = None;
    let mut numeric_code = None;
    let mut invalid_numeric = false;
    let mut has_hint = false;
    let mut retry_hint = true;
    for (key, value) in metadata.iter() {
        let grpc::core::MetadataValue::Ascii(value) = value else {
            continue;
        };
        if key.eq_ignore_ascii_case("INNER_ERROR_MSG") && inner.is_none() {
            inner = inner_diagnostic(value);
        } else if key.eq_ignore_ascii_case("x-greptime-err-msg") {
            if alias.is_none() {
                alias = inner_diagnostic(value);
            }
            if alias_message.is_none() {
                alias_message = Some(value.clone());
            }
        } else if key.eq_ignore_ascii_case("x-greptime-err-code") {
            match value.parse::<u32>() {
                Ok(code)
                    if !value.is_empty()
                        && value.bytes().all(|byte| byte.is_ascii_digit())
                        && numeric_code.is_none_or(|previous| previous == code) =>
                {
                    numeric_code = Some(code)
                }
                _ => invalid_numeric = true,
            }
        } else if key.eq_ignore_ascii_case("x-greptime-err-retry-hint") {
            has_hint |= value == "true" || value == "false";
            retry_hint &= value == "true";
        }
    }
    let (raw_code, message) = inner.or(alias).or_else(|| {
        numeric_code
            .filter(|code| !invalid_numeric && *code != 0)
            .map(|code| (code, alias_message.unwrap_or_default()))
    })?;
    Some(Error::ServerStatus {
        code: crate::error::ErrorCode::from_u32(raw_code),
        raw_code,
        message,
        retry_hint: has_hint && retry_hint,
    })
}

fn inner_diagnostic(value: &str) -> Option<(u32, String)> {
    let value = match sonic_rs::from_str::<sonic_rs::Value>(value) {
        Ok(value) => value,
        Err(_) => return None,
    };
    let raw_code = u32::try_from(value.get("code")?.as_u64()?).ok()?;
    if raw_code == 0 {
        return None;
    }
    Some((raw_code, value.get("err_msg")?.as_str()?.to_owned()))
}
