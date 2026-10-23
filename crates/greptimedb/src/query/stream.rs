//! Caller-thread Arrow Flight `DoGet` query stream.

use std::marker::PhantomData;
use std::rc::Rc;

use std::collections::HashMap;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_schema::SchemaRef;
use greptimedb_proto::flight::{FlightData, Ticket};
use greptimedb_proto::v1::{
    self, GreptimeRequest, InsertIntoPlan, PromRangeQuery, QueryRequest, TableName,
    greptime_request, query_request,
};
use prost::Message;

use crate::client::Client;
use crate::config::ClientOptions;
use crate::error::{Error, Result};
use crate::transport::EndpointTransport;

use super::decode::{
    decode_flight_record_batch_with_dictionaries, decode_flight_schema, parse_message,
};

const DO_GET_PATH: &str = "/arrow.flight.protocol.FlightService/DoGet";

/// Thread-affine stream of Arrow batches from `DoGet`.
///
/// The request ticket contains one `GreptimeRequest` whose body is a
/// [`QueryRequest`]. Dropping an unfinished stream cancels it.
pub struct QueryStream {
    call: Option<grpc::ServerStreamingCall<FlightData>>,
    schema: Option<SchemaRef>,
    finished: bool,
    failed: bool,
    dictionaries: HashMap<i64, ArrayRef>,
    affected_rows: u64,
    saw_affected_rows: bool,
    metrics: Vec<Vec<u8>>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl QueryStream {
    /// Run a SQL query.
    ///
    /// # Errors
    /// Returns a configuration, encoding, or transport error while opening the
    /// stream. Query execution errors arrive from [`Self::next_batch`].
    pub fn sql(
        channel: &mut grpc::SyncChannel<EndpointTransport>,
        options: &ClientOptions,
        query: &str,
    ) -> Result<Self> {
        Self::open(
            channel,
            options,
            QueryRequest {
                query: Some(query_request::Query::Sql(query.to_owned())),
            },
        )
    }

    /// Run a Prometheus range query.
    ///
    /// `start`, `end`, `step`, and `lookback` are the strings GreptimeDB expects
    /// on the PromQL wire format, such as `"1672531200"` or `"15s"`.
    ///
    /// # Errors
    /// Returns a configuration, encoding, or transport error while opening the stream.
    pub fn prom_range_query(
        channel: &mut grpc::SyncChannel<EndpointTransport>,
        options: &ClientOptions,
        query: &str,
        start: &str,
        end: &str,
        step: &str,
        lookback: &str,
    ) -> Result<Self> {
        Self::open(
            channel,
            options,
            QueryRequest {
                query: Some(query_request::Query::PromRangeQuery(PromRangeQuery {
                    query: query.to_owned(),
                    start: start.to_owned(),
                    end: end.to_owned(),
                    step: step.to_owned(),
                    lookback: lookback.to_owned(),
                })),
            },
        )
    }

    /// Run a serialized logical plan.
    ///
    /// # Errors
    /// Returns a configuration, encoding, or transport error while opening the stream.
    pub fn logical_plan(
        channel: &mut grpc::SyncChannel<EndpointTransport>,
        options: &ClientOptions,
        plan: &[u8],
    ) -> Result<Self> {
        Self::open(
            channel,
            options,
            QueryRequest {
                query: Some(query_request::Query::LogicalPlan(plan.to_vec())),
            },
        )
    }

    /// Run `INSERT INTO` expressed as a logical plan for one table.
    ///
    /// The table is addressed in the client's catalog and schema.
    ///
    /// # Errors
    /// Returns a configuration, encoding, or transport error while opening the stream.
    pub fn insert_into_plan(
        channel: &mut grpc::SyncChannel<EndpointTransport>,
        options: &ClientOptions,
        table_name: &str,
        logical_plan: &[u8],
    ) -> Result<Self> {
        Self::open(
            channel,
            options,
            QueryRequest {
                query: Some(query_request::Query::InsertIntoPlan(InsertIntoPlan {
                    table_name: Some(TableName {
                        catalog_name: options.catalog.clone(),
                        schema_name: options.schema.clone(),
                        table_name: table_name.to_owned(),
                    }),
                    logical_plan: logical_plan.to_vec(),
                })),
            },
        )
    }

    /// Borrow the schema announced by the first Flight frame, once seen.
    #[must_use]
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Read the next record batch.
    ///
    /// The initial schema frame is consumed internally and is not returned.
    /// `Ok(None)` means the server completed the stream successfully.
    ///
    /// # Errors
    /// Returns a transport, protocol, or decode error. A GreptimeDB trailer
    /// status is reported as [`Error::ServerStatus`].
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.failed {
            return Err(Error::Protocol(
                "Flight query failed and must be dropped".into(),
            ));
        }
        let result = self.next_batch_inner();
        if result.is_err() {
            self.failed = true;
            if let Some(call) = self.call.as_mut() {
                let _cancel_result = call.cancel();
            }
            self.finished = true;
        }
        result
    }

    fn next_batch_inner(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        loop {
            let received = {
                let call = self.call_mut()?;
                call.recv_next()
            };
            match received {
                Ok(None) => {
                    self.check_trailers()?;
                    self.finished = true;
                    return Ok(None);
                }
                Ok(Some(frame)) => {
                    let message = parse_message(&frame.data_header)?;
                    if message.bodyLength() < 0
                        || usize::try_from(message.bodyLength()).ok() != Some(frame.data_body.len())
                    {
                        return Err(Error::Protocol(
                            "Flight IPC body length does not match header".into(),
                        ));
                    }
                    match message.header_type() {
                        arrow_ipc::MessageHeader::NONE => {
                            self.accept_metadata(&frame.app_metadata)?;
                        }
                        arrow_ipc::MessageHeader::Schema => {
                            if self.schema.is_some() {
                                return Err(Error::Protocol("duplicate Flight schema".into()));
                            }
                            self.schema = Some(decode_flight_schema(&frame.data_header)?);
                        }
                        arrow_ipc::MessageHeader::DictionaryBatch => {
                            let schema = self.schema.as_ref().ok_or_else(|| {
                                Error::Protocol("dictionary preceded Flight schema".into())
                            })?;
                            let dictionary = message
                                .header_as_dictionary_batch()
                                .ok_or_else(|| Error::Decode("missing dictionary header".into()))?;
                            arrow_ipc::reader::read_dictionary(
                                &Buffer::from(frame.data_body),
                                dictionary,
                                schema,
                                &mut self.dictionaries,
                                &message.version(),
                            )
                            .map_err(|error| Error::Decode(error.to_string()))?;
                        }
                        arrow_ipc::MessageHeader::RecordBatch => {
                            let schema = self.schema.clone().ok_or_else(|| {
                                Error::Protocol("record batch preceded Flight schema".into())
                            })?;
                            return decode_flight_record_batch_with_dictionaries(
                                &frame.data_header,
                                &frame.data_body,
                                &schema,
                                &self.dictionaries,
                            )
                            .map(Some);
                        }
                        other => {
                            return Err(Error::Protocol(format!(
                                "unsupported Flight IPC header {other:?}"
                            )));
                        }
                    }
                }
                Err(error) => return Err(self.map_error(error)),
            }
        }
    }

    fn accept_metadata(&mut self, bytes: &[u8]) -> Result<()> {
        let metadata =
            v1::FlightMetadata::decode(bytes).map_err(|error| Error::Decode(error.to_string()))?;
        if metadata.affected_rows.is_none() && metadata.metrics.is_none() {
            return Err(Error::Protocol("empty Flight metadata".into()));
        }
        if let Some(rows) = metadata.affected_rows {
            self.affected_rows = self
                .affected_rows
                .checked_add(u64::from(rows.value))
                .ok_or_else(|| Error::Protocol("affected row count overflowed".into()))?;
            self.saw_affected_rows = true;
        }
        if let Some(metrics) = metadata.metrics {
            self.metrics.push(metrics.metrics);
        }
        Ok(())
    }

    /// Rows reported by metadata frames consumed so far.
    pub const fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Opaque GreptimeDB metrics payloads received so far.
    pub fn metrics(&self) -> &[Vec<u8>] {
        &self.metrics
    }

    /// Drain the stream into owned record batches.
    ///
    /// # Errors
    /// Returns the first error from [`Self::next_batch`].
    pub fn collect_batches(mut self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next_batch()? {
            batches.push(batch);
        }
        Ok(batches)
    }

    /// Drain remaining frames and return DML affected rows and query metrics.
    ///
    /// # Errors
    /// Returns the first stream, IPC, server diagnostic, or row-count conversion error.
    pub fn finish(mut self) -> Result<(Option<usize>, Vec<Vec<u8>>)> {
        while self.next_batch()?.is_some() {}
        let rows = if self.saw_affected_rows {
            Some(
                usize::try_from(self.affected_rows)
                    .map_err(|_| Error::Protocol("affected row count exceeds usize".into()))?,
            )
        } else {
            None
        };
        Ok((rows, std::mem::take(&mut self.metrics)))
    }

    fn open(
        channel: &mut grpc::SyncChannel<EndpointTransport>,
        options: &ClientOptions,
        query: QueryRequest,
    ) -> Result<Self> {
        let request = GreptimeRequest {
            header: Some(options.build_request_header(None)),
            request: Some(greptime_request::Request::Query(query)),
        };
        let ticket = Ticket {
            ticket: request.encode_to_vec(),
        };
        let call_options = options.try_build_call_options(None)?;
        let call = channel
            .server_streaming(
                DO_GET_PATH,
                &ticket,
                grpc::ProstCodec::<Ticket, FlightData>::new(),
                call_options,
            )
            .map_err(Error::from)?;
        Ok(Self {
            call: Some(call),
            schema: None,
            finished: false,
            failed: false,
            dictionaries: HashMap::new(),
            affected_rows: 0,
            saw_affected_rows: false,
            metrics: Vec::new(),
            _not_send_sync: PhantomData,
        })
    }

    fn call_mut(&mut self) -> Result<&mut grpc::ServerStreamingCall<FlightData>> {
        self.call
            .as_mut()
            .ok_or_else(|| Error::Protocol("query stream is closed".into()))
    }

    fn check_trailers(&self) -> Result<()> {
        let Some(call) = self.call.as_ref() else {
            return Err(Error::Protocol("query stream is closed".into()));
        };
        match call.trailing_metadata() {
            Ok(Some(metadata)) => {
                match crate::bulk::writer::application_error_from_trailers(&metadata) {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            }
            Ok(None) => Err(Error::Protocol(
                "Flight query ended without trailers".into(),
            )),
            Err(error) => Err(map_status(error)),
        }
    }

    fn map_error(&self, error: grpc::StreamError) -> Error {
        match error {
            grpc::StreamError::WouldBlock => {
                Error::Stream("blocking Flight query returned pending".into())
            }
            grpc::StreamError::Status(status) => {
                let trailers = self
                    .call
                    .as_ref()
                    .and_then(|call| call.trailing_metadata().ok().flatten());
                Error::from_grpc_status_and_trailers(status, trailers.as_ref())
            }
        }
    }
}

impl Client {
    /// Run a SQL query using this client's routing and RPC defaults.
    ///
    /// # Errors
    /// Returns missing-channel, configuration, encoding, or transport errors
    /// while opening the stream. Query execution errors arrive from
    /// [`QueryStream::next_batch`].
    pub fn query(&mut self, sql: &str) -> Result<QueryStream> {
        let client_options = self.options().clone();
        QueryStream::sql(self.channel_mut()?, &client_options, sql)
    }
}

impl Drop for QueryStream {
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

fn map_status(error: grpc::StreamError) -> Error {
    match error {
        grpc::StreamError::WouldBlock => Error::Stream("Flight query is pending".into()),
        grpc::StreamError::Status(status) => Error::Grpc(status),
    }
}
