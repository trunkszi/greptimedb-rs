//! Caller-thread client-streaming ingestion with cancellation on early drop.

use std::{marker::PhantomData, rc::Rc};

use greptimedb_proto::v1::{
    self as v1, GreptimeRequest, GreptimeResponse, RequestHeader, greptime_request,
    greptime_response,
};

use crate::{
    batch::{BatchEncode, RowBatch},
    buffer_policy::MemoryBuffer,
    client::{Client, StreamFeedback, channel_for, terminal_fault, validate_ok_trailers},
    config::ClientOptions,
    error::{Error, Result},
    hints::WriteHints,
    transport::EndpointTransport,
};

const STREAM_PATH: &str = "/greptime.v1.GreptimeDatabase/HandleRequests";

type StreamingCall = grpc::ClientStreamingCall<GreptimeRequest, GreptimeResponse>;

/// A thread-affine ingestion stream whose final response acknowledges all requests.
///
/// Dropping an unfinished stream cancels its HTTP/2 stream instead of waiting for
/// a server response. Cancellation does not undo writes already accepted by the
/// server; callers must not blindly replay requests after an uncertain failure.
pub struct StreamInserter {
    call: Option<StreamingCall>,
    header: RequestHeader,
    batches_sent: usize,
    rows_sent: usize,
    finished: bool,
    feedback: Option<StreamFeedback>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl StreamInserter {
    /// Opens a client-streaming RPC with validated routing, credentials and hints.
    ///
    /// # Errors
    /// Returns configuration, metadata, transport or stream-submission failures.
    pub fn open(
        channel: &mut grpc::SyncChannel<EndpointTransport>,
        options: &ClientOptions,
        extra_hints: Option<&WriteHints>,
    ) -> Result<Self> {
        let call_options = options.try_build_call_options(extra_hints)?;
        let header = options.build_request_header(None);
        let call = channel.client_streaming(
            STREAM_PATH,
            grpc::ProstCodec::<GreptimeRequest, GreptimeResponse>::new(),
            call_options,
        )?;
        Ok(Self {
            call: Some(call),
            header,
            batches_sent: 0,
            rows_sent: 0,
            finished: false,
            feedback: None,
            _not_send_sync: PhantomData,
        })
    }

    /// Sends one request with the stream's routing and authentication header.
    ///
    /// Counters advance only after the complete message has been accepted by the
    /// stream. A failed transport write may nevertheless have reached the peer.
    ///
    /// # Errors
    /// Returns counter overflow, terminal stream, transport or server errors.
    pub fn send_request(&mut self, request: greptime_request::Request) -> Result<()> {
        let rows = request_rows(&request)
            .ok_or_else(|| Error::Protocol("stream request row count exceeds usize".into()))?;
        let batches_sent = self
            .batches_sent
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("stream batch count exceeds usize".into()))?;
        let rows_sent = self
            .rows_sent
            .checked_add(rows)
            .ok_or_else(|| Error::Protocol("stream row count exceeds usize".into()))?;
        let call = self
            .call
            .as_mut()
            .ok_or_else(|| Error::Stream("ingestion stream is closed".into()))?;
        let message = GreptimeRequest {
            header: Some(self.header.clone()),
            request: Some(request),
        };
        if let Err(error) = call.send(&message) {
            let error = call_error(call, error);
            self.report_error(&error);
            return Err(error);
        }
        self.batches_sent = batches_sent;
        self.rows_sent = rows_sent;
        Ok(())
    }

    /// Sends a columnar insert without changing its reusable source buffer.
    ///
    /// # Errors
    /// Returns schema, encoding, transport or server errors.
    pub fn send_batch(&mut self, batch: &(impl BatchEncode + ?Sized)) -> Result<()> {
        self.send_request(greptime_request::Request::Inserts(v1::InsertRequests {
            inserts: vec![batch.encode_insert_request()?],
        }))
    }

    /// Sends a columnar insert to an explicit table without changing the batch.
    ///
    /// # Errors
    /// Returns schema, destination, encoding, transport or server errors.
    pub fn send_batch_to(
        &mut self,
        table_name: &str,
        batch: &(impl BatchEncode + ?Sized),
    ) -> Result<()> {
        self.send_request(greptime_request::Request::Inserts(v1::InsertRequests {
            inserts: vec![batch.encode_insert_request_for(table_name)?],
        }))
    }

    /// Sends several tables in one frame after every batch has encoded successfully.
    ///
    /// # Errors
    /// Returns schema or encoding errors before sending any frame, or transport
    /// and server errors while sending the completed request.
    pub fn send_batches(&mut self, batches: &[&dyn BatchEncode]) -> Result<()> {
        let inserts = batches
            .iter()
            .map(|batch| batch.encode_insert_request())
            .collect::<Result<Vec<_>>>()?;
        self.send_request(greptime_request::Request::Inserts(v1::InsertRequests {
            inserts,
        }))
    }

    /// Sends a row-oriented insert without modifying the source buffer.
    ///
    /// # Errors
    /// Returns schema, encoding, transport or server errors.
    pub fn send_rows(&mut self, batch: &RowBatch) -> Result<()> {
        self.send_request(greptime_request::Request::RowInserts(
            v1::RowInsertRequests {
                inserts: vec![batch.encode_row_insert_request()?],
            },
        ))
    }

    /// Sends a columnar delete containing only the batch's tag and timestamp keys.
    ///
    /// # Errors
    /// Returns schema, encoding, transport or server errors.
    pub fn send_delete_batch(&mut self, batch: &(impl BatchEncode + ?Sized)) -> Result<()> {
        self.send_request(greptime_request::Request::Deletes(v1::DeleteRequests {
            deletes: vec![batch.encode_delete_request()?],
        }))
    }

    /// Sends a row-oriented delete without modifying the source buffer.
    ///
    /// # Errors
    /// Returns schema, encoding, transport or server errors.
    pub fn send_delete_rows(&mut self, batch: &RowBatch) -> Result<()> {
        self.send_request(greptime_request::Request::RowDeletes(
            v1::RowDeleteRequests {
                deletes: vec![batch.encode_row_delete_request()?],
            },
        ))
    }

    /// Sends a columnar insert and clears its buffer only after a successful send.
    ///
    /// Clearing follows the buffer's retention policy. Successful sending is not
    /// a server commit acknowledgement; call [`Self::finish`] to obtain that.
    ///
    /// # Errors
    /// Returns schema, encoding, transport or server errors without clearing the
    /// source buffer. A transport error does not guarantee the peer saw no data.
    pub fn write(&mut self, batch: &mut (impl BatchEncode + MemoryBuffer + ?Sized)) -> Result<()> {
        self.send_batch(batch)?;
        batch.clear();
        Ok(())
    }

    /// Sends a key-only columnar delete, then clears the reusable source buffer.
    ///
    /// # Errors
    /// Returns schema, encoding, transport or server errors without clearing the
    /// source buffer. A transport error does not guarantee the peer saw no data.
    pub fn write_delete(
        &mut self,
        batch: &mut (impl BatchEncode + MemoryBuffer + ?Sized),
    ) -> Result<()> {
        self.send_delete_batch(batch)?;
        batch.clear();
        Ok(())
    }

    /// Returns the number of requests successfully accepted by the stream.
    ///
    /// A multi-table request counts as one batch. This is not an acknowledgement
    /// that the server has committed the requests; only `finish` provides that.
    #[must_use]
    pub fn batches_sent(&self) -> usize {
        self.batches_sent
    }

    /// Returns the total row count in successfully sent insert and delete frames.
    ///
    /// Server-side affected rows may differ from this count.
    #[must_use]
    pub fn rows_sent(&self) -> usize {
        self.rows_sent
    }

    /// Half-closes the request stream and validates its single terminal response.
    ///
    /// # Errors
    /// Returns transport, gRPC trailer, GreptimeDB application-header or missing
    /// affected-row errors. The consumed stream is cancelled on failed completion.
    pub fn finish(mut self) -> Result<u32> {
        self.finished = true;
        let mut call = self
            .call
            .take()
            .ok_or_else(|| Error::Stream("ingestion stream is closed".into()))?;
        let result = (|| {
            let response = call.finish().map_err(|error| call_error(&call, error))?;
            let trailers = call
                .trailing_metadata()
                .map_err(|error| call_error(&call, error))?;
            validate_ok_trailers(trailers.as_ref())?;
            Error::from_response_header(response.header.as_ref())?;
            match response.response {
                Some(greptime_response::Response::AffectedRows(rows)) => Ok(rows.value),
                None => Err(Error::Protocol(
                    "GreptimeDatabase returned no affected-row result".into(),
                )),
            }
        })();
        match &result {
            Ok(_) => {
                if let Some(feedback) = &mut self.feedback {
                    feedback.report(true);
                }
            }
            Err(error) => self.report_error(error),
        }
        result
    }

    fn report_error(&mut self, error: &Error) {
        if terminal_fault(error)
            && let Some(feedback) = &mut self.feedback
        {
            feedback.report(false);
        }
    }

    /// Cancels the stream without waiting for the server's final response.
    ///
    /// # Errors
    /// Returns stream-control or cancellation failures.
    pub fn cancel(mut self) -> Result<()> {
        self.finished = true;
        let mut call = self
            .call
            .take()
            .ok_or_else(|| Error::Stream("ingestion stream is closed".into()))?;
        call.cancel().map_err(|error| call_error(&call, error))
    }
}

impl Client {
    /// Opens an ingestion stream using this client's defaults.
    ///
    /// # Errors
    /// Returns endpoint-index, connection, configuration or stream-submission failures.
    pub fn stream_inserter(&mut self) -> Result<StreamInserter> {
        self.open_ingestion(None)
    }

    /// Opens ingestion with per-stream hints and bounded pre-send failover.
    ///
    /// # Errors
    /// Returns connection, admission-probe, or stream-submission failures.
    pub fn stream_inserter_with_hints(&mut self, hints: &WriteHints) -> Result<StreamInserter> {
        self.open_ingestion(Some(hints))
    }

    fn open_ingestion(&mut self, hints: Option<&WriteHints>) -> Result<StreamInserter> {
        self.options.try_build_call_options(hints)?;
        self.reconcile_stream_outcomes();
        let mut attempted = Vec::new();
        let mut last_error = None;
        for _ in 0..self.budget() {
            let index = match self.select(&attempted) {
                Ok(index) => index,
                Err(error) => return Err(last_error.unwrap_or(error)),
            };
            attempted.push(index);
            if let Err(error) = self.admit(index, true) {
                last_error = Some(error);
                continue;
            }
            let channel = channel_for(&mut self.channels, &self.options, index)?;
            match StreamInserter::open(channel, &self.options, hints) {
                Ok(mut stream) => {
                    stream.feedback = Some(self.stream_feedback(index));
                    return Ok(stream);
                }
                Err(error) => {
                    let retry = terminal_fault(&error);
                    self.attempt_failure(index, &error);
                    last_error = Some(error);
                    if !retry {
                        break;
                    }
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| Error::Protocol("no stream endpoint attempt was made".into())))
    }
}

impl Drop for StreamInserter {
    fn drop(&mut self) {
        if !self.finished
            && let Some(mut call) = self.call.take()
        {
            // Drop cannot return an error; the call retains cancellation cleanup.
            drop(call.cancel());
        }
    }
}

fn call_error(call: &StreamingCall, error: grpc::StreamError) -> Error {
    let status = match error {
        grpc::StreamError::Status(status) => status,
        grpc::StreamError::WouldBlock => {
            grpc::Status::internal("blocking ingestion stream returned pending")
        }
    };
    match call.trailing_metadata() {
        Ok(trailers) => Error::from_grpc_status_and_trailers(status, trailers.as_ref()),
        Err(_) => Error::Grpc(status),
    }
}

fn request_rows(request: &greptime_request::Request) -> Option<usize> {
    use greptime_request::Request;
    match request {
        Request::Inserts(request) => request.inserts.iter().try_fold(0usize, |sum, insert| {
            sum.checked_add(insert.row_count as usize)
        }),
        Request::Deletes(request) => request.deletes.iter().try_fold(0usize, |sum, delete| {
            sum.checked_add(delete.row_count as usize)
        }),
        Request::RowInserts(request) => request.inserts.iter().try_fold(0usize, |sum, insert| {
            sum.checked_add(insert.rows.as_ref().map_or(0, |rows| rows.rows.len()))
        }),
        Request::RowDeletes(request) => request.deletes.iter().try_fold(0usize, |sum, delete| {
            sum.checked_add(delete.rows.as_ref().map_or(0, |rows| rows.rows.len()))
        }),
        Request::Query(_) | Request::Ddl(_) => Some(0),
    }
}
