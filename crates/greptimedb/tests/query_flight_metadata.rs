//! GreptimeDB Flight extension frames through synchronous HTTP/2.

use std::{sync::Arc, thread, time::Duration};

use arrow_array::{Int32Array, RecordBatch};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use greptimedb::{
    ClientOptions, Endpoint,
    bulk::BulkStreamWriter,
    proto::{
        flight::{FlightData, Ticket},
        v1::{
            AffectedRows, FlightMetadata, GreptimeRequest, Metrics, greptime_request, query_request,
        },
    },
    query::QueryStream,
    transport::EndpointTransport,
};
use grpc::{
    ChannelConfig, Status, StreamError, SyncChannel, core::transport::MemoryPipe,
    server::SyncServer,
};
use prost::Message;

type TestResult = anyhow::Result<()>;
type Channel = SyncChannel<EndpointTransport>;

struct Fixture {
    channel: Option<Channel>,
    worker: Option<thread::JoinHandle<Result<(), Status>>>,
}

impl Fixture {
    fn new(frames: Vec<FlightData>) -> anyhow::Result<Self> {
        let (client, peer) = MemoryPipe::with_capacity(4096)?;
        let worker = thread::spawn(move || {
            let mut server = SyncServer::new(peer)?;
            server.register_server_streaming(
                "/arrow.flight.protocol.FlightService/DoGet",
                grpc::ProstCodec::<FlightData, Ticket>::new(),
                move |request, sink| {
                    let request = GreptimeRequest::decode(request.get_ref().ticket.as_slice())
                        .map_err(|error| Status::invalid_argument(error.to_string()))?;
                    match request.request {
                        Some(greptime_request::Request::Query(query))
                            if matches!(query.query, Some(query_request::Query::Sql(ref sql)) if sql == "select value") => {}
                        _ => return Err(Status::invalid_argument("unexpected query ticket")),
                    }
                    for frame in &frames {
                        sink.send(frame).map_err(|error| match error {
                            StreamError::Status(status) => status,
                            StreamError::WouldBlock => Status::internal("unexpected pending send"),
                        })?;
                    }
                    Ok(())
                },
            )?;
            server.serve_connection()
        });
        let channel = EndpointTransport::from_memory_pipe(client).into_channel(ChannelConfig {
            default_timeout: Some(Duration::from_secs(3)),
            ..ChannelConfig::default()
        })?;
        Ok(Self {
            channel: Some(channel),
            worker: Some(worker),
        })
    }

    fn query(&mut self) -> anyhow::Result<QueryStream> {
        let options = ClientOptions::from_endpoint(Endpoint::parse("localhost:4001")?);
        let channel = self
            .channel
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("closed fixture"))?;
        Ok(QueryStream::sql(channel, &options, "select value")?)
    }

    fn finish(mut self) -> TestResult {
        drop(self.channel.take());
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("server panicked"))??;
        }
        Ok(())
    }

    fn finish_aborted(mut self) -> TestResult {
        drop(self.channel.take());
        if let Some(worker) = self.worker.take() {
            match worker
                .join()
                .map_err(|_| anyhow::anyhow!("server panicked"))?
            {
                Ok(()) => {}
                Err(status) if status.code() == grpc::core::Code::Unavailable => {}
                Err(status) => return Err(status.into()),
            }
        }
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.channel.take());
        if let Some(worker) = self.worker.take() {
            drop(worker.join());
        }
    }
}

// FlatBuffers Message { version: V5, header_type: NONE, bodyLength: 0 }.
fn metadata_frame(bytes: Vec<u8>) -> FlightData {
    FlightData {
        data_header: vec![12, 0, 0, 0, 6, 0, 8, 0, 6, 0, 0, 0, 8, 0, 0, 0, 0, 0, 4, 0],
        app_metadata: bytes,
        ..FlightData::default()
    }
}

fn record_frames() -> anyhow::Result<(RecordBatch, Vec<FlightData>)> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![7, 11]))])?;
    let generator = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    let schema = generator.schema_to_bytes_with_dictionary_tracker(
        batch.schema().as_ref(),
        &mut tracker,
        &options,
    );
    let (dictionaries, data) = generator.encode(
        &batch,
        &mut tracker,
        &options,
        &mut IpcWriteContext::default(),
    )?;
    assert!(dictionaries.is_empty());
    let frame = |data: arrow_ipc::writer::EncodedData| FlightData {
        data_header: data.ipc_message,
        data_body: data.arrow_data,
        ..FlightData::default()
    };
    Ok((batch, vec![frame(schema), frame(data)]))
}

#[test]
fn metadata_before_schema_and_after_record_does_not_truncate_rows() -> TestResult {
    let (expected, encoded) = record_frames()?;
    let mut frames = vec![metadata_frame(
        FlightMetadata {
            affected_rows: Some(AffectedRows { value: 4 }),
            metrics: Some(Metrics {
                metrics: b"initial".to_vec(),
            }),
        }
        .encode_to_vec(),
    )];
    frames.extend(encoded);
    frames.push(metadata_frame(
        FlightMetadata {
            affected_rows: None,
            metrics: Some(Metrics {
                metrics: b"terminal".to_vec(),
            }),
        }
        .encode_to_vec(),
    ));
    let mut fixture = Fixture::new(frames)?;
    let mut query = fixture.query()?;
    assert_eq!(query.next_batch()?, Some(expected));
    assert!(query.next_batch()?.is_none());
    assert!(query.next_batch()?.is_none());
    assert_eq!(query.affected_rows(), 4);
    assert_eq!(
        query.metrics(),
        &[b"initial".to_vec(), b"terminal".to_vec()]
    );
    drop(query);
    fixture.finish()
}

#[test]
fn metadata_only_affected_rows_rpc_completes_without_a_schema() -> TestResult {
    let mut fixture = Fixture::new(vec![metadata_frame(
        FlightMetadata {
            affected_rows: Some(AffectedRows { value: 9 }),
            metrics: None,
        }
        .encode_to_vec(),
    )])?;
    let mut query = fixture.query()?;
    assert!(query.next_batch()?.is_none());
    assert_eq!(query.affected_rows(), 9);
    drop(query);
    fixture.finish()
}

#[test]
fn malformed_empty_and_unknown_metadata_fail_and_poison_query() -> TestResult {
    for bytes in [vec![0xff], Vec::new(), vec![0x1a, 0]] {
        let mut fixture = Fixture::new(vec![metadata_frame(bytes)])?;
        let mut query = fixture.query()?;
        assert!(query.next_batch().is_err());
        assert!(query.next_batch().is_err());
        assert_eq!(query.affected_rows(), 0);
        assert!(query.metrics().is_empty());
        drop(query);
        fixture.finish_aborted()?;
    }
    Ok(())
}

#[test]
fn flight_handles_are_neither_send_nor_sync() {
    macro_rules! assert_not_impl {
        ($ty:ty, $bound:path) => {{
            trait AmbiguousIfImpl<A> {
                fn probe() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
            struct Invalid;
            impl<T: ?Sized + $bound> AmbiguousIfImpl<Invalid> for T {}
            let _probe = <$ty as AmbiguousIfImpl<_>>::probe;
        }};
    }
    assert_not_impl!(BulkStreamWriter, Send);
    assert_not_impl!(BulkStreamWriter, Sync);
    assert_not_impl!(QueryStream, Send);
    assert_not_impl!(QueryStream, Sync);
}
