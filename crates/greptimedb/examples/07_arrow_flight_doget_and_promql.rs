//! SQL, Arrow dictionaries, DML metadata, and PromQL over synchronous HTTP/2.
//!
//! Offline: `cargo run -p greptimedb --example 07_arrow_flight_doget_and_promql`.
//! Live: `GREPTIMEDB_ENDPOINT=grpc://localhost:4001 cargo run -p greptimedb
//! --example 07_arrow_flight_doget_and_promql`.
//!
//! The offline server emits actual Zstd-compressed IPC bodies. This SDK has no
//! `QueryOptions`: compression is chosen by the Flight producer, not negotiated
//! by `QueryStream`. Its range method is named `prom_range_query`; the unary
//! gateway uses `PrometheusGateway/Handle` with a `PromqlRequest::RangeQuery`.

use std::{collections::HashMap, env, sync::Arc, thread, time::Duration};

use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, DictionaryArray, Float64Array, RecordBatch, TimestampMillisecondArray, types::Int32Type,
};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use greptimedb::{
    Client, ClientOptions, Endpoint, EndpointTransport, Row,
    proto::{
        flight::{FlightData, Ticket},
        v1::{
            AffectedRows, FlightMetadata, GreptimeRequest, Metrics, PromqlRequest, PromqlResponse,
            greptime_request, promql_request, query_request,
        },
    },
    query::{QueryStream, decode_record_batch_to_columnar_batch},
};
use grpc::{
    ChannelConfig, ProstCodec, Response, Status, StreamError,
    core::transport::{MemoryPipe, SyncTransport},
    server::SyncServer,
};
use prost::Message;

const BUDGET: Duration = Duration::from_secs(5);
const DO_GET: &str = "/arrow.flight.protocol.FlightService/DoGet";
const PROM: &str = "/greptime.v1.PrometheusGateway/Handle";

#[derive(Debug, Row)]
struct TaggedLatency {
    #[greptime(timestamp, unit = "ms")]
    ts: i64,
    #[greptime(tag)]
    service: String,
    avg_latency: f64,
}

fn main() -> Result<()> {
    let options = ClientOptions::from_endpoint(Endpoint::parse("localhost:4001")?);
    let (mut pipe, mut peer) = MemoryPipe::pair();
    for socket in [&mut pipe, &mut peer] {
        socket.set_read_timeout(Some(BUDGET))?;
        socket.set_write_timeout(Some(BUDGET))?;
    }
    // The server owns its endpoint; all SDK streams stay on this caller thread.
    let server = thread::spawn(move || serve(peer));
    let offline = (|| -> Result<()> {
        let channel = EndpointTransport::from_memory_pipe(pipe).into_channel(ChannelConfig {
            default_timeout: Some(BUDGET),
            ..ChannelConfig::default()
        })?;
        let mut client = Client::from_channel(options, channel);
        query_examples(&mut client)?;
        let response = client.prom_range_query("up", "0", "60", "15s", "5m")?;
        let json: sonic_rs::Value = sonic_rs::from_str(response.as_json_str()?)?;
        ensure!(json["status"] == "success", "gateway rejected query");
        println!("Unary PromQL: {}", json["data"]["resultType"]);
        Ok(())
    })();
    let server_result = server
        .join()
        .map_err(|_| anyhow::anyhow!("mock server panicked"))?;
    offline?;
    server_result?;
    match env::var("GREPTIMEDB_ENDPOINT") {
        Ok(endpoint) if !endpoint.is_empty() => live(&endpoint)?,
        Ok(_) | Err(env::VarError::NotPresent) => {
            println!("Offline checks passed; set GREPTIMEDB_ENDPOINT for live queries.");
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn query_examples(client: &mut Client) -> Result<()> {
    let options = client.options().clone();
    let mut query = QueryStream::sql(client.channel_mut()?, &options, "SELECT avg_latency")?;
    ensure!(
        query.schema().is_none(),
        "schema arrives with the first frame"
    );
    let mut seen = 0;
    while let Some(batch) = query.next_batch()? {
        let schema = query.schema().context("server omitted schema")?;
        ensure!(schema.fields().len() == 1, "projection schema differs");
        let columnar = decode_record_batch_to_columnar_batch(&batch)?;
        ensure!(columnar.columns().len() == 1, "projection gained columns");
        ensure!(
            columnar.columns()[0].row_count() == 2,
            "projection lost rows"
        );
        seen += batch.num_rows();
        println!("Timestamp-less projection: {} rows", batch.num_rows());
    }
    ensure!(seen == 4, "multi-batch SQL query lost rows");
    let (_, metrics) = query.finish()?;
    ensure!(metrics.len() == 1, "missing SQL metrics");

    let mut tagged = QueryStream::sql(
        client.channel_mut()?,
        &options,
        "SELECT service, ts, avg_latency",
    )?;
    let batch = tagged.next_batch()?.context("missing dictionary batch")?;
    ensure!(
        matches!(batch.column(0).data_type(), DataType::Dictionary(_, _)),
        "dictionary was lost"
    );
    let columnar = decode_record_batch_to_columnar_batch(&batch)?;
    let typed = TaggedLatencyBatch::from_record_batch(&batch)?;
    ensure!(
        columnar.columns().len() == 3 && typed.len() == 2,
        "dictionary decode failed"
    );
    println!("Dictionary tags: {:?}", typed.to_owned_rows());
    tagged.finish()?;

    let range = QueryStream::prom_range_query(
        client.channel_mut()?,
        &options,
        "up",
        "0",
        "60",
        "15s",
        "5m",
    )?;
    let batches = range.collect_batches()?;
    ensure!(batches.len() == 1, "Flight PromQL result is missing");

    let dml = QueryStream::sql(
        client.channel_mut()?,
        &options,
        "DELETE FROM example WHERE ts < 0",
    )?;
    let (affected_rows, metrics) = dml.finish()?;
    ensure!(
        affected_rows == Some(3) && metrics.len() == 1,
        "DML metadata differs"
    );
    println!(
        "DML affected rows: {affected_rows:?}; metrics: {}",
        String::from_utf8_lossy(&metrics[0])
    );
    Ok(())
}

fn serve(peer: MemoryPipe) -> Result<(), Status> {
    let mut server = SyncServer::new(peer)?;
    server.register_server_streaming(
        DO_GET,
        ProstCodec::<FlightData, Ticket>::new(),
        |request, sink| {
            let request = GreptimeRequest::decode(request.get_ref().ticket.as_slice())
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let Some(greptime_request::Request::Query(query)) = request.request else {
                return Err(Status::invalid_argument("expected a query ticket"));
            };
            let (dictionary, dml, copies) = match query.query {
                Some(query_request::Query::Sql(sql)) if sql.starts_with("DELETE") => {
                    (false, true, 0)
                }
                Some(query_request::Query::Sql(sql)) => (sql.contains("service"), false, 2),
                Some(query_request::Query::PromRangeQuery(range)) if range.query == "up" => {
                    (false, false, 1)
                }
                _ => return Err(Status::invalid_argument("unsupported example query")),
            };
            let mut frames = if dml {
                Vec::new()
            } else {
                flight_frames(dictionary, if dictionary { 1 } else { copies })
                    .map_err(|error| Status::internal(error.to_string()))?
            };
            frames.push(metadata_frame(dml));
            for frame in frames {
                sink.send(&frame).map_err(stream_status)?;
            }
            Ok(())
        },
    )?;
    server.register_unary(PROM, ProstCodec::<PromqlResponse, PromqlRequest>::new(), |request| {
        if !matches!(request.get_ref().promql, Some(promql_request::Promql::RangeQuery(_))) {
            return Err(Status::invalid_argument("expected a unary range query"));
        }
        Ok(Response::new(PromqlResponse {
            header: None,
            body: br#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"example"},"values":[[0,"1"],[15,"1"]]}]}}"#.to_vec(),
        }))
    })?;
    server.serve_connection()
}

fn flight_frames(dictionary: bool, copies: usize) -> Result<Vec<FlightData>> {
    let mut fields = Vec::new();
    let mut arrays: Vec<arrow_array::ArrayRef> = Vec::new();
    if dictionary {
        let tags = DictionaryArray::<Int32Type>::from_iter(["frontend", "backend"]);
        fields.push(
            Field::new("service", tags.data_type().clone(), false).with_metadata(HashMap::from([
                ("greptime:semantic_type".into(), "tag".into()),
            ])),
        );
        arrays.push(Arc::new(tags));
        fields.push(
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            )
            .with_metadata(HashMap::from([(
                "greptime:time_index".into(),
                "true".into(),
            )])),
        );
        arrays.push(Arc::new(TimestampMillisecondArray::from(vec![0, 15_000])));
    }
    fields.push(Field::new("avg_latency", DataType::Float64, false));
    arrays.push(Arc::new(Float64Array::from(vec![1.25, 2.5])));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    let generator = IpcDataGenerator::default();
    let options =
        IpcWriteOptions::default().try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))?;
    let mut tracker = DictionaryTracker::new(false);
    let schema = generator.schema_to_bytes_with_dictionary_tracker(
        batch.schema().as_ref(),
        &mut tracker,
        &options,
    );
    let frame = |data: arrow_ipc::writer::EncodedData| FlightData {
        data_header: data.ipc_message,
        data_body: data.arrow_data,
        ..FlightData::default()
    };
    let mut frames = vec![frame(schema)];
    for _ in 0..copies {
        let (dictionaries, record) = generator.encode(
            &batch,
            &mut tracker,
            &options,
            &mut IpcWriteContext::default(),
        )?;
        frames.extend(dictionaries.into_iter().map(frame));
        frames.push(frame(record));
    }
    Ok(frames)
}

fn metadata_frame(dml: bool) -> FlightData {
    FlightData {
        // FlatBuffers IPC Message: V5, NONE header, zero body length.
        data_header: vec![12, 0, 0, 0, 6, 0, 8, 0, 6, 0, 0, 0, 8, 0, 0, 0, 0, 0, 4, 0],
        app_metadata: FlightMetadata {
            affected_rows: dml.then_some(AffectedRows { value: 3 }),
            metrics: Some(Metrics {
                metrics: br#"{"elapsed_compute":42}"#.to_vec(),
            }),
        }
        .encode_to_vec(),
        ..FlightData::default()
    }
}

fn stream_status(error: StreamError) -> Status {
    match error {
        StreamError::Status(status) => status,
        StreamError::WouldBlock => Status::internal("blocking mock operation would block"),
    }
}

fn live(endpoint: &str) -> Result<()> {
    let mut client = Client::connect(ClientOptions::from_endpoint(Endpoint::parse(endpoint)?))?;
    // These read-only expressions do not require tables or mutate live data.
    let mut query = client.query("SELECT 1.25 AS avg_latency")?;
    while let Some(batch) = query.next_batch()? {
        println!("Live SQL: {} rows", batch.num_rows());
        let projection = decode_record_batch_to_columnar_batch(&batch)?;
        ensure!(
            projection.columns().len() == 1,
            "unexpected live projection"
        );
    }
    query.finish()?;
    let json = client.prom_range_query("up", "0", "60", "15s", "5m")?;
    println!("Live PromQL: {}", json.as_json_str()?);
    Ok(())
}
