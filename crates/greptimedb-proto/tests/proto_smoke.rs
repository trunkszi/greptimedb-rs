//! Integration smoke tests for the generated GreptimeDB and Arrow Flight bindings.
//!
//! These tests exercise the public `greptimedb-proto` surface through the same
//! `grpc` codec and channel types the SDK will use: Prost wire compatibility,
//! generated method descriptors, one real unary RPC over an in-memory pipe, and
//! the hand-written `Value` conversion.

use std::{thread, time::Duration};

use greptimedb_proto::v1::health_check::HealthCheck as _;
use greptimedb_proto::{flight, io as pb_io, prometheus, substrait_extension, v1};
use grpc::{
    Codec, MethodDescriptor, ProstCodec, Request, Response, RpcCardinality, Status,
    channel::{CallOptions, ChannelConfig, SyncChannel},
    core::transport::MemoryPipe,
    server::SyncServer,
};
use prost::Message;

/// Encodes with `ProstCodec`, checks the bytes against prost's own encoder,
/// decodes with both implementations, and returns the `ProstCodec` decode.
fn assert_codec_round_trip<M>(message: &M) -> M
where
    M: Message + Default + PartialEq,
{
    let mut codec = ProstCodec::<M, M>::new();
    let mut wire = Vec::new();
    codec
        .encode(message, &mut wire)
        .expect("ProstCodec must encode a valid message");
    assert_eq!(
        wire,
        message.encode_to_vec(),
        "ProstCodec and protobuf::Message must emit identical bytes"
    );
    let decoded = codec
        .decode(&wire)
        .expect("ProstCodec must decode bytes it produced");
    assert!(
        decoded == *message,
        "ProstCodec round trip must preserve the message"
    );
    let prost_decoded = M::decode(wire.as_slice()).expect("prost must decode the same bytes");
    assert!(
        prost_decoded == *message,
        "prost decode and ProstCodec decode must agree"
    );
    decoded
}

fn greptime_header() -> v1::RequestHeader {
    v1::RequestHeader {
        catalog: "greptime".to_owned(),
        schema: "public".to_owned(),
        authorization: Some(v1::AuthHeader {
            auth_scheme: Some(v1::auth_header::AuthScheme::Basic(v1::Basic {
                username: "reader".to_owned(),
                password: "secret".to_owned(),
            })),
        }),
        dbname: "greptime-public".to_owned(),
        timezone: "UTC".to_owned(),
        ..Default::default()
    }
}

#[test]
fn prost_codec_round_trips_greptime_insert_and_query_requests() {
    let insert_request = v1::GreptimeRequest {
        header: Some(greptime_header()),
        request: Some(v1::greptime_request::Request::Inserts(v1::InsertRequests {
            inserts: vec![v1::InsertRequest {
                table_name: "ticks".to_owned(),
                columns: vec![
                    v1::Column {
                        column_name: "ts".to_owned(),
                        // TIMESTAMP = 2 in greptime/v1/common.proto.
                        semantic_type: 2,
                        values: Some(v1::column::Values {
                            timestamp_millisecond_values: vec![
                                1_700_000_000_000,
                                1_700_000_000_002,
                            ],
                            ..Default::default()
                        }),
                        // Row 1 is NULL; values hold only non-null entries.
                        null_mask: vec![0b0000_0010],
                        // TIMESTAMP_MILLISECOND = 16 in common.proto.
                        datatype: 16,
                        ..Default::default()
                    },
                    v1::Column {
                        column_name: "price".to_owned(),
                        // FIELD = 1 in common.proto.
                        semantic_type: 1,
                        values: Some(v1::column::Values {
                            f64_values: vec![101.5, 101.75, 102.0],
                            ..Default::default()
                        }),
                        // FLOAT64 = 10 in common.proto.
                        datatype: 10,
                        ..Default::default()
                    },
                ],
                row_count: 3,
            }],
        })),
    };

    let decoded = assert_codec_round_trip(&insert_request);
    let header = decoded
        .header
        .expect("the decoded request must keep its header");
    assert_eq!(header.catalog, "greptime");
    assert_eq!(header.schema, "public");
    assert_eq!(header.dbname, "greptime-public");
    assert_eq!(header.timezone, "UTC");
    match header.authorization {
        Some(v1::AuthHeader {
            auth_scheme: Some(v1::auth_header::AuthScheme::Basic(basic)),
        }) => {
            assert_eq!(basic.username, "reader");
            assert_eq!(basic.password, "secret");
        }
        _ => panic!("the decoded header must carry the basic auth scheme"),
    }

    match decoded.request {
        Some(v1::greptime_request::Request::Inserts(inserts)) => {
            assert_eq!(inserts.inserts.len(), 1);
            let insert = &inserts.inserts[0];
            assert_eq!(insert.table_name, "ticks");
            assert_eq!(insert.row_count, 3);
            assert_eq!(insert.columns.len(), 2);
            assert_eq!(insert.columns[0].column_name, "ts");
            assert_eq!(insert.columns[0].semantic_type, 2);
            assert_eq!(insert.columns[0].datatype, 16);
            assert_eq!(insert.columns[0].null_mask, vec![0b0000_0010]);
            let values = insert.columns[0]
                .values
                .as_ref()
                .expect("the timestamp column must keep its values");
            assert_eq!(
                values.timestamp_millisecond_values,
                vec![1_700_000_000_000, 1_700_000_000_002]
            );
            let null_count: u32 = insert.columns[0]
                .null_mask
                .iter()
                .map(|byte| byte.count_ones())
                .sum();
            assert_eq!(
                u32::try_from(values.timestamp_millisecond_values.len())
                    .expect("column length fits u32")
                    + null_count,
                insert.row_count,
                "values_count + popcnt(null_mask) must equal row_count"
            );
            let price_values = insert.columns[1]
                .values
                .as_ref()
                .expect("the price column must keep its values");
            assert_eq!(price_values.f64_values, vec![101.5, 101.75, 102.0]);
        }
        _ => panic!("the decoded request must carry the insert oneof"),
    }

    let query_request = v1::GreptimeRequest {
        header: Some(greptime_header()),
        request: Some(v1::greptime_request::Request::Query(v1::QueryRequest {
            query: Some(v1::query_request::Query::Sql(
                "SELECT ts, price FROM ticks ORDER BY ts".to_owned(),
            )),
        })),
    };
    let decoded = assert_codec_round_trip(&query_request);
    match decoded.request {
        Some(v1::greptime_request::Request::Query(query)) => {
            assert_eq!(
                query.query,
                Some(v1::query_request::Query::Sql(
                    "SELECT ts, price FROM ticks ORDER BY ts".to_owned()
                ))
            );
        }
        _ => panic!("the decoded request must carry the query oneof"),
    }
}

#[test]
fn prost_codec_round_trips_empty_health_messages() {
    assert_codec_round_trip(&v1::HealthCheckRequest {});
    assert_codec_round_trip(&v1::HealthCheckResponse {});
    assert!(
        v1::HealthCheckRequest {}.encode_to_vec().is_empty(),
        "an empty protobuf message has an empty payload"
    );
    assert!(
        ProstCodec::<v1::HealthCheckResponse, v1::HealthCheckRequest>::new()
            .decode(&[])
            .is_ok(),
        "an empty payload decodes into the empty request message"
    );
}

#[test]
fn prost_codec_round_trips_meta_request_header() {
    let header = v1::meta::RequestHeader {
        protocol_version: 1,
        member_id: 42,
        // FRONTEND = 1 in greptime/v1/meta/common.proto.
        role: 1,
        ..Default::default()
    };
    let decoded = assert_codec_round_trip(&header);
    assert_eq!(decoded.protocol_version, 1);
    assert_eq!(decoded.member_id, 42);
    assert_eq!(decoded.role, 1);
}

#[test]
fn prost_codec_round_trips_region_request_oneof() {
    let request = v1::region::RegionRequest {
        header: Some(v1::region::RegionRequestHeader {
            dbname: "greptime-public".to_owned(),
            ..Default::default()
        }),
        body: Some(v1::region::region_request::Body::Create(
            v1::region::CreateRequest {
                region_id: 4_096,
                engine: "mito".to_owned(),
                ..Default::default()
            },
        )),
    };
    let decoded = assert_codec_round_trip(&request);
    let header = decoded
        .header
        .expect("the decoded region request must keep its header");
    assert_eq!(header.dbname, "greptime-public");
    match decoded.body {
        Some(v1::region::region_request::Body::Create(create)) => {
            assert_eq!(create.region_id, 4_096);
            assert_eq!(create.engine, "mito");
        }
        _ => panic!("the decoded region request must carry the create body"),
    }
}

#[test]
fn prost_codec_round_trips_inverted_index_meta() {
    let meta = v1::index::InvertedIndexMeta {
        name: "host".to_owned(),
        base_offset: 128,
        inverted_index_size: 4_096,
        relative_fst_offset: 64,
        fst_size: 512,
        relative_null_bitmap_offset: 0,
        null_bitmap_size: 8,
        stats: Some(v1::index::InvertedIndexStats {
            null_count: 3,
            distinct_count: 17,
            min_value: b"alpha".to_vec(),
            max_value: b"omega".to_vec(),
        }),
        // Roaring = 1 in greptime/v1/index/inverted_index.proto.
        bitmap_type: 1,
    };
    let decoded = assert_codec_round_trip(&meta);
    assert_eq!(decoded.name, "host");
    assert_eq!(decoded.base_offset, 128);
    assert_eq!(decoded.inverted_index_size, 4_096);
    assert_eq!(decoded.relative_fst_offset, 64);
    assert_eq!(decoded.fst_size, 512);
    assert_eq!(decoded.null_bitmap_size, 8);
    assert_eq!(decoded.bitmap_type, 1);
    let stats = decoded.stats.expect("the decoded meta must keep its stats");
    assert_eq!(stats.null_count, 3);
    assert_eq!(stats.distinct_count, 17);
    assert_eq!(stats.min_value, b"alpha");
    assert_eq!(stats.max_value, b"omega");
}

#[test]
fn prost_codec_round_trips_prometheus_remote_write_request() {
    let request = prometheus::remote::WriteRequest {
        timeseries: vec![prometheus::remote::TimeSeries {
            labels: vec![
                prometheus::remote::Label {
                    name: "__name__".to_owned(),
                    value: "http_requests_total".to_owned(),
                },
                prometheus::remote::Label {
                    name: "job".to_owned(),
                    value: "api".to_owned(),
                },
            ],
            samples: vec![prometheus::remote::Sample {
                value: 42.5,
                timestamp: 1_700_000_000_000,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let decoded = assert_codec_round_trip(&request);
    assert_eq!(decoded.timeseries.len(), 1);
    let series = &decoded.timeseries[0];
    assert_eq!(series.labels.len(), 2);
    assert_eq!(series.labels[0].name, "__name__");
    assert_eq!(series.labels[0].value, "http_requests_total");
    assert_eq!(series.labels[1].name, "job");
    assert_eq!(series.labels[1].value, "api");
    assert_eq!(series.samples.len(), 1);
    assert_eq!(series.samples[0].value, 42.5);
    assert_eq!(series.samples[0].timestamp, 1_700_000_000_000);
}

#[test]
fn prost_codec_round_trips_prometheus_v2_request() {
    let request = pb_io::prometheus::write::v2::Request {
        symbols: vec![
            String::new(),
            "__name__".to_owned(),
            "http_requests_total".to_owned(),
        ],
        timeseries: vec![pb_io::prometheus::write::v2::TimeSeries {
            labels_refs: vec![1, 2],
            samples: vec![pb_io::prometheus::write::v2::Sample {
                value: 1.25,
                timestamp: 1_700_000_000_000,
                start_timestamp: 1_699_999_999_000,
            }],
            ..Default::default()
        }],
    };
    let decoded = assert_codec_round_trip(&request);
    assert_eq!(
        decoded.symbols,
        vec![
            String::new(),
            "__name__".to_owned(),
            "http_requests_total".to_owned()
        ]
    );
    assert_eq!(decoded.timeseries.len(), 1);
    assert_eq!(decoded.timeseries[0].labels_refs, vec![1, 2]);
    assert_eq!(decoded.timeseries[0].samples[0].value, 1.25);
    assert_eq!(
        decoded.timeseries[0].samples[0].timestamp,
        1_700_000_000_000
    );
    assert_eq!(
        decoded.timeseries[0].samples[0].start_timestamp,
        1_699_999_999_000
    );
}

#[test]
fn prost_codec_round_trips_promql_service_messages() {
    let request = v1::PromqlRequest {
        header: Some(greptime_header()),
        promql: Some(v1::promql_request::Promql::InstantQuery(
            v1::PromInstantQuery {
                query: "rate(http_requests_total[5m])".to_owned(),
                time: "1700000000".to_owned(),
                lookback: "5m".to_owned(),
            },
        )),
    };
    let decoded = assert_codec_round_trip(&request);
    match decoded.promql {
        Some(v1::promql_request::Promql::InstantQuery(query)) => {
            assert_eq!(query.query, "rate(http_requests_total[5m])");
            assert_eq!(query.time, "1700000000");
            assert_eq!(query.lookback, "5m");
        }
        _ => panic!("the decoded PromQL request must carry the instant query"),
    }

    let response = v1::PromqlResponse {
        header: None,
        body: br#"{"status":"success","data":{}}"#.to_vec(),
    };
    let decoded = assert_codec_round_trip(&response);
    assert!(decoded.header.is_none());
    assert_eq!(decoded.body, br#"{"status":"success","data":{}}"#);
}

#[test]
fn prost_codec_round_trips_substrait_instant_manipulate() {
    let plan = substrait_extension::InstantManipulate {
        start: 1_700_000_000_000,
        end: 1_700_000_060_000,
        interval: 60_000,
        lookback_delta: 300_000,
        time_index_idx: 0,
        field_index_idx: 1,
        ..Default::default()
    };
    let decoded = assert_codec_round_trip(&plan);
    assert_eq!(decoded.start, 1_700_000_000_000);
    assert_eq!(decoded.end, 1_700_000_060_000);
    assert_eq!(decoded.interval, 60_000);
    assert_eq!(decoded.lookback_delta, 300_000);
    assert_eq!(decoded.time_index_idx, 0);
    assert_eq!(decoded.field_index_idx, 1);
}

#[test]
fn prost_codec_round_trips_flight_data_with_large_body() {
    let data_body = vec![0x5a_u8; 65_536];
    let flight_data = flight::FlightData {
        flight_descriptor: Some(flight::FlightDescriptor {
            path: vec!["ticks".to_owned()],
            ..Default::default()
        }),
        data_header: vec![0xff, 0xfb, 0x00, 0x00],
        app_metadata: vec![0x01, 0x02],
        data_body: data_body.clone(),
    };
    let decoded = assert_codec_round_trip(&flight_data);
    let descriptor = decoded
        .flight_descriptor
        .expect("the decoded FlightData must keep its descriptor");
    assert_eq!(descriptor.path, vec!["ticks".to_owned()]);
    assert_eq!(decoded.data_header, vec![0xff, 0xfb, 0x00, 0x00]);
    assert_eq!(decoded.app_metadata, vec![0x01, 0x02]);
    assert_eq!(decoded.data_body.len(), 65_536);

    // data_body is field 1000, so its tag is (1000 << 3) | 2 = 8002, encoded
    // as the varint [0xc2, 0x3e]; a 65,536-byte payload adds [0x80, 0x80, 0x04].
    let wire = flight_data.encode_to_vec();
    let body_start = wire.len() - data_body.len();
    assert_eq!(
        &wire[body_start - 5..body_start],
        &[0xc2, 0x3e, 0x80, 0x80, 0x04],
        "data_body must use field number 1000 with a three-byte length varint"
    );
    assert_eq!(&wire[body_start..], data_body.as_slice());

    let ticket = flight::Ticket {
        ticket: b"greptime-query".to_vec(),
    };
    let decoded_ticket = assert_codec_round_trip(&ticket);
    assert_eq!(decoded_ticket.ticket, b"greptime-query");

    let put_result = flight::PutResult {
        app_metadata: vec![0xde, 0xad, 0xbe, 0xef],
    };
    let decoded_result = assert_codec_round_trip(&put_result);
    assert_eq!(decoded_result.app_metadata, vec![0xde, 0xad, 0xbe, 0xef]);
}

#[test]
fn value_data_converts_into_value_with_expected_wire_bytes() {
    let converted: v1::Value = v1::value::ValueData::I64Value(7).into();
    match converted.value_data {
        Some(v1::value::ValueData::I64Value(value)) => assert_eq!(value, 7),
        _ => panic!("Value::from must set the i64 oneof"),
    }
    // i64_value is field 4: tag (4 << 3) | 0 = 0x20.
    assert_eq!(converted.encode_to_vec(), vec![0x20, 0x07]);

    let text: v1::Value = v1::value::ValueData::StringValue("tick".to_owned()).into();
    // string_value is field 13: tag (13 << 3) | 2 = 0x6a.
    assert_eq!(text.encode_to_vec(), b"\x6a\x04tick");
}

fn assert_descriptor(descriptor: MethodDescriptor, path: &str, cardinality: RpcCardinality) {
    assert_eq!(descriptor.path, path);
    assert_eq!(descriptor.cardinality, cardinality);
}

#[test]
fn method_descriptors_expose_wire_paths_and_cardinalities() {
    assert_descriptor(
        v1::greptime_database::GREPTIME_DATABASE_HANDLE,
        "/greptime.v1.GreptimeDatabase/Handle",
        RpcCardinality::Unary,
    );
    assert_descriptor(
        v1::greptime_database::GREPTIME_DATABASE_HANDLE_REQUESTS,
        "/greptime.v1.GreptimeDatabase/HandleRequests",
        RpcCardinality::ClientStreaming,
    );
    assert_descriptor(
        v1::health_check::HEALTH_CHECK_HEALTH_CHECK,
        "/greptime.v1.HealthCheck/HealthCheck",
        RpcCardinality::Unary,
    );
    assert_descriptor(
        v1::prometheus_gateway::PROMETHEUS_GATEWAY_HANDLE,
        "/greptime.v1.PrometheusGateway/Handle",
        RpcCardinality::Unary,
    );
    assert_descriptor(
        flight::flight_service::FLIGHT_SERVICE_DO_PUT,
        "/arrow.flight.protocol.FlightService/DoPut",
        RpcCardinality::BidiStreaming,
    );
    assert_descriptor(
        flight::flight_service::FLIGHT_SERVICE_DO_GET,
        "/arrow.flight.protocol.FlightService/DoGet",
        RpcCardinality::ServerStreaming,
    );

    // Every other generated service module must expose the same descriptor
    // shape as its schema declares.
    assert_descriptor(
        v1::region::region::REGION_HANDLE,
        "/greptime.v1.region.Region/Handle",
        RpcCardinality::Unary,
    );
    assert_descriptor(
        v1::flow::flow::FLOW_HANDLE_CREATE_REMOVE,
        "/greptime.v1.flow.Flow/HandleCreateRemove",
        RpcCardinality::Unary,
    );
    assert_descriptor(
        v1::frontend::frontend::FRONTEND_LIST_PROCESS,
        "/greptime.v1.frontend.Frontend/ListProcess",
        RpcCardinality::Unary,
    );
    assert_descriptor(
        v1::meta::cluster::CLUSTER_RANGE,
        "/greptime.v1.meta.Cluster/Range",
        RpcCardinality::Unary,
    );
    assert_descriptor(
        v1::meta::config::CONFIG_PULL_CONFIG,
        "/greptime.v1.meta.Config/PullConfig",
        RpcCardinality::Unary,
    );
    assert_descriptor(
        v1::meta::heartbeat::HEARTBEAT_HEARTBEAT,
        "/greptime.v1.meta.Heartbeat/Heartbeat",
        RpcCardinality::BidiStreaming,
    );
    assert_descriptor(
        v1::meta::lock::LOCK_LOCK,
        "/greptime.v1.meta.Lock/Lock",
        RpcCardinality::Unary,
    );
    // ProcedureService RPCs are lower-case in the proto; grpc-rs normalizes
    // them to PascalCase, so its descriptor would not match the meta wire
    // route. The SDK does not use that service, so it is not asserted here.
    assert_descriptor(
        v1::meta::store::STORE_RANGE,
        "/greptime.v1.meta.Store/Range",
        RpcCardinality::Unary,
    );
}

type HealthClient = v1::health_check::HealthCheckClient<SyncChannel<MemoryPipe>>;
type DatabaseClient = v1::greptime_database::GreptimeDatabaseClient<SyncChannel<MemoryPipe>>;
type GatewayClient = v1::prometheus_gateway::PrometheusGatewayClient<SyncChannel<MemoryPipe>>;

fn assert_database_trait<T: v1::greptime_database::GreptimeDatabase>() {}
fn assert_health_trait<T: v1::health_check::HealthCheck>() {}
fn assert_gateway_trait<T: v1::prometheus_gateway::PrometheusGateway>() {}

struct DatabaseHandler;

#[allow(non_snake_case)]
impl v1::greptime_database::GreptimeDatabase for DatabaseHandler {
    fn Handle(
        &mut self,
        _request: Request<v1::GreptimeRequest>,
    ) -> Result<Response<v1::GreptimeResponse>, Status> {
        Ok(Response::new(v1::GreptimeResponse::default()))
    }

    fn HandleRequests(
        &mut self,
        _request: Request<Vec<v1::GreptimeRequest>>,
    ) -> Result<Response<v1::GreptimeResponse>, Status> {
        Ok(Response::new(v1::GreptimeResponse::default()))
    }
}

struct GatewayHandler;

#[allow(non_snake_case)]
impl v1::prometheus_gateway::PrometheusGateway for GatewayHandler {
    fn Handle(
        &mut self,
        request: Request<v1::PromqlRequest>,
    ) -> Result<Response<v1::PromqlResponse>, Status> {
        let body = request.get_ref().header.as_ref().map_or_else(
            || b"instant vector".to_vec(),
            |header| header.dbname.as_bytes().to_vec(),
        );
        Ok(Response::new(v1::PromqlResponse { header: None, body }))
    }
}

struct HealthHandler;

#[allow(non_snake_case)]
impl v1::health_check::HealthCheck for HealthHandler {
    fn HealthCheck(
        &mut self,
        request: Request<v1::HealthCheckRequest>,
    ) -> Result<Response<v1::HealthCheckResponse>, Status> {
        assert!(
            request.get_ref().encode_to_vec().is_empty(),
            "HealthCheckRequest declares no protobuf fields"
        );
        Ok(Response::new(v1::HealthCheckResponse {}))
    }
}

#[test]
fn generated_service_traits_and_client_server_types_are_exposed() {
    assert_database_trait::<DatabaseHandler>();
    assert_health_trait::<HealthHandler>();
    assert_gateway_trait::<GatewayHandler>();

    // Naming the generated client and server shells proves the grpc_service
    // macro emitted them with the expected generic parameters.
    let _health_client: Option<HealthClient> = None;
    let _database_client: Option<DatabaseClient> = None;
    let _gateway_client: Option<GatewayClient> = None;
    let _health_server: Option<v1::health_check::HealthCheckServer<HealthHandler>> = None;
    let _database_server: Option<v1::greptime_database::GreptimeDatabaseServer<DatabaseHandler>> =
        None;
    let _gateway_server: Option<v1::prometheus_gateway::PrometheusGatewayServer<GatewayHandler>> =
        None;

    assert_eq!(
        <ProstCodec<v1::HealthCheckRequest, v1::HealthCheckResponse> as Codec>::CONTENT_TYPE,
        "application/grpc",
        "the codec must advertise the Protobuf gRPC media type"
    );
}

#[test]
fn health_check_unary_round_trip_over_memory_pipe() -> Result<(), Status> {
    let (client_transport, server_transport) = MemoryPipe::pair();
    let worker = thread::spawn(move || -> Result<(), Status> {
        let mut server = SyncServer::new(server_transport)?;
        let mut handler = HealthHandler;
        server.register_unary(
            v1::health_check::HEALTH_CHECK_HEALTH_CHECK.path,
            ProstCodec::<v1::HealthCheckResponse, v1::HealthCheckRequest>::new(),
            move |request| handler.HealthCheck(request),
        )?;
        server.serve_connection()
    });

    let mut channel = SyncChannel::with_config(
        client_transport,
        ChannelConfig {
            default_timeout: Some(Duration::from_secs(5)),
            ..ChannelConfig::default()
        },
    )?;
    let mut codec = ProstCodec::<v1::HealthCheckRequest, v1::HealthCheckResponse>::new();
    let (response, initial_metadata, trailing_metadata) = channel.unary(
        v1::health_check::HEALTH_CHECK_HEALTH_CHECK.path,
        &v1::HealthCheckRequest {},
        &mut codec,
        CallOptions::default(),
    )?;
    assert!(
        response.encode_to_vec().is_empty(),
        "an empty HealthCheckResponse must travel as an empty payload"
    );
    assert!(initial_metadata.is_empty());
    assert!(trailing_metadata.is_empty());

    drop(channel);
    worker
        .join()
        .expect("the health server worker must not panic")
        .expect("the health server must serve one clean RPC and shut down");
    Ok(())
}
