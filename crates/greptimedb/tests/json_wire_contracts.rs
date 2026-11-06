//! JSON wire boundaries shared by Flight metadata and server diagnostics.

use greptimedb::{
    Error, RetryableError,
    bulk::{DoPutMetadata, DoPutResponse},
};
use sonic_rs::JsonValueTrait;

#[test]
fn integration_flight_metadata_preserves_integer_precision_and_optional_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let metadata = DoPutMetadata {
        request_id: i64::MAX,
        min_timestamp: Some(i64::MIN),
        max_timestamp: Some(i64::MAX),
    };
    let encoded = sonic_rs::to_vec(&metadata)?;
    let dynamic: sonic_rs::Value = sonic_rs::from_slice(&encoded)?;
    assert_eq!(dynamic["request_id"].as_i64(), Some(i64::MAX));
    assert_eq!(dynamic["min_timestamp"].as_i64(), Some(i64::MIN));
    assert_eq!(sonic_rs::from_slice::<DoPutMetadata>(&encoded)?, metadata);

    let schema: DoPutMetadata = sonic_rs::from_str(r#"{"request_id":0}"#)?;
    assert_eq!(schema.min_timestamp, None);
    assert_eq!(schema.max_timestamp, None);
    assert_eq!(sonic_rs::to_string(&schema)?, r#"{"request_id":0}"#);

    let ack = DoPutResponse {
        request_id: i64::MAX,
        affected_rows: usize::MAX,
    };
    assert_eq!(
        sonic_rs::from_slice::<DoPutResponse>(&sonic_rs::to_vec(&ack)?)?,
        ack
    );
    for invalid in [
        r#"{"request_id":1,"affected_rows":-1}"#,
        r#"{"request_id":1,"affected_rows":1.5}"#,
        r#"{"request_id":1,"affected_rows":18446744073709551616}"#,
        r#"{"request_id":1}"#,
    ] {
        assert!(
            sonic_rs::from_str::<DoPutResponse>(invalid).is_err(),
            "{invalid}"
        );
    }
    Ok(())
}

#[test]
fn scenario_server_json_keeps_escaped_messages_and_rejects_invalid_numbers()
-> Result<(), Box<dyn std::error::Error>> {
    let mut trailers = grpc::Metadata::new();
    trailers.append_ascii(
        "inner_error_msg",
        r#"{"code":4294967295,"err_msg":"busy\n\u00e9"}"#,
    )?;
    trailers.append_ascii("x-greptime-err-retry-hint", "true")?;
    let error = Error::from_grpc_status_and_trailers(
        grpc::Status::unavailable("transport"),
        Some(&trailers),
    );
    assert_eq!(error.retry_hint(), Some(true));
    assert!(
        matches!(error, Error::ServerStatus { raw_code: u32::MAX, message, .. } if message == "busy\n\u{e9}")
    );

    for payload in [
        r#"{"code":1e400,"err_msg":"overflow"}"#,
        r#"{"code":18446744073709551616,"err_msg":"overflow"}"#,
        r#"{"code":4003,"err_msg":"truncated""#,
    ] {
        let mut trailers = grpc::Metadata::new();
        trailers.append_ascii("inner_error_msg", payload)?;
        let error = Error::from_grpc_status_and_trailers(
            grpc::Status::unavailable("original"),
            Some(&trailers),
        );
        assert!(matches!(error, Error::Grpc(status) if status.message() == "original"));
    }
    Ok(())
}
