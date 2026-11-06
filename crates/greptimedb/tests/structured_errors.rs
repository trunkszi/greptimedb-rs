//! Application diagnostics, gRPC trailers, error classification, and retry decisions.

use anyhow::Result;
use greptimedb::hints::*;
use greptimedb::{Error, ErrorCode, RetryableError};
use greptimedb_proto::v1::{self, ResponseHeader, Status};
use std::{error::Error as _, io};

fn trailers(inner: Option<&str>, hints: &[&str]) -> Result<grpc::Metadata> {
    let mut metadata = grpc::Metadata::new();
    if let Some(inner) = inner {
        metadata.append_ascii("inner_error_msg", inner)?;
    }
    for hint in hints {
        metadata.append_ascii(GREPTIME_RETRY_HINT_HEADER, hint)?;
    }
    Ok(metadata)
}

fn status_error(metadata: &grpc::Metadata) -> Error {
    Error::from_grpc_status_and_trailers(
        grpc::Status::unavailable("original gRPC failure"),
        Some(metadata),
    )
}

fn metadata(entries: &[(&str, &str)]) -> Result<grpc::Metadata> {
    let mut metadata = grpc::Metadata::new();
    for (key, value) in entries {
        metadata.append_ascii(key, value)?;
    }
    Ok(metadata)
}

#[test]
fn application_code_mapping_preserves_every_category_and_unknown_code() {
    let codes = [
        (0, ErrorCode::Success),
        (1000, ErrorCode::Unknown),
        (1001, ErrorCode::Unsupported),
        (1002, ErrorCode::Unexpected),
        (1003, ErrorCode::Internal),
        (1004, ErrorCode::InvalidArguments),
        (1005, ErrorCode::Cancelled),
        (1006, ErrorCode::IllegalState),
        (2000, ErrorCode::Syntax),
        (2001, ErrorCode::Plan),
        (2002, ErrorCode::EngineExecute),
        (3000, ErrorCode::TableAlreadyExists),
        (3001, ErrorCode::TableNotFound),
        (3002, ErrorCode::TableColumnNotFound),
        (3003, ErrorCode::TableColumnExists),
        (3004, ErrorCode::DatabaseNotFound),
        (3005, ErrorCode::DatabaseAlreadyExists),
        (4000, ErrorCode::StorageUnavailable),
        (4001, ErrorCode::RegionNotFound),
        (4002, ErrorCode::RegionReadonly),
        (4003, ErrorCode::RegionBusy),
        (4004, ErrorCode::RateLimited),
        (4005, ErrorCode::FlowControl),
        (5000, ErrorCode::MetaSrv),
        (6000, ErrorCode::AuthFailed),
    ];
    for (raw, category) in codes {
        assert_eq!(ErrorCode::from_u32(raw), category);
        assert_eq!(category.as_u32(), raw);
    }
    for raw in [1, 1007, 5999, 7000, u32::MAX] {
        assert_eq!(ErrorCode::from_u32(raw), ErrorCode::Other(raw));
        assert_eq!(ErrorCode::from_u32(raw).as_u32(), raw);
    }
    assert_eq!(ErrorCode::from_u32(6999), ErrorCode::AuthFailed);
}

#[test]
fn response_headers_are_checked_even_when_grpc_succeeds() -> Result<()> {
    Error::from_response_header(None)?;
    Error::from_response_header(Some(&v1::ResponseHeader { status: None }))?;
    Error::from_response_header(Some(&v1::ResponseHeader {
        status: Some(v1::Status {
            status_code: 0,
            err_msg: String::new(),
            retry_hint: "true".into(),
        }),
    }))?;
    for hint in ["true", "false", "", "TRUE", "junk"] {
        let header = v1::ResponseHeader {
            status: Some(v1::Status {
                status_code: 6999,
                err_msg: "access denied".into(),
                retry_hint: hint.into(),
            }),
        };
        let error =
            Error::from_response_header(Some(&header)).expect_err("nonzero application status");
        assert!(
            matches!(&error, Error::ServerStatus { code: ErrorCode::AuthFailed, raw_code: 6999, message, retry_hint } if message == "access denied" && *retry_hint == (hint == "true"))
        );
        assert_eq!(error.is_retryable(), hint == "true");
        assert_eq!(error.retry_hint(), Some(hint == "true"));
    }
    Ok(())
}

#[test]
fn grpc_trailers_preserve_structured_status_and_explicit_retry_decisions() -> Result<()> {
    assert_eq!(GREPTIME_INNER_ERROR_HEADER, "INNER_ERROR_MSG");
    for hint in ["true", "false"] {
        let metadata = trailers(Some(r#"{"code":4003,"err_msg":"region busy"}"#), &[hint])?;
        let error = status_error(&metadata);
        assert!(
            matches!(&error, Error::ServerStatus { code: ErrorCode::RegionBusy, raw_code: 4003, message, retry_hint } if message == "region busy" && *retry_hint == (hint == "true"))
        );
        assert_eq!(error.retry_hint(), Some(hint == "true"));
    }
    let metadata = trailers(Some(r#"{"code":999999,"err_msg":"unknown failure"}"#), &[])?;
    assert!(matches!(
        status_error(&metadata),
        Error::ServerStatus {
            code: ErrorCode::Other(999999),
            raw_code: 999999,
            retry_hint: false,
            ..
        }
    ));
    Ok(())
}

#[test]
fn malformed_inner_errors_never_replace_the_original_grpc_status() -> Result<()> {
    for inner in [
        "not json",
        "[]",
        "null",
        r#"{"code":0,"err_msg":"success"}"#,
        r#"{"code":-1,"err_msg":"bad"}"#,
        r#"{"code":4294967296,"err_msg":"overflow"}"#,
        r#"{"code":"4003","err_msg":"string"}"#,
        r#"{"code":4003.5,"err_msg":"fraction"}"#,
        r#"{"code":4003}"#,
        r#"{"code":4003,"err_msg":2}"#,
    ] {
        match status_error(&trailers(Some(inner), &[])?) {
            Error::Grpc(status) => {
                assert_eq!(status.code(), grpc::core::Code::Unavailable);
                assert_eq!(status.message(), "original gRPC failure");
            }
            other => anyhow::bail!("malformed JSON replaced gRPC status: {other}"),
        }
    }
    assert!(matches!(
        Error::from_grpc_status_and_trailers(grpc::Status::unavailable("original"), None),
        Error::Grpc(_)
    ));
    Ok(())
}

#[test]
fn retry_hints_without_json_and_conflicting_duplicates_are_conservative() -> Result<()> {
    for hint in ["true", "false"] {
        let error = status_error(&trailers(Some("invalid JSON"), &[hint])?);
        assert_eq!(error.retry_hint(), Some(hint == "true"));
        assert!(
            matches!(error, Error::ServerStatus { code: ErrorCode::Unknown, raw_code: 1000, message, .. } if message == "original gRPC failure")
        );
    }
    for hints in [
        vec!["true", "false"],
        vec!["false", "true"],
        vec!["true", "junk"],
        vec!["junk", "true"],
    ] {
        assert!(
            !status_error(&trailers(
                Some(r#"{"code":4003,"err_msg":"busy"}"#),
                &hints
            )?)
            .is_retryable()
        );
    }
    let no_hint = status_error(&trailers(None, &[])?);
    assert_eq!(no_hint.retry_hint(), None);
    Ok(())
}

#[test]
fn retry_and_source_chains_do_not_authorize_uncertain_write_replay() {
    let transport = Error::from(io::Error::new(
        io::ErrorKind::TimedOut,
        "write outcome unknown",
    ));
    assert!(!transport.is_retryable());
    assert!(transport.source().is_some());
    let aggregate = Error::AllEndpointsFailed {
        attempts: 2,
        last_error: Box::new(Error::ServerStatus {
            code: ErrorCode::RegionBusy,
            raw_code: 4003,
            message: "busy".into(),
            retry_hint: true,
        }),
    };
    assert!(aggregate.is_retryable());
    assert_eq!(aggregate.retry_hint(), Some(true));
    assert!(aggregate.source().is_some());
    assert!(aggregate.to_string().contains("2 attempts"));
    for error in [
        Error::Stream("stream".into()),
        Error::InvalidConfig("config".into()),
        Error::SchemaViolation("schema".into()),
        Error::Encode("encode".into()),
        Error::Decode("decode".into()),
        Error::Protocol("protocol".into()),
        Error::CircuitOpen {
            endpoint: "localhost:4001".into(),
        },
    ] {
        assert!(!error.is_retryable());
    }
}

#[test]
fn mixed_case_or_numeric_retry_hints_never_authorize_replay() -> Result<()> {
    for hint in ["TRUE", "True", "1", " true", "true "] {
        for inner in [None, Some(r#"{"code":4003,"err_msg":"busy"}"#)] {
            let error = status_error(&trailers(inner, &[hint])?);
            assert!(!error.is_retryable(), "accepted {hint:?}");
        }
    }
    for raw_code in [4000, 4001, 4002, 4003, 4004, 4005, 5000] {
        let inner = format!(r#"{{"code":{raw_code},"err_msg":"transient"}}"#);
        for hint in [None, Some("true"), Some("false")] {
            let hints: Vec<_> = hint.into_iter().collect();
            let error = status_error(&trailers(Some(&inner), &hints)?);
            assert_eq!(error.is_retryable(), hint == Some("true"));
        }
    }
    for kind in [
        io::ErrorKind::TimedOut,
        io::ErrorKind::WouldBlock,
        io::ErrorKind::Interrupted,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::ConnectionRefused,
        io::ErrorKind::BrokenPipe,
    ] {
        assert!(!Error::Transport(io::Error::new(kind, "uncertain delivery")).is_retryable());
    }
    Ok(())
}

#[test]
fn numeric_error_trailers_require_consistent_nonzero_decimal_u32() -> Result<()> {
    for code in [
        "",
        "junk",
        "4294967296",
        "-1",
        "+4003",
        "0",
        "4003.0",
        " 4003",
        "4003 ",
    ] {
        let mut metadata = trailers(None, &[])?;
        metadata.append_ascii("x-greptime-err-code", code)?;
        assert!(
            matches!(status_error(&metadata), Error::Grpc(_)),
            "accepted {code:?}"
        );
    }
    for hint in [None, Some("true"), Some("false"), Some("TRUE")] {
        let hints: Vec<_> = hint.into_iter().collect();
        let mut metadata = trailers(None, &hints)?;
        metadata.append_ascii("x-greptime-err-code", "4003")?;
        let error = status_error(&metadata);
        assert!(
            matches!(&error, Error::ServerStatus { code: ErrorCode::RegionBusy, raw_code: 4003, message, .. } if message == "original gRPC failure")
        );
        assert_eq!(error.is_retryable(), hint == Some("true"));
    }
    for codes in [["4003", "4004"], ["4003", "invalid"], ["invalid", "4003"]] {
        let mut metadata = trailers(None, &[])?;
        for code in codes {
            metadata.append_ascii("x-greptime-err-code", code)?;
        }
        assert!(matches!(status_error(&metadata), Error::Grpc(_)));
    }
    let mut repeated = trailers(Some("invalid JSON"), &[])?;
    repeated.append_ascii("x-greptime-err-code", "4294967295")?;
    repeated.append_ascii("x-greptime-err-code", "4294967295")?;
    assert!(matches!(
        status_error(&repeated),
        Error::ServerStatus {
            raw_code: u32::MAX,
            code: ErrorCode::Other(u32::MAX),
            retry_hint: false,
            ..
        }
    ));
    let mut invalid = trailers(None, &["true"])?;
    invalid.append_ascii("x-greptime-err-code", "overflow")?;
    assert!(matches!(
        status_error(&invalid),
        Error::ServerStatus {
            code: ErrorCode::Unknown,
            raw_code: 1000,
            retry_hint: true,
            ..
        }
    ));
    let mut metadata = trailers(
        Some(r#"{"code":3001,"err_msg":"missing table"}"#),
        &["false"],
    )?;
    metadata.append_ascii("x-greptime-err-code", "4003")?;
    assert!(matches!(
        status_error(&metadata),
        Error::ServerStatus {
            code: ErrorCode::TableNotFound,
            raw_code: 3001,
            ..
        }
    ));
    Ok(())
}

#[test]
fn retry_hint_wire_vocabulary_is_shared_and_remains_strict() -> Result<()> {
    for (hint, expected) in [
        ("true", true),
        ("retryable", true),
        ("false", false),
        ("non_retryable", false),
        ("TRUE", false),
        ("Retryable", false),
        (" true ", false),
        (" retryable ", false),
        ("1", false),
    ] {
        let header = ResponseHeader {
            status: Some(Status {
                status_code: 4003,
                err_msg: "busy".into(),
                retry_hint: hint.into(),
            }),
        };
        let error = Error::from_response_header(Some(&header)).expect_err("application error");
        assert_eq!(error.is_retryable(), expected, "response hint {hint:?}");
        let trailers = metadata(&[
            ("x-greptime-err-code", "4003"),
            ("x-greptime-err-retry-hint", hint),
        ])?;
        let error = Error::from_grpc_status_and_trailers(
            grpc::Status::unavailable("busy"),
            Some(&trailers),
        );
        assert_eq!(error.is_retryable(), expected, "trailer hint {hint:?}");
    }
    for second in ["false", "non_retryable", "junk", "TRUE"] {
        let trailers = metadata(&[
            ("x-greptime-err-retry-hint", "retryable"),
            ("x-greptime-err-retry-hint", second),
        ])?;
        assert!(
            !Error::from_grpc_status_and_trailers(
                grpc::Status::unavailable("busy"),
                Some(&trailers)
            )
            .is_retryable()
        );
    }
    Ok(())
}

#[test]
fn ok_trailers_require_an_actual_application_failure() -> Result<()> {
    Error::check_ok_trailers(&grpc::Metadata::new())?;
    for hint in ["true", "false", "retryable", "non_retryable"] {
        Error::check_ok_trailers(&metadata(&[("x-greptime-err-retry-hint", hint)])?)?;
        Error::check_ok_trailers(&metadata(&[
            ("x-greptime-err-code", "0"),
            ("x-greptime-err-retry-hint", hint),
        ])?)?;
    }
    Error::check_ok_trailers(&metadata(&[(
        "inner_error_msg",
        r#"{"code":0,"err_msg":""}"#,
    )])?)?;
    let trailers = metadata(&[
        ("x-greptime-err-code", "4003"),
        ("x-greptime-err-msg", "region unavailable"),
    ])?;
    assert!(
        matches!(Error::check_ok_trailers(&trailers), Err(Error::ServerStatus { raw_code: 4003, message, .. }) if message == "region unavailable")
    );
    let trailers = metadata(&[(
        "inner_error_msg",
        r#"{"code":4000,"err_msg":"storage failed"}"#,
    )])?;
    assert!(
        matches!(Error::check_ok_trailers(&trailers), Err(Error::ServerStatus { raw_code: 4000, message, .. }) if message == "storage failed")
    );
    let trailers = metadata(&[("x-greptime-err-msg", "diagnostic without code")])?;
    assert!(Error::check_ok_trailers(&trailers).is_err());
    Ok(())
}

#[test]
fn v121_classifier_is_explicit_and_legacy_numbers_remain_compatible() {
    for (raw, code) in [
        (3000, ErrorCode::TableAlreadyExists),
        (4001, ErrorCode::RegionNotFound),
        (6001, ErrorCode::AuthFailed),
        (7000, ErrorCode::Other(7000)),
        (7002, ErrorCode::AuthFailed),
    ] {
        assert_eq!(ErrorCode::from_u32(raw), code);
    }
    assert_eq!(ErrorCode::TableAlreadyExists.as_u32(), 3000);
    assert_eq!(ErrorCode::RegionNotFound.as_u32(), 4001);
    assert_eq!(ErrorCode::AuthFailed.as_u32(), 6000);
    for (raw, code) in [
        (0, ErrorCode::Success),
        (3000, ErrorCode::Plan),
        (3001, ErrorCode::EngineExecute),
        (4000, ErrorCode::TableAlreadyExists),
        (4001, ErrorCode::TableNotFound),
        (4003, ErrorCode::RegionBusy),
        (4005, ErrorCode::RegionNotFound),
        (5000, ErrorCode::StorageUnavailable),
        (6000, ErrorCode::ResourceExhausted),
        (6001, ErrorCode::RateLimited),
        (7000, ErrorCode::AuthFailed),
        (7002, ErrorCode::AuthFailed),
        (7006, ErrorCode::AuthFailed),
        (u32::MAX, ErrorCode::Other(u32::MAX)),
    ] {
        assert_eq!(ErrorCode::from_v121_u32(raw), code, "wire code {raw}");
    }
}
