//! Request metadata, routing headers, and ingestion hint overlays.

use anyhow::Result;
use greptimedb::hints::*;
use greptimedb::{Auth, ClientOptions, Compression, Endpoint, MergeMode, WriteHints};
use std::{collections::HashMap, time::Duration};

fn options() -> Result<ClientOptions> {
    Ok(ClientOptions::from_endpoint(Endpoint::parse(
        "localhost:4001",
    )?))
}

fn ascii<'a>(metadata: &'a grpc::Metadata, key: &str) -> Option<&'a str> {
    metadata.iter().find_map(|(name, value)| match value {
        grpc::core::MetadataValue::Ascii(value) if name == key => Some(value.as_str()),
        _ => None,
    })
}

#[test]
fn hints_encode_wire_directives_and_replace_existing_keys() {
    assert!(WriteHints::new().is_empty());
    assert_eq!(WriteHints::default().encode_header_value(), None);
    assert_eq!(MergeMode::LastRow.as_str(), "last_row");
    assert_eq!(MergeMode::LastNonNull.as_str(), "last_non_null");
    let hints = WriteHints::new()
        .with_auto_create_table(true)
        .with_ttl("7d")
        .with_append_mode(false)
        .with_merge_mode(MergeMode::LastNonNull)
        .with_physical_table("metrics")
        .with_hint("custom", "a=b");
    assert_eq!(
        hints.encode_header_value().as_deref(),
        Some(
            "auto_create_table=true,ttl=7d,append_mode=false,merge_mode=last_non_null,physical_table=metrics,custom=a=b"
        )
    );
    let updated = hints.with_ttl("1d").with_hint("auto_create_table", "false");
    assert_eq!(
        updated.encode_header_value().as_deref(),
        Some(
            "auto_create_table=false,ttl=1d,append_mode=false,merge_mode=last_non_null,physical_table=metrics,custom=a=b"
        )
    );
}

#[test]
fn hints_reject_delimiter_injection_without_inventing_escaping() -> Result<()> {
    for (key, value) in [
        ("", "x"),
        ("a=b", "x"),
        ("a,b", "x"),
        ("a", "7d,append_mode=true"),
        ("a", "\nx"),
        ("a", "value\0"),
        ("a", "caf\u{e9}"),
        (" a", "x"),
        ("a", " x"),
    ] {
        assert!(
            WriteHints::new().try_with_hint(key, value).is_err(),
            "accepted {key:?}={value:?}"
        );
    }
    assert_eq!(
        WriteHints::new()
            .try_with_hint("custom", "")?
            .encode_header_value()
            .as_deref(),
        Some("custom=")
    );
    Ok(())
}

#[test]
fn request_and_call_headers_agree_on_auth_database_and_overlaid_hints() -> Result<()> {
    let mut client = options()?.with_dbname("analytics-ticks");
    client.auth = Some(Auth::basic("alice", "secret"));
    client.timezone = Some("UTC".into());
    client.request_timeout = Duration::from_secs(7);
    client.compression = Compression::Zstd;
    client.write_hints = WriteHints::new()
        .with_auto_create_table(true)
        .with_ttl("7d");
    let tracing = HashMap::from([("traceparent".into(), "00-trace-span-01".into())]);
    let header = client.build_request_header(Some(tracing.clone()));
    assert_eq!(
        (&*header.catalog, &*header.schema, &*header.dbname),
        ("analytics", "ticks", "analytics-ticks")
    );
    assert_eq!(header.timezone, "UTC");
    assert_eq!(header.tracing_context, tracing);
    assert_eq!(
        header.authorization,
        client.auth.as_ref().map(Auth::to_proto_auth_header)
    );
    let extra = WriteHints::new().with_ttl("1d").with_append_mode(true);
    let call = client.build_call_options(Some(&extra));
    assert_eq!(
        ascii(&call.metadata, GREPTIME_DB_HEADER),
        Some("analytics-ticks")
    );
    assert_eq!(
        ascii(&call.metadata, GREPTIME_AUTH_HEADER),
        Some("Basic YWxpY2U6c2VjcmV0")
    );
    assert_eq!(
        ascii(&call.metadata, GREPTIME_HINTS_HEADER),
        Some("auto_create_table=true,ttl=1d,append_mode=true")
    );
    assert_eq!(call.timeout, Some(Duration::from_secs(7)));
    assert_eq!(call.compression, Some(grpc::CompressionAlgorithm::Zstd));
    assert_eq!(
        client.write_hints.encode_header_value().as_deref(),
        Some("auto_create_table=true,ttl=7d")
    );
    let default_header = options()?.build_request_header(None);
    assert!(default_header.tracing_context.is_empty() && default_header.timezone.is_empty());
    assert!(default_header.authorization.is_none());
    Ok(())
}

#[test]
fn empty_hint_overlay_retains_defaults_and_typed_builders_replace_custom_values() -> Result<()> {
    let mut client = options()?;
    client.write_hints = WriteHints::new().with_hint("ttl", "7d").with_ttl("1d");
    assert_eq!(
        ascii(
            &client.build_call_options(Some(&WriteHints::new())).metadata,
            GREPTIME_HINTS_HEADER
        ),
        Some("ttl=1d")
    );
    Ok(())
}

#[test]
fn reserved_hints_never_produce_duplicate_directives() -> Result<()> {
    let hints = WriteHints::new()
        .with_ttl("1d")
        .with_hint("ttl", "7d")
        .with_auto_create_table(false)
        .with_hint("auto_create_table", "true")
        .with_merge_mode(MergeMode::LastRow)
        .with_hint("merge_mode", "last_non_null")
        .with_physical_table("old")
        .with_hint("physical_table", "new")
        .with_hint("skip_wal", "false")
        .with_hint("skip_wal", "true")
        .with_append_mode(false)
        .with_hint("append_mode", "true");
    assert_eq!(
        hints.encode_header_value().as_deref(),
        Some(
            "ttl=7d,auto_create_table=true,merge_mode=last_non_null,physical_table=new,skip_wal=true,append_mode=true"
        )
    );
    for (key, value) in [
        ("ttl=7d", "x"),
        ("ttl,skip_wal", "true"),
        ("ttl", "7d,skip_wal=true"),
        ("ttl ", "7d"),
        ("ttl", "7d "),
        ("ttl", "\t7d"),
    ] {
        assert!(WriteHints::new().try_with_hint(key, value).is_err());
    }
    assert_eq!(
        WriteHints::new()
            .try_with_hint("custom", "a=b")?
            .encode_header_value()
            .as_deref(),
        Some("custom=a=b")
    );
    Ok(())
}

#[test]
fn metadata_header_names_match_the_greptime_wire_contract() {
    assert_eq!(GREPTIME_DB_HEADER, "x-greptime-db-name");
    assert_eq!(GREPTIME_AUTH_HEADER, "x-greptime-auth");
    assert_eq!(GREPTIME_HINTS_HEADER, "x-greptime-hints");
    assert_eq!(GREPTIME_RETRY_HINT_HEADER, "x-greptime-err-retry-hint");
    assert_eq!(GREPTIME_INNER_ERROR_HEADER, "INNER_ERROR_MSG");
}
