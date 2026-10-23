//! Endpoint, credential, and failover-admission configuration contracts.

use anyhow::Result;
use greptimedb::hints::*;
use greptimedb::{
    Auth, ClientOptions, Compression, Endpoint, Error, FailoverPolicy, TransportScheme,
};
use greptimedb_proto::v1::{self, auth_header::AuthScheme};
use std::{path::Path, time::Duration};

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
fn endpoint_schemes_ports_and_authorities() -> Result<()> {
    for (raw, scheme, host, port, authority) in [
        (
            "node.example",
            TransportScheme::Grpc,
            "node.example",
            4001,
            "node.example:4001",
        ),
        (
            "node.example:5000",
            TransportScheme::Grpc,
            "node.example",
            5000,
            "node.example:5000",
        ),
        (
            "grpc://127.0.0.1",
            TransportScheme::Grpc,
            "127.0.0.1",
            4001,
            "127.0.0.1:4001",
        ),
        (
            "http://localhost:42",
            TransportScheme::Grpc,
            "localhost",
            42,
            "localhost:42",
        ),
        (
            "grpcs://node.example:65535",
            TransportScheme::Grpcs,
            "node.example",
            65535,
            "node.example:65535",
        ),
        (
            "https://localhost",
            TransportScheme::Grpcs,
            "localhost",
            4001,
            "localhost:4001",
        ),
        ("[::1]", TransportScheme::Grpc, "::1", 4001, "[::1]:4001"),
        (
            "https://[2001:db8::1]:443",
            TransportScheme::Grpcs,
            "2001:db8::1",
            443,
            "[2001:db8::1]:443",
        ),
    ] {
        let endpoint = Endpoint::parse(raw)?;
        assert_eq!(endpoint.scheme(), scheme, "{raw}");
        assert_eq!(endpoint.host(), host);
        assert_eq!(endpoint.port(), port);
        assert_eq!(endpoint.socket_addr_str(), authority);
        assert_eq!(endpoint.authority(), authority);
        assert!(endpoint.unix_path().is_none());
    }
    let unix = Endpoint::parse("unix:///tmp/greptime.sock")?;
    assert_eq!(unix.scheme(), TransportScheme::Unix);
    assert_eq!(unix.unix_path(), Some(Path::new("/tmp/greptime.sock")));
    assert_eq!(unix.authority(), "localhost");
    Ok(())
}

#[test]
fn endpoint_rejects_ambiguous_or_unsafe_input() {
    for raw in [
        "",
        " ",
        "grpc://",
        "ftp://localhost",
        "localhost:0",
        "localhost:65536",
        "localhost:-1",
        "localhost:+1",
        "localhost:",
        "localhost:a",
        "localhost/path",
        "localhost?query",
        "localhost#fragment",
        "user:pass@localhost",
        "a\nb",
        "a\0b",
        "::1",
        "[::1",
        "[not-ip]:42",
        "[::1]garbage",
        "[::1]:0",
        "unix://relative",
        "unix:///",
        "unix:///tmp/sock?query",
        "999.999.999.999",
        "-bad.example",
        "bad..example",
    ] {
        assert!(
            matches!(Endpoint::parse(raw), Err(Error::InvalidConfig(_))),
            "accepted {raw:?}"
        );
    }
}

#[test]
fn authentication_matches_metadata_and_protobuf_fixtures() {
    let basic = Auth::basic("alice", "secret");
    assert_eq!(basic.to_metadata_value(), "Basic YWxpY2U6c2VjcmV0");
    assert_eq!(
        basic.to_proto_auth_header().auth_scheme,
        Some(AuthScheme::Basic(v1::Basic {
            username: "alice".into(),
            password: "secret".into()
        }))
    );
    let token = Auth::token("opaque.token");
    assert_eq!(token.to_metadata_value(), "Bearer opaque.token");
    assert_eq!(
        token.to_proto_auth_header().auth_scheme,
        Some(AuthScheme::Token(v1::Token {
            token: "opaque.token".into()
        }))
    );
}

#[test]
fn client_defaults_and_database_routing() -> Result<()> {
    let client = options()?;
    assert_eq!(client.catalog, "greptime");
    assert_eq!(client.schema, "public");
    assert_eq!(client.dbname(), "greptime-public");
    assert!(client.tcp_nodelay);
    assert_eq!(client.max_in_flight, 8);
    assert_eq!(client.compression, Compression::Identity);
    assert!(client.auth.is_none() && client.timezone.is_none());
    assert!(client.connect_timeout > Duration::ZERO && client.request_timeout > Duration::ZERO);
    assert_eq!(
        client.clone().with_dbname("analytics-ticks-live").dbname(),
        "analytics-ticks-live"
    );
    let schema = client.clone().with_dbname("ticks");
    assert_eq!(schema.catalog, "greptime");
    assert_eq!(schema.schema, "ticks");
    assert_eq!(
        client.with_catalog("").with_schema("ticks").dbname(),
        "ticks"
    );
    Ok(())
}

#[test]
fn compression_never_inherits_an_unintended_channel_algorithm() {
    for (sdk, wire) in [
        (Compression::Identity, grpc::CompressionAlgorithm::Identity),
        (Compression::Gzip, grpc::CompressionAlgorithm::Gzip),
        (Compression::Deflate, grpc::CompressionAlgorithm::Deflate),
        (Compression::Snappy, grpc::CompressionAlgorithm::Snappy),
        (Compression::Zstd, grpc::CompressionAlgorithm::Zstd),
        (Compression::Lz4, grpc::CompressionAlgorithm::Lz4),
    ] {
        assert_eq!(sdk.to_grpc(), Some(wire));
    }
}

#[test]
fn mutable_invalid_configuration_fails_before_any_call() -> Result<()> {
    let valid = options()?;
    let mut cases = Vec::new();
    let mut bad = valid.clone();
    bad.endpoints.clear();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.schema.clear();
    cases.push(bad);
    let mut bad = valid.clone();
    bad.max_in_flight = 0;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.request_timeout = Duration::ZERO;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.connect_timeout = Duration::MAX;
    cases.push(bad);
    let mut bad = valid.clone();
    bad.auth = Some(Auth::token("bad\r\ntoken"));
    cases.push(bad);
    let mut bad = valid;
    bad.schema = "bad\nschema".into();
    cases.push(bad);
    for client in cases {
        assert!(matches!(
            client.try_build_call_options(None),
            Err(Error::InvalidConfig(_))
        ));
    }
    Ok(())
}

#[test]
fn basic_usernames_cannot_change_meaning_between_request_layers() -> Result<()> {
    let mut client = options()?;
    client.auth = Some(Auth::basic("tenant:user", "secret"));
    assert!(matches!(
        client.try_build_call_options(None),
        Err(Error::InvalidConfig(_))
    ));
    client.auth = Some(Auth::basic("user", "password:with:colons"));
    let call = client.try_build_call_options(None)?;
    assert_eq!(
        ascii(&call.metadata, GREPTIME_AUTH_HEADER),
        Some("Basic dXNlcjpwYXNzd29yZDp3aXRoOmNvbG9ucw==")
    );
    Ok(())
}

#[test]
fn ipv6_authorities_keep_exactly_one_bracket_pair() -> Result<()> {
    for (raw, expected) in [
        ("[::1]:4001", "[::1]:4001"),
        ("grpc://[::1]:1", "[::1]:1"),
        ("https://[2001:db8::1]", "[2001:db8::1]:4001"),
    ] {
        let endpoint = Endpoint::parse(raw)?;
        assert!(!endpoint.host().contains(['[', ']']));
        assert_eq!(endpoint.socket_addr_str(), expected);
        assert_eq!(endpoint.authority(), expected);
    }
    for raw in ["[::1]:0", "grpcs://[::1]:0", "localhost:00000"] {
        assert!(Endpoint::parse(raw).is_err(), "accepted {raw}");
    }
    Ok(())
}

#[test]
fn basic_credentials_preserve_empty_components_and_utf8_bytes() -> Result<()> {
    for (username, password, encoded) in [
        ("", "", "Basic Og=="),
        ("", "secret", "Basic OnNlY3JldA=="),
        ("alice", "", "Basic YWxpY2U6"),
        (
            "\u{fc}ser",
            "p\u{e4}ss:word",
            "Basic w7xzZXI6cMOkc3M6d29yZA==",
        ),
    ] {
        let mut client = options()?;
        client.auth = Some(Auth::basic(username, password));
        let call = client.try_build_call_options(None)?;
        assert_eq!(ascii(&call.metadata, GREPTIME_AUTH_HEADER), Some(encoded));
        assert_eq!(
            client.build_request_header(None).authorization,
            Some(Auth::basic(username, password).to_proto_auth_header())
        );
    }
    for byte in (0_u8..=31).chain(std::iter::once(127)) {
        for username_field in [true, false] {
            let control = format!("a{}b", char::from(byte));
            let mut client = options()?;
            client.auth = Some(if username_field {
                Auth::basic(control, "ok")
            } else {
                Auth::basic("ok", control)
            });
            assert!(matches!(
                client.try_build_call_options(None),
                Err(Error::InvalidConfig(_))
            ));
        }
    }
    Ok(())
}

#[test]
fn timeout_validation_rejects_zero_without_rounding_submillisecond_values() -> Result<()> {
    for timeout in [Duration::from_nanos(1), Duration::from_micros(1)] {
        let mut client = options()?;
        client.connect_timeout = timeout;
        client.request_timeout = timeout;
        assert_eq!(client.try_build_call_options(None)?.timeout, Some(timeout));
    }
    for connect in [true, false] {
        let mut client = options()?;
        if connect {
            client.connect_timeout = Duration::ZERO;
        } else {
            client.request_timeout = Duration::ZERO;
        }
        assert!(matches!(
            client.try_build_call_options(None),
            Err(Error::InvalidConfig(_))
        ));
    }
    Ok(())
}

#[test]
fn auth_debug_and_nested_options_redact_secrets() -> Result<()> {
    let basic = Auth::basic("alice", "private-password");
    assert_eq!(
        format!("{basic:?}"),
        "Basic { username: \"alice\", password: \"<redacted>\" }"
    );
    let token = Auth::token("private-token");
    assert_eq!(format!("{token:?}"), "Token(\"<redacted>\")");
    for auth in [basic, token] {
        let mut options = ClientOptions::from_endpoint(Endpoint::parse("localhost:4001")?);
        options.auth = Some(auth);
        let debug = format!("{options:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("private-password"));
        assert!(!debug.contains("private-token"));
    }
    Ok(())
}

#[test]
fn failover_checks_endpoint_identity_order_but_accepts_short_labels() -> Result<()> {
    let endpoints = vec![
        Endpoint::parse("a.example:4001")?,
        Endpoint::parse("b.example:4002")?,
    ];
    let options = |names: &[&str]| {
        ClientOptions::new(endpoints.clone())
            .with_failover(FailoverPolicy::new(names.iter().copied()).config())
    };
    options(&["a.example:4001", "b.example:4002"]).validate()?;
    options(&["http://a.example:4001", "grpc://b.example:4002"]).validate()?;
    options(&["a", "b"]).validate()?;
    for names in [
        ["b.example:4002", "a.example:4001"],
        ["a.example:4002", "b.example:4002"],
        ["grpcs://a.example:4001", "b.example:4002"],
        ["other.example:4001", "b.example:4002"],
    ] {
        assert!(matches!(
            options(&names).validate(),
            Err(Error::InvalidConfig(_))
        ));
    }
    let ipv6 = ClientOptions::from_endpoint(Endpoint::parse("grpc://[::1]:4001")?)
        .with_failover(FailoverPolicy::new(["http://[::1]:4001"]).config());
    ipv6.validate()?;
    Ok(())
}
