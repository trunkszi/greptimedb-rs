//! Endpoint parsing, credentials, and per-request routing defaults.

use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use greptimedb_proto::v1::{self, auth_header::AuthScheme};

use crate::{
    error::{Error, Result},
    failover::FailoverConfig,
    hints::{
        DEFAULT_CATALOG, DEFAULT_SCHEMA, GREPTIME_AUTH_HEADER, GREPTIME_DB_HEADER,
        GREPTIME_HINTS_HEADER, WriteHints,
    },
};

/// Wire transport selected by an endpoint URI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportScheme {
    /// Plaintext synchronous gRPC over TCP.
    Grpc,
    /// Synchronous gRPC over TLS.
    Grpcs,
    /// A local Unix domain socket.
    Unix,
}

/// Validated endpoint; parsing does not perform DNS resolution or network I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    scheme: TransportScheme,
    host: String,
    port: u16,
    unix_path: Option<PathBuf>,
}

impl Endpoint {
    /// Parse a hostname, IPv4 address, bracketed IPv6 address, or absolute Unix URI.
    /// TCP ports default to 4001. Paths, user information, queries, and fragments
    /// are rejected for TCP endpoints; Unix paths are literal, not percent decoded.
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfig`] for an unsupported or malformed endpoint.
    pub fn parse(raw: &str) -> Result<Self> {
        let invalid = || Error::InvalidConfig("invalid endpoint address".into());
        if raw.is_empty() || raw.chars().any(|ch| ch.is_whitespace() || ch.is_control()) {
            return Err(invalid());
        }
        let (scheme, address) = match raw.split_once("://") {
            Some(("grpc" | "http", address)) => (TransportScheme::Grpc, address),
            Some(("grpcs" | "https", address)) => (TransportScheme::Grpcs, address),
            Some(("unix", address)) => (TransportScheme::Unix, address),
            Some(_) => return Err(Error::InvalidConfig("unsupported endpoint scheme".into())),
            None => (TransportScheme::Grpc, raw),
        };
        if scheme == TransportScheme::Unix {
            if !address.starts_with('/') || address.contains(['?', '#']) || address == "/" {
                return Err(invalid());
            }
            return Ok(Self {
                scheme,
                host: String::new(),
                port: 4001,
                unix_path: Some(PathBuf::from(address)),
            });
        }
        if address.contains(['/', '?', '#', '@', '\\']) {
            return Err(invalid());
        }
        let (host, port) = if let Some(rest) = address.strip_prefix('[') {
            let (host, suffix) = rest.split_once(']').ok_or_else(invalid)?;
            host.parse::<Ipv6Addr>().map_err(|_| invalid())?;
            let port = if suffix.is_empty() {
                4001
            } else {
                parse_port(suffix.strip_prefix(':').ok_or_else(invalid)?)?
            };
            (host, port)
        } else {
            let (host, port) = match address.split_once(':') {
                Some((host, port)) => (host, parse_port(port)?),
                None => (address, 4001),
            };
            validate_host(host)?;
            (host, port)
        };
        Ok(Self {
            scheme,
            host: host.to_owned(),
            port,
            unix_path: None,
        })
    }

    /// Selected transport scheme.
    #[must_use]
    pub const fn scheme(&self) -> TransportScheme {
        self.scheme
    }

    /// Hostname or unbracketed IP address; empty for a Unix endpoint.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// TCP port, defaulting to 4001; unused for Unix endpoints.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// TCP connection address, preserving brackets around IPv6 addresses.
    /// Unix endpoints return their socket path.
    #[must_use]
    pub fn socket_addr_str(&self) -> String {
        if let Some(path) = &self.unix_path {
            return path.to_string_lossy().into_owned();
        }
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// Filesystem socket path for Unix endpoints.
    #[must_use]
    pub fn unix_path(&self) -> Option<&Path> {
        self.unix_path.as_deref()
    }

    /// HTTP/2 authority, or `localhost` for a Unix socket connection.
    #[must_use]
    pub fn authority(&self) -> String {
        if self.scheme == TransportScheme::Unix {
            "localhost".into()
        } else {
            self.socket_addr_str()
        }
    }
}

fn parse_port(raw: &str) -> Result<u16> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::InvalidConfig(
            "endpoint port must be a decimal integer".into(),
        ));
    }
    match raw.parse::<u16>() {
        Ok(port) if port != 0 => Ok(port),
        _ => Err(Error::InvalidConfig(
            "endpoint port must be between 1 and 65535".into(),
        )),
    }
}

fn validate_host(host: &str) -> Result<()> {
    let invalid = || Error::InvalidConfig("invalid endpoint host".into());
    if host.parse::<Ipv4Addr>().is_ok() {
        return Ok(());
    }
    let dns = host.strip_suffix('.').unwrap_or(host);
    if dns.is_empty() || dns.len() > 253 {
        return Err(invalid());
    }
    for label in dns.split('.') {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(invalid());
        }
    }
    // Numeric dotted input must be a valid IPv4 address, not a DNS fallback.
    if dns.contains('.')
        && dns
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return Err(invalid());
    }
    Ok(())
}

/// Credentials for both gRPC metadata and protobuf request headers.
#[derive(Clone, PartialEq, Eq)]
pub enum Auth {
    /// Username and password authentication.
    Basic {
        /// Database username.
        username: String,
        /// Database password.
        password: String,
    },
    /// Bearer token authentication.
    Token(String),
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Basic { username, .. } => formatter
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
            Self::Token(_) => formatter.debug_tuple("Token").field(&"<redacted>").finish(),
        }
    }
}

impl Auth {
    /// Construct username/password credentials.
    /// Empty strings and UTF-8 credentials are preserved. Client option validation
    /// rejects ASCII control characters in either field and colons in the username.
    #[must_use]
    pub fn basic(user: impl Into<String>, pass: impl Into<String>) -> Self {
        Self::Basic {
            username: user.into(),
            password: pass.into(),
        }
    }

    /// Construct bearer token credentials.
    #[must_use]
    pub fn token(token: impl Into<String>) -> Self {
        Self::Token(token.into())
    }

    /// Encode an HTTP-style authentication metadata value.
    #[must_use]
    pub fn to_metadata_value(&self) -> String {
        match self {
            Self::Basic { username, password } => {
                format!(
                    "Basic {}",
                    STANDARD.encode(format!("{username}:{password}"))
                )
            }
            Self::Token(token) => format!("Bearer {token}"),
        }
    }

    /// Construct the matching protobuf authentication oneof.
    #[must_use]
    pub fn to_proto_auth_header(&self) -> v1::AuthHeader {
        let auth_scheme = match self {
            Self::Basic { username, password } => AuthScheme::Basic(v1::Basic {
                username: username.clone(),
                password: password.clone(),
            }),
            Self::Token(token) => AuthScheme::Token(v1::Token {
                token: token.clone(),
            }),
        };
        v1::AuthHeader {
            auth_scheme: Some(auth_scheme),
        }
    }
}

/// Per-message gRPC compression selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Compression {
    /// Send uncompressed messages.
    #[default]
    Identity,
    /// Gzip frames.
    Gzip,
    /// Zlib-wrapped deflate frames.
    Deflate,
    /// Raw Snappy blocks.
    Snappy,
    /// Zstandard frames.
    Zstd,
    /// LZ4 frames.
    Lz4,
}

impl Compression {
    /// Explicitly select the algorithm, including identity, for each RPC.
    #[must_use]
    pub const fn to_grpc(self) -> Option<grpc::CompressionAlgorithm> {
        Some(match self {
            Self::Identity => grpc::CompressionAlgorithm::Identity,
            Self::Gzip => grpc::CompressionAlgorithm::Gzip,
            Self::Deflate => grpc::CompressionAlgorithm::Deflate,
            Self::Snappy => grpc::CompressionAlgorithm::Snappy,
            Self::Zstd => grpc::CompressionAlgorithm::Zstd,
            Self::Lz4 => grpc::CompressionAlgorithm::Lz4,
        })
    }
}

/// Owned connection defaults and database routing for synchronous clients.
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// Ordered endpoints available to the client.
    pub endpoints: Vec<Endpoint>,
    /// Multi-endpoint selection, circuit breaking, and write-replay settings.
    ///
    /// The endpoint names must match [`Self::endpoints`] by count and order.
    /// [`crate::Client::connect`] builds a policy from this snapshot.
    pub failover: Option<FailoverConfig>,
    /// Default database catalog.
    pub catalog: String,
    /// Default database schema.
    pub schema: String,
    /// Optional credentials.
    pub auth: Option<Auth>,
    /// Optional server query timezone.
    pub timezone: Option<String>,
    /// Client-wide ingestion directives.
    pub write_hints: WriteHints,
    /// Outgoing gRPC compression.
    pub compression: Compression,
    /// Maximum time to establish a connection.
    pub connect_timeout: Duration,
    /// Relative deadline attached to each request.
    pub request_timeout: Duration,
    /// Disable Nagle's algorithm on TCP sockets.
    pub tcp_nodelay: bool,
    /// Maximum outstanding bulk stream frames.
    pub max_in_flight: usize,
    /// TLS certificate and trust configuration.
    #[cfg(feature = "tls")]
    pub tls: Option<crate::tls::TlsOptions>,
}

impl ClientOptions {
    /// Construct defaults for an endpoint list. Validate before connecting.
    #[must_use]
    pub fn new(endpoints: Vec<Endpoint>) -> Self {
        Self {
            endpoints,
            failover: None,
            catalog: DEFAULT_CATALOG.into(),
            schema: DEFAULT_SCHEMA.into(),
            auth: None,
            timezone: None,
            write_hints: WriteHints::new(),
            compression: Compression::Identity,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            tcp_nodelay: true,
            max_in_flight: 8,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }

    /// Construct defaults for one endpoint.
    #[must_use]
    pub fn from_endpoint(endpoint: Endpoint) -> Self {
        Self::new(vec![endpoint])
    }

    /// Attach multi-endpoint selection and replay settings.
    ///
    /// The names in `config` must match [`Self::endpoints`] by count and order;
    /// [`Self::validate`] rejects a mismatch.
    #[must_use]
    pub fn with_failover(mut self, config: FailoverConfig) -> Self {
        self.failover = Some(config);
        self
    }

    /// Set the default catalog.
    #[must_use]
    pub fn with_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = catalog.into();
        self
    }

    /// Set the default schema.
    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Split `catalog-schema` at its first hyphen, or set only the schema.
    #[must_use]
    pub fn with_dbname(mut self, dbname: impl Into<String>) -> Self {
        let dbname = dbname.into();
        if let Some((catalog, schema)) = dbname.split_once('-') {
            self.catalog = catalog.into();
            self.schema = schema.into();
        } else {
            self.schema = dbname;
        }
        self
    }

    /// Return the database routing name expected by GreptimeDB.
    #[must_use]
    pub fn dbname(&self) -> String {
        if self.catalog.is_empty() {
            self.schema.clone()
        } else {
            format!("{}-{}", self.catalog, self.schema)
        }
    }

    /// Construct the protobuf request header, including tracing and timezone.
    #[must_use]
    pub fn build_request_header(
        &self,
        tracing_context: Option<HashMap<String, String>>,
    ) -> v1::RequestHeader {
        v1::RequestHeader {
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
            authorization: self.auth.as_ref().map(Auth::to_proto_auth_header),
            dbname: self.dbname(),
            tracing_context: tracing_context.unwrap_or_default(),
            timezone: self.timezone.clone().unwrap_or_default(),
        }
    }

    /// Validate mutable options before constructing a client or RPC.
    ///
    /// # Errors
    /// Rejects empty endpoint lists, schemas, invalid deadlines, zero in-flight
    /// capacity, and a failover endpoint list that does not match the configured
    /// endpoints. Metadata validation is performed by [`Self::try_build_call_options`].
    pub fn validate(&self) -> Result<()> {
        if let Some(failover) = &self.failover {
            if failover.endpoints.len() != self.endpoints.len() {
                return Err(Error::InvalidConfig(
                    "failover endpoint names must match the configured endpoints".into(),
                ));
            }
            // Short display labels remain valid; address lists must identify the
            // same endpoints in the same order as the connection configuration.
            let addresses = failover
                .endpoints
                .iter()
                .filter(|name| name.contains(':'))
                .map(|name| Endpoint::parse(name))
                .collect::<Result<Vec<_>>>();
            if let Ok(addresses) = addresses
                && addresses.len() == failover.endpoints.len()
                && addresses != self.endpoints
            {
                return Err(Error::InvalidConfig(
                    "failover endpoint addresses must match the configured endpoints in order"
                        .into(),
                ));
            }
        }
        if let Some(Auth::Basic { username, password }) = &self.auth {
            if username.contains(':') {
                return Err(Error::InvalidConfig(
                    "Basic authentication usernames must not contain a colon".into(),
                ));
            }
            if username
                .bytes()
                .chain(password.bytes())
                .any(|byte| byte.is_ascii_control())
            {
                return Err(Error::InvalidConfig(
                    "Basic authentication credentials must not contain ASCII control characters"
                        .into(),
                ));
            }
        }
        if self.endpoints.is_empty() || self.schema.is_empty() || self.max_in_flight == 0 {
            return Err(Error::InvalidConfig(
                "endpoints, schema, and in-flight capacity must be nonempty".into(),
            ));
        }
        for timeout in [self.connect_timeout, self.request_timeout] {
            if timeout.is_zero() || Instant::now().checked_add(timeout).is_none() {
                return Err(Error::InvalidConfig(
                    "timeouts must be nonzero and fit a monotonic deadline".into(),
                ));
            }
        }
        grpc::core::format_grpc_timeout(self.request_timeout).map_err(|_| {
            Error::InvalidConfig("request timeout exceeds the gRPC wire limit".into())
        })?;
        Ok(())
    }

    /// Construct call metadata with per-call hints overriding client defaults.
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfig`] for invalid options or non-ASCII/control
    /// characters in database names, tokens, or hint values. No headers are dropped.
    pub fn try_build_call_options(
        &self,
        extra_hints: Option<&WriteHints>,
    ) -> Result<grpc::CallOptions> {
        self.validate()?;
        let mut metadata = grpc::Metadata::new();
        let mut append = |key: &str, value: &str| {
            metadata.append_ascii(key, value).map_err(|_| {
                Error::InvalidConfig(format!("invalid ASCII metadata value for {key}"))
            })
        };
        append(GREPTIME_DB_HEADER, &self.dbname())?;
        if let Some(auth) = &self.auth {
            append(GREPTIME_AUTH_HEADER, &auth.to_metadata_value())?;
        }
        let hints = match extra_hints {
            Some(extra) => self.write_hints.merged_with(extra),
            None => self.write_hints.clone(),
        };
        if let Some(encoded) = hints.encode_header_value() {
            append(GREPTIME_HINTS_HEADER, &encoded)?;
        }
        Ok(grpc::CallOptions {
            metadata,
            timeout: Some(self.request_timeout),
            compression: self.compression.to_grpc(),
        })
    }

    /// Construct call options from validated configuration.
    ///
    /// # Panics
    /// Panics when configuration or outgoing metadata is invalid. Use
    /// [`Self::try_build_call_options`] when configuration comes from untrusted input.
    #[must_use]
    pub fn build_call_options(&self, extra_hints: Option<&WriteHints>) -> grpc::CallOptions {
        match self.try_build_call_options(extra_hints) {
            Ok(options) => options,
            Err(error) => panic!("cannot build GreptimeDB call options: {error}"),
        }
    }
}
