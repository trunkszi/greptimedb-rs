//! Structured transport and GreptimeDB application errors.

use std::io;

use sonic_rs::JsonValueTrait;

use crate::hints::{GREPTIME_INNER_ERROR_HEADER, GREPTIME_RETRY_HINT_HEADER};

const GREPTIME_ERROR_CODE_HEADER: &str = "x-greptime-err-code";
const GREPTIME_ERROR_MESSAGE_HEADER: &str = "x-greptime-err-msg";

/// Result of a synchronous GreptimeDB operation.
pub type Result<T> = std::result::Result<T, Error>;

/// GreptimeDB application status categories.
///
/// Authentication codes share one category. The original numeric value remains
/// available in the `raw_code` field of [`Error::ServerStatus`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    /// Successful operation.
    Success,
    /// Unknown failure.
    Unknown,
    /// Unsupported operation.
    Unsupported,
    /// Unexpected failure.
    Unexpected,
    /// Internal failure.
    Internal,
    /// Invalid arguments.
    InvalidArguments,
    /// Cancelled operation.
    Cancelled,
    /// Invalid server state.
    IllegalState,
    /// Query syntax error.
    Syntax,
    /// Query planning error.
    Plan,
    /// Query execution error.
    EngineExecute,
    /// Table already exists.
    TableAlreadyExists,
    /// Table does not exist.
    TableNotFound,
    /// Column does not exist.
    TableColumnNotFound,
    /// Column already exists.
    TableColumnExists,
    /// Database does not exist.
    DatabaseNotFound,
    /// Database already exists.
    DatabaseAlreadyExists,
    /// Storage is unavailable.
    StorageUnavailable,
    /// Region does not exist.
    RegionNotFound,
    /// Region is read-only.
    RegionReadonly,
    /// Region is busy.
    RegionBusy,
    /// Server rate limit exceeded.
    RateLimited,
    /// Flow control rejected the request.
    FlowControl,
    /// Metadata service failure.
    MetaSrv,
    /// Server runtime resources exhausted.
    ResourceExhausted,
    /// Authentication or authorization failure, including v1.2.1 auth subcodes.
    AuthFailed,
    /// Unrecognized status, retaining its exact numeric value.
    Other(u32),
}

impl ErrorCode {
    /// Classifies an application status without discarding unknown values.
    #[must_use]
    pub const fn from_u32(code: u32) -> Self {
        match code {
            0 => Self::Success,
            1000 => Self::Unknown,
            1001 => Self::Unsupported,
            1002 => Self::Unexpected,
            1003 => Self::Internal,
            1004 => Self::InvalidArguments,
            1005 => Self::Cancelled,
            1006 => Self::IllegalState,
            2000 => Self::Syntax,
            2001 => Self::Plan,
            2002 => Self::EngineExecute,
            3000 => Self::TableAlreadyExists,
            3001 => Self::TableNotFound,
            3002 => Self::TableColumnNotFound,
            3003 => Self::TableColumnExists,
            3004 => Self::DatabaseNotFound,
            3005 => Self::DatabaseAlreadyExists,
            4000 => Self::StorageUnavailable,
            4001 => Self::RegionNotFound,
            4002 => Self::RegionReadonly,
            4003 => Self::RegionBusy,
            4004 => Self::RateLimited,
            4005 => Self::FlowControl,
            5000 => Self::MetaSrv,
            6000..=6999 | 7001..=7006 => Self::AuthFailed,
            other => Self::Other(other),
        }
    }

    /// Classifies GreptimeDB v1.2.1 wire codes independently of legacy SDK codes.
    ///
    /// Unrepresented server categories retain their exact value in `Other`.
    /// Code 4003 retains this SDK's explicitly supported region-busy category.
    /// [`Self::as_u32`] still returns legacy canonical codes; retain the original
    /// wire value separately when decoding versioned server responses.
    #[must_use]
    pub const fn from_v121_u32(raw: u32) -> Self {
        match raw {
            0 | 1000..=1006 | 2000 => Self::from_u32(raw),
            3000 => Self::Plan,
            3001 => Self::EngineExecute,
            4000 => Self::TableAlreadyExists,
            4001 => Self::TableNotFound,
            4002 => Self::TableColumnNotFound,
            4003 | 4009 => Self::RegionBusy,
            4004 => Self::DatabaseNotFound,
            4005 => Self::RegionNotFound,
            4007 => Self::RegionReadonly,
            4011 => Self::DatabaseAlreadyExists,
            5000 => Self::StorageUnavailable,
            6000 => Self::ResourceExhausted,
            6001 => Self::RateLimited,
            7000..=7006 => Self::AuthFailed,
            other => Self::Other(other),
        }
    }

    /// Returns the canonical code, using 6000 for the authentication category.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::Success => 0,
            Self::Unknown => 1000,
            Self::Unsupported => 1001,
            Self::Unexpected => 1002,
            Self::Internal => 1003,
            Self::InvalidArguments => 1004,
            Self::Cancelled => 1005,
            Self::IllegalState => 1006,
            Self::Syntax => 2000,
            Self::Plan => 2001,
            Self::EngineExecute => 2002,
            Self::TableAlreadyExists => 3000,
            Self::TableNotFound => 3001,
            Self::TableColumnNotFound => 3002,
            Self::TableColumnExists => 3003,
            Self::DatabaseNotFound => 3004,
            Self::DatabaseAlreadyExists => 3005,
            Self::StorageUnavailable => 4000,
            Self::RegionNotFound => 4001,
            Self::RegionReadonly => 4002,
            Self::RegionBusy => 4003,
            Self::RateLimited => 4004,
            Self::FlowControl => 4005,
            Self::MetaSrv => 5000,
            Self::ResourceExhausted => 6000,
            Self::AuthFailed => 6000,
            Self::Other(code) => code,
        }
    }
}

/// Retry guidance for an unsuccessful operation.
///
/// Callers must separately establish whether a transport failure happened before
/// sending a non-idempotent request. An uncertain in-flight write is never safe
/// to replay merely because a socket or gRPC status suggests a transient fault.
pub trait RetryableError {
    /// Whether the server explicitly grants permission to retry.
    fn is_retryable(&self) -> bool;

    /// The server's explicit guidance, when present.
    fn retry_hint(&self) -> Option<bool>;
}

/// Configuration, wire-protocol, transport, and application failures.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Synchronous transport failure; request delivery may be uncertain.
    #[error("transport: {0}")]
    Transport(#[from] io::Error),
    /// A gRPC failure without a usable GreptimeDB diagnostic.
    #[error("gRPC: {0}")]
    Grpc(#[from] grpc::Status),
    /// Streaming session failure.
    #[error("stream: {0}")]
    Stream(String),
    /// Application failure reported by GreptimeDB.
    #[error("server status {raw_code}: {message}")]
    ServerStatus {
        /// Classified application code.
        code: ErrorCode,
        /// Original code, including authentication subcodes.
        raw_code: u32,
        /// Server diagnostic.
        message: String,
        /// Whether the server explicitly authorizes retrying the operation.
        retry_hint: bool,
    },
    /// Invalid client configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    /// Data does not conform to its declared schema.
    #[error("schema violation: {0}")]
    SchemaViolation(String),
    /// Request encoding failed.
    #[error("encode: {0}")]
    Encode(String),
    /// Response decoding failed.
    #[error("decode: {0}")]
    Decode(String),
    /// Response violated a wire-protocol contract.
    #[error("protocol: {0}")]
    Protocol(String),
    /// Endpoint circuit breaker rejected the operation.
    #[error("circuit open for endpoint {endpoint}")]
    CircuitOpen {
        /// Endpoint whose circuit is open.
        endpoint: String,
    },
    /// Every permitted endpoint attempt failed.
    #[error("all endpoints failed after {attempts} attempts: {last_error}")]
    AllEndpointsFailed {
        /// Number of attempts performed.
        attempts: usize,
        /// Failure from the final attempt.
        #[source]
        last_error: Box<Error>,
    },
}

impl Error {
    /// Checks the application status independently of the enclosing gRPC status.
    ///
    /// Missing optional headers or statuses carry no application error.
    ///
    /// # Errors
    /// Returns a structured error for every nonzero application status.
    pub fn from_response_header(
        header: Option<&greptimedb_proto::v1::ResponseHeader>,
    ) -> Result<()> {
        let Some(status) = header.and_then(|header| header.status.as_ref()) else {
            return Ok(());
        };
        if status.status_code == 0 {
            return Ok(());
        }
        Err(Self::ServerStatus {
            code: ErrorCode::from_u32(status.status_code),
            raw_code: status.status_code,
            message: status.err_msg.clone(),
            retry_hint: parse_retry_hint_token(&status.retry_hint) == Some(true),
        })
    }

    /// Checks application diagnostics on a successful gRPC response.
    ///
    /// Retry guidance alone, an absent code, and code zero do not report errors.
    ///
    /// # Errors
    /// Returns an application error for a nonzero code or a server diagnostic.
    pub fn check_ok_trailers(trailers: &grpc::Metadata) -> Result<()> {
        let has_error = trailers.iter().any(|(key, value)| {
            let grpc::core::MetadataValue::Ascii(value) = value else {
                return false;
            };
            (key.eq_ignore_ascii_case(GREPTIME_ERROR_CODE_HEADER)
                && parse_error_code(value).is_some())
                || (key.eq_ignore_ascii_case(GREPTIME_INNER_ERROR_HEADER)
                    && parse_inner_error(value).is_some())
                || (key.eq_ignore_ascii_case(GREPTIME_ERROR_MESSAGE_HEADER) && !value.is_empty())
        });
        if has_error {
            Err(Self::from_grpc_status_and_trailers(
                grpc::Status::ok(""),
                Some(trailers),
            ))
        } else {
            Ok(())
        }
    }

    /// Extracts GreptimeDB diagnostics from gRPC trailers.
    ///
    /// A nonzero decimal `x-greptime-err-code` is used when no usable inner JSON
    /// exists. Conflicting or malformed duplicate numeric codes are ignored.
    /// Malformed diagnostics alone cannot replace the original gRPC failure. A valid
    /// retry hint without a usable inner diagnostic yields `Unknown` (1000),
    /// retaining the gRPC message. Metadata names
    /// are compared case-insensitively because HTTP/2 sends lowercase names.
    /// Conflicting retry hints deny retry; only an unambiguous `true` grants it.
    #[must_use]
    pub fn from_grpc_status_and_trailers(
        status: grpc::Status,
        metadata: Option<&grpc::Metadata>,
    ) -> Self {
        let Some(metadata) = metadata else {
            return Self::Grpc(status);
        };
        let mut inner_error = None;
        let mut error_message = None;
        let mut numeric_code = None;
        let mut invalid_numeric_code = false;
        let mut hint = true;
        let mut has_valid_hint = false;
        for (key, value) in metadata.iter() {
            let grpc::core::MetadataValue::Ascii(value) = value else {
                continue;
            };
            if key.eq_ignore_ascii_case(GREPTIME_RETRY_HINT_HEADER) {
                let parsed = parse_retry_hint_token(value);
                has_valid_hint |= parsed.is_some();
                hint &= parsed == Some(true);
            } else if key.eq_ignore_ascii_case(GREPTIME_INNER_ERROR_HEADER) && inner_error.is_none()
            {
                inner_error = parse_inner_error(value);
            } else if key.eq_ignore_ascii_case(GREPTIME_ERROR_MESSAGE_HEADER)
                && !value.is_empty()
                && error_message.is_none()
            {
                error_message = Some(value.as_str());
            } else if key.eq_ignore_ascii_case(GREPTIME_ERROR_CODE_HEADER) {
                match parse_error_code(value) {
                    Some(code) if numeric_code.is_none_or(|previous| previous == code) => {
                        numeric_code = Some(code);
                    }
                    _ => invalid_numeric_code = true,
                }
            }
        }
        if let Some((raw_code, message)) = inner_error {
            return Self::ServerStatus {
                code: ErrorCode::from_u32(raw_code),
                raw_code,
                message,
                retry_hint: has_valid_hint && hint,
            };
        }
        let message = if status.message().is_empty() {
            error_message.unwrap_or_default()
        } else {
            status.message()
        };
        if let Some(raw_code) = numeric_code.filter(|_| !invalid_numeric_code) {
            return Self::ServerStatus {
                code: ErrorCode::from_u32(raw_code),
                raw_code,
                message: message.to_owned(),
                retry_hint: has_valid_hint && hint,
            };
        }
        if has_valid_hint || error_message.is_some() {
            return Self::ServerStatus {
                code: ErrorCode::Unknown,
                raw_code: ErrorCode::Unknown.as_u32(),
                message: message.to_owned(),
                retry_hint: has_valid_hint && hint,
            };
        }
        Self::Grpc(status)
    }
}

impl RetryableError for Error {
    fn is_retryable(&self) -> bool {
        self.retry_hint() == Some(true)
    }

    fn retry_hint(&self) -> Option<bool> {
        match self {
            Self::ServerStatus { retry_hint, .. } => Some(*retry_hint),
            Self::AllEndpointsFailed { last_error, .. } => last_error.retry_hint(),
            _ => None,
        }
    }
}

impl From<crate::column_buf::ColumnBufError> for Error {
    /// Maps a typed column buffer rejection to the schema violation it reports.
    fn from(error: crate::column_buf::ColumnBufError) -> Self {
        Self::SchemaViolation(error.to_string())
    }
}

fn parse_retry_hint_token(value: &str) -> Option<bool> {
    match value {
        "true" | "retryable" => Some(true),
        "false" | "non_retryable" => Some(false),
        _ => None,
    }
}

fn parse_error_code(value: &str) -> Option<u32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok().filter(|code| *code != 0)
}

fn parse_inner_error(value: &str) -> Option<(u32, String)> {
    let value: sonic_rs::Value = sonic_rs::from_str(value).ok()?;
    let code = u32::try_from(value.get("code")?.as_u64()?).ok()?;
    if code == 0 {
        return None;
    }
    let message = value.get("err_msg")?.as_str()?.to_owned();
    Some((code, message))
}

#[cfg(test)]
mod tests {
    use crate::column_buf::ColumnBufError;

    use super::Error;

    #[test]
    fn column_buffer_errors_become_schema_violations() {
        for error in [
            ColumnBufError::NullTimestamp,
            ColumnBufError::InvalidDecimal {
                precision: 39,
                scale: 0,
            },
        ] {
            let message = error.to_string();
            let converted = Error::from(error);
            assert!(matches!(converted, Error::SchemaViolation(_)));
            assert!(converted.to_string().contains(&message));
        }
    }
}
