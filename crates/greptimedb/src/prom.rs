//! Prometheus-compatible query requests and JSON response bodies.

use greptimedb_proto::v1::{
    PromInstantQuery, PromRangeQuery, PromqlRequest, PromqlResponse, promql_request,
};

use crate::client::Client;
use crate::error::{Error, Result};

const PROMETHEUS_HANDLE: &str = "/greptime.v1.PrometheusGateway/Handle";

/// The Prometheus JSON payload returned by the gateway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromQueryResult {
    /// The response body in the Prometheus HTTP API JSON format.
    pub body: Vec<u8>,
}

impl PromQueryResult {
    /// Borrows the response body as UTF-8 without allocating or parsing JSON.
    ///
    /// # Errors
    ///
    /// Returns a protocol error if the response body is not valid UTF-8.
    pub fn as_json_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.body).map_err(|error| Error::Protocol(error.to_string()))
    }
}

impl Client {
    /// Executes a PromQL instant query through the Prometheus gateway.
    ///
    /// Time and lookback strings are passed to GreptimeDB unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error if request configuration, transport, gRPC status, or
    /// GreptimeDB response status rejects the query.
    pub fn prom_instant_query(
        &mut self,
        query: &str,
        time: &str,
        lookback: &str,
    ) -> Result<PromQueryResult> {
        self.prom_query(promql_request::Promql::InstantQuery(PromInstantQuery {
            query: query.to_owned(),
            time: time.to_owned(),
            lookback: lookback.to_owned(),
        }))
    }

    /// Executes a PromQL range query through the Prometheus gateway.
    ///
    /// Start, end, step, and lookback strings are passed to GreptimeDB unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error if request configuration, transport, gRPC status, or
    /// GreptimeDB response status rejects the query.
    pub fn prom_range_query(
        &mut self,
        query: &str,
        start: &str,
        end: &str,
        step: &str,
        lookback: &str,
    ) -> Result<PromQueryResult> {
        self.prom_query(promql_request::Promql::RangeQuery(PromRangeQuery {
            query: query.to_owned(),
            start: start.to_owned(),
            end: end.to_owned(),
            step: step.to_owned(),
            lookback: lookback.to_owned(),
        }))
    }

    fn prom_query(&mut self, promql: promql_request::Promql) -> Result<PromQueryResult> {
        let request = PromqlRequest {
            header: Some(self.options().build_request_header(None)),
            promql: Some(promql),
        };
        let response: PromqlResponse = self.unary(
            PROMETHEUS_HANDLE,
            &request,
            None,
            true,
            |response: &PromqlResponse| Error::from_response_header(response.header.as_ref()),
        )?;
        Ok(PromQueryResult {
            body: response.body,
        })
    }
}
