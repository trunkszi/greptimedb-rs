//! Synchronous, caller-thread GreptimeDB unary operations.

use std::{
    marker::PhantomData,
    rc::Rc,
    time::{Duration, Instant},
};

use greptimedb_proto::v1::{self, greptime_request::Request, greptime_response::Response};

use crate::{
    batch::{BatchEncode, MultiTableBatch},
    config::{ClientOptions, Endpoint, TransportScheme},
    error::{Error, Result, RetryableError},
    failover::FailoverPolicy,
    hints::WriteHints,
    row::Row,
    row_batch::RowBatch,
    transport::EndpointTransport,
};

mod attempt;
use attempt::OutcomeQueue;
pub(crate) use attempt::{StreamFeedback, terminal_fault, validate_ok_trailers};

const DATABASE_PATH: &str = "/greptime.v1.GreptimeDatabase/Handle";
const HEALTH_PATH: &str = "/greptime.v1.HealthCheck/HealthCheck";

/// The result of a successful health-check exchange.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthReport {
    /// Endpoint used for the probe.
    pub endpoint: String,
    /// Elapsed time for the complete health-check RPC.
    pub rtt: Duration,
    /// Whether the health-check RPC succeeded.
    pub healthy: bool,
}

/// A synchronous GreptimeDB connection confined to its owning OS thread.
pub struct Client {
    pub(crate) options: ClientOptions,
    pub(crate) channels: Vec<Option<grpc::SyncChannel<EndpointTransport>>>,
    pub(crate) active_endpoint_idx: usize,
    pub(crate) failover: Option<FailoverPolicy>,
    pub(crate) stream_outcomes: OutcomeQueue,
    pub(crate) channel_generations: Vec<u64>,
    pub(crate) health_epochs: Vec<u64>,
    _not_send_sync: PhantomData<Rc<()>>,
}

impl Client {
    /// Connect using the configured endpoints and failover settings.
    ///
    /// Without [`ClientOptions::failover`] this dials the first configured
    /// endpoint. With a policy this attempts endpoints in policy order and keeps
    /// the first channel that opens.
    ///
    /// # Errors
    /// Returns invalid configuration, connection, TLS, or channel errors.
    pub fn connect(options: ClientOptions) -> Result<Self> {
        Self::open(options, None)
    }

    /// Connect with an explicit failover policy.
    ///
    /// The policy takes precedence over [`ClientOptions::failover`] and must
    /// name the same number of endpoints as [`ClientOptions::endpoints`].
    ///
    /// # Errors
    /// Returns invalid configuration, connection, TLS, or channel errors.
    pub fn connect_with_policy(options: ClientOptions, policy: FailoverPolicy) -> Result<Self> {
        Self::open(options, Some(policy))
    }

    /// Use an existing channel with the supplied routing and RPC defaults.
    ///
    /// The caller is responsible for associating the channel with the first
    /// configured endpoint, whose identity is returned by health reports.
    /// Failover policies apply only to connections opened by [`Self::connect`] or
    /// [`Self::connect_with_policy`].
    ///
    /// Configuration and metadata are validated before each RPC is sent.
    pub fn from_channel(
        options: ClientOptions,
        channel: grpc::SyncChannel<EndpointTransport>,
    ) -> Self {
        Self {
            options,
            channels: vec![Some(channel)],
            active_endpoint_idx: 0,
            failover: None,
            channel_generations: vec![0],
            health_epochs: vec![0],
            stream_outcomes: Rc::default(),
            _not_send_sync: PhantomData,
        }
    }

    /// Use an in-memory transport for synchronous protocol exchanges.
    ///
    /// # Errors
    /// Returns invalid options, metadata, or channel initialization errors.
    pub fn from_memory_pipe(
        options: ClientOptions,
        pipe: grpc::core::transport::MemoryPipe,
    ) -> Result<Self> {
        options.try_build_call_options(None)?;
        let channel = grpc::SyncChannel::with_config(
            EndpointTransport::from_memory_pipe(pipe),
            channel_config(&options.endpoints[0]),
        )?;
        Ok(Self::from_channel(options, channel))
    }

    /// Return the configuration used by this client.
    #[must_use]
    pub const fn options(&self) -> &ClientOptions {
        &self.options
    }

    /// Return the failover policy, when this client was opened with one.
    ///
    /// Call `reconcile_stream_outcomes` first to include detached stream outcomes.
    #[must_use]
    pub const fn failover(&self) -> Option<&FailoverPolicy> {
        self.failover.as_ref()
    }

    /// Return the failover policy for health or selection inspection.
    #[must_use]
    pub fn failover_mut(&mut self) -> Option<&mut FailoverPolicy> {
        self.reconcile_stream_outcomes();
        // Mutable policy access may open a newer circuit. An older stream's
        // success must not close it, while failures still invalidate its socket.
        for epoch in &mut self.health_epochs {
            *epoch = epoch.wrapping_add(1);
        }
        self.failover.as_mut()
    }

    /// Return the active endpoint's channel, dialing it on first use.
    ///
    /// # Errors
    /// Returns a protocol error when the active endpoint index has no configured
    /// endpoint, or connection and channel errors from the lazy dial.
    pub fn channel_mut(&mut self) -> Result<&mut grpc::SyncChannel<EndpointTransport>> {
        let index = self.prepare_stream_channel()?;
        channel_for(&mut self.channels, &self.options, index)
    }

    /// Check whether the connected endpoint accepts health-check RPCs.
    ///
    /// # Errors
    /// Returns transport, deadline, protocol, or server errors.
    pub fn ping(&mut self) -> Result<()> {
        let _: v1::HealthCheckResponse =
            self.unary(HEALTH_PATH, &v1::HealthCheckRequest {}, None, true, |_| {
                Ok(())
            })?;
        Ok(())
    }

    /// Measure a complete health-check exchange with the active endpoint.
    ///
    /// A failed probe may have switched endpoints; the report names the endpoint
    /// that answered. The round trip covers the complete health-check RPC.
    ///
    /// # Errors
    /// Returns errors from the health RPC; failed probes are never healthy reports.
    pub fn probe_health(&mut self) -> Result<HealthReport> {
        let started = Instant::now();
        self.ping()?;
        let rtt = started.elapsed();
        let endpoint = self
            .options
            .endpoints
            .get(self.active_endpoint_idx)
            .ok_or_else(|| Error::Protocol("active endpoint index is out of range".into()))?;
        let endpoint = match endpoint.scheme() {
            TransportScheme::Grpc => format!("grpc://{}", endpoint.socket_addr_str()),
            TransportScheme::Grpcs => format!("grpcs://{}", endpoint.socket_addr_str()),
            TransportScheme::Unix => format!("unix://{}", endpoint.socket_addr_str()),
        };
        Ok(HealthReport {
            endpoint,
            rtt,
            healthy: true,
        })
    }

    /// Buffer typed rows columnarly and insert them into their declared table.
    ///
    /// This convenience method allocates a temporary batch. Repeated ingestion
    /// can retain a batch between calls to [`Self::insert_batch`].
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert<R: Row>(&mut self, rows: &[R]) -> Result<u32> {
        self.insert_to(R::TABLE_NAME, rows)
    }

    /// Buffer typed rows columnarly and insert them into an explicit table.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert_to<R: Row>(&mut self, table_name: &str, rows: &[R]) -> Result<u32> {
        let mut batch = R::new_batch_with_capacity(rows.len());
        for row in rows {
            row.push_into_batch(&mut batch);
        }
        self.insert_batch_to(table_name, &batch)
    }

    /// Insert one columnar batch into its configured table.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert_batch(&mut self, batch: &(impl BatchEncode + ?Sized)) -> Result<u32> {
        self.insert_batch_with_hints(batch, &WriteHints::new())
    }

    /// Insert a columnar batch with additional ingestion directives.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert_batch_with_hints(
        &mut self,
        batch: &(impl BatchEncode + ?Sized),
        hints: &WriteHints,
    ) -> Result<u32> {
        let request = batch.encode_insert_request()?;
        self.handle_request(
            Request::Inserts(v1::InsertRequests {
                inserts: vec![request],
            }),
            Some(hints),
        )
    }

    /// Insert a columnar batch into an explicit destination table.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert_batch_to(
        &mut self,
        table_name: &str,
        batch: &(impl BatchEncode + ?Sized),
    ) -> Result<u32> {
        self.insert_batch_to_with_hints(table_name, batch, &WriteHints::new())
    }

    /// Insert a columnar batch into an explicit table with ingestion directives.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert_batch_to_with_hints(
        &mut self,
        table_name: &str,
        batch: &(impl BatchEncode + ?Sized),
        hints: &WriteHints,
    ) -> Result<u32> {
        let request = batch.encode_insert_request_for(table_name)?;
        self.handle_request(
            Request::Inserts(v1::InsertRequests {
                inserts: vec![request],
            }),
            Some(hints),
        )
    }

    /// Insert several tables in a single request.
    ///
    /// Every batch is encoded before any request is sent.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert_batches(&mut self, batches: &[&dyn BatchEncode]) -> Result<u32> {
        let inserts = batches
            .iter()
            .map(|batch| batch.encode_insert_request())
            .collect::<Result<Vec<_>>>()?;
        self.handle_request(Request::Inserts(v1::InsertRequests { inserts }), None)
    }

    /// Insert the owned request snapshots in a multi-table batch.
    ///
    /// # Errors
    /// Returns transport or server errors.
    pub fn insert_multi_batch(&mut self, batch: &MultiTableBatch) -> Result<u32> {
        self.handle_request(Request::Inserts(batch.to_insert_requests()), None)
    }

    /// Delete rows identified by the batch's tag and timestamp columns.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn delete_batch(&mut self, batch: &(impl BatchEncode + ?Sized)) -> Result<u32> {
        let request = batch.encode_delete_request()?;
        self.handle_request(
            Request::Deletes(v1::DeleteRequests {
                deletes: vec![request],
            }),
            None,
        )
    }

    /// Delete rows from an explicit table using tag and timestamp keys.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn delete_batch_from(
        &mut self,
        table_name: &str,
        batch: &(impl BatchEncode + ?Sized),
    ) -> Result<u32> {
        let request = batch.encode_delete_request_for(table_name)?;
        self.handle_request(
            Request::Deletes(v1::DeleteRequests {
                deletes: vec![request],
            }),
            None,
        )
    }

    /// Insert a row-oriented batch into its configured table.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert_rows(&mut self, batch: &RowBatch) -> Result<u32> {
        let request = batch.encode_row_insert_request()?;
        self.handle_request(
            Request::RowInserts(v1::RowInsertRequests {
                inserts: vec![request],
            }),
            None,
        )
    }

    /// Insert a row-oriented batch into an explicit destination table.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn insert_rows_to(&mut self, table_name: &str, batch: &RowBatch) -> Result<u32> {
        let request = batch.encode_row_insert_request_for(table_name)?;
        self.handle_request(
            Request::RowInserts(v1::RowInsertRequests {
                inserts: vec![request],
            }),
            None,
        )
    }

    /// Delete rows using a row-oriented batch's tag and timestamp keys.
    ///
    /// # Errors
    /// Returns schema, encoding, transport, or server errors.
    pub fn delete_rows(&mut self, batch: &RowBatch) -> Result<u32> {
        let request = batch.encode_row_delete_request()?;
        self.handle_request(
            Request::RowDeletes(v1::RowDeleteRequests {
                deletes: vec![request],
            }),
            None,
        )
    }

    pub(crate) fn handle_request(
        &mut self,
        request: Request,
        hints: Option<&WriteHints>,
    ) -> Result<u32> {
        let request = v1::GreptimeRequest {
            header: Some(self.options.build_request_header(None)),
            request: Some(request),
        };
        let response: v1::GreptimeResponse = self.unary(
            DATABASE_PATH,
            &request,
            hints,
            false,
            |response: &v1::GreptimeResponse| Error::from_response_header(response.header.as_ref()),
        )?;
        match response.response {
            Some(Response::AffectedRows(rows)) => Ok(rows.value),
            _ => Err(Error::Protocol(
                "GreptimeDatabase returned no affected-row result".into(),
            )),
        }
    }
}

/// Borrow the channel of `index`, dialing and caching it when the slot is empty.
pub(crate) fn channel_for<'a>(
    channels: &'a mut [Option<grpc::SyncChannel<EndpointTransport>>],
    options: &ClientOptions,
    index: usize,
) -> Result<&'a mut grpc::SyncChannel<EndpointTransport>> {
    let slot = channels
        .get_mut(index)
        .ok_or_else(|| Error::Protocol("active endpoint index is out of range".into()))?;
    if slot.is_none() {
        let endpoint = options
            .endpoints
            .get(index)
            .ok_or_else(|| Error::Protocol("active endpoint index is out of range".into()))?;
        let transport = connect_transport(endpoint, options)?;
        *slot = Some(grpc::SyncChannel::with_config(
            transport,
            channel_config(endpoint),
        )?);
    }
    slot.as_mut()
        .ok_or_else(|| Error::Protocol("client has no connected channel".into()))
}

fn channel_config(endpoint: &Endpoint) -> grpc::ChannelConfig {
    grpc::ChannelConfig {
        authority: endpoint.authority(),
        scheme: if endpoint.scheme() == TransportScheme::Grpcs {
            "https"
        } else {
            "http"
        }
        .into(),
        ..grpc::ChannelConfig::default()
    }
}

fn connect_transport(endpoint: &Endpoint, options: &ClientOptions) -> Result<EndpointTransport> {
    let address = endpoint.socket_addr_str();
    match endpoint.scheme() {
        TransportScheme::Grpc => EndpointTransport::connect_tcp(
            &address,
            Some(options.connect_timeout),
            Some(options.request_timeout),
            Some(options.request_timeout),
            options.tcp_nodelay,
        )
        .map_err(Error::from),
        TransportScheme::Grpcs => {
            #[cfg(feature = "tls")]
            {
                let tls = options.tls.clone().unwrap_or_default();
                EndpointTransport::connect_tls(
                    &address,
                    endpoint.host(),
                    &tls,
                    Some(options.connect_timeout),
                    Some(options.request_timeout),
                    Some(options.request_timeout),
                    options.tcp_nodelay,
                )
                .map_err(Error::from)
            }
            #[cfg(not(feature = "tls"))]
            {
                Err(Error::InvalidConfig(
                    "TLS endpoint requires the tls feature".into(),
                ))
            }
        }
        TransportScheme::Unix => {
            #[cfg(unix)]
            {
                let path = endpoint.unix_path().ok_or_else(|| {
                    Error::InvalidConfig("Unix endpoint has no socket path".into())
                })?;
                EndpointTransport::connect_unix_with_timeout(
                    path,
                    Some(options.connect_timeout),
                    Some(options.request_timeout),
                    Some(options.request_timeout),
                )
                .map_err(Error::from)
            }
            #[cfg(not(unix))]
            {
                Err(Error::InvalidConfig(
                    "Unix endpoints are unavailable on this platform".into(),
                ))
            }
        }
    }
}

fn stream_error(error: grpc::StreamError) -> Error {
    match error {
        grpc::StreamError::Status(status) => Error::Grpc(status),
        grpc::StreamError::WouldBlock => Error::Protocol("blocking RPC returned pending".into()),
    }
}
