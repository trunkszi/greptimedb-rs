//! Endpoint admission and per-operation outcomes for the thread-affine client.

use std::cell::RefCell;

use super::*;

pub(super) type OutcomeQueue = Rc<RefCell<Vec<(usize, u64, u64, bool)>>>;

/// A stream reports its terminal outcome once, against its channel generation.
pub(crate) struct StreamFeedback {
    queue: OutcomeQueue,
    index: usize,
    generation: u64,
    health_epoch: u64,
    reported: bool,
}

impl StreamFeedback {
    pub(crate) fn report(&mut self, success: bool) {
        if !self.reported {
            self.queue
                .borrow_mut()
                .push((self.index, self.generation, self.health_epoch, success));
            self.reported = true;
        }
    }
}

impl Client {
    pub(super) fn open(
        mut options: ClientOptions,
        supplied: Option<FailoverPolicy>,
    ) -> Result<Self> {
        // An explicit policy owns routing; a stale options snapshot must not
        // invalidate otherwise valid connection settings.
        let config = options.failover.take();
        options.try_build_call_options(None)?;
        options.failover = config;
        let mut policy =
            supplied.or_else(|| options.failover.clone().map(FailoverPolicy::from_config));
        if let Some(policy) = &mut policy {
            if policy.len() != options.endpoints.len() {
                return Err(Error::InvalidConfig(
                    "failover endpoint count does not match the configured endpoints".into(),
                ));
            }
            policy.bind_endpoints(options.endpoints.iter().map(endpoint_name).collect());
        }
        options.failover = policy.as_ref().map(FailoverPolicy::config);
        let count = options.endpoints.len();
        let mut client = Self {
            options,
            channels: (0..count).map(|_| None).collect(),
            active_endpoint_idx: 0,
            failover: policy,
            stream_outcomes: Rc::default(),
            channel_generations: vec![0; count],
            health_epochs: vec![0; count],
            _not_send_sync: PhantomData,
        };
        client.prepare_stream_channel()?;
        Ok(client)
    }

    /// Apply completed stream outcomes before inspecting endpoint health.
    ///
    /// Outcomes from an evicted channel generation are ignored, so an old
    /// stream cannot invalidate a replacement connection or clear its circuit.
    pub fn reconcile_stream_outcomes(&mut self) {
        let outcomes = std::mem::take(&mut *self.stream_outcomes.borrow_mut());
        for (index, generation, epoch, success) in outcomes {
            if self.channel_generations.get(index) != Some(&generation) {
                continue;
            }
            if success {
                if self.health_epochs.get(index) != Some(&epoch) {
                    continue;
                }
                if let Some(policy) = &mut self.failover {
                    policy.record_stream_success(index);
                }
            } else {
                self.record_endpoint_failure(index);
                self.evict(index);
            }
        }
    }

    pub(crate) fn stream_feedback(&self, index: usize) -> StreamFeedback {
        StreamFeedback {
            queue: Rc::clone(&self.stream_outcomes),
            index,
            generation: self.channel_generations[index],
            health_epoch: self.health_epochs[index],
            reported: false,
        }
    }

    fn evict(&mut self, index: usize) {
        if let Some(slot) = self.channels.get_mut(index) {
            *slot = None;
        }
        if let Some(generation) = self.channel_generations.get_mut(index) {
            *generation = generation.wrapping_add(1);
        }
    }

    fn record_endpoint_failure(&mut self, index: usize) {
        if let Some(epoch) = self.health_epochs.get_mut(index) {
            *epoch = epoch.wrapping_add(1);
        }
        if let Some(policy) = &mut self.failover {
            policy.record_failure(index, Instant::now());
        }
    }

    pub(crate) fn attempt_failure(&mut self, index: usize, error: &Error) {
        if endpoint_fault(error) {
            self.record_endpoint_failure(index);
        }
        if terminal_fault(error) {
            self.evict(index);
        }
    }

    pub(crate) fn select(&mut self, attempted: &[usize]) -> Result<usize> {
        let index = match &mut self.failover {
            Some(policy) => policy.select_endpoint_excluding(Instant::now(), attempted)?,
            None => self.active_endpoint_idx,
        };
        self.active_endpoint_idx = index;
        Ok(index)
    }

    pub(crate) fn budget(&self) -> usize {
        self.failover
            .as_ref()
            .map_or(1, |policy| policy.max_attempts().max(1))
    }

    /// Dial and probe an endpoint before allowing a business RPC onto it.
    pub(crate) fn admit(&mut self, index: usize, probe: bool) -> Result<()> {
        let needs_connect = self.channels.get(index).is_some_and(Option::is_none);
        if let Err(error) = channel_for(&mut self.channels, &self.options, index) {
            self.attempt_failure(index, &error);
            return Err(error);
        }
        if needs_connect && let Some(policy) = &mut self.failover {
            policy.record_connect_success(index);
        }
        if probe
            && self
                .failover
                .as_ref()
                .is_some_and(|policy| policy.needs_probe(index, Instant::now()))
        {
            let result: std::result::Result<v1::HealthCheckResponse, _> =
                self.unary_once(index, HEALTH_PATH, &v1::HealthCheckRequest {}, None);
            match result {
                Ok(_) => {
                    if let Some(policy) = &mut self.failover {
                        policy.record_probe_success(index);
                    }
                }
                Err((error, _)) => {
                    // Any failed admission probe reopens the circuit, including
                    // application failures that do not require channel eviction.
                    self.record_endpoint_failure(index);
                    if terminal_fault(&error) {
                        self.evict(index);
                    }
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn prepare_stream_channel(&mut self) -> Result<usize> {
        self.reconcile_stream_outcomes();
        let mut attempted = Vec::new();
        let mut last_error = None;
        for _ in 0..self.budget() {
            let index = match self.select(&attempted) {
                Ok(index) => index,
                Err(error) => return Err(last_error.unwrap_or(error)),
            };
            attempted.push(index);
            match self.admit(index, true) {
                Ok(()) => return Ok(index),
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| Error::Protocol("no endpoint attempt was made".into())))
    }

    pub(crate) fn unary<Req, Resp>(
        &mut self,
        path: &str,
        request: &Req,
        hints: Option<&WriteHints>,
        replay_safe: bool,
        validate: impl Fn(&Resp) -> Result<()>,
    ) -> Result<Resp>
    where
        Req: prost::Message + 'static,
        Resp: prost::Message + Default + 'static,
    {
        self.reconcile_stream_outcomes();
        let mut last_error = None;
        let mut attempted = Vec::new();
        for _ in 0..self.budget() {
            let index = match self.select(&attempted) {
                Ok(index) => index,
                Err(error) => return Err(last_error.unwrap_or(error)),
            };
            attempted.push(index);
            if let Err(error) = self.admit(index, path != HEALTH_PATH) {
                last_error = Some(error);
                continue;
            }
            let started = Instant::now();
            let outcome = self
                .unary_once(index, path, request, hints)
                .and_then(|response| {
                    validate(&response).map_err(|error| (error, true))?;
                    Ok(response)
                });
            match outcome {
                Ok(response) => {
                    if let Some(policy) = &mut self.failover {
                        policy.record_success(index, started.elapsed());
                    }
                    return Ok(response);
                }
                Err((error, sent)) => {
                    let hinted = error.retry_hint() == Some(true);
                    let retry = (endpoint_fault(&error) || hinted)
                        && (replay_safe || FailoverPolicy::should_failover_write(sent, &error));
                    self.attempt_failure(index, &error);
                    last_error = Some(error);
                    if !retry || self.failover.is_none() {
                        break;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| Error::Protocol("no endpoint attempt was made".into())))
    }

    fn unary_once<Req, Resp>(
        &mut self,
        index: usize,
        path: &str,
        request: &Req,
        hints: Option<&WriteHints>,
    ) -> std::result::Result<Resp, (Error, bool)>
    where
        Req: prost::Message + 'static,
        Resp: prost::Message + Default + 'static,
    {
        let options = self
            .options
            .try_build_call_options(hints)
            .map_err(|error| (error, false))?;
        let channel = channel_for(&mut self.channels, &self.options, index)
            .map_err(|error| (error, false))?;
        let mut call = channel
            .server_streaming(path, request, grpc::ProstCodec::<Req, Resp>::new(), options)
            .map_err(|status| (Error::from(status), true))?;
        let mut response = None;
        loop {
            match call.recv_next() {
                Ok(Some(message)) => {
                    if response.replace(message).is_some() {
                        return Err((
                            Error::Protocol("unary RPC returned multiple response messages".into()),
                            true,
                        ));
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let status = match error {
                        grpc::StreamError::Status(status) => status,
                        grpc::StreamError::WouldBlock => {
                            grpc::Status::unavailable("blocking RPC returned pending")
                        }
                    };
                    let trailers = call.trailing_metadata().ok().flatten();
                    return Err((
                        Error::from_grpc_status_and_trailers(status, trailers.as_ref()),
                        true,
                    ));
                }
            }
        }
        let trailers = call
            .trailing_metadata()
            .map_err(stream_error)
            .map_err(|error| (error, true))?;
        validate_ok_trailers(trailers.as_ref()).map_err(|error| (error, true))?;
        response.ok_or_else(|| {
            (
                Error::Protocol("unary RPC returned no response message".into()),
                true,
            )
        })
    }
}

fn endpoint_name(endpoint: &Endpoint) -> String {
    let scheme = match endpoint.scheme() {
        TransportScheme::Grpc => "grpc",
        TransportScheme::Grpcs => "grpcs",
        TransportScheme::Unix => "unix",
    };
    format!("{scheme}://{}", endpoint.socket_addr_str())
}

fn endpoint_fault(error: &Error) -> bool {
    terminal_fault(error)
        || matches!(error, Error::Grpc(status) if status.code() == grpc::core::Code::DeadlineExceeded)
}

pub(crate) fn terminal_fault(error: &Error) -> bool {
    matches!(error, Error::Transport(_))
        || matches!(error, Error::Grpc(status) if status.code() == grpc::core::Code::Unavailable)
}

/// OK gRPC trailers can still carry a nonzero GreptimeDB application status.
pub(crate) fn validate_ok_trailers(metadata: Option<&grpc::Metadata>) -> Result<()> {
    let Some(metadata) = metadata else {
        return Ok(());
    };
    // Parse diagnostics without retry hints first: hints alone carry no failure.
    // Reuse the shared parser's duplicate-code and malformed-JSON rules.
    let mut diagnostics = grpc::Metadata::new();
    for (key, value) in metadata.iter() {
        if (key.eq_ignore_ascii_case("x-greptime-err-code")
            || key.eq_ignore_ascii_case(crate::hints::GREPTIME_INNER_ERROR_HEADER))
            && let grpc::core::MetadataValue::Ascii(value) = value
        {
            diagnostics
                .append_header(key, value)
                .map_err(|error| Error::Protocol(error.to_string()))?;
        }
    }
    let ok_status = || grpc::Status::new(grpc::core::Code::Ok, "");
    if let Error::ServerStatus { raw_code, .. } =
        Error::from_grpc_status_and_trailers(ok_status(), Some(&diagnostics))
        && raw_code != 0
    {
        return Err(Error::from_grpc_status_and_trailers(
            ok_status(),
            Some(metadata),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_trailers_require_usable_nonzero_application_diagnostics() {
        for values in [vec![], vec!["0"], vec!["bogus"], vec!["4003", "4004"]] {
            let mut metadata = grpc::Metadata::new();
            metadata
                .append_ascii(crate::hints::GREPTIME_RETRY_HINT_HEADER, "true")
                .expect("hint");
            for value in values {
                metadata
                    .append_ascii("x-greptime-err-code", value)
                    .expect("code");
            }
            validate_ok_trailers(Some(&metadata)).expect("no usable application failure");
        }
        for inner in [
            r#"{"code":0,"err_msg":"none"}"#,
            r#"{"code":4003}"#,
            "malformed",
        ] {
            let mut metadata = grpc::Metadata::new();
            metadata
                .append_ascii(crate::hints::GREPTIME_RETRY_HINT_HEADER, "true")
                .expect("hint");
            metadata
                .append_ascii(
                    &crate::hints::GREPTIME_INNER_ERROR_HEADER.to_ascii_lowercase(),
                    inner,
                )
                .expect("inner error");
            validate_ok_trailers(Some(&metadata)).expect("no usable application failure");
        }
        let mut metadata = grpc::Metadata::new();
        metadata
            .append_ascii("x-greptime-err-code", "4003")
            .expect("code");
        metadata
            .append_ascii(crate::hints::GREPTIME_RETRY_HINT_HEADER, "true")
            .expect("hint");
        assert!(matches!(
            validate_ok_trailers(Some(&metadata)),
            Err(Error::ServerStatus {
                raw_code: 4003,
                retry_hint: true,
                ..
            })
        ));
    }
    fn client_with_policy() -> Client {
        let endpoint = Endpoint::parse("grpc://127.0.0.1:4001").expect("endpoint");
        Client {
            options: ClientOptions::from_endpoint(endpoint),
            channels: vec![None],
            active_endpoint_idx: 0,
            failover: Some(
                FailoverPolicy::new(["endpoint"]).with_circuit_breaker(1, Duration::from_secs(60)),
            ),
            stream_outcomes: Rc::default(),
            channel_generations: vec![0],
            health_epochs: vec![0],
            _not_send_sync: PhantomData,
        }
    }

    #[test]
    fn stale_stream_outcomes_cannot_change_replacement_connection_health() {
        let mut client = client_with_policy();
        let mut failure = client.stream_feedback(0);
        let mut success = client.stream_feedback(0);
        client.attempt_failure(0, &Error::Grpc(grpc::Status::unavailable("disconnected")));
        failure.report(false);
        success.report(true);
        client.reconcile_stream_outcomes();
        let state = client
            .failover()
            .expect("policy")
            .health(0)
            .expect("health");
        assert_eq!(state.consecutive_failures, 1);
        assert!(state.circuit_opened_at.is_some());
    }

    #[test]
    fn old_success_cannot_clear_a_newer_failure_without_channel_eviction() {
        let mut client = client_with_policy();
        let mut success = client.stream_feedback(0);
        client.record_endpoint_failure(0);
        success.report(true);
        client.reconcile_stream_outcomes();
        assert!(
            client
                .failover()
                .expect("policy")
                .health(0)
                .expect("health")
                .circuit_opened_at
                .is_some()
        );
    }

    #[test]
    fn policy_inspection_does_not_discard_terminal_stream_failure() {
        let mut client = client_with_policy();
        let mut failure = client.stream_feedback(0);
        client.failover_mut().expect("policy");
        failure.report(false);
        failure.report(false);
        client.reconcile_stream_outcomes();
        assert_eq!(client.channel_generations[0], 1);
        assert_eq!(
            client
                .failover()
                .expect("policy")
                .health(0)
                .expect("health")
                .consecutive_failures,
            1
        );
    }

    #[test]
    fn older_stream_success_cannot_override_mutable_policy_changes() {
        let mut client = client_with_policy();
        let mut success = client.stream_feedback(0);
        client
            .failover_mut()
            .expect("policy")
            .record_failure(0, Instant::now());
        success.report(true);
        client.reconcile_stream_outcomes();
        assert!(
            client
                .failover()
                .expect("policy")
                .health(0)
                .expect("health")
                .circuit_opened_at
                .is_some()
        );
    }
}
