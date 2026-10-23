//! Multi-endpoint selection, circuit breaking, and write-replay policy.
//!
//! A write that already placed bytes on the wire is eligible for another
//! endpoint only when the server sets `retry_hint` to `Some(true)`.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::error::{Error, Result, RetryableError};

/// How the next eligible endpoint is chosen.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EndpointSelection {
    /// Walk the configured order.
    #[default]
    RoundRobin,
    /// Draw from the eligible window with a deterministic generator.
    Random,
    /// Prefer the lowest measured round-trip sample.
    LatencyAware,
    /// Stay on the current endpoint until it is ineligible.
    Sticky,
}

/// One observed switch between endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailoverEvent {
    /// Endpoint that was current before the switch.
    pub from_endpoint: String,
    /// Endpoint selected by this call.
    pub to_endpoint: String,
    /// Why the previous endpoint was left.
    pub reason: String,
    /// Candidates examined by the call, counting from 1.
    pub attempt: usize,
}

/// Circuit and latency state for one endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointHealthState {
    /// Failures since the last success.
    pub consecutive_failures: u32,
    /// When the circuit last opened. `None` means the circuit is closed.
    pub circuit_opened_at: Option<Instant>,
    /// Smoothed round trip in microseconds. Zero means no sample yet.
    pub ewma_rtt_us: u64,
    /// Whether the last completed attempt succeeded.
    pub healthy: bool,
}

impl EndpointHealthState {
    fn fresh() -> Self {
        Self {
            consecutive_failures: 0,
            circuit_opened_at: None,
            ewma_rtt_us: 0,
            healthy: true,
        }
    }
}

/// Sendable copy of policy settings. The observer is not included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailoverConfig {
    /// Selection strategy.
    pub selection: EndpointSelection,
    /// Maximum endpoint attempts in one client operation.
    pub max_attempts: usize,
    /// Consecutive failures that open the circuit. Zero disables the breaker.
    pub circuit_breaker_threshold: u32,
    /// How long an open circuit stays ineligible.
    pub circuit_breaker_cooldown: Duration,
    /// Seed for [`EndpointSelection::Random`]. `None` uses the built-in seed.
    pub random_seed: Option<u64>,
    /// Endpoint names in selection order.
    pub endpoints: Vec<String>,
}

type FailoverObserver = Rc<RefCell<dyn FnMut(&FailoverEvent)>>;

/// Per-endpoint health and the rule for choosing the next attempt.
///
/// The observer is shared with `Rc` and is not safe to move across threads.
pub struct FailoverPolicy {
    endpoints: Vec<String>,
    selection: EndpointSelection,
    max_attempts: usize,
    circuit_breaker_threshold: u32,
    circuit_breaker_cooldown: Duration,
    random_seed: Option<u64>,
    rng: u64,
    on_failover: Option<FailoverObserver>,
    health: Vec<EndpointHealthState>,
    cursor: usize,
    current: Option<usize>,
}

const DEFAULT_SEED: u64 = 0x9E37_79B9_7F4A_7C15;
const DEFAULT_THRESHOLD: u32 = 3;
const DEFAULT_COOLDOWN: Duration = Duration::from_secs(30);

impl FailoverPolicy {
    /// Policy for `endpoints` in the given order.
    #[must_use]
    pub fn new(endpoints: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let endpoints: Vec<String> = endpoints.into_iter().map(Into::into).collect();
        let max_attempts = if endpoints.is_empty() {
            0
        } else {
            endpoints.len()
        };
        let health = endpoints
            .iter()
            .map(|_| EndpointHealthState::fresh())
            .collect();
        Self {
            endpoints,
            selection: EndpointSelection::RoundRobin,
            max_attempts,
            circuit_breaker_threshold: DEFAULT_THRESHOLD,
            circuit_breaker_cooldown: DEFAULT_COOLDOWN,
            random_seed: None,
            rng: DEFAULT_SEED,
            on_failover: None,
            health,
            cursor: 0,
            current: None,
        }
    }

    /// Replace the selection strategy.
    #[must_use]
    pub fn with_selection(mut self, selection: EndpointSelection) -> Self {
        self.selection = selection;
        self
    }

    /// Limit endpoint attempts in each client operation.
    ///
    /// Selection skips open circuits without consuming an attempt. Zero makes
    /// [`Self::select_endpoint`] fail before inspecting candidates.
    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: usize) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Open a circuit after `threshold` consecutive failures.
    ///
    /// A threshold of zero never opens the circuit. `cooldown` is how long an
    /// open circuit stays ineligible. After it elapses the endpoint is half-open
    /// and may be selected again; only [`Self::record_success`] closes it.
    #[must_use]
    pub fn with_circuit_breaker(mut self, threshold: u32, cooldown: Duration) -> Self {
        self.circuit_breaker_threshold = threshold;
        self.circuit_breaker_cooldown = cooldown;
        self
    }

    /// Fix the random sequence. Zero is stored as one.
    #[must_use]
    pub fn with_random_seed(mut self, seed: u64) -> Self {
        let seed = seed.max(1);
        self.random_seed = Some(seed);
        self.rng = seed;
        self
    }

    /// Observe each switch. The callback runs on the selecting thread.
    #[must_use]
    pub fn on_failover<F>(mut self, callback: F) -> Self
    where
        F: FnMut(&FailoverEvent) + 'static,
    {
        self.on_failover = Some(Rc::new(RefCell::new(callback)));
        self
    }

    /// Rebuild a policy from a sendable snapshot. The observer is not restored.
    #[must_use]
    pub fn from_config(config: FailoverConfig) -> Self {
        let mut policy = Self::new(config.endpoints).with_selection(config.selection);
        policy.max_attempts = config.max_attempts;
        policy.circuit_breaker_threshold = config.circuit_breaker_threshold;
        policy.circuit_breaker_cooldown = config.circuit_breaker_cooldown;
        if let Some(seed) = config.random_seed {
            policy = policy.with_random_seed(seed);
        }
        policy
    }

    /// Snapshot the settings that can move to another thread.
    #[must_use]
    pub fn config(&self) -> FailoverConfig {
        FailoverConfig {
            selection: self.selection,
            max_attempts: self.max_attempts,
            circuit_breaker_threshold: self.circuit_breaker_threshold,
            circuit_breaker_cooldown: self.circuit_breaker_cooldown,
            random_seed: self.random_seed,
            endpoints: self.endpoints.clone(),
        }
    }

    /// Choose the next endpoint index.
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfig`] when no endpoints are configured.
    /// Returns [`Error::AllEndpointsFailed`] when `max_attempts` is zero or
    /// every examined candidate has an open circuit.
    pub fn select_endpoint(&mut self, now: Instant) -> Result<usize> {
        self.select_endpoint_excluding(now, &[])
    }

    /// Select an eligible endpoint not yet attempted by this operation.
    ///
    /// # Errors
    /// Returns an exhaustion error when no eligible candidate remains.
    pub fn select_endpoint_excluding(&mut self, now: Instant, excluded: &[usize]) -> Result<usize> {
        if self.endpoints.is_empty() {
            return Err(Error::InvalidConfig(
                "failover policy has no endpoints".into(),
            ));
        }
        if self.max_attempts == 0 {
            return Err(self.exhausted(0, 0));
        }
        if self.selection == EndpointSelection::Sticky
            && let Some(current) = self.current
            && !excluded.contains(&current)
            && self.is_available(current, now)
        {
            return Ok(current);
        }

        let count = self.endpoints.len();
        let origin = match self.selection {
            EndpointSelection::Sticky => self.current.map(|index| (index + 1) % count).unwrap_or(0),
            _ => self.cursor % count,
        };
        let mut available = Vec::with_capacity(count);
        let mut examined = 0usize;
        for step in 0..count {
            let index = (origin + step) % count;
            if excluded.contains(&index) {
                continue;
            }
            examined += 1;
            if self.is_available(index, now) {
                available.push(index);
            }
        }
        if available.is_empty() {
            return Err(self.exhausted(examined, (origin + examined.saturating_sub(1)) % count));
        }

        let chosen = match self.selection {
            EndpointSelection::Random => {
                let draw = self.next_random();
                available[usize::try_from(draw % available.len() as u64).unwrap_or(0)]
            }
            EndpointSelection::LatencyAware => lowest_latency(&self.health, &available),
            EndpointSelection::RoundRobin | EndpointSelection::Sticky => available[0],
        };
        self.commit_selection(chosen, examined, now);
        Ok(chosen)
    }

    /// Bind configured endpoint display names without resetting health.
    pub(crate) fn bind_endpoints(&mut self, endpoints: Vec<String>) {
        self.endpoints = endpoints;
    }

    /// Whether an available endpoint requires a half-open health probe.
    #[must_use]
    pub fn needs_probe(&self, index: usize, now: Instant) -> bool {
        self.health
            .get(index)
            .is_some_and(|state| state.circuit_opened_at.is_some())
            && self.is_available(index, now)
    }

    /// Record a dial without clearing circuit history or sampling RPC latency.
    pub fn record_connect_success(&mut self, index: usize) {
        if let Some(state) = self.health.get_mut(index)
            && state.circuit_opened_at.is_none()
        {
            state.healthy = true;
        }
    }

    /// Admit a successful half-open health probe without changing RPC latency.
    pub fn record_probe_success(&mut self, index: usize) {
        if let Some(state) = self.health.get_mut(index) {
            state.healthy = true;
            state.consecutive_failures = 0;
            state.circuit_opened_at = None;
        }
    }

    /// Record successful stream completion without sampling unary latency.
    pub fn record_stream_success(&mut self, index: usize) {
        self.record_probe_success(index);
    }

    /// Record a completed attempt and close the circuit.
    ///
    /// An out-of-range index leaves every endpoint unchanged.
    pub fn record_success(&mut self, idx: usize, rtt: Duration) {
        let Some(state) = self.health.get_mut(idx) else {
            return;
        };
        state.consecutive_failures = 0;
        state.healthy = true;
        state.circuit_opened_at = None;
        state.ewma_rtt_us = smooth_ewma(state.ewma_rtt_us, duration_us(rtt));
    }

    /// Record a failed attempt and open the circuit when the threshold is met.
    ///
    /// An out-of-range index leaves every endpoint unchanged. A threshold of
    /// zero never opens the circuit. A later failure while the circuit is open
    /// or half-open refreshes `circuit_opened_at`.
    pub fn record_failure(&mut self, idx: usize, now: Instant) {
        let threshold = self.circuit_breaker_threshold;
        let Some(state) = self.health.get_mut(idx) else {
            return;
        };
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        state.healthy = false;
        if state.circuit_opened_at.is_some()
            || (threshold > 0 && state.consecutive_failures >= threshold)
        {
            state.circuit_opened_at = Some(now);
        }
    }

    /// Whether a failed write may be sent to another endpoint.
    ///
    /// Returns `true` when nothing was transmitted, or when `retry_hint` is
    /// `Some(true)`. A write that already placed bytes on the wire stays put
    /// when the hint is missing or `Some(false)`.
    #[must_use]
    pub fn should_failover_write(bytes_sent_on_wire: bool, err: &Error) -> bool {
        !bytes_sent_on_wire || RetryableError::retry_hint(err) == Some(true)
    }

    /// Health of one endpoint.
    #[must_use]
    pub fn health(&self, idx: usize) -> Option<&EndpointHealthState> {
        self.health.get(idx)
    }

    /// Configured name of one endpoint.
    #[must_use]
    pub fn endpoint(&self, idx: usize) -> Option<&str> {
        self.endpoints.get(idx).map(String::as_str)
    }

    /// Number of configured endpoints.
    #[must_use]
    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    /// Whether [`Self::new`] received no names.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// Active selection strategy.
    #[must_use]
    pub const fn selection(&self) -> EndpointSelection {
        self.selection
    }

    /// Endpoint attempt limit used by client operations.
    #[must_use]
    pub const fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    /// Whether [`Self::on_failover`] installed a callback.
    #[must_use]
    pub const fn has_observer(&self) -> bool {
        self.on_failover.is_some()
    }

    /// Selection, attempt limit, threshold, cooldown, and random seed.
    #[must_use]
    pub const fn config_parts(&self) -> (EndpointSelection, usize, u32, Duration, Option<u64>) {
        (
            self.selection,
            self.max_attempts,
            self.circuit_breaker_threshold,
            self.circuit_breaker_cooldown,
            self.random_seed,
        )
    }

    /// Configured endpoint names.
    #[must_use]
    pub fn endpoint_names(&self) -> &[String] {
        &self.endpoints
    }

    fn is_available(&self, idx: usize, now: Instant) -> bool {
        match self.health[idx].circuit_opened_at {
            None => true,
            Some(opened) => opened
                .checked_add(self.circuit_breaker_cooldown)
                .is_some_and(|deadline| now >= deadline),
        }
    }

    fn commit_selection(&mut self, chosen: usize, examined: usize, now: Instant) {
        let previous = self.current.replace(chosen);
        let count = self.endpoints.len();
        self.cursor = (chosen + 1) % count;
        let Some(from) = previous.filter(|index| *index != chosen) else {
            return;
        };
        let event = FailoverEvent {
            from_endpoint: self.endpoints[from].clone(),
            to_endpoint: self.endpoints[chosen].clone(),
            reason: switch_reason(
                self.selection,
                &self.health[from],
                now,
                self.circuit_breaker_cooldown,
            )
            .to_owned(),
            attempt: examined.max(1),
        };
        if let Some(callback) = &self.on_failover {
            callback.borrow_mut()(&event);
        }
    }

    fn exhausted(&self, attempts: usize, index: usize) -> Error {
        let endpoint = self
            .endpoints
            .get(index)
            .cloned()
            .unwrap_or_else(|| "unknown".to_owned());
        Error::AllEndpointsFailed {
            attempts,
            last_error: Box::new(Error::CircuitOpen { endpoint }),
        }
    }

    fn next_random(&mut self) -> u64 {
        let mut state = self.rng.max(1);
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.rng = state;
        state
    }
}

impl Clone for FailoverPolicy {
    fn clone(&self) -> Self {
        Self {
            endpoints: self.endpoints.clone(),
            selection: self.selection,
            max_attempts: self.max_attempts,
            circuit_breaker_threshold: self.circuit_breaker_threshold,
            circuit_breaker_cooldown: self.circuit_breaker_cooldown,
            random_seed: self.random_seed,
            rng: self.rng,
            on_failover: self.on_failover.clone(),
            health: self.health.clone(),
            cursor: self.cursor,
            current: self.current,
        }
    }
}

fn lowest_latency(health: &[EndpointHealthState], available: &[usize]) -> usize {
    let any_positive = available.iter().any(|&index| health[index].ewma_rtt_us > 0);
    let mut best = available[0];
    for &index in &available[1..] {
        if latency_rank(health[index].ewma_rtt_us, any_positive)
            < latency_rank(health[best].ewma_rtt_us, any_positive)
        {
            best = index;
        }
    }
    best
}

fn latency_rank(ewma: u64, any_positive: bool) -> u64 {
    if any_positive && ewma == 0 {
        u64::MAX
    } else {
        ewma
    }
}

fn switch_reason(
    selection: EndpointSelection,
    previous: &EndpointHealthState,
    now: Instant,
    cooldown: Duration,
) -> &'static str {
    let open = previous.circuit_opened_at.is_some_and(|opened| {
        opened
            .checked_add(cooldown)
            .is_none_or(|deadline| now < deadline)
    });
    if open {
        "circuit open"
    } else if previous.healthy {
        if selection == EndpointSelection::RoundRobin {
            "round-robin rotation"
        } else {
            "endpoint rotation"
        }
    } else {
        "endpoint unavailable"
    }
}

fn duration_us(rtt: Duration) -> u64 {
    u64::try_from(rtt.as_micros()).unwrap_or(u64::MAX)
}

fn smooth_ewma(current: u64, sample: u64) -> u64 {
    if current == 0 {
        return sample;
    }
    let current = u128::from(current);
    let sample = u128::from(sample);
    let next = current - current / 8 + sample / 8;
    u64::try_from(next).unwrap_or(u64::MAX)
}

const _: fn() = || {
    struct Invalid;
    trait AmbiguousIfImpl<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfImpl<Invalid> for T {}
    let _ambiguous = <FailoverPolicy as AmbiguousIfImpl<_>>::some_item;
};

const _: fn() = || {
    struct Invalid;
    trait AmbiguousIfImpl<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfImpl<Invalid> for T {}
    let _ambiguous = <FailoverPolicy as AmbiguousIfImpl<_>>::some_item;
};
