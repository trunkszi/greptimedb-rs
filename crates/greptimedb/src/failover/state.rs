//! Circuit metadata and compatibility health snapshots.
use super::CircuitPhase;
use std::time::{Duration, Instant};
use uarch::prelude::*;

bitword! {
    #[derive(Debug)]
    pub(super) struct CircuitWord(u64) {
        [0] pub(super) HEALTHY,
        [1] pub(super) OPEN,
        [2] pub(super) PROBE_RESERVED,
        [32..=63] pub(super) FAILURES: u32,
    }
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
    /// Half-open reservation deadline. A lost outcome expires with the cooldown.
    pub(super) probe_until: Option<Instant>,
    pub(super) word: CircuitWord,
}

impl EndpointHealthState {
    pub(super) fn fresh() -> Self {
        Self {
            consecutive_failures: 0,
            circuit_opened_at: None,
            ewma_rtt_us: 0,
            healthy: true,
            probe_until: None,
            word: CircuitWord::default().patch(patch!(CircuitWord { HEALTHY: true })),
        }
    }

    /// Phase implied by this sample at `now`.
    ///
    /// An open timestamp stays in cooldown until `cooldown` elapses, then becomes
    /// one half-open probe. An active probe reservation stays in cooldown until
    /// its deadline. Without an open timestamp the circuit stays closed:
    /// a clean sample is healthy, and failures at or above `threshold` stay
    /// degraded until a failure record stamps the open timestamp.
    #[inline]
    #[must_use]
    pub fn phase(&self, threshold: u32, cooldown: Duration, now: Instant) -> CircuitPhase {
        if self.probe_reserved(now) {
            return CircuitPhase::OpenCooldown;
        }
        if let Some(opened) = self.circuit_opened_at {
            // `None` means `cooldown` does not fit in `Instant`, so the sample never expires.
            let cooling = opened
                .checked_add(cooldown)
                .is_none_or(|deadline| now < deadline);
            if cooling {
                CircuitPhase::OpenCooldown
            } else {
                CircuitPhase::HalfOpenProbe
            }
        } else if threshold > 0 && self.consecutive_failures >= threshold {
            // Cooldown starts only from `circuit_opened_at`. Failure count alone
            // stays closed, so a later threshold leaves this endpoint eligible.
            CircuitPhase::ClosedDegraded
        } else if self.consecutive_failures == 0 && self.healthy {
            CircuitPhase::ClosedHealthy
        } else {
            CircuitPhase::ClosedDegraded
        }
    }
}

impl EndpointHealthState {
    #[hot_inline]
    pub(super) fn mark_healthy(&mut self) {
        self.word = self.word.patch(patch!(CircuitWord { HEALTHY: true }));
        self.healthy = true;
    }

    #[hot_inline]
    pub(super) fn close(&mut self) {
        self.word = CircuitWord::default().patch(patch!(CircuitWord { HEALTHY: true }));
        self.consecutive_failures = 0;
        self.healthy = true;
        self.circuit_opened_at = None;
        self.probe_until = None;
    }

    #[hot_inline]
    pub(super) fn fail(&mut self, threshold: u32, now: Instant) -> bool {
        let failures = self.word.get(CircuitWord::FAILURES).saturating_add(1);
        let open = self.word.test(CircuitWord::OPEN) || (threshold > 0 && failures >= threshold);
        self.word = self.word.patch(
            patch!(CircuitWord {
                HEALTHY: false,
                PROBE_RESERVED: false
            })
            .with_flag(CircuitWord::OPEN, open)
            .with_field(CircuitWord::FAILURES, u64::from(failures)),
        );
        // Public fields remain snapshots; policy transitions use the packed word.
        self.consecutive_failures = failures;
        self.healthy = false;
        self.probe_until = None;
        if open {
            self.circuit_opened_at = Some(now);
        }
        open
    }

    pub(super) fn reserve_probe(&mut self, now: Instant, cooldown: Duration) {
        self.word = self.word.patch(patch!(CircuitWord {
            PROBE_RESERVED: true
        }));
        // An unrepresentable deadline retains ownership until explicit release.
        self.probe_until = now.checked_add(cooldown);
    }

    pub(super) fn release_probe(&mut self) {
        self.word = self.word.patch(patch!(CircuitWord {
            PROBE_RESERVED: false
        }));
        self.probe_until = None;
    }

    pub(super) fn probe_reserved(&self, now: Instant) -> bool {
        self.word.test(CircuitWord::PROBE_RESERVED)
            && self.probe_until.is_none_or(|until| now < until)
    }

    #[hot_inline]
    pub(super) fn policy_phase(&self, cooldown: Duration, now: Instant) -> CircuitPhase {
        if self
            .word
            .matches(pattern!(CircuitWord { HEALTHY & !OPEN & FAILURES == 0 }))
        {
            CircuitPhase::ClosedHealthy
        } else if !self.word.test(CircuitWord::OPEN) {
            CircuitPhase::ClosedDegraded
        } else {
            self.phase(0, cooldown, now)
        }
    }
}
