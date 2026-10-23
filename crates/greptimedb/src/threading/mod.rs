//! Per-thread clients and leased slots.
//!
//! `Client` stays on the OS thread that created it. `ThreadLocalClient` and
//! `WorkerClient` only share configuration. When `WorkerConfig::stripe_endpoints`
//! is set, slot `s` on worker `w` dials endpoints rotated by
//! `(w + s) % endpoints.len()`.

mod thread_local;
mod worker;

pub use thread_local::ThreadLocalClient;
pub use worker::{LeaseGuard, WorkerClient};

use crate::error::{Error, Result};
use crate::failover::FailoverPolicy;

/// How a worker picks a slot on the calling thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkerBalance {
    /// Advance one slot each lease.
    #[default]
    RoundRobin,
    /// Prefer the slot with the fewest active leases. Ties rotate.
    LeastInFlight,
    /// Unkeyed leases use round robin. [`WorkerClient::lease_by_key`] hashes the key.
    KeyHash,
}

/// Slot count and placement for one [`WorkerClient`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConfig {
    /// Clients created on each OS thread. Zero is treated as one.
    pub slots_per_thread: usize,
    /// Unkeyed lease strategy.
    pub balance: WorkerBalance,
    /// Rotate endpoints by `(worker_id + slot) % endpoints.len()`.
    pub stripe_endpoints: bool,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            slots_per_thread: 4,
            balance: WorkerBalance::RoundRobin,
            stripe_endpoints: true,
        }
    }
}

impl WorkerConfig {
    /// Slot count after replacing zero with one.
    #[must_use]
    pub const fn slot_count(&self) -> usize {
        if self.slots_per_thread == 0 {
            1
        } else {
            self.slots_per_thread
        }
    }
}

/// Index of the endpoint that should be dialed first for this worker slot.
#[must_use]
pub fn stripe_shift(endpoint_count: usize, worker_id: u64, slot: usize) -> usize {
    if endpoint_count == 0 {
        0
    } else {
        usize::try_from(worker_id)
            .unwrap_or(usize::MAX)
            .wrapping_add(slot)
            % endpoint_count
    }
}

/// Rotate `endpoints` so index `(worker_id + slot) % len` is first.
///
/// An empty list or `enabled == false` returns the original order.
#[must_use]
pub fn striped_endpoints(
    endpoints: &[String],
    worker_id: u64,
    slot: usize,
    enabled: bool,
) -> Vec<String> {
    let mut rotated = endpoints.to_vec();
    if enabled {
        let shift = stripe_shift(rotated.len(), worker_id, slot);
        if shift != 0 {
            rotated.rotate_left(shift);
        }
    }
    rotated
}

/// Ignore the disappearance of the calling thread's local storage during teardown.
pub(crate) fn thread_already_gone(_: std::thread::AccessError) {}

/// Reject a policy that cannot cross a thread boundary because it observes switches.
pub(crate) fn reject_observer(policy: &FailoverPolicy) -> Result<()> {
    if policy.has_observer() {
        Err(Error::InvalidConfig(
            "failover observers are thread-affine and cannot be shared".into(),
        ))
    } else {
        Ok(())
    }
}

/// Close all cached clients and pools on the calling OS thread.
///
/// Returns the number of removed factory entries. Active checkouts and leases
/// remain usable until dropped, then release their clients without caching them.
#[must_use]
pub fn close_thread_caches() -> usize {
    thread_local::close_thread_caches() + worker::close_thread_caches()
}
