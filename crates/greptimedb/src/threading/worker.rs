//! Leased worker slots, one pool per calling OS thread.

use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use super::{WorkerBalance, WorkerConfig, stripe_shift, thread_already_gone};
use crate::client::Client;
use crate::config::ClientOptions;
use crate::error::{Error, Result};
use crate::failover::{FailoverConfig, FailoverPolicy};

static NEXT_FACTORY: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static LOCAL_POOLS: RefCell<HashMap<u64, LocalPool>> = RefCell::new(HashMap::new());
}

struct LocalPool {
    owner: Weak<()>,
    generation: Arc<()>,
    worker_id: u64,
    slots: Vec<Slot>,
    in_flight: Vec<u32>,
    rr: usize,
}

// A pool holds at most `slots_per_thread` entries on one thread, so keeping the
// client inline is cheaper than allocating on every lease release.
#[allow(clippy::large_enum_variant)]
enum Slot {
    Idle(Option<Client>),
    Leased,
}

impl LocalPool {
    fn new(worker_id: u64, slots: usize, owner: &Arc<()>) -> Self {
        let slots = slots.max(1);
        Self {
            owner: Arc::downgrade(owner),
            generation: Arc::new(()),
            worker_id,
            slots: (0..slots).map(|_| Slot::Idle(None)).collect(),
            in_flight: vec![0; slots],
            rr: 0,
        }
    }
}

/// Shared factory for per-thread leased clients.
pub struct WorkerClient {
    id: u64,
    options: ClientOptions,
    config: WorkerConfig,
    next_worker: AtomicU64,
    lifetime: Arc<()>,
}

impl WorkerClient {
    /// Store options and the slot configuration.
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfig`] when `options` fail validation.
    pub fn new(options: ClientOptions, config: WorkerConfig) -> Result<Self> {
        options.validate()?;
        Ok(Self {
            id: NEXT_FACTORY.fetch_add(1, Ordering::Relaxed),
            options,
            config,
            next_worker: AtomicU64::new(0),
            lifetime: Arc::new(()),
        })
    }

    /// Store options, slot configuration, and a sendable failover snapshot.
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfig`] when options are invalid or `policy`
    /// has an observer.
    pub fn with_policy(
        options: ClientOptions,
        config: WorkerConfig,
        policy: FailoverPolicy,
    ) -> Result<Self> {
        super::reject_observer(&policy)?;
        let mut options = options;
        options.failover = Some(policy.config());
        Self::new(options, config)
    }

    /// Slot configuration.
    #[must_use]
    pub const fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Failover snapshot captured at construction, when one was supplied.
    #[must_use]
    pub fn failover_config(&self) -> Option<&FailoverConfig> {
        self.options.failover.as_ref()
    }

    /// Endpoint order a slot would dial, without connecting.
    #[must_use]
    pub fn preview_endpoints(&self, worker_id: u64, slot: usize) -> Vec<String> {
        let mut names: Vec<String> = self
            .options
            .endpoints
            .iter()
            .map(|endpoint| endpoint.socket_addr_str())
            .collect();
        if self.config.stripe_endpoints {
            let shift = stripe_shift(names.len(), worker_id, slot);
            if shift != 0 {
                names.rotate_left(shift);
            }
        }
        names
    }

    /// Lease the next slot on this thread.
    ///
    /// # Errors
    /// Returns a configuration error when every local slot is already leased,
    /// or the error from connecting the slot.
    pub fn lease(&self) -> Result<LeaseGuard<'_>> {
        self.lease_slot(None)
    }

    /// Lease the slot selected by FNV-1a of `key_hash`.
    ///
    /// # Errors
    /// Returns a configuration error when that slot is already leased, or the
    /// error from connecting it. Other balance settings do not move the key.
    pub fn lease_by_key(&self, key_hash: u64) -> Result<LeaseGuard<'_>> {
        self.lease_slot(Some(key_hash))
    }

    /// Lease a slot, run `f`, and release the slot.
    ///
    /// # Errors
    /// Returns the error from [`Self::lease`] or from `f`.
    pub fn with_client<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Client) -> Result<R>,
    {
        let mut guard = self.lease()?;
        f(&mut guard)
    }

    /// Lease the keyed slot, run `f`, and release the slot.
    ///
    /// # Errors
    /// Returns the error from [`Self::lease_by_key`] or from `f`.
    pub fn with_client_by_key<F, R>(&self, key_hash: u64, f: F) -> Result<R>
    where
        F: FnOnce(&mut Client) -> Result<R>,
    {
        let mut guard = self.lease_by_key(key_hash)?;
        f(&mut guard)
    }

    fn lease_slot(&self, key: Option<u64>) -> Result<LeaseGuard<'_>> {
        prune_pools(None, false);
        let (worker_id, slot, existing, generation) = self.take_slot(key)?;
        let client = match existing {
            Some(client) => client,
            None => match Client::connect(self.prepare_options(worker_id, slot)) {
                Ok(client) => client,
                Err(error) => {
                    self.release_empty(slot);
                    return Err(error);
                }
            },
        };
        Ok(LeaseGuard {
            client: Some(client),
            pool_id: self.id,
            generation,
            slot,
            _lifetime: PhantomData,
            _thread: PhantomData,
        })
    }

    fn take_slot(&self, key: Option<u64>) -> Result<(u64, usize, Option<Client>, Weak<()>)> {
        LOCAL_POOLS.with(|cell| {
            let mut pools = cell.borrow_mut();
            let pool = pools.entry(self.id).or_insert_with(|| {
                LocalPool::new(
                    self.next_worker.fetch_add(1, Ordering::Relaxed),
                    self.config.slot_count(),
                    &self.lifetime,
                )
            });
            let slot = pick_slot(pool, self.config.balance, key)?;
            let existing = match &mut pool.slots[slot] {
                Slot::Leased => {
                    return Err(Error::InvalidConfig("all local slots are leased".into()));
                }
                Slot::Idle(client) => client.take(),
            };
            pool.slots[slot] = Slot::Leased;
            pool.in_flight[slot] = pool.in_flight[slot].saturating_add(1);
            Ok((
                pool.worker_id,
                slot,
                existing,
                Arc::downgrade(&pool.generation),
            ))
        })
    }

    fn release_empty(&self, slot: usize) {
        let access = LOCAL_POOLS.try_with(|cell| {
            let Ok(mut pools) = cell.try_borrow_mut() else {
                return;
            };
            let Some(pool) = pools.get_mut(&self.id) else {
                return;
            };
            if let Some(slot_state) = pool.slots.get_mut(slot) {
                *slot_state = Slot::Idle(None);
            }
            if let Some(in_flight) = pool.in_flight.get_mut(slot) {
                *in_flight = in_flight.saturating_sub(1);
            }
        });
        match access {
            Ok(()) => {}
            Err(error) => thread_already_gone(error),
        }
    }

    fn prepare_options(&self, worker_id: u64, slot: usize) -> ClientOptions {
        let mut options = self.options.clone();
        if self.config.stripe_endpoints {
            let shift = stripe_shift(options.endpoints.len(), worker_id, slot);
            if shift != 0 {
                options.endpoints.rotate_left(shift);
                let endpoint_count = options.endpoints.len();
                if let Some(failover) = options.failover.as_mut()
                    && failover.endpoints.len() == endpoint_count
                {
                    failover.endpoints.rotate_left(shift);
                }
            }
        }
        options
    }
}

impl WorkerClient {
    /// Close this factory's pool on the calling thread.
    ///
    /// Active leases finish normally but are not returned to a retired pool.
    pub fn close_current_thread(&self) -> bool {
        prune_pools(Some(self.id), false).1
    }
}

impl Drop for WorkerClient {
    fn drop(&mut self) {
        self.close_current_thread();
    }
}

fn prune_pools(id: Option<u64>, all: bool) -> (usize, bool) {
    let removed = LOCAL_POOLS.try_with(|cell| {
        let Ok(mut pools) = cell.try_borrow_mut() else {
            return Vec::new();
        };
        let ids: Vec<_> = pools
            .iter()
            .filter_map(|(&key, pool)| {
                (all || Some(key) == id || pool.owner.strong_count() == 0).then_some(key)
            })
            .collect();
        ids.into_iter()
            .filter_map(|key| pools.remove(&key).map(|pool| (key, pool)))
            .collect::<Vec<_>>()
    });
    match removed {
        Ok(pools) => {
            let result = (pools.len(), pools.iter().any(|(key, _)| Some(*key) == id));
            drop(pools);
            result
        }
        Err(error) => {
            thread_already_gone(error);
            (0, false)
        }
    }
}

pub(super) fn close_thread_caches() -> usize {
    prune_pools(None, true).0
}

fn pick_slot(pool: &mut LocalPool, balance: WorkerBalance, key: Option<u64>) -> Result<usize> {
    let count = pool.slots.len().max(1);
    if let Some(key) = key {
        let slot = usize::try_from(fnv1a(key) % u64::try_from(count).unwrap_or(1)).unwrap_or(0);
        return if matches!(pool.slots[slot], Slot::Leased) {
            Err(Error::InvalidConfig("keyed slot is already leased".into()))
        } else {
            Ok(slot)
        };
    }
    match balance {
        WorkerBalance::LeastInFlight => least_in_flight(pool),
        WorkerBalance::RoundRobin | WorkerBalance::KeyHash => round_robin(pool),
    }
}

fn round_robin(pool: &mut LocalPool) -> Result<usize> {
    let count = pool.slots.len();
    for step in 0..count {
        let slot = (pool.rr + step) % count;
        if matches!(pool.slots[slot], Slot::Idle(_)) {
            pool.rr = (slot + 1) % count;
            return Ok(slot);
        }
    }
    Err(Error::InvalidConfig("all local slots are leased".into()))
}

fn least_in_flight(pool: &mut LocalPool) -> Result<usize> {
    let count = pool.slots.len();
    let mut best: Option<(u32, usize)> = None;
    for step in 0..count {
        let slot = (pool.rr + step) % count;
        if matches!(pool.slots[slot], Slot::Leased) {
            continue;
        }
        let load = pool.in_flight[slot];
        if best.is_none_or(|(best_load, _)| load < best_load) {
            best = Some((load, slot));
        }
    }
    let Some((_, slot)) = best else {
        return Err(Error::InvalidConfig("all local slots are leased".into()));
    };
    pool.rr = (slot + 1) % count;
    Ok(slot)
}

fn fnv1a(key: u64) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in key.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// A leased `Client` returned to its slot when dropped.
///
/// The guard is confined to the thread that leased it.
pub struct LeaseGuard<'a> {
    client: Option<Client>,
    pool_id: u64,
    generation: Weak<()>,
    slot: usize,
    _lifetime: PhantomData<&'a WorkerClient>,
    _thread: PhantomData<*const ()>,
}

impl Deref for LeaseGuard<'_> {
    type Target = Client;

    /// # Panics
    /// Panics if the lease was already released. Drop releases it only as the
    /// guard goes out of scope.
    fn deref(&self) -> &Self::Target {
        match &self.client {
            Some(client) => client,
            None => panic!("lease holds a client"),
        }
    }
}

impl DerefMut for LeaseGuard<'_> {
    /// # Panics
    /// Panics if the lease was already released. Drop releases it only as the
    /// guard goes out of scope.
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.client {
            Some(client) => client,
            None => panic!("lease holds a client"),
        }
    }
}

impl Drop for LeaseGuard<'_> {
    fn drop(&mut self) {
        let mut client = self.client.take();
        let access = LOCAL_POOLS.try_with(|cell| {
            let Ok(mut pools) = cell.try_borrow_mut() else {
                return;
            };
            let Some(pool) = pools.get_mut(&self.pool_id) else {
                return;
            };
            if !Weak::ptr_eq(&Arc::downgrade(&pool.generation), &self.generation) {
                return;
            }
            if let Some(slot_state) = pool.slots.get_mut(self.slot) {
                *slot_state = Slot::Idle(client.take());
            }
            if let Some(in_flight) = pool.in_flight.get_mut(self.slot) {
                *in_flight = in_flight.saturating_sub(1);
            }
        });
        match access {
            Ok(()) => {}
            Err(error) => thread_already_gone(error),
        }
        // A retired pool never receives a leased client again.
        drop(client);
    }
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<super::ThreadLocalClient>();
    assert_send_sync::<WorkerClient>();
};

const _: fn() = || {
    struct Invalid;
    trait AmbiguousIfImpl<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfImpl<Invalid> for T {}
    let _client = <Client as AmbiguousIfImpl<_>>::some_item;
    let _guard = <LeaseGuard<'static> as AmbiguousIfImpl<_>>::some_item;
    let _policy = <FailoverPolicy as AmbiguousIfImpl<_>>::some_item;
};

const _: fn() = || {
    struct Invalid;
    trait AmbiguousIfImpl<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfImpl<Invalid> for T {}
    let _client = <Client as AmbiguousIfImpl<_>>::some_item;
    let _guard = <LeaseGuard<'static> as AmbiguousIfImpl<_>>::some_item;
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_pool_owner_dropped_on_another_thread() {
        close_thread_caches();
        let owner = Arc::new(());
        LOCAL_POOLS.with(|cell| {
            cell.borrow_mut().insert(41, LocalPool::new(0, 1, &owner));
        });
        std::thread::spawn(move || drop(owner))
            .join()
            .expect("join");
        assert_eq!(prune_pools(None, false).0, 1);
        assert_eq!(close_thread_caches(), 0);
    }

    #[test]
    fn retired_lease_cannot_modify_replacement_pool() {
        close_thread_caches();
        let owner = Arc::new(());
        let pool = LocalPool::new(0, 1, &owner);
        let guard = LeaseGuard {
            client: None,
            pool_id: 42,
            generation: Arc::downgrade(&pool.generation),
            slot: 0,
            _lifetime: PhantomData,
            _thread: PhantomData,
        };
        LOCAL_POOLS.with(|cell| {
            cell.borrow_mut().insert(42, pool);
        });
        assert!(prune_pools(Some(42), false).1);
        let mut replacement = LocalPool::new(0, 1, &owner);
        replacement.slots[0] = Slot::Leased;
        replacement.in_flight[0] = 1;
        LOCAL_POOLS.with(|cell| {
            cell.borrow_mut().insert(42, replacement);
        });
        drop(guard);
        LOCAL_POOLS.with(|cell| {
            let pools = cell.borrow();
            let pool = pools.get(&42).expect("replacement");
            assert!(matches!(pool.slots[0], Slot::Leased));
            assert_eq!(pool.in_flight[0], 1);
        });
        assert_eq!(close_thread_caches(), 1);
    }
}
