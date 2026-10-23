//! One client per calling OS thread.

use std::cell::RefCell;
use std::collections::{HashMap, hash_map::Entry};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use super::thread_already_gone;
use crate::client::Client;
use crate::config::ClientOptions;
use crate::error::{Error, Result};
use crate::failover::{FailoverConfig, FailoverPolicy};

static NEXT_FACTORY: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static THREAD_CLIENTS: RefCell<HashMap<u64, LocalClient>> = RefCell::new(HashMap::new());
}

struct LocalClient {
    owner: Weak<()>,
    generation: Arc<()>,
    client: Option<Client>,
}

/// One `Client` per calling OS thread.
pub struct ThreadLocalClient {
    id: u64,
    options: ClientOptions,
    lifetime: Arc<()>,
}

impl ThreadLocalClient {
    /// Store options. The client is created on the first [`Self::with_client`] call.
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfig`] when `options` fail validation.
    pub fn new(options: ClientOptions) -> Result<Self> {
        options.validate()?;
        Ok(Self {
            id: NEXT_FACTORY.fetch_add(1, Ordering::Relaxed),
            options,
            lifetime: Arc::new(()),
        })
    }

    /// Store options and a sendable failover snapshot.
    ///
    /// # Errors
    /// Returns [`Error::InvalidConfig`] when options are invalid or `policy`
    /// has an observer. The observer cannot be shared with other threads.
    pub fn with_policy(options: ClientOptions, policy: FailoverPolicy) -> Result<Self> {
        super::reject_observer(&policy)?;
        let mut options = options;
        options.failover = Some(policy.config());
        Self::new(options)
    }

    /// Failover snapshot captured at construction, when one was supplied.
    #[must_use]
    pub fn failover_config(&self) -> Option<&FailoverConfig> {
        self.options.failover.as_ref()
    }

    /// Run `f` on this thread's client, creating it on first use.
    ///
    /// # Errors
    /// Returns a configuration error when `with_client` is already running on
    /// this handle on this thread, or the error from connecting or from `f`.
    pub fn with_client<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Client) -> Result<R>,
    {
        prune_clients(None, false);
        let mut checkout = Checkout::enter(self.id, &self.lifetime)?;
        if checkout.client.is_none() {
            let client = Client::connect(self.options.clone())?;
            checkout.client = Some(client);
        }
        let Some(client) = checkout.client.as_mut() else {
            return Err(Error::InvalidConfig(
                "reentrant client call on this thread".into(),
            ));
        };
        f(client)
    }
}

impl ThreadLocalClient {
    /// Close this factory's cached client on the calling thread.
    ///
    /// An active checkout finishes normally but is not returned to the cache.
    pub fn close_current_thread(&self) -> bool {
        prune_clients(Some(self.id), false).1
    }
}

impl Drop for ThreadLocalClient {
    fn drop(&mut self) {
        self.close_current_thread();
    }
}

/// Remove entries while borrowed and destroy clients only after releasing the borrow.
fn prune_clients(id: Option<u64>, all: bool) -> (usize, bool) {
    let removed = THREAD_CLIENTS.try_with(|cell| {
        let Ok(mut entries) = cell.try_borrow_mut() else {
            return Vec::new();
        };
        let ids: Vec<_> = entries
            .iter()
            .filter_map(|(&key, entry)| {
                (all || Some(key) == id || entry.owner.strong_count() == 0).then_some(key)
            })
            .collect();
        ids.into_iter()
            .filter_map(|key| entries.remove(&key).map(|entry| (key, entry)))
            .collect::<Vec<_>>()
    });
    match removed {
        Ok(entries) => {
            let result = (
                entries.len(),
                entries.iter().any(|(key, _)| Some(*key) == id),
            );
            drop(entries);
            result
        }
        Err(error) => {
            thread_already_gone(error);
            (0, false)
        }
    }
}

pub(super) fn close_thread_caches() -> usize {
    prune_clients(None, true).0
}

struct Checkout {
    id: u64,
    generation: Weak<()>,
    client: Option<Client>,
}

impl Checkout {
    fn enter(id: u64, owner: &Arc<()>) -> Result<Self> {
        THREAD_CLIENTS.with(|cell| {
            let mut entries = cell.borrow_mut();
            match entries.entry(id) {
                Entry::Vacant(entry) => {
                    let generation = Arc::new(());
                    let checkout = Self {
                        id,
                        generation: Arc::downgrade(&generation),
                        client: None,
                    };
                    entry.insert(LocalClient {
                        owner: Arc::downgrade(owner),
                        generation,
                        client: None,
                    });
                    Ok(checkout)
                }
                Entry::Occupied(mut entry) => {
                    let generation = Arc::downgrade(&entry.get().generation);
                    match entry.get_mut().client.take() {
                        Some(client) => Ok(Self {
                            id,
                            generation,
                            client: Some(client),
                        }),
                        None => Err(Error::InvalidConfig(
                            "reentrant client call on this thread".into(),
                        )),
                    }
                }
            }
        })
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        let access = THREAD_CLIENTS.try_with(|cell| {
            let Ok(mut entries) = cell.try_borrow_mut() else {
                return;
            };
            let Some(entry) = entries.get_mut(&self.id) else {
                return;
            };
            if !Weak::ptr_eq(&Arc::downgrade(&entry.generation), &self.generation) {
                return;
            }
            if self.client.is_some() && entry.owner.strong_count() > 0 {
                entry.client = self.client.take();
            } else {
                entries.remove(&self.id);
            }
        });
        if let Err(error) = access {
            thread_already_gone(error);
        }
        // Any client from a retired checkout is destroyed outside the cache borrow.
        drop(self.client.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prune_owner_dropped_on_another_thread() {
        close_thread_caches();
        let owner = Arc::new(());
        let checkout = Checkout::enter(41, &owner).expect("checkout");
        std::thread::spawn(move || drop(owner))
            .join()
            .expect("join");
        assert_eq!(prune_clients(None, false).0, 1);
        drop(checkout);
        assert_eq!(close_thread_caches(), 0);
    }

    #[test]
    fn retired_checkout_cannot_remove_replacement_or_resurrect() {
        close_thread_caches();
        let owner = Arc::new(());
        let first = Checkout::enter(42, &owner).expect("first checkout");
        assert!(prune_clients(Some(42), false).1);
        let second = Checkout::enter(42, &owner).expect("replacement checkout");
        drop(first);
        assert!(Checkout::enter(42, &owner).is_err());
        assert_eq!(close_thread_caches(), 1);
        drop(second);
        assert_eq!(close_thread_caches(), 0);
    }

    #[test]
    fn panic_during_initial_checkout_releases_reservation() {
        close_thread_caches();
        let owner = Arc::new(());
        let result = std::panic::catch_unwind(|| {
            let _checkout = Checkout::enter(43, &owner).expect("checkout");
            panic!("user panic");
        });
        assert!(result.is_err());
        let checkout = Checkout::enter(43, &owner).expect("checkout after panic");
        drop(checkout);
        assert_eq!(close_thread_caches(), 0);
    }
}
