//! Atomic event snapshots for controlled finite peer histories.
use std::sync::atomic::{AtomicUsize, Ordering};

const LABELS: [&str; 4] = ["health", "handle", "prom", "stream"];

pub struct Events {
    next: AtomicUsize,
    slots: [AtomicUsize; 256],
}

impl Events {
    pub fn new() -> Self {
        Self {
            next: AtomicUsize::new(0),
            slots: [const { AtomicUsize::new(0) }; 256],
        }
    }

    /// Records one known fixture event.
    ///
    /// # Panics
    /// Panics for an unknown event or an exhausted fixture budget.
    pub fn push(&self, label: &str) {
        let code = LABELS
            .iter()
            .position(|value| *value == label)
            .expect("known event")
            + 1;
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        assert!(index < self.slots.len(), "finite fixture event budget");
        self.slots[index].store(code, Ordering::Release);
    }

    pub fn snapshot(&self) -> Vec<&'static str> {
        self.slots
            .iter()
            .map(|slot| slot.load(Ordering::Acquire))
            .take_while(|code| *code != 0)
            .map(|code| LABELS[code - 1])
            .collect()
    }
}

impl Default for Events {
    fn default() -> Self {
        Self::new()
    }
}
