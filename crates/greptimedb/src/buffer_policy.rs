//! Capacity limits for reusable column buffers.

/// Default number of rows retained when a reusable buffer is cleared.
pub const DEFAULT_MAX_RETAINED_ROWS: usize = 65_536;
/// Default number of allocated value-buffer bytes retained after clearing.
pub const DEFAULT_MAX_RETAINED_BYTES: usize = 8 * 1024 * 1024;

/// Upper watermarks for retaining reusable value allocations after a clear.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferPolicy {
    /// Maximum retained row capacity.
    pub max_retained_rows: usize,
    /// Maximum retained heap allocation in bytes.
    pub max_retained_bytes: usize,
}

impl BufferPolicy {
    /// Create independent row and byte retention limits.
    pub const fn new(max_retained_rows: usize, max_retained_bytes: usize) -> Self {
        Self {
            max_retained_rows,
            max_retained_bytes,
        }
    }

    /// Keep warmed allocations regardless of their size.
    pub const fn unbounded() -> Self {
        Self::new(usize::MAX, usize::MAX)
    }

    /// Release value-buffer allocations after clearing.
    pub const fn zero() -> Self {
        Self::new(0, 0)
    }

    /// Whether either allocation watermark has been exceeded.
    pub const fn should_shrink(&self, row_capacity: usize, allocated_bytes: usize) -> bool {
        row_capacity > self.max_retained_rows || allocated_bytes > self.max_retained_bytes
    }
}

impl Default for BufferPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_RETAINED_ROWS, DEFAULT_MAX_RETAINED_BYTES)
    }
}

/// Alias for [`BufferPolicy`] naming its role as a retention policy.
pub type BufferRetentionPolicy = BufferPolicy;

/// Capacity control shared by null masks and typed value buffers.
///
/// Allocation accounting includes retained nested value allocations and excludes
/// schema metadata and allocator bookkeeping. Clearing discards rows; shrinking
/// alone preserves all rows.
pub trait MemoryBuffer {
    /// Return retained value-buffer allocation capacity in bytes.
    fn allocated_bytes(&self) -> usize;
    /// Return the largest retained capacity among row-oriented buffers.
    fn row_capacity(&self) -> usize;
    /// Discard rows and reclaim allocations that exceed the supplied policy.
    fn clear_with_policy(&mut self, policy: BufferPolicy);
    /// Discard rows using the default warm-capacity watermarks.
    fn clear(&mut self) {
        self.clear_with_policy(BufferPolicy::default());
    }
    /// Discard rows while retaining warmed allocations.
    fn clear_unbounded(&mut self) {
        self.clear_with_policy(BufferPolicy::unbounded());
    }
    /// Release unused capacity while retaining all current rows.
    fn shrink_to_fit(&mut self);
}
