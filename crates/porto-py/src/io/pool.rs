//! The per-lane buffer pool (13-io-driver.md §6): byte buffers in four size classes (4 KiB,
//! 16 KiB, 64 KiB, 256 KiB), reused by the buffers of `porto.Buffer` and the receives that
//! fill one, so a read loop does not allocate per read.
//!
//! [`PooledBytes`] takes its storage from the pool of the Python lane it is made on, and gives
//! it back when dropped on a Python lane; anywhere else it allocates and frees as usual. A
//! request above the largest class is allocated exactly and never pooled. Each class keeps at
//! most [`MAX_POOLED_BYTES_PER_CLASS`] of free storage; the lane empties its pool when its run
//! ends ([`clear`]).

use std::cell::RefCell;
use std::io;
use std::ops::{Deref, DerefMut};

use crate::driver::lane;

/// The capacities of the pooled buffers, smallest first.
pub(crate) const SIZE_CLASSES: [usize; 4] = [4 << 10, 16 << 10, 64 << 10, 256 << 10];

/// The capacity of the largest pooled class: a request above it is allocated exactly and never
/// pooled.
pub(crate) const LARGEST_CLASS: usize = SIZE_CLASSES[SIZE_CLASSES.len() - 1];

/// The most free storage one class keeps: 256 buffers of 4 KiB, ..., 4 of 256 KiB.
const MAX_POOLED_BYTES_PER_CLASS: usize = 1 << 20;

/// How one size class of a lane's pool was used.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) struct ClassStats {
    /// Free buffers now in the pool.
    pub(crate) free: usize,
    /// Requests served from the pool.
    pub(crate) hits: u64,
    /// Requests that allocated (the pool was empty).
    pub(crate) misses: u64,
    /// Buffers dropped instead of pooled (the class was full).
    pub(crate) discarded: u64,
}

#[derive(Default)]
struct LanePool {
    free: [Vec<Vec<u8>>; SIZE_CLASSES.len()],
    stats: [ClassStats; SIZE_CLASSES.len()],
}

thread_local! {
    static POOL: RefCell<LanePool> = RefCell::default();
}

/// The class that holds `capacity` bytes, if one does.
fn class_of(capacity: usize) -> Option<usize> {
    SIZE_CLASSES.iter().position(|&class| capacity <= class)
}

/// An empty byte buffer of capacity at least `capacity`, from this lane's pool when it can be.
pub(crate) struct PooledBytes(Vec<u8>);

impl PooledBytes {
    /// An empty buffer with room for `capacity` bytes.
    ///
    /// # Errors
    ///
    /// `OutOfMemory` when the storage cannot be allocated.
    pub(crate) fn with_capacity(capacity: usize) -> io::Result<Self> {
        if capacity == 0 {
            return Ok(Self(Vec::new()));
        }
        let Some(class) = class_of(capacity) else {
            return allocate(capacity).map(Self);
        };
        if lane::is_active() {
            let reused = POOL.with_borrow_mut(|pool| {
                let reused = pool.free[class].pop();
                let stats = &mut pool.stats[class];
                if reused.is_some() {
                    stats.hits += 1;
                } else {
                    stats.misses += 1;
                }
                reused
            });
            if let Some(bytes) = reused {
                return Ok(Self(bytes));
            }
        }
        allocate(SIZE_CLASSES[class]).map(Self)
    }

    /// The bytes.
    pub(crate) fn vec(&self) -> &Vec<u8> {
        &self.0
    }

    /// The bytes, to change their length or contents. Growing past the capacity moves them to
    /// storage of the right class: use [`PooledBytes::reserve_total`] for that.
    pub(crate) fn vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }

    /// Makes room for `total` bytes, moving the contents to a larger pooled buffer if needed.
    ///
    /// # Errors
    ///
    /// `OutOfMemory` when the storage cannot be allocated.
    pub(crate) fn reserve_total(&mut self, total: usize) -> io::Result<()> {
        if total <= self.0.capacity() {
            return Ok(());
        }
        let mut larger = Self::with_capacity(total)?;
        larger.0.extend_from_slice(&self.0);
        *self = larger;
        Ok(())
    }
}

impl Deref for PooledBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl DerefMut for PooledBytes {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl Drop for PooledBytes {
    fn drop(&mut self) {
        let capacity = self.0.capacity();
        let Some(class) = SIZE_CLASSES.iter().position(|&class| class == capacity) else {
            return;
        };
        if !lane::is_active() {
            return;
        }
        let mut bytes = std::mem::take(&mut self.0);
        bytes.clear();
        // `try_with`: a buffer freed while the thread's locals are destroyed is just freed.
        let _pooled_or_freed = POOL.try_with(|pool| {
            let mut pool = pool.borrow_mut();
            if (pool.free[class].len() + 1) * capacity > MAX_POOLED_BYTES_PER_CLASS {
                pool.stats[class].discarded += 1;
                return false;
            }
            pool.free[class].push(bytes);
            true
        });
    }
}

fn allocate(capacity: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    Ok(bytes)
}

/// The use of each size class of this thread's pool, smallest first.
pub(crate) fn stats() -> [ClassStats; SIZE_CLASSES.len()] {
    POOL.with_borrow(|pool| {
        let mut stats = pool.stats;
        for (class, stats) in stats.iter_mut().enumerate() {
            stats.free = pool.free[class].len();
        }
        stats
    })
}

/// Frees this thread's pooled buffers and resets its statistics: the run on it ended.
pub(crate) fn clear() {
    let pool = POOL.try_with(RefCell::take);
    drop(pool);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_takes_the_smallest_class_that_holds_it() {
        assert_eq!(class_of(0), Some(0));
        assert_eq!(class_of(4096), Some(0));
        assert_eq!(class_of(4097), Some(1));
        assert_eq!(class_of(256 << 10), Some(3));
        assert_eq!(class_of((256 << 10) + 1), None);
        assert_eq!(PooledBytes::with_capacity(0).unwrap().vec().capacity(), 0);
        let bytes = PooledBytes::with_capacity(5000).unwrap();
        assert_eq!(bytes.vec().capacity(), 16 << 10);
        let large = PooledBytes::with_capacity(1 << 20).unwrap();
        assert_eq!(large.vec().capacity(), 1 << 20, "exact, never pooled");
    }

    #[test]
    fn growing_moves_the_contents_to_a_larger_class() {
        let mut bytes = PooledBytes::with_capacity(10).unwrap();
        bytes.vec_mut().extend_from_slice(b"kept");
        bytes.reserve_total(20_000).unwrap();
        assert_eq!(bytes.vec().capacity(), 64 << 10);
        assert_eq!(&*bytes, b"kept");
    }

    #[test]
    fn off_a_lane_nothing_is_pooled() {
        drop(PooledBytes::with_capacity(100).unwrap());
        assert_eq!(stats()[0], ClassStats::default());
    }

    #[test]
    fn an_impossible_size_is_an_out_of_memory_error() {
        let error = PooledBytes::with_capacity(usize::MAX).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    }
}
