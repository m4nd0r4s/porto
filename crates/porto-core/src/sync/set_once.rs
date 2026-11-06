//! [`SetOnce`]: an `Arc<T>` that is set at most once and read without a lock.
//!
//! `std::sync::OnceLock::set` goes through `Once`, whose waiter queue costs more than the rest
//! of entering a Python scope (porto-itr.7). A value that is only ever set, never computed by a
//! racing initializer, needs one compare-and-swap: the losers get their `Arc` back.

use std::marker::PhantomData;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

/// An `Arc<T>` set at most once; reads are one atomic load.
///
/// ```
/// use std::sync::Arc;
/// use porto_core::sync::SetOnce;
///
/// let slot = SetOnce::new();
/// assert!(slot.get().is_none());
/// assert!(slot.set(Arc::new(1)).is_ok());
/// assert_eq!(slot.set(Arc::new(2)), Err(Arc::new(2)), "set once: the loser gets it back");
/// assert_eq!(slot.get(), Some(&1));
/// assert_eq!(slot.get_arc().as_deref(), Some(&1));
/// ```
pub struct SetOnce<T> {
    /// Null, or the pointer of the `Arc` the slot owns (`Arc::into_raw`).
    value: AtomicPtr<T>,
    /// The slot owns an `Arc<T>`: it is `Send`/`Sync` exactly when `Arc<T>` is.
    _owns: PhantomData<Arc<T>>,
}

impl<T> SetOnce<T> {
    /// An empty slot.
    pub const fn new() -> Self {
        Self {
            value: AtomicPtr::new(ptr::null_mut()),
            _owns: PhantomData,
        }
    }

    /// Sets the value if the slot is empty; otherwise gives `value` back.
    ///
    /// # Errors
    /// The slot was already set: `value` is returned unchanged.
    pub fn set(&self, value: Arc<T>) -> Result<(), Arc<T>> {
        let raw = Arc::into_raw(value).cast_mut();
        match self
            .value
            .compare_exchange(ptr::null_mut(), raw, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            // SAFETY: `raw` came from `Arc::into_raw` above and was not stored.
            Err(_) => Err(unsafe { Arc::from_raw(raw) }),
        }
    }

    /// The value, if set.
    pub fn get(&self) -> Option<&T> {
        let raw = self.value.load(Ordering::Acquire);
        // SAFETY: non-null means the slot owns an `Arc` at `raw` (Acquire pairs with `set`'s
        // release), which lives until the slot is dropped and is never replaced: the borrow
        // of `self` bounds the reference.
        unsafe { raw.cast_const().as_ref() }
    }

    /// A new reference to the value, if set.
    pub fn get_arc(&self) -> Option<Arc<T>> {
        let raw = self.value.load(Ordering::Acquire).cast_const();
        if raw.is_null() {
            return None;
        }
        // SAFETY: as in `get`, the slot owns a live `Arc` at `raw`: adding a count and taking
        // that new count as an `Arc` is balanced.
        unsafe {
            Arc::increment_strong_count(raw);
            Some(Arc::from_raw(raw))
        }
    }
}

impl<T> Default for SetOnce<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for SetOnce<T> {
    fn drop(&mut self) {
        let raw = *self.value.get_mut();
        if !raw.is_null() {
            // SAFETY: the `Arc` the slot owns (see `set`), released once.
            drop(unsafe { Arc::from_raw(raw) });
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SetOnce<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SetOnce").field(&self.get()).finish()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    struct CountDrops<'a>(&'a AtomicUsize);

    impl Drop for CountDrops<'_> {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn the_slot_owns_one_reference_and_releases_it_once() {
        let drops = AtomicUsize::new(0);
        let slot = SetOnce::new();
        let value = Arc::new(CountDrops(&drops));
        assert!(slot.set(Arc::clone(&value)).is_ok(), "empty slot");
        let again = slot.get_arc().expect("set");
        assert_eq!(Arc::strong_count(&value), 3);
        drop((value, again));
        assert!(slot.set(Arc::new(CountDrops(&drops))).is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 1, "the loser was dropped");
        drop(slot);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn racing_setters_leave_exactly_one_value() {
        let slot = SetOnce::new();
        let winners = AtomicUsize::new(0);
        std::thread::scope(|threads| {
            for n in 0..4 {
                let (slot, winners) = (&slot, &winners);
                threads.spawn(move || {
                    if slot.set(Arc::new(n)).is_ok() {
                        winners.fetch_add(1, Ordering::Relaxed);
                    }
                    assert!(slot.get().is_some());
                });
            }
        });
        assert_eq!(winners.load(Ordering::Relaxed), 1);
    }
}
