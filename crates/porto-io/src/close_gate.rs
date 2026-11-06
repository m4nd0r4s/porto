//! [`CloseGate`]: owns a registration's OS source and closes it exactly once, after the last
//! operation that uses it (13-io-driver.md §4).
//!
//! An operation takes a [`SourceRef`] before its syscall and drops it after. `close` marks the
//! gate closed, so no new operation starts, and itself takes a reference for the time it
//! deregisters the source: the handle stays open while the driver removes it, and a handle
//! number freed by the close cannot be reused by another resource while an operation or the
//! deregistration still names it. Whichever reference is dropped last drops the source, which
//! closes the handle. That is Go's `fdMutex` scheme, minus the read and write serialization.
//!
//! The state word packs a closed bit and the reference count:
//!
//! | bits  | field                                                  |
//! |-------|--------------------------------------------------------|
//! | 0     | `CLOSED`: no new reference can be taken                |
//! | 1..   | references held (operations, and the closer)          |

use std::mem::ManuallyDrop;
#[cfg(not(all(loom, test)))]
use std::ops::Deref;
use std::sync::atomic::Ordering;

use crate::primitives::{AtomicUsize, UnsafeCell};

const CLOSED: usize = 1;
const REF: usize = 2;
/// Far below `usize::MAX`, so a count that would overflow is caught before it wraps.
const MAX_REFS: usize = usize::MAX / 4;

/// A source shared by concurrent operations and closed once, after the last of them.
pub(crate) struct CloseGate<S> {
    state: AtomicUsize,
    /// Read through a [`SourceRef`] only; dropped by whoever releases the last reference after
    /// the gate was closed (or by the gate's own drop if it never was).
    source: UnsafeCell<ManuallyDrop<S>>,
}

// SAFETY: the source is only read through a `SourceRef` (shared access, from any thread: needs
// `S: Sync`) and dropped by exactly one thread, possibly not the one that created it (needs
// `S: Send`). The state word's Acquire/Release orders every use before the drop.
unsafe impl<S: Send + Sync> Send for CloseGate<S> {}
// SAFETY: as above.
unsafe impl<S: Send + Sync> Sync for CloseGate<S> {}

impl<S> CloseGate<S> {
    pub(crate) fn new(source: S) -> Self {
        Self {
            state: AtomicUsize::new(0),
            source: UnsafeCell::new(ManuallyDrop::new(source)),
        }
    }

    /// Whether [`close`](Self::close) was called.
    pub(crate) fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) & CLOSED != 0
    }

    /// A reference to the source for one operation, or `None` once the gate is closed.
    pub(crate) fn acquire(&self) -> Option<SourceRef<'_, S>> {
        self.take_ref(false)
    }

    /// Closes the gate. Returns `None` if it was closed already; otherwise a reference that
    /// keeps the source open until dropped: deregister the source, then drop it. The source is
    /// dropped by then, or by the last operation still using it.
    pub(crate) fn close(&self) -> Option<SourceRef<'_, S>> {
        self.take_ref(true)
    }

    fn take_ref(&self, close: bool) -> Option<SourceRef<'_, S>> {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & CLOSED != 0 {
                return None;
            }
            assert!(
                current < MAX_REFS,
                "porto: too many concurrent operations on one I/O registration"
            );
            let next = (current + REF) | if close { CLOSED } else { 0 };
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(SourceRef { gate: self }),
                Err(actual) => current = actual,
            }
        }
    }

    /// Drops one reference, and the source if it was the last one of a closed gate.
    fn release(&self) {
        // Release: this reference's use of the source happens before the drop. Acquire: the
        // dropper sees every other reference's use.
        let previous = self.state.fetch_sub(REF, Ordering::AcqRel);
        if previous == CLOSED | REF {
            // SAFETY: the gate is closed, so no reference can be taken any more, and this was
            // the last one: nothing else reads the source, and it was not dropped before (only
            // the last release of a closed gate drops it, and a closed gate never reopens).
            self.source
                .with_mut(|source| unsafe { ManuallyDrop::drop(&mut *source) });
        }
    }
}

impl<S> Drop for CloseGate<S> {
    fn drop(&mut self) {
        // `&mut self`: no reference is held. Closed means the last one dropped the source.
        if self.state.load(Ordering::Acquire) & CLOSED == 0 {
            // SAFETY: never closed, so never dropped, and nothing can reach it any more.
            self.source
                .with_mut(|source| unsafe { ManuallyDrop::drop(&mut *source) });
        }
    }
}

/// A reference to a registration's source for one operation: the source stays open while
/// this lives, even if the registration is closed meanwhile. Derefs to the source.
pub struct SourceRef<'a, S> {
    gate: &'a CloseGate<S>,
}

impl<S> SourceRef<'_, S> {
    /// Runs `f` on the source.
    pub(crate) fn with<R>(&self, f: impl FnOnce(&S) -> R) -> R {
        // SAFETY: a held reference keeps the source from being dropped (only the last release
        // drops it), and sources are only ever read through references.
        self.gate.source.with(|source| f(unsafe { &**source }))
    }
}

impl<S> Drop for SourceRef<'_, S> {
    fn drop(&mut self) {
        self.gate.release();
    }
}

// std builds only: loom's cell hands out raw pointers in closures, not references.
#[cfg(not(all(loom, test)))]
impl<S> Deref for SourceRef<'_, S> {
    type Target = S;

    fn deref(&self) -> &S {
        // SAFETY: as in `with`; the reference cannot outlive `self`, which keeps the source.
        self.gate.source.with(|source| unsafe { &**source })
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Counts its drops.
    struct Source(Arc<AtomicUsize>);

    impl Drop for Source {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn gate() -> (CloseGate<Source>, Arc<AtomicUsize>) {
        let drops = Arc::new(AtomicUsize::new(0));
        (CloseGate::new(Source(Arc::clone(&drops))), drops)
    }

    #[test]
    fn close_without_operations_drops_the_source_with_the_closer() {
        let (gate, drops) = gate();
        let closer = gate.close().expect("first close");
        assert!(gate.is_closed());
        assert!(gate.acquire().is_none(), "no operation starts once closed");
        assert_eq!(drops.load(Ordering::SeqCst), 0, "open while deregistering");
        drop(closer);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(gate.close().is_none(), "a second close does nothing");
        drop(gate);
        assert_eq!(drops.load(Ordering::SeqCst), 1, "dropped once");
    }

    #[test]
    fn the_last_operation_drops_a_closed_source() {
        let (gate, drops) = gate();
        let operation = gate.acquire().unwrap();
        drop(gate.close().unwrap());
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "an operation still uses it"
        );
        operation.with(|_| ());
        drop(operation);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_gate_never_closed_drops_its_source() {
        let (gate, drops) = gate();
        drop(gate.acquire().unwrap());
        drop(gate);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[cfg(all(test, loom))]
mod loom_tests;
