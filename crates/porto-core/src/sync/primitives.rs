//! The concurrency primitives the wait queue and the channels are built from: std's, or loom's
//! under `cfg(loom)` (in loom test builds) so the model checker sees every lock, atomic and
//! cell access.

#[cfg(all(loom, test))]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(all(loom, test))]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, fence};
#[cfg(all(loom, test))]
pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};

#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, fence};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard};

/// Lets other threads run while spinning on a peer's half-finished push or pop. Under loom it
/// yields to the model checker, which would otherwise explore the spin forever.
#[cfg(all(loom, test))]
pub(crate) fn spin(_step: u32) {
    loom::thread::yield_now();
}

/// Lets other threads run while spinning on a peer's half-finished push or pop: a few CPU spin
/// hints first, then the OS scheduler (the peer may have been preempted mid-operation).
#[cfg(not(all(loom, test)))]
pub(crate) fn spin(step: u32) {
    if step < 6 {
        for _ in 0..1u32 << step {
            std::hint::spin_loop();
        }
    } else {
        std::thread::yield_now();
    }
}

/// `std::cell::UnsafeCell` with loom's closure-based API.
#[cfg(not(all(loom, test)))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(all(loom, test)))]
impl<T> UnsafeCell<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
