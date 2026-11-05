//! The concurrency primitives the wait queue is built from: std's, or loom's under `cfg(loom)`
//! (in loom test builds) so the model checker sees every lock, atomic and cell access.

#[cfg(all(loom, test))]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(all(loom, test))]
pub(crate) use loom::sync::atomic::AtomicU8;
#[cfg(all(loom, test))]
pub(crate) use loom::sync::{Mutex, MutexGuard};

#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::atomic::AtomicU8;
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::{Mutex, MutexGuard};

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
