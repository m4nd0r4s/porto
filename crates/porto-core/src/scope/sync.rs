//! The synchronization types of the scope tree: std's (and the crate's short lock), or loom's
//! when the loom models run.

#[cfg(not(all(test, loom)))]
pub(super) use crate::sync::primitives::{Mutex, MutexGuard};
#[cfg(not(all(test, loom)))]
pub(super) use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicU64},
};

#[cfg(all(test, loom))]
pub(super) use loom::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicU8, AtomicU64},
};

/// Bumped (after the scope's own bits and epochs) by every change that can alter a walk's
/// result: a cancel or a shield change anywhere. A check that sees it unchanged needs no walk.
#[cfg(not(all(test, loom)))]
pub(super) static CANCEL_GENERATION: AtomicU64 = AtomicU64::new(0);

#[cfg(all(test, loom))]
loom::lazy_static! {
    pub(super) static ref CANCEL_GENERATION: AtomicU64 = AtomicU64::new(0);
}

/// An `UnsafeCell` with loom's closure API, so loom checks the accesses in its models.
#[cfg(not(all(test, loom)))]
#[derive(Debug)]
pub(super) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(all(test, loom)))]
impl<T> UnsafeCell<T> {
    pub(super) const fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    pub(super) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    pub(super) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

#[cfg(all(test, loom))]
pub(super) use loom::cell::UnsafeCell;
