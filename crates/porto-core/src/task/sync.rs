//! The synchronization types of the task core: std's, or loom's when the loom models run.

#[cfg(not(all(test, loom)))]
pub(super) use std::sync::atomic::AtomicU32;

#[cfg(all(test, loom))]
pub(super) use loom::sync::atomic::AtomicU32;

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
