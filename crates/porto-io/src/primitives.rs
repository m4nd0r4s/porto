//! The primitives the registration's close gate is built from: std's, or loom's under
//! `cfg(loom)` in test builds, so the model checker sees every atomic and every access to the
//! source the gate guards. Run the models with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-io --release loom`.

#[cfg(all(loom, test))]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(all(loom, test))]
pub(crate) use loom::sync::atomic::AtomicUsize;

#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::atomic::AtomicUsize;

/// `std::cell::UnsafeCell` with loom's closure-based API.
#[cfg(not(all(loom, test)))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(all(loom, test)))]
impl<T> UnsafeCell<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
