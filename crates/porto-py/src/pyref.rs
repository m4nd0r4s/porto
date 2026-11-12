//! Who may own a Python reference, and what happens to one that is dropped in the wrong place
//! (25-cpython-integration-and-memory.md P1, stress test ST-21).
//!
//! A `Py<T>` dropped on a thread that has no attached thread state is not released at once:
//! pyo3 files it in a global pool, and the next thread that attaches through pyo3 releases it.
//! That is memory-safe but it runs the destructor on an arbitrary thread at an arbitrary time,
//! and for a coroutine (closed by its last decref), a task's context or a result, that is wrong:
//! their code expects the lane. So a reference is *owned* in one of three ways:
//!
//! - [`PyObj`] where the owner always runs attached: a lane, or a blocking worker while it runs
//!   a Python job. Its `Drop` asserts that in debug builds, so a violation of P1 fails a test
//!   instead of shipping as a deferred, misplaced destructor;
//! - [`PyDeferred`] where the owner may be dropped anywhere (a message in flight, a job's
//!   result, a queue entry): its `Drop` releases the reference directly on the owning lane, and
//!   anywhere else queues the release on that lane ([`release_on_lane`]), which runs it at its
//!   next maintenance tick (`Handle::defer`, the `deferred_decref` queue of spec 12 §2);
//! - a body that owns Python references and is freed with its last waker (`PyTask`) moves them
//!   out in [`TaskBody::release`](porto_core::task::TaskBody::release) and hands them to
//!   [`release_to`] when it is not on the lane.
//!
//! # What becomes of a release nobody can run
//!
//! The lane runs its queue until the runtime is dropped, then once more as the runtime closes
//! its queue (the runtime's drop, attached: `lane::run` drops it before it returns). A release
//! after that has no lane: it runs at once if this thread is attached, else it goes to pyo3's
//! pool (the next attach on any thread). Once the interpreter finalizes nothing is run: the
//! reference is leaked, as spec 25 §7 says (a leak, not a crash).

use std::fmt;
use std::mem::{self, ManuallyDrop};
use std::ops::Deref;

use porto_core::{Handle, WeakHandle};
use pyo3::ffi;
use pyo3::prelude::*;

/// Whether this thread has an attached Python thread state (it holds the GIL on a GIL build).
///
/// A thread that Python did not make and never attached has none: `PyGILState_Check` is false
/// for it.
pub(crate) fn thread_is_attached() -> bool {
    // SAFETY: a plain query of this thread's state, callable detached.
    unsafe { ffi::PyGILState_Check() == 1 }
}

/// Whether the interpreter is shutting down: nothing may run Python code (or release what it
/// would run code for) from here on.
pub(crate) fn interpreter_is_finalizing() -> bool {
    // SAFETY: a plain query, callable from any thread, also after the interpreter ended.
    unsafe { ffi::Py_IsFinalizing() != 0 }
}

/// A Python reference owned by code that always runs attached (P1 a, b). Same size as `Py<T>`.
///
/// Dropping one without an attached thread state panics in debug builds (a `debug_assert!`,
/// free in release). Release it from where it was made, or make it a [`PyDeferred`] if its
/// owner can be dropped on any thread. `Deref` reaches the `Py<T>`.
#[repr(transparent)]
pub(crate) struct PyObj<T = PyAny>(ManuallyDrop<Py<T>>);

impl<T> PyObj<T> {
    /// Owns `object`. The caller is attached now, and must be again when this is dropped.
    #[inline]
    pub(crate) fn new(object: Py<T>) -> Self {
        Self(ManuallyDrop::new(object))
    }

    /// The reference, for a caller that is attached (the `Python` token says so).
    #[inline]
    pub(crate) fn into_inner(self, _py: Python<'_>) -> Py<T> {
        self.into_unchecked()
    }

    /// The reference, released from the assertion: for code that is about to hand it to
    /// something that routes its release ([`PyDeferred`], [`release_to`]).
    #[inline]
    pub(crate) fn into_unchecked(self) -> Py<T> {
        let mut this = ManuallyDrop::new(self);
        // SAFETY: `this` is never dropped, so the field is taken exactly once.
        unsafe { ManuallyDrop::take(&mut this.0) }
    }
}

impl<T> From<Py<T>> for PyObj<T> {
    #[inline]
    fn from(object: Py<T>) -> Self {
        Self::new(object)
    }
}

impl<T> Deref for PyObj<T> {
    type Target = Py<T>;
    #[inline]
    fn deref(&self) -> &Py<T> {
        &self.0
    }
}

impl<T> Drop for PyObj<T> {
    #[inline]
    fn drop(&mut self) {
        debug_assert!(
            thread_is_attached(),
            "porto: a Python reference owned by a lane was dropped on a thread with no attached \
             thread state (25 P1); release it on the lane, or hold it as a PyDeferred if its \
             owner can be dropped elsewhere"
        );
        // SAFETY: dropped once, here (`into_unchecked` skips this `Drop`).
        unsafe { ManuallyDrop::drop(&mut self.0) };
    }
}

impl<T> fmt::Debug for PyObj<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PyObj")
    }
}

/// A Python reference that may be dropped on any thread: if that is not the lane of the runtime
/// it belongs to, the release is queued on that lane ([`release_on_lane`]). `Send`, so it can
/// travel in a message, a job or a result.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "first used by the blocking pool (porto-szb.25)")
)]
pub(crate) struct PyDeferred<T: 'static = PyAny> {
    object: Option<Py<T>>,
    home: WeakHandle,
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "first used by the blocking pool (porto-szb.25)")
)]
impl<T: 'static> PyDeferred<T> {
    /// Owns `object` for the lane of `home`.
    pub(crate) fn new(object: Py<T>, home: &Handle) -> Self {
        Self {
            object: Some(object),
            home: home.downgrade(),
        }
    }

    /// The reference, for a caller that is attached.
    pub(crate) fn into_inner(mut self, _py: Python<'_>) -> Py<T> {
        self.object
            .take()
            .expect("a PyDeferred holds its object until it is taken")
    }
}

impl<T: 'static> Drop for PyDeferred<T> {
    fn drop(&mut self) {
        if let Some(object) = self.object.take() {
            release_on_lane(&self.home, object);
        }
    }
}

impl<T: 'static> fmt::Debug for PyDeferred<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PyDeferred")
    }
}

/// Drops `owned` (which owns Python references) where it belongs: here if this thread runs the
/// lane of `home`, else on that lane at its next maintenance tick. See the module docs for what
/// happens once there is no lane.
#[cfg_attr(
    not(test),
    allow(dead_code, reason = "first used by the blocking pool (porto-szb.25)")
)]
pub(crate) fn release_on_lane<O: Send + 'static>(home: &WeakHandle, owned: O) {
    match home.upgrade() {
        Some(handle) => release_to(&handle, owned),
        None => drop_attached(owned),
    }
}

/// [`release_on_lane`] for the runtime `home`.
pub(crate) fn release_to<O: Send + 'static>(home: &Handle, owned: O) {
    if home.is_current_lane() {
        drop_attached(owned);
        return;
    }
    // The queue hands the work back, with `owned` in it, if the runtime is dropped already.
    if let Err(work) = home.defer(move || drop_attached(owned)) {
        work();
    }
}

/// Drops `owned` where it is: on the lane (attached), or with no lane left, directly if this
/// thread is attached, else through pyo3's pool, which releases it when some thread next
/// attaches. Leaks it once the interpreter is finalizing.
fn drop_attached<O>(owned: O) {
    if interpreter_is_finalizing() {
        // The lane must not run Python after finalization began (25 §7): leak instead.
        mem::forget(owned);
    } else {
        drop(owned);
    }
}
