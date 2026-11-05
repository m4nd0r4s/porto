//! `porto.Lock`: an async lock with fair FIFO handoff.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::{Mutex, MutexGuard, WaitNode};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::blocking;
use super::wait_op::{Grant, SyncWait};
use crate::api::time::require_lane;
use crate::driver::ready::Ready;
use crate::driver::step::checkpoint_if_needed;
use crate::driver::thin::{WaitSource, Waitable};

/// The lock: a `Mutex<()>` held without guards, since a Python holder is not a Rust scope.
pub(super) struct LockSource(Mutex<()>);

// SAFETY: forwards to the mutex's node API, whose contract is `WaitSource`'s. A ready poll
// means the waiting task holds the lock.
unsafe impl WaitSource for LockSource {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.0.poll_lock_node(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.0.cancel_lock_node(node) }
    }
}

impl LockSource {
    /// Takes the lock if it is free; the holder releases it with [`LockSource::unlock`].
    pub(super) fn try_lock(&self) -> bool {
        self.0.try_lock().map(MutexGuard::leak).is_some()
    }

    /// Releases the lock, handing it to the first waiter; `false` if it was not locked.
    pub(super) fn unlock(&self) -> bool {
        // SAFETY: the value is `()`: no reference to it outlives the lock.
        unsafe { self.0.force_unlock() }
    }
}

/// `porto.Lock()`: an async lock. Fair: waiters get it in arrival order, and `release()`
/// hands it straight to the first one. Not reentrant: acquiring it again from the task that
/// holds it waits forever. It does not record its holder (like `asyncio.Lock`): a `release()`
/// from a task that does not hold it, or a second one, releases it from under the holder.
#[pyclass(frozen, module = "porto", name = "Lock")]
pub(super) struct Lock {
    lock: Arc<Waitable<LockSource>>,
}

#[pymethods]
impl Lock {
    #[new]
    fn new() -> Self {
        Self {
            lock: Waitable::new(LockSource(Mutex::new(()))),
        }
    }

    /// An awaitable that completes once the task holds the lock: without suspending if it is
    /// free. Nothing happens until it is awaited, and the lock is only taken in the step that
    /// resumes the task with it: cancelling or closing the task never leaks it.
    fn acquire(&self) -> PyResult<SyncWait> {
        require_lane("Lock.acquire")?;
        Ok(SyncWait::new(Grant::Locked(Arc::clone(&self.lock))))
    }

    /// `acquire()`, then `on_acquired()` in the same step; the await evaluates to its result
    /// (if it raises, the lock is released and the await raises). Internal: `porto.Mutex`
    /// enters through it, so its guard exists exactly when the lock is held.
    fn _acquire_then(&self, on_acquired: Py<PyAny>) -> PyResult<SyncWait> {
        require_lane("Mutex.__aenter__")?;
        Ok(SyncWait::calling(
            Grant::Locked(Arc::clone(&self.lock)),
            on_acquired,
        ))
    }

    /// Takes the lock if it is free right now; returns whether it did. Never waits.
    fn try_acquire(&self) -> bool {
        self.lock.try_lock()
    }

    /// Releases the lock, handing it to the task that has waited longest. Raises
    /// `RuntimeError` if the lock is not held. The holder is not recorded: a release from a
    /// task that does not hold it is not detected while some task does.
    fn release(&self) -> PyResult<()> {
        if self.lock.unlock() {
            return Ok(());
        }
        Err(PyRuntimeError::new_err(
            "porto: Lock.release() was called on a lock that is not held; release a lock once \
             per acquire, from the task that acquired it (`async with lock:` does it for you). \
             If this task did hold it, another task released it first (the lock does not \
             record its holder): find the extra release()",
        ))
    }

    /// Whether the lock is held (or being handed to a waiter).
    fn locked(&self) -> bool {
        self.lock.0.is_locked()
    }

    /// Blocks the calling thread until it holds the lock; release it with `release()`. Only
    /// for threads that do not run porto: on a lane thread it raises `RuntimeError`.
    fn blocking_acquire(&self, py: Python<'_>) -> PyResult<()> {
        blocking(py, "Lock.blocking_acquire", || {
            self.lock.0.blocking_lock().map(MutexGuard::leak)
        })
    }

    fn __aenter__(&self) -> PyResult<SyncWait> {
        self.acquire()
    }

    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __aexit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        exc: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.release()?;
        // An exception is in flight: no checkpoint, which could raise `Cancelled` in its place
        // (and a cancelled scope would then absorb it, losing the original error).
        if !exc.is_none() {
            return Ok(Py::new(py, Ready::new(py.None()))?.into_any());
        }
        // After the release: a checkpoint here (T2) never holds the lock.
        checkpoint_if_needed(py, py.None())
    }

    fn __repr__(&self) -> &'static str {
        if self.lock.0.is_locked() {
            "<porto.Lock locked>"
        } else {
            "<porto.Lock unlocked>"
        }
    }
}
