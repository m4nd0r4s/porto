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
use crate::driver::ready::ready_none;
use crate::driver::step::{self, checkpoint_if_needed};
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
    /// A new, free lock to wait on.
    pub(super) fn waitable() -> Arc<Waitable<Self>> {
        Waitable::new(Self(Mutex::new(())))
    }

    /// Whether the lock is held (or being handed to a waiter).
    pub(super) fn is_locked(&self) -> bool {
        self.0.is_locked()
    }

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
            lock: LockSource::waitable(),
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
    /// (if it raises, the lock is released and the await raises). Internal: the lazy wait
    /// `porto.Mutex` enters through when its lock is held, exposed for the tests.
    fn _acquire_then(&self, on_acquired: Py<PyAny>) -> PyResult<SyncWait> {
        require_lane("Lock._acquire_then")?;
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
        release(&self.lock)
    }

    /// Whether the lock is held (or being handed to a waiter).
    fn locked(&self) -> bool {
        self.lock.is_locked()
    }

    /// Blocks the calling thread until it holds the lock; release it with `release()`. Only
    /// for threads that do not run porto: on a lane thread it raises `RuntimeError`.
    fn blocking_acquire(&self, py: Python<'_>) -> PyResult<()> {
        blocking(py, "Lock.blocking_acquire", || {
            self.lock.0.blocking_lock().map(MutexGuard::leak)
        })
    }

    /// `async with lock:`. A free lock is taken in this call (T2, `step::take_now`), and the
    /// shared `_Ready(None)` comes back: no allocation and no suspension. `async with` awaits it
    /// in the same step, so the coroutine holds the lock before anything else runs and a task
    /// cancelled or closed at a suspension point never holds it unobserved (spec 16 §1). A
    /// held lock, a spent budget or a call outside a running task get `acquire()`'s lazy wait.
    fn __aenter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        require_lane("Lock.__aenter__")?;
        if step::take_now(py, || self.lock.try_lock())? {
            return ready_none(py);
        }
        Ok(Py::new(py, SyncWait::new(Grant::Locked(Arc::clone(&self.lock))))?.into_any())
    }

    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __aexit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        exc: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        release_then_checkpoint(py, &self.lock, exc)
    }

    fn __repr__(&self) -> &'static str {
        if self.lock.is_locked() {
            "<porto.Lock locked>"
        } else {
            "<porto.Lock unlocked>"
        }
    }
}

/// Releases `lock`, handing it to the task that has waited longest; `RuntimeError` if it is not
/// held.
fn release(lock: &LockSource) -> PyResult<()> {
    if lock.unlock() {
        return Ok(());
    }
    Err(PyRuntimeError::new_err(
        "porto: Lock.release() was called on a lock that is not held; release a lock once per \
         acquire, from the task that acquired it (`async with lock:` does it for you). If this \
         task did hold it, another task released it first (the lock does not record its \
         holder): find the extra release()",
    ))
}

/// The `__aexit__` of `async with lock:` (and of a `porto.Mutex`): releases the lock, then the
/// exit's checkpoint (T2), which never holds the lock. `exc` is the exception leaving the
/// block, if any.
pub(super) fn release_then_checkpoint(
    py: Python<'_>,
    lock: &LockSource,
    exc: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    release(lock)?;
    // An exception is in flight: no checkpoint, which could raise `Cancelled` in its place (and
    // a cancelled scope would then absorb it, losing the original error).
    if !exc.is_none() {
        return ready_none(py);
    }
    checkpoint_if_needed(py, py.None())
}
