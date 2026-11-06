//! `porto.Lock`: an async lock with fair FIFO handoff.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::{Mutex, MutexGuard, WaitNode};
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;

use super::blocking;
use super::wait_op::{Grant, SyncWait, Then};
use crate::api::time::require_lane;
use crate::driver::ready::ready_none;
use crate::driver::step::checkpoint_if_needed;
use crate::driver::thin::{WaitSource, Waitable};
use crate::raw_methods::{self, MethodDef};

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
    fn acquire(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("Lock.acquire")?;
        SyncWait::new(Grant::Locked(Arc::clone(&self.lock))).into_object(py)
    }

    /// `acquire()`, then `on_acquired()` in the same step; the await evaluates to its result
    /// (if it raises, the lock is released and the await raises). Internal, for the tests: the
    /// way `porto.Mutex` makes its guard, with a Python callback.
    fn _acquire_then(&self, py: Python<'_>, on_acquired: Py<PyAny>) -> PyResult<Py<SyncWait>> {
        require_lane("Lock._acquire_then")?;
        SyncWait::with(
            Grant::Locked(Arc::clone(&self.lock)),
            Then::Call(on_acquired),
            "Lock._acquire_then()",
        )
        .into_object(py)
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

    fn __repr__(&self) -> &'static str {
        if self.lock.is_locked() {
            "<porto.Lock locked>"
        } else {
            "<porto.Lock unlocked>"
        }
    }
}

impl Lock {
    /// `async with lock:`: `acquire()`'s lazy wait, named for the warning if it is dropped
    /// unawaited. `async with` awaits it in the same step, and its first resume takes a free
    /// lock without suspending (T2: `Cancelled` first if the task is cancelled, one unit of
    /// the budget). Nothing is taken in this call: an exception between it and the await
    /// leaves the lock free.
    fn enter(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("Lock.__aenter__")?;
        SyncWait::with(
            Grant::Locked(Arc::clone(&self.lock)),
            Then::Nothing,
            "Lock.__aenter__()",
        )
        .into_object(py)
    }
}

/// `Lock.__aenter__` and `Lock.__aexit__`, as raw methods (`raw_methods`).
pub(super) fn install_raw_methods(py: Python<'_>) -> PyResult<()> {
    raw_methods::install::<Lock>(py, &LOCK_AENTER)?;
    raw_methods::install::<Lock>(py, &LOCK_AEXIT)
}

static LOCK_AENTER: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"__aenter__".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: lock_aenter,
    },
    ml_flags: ffi::METH_NOARGS,
    ml_doc: c"__aenter__($self, /)\n--\n\nWaits for the lock: `async with lock:`.".as_ptr(),
});

static LOCK_AEXIT: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"__aexit__".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunctionFast: lock_aexit,
    },
    ml_flags: ffi::METH_FASTCALL,
    ml_doc: c"__aexit__($self, exc_type, exc, traceback, /)\n--\n\nReleases the lock.".as_ptr(),
});

/// `Lock.__aenter__` (`METH_NOARGS`).
///
/// # Safety
/// CPython calls it as a `METH_NOARGS` method of `Lock`, attached: `slf` is a live `Lock`.
unsafe extern "C" fn lock_aenter(
    slf: *mut ffi::PyObject,
    _args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    raw_methods::guard(|py| {
        // SAFETY: the caller's contract (the method descriptor checked the type).
        let lock = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<Lock>() };
        Ok(lock.get().enter(py)?.into_any())
    })
}

/// `Lock.__aexit__(exc_type, exc, traceback)` (`METH_FASTCALL`): releases the lock, then the
/// exit's checkpoint (see [`release_then_checkpoint`]).
///
/// # Safety
/// CPython calls it as a `METH_FASTCALL` method of `Lock`, attached: `slf` is a live `Lock`
/// and `args` points at `nargs` live objects.
unsafe extern "C" fn lock_aexit(
    slf: *mut ffi::PyObject,
    args: *mut *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
) -> *mut ffi::PyObject {
    raw_methods::guard(|py| {
        // SAFETY: the caller's contract.
        let [_, exc, _] = unsafe { raw_methods::aexit_args(py, c"Lock.__aexit__", args, nargs)? };
        // SAFETY: the caller's contract (the method descriptor checked the type).
        let lock = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<Lock>() };
        release_then_checkpoint(py, &lock.get().lock, &exc)
    })
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
