//! `porto.Lock`: an async lock with fair FIFO handoff.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::{Mutex, MutexGuard, WaitNode};
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;

use super::blocking;
use super::entry::{Entry, Step, Strong, install_dealloc};
use super::wait_op::{Grant, SyncWait, Then};
use crate::api::time::require_lane;
use crate::async_slots::{self, IterNext, guard_next, guard_send, stop_iteration};
use crate::driver::fast_new::{FastNew, Unawaited};
use crate::driver::freelist;
use crate::driver::ready::ready_none;
use crate::driver::step::checkpoint_if_needed_none;
use crate::driver::thin::{WaitSource, Waitable};
use crate::raw_methods::{self, MethodDef};
use crate::throw_args::thrown_exception;

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
    pub(super) lock: Arc<Waitable<LockSource>>,
}

#[pymethods]
impl Lock {
    #[new]
    pub(super) fn new() -> Self {
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
    /// `async with lock:`: a lazy entry (`entry`), named `Lock.__aenter__()` for the warning if
    /// it is dropped unawaited. `async with` awaits it in the same step, and its first resume
    /// takes a free lock without suspending (T2: `Cancelled` first if the task is cancelled,
    /// one unit of the budget). Nothing is taken in this call: an exception between it and the
    /// await leaves the lock free. Outside a porto runtime the await raises `NoRuntimeError`.
    pub(super) fn enter(slf: &Bound<'_, Self>) -> PyResult<Py<LockEntry>> {
        LOCK_ENTRIES.object(
            slf.py(),
            LockEntry {
                lock: Strong::new(slf),
                entry: Entry::new(),
            },
        )
    }
}

/// The awaitable of `async with lock:` (`Lock.__aenter__()`): see `entry`.
#[pyclass(frozen, freelist = 64, module = "porto._core", name = "_LockEntry")]
pub(super) struct LockEntry {
    lock: Strong<Lock>,
    entry: Entry,
}

impl LockEntry {
    fn resume(&self, py: Python<'_>, sent: &Bound<'_, PyAny>) -> PyResult<Step> {
        self.entry
            .resume(py, &self.lock.py().get().lock, sent, "Lock.__aenter__")
    }
}

impl Unawaited for LockEntry {
    fn unwatch(&mut self) {
        // Finished rather than forgotten; nothing was taken.
        self.entry.finish();
    }
}

/// Never resumed, thrown into or closed: the `await` was forgotten (see `unawaited.rs`).
impl Drop for LockEntry {
    fn drop(&mut self) {
        if self.entry.never_awaited() {
            crate::unawaited::warn_never_awaited("Lock.__aenter__()");
        }
    }
}

#[pymethods]
impl LockEntry {
    fn __await__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.resume(py, &py.None().into_bound(py))? {
            Step::Yield(trap) => Ok(Some(trap)),
            Step::Entered => Ok(None),
        }
    }

    #[pyo3(signature = (value, /))]
    fn send(&self, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        match self.resume(value.py(), value)? {
            Step::Yield(trap) => Ok(trap),
            Step::Entered => Err(stop_iteration(value.py().None().into_bound(value.py()))),
        }
    }

    /// The driver gave the wait up before throwing in (T3); the entry is over.
    #[pyo3(signature = (typ, val = None, tb = None, /))]
    fn throw(
        &self,
        typ: &Bound<'_, PyAny>,
        val: Option<&Bound<'_, PyAny>>,
        tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        self.entry.finish();
        Err(thrown_exception(typ, val, tb))
    }

    fn close(&self) {
        self.entry.finish();
    }
}

/// `_LockEntry` objects made without pyo3's generic path.
static LOCK_ENTRIES: FastNew<LockEntry> = FastNew::new();

/// `_LockEntry`'s raw slots and fast constructor. Call it once from module init, right after
/// `add_class::<LockEntry>()`.
pub(super) fn init_entry_type(py: Python<'_>) -> PyResult<()> {
    async_slots::install_am_send::<LockEntry>(py, lock_entry_am_send)?;
    async_slots::install_iter_slots::<LockEntry>(py, lock_entry_iternext)?;
    freelist::install::<LockEntry>(py, &lock_entry_freelist::SLOTS)?;
    // A finished entry: dropping it does not warn.
    let probe = LockEntry {
        lock: Strong::new(&Bound::new(py, Lock::new())?),
        entry: Entry::new(),
    };
    probe.entry.finish();
    LOCK_ENTRIES.init(py, probe)?;
    install_dealloc::<LockEntry>(py, &LOCK_ENTRIES)
}

freelist::gil_freelist!(lock_entry_freelist, 64);

/// `am_send` for `_LockEntry`: the same steps as `send`, with no `StopIteration`.
///
/// # Safety
/// CPython calls it as `tp_as_async->am_send` of `_LockEntry`, attached, with `slf` a live
/// `_LockEntry`, `arg` a live object and `presult` writable.
unsafe extern "C" fn lock_entry_am_send(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
    presult: *mut *mut ffi::PyObject,
) -> ffi::PySendResult {
    guard_send(|| {
        // SAFETY: the caller is attached (am_send contract).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: `slf` and `arg` are live for the call, and the slot is only installed on
        // `_LockEntry`.
        let (entry, arg) = unsafe {
            (
                Borrowed::from_ptr(py, slf).cast_unchecked::<LockEntry>(),
                Borrowed::from_ptr(py, arg),
            )
        };
        let (result, status) = match entry.get().resume(py, &arg) {
            Ok(Step::Yield(trap)) => (trap, ffi::PySendResult::PYGEN_NEXT),
            Ok(Step::Entered) => (py.None(), ffi::PySendResult::PYGEN_RETURN),
            Err(error) => {
                error.restore(py);
                return ffi::PySendResult::PYGEN_ERROR;
            }
        };
        // SAFETY: `presult` is writable (am_send contract); it receives a new reference.
        unsafe { *presult = result.into_ptr() };
        status
    })
}

/// `tp_iternext` for `_LockEntry` (see `async_slots`): the same steps as `__next__`.
///
/// # Safety
/// CPython calls it as `tp_iternext` of `_LockEntry`, attached, with `slf` a live `_LockEntry`.
unsafe extern "C" fn lock_entry_iternext(slf: *mut ffi::PyObject) -> *mut ffi::PyObject {
    guard_next(|py| {
        // SAFETY: `slf` is live for the call, and the slot is only installed on `_LockEntry`.
        let entry = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<LockEntry>() };
        Ok(match entry.get().resume(py, &py.None().into_bound(py))? {
            Step::Yield(trap) => IterNext::Yield(trap.into_bound(py)),
            Step::Entered => IterNext::Return(py.None().into_bound(py)),
        })
    })
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
        Ok(Lock::enter(&lock)?.into_any())
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
pub(super) fn release(lock: &LockSource) -> PyResult<()> {
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
    checkpoint_if_needed_none(py)
}
