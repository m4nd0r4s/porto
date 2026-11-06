//! `_SyncEntry`: the lazy `async with` entry of `Semaphore`, `CapacityLimiter` and
//! `RwLock.read()`/`RwLock.write()`, the same object and rules as `Lock`'s (`entry`): nothing is
//! taken until the await, whose first resume takes a free permit, token or lock without
//! suspending (T2), and an entry dropped unawaited warns.
//!
//! For a semaphore or a limiter, the primitive's `__aenter__` returns the entry and its
//! `__aexit__` gives the grant back. `RwLock.read()` and `RwLock.write()` return the entry
//! itself, which is then the context manager too: its `__aenter__` returns it, and its
//! `__aexit__` releases the lock it took.

use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;

use super::entry::{Entry, Step, Strong, install_dealloc};
use super::limiter::{CapacityLimiter, CurrentTask};
use super::rwlock::{RwLock, release_read, release_write};
use super::semaphore::{Semaphore, exit_checkpoint};
use crate::async_slots::{self, IterNext, guard_next, guard_send, stop_iteration};
use crate::driver::fast_new::{FastNew, Unawaited};
use crate::driver::freelist;
use crate::throw_args::thrown_exception;

/// What the entry takes, through the object being entered.
enum Owner {
    /// One permit.
    Permit(Strong<Semaphore>),
    /// One token, for the running task.
    Token(Strong<CapacityLimiter>),
    /// A read lock.
    Read(Strong<RwLock>),
    /// The write lock.
    Write(Strong<RwLock>),
}

/// The awaitable of `async with semaphore:`, `async with limiter:`, `async with lock.read():`
/// and `async with lock.write():` (see the module docs).
#[pyclass(frozen, freelist = 64, module = "porto._core", name = "_SyncEntry")]
pub(super) struct SyncEntry {
    owner: Owner,
    entry: Entry,
}

impl SyncEntry {
    fn object(py: Python<'_>, owner: Owner) -> PyResult<Py<Self>> {
        ENTRIES.object(
            py,
            Self {
                owner,
                entry: Entry::new(),
            },
        )
    }

    /// `Semaphore.__aenter__()`.
    pub(super) fn permit(semaphore: &Bound<'_, Semaphore>) -> PyResult<Py<Self>> {
        Self::object(semaphore.py(), Owner::Permit(Strong::new(semaphore)))
    }

    /// `CapacityLimiter.__aenter__()`.
    pub(super) fn token(limiter: &Bound<'_, CapacityLimiter>) -> PyResult<Py<Self>> {
        Self::object(limiter.py(), Owner::Token(Strong::new(limiter)))
    }

    /// `RwLock.read()`.
    pub(super) fn read(lock: &Bound<'_, RwLock>) -> PyResult<Py<Self>> {
        Self::object(lock.py(), Owner::Read(Strong::new(lock)))
    }

    /// `RwLock.write()`.
    pub(super) fn write(lock: &Bound<'_, RwLock>) -> PyResult<Py<Self>> {
        Self::object(lock.py(), Owner::Write(Strong::new(lock)))
    }

    /// The call that made it, as the user wrote it.
    fn call(&self) -> &'static str {
        match self.owner {
            Owner::Permit(_) => "Semaphore.__aenter__()",
            Owner::Token(_) => "CapacityLimiter.__aenter__()",
            Owner::Read(_) => "RwLock.read()",
            Owner::Write(_) => "RwLock.write()",
        }
    }

    fn resume(&self, py: Python<'_>, sent: &Bound<'_, PyAny>) -> PyResult<Step> {
        let entry = &self.entry;
        match &self.owner {
            Owner::Permit(semaphore) => entry.resume(
                py,
                &semaphore.py().get().semaphore,
                sent,
                "Semaphore.__aenter__",
            ),
            Owner::Token(limiter) => entry.resume(
                py,
                &CurrentTask(&limiter.py().get().core),
                sent,
                "CapacityLimiter.__aenter__",
            ),
            Owner::Read(lock) => entry.resume(py, &lock.py().get().read, sent, "RwLock.read"),
            Owner::Write(lock) => entry.resume(py, &lock.py().get().write, sent, "RwLock.write"),
        }
    }
}

impl Unawaited for SyncEntry {
    fn unwatch(&mut self) {
        // Finished rather than forgotten; nothing was taken.
        self.entry.finish();
    }
}

/// Never resumed, thrown into or closed: the `await` was forgotten (see `unawaited.rs`).
impl Drop for SyncEntry {
    fn drop(&mut self) {
        if self.entry.never_awaited() {
            crate::unawaited::warn_never_awaited(self.call());
        }
    }
}

#[pymethods]
impl SyncEntry {
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

    /// `async with lock.read():` / `async with lock.write():` the entry is the context manager.
    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<Self>> {
        match slf.get().owner {
            Owner::Read(_) | Owner::Write(_) => Ok(slf.clone().unbind()),
            Owner::Permit(_) | Owner::Token(_) => Err(not_a_context_manager()),
        }
    }

    /// Releases the lock the block took, then the exit's checkpoint.
    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __aexit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        exc: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let lock = match &self.owner {
            Owner::Read(lock) | Owner::Write(lock) => lock.py().get(),
            Owner::Permit(_) | Owner::Token(_) => return Err(not_a_context_manager()),
        };
        if !self.entry.exit() {
            return Err(PyRuntimeError::new_err(format!(
                "porto: {}.__aexit__() was called on a block that does not hold the lock (it \
                 was never entered, or already exited); use each read() or write() in one \
                 `async with` statement",
                self.call()
            )));
        }
        match self.owner {
            Owner::Read(_) => release_read(&lock.read)?,
            _ => release_write(&lock.read)?,
        }
        exit_checkpoint(py, exc)
    }

    fn __repr__(&self) -> String {
        format!("<porto._core._SyncEntry of {}>", self.call())
    }
}

fn not_a_context_manager() -> PyErr {
    PyTypeError::new_err(
        "porto: this is the awaitable of an `async with` on a Semaphore or CapacityLimiter, not \
         a context manager; write `async with semaphore:`",
    )
}

/// `_SyncEntry` objects made without pyo3's generic path.
static ENTRIES: FastNew<SyncEntry> = FastNew::new();

/// `_SyncEntry`'s raw slots and fast constructor. Call it once from module init, right after
/// `add_class::<SyncEntry>()`.
pub(super) fn init_entry_type(py: Python<'_>) -> PyResult<()> {
    async_slots::install_am_send::<SyncEntry>(py, sync_entry_am_send)?;
    async_slots::install_iter_slots::<SyncEntry>(py, sync_entry_iternext)?;
    freelist::install::<SyncEntry>(py, &sync_entry_freelist::SLOTS)?;
    // A finished entry: dropping it does not warn.
    let probe = SyncEntry {
        owner: Owner::Read(Strong::new(&Bound::new(py, RwLock::new())?)),
        entry: Entry::new(),
    };
    probe.entry.finish();
    ENTRIES.init(py, probe)?;
    install_dealloc::<SyncEntry>(py, &ENTRIES)
}

freelist::gil_freelist!(sync_entry_freelist, 64);

/// `am_send` for `_SyncEntry`: the same steps as `send`, with no `StopIteration`.
///
/// # Safety
/// CPython calls it as `tp_as_async->am_send` of `_SyncEntry`, attached, with `slf` a live
/// `_SyncEntry`, `arg` a live object and `presult` writable.
unsafe extern "C" fn sync_entry_am_send(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
    presult: *mut *mut ffi::PyObject,
) -> ffi::PySendResult {
    guard_send(|| {
        // SAFETY: the caller is attached (am_send contract).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: `slf` and `arg` are live for the call, and the slot is only installed on
        // `_SyncEntry`.
        let (entry, arg) = unsafe {
            (
                Borrowed::from_ptr(py, slf).cast_unchecked::<SyncEntry>(),
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

/// `tp_iternext` for `_SyncEntry` (see `async_slots`): the same steps as `__next__`.
///
/// # Safety
/// CPython calls it as `tp_iternext` of `_SyncEntry`, attached, with `slf` a live `_SyncEntry`.
unsafe extern "C" fn sync_entry_iternext(slf: *mut ffi::PyObject) -> *mut ffi::PyObject {
    guard_next(|py| {
        // SAFETY: `slf` is live for the call, and the slot is only installed on `_SyncEntry`.
        let entry = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<SyncEntry>() };
        Ok(match entry.get().resume(py, &py.None().into_bound(py))? {
            Step::Yield(trap) => IterNext::Yield(trap.into_bound(py)),
            Step::Entered => IterNext::Return(py.None().into_bound(py)),
        })
    })
}
