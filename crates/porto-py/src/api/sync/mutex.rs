//! `porto.Mutex[T]` and `porto.MutexGuard[T]`: a lock that owns its value
//! (16-sync-primitives-and-channels.md §1).
//!
//! The mutex is a `porto.Lock` (the same `LockSource`) plus the value. `async with mutex as
//! guard:` takes the lock like `async with lock:`: `__aenter__()` returns a fresh guard that is
//! also the lazy entry (`entry`), which takes a free lock on its first resume and evaluates to
//! itself. One object per entry, its `value` and `set` raw slots (porto-szb.118).
//!
//! A guard works while its block is open: it records the mutex's `generation` in the step that
//! takes the lock, and every exit bumps the generation before releasing the lock, so a guard is
//! current exactly while the lock is held for its block (an entry not yet entered holds a
//! generation no mutex reaches). Only the current guard reaches the value. The value is
//! read and replaced in the mutex's critical section (`with_critical_section`: nothing on a GIL
//! build, where being attached already serializes them; the object's lock on 3.14t), so a guard
//! shared between threads by mistake cannot tear it. The guard checks the generation, and the
//! exit bumps it, in that same section: an access that found its guard current finishes before
//! the block's exit releases the lock.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::PyTraverseError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::gc::PyVisit;
use pyo3::prelude::*;
use pyo3::sync::critical_section::with_critical_section;
use pyo3::types::PyType;

use super::entry::{Entry, Step, Strong, install_dealloc};
use super::lock::{LockSource, release_then_checkpoint};
use crate::async_slots::{self, IterNext, guard_next, guard_send, stop_iteration};
use crate::driver::fast_new::{FastNew, Unawaited};
use crate::driver::freelist;
use crate::driver::thin::Waitable;
use crate::raw_methods::{self, GetterDef, MethodDef};
use crate::throw_args::thrown_exception;

/// The value a mutex owns, reached only in the mutex's critical section (see the module docs).
struct ValueCell(UnsafeCell<Py<PyAny>>);

// SAFETY: the cell is only reached through `Mutex::value` and `Mutex::replace_value`, inside the
// mutex object's critical section, which serializes them: attached threads on a GIL build, the
// object's lock on a free-threaded one. `Py<PyAny>` is `Send`.
unsafe impl Sync for ValueCell {}

/// `porto.Mutex[T](value)`: an async lock that owns `value`.
#[pyclass(frozen, module = "porto", name = "Mutex")]
pub(super) struct Mutex {
    lock: Arc<Waitable<LockSource>>,
    /// Bumped by every exit, before the release: a guard is current while it matches.
    generation: AtomicU64,
    value: ValueCell,
}

impl Mutex {
    /// A new reference to the value, if `generation` is current (else `None`).
    fn value(slf: &Bound<'_, Self>, generation: u64) -> Option<Py<PyAny>> {
        let py = slf.py();
        let this = slf.get();
        with_critical_section(slf.as_any(), || {
            // In the section, so no exit can end the block between the check and the read.
            (this.generation.load(Ordering::Relaxed) == generation)
                // SAFETY: in the critical section (see `ValueCell`); only a shared read.
                .then(|| unsafe { (*this.value.0.get()).clone_ref(py) })
        })
    }

    /// Replaces the value if `generation` is current; returns the old one, for the caller to
    /// drop outside the critical section (dropping an object can run arbitrary code), or gives
    /// `value` back if the generation is not current.
    fn replace_value(
        slf: &Bound<'_, Self>,
        generation: u64,
        value: Py<PyAny>,
    ) -> Result<Py<PyAny>, Py<PyAny>> {
        let this = slf.get();
        with_critical_section(slf.as_any(), || {
            if this.generation.load(Ordering::Relaxed) != generation {
                return Err(value);
            }
            // SAFETY: in the critical section (see `ValueCell`), which excludes every other
            // access for this write.
            Ok(unsafe { std::mem::replace(&mut *this.value.0.get(), value) })
        })
    }

    /// `async with mutex as guard:`: a fresh guard, which is also the lazy entry (see the module
    /// docs), named `Mutex.__aenter__()` for the warning if it is dropped unawaited.
    fn enter(slf: &Bound<'_, Self>) -> PyResult<Py<MutexGuard>> {
        GUARDS.object(slf.py(), MutexGuard::new(slf))
    }

    /// The end of the block: its guard stops working before another task can take the lock,
    /// then the lock is released and the exit's checkpoint runs (`release_then_checkpoint`).
    fn exit(slf: &Bound<'_, Self>, exc: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let this = slf.get();
        // In the section, so a guard that checked the generation finishes its access first.
        // Every bump is made in the section, which serializes them: no read-modify-write needed.
        with_critical_section(slf.as_any(), || {
            let generation = this.generation.load(Ordering::Relaxed);
            this.generation.store(generation + 1, Ordering::Release);
        });
        release_then_checkpoint(slf.py(), &this.lock, exc)
    }
}

#[pymethods]
impl Mutex {
    #[new]
    #[pyo3(signature = (value, /))]
    fn new(value: Py<PyAny>) -> Self {
        Self {
            lock: LockSource::waitable(),
            generation: AtomicU64::new(0),
            value: ValueCell(UnsafeCell::new(value)),
        }
    }

    /// Whether the lock is held (or being handed to a waiter).
    fn locked(&self) -> bool {
        self.lock.is_locked()
    }

    /// `Mutex[int]` at runtime, for annotations that are evaluated.
    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias(cls, item)
    }

    fn __repr__(&self) -> &'static str {
        if self.lock.is_locked() {
            "<porto.Mutex locked>"
        } else {
            "<porto.Mutex unlocked>"
        }
    }

    // The value may reference the mutex again (an object that owns its own mutex, a guard
    // stored in it): let the cycle collector see through it.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        // SAFETY: the collector traverses with every other thread stopped (or, on a GIL build,
        // not attached), so no write to the cell overlaps this read.
        visit.call(unsafe { &*self.value.0.get() })
    }

    fn __clear__(&self) {
        let old = Python::attach(|py| {
            // SAFETY: the collector clears only objects nothing else references, so no other
            // access to the cell can overlap this write.
            unsafe { std::mem::replace(&mut *self.value.0.get(), py.None()) }
        });
        drop(old);
    }
}

/// `Mutex.__aenter__` and `Mutex.__aexit__`, as raw methods (`raw_methods`).
pub(super) fn install_raw_methods(py: Python<'_>) -> PyResult<()> {
    raw_methods::install::<Mutex>(py, &MUTEX_AENTER)?;
    raw_methods::install::<Mutex>(py, &MUTEX_AEXIT)
}

static MUTEX_AENTER: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"__aenter__".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: mutex_aenter,
    },
    ml_flags: ffi::METH_NOARGS,
    ml_doc: c"__aenter__($self, /)\n--\n\nWaits for the lock; evaluates to the guard: \
             `async with mutex as guard:`."
        .as_ptr(),
});

static MUTEX_AEXIT: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"__aexit__".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunctionFast: mutex_aexit,
    },
    ml_flags: ffi::METH_FASTCALL,
    ml_doc: c"__aexit__($self, exc_type, exc, traceback, /)\n--\n\nReleases the lock; the \
             block's guard stops working."
        .as_ptr(),
});

/// `Mutex.__aenter__` (`METH_NOARGS`).
///
/// # Safety
/// CPython calls it as a `METH_NOARGS` method of `Mutex`, attached: `slf` is a live `Mutex`.
unsafe extern "C" fn mutex_aenter(
    slf: *mut ffi::PyObject,
    _args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    raw_methods::guard(|py| {
        // SAFETY: the caller's contract (the method descriptor checked the type).
        let mutex = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<Mutex>() };
        Ok(Mutex::enter(&mutex)?.into_any())
    })
}

/// `Mutex.__aexit__(exc_type, exc, traceback)` (`METH_FASTCALL`).
///
/// # Safety
/// CPython calls it as a `METH_FASTCALL` method of `Mutex`, attached: `slf` is a live `Mutex`
/// and `args` points at `nargs` live objects.
unsafe extern "C" fn mutex_aexit(
    slf: *mut ffi::PyObject,
    args: *mut *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
) -> *mut ffi::PyObject {
    raw_methods::guard(|py| {
        // SAFETY: the caller's contract.
        let [_, exc, _] = unsafe { raw_methods::aexit_args(py, c"Mutex.__aexit__", args, nargs)? };
        // SAFETY: the caller's contract (the method descriptor checked the type).
        let mutex = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<Mutex>() };
        Mutex::exit(&mutex, &exc)
    })
}

/// Access to a `porto.Mutex`'s value while its lock is held; what `async with` gives. It is
/// also the entry `async with` awaits (see the module docs).
#[pyclass(frozen, freelist = 64, module = "porto", name = "MutexGuard")]
pub(super) struct MutexGuard {
    mutex: Strong<Mutex>,
    entry: Entry,
    /// The mutex's generation for the block this guard belongs to, once entered.
    generation: AtomicU64,
}

/// The generation of a guard that was not entered: no mutex reaches it.
const NOT_ENTERED: u64 = u64::MAX;

/// The error for a guard used after its block exited.
fn released_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: this Mutex guard was used after its `async with` block exited; the value is \
         only reachable while the lock is held, so use it inside the block (or enter the \
         mutex again)",
    )
}

/// The error for a guard used before its entry was awaited.
fn not_entered_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: this Mutex guard was used before the mutex was entered; use the guard that \
         `async with mutex as guard:` gives (or await `mutex.__aenter__()` first)",
    )
}

impl MutexGuard {
    /// A guard for `mutex` that is not entered yet.
    fn new(mutex: &Bound<'_, Mutex>) -> Self {
        Self {
            mutex: Strong::new(mutex),
            entry: Entry::new(),
            generation: AtomicU64::new(NOT_ENTERED),
        }
    }

    /// Resumes the entry; once the lock is taken, the guard belongs to the block.
    fn resume(&self, py: Python<'_>, sent: &Bound<'_, PyAny>) -> PyResult<Step> {
        let mutex = self.mutex.py().get();
        let step = self
            .entry
            .resume(py, &mutex.lock, sent, "Mutex.__aenter__")?;
        if let Step::Entered = step {
            self.generation
                .store(mutex.generation.load(Ordering::Acquire), Ordering::Relaxed);
        }
        Ok(step)
    }

    /// The error for an access that found the guard not current.
    fn stale_error(&self) -> PyErr {
        if self.generation.load(Ordering::Relaxed) == NOT_ENTERED {
            not_entered_error()
        } else {
            released_error()
        }
    }

    /// `guard.value`.
    fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Mutex::value(
            self.mutex.py().bind(py),
            self.generation.load(Ordering::Relaxed),
        )
        .ok_or_else(|| self.stale_error())
    }

    /// `guard.set(value)`.
    fn set(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let generation = self.generation.load(Ordering::Relaxed);
        // Dropped as `Bound`s: a plain `Py_DECREF`, not `Py`'s check of the attached thread.
        match Mutex::replace_value(self.mutex.py().bind(py), generation, value) {
            Ok(old) => {
                drop(old.into_bound(py));
                Ok(())
            }
            Err(value) => {
                drop(value.into_bound(py));
                Err(self.stale_error())
            }
        }
    }
}

impl Unawaited for MutexGuard {
    fn unwatch(&mut self) {
        // Finished rather than forgotten; nothing was taken.
        self.entry.finish();
    }
}

/// Never resumed, thrown into or closed: the `await` was forgotten (see `unawaited.rs`).
impl Drop for MutexGuard {
    fn drop(&mut self) {
        if self.entry.never_awaited() {
            crate::unawaited::warn_never_awaited("Mutex.__aenter__()");
        }
    }
}

#[pymethods]
impl MutexGuard {
    /// The value the mutex owns (replaced by a raw getter, `GUARD_VALUE`).
    #[getter(value)]
    fn py_value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.value(py)
    }

    /// Replaces the value the mutex owns (replaced by a raw method, `GUARD_SET`).
    #[pyo3(name = "set", signature = (value, /))]
    fn py_set(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        self.set(py, value)
    }

    fn __await__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __next__(slf: &Bound<'_, Self>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();
        match slf.get().resume(py, &py.None().into_bound(py))? {
            Step::Yield(trap) => Ok(Some(trap)),
            Step::Entered => Err(stop_iteration(slf.clone().into_any())),
        }
    }

    #[pyo3(signature = (value, /))]
    fn send(slf: &Bound<'_, Self>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        match slf.get().resume(slf.py(), value)? {
            Step::Yield(trap) => Ok(trap),
            Step::Entered => Err(stop_iteration(slf.clone().into_any())),
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

    /// `MutexGuard[int]` at runtime, for annotations that are evaluated.
    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias(cls, item)
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(self.mutex.py())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let generation = self.generation.load(Ordering::Relaxed);
        if generation == NOT_ENTERED {
            return Ok("<porto.MutexGuard not entered>".to_owned());
        }
        let Some(value) = Mutex::value(self.mutex.py().bind(py), generation) else {
            return Ok("<porto.MutexGuard released>".to_owned());
        };
        Ok(format!(
            "<porto.MutexGuard value={}>",
            value.bind(py).repr()?
        ))
    }
}

/// `MutexGuard` objects made without pyo3's generic path.
static GUARDS: FastNew<MutexGuard> = FastNew::tracked();

/// `MutexGuard`'s raw slots and fast constructor. Call it once from module init, right after
/// `add_class::<MutexGuard>()`.
pub(super) fn init_guard_type(py: Python<'_>) -> PyResult<()> {
    async_slots::install_am_send::<MutexGuard>(py, guard_am_send)?;
    async_slots::install_iter_slots::<MutexGuard>(py, guard_iternext)?;
    freelist::install::<MutexGuard>(py, &guard_freelist::SLOTS)?;
    raw_methods::install_getter::<MutexGuard>(py, &GUARD_VALUE)?;
    raw_methods::install::<MutexGuard>(py, &GUARD_SET)?;
    // A finished entry: dropping it does not warn.
    let probe = MutexGuard::new(&Bound::new(py, Mutex::new(py.None()))?);
    probe.entry.finish();
    GUARDS.init(py, probe)?;
    install_dealloc::<MutexGuard>(py, &GUARDS)
}

freelist::gil_freelist!(guard_freelist, 64);

/// `am_send` for `MutexGuard`: the same steps as `send`; the await evaluates to the guard, with
/// no `StopIteration`.
///
/// # Safety
/// CPython calls it as `tp_as_async->am_send` of `MutexGuard`, attached, with `slf` a live
/// `MutexGuard`, `arg` a live object and `presult` writable.
unsafe extern "C" fn guard_am_send(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
    presult: *mut *mut ffi::PyObject,
) -> ffi::PySendResult {
    guard_send(|| {
        // SAFETY: the caller is attached (am_send contract).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: `slf` and `arg` are live for the call, and the slot is only installed on
        // `MutexGuard`.
        let (guard, arg) = unsafe {
            (
                Borrowed::from_ptr(py, slf).cast_unchecked::<MutexGuard>(),
                Borrowed::from_ptr(py, arg),
            )
        };
        let (result, status) = match guard.get().resume(py, &arg) {
            Ok(Step::Yield(trap)) => (trap, ffi::PySendResult::PYGEN_NEXT),
            Ok(Step::Entered) => (
                guard.to_owned().into_any().unbind(),
                ffi::PySendResult::PYGEN_RETURN,
            ),
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

/// `tp_iternext` for `MutexGuard` (see `async_slots`): the same steps as `__next__`.
///
/// # Safety
/// CPython calls it as `tp_iternext` of `MutexGuard`, attached, with `slf` a live `MutexGuard`.
unsafe extern "C" fn guard_iternext(slf: *mut ffi::PyObject) -> *mut ffi::PyObject {
    guard_next(|py| {
        // SAFETY: `slf` is live for the call, and the slot is only installed on `MutexGuard`.
        let guard = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<MutexGuard>() };
        Ok(match guard.get().resume(py, &py.None().into_bound(py))? {
            Step::Yield(trap) => IterNext::Yield(trap.into_bound(py)),
            Step::Entered => IterNext::Return(guard.to_owned().into_any()),
        })
    })
}

static GUARD_VALUE: GetterDef = GetterDef(ffi::PyGetSetDef {
    name: c"value".as_ptr(),
    get: Some(guard_value),
    set: None,
    doc: c"The value the mutex owns.".as_ptr(),
    closure: core::ptr::null_mut(),
});

static GUARD_SET: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"set".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: guard_set,
    },
    ml_flags: ffi::METH_O,
    ml_doc: c"set($self, value, /)\n--\n\nReplaces the value the mutex owns.".as_ptr(),
});

/// `MutexGuard.value` (a getter).
///
/// # Safety
/// CPython calls it as the getter of `MutexGuard.value`, attached: `slf` is a live `MutexGuard`.
unsafe extern "C" fn guard_value(
    slf: *mut ffi::PyObject,
    _closure: *mut core::ffi::c_void,
) -> *mut ffi::PyObject {
    raw_methods::guard(|py| {
        // SAFETY: the caller's contract (the descriptor checked the type).
        let guard = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<MutexGuard>() };
        guard.get().value(py)
    })
}

/// `MutexGuard.set(value)` (`METH_O`).
///
/// # Safety
/// CPython calls it as a `METH_O` method of `MutexGuard`, attached: `slf` is a live
/// `MutexGuard` and `value` a live object.
unsafe extern "C" fn guard_set(
    slf: *mut ffi::PyObject,
    value: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    raw_methods::guard(|py| {
        // SAFETY: the caller's contract (the descriptor checked the type).
        let (guard, value) = unsafe {
            (
                Borrowed::from_ptr(py, slf).cast_unchecked::<MutexGuard>(),
                Borrowed::from_ptr(py, value),
            )
        };
        guard.get().set(py, value.to_owned().unbind())?;
        Ok(py.None())
    })
}

/// `types.GenericAlias(cls, item)`: what `cls[item]` is for a generic class.
fn generic_alias<'py>(
    cls: &Bound<'py, PyType>,
    item: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let py = cls.py();
    py.import("types")?
        .getattr("GenericAlias")?
        .call1((cls, item))
}
