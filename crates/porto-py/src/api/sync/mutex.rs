//! `porto.Mutex[T]` and `porto.MutexGuard[T]`: a lock that owns its value
//! (16-sync-primitives-and-channels.md §1).
//!
//! The mutex is a `porto.Lock` (the same `LockSource`) plus the value. `async with mutex as
//! guard:` takes the lock like `async with lock:` (the lazy `_SyncWait`, which takes a free lock
//! on its first resume) and evaluates to a fresh guard, made in the step that takes the lock.
//!
//! A guard works while its block is open: it records the mutex's `generation` when it is made,
//! and every exit bumps the generation before releasing the lock, so a guard is current exactly
//! while the lock is held for its block. Only the current guard reaches the value. The value is
//! read and replaced in the mutex's critical section (`with_critical_section`: nothing on a GIL
//! build, where being attached already serializes them; the object's lock on 3.14t), so a guard
//! shared between threads by mistake cannot tear it.

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

use super::lock::{LockSource, release_then_checkpoint};
use super::wait_op::{Grant, SyncWait, Then};
use crate::api::time::require_lane;
use crate::driver::thin::Waitable;
use crate::raw_methods::{self, MethodDef};

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
    /// A new reference to the value.
    fn value(slf: &Bound<'_, Self>) -> Py<PyAny> {
        let py = slf.py();
        with_critical_section(slf.as_any(), || {
            // SAFETY: in the critical section (see `ValueCell`); only a shared read.
            unsafe { (*slf.get().value.0.get()).clone_ref(py) }
        })
    }

    /// Replaces the value; returns the old one, for the caller to drop outside the critical
    /// section (dropping an object can run arbitrary code).
    fn replace_value(slf: &Bound<'_, Self>, value: Py<PyAny>) -> Py<PyAny> {
        with_critical_section(slf.as_any(), || {
            // SAFETY: in the critical section (see `ValueCell`), which excludes every other
            // access for this write.
            unsafe { std::mem::replace(&mut *slf.get().value.0.get(), value) }
        })
    }

    /// `async with mutex as guard:`: a lazy wait for the lock, like `Lock.__aenter__`, that
    /// evaluates to a fresh guard made in the step that takes the lock (a free one on its
    /// first resume, without suspending). If making the guard fails, the lock is released.
    fn enter(slf: &Bound<'_, Self>) -> PyResult<Py<SyncWait>> {
        require_lane("Mutex.__aenter__")?;
        SyncWait::with(
            Grant::Locked(Arc::clone(&slf.get().lock)),
            Then::Guard(slf.clone().unbind()),
            "Mutex.__aenter__()",
        )
        .into_object(slf.py())
    }

    /// The end of the block: its guard stops working before another task can take the lock,
    /// then the lock is released and the exit's checkpoint runs (`release_then_checkpoint`).
    fn exit(&self, py: Python<'_>, exc: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.generation.fetch_add(1, Ordering::AcqRel);
        release_then_checkpoint(py, &self.lock, exc)
    }

    /// The guard for the block that now holds the lock.
    pub(super) fn guard(slf: &Bound<'_, Self>) -> PyResult<Py<MutexGuard>> {
        Py::new(
            slf.py(),
            MutexGuard {
                mutex: slf.clone().unbind(),
                generation: slf.get().generation.load(Ordering::Acquire),
            },
        )
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
        mutex.get().exit(py, &exc)
    })
}

/// Access to a `porto.Mutex`'s value while its lock is held; what `async with` gives.
#[pyclass(frozen, freelist = 64, module = "porto", name = "MutexGuard")]
pub(super) struct MutexGuard {
    mutex: Py<Mutex>,
    /// The mutex's generation for the block this guard belongs to.
    generation: u64,
}

impl MutexGuard {
    /// The mutex, if this guard's block is still open.
    fn held<'py>(&self, py: Python<'py>) -> PyResult<&Bound<'py, Mutex>> {
        let mutex = self.mutex.bind(py);
        if mutex.get().generation.load(Ordering::Acquire) == self.generation {
            return Ok(mutex);
        }
        Err(PyRuntimeError::new_err(
            "porto: this Mutex guard was used after its `async with` block exited; the value is \
             only reachable while the lock is held, so use it inside the block (or enter the \
             mutex again)",
        ))
    }
}

#[pymethods]
impl MutexGuard {
    /// The value the mutex owns.
    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(Mutex::value(self.held(py)?))
    }

    /// Replaces the value the mutex owns.
    #[pyo3(signature = (value, /))]
    fn set(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        let old = Mutex::replace_value(self.held(py)?, value);
        drop(old);
        Ok(())
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
        visit.call(&self.mutex)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let Ok(mutex) = self.held(py) else {
            return Ok("<porto.MutexGuard released>".to_owned());
        };
        let value = Mutex::value(mutex);
        Ok(format!(
            "<porto.MutexGuard value={}>",
            value.bind(py).repr()?
        ))
    }
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
