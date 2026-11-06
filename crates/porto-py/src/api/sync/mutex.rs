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
use pyo3::gc::PyVisit;
use pyo3::prelude::*;
use pyo3::sync::critical_section::with_critical_section;
use pyo3::types::PyType;

use super::lock::{LockSource, release_then_checkpoint};
use super::wait_op::{Grant, SyncWait, Then};
use crate::api::time::require_lane;
use crate::driver::thin::Waitable;

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

    /// `async with mutex as guard:`: a lazy wait for the lock, like `Lock.__aenter__`, that
    /// evaluates to a fresh guard made in the step that takes the lock (a free one on its
    /// first resume, without suspending). If making the guard fails, the lock is released.
    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<SyncWait> {
        require_lane("Mutex.__aenter__")?;
        Ok(SyncWait::with(
            Grant::Locked(Arc::clone(&slf.get().lock)),
            Then::Guard(slf.clone().unbind()),
            "Mutex.__aenter__()",
        ))
    }

    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __aexit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        exc: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        // The block's guard stops working before another task can take the lock.
        self.generation.fetch_add(1, Ordering::AcqRel);
        release_then_checkpoint(py, &self.lock, exc)
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
