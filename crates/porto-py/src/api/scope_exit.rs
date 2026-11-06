//! `_ScopeExit`: the waiting part of `Scope.__aexit__` (15-structured-concurrency-and-
//! cancellation.md §1.1, C5).
//!
//! While children run, each resume yields a `ScopeExit` trap to the driver, which files the
//! owner's waker as the scope's exit waiter; the last child to end wakes it. Once no child runs,
//! the resume closes the scope (raising the children's failures as an exception group) and the
//! await evaluates to whether the scope absorbs the body's exception. A native awaitable rather
//! than a coroutine: a `porto.scope()` per task was paying for a Python frame at every exit
//! (porto-itr.7).
//!
//! A throw (the owner closed at shutdown, a test interrupt) ends the wait without closing the
//! scope, as the coroutine did: the exception goes on from the `await`.

use pyo3::PyTraverseError;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::gc::PyVisit;
use pyo3::prelude::*;
use pyo3::types::PyBool;

use super::scope::Scope;
use crate::async_slots::{IterNext, guard_next, stop_iteration};
use crate::driver::trap::Trap;
use crate::throw_args::thrown_exception;

/// The awaitable `Scope.__aexit__` returns while children run.
#[pyclass(freelist = 64, module = "porto._core", name = "_ScopeExit")]
pub(crate) struct ScopeExit {
    scope: Py<Scope>,
    /// The exception leaving the block, or `None`.
    exc: Py<PyAny>,
    /// Whether it has been resumed: a first resume must send `None`.
    started: bool,
    /// Returned, raised, thrown into or closed.
    done: bool,
}

impl ScopeExit {
    pub(super) fn new(scope: Py<Scope>, exc: Py<PyAny>) -> Self {
        Self {
            scope,
            exc,
            started: false,
            done: false,
        }
    }

    /// One resume: yield a `ScopeExit` trap while children run, else close the scope.
    fn resume<'py>(&mut self, py: Python<'py>, sent_none: bool) -> PyResult<IterNext<'py>> {
        if !self.started && !sent_none {
            return Err(PyTypeError::new_err(
                "can't send a non-None value to a porto scope exit that has not started; the \
                 first resume of an awaitable must be send(None)",
            ));
        }
        self.started = true;
        if self.done {
            return Err(PyRuntimeError::new_err(
                "porto: a scope exit was awaited again after it finished; `async with` awaits it \
                 once, so do not call Scope.__aexit__ yourself",
            ));
        }
        let scope = self.scope.bind(py).get();
        if let Some(kind) = scope.exit_wait() {
            let trap = Bound::new(py, Trap::suspended(kind))?;
            return Ok(IterNext::Yield(trap.into_any()));
        }
        self.done = true;
        let absorbed = scope.finish(self.exc.bind(py))?;
        Ok(IterNext::Return(
            PyBool::new(py, absorbed).to_owned().into_any(),
        ))
    }
}

#[pymethods]
impl ScopeExit {
    fn __await__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __iter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.resume(py, true)? {
            IterNext::Yield(trap) => Ok(Some(trap.unbind())),
            IterNext::Return(value) => Err(stop_iteration(value)),
        }
    }

    #[pyo3(signature = (value, /))]
    fn send(&mut self, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        match self.resume(value.py(), value.is_none())? {
            IterNext::Yield(trap) => Ok(trap.unbind()),
            IterNext::Return(value) => Err(stop_iteration(value)),
        }
    }

    #[pyo3(signature = (typ, val = None, tb = None, /))]
    fn throw(
        &mut self,
        typ: &Bound<'_, PyAny>,
        val: Option<&Bound<'_, PyAny>>,
        tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        self.done = true;
        Err(thrown_exception(typ, val, tb))
    }

    fn close(&mut self) {
        self.done = true;
    }

    // The exception's traceback reaches the frame that awaits this: let the cycle collector
    // see through it.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.scope)?;
        visit.call(&self.exc)
    }

    fn __clear__(&mut self) {
        let exc = Python::attach(|py| std::mem::replace(&mut self.exc, py.None()));
        drop(exc);
    }

    fn __repr__(&self) -> &'static str {
        if self.done {
            "<porto._core._ScopeExit done>"
        } else {
            "<porto._core._ScopeExit waiting>"
        }
    }
}

/// `tp_iternext` for `_ScopeExit` (see `async_slots`): the same steps as `__next__`.
///
/// # Safety
///
/// CPython calls it as `tp_iternext` of `_ScopeExit`, attached, with `slf` a live `_ScopeExit`.
pub(crate) unsafe extern "C" fn scope_exit_iternext(slf: *mut ffi::PyObject) -> *mut ffi::PyObject {
    guard_next(|py| {
        // SAFETY: `slf` is live for the call, and the slot is only installed on `_ScopeExit`.
        let exit = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<ScopeExit>() };
        let mut exit = exit.try_borrow_mut().map_err(|_| {
            PyRuntimeError::new_err(
                "porto: a scope exit was resumed from two places at once; let `async with` \
                 await it",
            )
        })?;
        exit.resume(py, true)
    })
}
