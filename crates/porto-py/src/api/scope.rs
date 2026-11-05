//! `porto.scope()`, `Scope` and `Task`: the M0 nursery (15-structured-concurrency-and-
//! cancellation.md §1-§2, the parts without cancellation).
//!
//! A scope counts its live children. `__aexit__` waits until the count is zero (C5), parked on a
//! `Park` trap whose waker the lane holds (`lane::register_scope_waiter`); the last child to
//! finish wakes it. Child failures are collected and raised at exit as a strict exception group,
//! the body's exception first (C4, ADR-009). Spawning into a closed scope raises (C7).
//!
//! Not in M0: cancellation (a failing child does not cancel its siblings or the body; C1-C3,
//! C6), deadlines and timeouts (C9), shields (C8), `start()` (§3), `Task.cancel()` and awaiting a
//! task (§4), names, and spawning from other lanes or threads (C10).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use pyo3::exceptions::{PyBaseException, PyGeneratorExit, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyTuple, PyType};
use pyo3::{PyTraverseError, PyVisit};

use super::run::coroutine_from;
use crate::driver::lane;
use crate::driver::pytask::{self, Spawned, TaskOutcome};
use crate::driver::ready::Ready;
use crate::driver::step::{checkpoint_if_needed, current_waker};
use crate::driver::trap::Trap;
use crate::errors::{InvalidStateError, NoRuntimeError};

/// Adds the classes and functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Scope>()?;
    module.add_class::<Task>()?;
    module.add_function(wrap_pyfunction!(scope, module)?)
}

/// `porto.scope()`: a new scope, to enter with `async with`.
#[pyfunction]
fn scope() -> Scope {
    Scope {
        shared: Arc::new(ScopeShared {
            state: Mutex::new(ScopeState {
                phase: Phase::New,
                live: 0,
                errors: Vec::new(),
            }),
        }),
    }
}

/// A nursery: every task spawned into it finishes before its `async with` block exits.
#[pyclass(frozen, module = "porto._core", name = "Scope")]
pub struct Scope {
    shared: Arc<ScopeShared>,
}

/// The part of a scope its children reach.
pub(crate) struct ScopeShared {
    /// Only held to read or update the counters, never while calling into Python.
    state: Mutex<ScopeState>,
}

struct ScopeState {
    phase: Phase,
    /// Children spawned and not finished.
    live: usize,
    /// The exceptions children raised, in the order they finished.
    errors: Vec<Py<PyBaseException>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Created, not entered yet.
    New,
    /// Inside its `async with` block, or waiting for children at its exit.
    Open,
    /// Its `async with` block exited.
    Closed,
}

impl ScopeShared {
    /// The key the lane files the scope's waiter under: the scope's address, which cannot be
    /// reused while a waiter is filed (the waiting task keeps the scope alive).
    fn key(self: &Arc<Self>) -> usize {
        Arc::as_ptr(self).addr()
    }

    fn lock(&self) -> MutexGuard<'_, ScopeState> {
        // Nothing panics under the lock but an allocation failure; the state stays consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A child finished, raising `error` if it failed. The last one wakes the scope's exit.
    fn child_finished(self: &Arc<Self>, error: Option<Py<PyBaseException>>) {
        let last = {
            let mut state = self.lock();
            state.live -= 1;
            if let Some(error) = error {
                state.errors.push(error);
            }
            state.live == 0
        };
        if last {
            lane::wake_scope_waiter(self.key());
        }
    }
}

/// `BaseExceptionGroup`, built in since 3.11.
static BASE_EXCEPTION_GROUP: PyOnceLock<Py<PyType>> = PyOnceLock::new();
/// `porto._bridge.finish_scope`: the waiting part of `__aexit__`.
static FINISH_SCOPE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

#[pymethods]
impl Scope {
    /// Starts `coro_or_fn` (a coroutine object, or an async function called with `args`) as a
    /// child task of this scope, in a copy of the current context, and returns its `Task`.
    #[pyo3(signature = (coro_or_fn, /, *args))]
    fn spawn(&self, coro_or_fn: Bound<'_, PyAny>, args: Bound<'_, PyTuple>) -> PyResult<Py<Task>> {
        let py = coro_or_fn.py();
        let Some(handle) = lane::current() else {
            return Err(NoRuntimeError::new_err(
                "porto: Scope.spawn() must be called from a task running under porto.run(); \
                 start the program with porto.run(main) and open the scope inside main",
            ));
        };
        match self.shared.lock().phase {
            Phase::Open => {}
            Phase::New => {
                return Err(PyRuntimeError::new_err(
                    "porto: the scope is not entered yet; spawn inside its block: \
                     `async with porto.scope() as s: s.spawn(...)`",
                ));
            }
            Phase::Closed => {
                return Err(PyRuntimeError::new_err(
                    "porto: scope is closed: its `async with` block has exited, so it cannot \
                     spawn more tasks; spawn from inside the block, or open a new scope",
                ));
            }
        }
        let coro = coroutine_from(&coro_or_fn, &args, "Scope.spawn")?;
        let task = Py::new(
            py,
            Task {
                scope: self.shared.clone(),
                outcome: Mutex::new(None),
            },
        )?;
        self.shared.lock().live += 1;
        if let Err(error) = pytask::spawn(&handle, coro, Spawned::Child(task.clone_ref(py))) {
            self.shared.lock().live -= 1;
            return Err(error);
        }
        Ok(task)
    }

    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        if !lane::is_active() {
            return Err(NoRuntimeError::new_err(
                "porto: `async with porto.scope()` only works in tasks of a porto runtime; \
                 start the program with porto.run(main) and open the scope inside main",
            ));
        }
        let mut state = slf.get().shared.lock();
        if state.phase != Phase::New {
            return Err(PyRuntimeError::new_err(
                "porto: a scope can be entered only once; open a new one with \
                 `async with porto.scope() as s:` for each block",
            ));
        }
        state.phase = Phase::Open;
        drop(state);
        // T2: entering completes synchronously.
        checkpoint_if_needed(slf.py(), slf.clone().into_any().unbind())
    }

    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __aexit__<'py>(
        slf: &Bound<'py, Self>,
        _exc_type: &Bound<'py, PyAny>,
        exc: &Bound<'py, PyAny>,
        _traceback: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        // The owner is being closed (its runtime is shutting down): it cannot wait any more, its
        // children are dropped with the runtime, and `GeneratorExit` must go through, so the
        // exit must not yield (no checkpoint).
        if exc.is_instance_of::<PyGeneratorExit>() {
            let errors = slf.get().close();
            drop(errors);
            return Ok(Bound::new(py, Ready::new(py.None()))?.into_any());
        }
        if slf.get().shared.lock().live == 0 {
            slf.get().finish(exc)?;
            // T2: no child to wait for, so the exit completes synchronously.
            return Ok(checkpoint_if_needed(py, py.None())?.into_bound(py));
        }
        FINISH_SCOPE
            .import(py, "porto._bridge", "finish_scope")?
            .call1((slf, exc))
    }

    /// While children run: files the running task as the scope's waiter and returns a trap to
    /// await, which resumes it once the last child finished. `None` once none runs.
    fn _wait_trap(&self) -> PyResult<Option<Trap>> {
        if self.shared.lock().live == 0 {
            return Ok(None);
        }
        let Some(waker) = current_waker() else {
            return Err(NoRuntimeError::new_err(
                "porto: a scope's exit must be awaited by a porto task; use \
                 `async with porto.scope()` inside porto.run(main)",
            ));
        };
        lane::register_scope_waiter(self.shared.key(), waker);
        Ok(Some(Trap::park()))
    }

    /// Closes the scope; raises the children's failures, after `exc` (the body's exception, or
    /// `None`), as an exception group.
    #[pyo3(signature = (exc, /))]
    fn _finish(&self, exc: &Bound<'_, PyAny>) -> PyResult<()> {
        self.finish(exc)
    }

    fn __repr__(&self) -> String {
        let state = self.shared.lock();
        let phase = match state.phase {
            Phase::New => "new",
            Phase::Open => "open",
            Phase::Closed => "closed",
        };
        format!("<porto.Scope {phase}, {} live tasks>", state.live)
    }
}

impl Scope {
    /// Closes the scope and returns the children's failures.
    fn close(&self) -> Vec<Py<PyBaseException>> {
        let mut state = self.shared.lock();
        state.phase = Phase::Closed;
        std::mem::take(&mut state.errors)
    }

    fn finish(&self, exc: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = exc.py();
        let errors = self.close();
        if errors.is_empty() {
            return Ok(());
        }
        let exceptions = PyList::empty(py);
        if !exc.is_none() {
            exceptions.append(exc)?;
        }
        for error in errors {
            exceptions.append(error)?;
        }
        // `BaseExceptionGroup(...)` returns an `ExceptionGroup` when every member is an
        // `Exception`.
        let group = BASE_EXCEPTION_GROUP
            .import(py, "builtins", "BaseExceptionGroup")?
            .call1(("porto scope failed", exceptions))?;
        Err(PyErr::from_value(group))
    }
}

/// A child task of a scope. Its outcome is readable once it finished, which is certain after
/// the scope's `async with` block exited.
#[pyclass(frozen, module = "porto._core", name = "Task")]
pub struct Task {
    scope: Arc<ScopeShared>,
    /// `None` until the task finishes. Only held to store, read or take the outcome.
    outcome: Mutex<Option<TaskOutcome>>,
}

impl Task {
    fn lock(&self) -> MutexGuard<'_, Option<TaskOutcome>> {
        self.outcome.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stores the outcome of the finished task and tells its scope.
    pub(crate) fn finish(&self, py: Python<'_>, outcome: TaskOutcome) {
        let error = match &outcome {
            TaskOutcome::Ok(_) | TaskOutcome::Cancelled => None,
            TaskOutcome::Err(exception) => Some(exception.clone_ref(py)),
        };
        *self.lock() = Some(outcome);
        self.scope.child_finished(error);
    }
}

#[pymethods]
impl Task {
    /// Whether the task finished (returned or raised).
    fn done(&self) -> bool {
        self.lock().is_some()
    }

    /// What the task returned, or raises what it raised.
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &*self.lock() {
            Some(outcome) => outcome.clone_ref(py).into_result(py),
            None => Err(InvalidStateError::new_err(
                "porto: Task.result() was called before the task finished; read it after the \
                 scope's `async with` block, which waits for every task spawned into it \
                 (task.done() tells whether it finished)",
            )),
        }
    }

    /// `Task[int]` at runtime, for annotations that are evaluated.
    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = cls.py();
        py.import("types")?
            .getattr("GenericAlias")?
            .call1((cls, item))
    }

    fn __repr__(&self) -> &'static str {
        match &*self.lock() {
            None => "<porto.Task running>",
            Some(TaskOutcome::Ok(_)) => "<porto.Task returned>",
            Some(TaskOutcome::Err(_)) => "<porto.Task raised>",
            Some(TaskOutcome::Cancelled) => "<porto.Task cancelled>",
        }
    }

    // The outcome may reference the task again (an exception's traceback, a returned object):
    // let the cycle collector see through it.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        // Never block in the collector; a held lock means the outcome is being stored or read.
        let Ok(outcome) = self.outcome.try_lock() else {
            return Ok(());
        };
        match &*outcome {
            Some(TaskOutcome::Ok(value)) => visit.call(value),
            Some(TaskOutcome::Err(exception)) => visit.call(exception),
            Some(TaskOutcome::Cancelled) | None => Ok(()),
        }
    }

    fn __clear__(&self) {
        let outcome = self.lock().take();
        drop(outcome);
    }
}
