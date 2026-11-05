//! `porto.scope()`, `Scope` and `Task`: the nursery (15-structured-concurrency-and-
//! cancellation.md §1-§2, the parts without cancel delivery).
//!
//! A scope is a node of porto-core's scope tree (`ScopeNode`), made when its block is entered.
//! Its children are spawned into the node, which counts them (`live_children`) and records
//! their failures, each of which cancels the scope with `ChildFailed` (C4, through
//! `PyTask::take_failure`). `__aexit__` waits until no child is left (C5) on a `ScopeExit`
//! trap, which files the owner's waker as the node's `exit_waiter`; the last child to end wakes
//! it. The failures are raised at exit as a strict exception group, the body's exception first
//! (ADR-009). Spawning into a closed scope raises (C7).
//!
//! Not yet: delivering the cancellation (porto-szb.36). A failing child cancels the scope, but
//! its siblings and the body only stop once checkpoints raise `Cancelled`; until then they run
//! to completion. Nor are deadlines and timeouts (C9), shields (C8), `start()` (§3),
//! `Task.cancel()` and awaiting a task (§4), names, and spawning from other lanes or threads
//! (C10). The node is a root for now, owned by no recorded task: linking it under the owner's
//! innermost scope needs the running task's cancel stack, which cancel delivery makes
//! reachable from Python calls (porto-szb.36).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use porto_core::scope::{CancelReason, ScopeNode};
use pyo3::exceptions::{PyBaseException, PyGeneratorExit, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyTuple, PyType};
use pyo3::{PyTraverseError, PyVisit};

use super::run::coroutine_from;
use crate::driver::lane;
use crate::driver::pytask::{self, PyScopeRef, Spawned, TaskOutcome};
use crate::driver::ready::Ready;
use crate::driver::step::checkpoint_if_needed;
use crate::driver::trap::{Trap, TrapKind};
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
        node: OnceLock::new(),
        closed: AtomicBool::new(false),
    }
}

/// A nursery: every task spawned into it finishes before its `async with` block exits.
#[pyclass(frozen, module = "porto._core", name = "Scope")]
pub struct Scope {
    /// The scope's node in the tree, set when the block is entered.
    node: OnceLock<PyScopeRef>,
    /// Whether its `async with` block exited.
    closed: AtomicBool,
}

/// Where a scope is in its life.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Created, not entered yet.
    New,
    /// Inside its `async with` block, or waiting for children at its exit.
    Open,
    /// Its `async with` block exited.
    Closed,
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
        let node = match (self.phase(), self.node.get()) {
            (Phase::Open, Some(node)) => node.clone(),
            (Phase::Closed, _) => {
                return Err(PyRuntimeError::new_err(
                    "porto: scope is closed: its `async with` block has exited, so it cannot \
                     spawn more tasks; spawn from inside the block, or open a new scope",
                ));
            }
            _ => {
                return Err(PyRuntimeError::new_err(
                    "porto: the scope is not entered yet; spawn inside its block: \
                     `async with porto.scope() as s: s.spawn(...)`",
                ));
            }
        };
        let coro = coroutine_from(&coro_or_fn, &args, "Scope.spawn")?;
        let task = Py::new(
            py,
            Task {
                outcome: Mutex::new(None),
            },
        )?;
        let spawned = Spawned::Child {
            task: task.clone_ref(py),
            scope: node,
        };
        pytask::spawn(&handle, coro, spawned)?;
        Ok(task)
    }

    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        if !lane::is_active() {
            return Err(NoRuntimeError::new_err(
                "porto: `async with porto.scope()` only works in tasks of a porto runtime; \
                 start the program with porto.run(main) and open the scope inside main",
            ));
        }
        if slf.get().node.set(ScopeNode::new_root(0)).is_err() {
            return Err(PyRuntimeError::new_err(
                "porto: a scope can be entered only once; open a new one with \
                 `async with porto.scope() as s:` for each block",
            ));
        }
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
        if slf.get().live_children() == 0 {
            slf.get().finish(exc)?;
            // T2: no child to wait for, so the exit completes synchronously.
            return Ok(checkpoint_if_needed(py, py.None())?.into_bound(py));
        }
        FINISH_SCOPE
            .import(py, "porto._bridge", "finish_scope")?
            .call1((slf, exc))
    }

    /// While children run: a `ScopeExit` trap to await, which resumes the awaiting task once
    /// the last child ended. `None` once none runs.
    fn _wait_trap(&self) -> Option<Trap> {
        if self.live_children() == 0 {
            return None;
        }
        let scope = self.node.get()?.clone();
        Some(Trap::new(TrapKind::ScopeExit { scope }))
    }

    /// Closes the scope; raises the children's failures, after `exc` (the body's exception, or
    /// `None`), as an exception group.
    #[pyo3(signature = (exc, /))]
    fn _finish(&self, exc: &Bound<'_, PyAny>) -> PyResult<()> {
        self.finish(exc)
    }

    fn __repr__(&self) -> String {
        let phase = match self.phase() {
            Phase::New => "new",
            Phase::Open => "open",
            Phase::Closed => "closed",
        };
        let Some(node) = self.node.get() else {
            return format!("<porto.Scope {phase}>");
        };
        let cancelled = match node.cancel_called() {
            None => "",
            Some(CancelReason::Explicit) => ", cancelled",
            Some(CancelReason::Deadline) => ", cancelled: deadline",
            Some(CancelReason::Parent) => ", cancelled: parent",
            Some(CancelReason::ChildFailed) => ", cancelled: a child failed",
            Some(CancelReason::Shutdown) => ", cancelled: shutdown",
        };
        let state = node.lock_state();
        let exit = if state.exit_waiter.is_some() {
            ", exit waiting"
        } else {
            ""
        };
        format!(
            "<porto.Scope {phase}{cancelled}, {} live tasks{exit}>",
            state.live_children
        )
    }
}

impl Scope {
    fn phase(&self) -> Phase {
        if self.closed.load(Ordering::Acquire) {
            Phase::Closed
        } else if self.node.get().is_some() {
            Phase::Open
        } else {
            Phase::New
        }
    }

    /// Children spawned and not ended.
    fn live_children(&self) -> u32 {
        self.node
            .get()
            .map_or(0, |node| node.lock_state().live_children)
    }

    /// Closes the scope and returns the children's failures.
    fn close(&self) -> Vec<Py<PyBaseException>> {
        self.closed.store(true, Ordering::Release);
        let Some(node) = self.node.get() else {
            return Vec::new();
        };
        // Taken out under the lock, dropped by the caller after it.
        std::mem::take(&mut node.lock_state().errors)
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
    /// `None` until the task finishes. Only held to store, read or take the outcome.
    outcome: Mutex<Option<TaskOutcome>>,
}

impl Task {
    fn lock(&self) -> MutexGuard<'_, Option<TaskOutcome>> {
        self.outcome.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stores the outcome of the finished task (its scope hears of it from porto-core).
    pub(crate) fn finish(&self, outcome: TaskOutcome) {
        let previous = self.lock().replace(outcome);
        drop(previous);
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
