//! `Started[T]`: what `Scope.start()` hands the task it starts (15-structured-concurrency-and-
//! cancellation.md §3).
//!
//! `await s.start(fn, *args)` spawns `fn(*args, started=Started())` into a hidden scope that the
//! caller waits in (`porto._bridge.start`), nested in the scope the caller is in, so cancelling
//! the caller cancels the task, and a failure of the task before it reports ends the wait and
//! is raised from `start` bare. `started.set(value)` is the report: it hands `value` to the
//! caller and moves the task into the scope `start` was called on (Trio's `nursery.start`):
//!
//! - the task now counts in that scope (`live_children`, its failures and its exit wait), and is
//!   counted out of the hidden scope, whose exit the caller waits in then ends;
//! - the hidden scope, which the task and every scope it entered are nested in, is adopted by
//!   that scope (`ScopeNode::adopt`): its cancel, deadline and shield now reach the task, and
//!   the caller's no longer do.

use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use porto_core::sync::SetOnce;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3::{PyTraverseError, PyVisit};

use super::scope::Scope;
use crate::driver::pytask::{PyScopeNode, PyScopeRef};
use crate::driver::step;

/// The `started` argument of a task started by `Scope.start()`: it reports once with `set()`.
#[pyclass(frozen, module = "porto._core", name = "Started")]
pub struct Started {
    /// The scope `start` was called on: where `set` moves the task.
    target: PyScopeRef,
    /// The hidden scope the task runs in until it reports, set by `start` before it spawns.
    hidden: SetOnce<PyScopeNode>,
    /// `set` was called (and took effect, or is taking effect).
    called: AtomicBool,
    /// What `set` was given.
    value: Mutex<Option<Py<PyAny>>>,
}

impl Started {
    /// A `Started` that moves its task into `target`.
    pub(super) fn new_for(target: PyScopeRef) -> Self {
        Self {
            target,
            hidden: SetOnce::new(),
            called: AtomicBool::new(false),
            value: Mutex::new(None),
        }
    }
}

#[pymethods]
impl Started {
    #[new]
    fn new() -> PyResult<Self> {
        Err(PyTypeError::new_err(
            "porto: Started is made by Scope.start(), not by you: define \
             `async def work(..., *, started: porto.Started[T])`, call `started.set(value)` once \
             it is ready, and start it with `value = await scope.start(work, ...)`",
        ))
    }

    /// Reports that the task is ready: hands `value` (default `None`) to the caller of
    /// `Scope.start()`, which returns it, and moves the task into the scope `start` was called
    /// on. Call it once, from the task `start` started, while the scope is open; it raises
    /// `RuntimeError` otherwise.
    #[pyo3(signature = (value=None, /))]
    fn set(&self, py: Python<'_>, value: Option<Py<PyAny>>) -> PyResult<()> {
        if self.called.swap(true, Ordering::AcqRel) {
            return Err(PyRuntimeError::new_err(
                "porto: Started.set() was called twice; a started task reports once, when it is \
                 ready: call it a single time",
            ));
        }
        let Some(hidden) = self.hidden.get_arc() else {
            self.called.store(false, Ordering::Release);
            return Err(PyRuntimeError::new_err(
                "porto: this Started does not belong to a running Scope.start(); pass the one \
                 start() gave the task, and call set() from that task",
            ));
        };
        // The value is in place before the caller can wake (the move ends its wait).
        let value = value.unwrap_or_else(|| py.None());
        *self.value.lock() = Some(value);
        if let Err(error) = step::move_running_task(&hidden, &self.target) {
            let value = self.value.lock().take();
            self.called.store(false, Ordering::Release);
            drop(value);
            return Err(error);
        }
        // `Scope.tasks` of the target lists it from now on: the move changed the scope the task
        // counts in, which is what the registry's walk filters by.
        Ok(())
    }

    /// For `Scope.start`: the hidden scope the task is spawned into.
    #[pyo3(name = "_bind")]
    fn bind(&self, hidden: &Bound<'_, Scope>) -> PyResult<()> {
        let node = hidden.get().node().ok_or_else(|| {
            PyRuntimeError::new_err("porto: Started._bind() needs an entered scope")
        })?;
        self.hidden
            .set(node)
            .map_err(|_| PyRuntimeError::new_err("porto: this Started was bound already"))
    }

    /// For `Scope.start`: whether `set` was called.
    #[pyo3(name = "_was_set")]
    fn was_set(&self) -> bool {
        self.called.load(Ordering::Acquire)
    }

    /// For `Scope.start`: what `set` was given. The task finished without calling `set` if it
    /// was not called.
    #[pyo3(name = "_result")]
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &*self.value.lock() {
            Some(value) if self.called.load(Ordering::Acquire) => Ok(value.clone_ref(py)),
            _ => Err(PyRuntimeError::new_err(
                "porto: the task Scope.start() started finished without calling \
                 started.set(value); call it once the task is ready, before it returns",
            )),
        }
    }

    /// `Started[int]` at runtime, for annotations that are evaluated.
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
        if self.called.load(Ordering::Acquire) {
            "<porto.Started set>"
        } else {
            "<porto.Started waiting>"
        }
    }

    // The value may reference the `Started` again (a task that hands out what holds it): let
    // the cycle collector see through it.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        // Never block in the collector.
        let Some(value) = self.value.try_lock() else {
            return Ok(());
        };
        match &*value {
            Some(value) => visit.call(value),
            None => Ok(()),
        }
    }

    fn __clear__(&self) {
        let value = self.value.lock().take();
        drop(value);
    }
}
