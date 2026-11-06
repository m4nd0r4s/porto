//! `porto.Task`: the handle `Scope.spawn` returns (11-task-model-and-trap-protocol.md §7,
//! 20-python-api-reference.md `Task`).

use parking_lot::{Mutex, MutexGuard};
use pyo3::prelude::*;
use pyo3::types::{PyString, PyType};
use pyo3::{PyTraverseError, PyVisit};

use crate::driver::pytask::TaskOutcome;
use crate::errors::InvalidStateError;

/// Adds the class to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Task>()
}

/// A child task of a scope. Its outcome is readable once it finished, which is certain after
/// the scope's `async with` block exited.
#[pyclass(frozen, module = "porto._core", name = "Task")]
pub struct Task {
    /// `None` until the task finishes. Only held to store, read or take the outcome. A
    /// parking_lot lock: std's allocates a pthread mutex per task on macOS (porto-itr.7).
    outcome: Mutex<Option<TaskOutcome>>,
    /// The name given to `spawn` (a `str`), else what was spawned (the async function or the
    /// coroutine object), whose `__qualname__` is the name: read when the name is, so a spawn
    /// pays one reference for it.
    label: Py<PyAny>,
    /// Spawned with `pin=True`, or by a pinned task (the header's `PIN` flag).
    pinned: bool,
}

impl Task {
    /// A handle for a task about to be spawned. `label` is its name (a `str`), or the object it
    /// is spawned from, whose `__qualname__` names it.
    pub(crate) fn new(label: Py<PyAny>, pinned: bool) -> Self {
        Self {
            outcome: Mutex::new(None),
            label,
            pinned,
        }
    }

    /// The task's name: the one given to `spawn`, else the `__qualname__` of what was spawned
    /// (as Trio names tasks), else its type's.
    fn resolve_name<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        let label = self.label.bind(py);
        if let Ok(name) = label.cast::<PyString>() {
            return Ok(name.clone());
        }
        match label.getattr(pyo3::intern!(py, "__qualname__")) {
            Ok(name) => name.str(),
            Err(_) => label.get_type().qualname(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<TaskOutcome>> {
        self.outcome.lock()
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

    /// The task's name: `spawn(..., name=...)`, else the `__qualname__` of the async function
    /// or coroutine it runs.
    #[getter]
    fn name<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        self.resolve_name(py)
    }

    /// Whether the task may never move to another lane: spawned with `pin=True`, or with no
    /// `pin` by a pinned task.
    #[getter]
    fn pinned(&self) -> bool {
        self.pinned
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let state = match &*self.lock() {
            None => "running",
            Some(TaskOutcome::Ok(_)) => "returned",
            Some(TaskOutcome::Err(_)) => "raised",
            Some(TaskOutcome::Cancelled(_)) => "cancelled",
        };
        let name = self.resolve_name(py)?.repr()?;
        let pinned = if self.pinned { ", pinned" } else { "" };
        Ok(format!("<porto.Task {name} {state}{pinned}>"))
    }

    // The outcome may reference the task again (an exception's traceback, a returned object),
    // and so may what it was spawned from (a closure): let the cycle collector see through
    // them. The label is never cleared (the type is frozen); clearing the outcome, or the
    // function or coroutine the cycle goes through, breaks the cycle.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.label)?;
        // Never block in the collector; a held lock means the outcome is being stored or read.
        let Some(outcome) = self.outcome.try_lock() else {
            return Ok(());
        };
        match &*outcome {
            Some(TaskOutcome::Ok(value)) => visit.call(value),
            Some(TaskOutcome::Err(exception) | TaskOutcome::Cancelled(Some(exception))) => {
                visit.call(exception)
            }
            Some(TaskOutcome::Cancelled(None)) | None => Ok(()),
        }
    }

    fn __clear__(&self) {
        let outcome = self.lock().take();
        drop(outcome);
    }
}
