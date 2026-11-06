//! `porto.Task`: the handle `Scope.spawn` returns (11-task-model-and-trap-protocol.md §7,
//! 20-python-api-reference.md `Task`).

use parking_lot::{Mutex, MutexGuard};
use pyo3::prelude::*;
use pyo3::types::PyType;
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
}

impl Task {
    /// A handle for a task about to be spawned.
    pub(crate) fn new() -> Self {
        Self {
            outcome: Mutex::new(None),
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

    fn __repr__(&self) -> &'static str {
        match &*self.lock() {
            None => "<porto.Task running>",
            Some(TaskOutcome::Ok(_)) => "<porto.Task returned>",
            Some(TaskOutcome::Err(_)) => "<porto.Task raised>",
            Some(TaskOutcome::Cancelled(_)) => "<porto.Task cancelled>",
        }
    }

    // The outcome may reference the task again (an exception's traceback, a returned object):
    // let the cycle collector see through it.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
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
