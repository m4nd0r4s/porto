//! `porto.Task`: the handle `Scope.spawn` returns, and `porto.current_task()`
//! (11-task-model-and-trap-protocol.md §7, 20-python-api-reference.md `Task`).
//!
//! A task and its handle point at each other, but only one of the two ways owns: the task's body
//! holds its handle strongly (`PyTask::py_handle`) until it ends, when it hands the handle its
//! outcome ([`Task::finish`]), while the handle holds the task *weakly*, as a pointer to its
//! header that is upgraded to a counted reference when a method needs the task
//! (`TaskBodyRef::try_from_header`). So no reference cycle keeps a task alive that nothing can
//! wake any more, and the handle owns the outcome (and so shows it to the cycle collector) once
//! the task ended, however it ended.
//!
//! A handle is made lazily (spec 11 §7): `Scope.spawn` makes one, `porto.current_task()` makes
//! one for a task that has none, and an internal spawn (`_spawn`, the Douro request tasks) makes
//! none. [`handles_created`] counts them, for the test that proves it.

use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, MutexGuard};
use porto_core::task::{Header, TaskBodyRef};
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use pyo3::types::{PyString, PyType};
use pyo3::{PyTraverseError, PyVisit};

use super::time::require_lane;
use crate::driver::pytask::{PyTask, TaskOutcome, shut_down_error};
use crate::driver::step;
use crate::driver::trap::{Trap, TrapKind};
use crate::errors::{InvalidStateError, NoRuntimeError};

/// How many `porto.Task` objects were made in this process.
static HANDLES_CREATED: AtomicU64 = AtomicU64::new(0);

/// Adds the class and the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Task>()?;
    module.add_function(wrap_pyfunction!(current_task, module)?)?;
    module.add_function(wrap_pyfunction!(task_handles_created, module)?)
}

/// `porto.current_task()`: the `Task` of the task that calls it. It is the object `Scope.spawn`
/// returned for a task spawned into a scope; a task with no handle (an internal spawn) gets one
/// now, named `Task-<id>`, and keeps it for its next calls.
#[pyfunction]
fn current_task(py: Python<'_>) -> PyResult<Py<Task>> {
    step::current_task(py)?.ok_or_else(|| {
        NoRuntimeError::new_err(
            "porto: porto.current_task() was called outside a task of a porto runtime; call it \
             from code that porto.run(main) runs",
        )
    })
}

/// Test hook: how many `porto.Task` objects were made so far in this process. An internal spawn
/// makes none; `Scope.spawn` makes one per task, and `current_task()` one for a task with none.
#[pyfunction(name = "_task_handles_created")]
fn task_handles_created() -> u64 {
    handles_created()
}

/// How many `porto.Task` objects were made so far in this process.
pub(crate) fn handles_created() -> u64 {
    HANDLES_CREATED.load(Ordering::Relaxed)
}

/// A pointer to a task's header that the handle may follow only under its lock.
struct BodyPtr(NonNull<Header>);

// SAFETY: the pointer is only followed while the handle's lock is held, and the task's body
// takes that lock to clear it (`Task::finish`) before the allocation is released, so it never
// dangles while it can be read. The header's own fields are `Sync`.
unsafe impl Send for BodyPtr {}

/// What a handle knows about its task, under its lock.
struct Inner {
    /// The task's id, 0 until the handle is bound to its task.
    id: u64,
    /// The task, while it runs: `None` before the handle is bound and once it ended.
    body: Option<BodyPtr>,
    /// `None` until the task finishes. A parking_lot lock guards it: std's allocates a pthread
    /// mutex per task on macOS (porto-itr.7).
    outcome: Option<TaskOutcome>,
}

/// A task: the child of a scope, or the one running now. Its outcome is readable once it
/// finished, which is certain after the `async with` block of its scope exited.
#[pyclass(frozen, module = "porto._core", name = "Task")]
pub struct Task {
    inner: Mutex<Inner>,
    /// The name given to `spawn` (a `str`), else what was spawned (the async function or the
    /// coroutine object), whose `__qualname__` is the name: read when the name is, so a spawn
    /// pays one reference for it. `None` for a task spawned with neither: it is `Task-<id>`.
    label: Option<Py<PyAny>>,
    /// Spawned with `pin=True`, or by a pinned task (the header's `PIN` flag).
    pinned: bool,
}

impl Task {
    /// A handle for a task about to be spawned, to [`bind`](Task::bind) to it. `label` is its
    /// name (a `str`), or the object it is spawned from, whose `__qualname__` names it.
    pub(crate) fn new(label: Option<Py<PyAny>>, pinned: bool) -> Self {
        HANDLES_CREATED.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Mutex::new(Inner {
                id: 0,
                body: None,
                outcome: None,
            }),
            label,
            pinned,
        }
    }

    /// Points the handle at its task, which was just spawned.
    pub(crate) fn bind(&self, task: &TaskBodyRef<PyTask>) {
        self.bind_header(task.header());
    }

    /// [`Task::bind`], from the header of a task that is alive, with its handle already known
    /// by it (or the one polling it).
    pub(crate) fn bind_header(&self, header: &Header) {
        let mut inner = self.lock();
        inner.id = header.id();
        // A task that ended already (another lane ran it to the end before this) has told
        // the handle: nothing to follow.
        if inner.outcome.is_none() {
            inner.body = Some(BodyPtr(NonNull::from(header)));
        }
    }

    /// The task's name: the one given to `spawn`, else the `__qualname__` of what was spawned
    /// (as Trio names tasks), else its type's; `Task-<id>` for a task with neither.
    fn resolve_name<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        let Some(label) = &self.label else {
            return Ok(PyString::new(py, &format!("Task-{}", self.lock().id)));
        };
        let label = label.bind(py);
        if let Ok(name) = label.cast::<PyString>() {
            return Ok(name.clone());
        }
        match label.getattr(pyo3::intern!(py, "__qualname__")) {
            Ok(name) => name.str(),
            Err(_) => label.get_type().qualname(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock()
    }

    /// Stores the outcome of the finished task (its scope hears of it from porto-core) and
    /// forgets the task itself: the handle needs nothing from it any more.
    pub(crate) fn finish(&self, outcome: TaskOutcome) {
        let mut inner = self.lock();
        inner.body = None;
        let previous = inner.outcome.replace(outcome);
        drop(inner);
        drop(previous);
    }

    /// A counted reference to the task, if it still runs.
    fn upgrade(inner: &Inner) -> Option<TaskBodyRef<PyTask>> {
        let body = inner.body.as_ref()?;
        // SAFETY: `body` points at the header of a task of this handle, which the caller holds
        // the lock of: the task's body clears the pointer under that lock before it is freed
        // (`Task::finish`, from `PyTask::drop`), so the allocation is still there. The task is
        // a PyTask: only `Scope.spawn` and the step driver bind a handle.
        unsafe { TaskBodyRef::try_from_header(body.0.as_ref()) }
    }

    /// The outcome, cloned, if the task finished.
    fn outcome(&self, py: Python<'_>) -> Option<TaskOutcome> {
        self.lock()
            .outcome
            .as_ref()
            .map(|outcome| outcome.clone_ref(py))
    }
}

/// What awaiting a task that finished gives: its result (a synchronous completion, T2), or its
/// exception raised.
fn finished(py: Python<'_>, outcome: TaskOutcome) -> PyResult<Py<PyAny>> {
    match outcome {
        TaskOutcome::Ok(value) => step::checkpoint_if_needed(py, value),
        failed => failed.into_result(py),
    }
}

fn not_finished(method: &str) -> PyErr {
    InvalidStateError::new_err(format!(
        "porto: Task.{method}() was called before the task finished; read it after the scope's \
         `async with` block, which waits for every task spawned into it, or `await` the task \
         (task.done() tells whether it finished)"
    ))
}

#[pymethods]
impl Task {
    /// The task's id: unique in the process, increasing in spawn order.
    #[getter]
    fn id(&self) -> u64 {
        self.lock().id
    }

    /// Whether the task finished: it returned, raised, or was cancelled.
    fn done(&self) -> bool {
        self.lock().outcome.is_some()
    }

    /// Whether the task ended cancelled: a cancellation reached it (`Scope.cancel()`, a
    /// deadline, a failing sibling, `Task.cancel()`), or its runtime shut down first.
    fn cancelled(&self) -> bool {
        matches!(self.lock().outcome, Some(TaskOutcome::Cancelled(_)))
    }

    /// What the task returned, or raises what it raised (`Cancelled` if it was cancelled).
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.outcome(py) {
            Some(outcome) => outcome.into_result(py),
            None => Err(not_finished("result")),
        }
    }

    /// What the task raised, or `None` if it returned. Raises like `result()` for a task that
    /// was cancelled, as `asyncio.Task.exception()` does.
    fn exception(&self, py: Python<'_>) -> PyResult<Option<Py<PyBaseException>>> {
        match self.outcome(py) {
            Some(TaskOutcome::Err(exception)) => Ok(Some(exception)),
            Some(TaskOutcome::Ok(_)) => Ok(None),
            Some(cancelled) => cancelled.into_result(py).map(|_| None),
            None => Err(not_finished("exception")),
        }
    }

    /// `await task`: waits for the task, then evaluates to its result or raises its exception.
    /// A task that finished already completes at once, with no trap, like any synchronous
    /// completion (T2); a task awaited while it runs yields a `Join` trap (spec 11 §7).
    fn __await__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let target = {
            let inner = self.lock();
            match &inner.outcome {
                Some(outcome) => {
                    let outcome = outcome.clone_ref(py);
                    drop(inner);
                    return finished(py, outcome);
                }
                None => Self::upgrade(&inner),
            }
        };
        require_lane("Task.__await__")?;
        // No target and no outcome: the task is being freed unfinished, which ends it
        // cancelled (`PyTask::drop`).
        let Some(target) = target else {
            return Err(shut_down_error());
        };
        Ok(Py::new(py, Trap::new(TrapKind::Join { target }))?.into_any())
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

    /// The lane the task last ran on, `None` once it finished (and before lanes record
    /// themselves, M2: lane 0 is the only one).
    #[getter]
    fn lane(&self) -> Option<u16> {
        let inner = self.lock();
        let task = Self::upgrade(&inner)?;
        Some(task.header().owner_lane())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let state = match &self.lock().outcome {
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
        if let Some(label) = &self.label {
            visit.call(label)?;
        }
        // Never block in the collector; a held lock means the outcome is being stored or read.
        let Some(inner) = self.inner.try_lock() else {
            return Ok(());
        };
        match &inner.outcome {
            Some(TaskOutcome::Ok(value)) => visit.call(value),
            Some(TaskOutcome::Err(exception) | TaskOutcome::Cancelled(Some(exception))) => {
                visit.call(exception)
            }
            Some(TaskOutcome::Cancelled(None)) | None => Ok(()),
        }
    }

    fn __clear__(&self) {
        let outcome = self.lock().outcome.take();
        drop(outcome);
    }
}
