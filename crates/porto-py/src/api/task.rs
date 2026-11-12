//! `porto.Task`: the handle `Scope.spawn` returns, and `porto.current_task()`
//! (11-task-model-and-trap-protocol.md §7, 20-python-api-reference.md `Task`).
//!
//! A task and its handle point at each other, but only one of the two ways owns: the task's body
//! holds its handle strongly (`PyTask::py_handle`) until it ends, when it hands the handle its
//! outcome ([`Task::finish`]), while the handle holds the task *weakly*, as a pointer to its
//! header that is upgraded to a counted reference when a method needs the task
//! (`TaskBodyRef::try_from_ptr`). So no reference cycle keeps a task alive that nothing can
//! wake any more, and the handle owns the outcome (and so shows it to the cycle collector) once
//! the task ended, however it ended.
//!
//! A handle is made lazily (spec 11 §7): `Scope.spawn` makes one, `porto.current_task()` makes
//! one for a task that has none, and an internal spawn (`_spawn`, the Douro request tasks) makes
//! none. [`handles_created`] counts them, for the test that proves it.

use std::cell::Cell;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

use parking_lot::{Mutex, MutexGuard};
use porto_core::scope::CancelReason;
use porto_core::task::{Header, TaskBodyRef};
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use pyo3::types::{PyString, PyType};
use pyo3::{PyTraverseError, PyVisit};

use super::time::require_lane;
use crate::driver::pytask::{PyScopeNode, PyScopeRef, PyTask, TaskOutcome, shut_down_error};
use crate::driver::step;
use crate::driver::trap::{Trap, TrapKind};
use crate::errors::{InvalidStateError, NoRuntimeError};

thread_local! {
    /// How many `porto.Task` objects this thread made. Per thread, not one counter for the
    /// process: a handle is made at every spawn, and a shared atomic would be contended once
    /// lanes spawn in parallel (M2). Tests read it on the lane thread they run on.
    static HANDLES_CREATED: Cell<u64> = const { Cell::new(0) };
}

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

/// Test hook: how many `porto.Task` objects the calling thread made so far. An internal spawn
/// makes none; `Scope.spawn` makes one per task, and `current_task()` one for a task with none.
#[pyfunction(name = "_task_handles_created")]
fn task_handles_created() -> u64 {
    handles_created()
}

/// How many `porto.Task` objects the calling thread made so far.
pub(crate) fn handles_created() -> u64 {
    HANDLES_CREATED.get()
}

/// `Task::body` once the task ended: no pointer to follow any more, and none to set.
const ENDED: *mut Header = usize::MAX as *mut Header;

/// What a handle knows about its task, under its lock.
struct Inner {
    /// `None` until the task finishes. A parking_lot lock guards it: std's allocates a pthread
    /// mutex per task on macOS (porto-itr.7).
    outcome: Option<TaskOutcome>,
    /// The task's implicit scope (spec 15 §4), made when the task first enters a scope: the
    /// base scope of everything it enters, which `Task.cancel()` cancels. Dropped once the
    /// task ended.
    implicit: Option<PyScopeRef>,
}

/// A task: the child of a scope, or the one running now. Its outcome is readable once it
/// finished, which is certain after the `async with` block of its scope exited.
#[pyclass(frozen, module = "porto._core", name = "Task")]
pub struct Task {
    inner: Mutex<Inner>,
    /// The task's id, 0 until the handle is bound to its task.
    id: AtomicU64,
    /// A pointer to the task's header while it runs: null before the handle is bound, [`ENDED`]
    /// once it ended. Set by the spawner without the lock (the spawn is a hot path), cleared by
    /// the task's body under it (`Task::finish`), and followed only under it, so it never
    /// dangles while it can be followed: the task's body takes that lock before the allocation
    /// is released.
    body: AtomicPtr<Header>,
    /// The name given to `spawn` (a `str`), else what was spawned (the async function or the
    /// coroutine object), whose `__qualname__` is the name: read when the name is, so a spawn
    /// pays one reference for it. `None` for a task spawned with neither: it is `Task-<id>`.
    label: Option<Py<PyAny>>,
    /// Spawned with `pin=True`, or by a pinned task (the header's `PIN` flag).
    pinned: bool,
    /// `Task.cancel()` was called. The task reads it at every checkpoint while it has no
    /// implicit scope yet (a task that entered no scope): then it is the task's own cancel
    /// scope, and one the task makes once it first enters a scope is cancelled at once.
    cancel_requested: AtomicBool,
}

impl Task {
    /// A handle for a task about to be spawned, to [`bind`](Task::bind) to it. `label` is its
    /// name (a `str`), or the object it is spawned from, whose `__qualname__` names it.
    pub(crate) fn new(label: Option<Py<PyAny>>, pinned: bool) -> Self {
        HANDLES_CREATED.set(HANDLES_CREATED.get() + 1);
        Self {
            inner: Mutex::new(Inner {
                outcome: None,
                implicit: None,
            }),
            id: AtomicU64::new(0),
            body: AtomicPtr::new(ptr::null_mut()),
            label,
            pinned,
            cancel_requested: AtomicBool::new(false),
        }
    }

    /// Whether the task ended (its outcome is stored, or the task was freed unfinished), for
    /// the scope's list of children to drop it.
    #[inline]
    pub(crate) fn has_ended(&self) -> bool {
        self.body.load(Ordering::Acquire) == ENDED
    }

    /// Whether `Task.cancel()` was called.
    #[inline]
    pub(crate) fn cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::Relaxed)
    }

    /// Makes the task's implicit scope when the task, which runs on this thread and has not
    /// entered a scope, is about to enter its first: a child of `base`, the scope it was
    /// spawned into (a root for a task spawned into none), which the caller installs as the
    /// task's base so that the scopes it enters are nested in it. `None` if the task has one
    /// already (or ended). Cancelled at once if `Task.cancel()` was called before.
    pub(crate) fn install_implicit(
        &self,
        base: Option<&PyScopeRef>,
        owner: u64,
    ) -> Option<PyScopeRef> {
        let mut inner = self.lock();
        if inner.implicit.is_some() || inner.outcome.is_some() {
            return None;
        }
        let scope = match base {
            Some(base) => PyScopeNode::new_child(base, owner, false),
            None => PyScopeNode::new_root(owner),
        };
        inner.implicit = Some(scope.clone());
        // Read under the lock `cancel` stores under: it either sees the scope and cancels it,
        // or stored first and is seen here.
        let requested = self.cancel_requested.load(Ordering::SeqCst);
        drop(inner);
        if requested {
            scope.cancel(CancelReason::TaskCancelled);
        }
        Some(scope)
    }

    /// Points the handle at its task, which was just spawned.
    pub(crate) fn bind(&self, task: &TaskBodyRef<PyTask>) {
        // SAFETY: the reference keeps the task alive, and `as_ptr` is the allocation's pointer.
        unsafe { self.bind_ptr(task.as_ptr()) };
    }

    /// [`Task::bind`], from the pointer of a task that is alive, with its handle already
    /// known by it (or the one polling it).
    ///
    /// # Safety
    /// `ptr` is a task's pointer with the provenance of its allocation (`TaskBodyRef::as_ptr`,
    /// or the data of its waker), and the task is alive now.
    pub(crate) unsafe fn bind_ptr(&self, ptr: NonNull<Header>) {
        // SAFETY: the caller's contract.
        self.id
            .store(unsafe { ptr.as_ref() }.id(), Ordering::Relaxed);
        // A task that ended already (another lane ran it to the end before this) has told
        // the handle (`ENDED`): nothing to follow.
        let _ended = self.body.compare_exchange(
            ptr::null_mut(),
            ptr.as_ptr(),
            Ordering::Release,
            Ordering::Relaxed,
        );
    }

    /// The task's name: the one given to `spawn`, else the `__qualname__` of what was spawned
    /// (as Trio names tasks), else its type's; `Task-<id>` for a task with neither.
    fn resolve_name<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        let Some(label) = &self.label else {
            return Ok(PyString::new(
                py,
                &format!("Task-{}", self.id.load(Ordering::Relaxed)),
            ));
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
        self.body.store(ENDED, Ordering::Release);
        let implicit = inner.implicit.take();
        let previous = inner.outcome.replace(outcome);
        drop(inner);
        drop(previous);
        drop(implicit);
    }

    /// A counted reference to the task, if it still runs.
    fn upgrade(&self, _locked: &Inner) -> Option<TaskBodyRef<PyTask>> {
        let body = self.body.load(Ordering::Acquire);
        if body.is_null() || body == ENDED {
            return None;
        }
        // SAFETY: `body` points at the header of a task of this handle, and the caller holds
        // the lock (`_locked`): the task's body ends the pointer under that lock before it is
        // freed (`Task::finish`, from `PyTask::drop`), so the allocation is still there. The
        // task is a PyTask: only `Scope.spawn` and the step driver bind a handle.
        unsafe { TaskBodyRef::try_from_ptr(NonNull::new_unchecked(body)) }
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
        self.id.load(Ordering::Relaxed)
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

    /// Cancels the task, and only it: it is cancelled as if it ran inside a scope of its own
    /// that `Scope.cancel()` was called on (spec 15 §4). The task gets `porto.Cancelled` at its
    /// next checkpoint (a waiting task is woken), again at every one while it goes on, and so
    /// do the tasks spawned into scopes it entered. The siblings and the scope it was spawned
    /// into are not cancelled, and the `Cancelled` that ends the task is not a failure: the
    /// scope's exit raises nothing for it. A shield the task entered holds the cancellation
    /// off until it is left. Cancelling a task that finished does nothing; cancelling again
    /// does nothing more. Safe from any thread.
    fn cancel(&self) {
        let (implicit, task) = {
            let inner = self.lock();
            if inner.outcome.is_some() {
                return;
            }
            self.cancel_requested.store(true, Ordering::SeqCst);
            (inner.implicit.clone(), self.upgrade(&inner))
        };
        // The scope's cancel wakes the tasks waiting in it, this one included...
        if let Some(scope) = implicit {
            scope.cancel(CancelReason::TaskCancelled);
        }
        // ... and a task with no scope yet (or running, or queued) is woken here, which
        // makes it look at its flag at its next checkpoint.
        if let Some(task) = task {
            task.task_ref().clone().into_waker().wake();
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
                None => self.upgrade(&inner),
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
        let task = {
            let inner = self.lock();
            self.upgrade(&inner)
        }?;
        // The reference may be the last one (the task finished and was freed meanwhile): its
        // drop runs `PyTask::drop`, which takes the lock, so it must not be held here.
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
