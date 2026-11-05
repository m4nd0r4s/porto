//! Test hooks for the trap kinds that no public API makes yet (11-task-model §5.2, §5.3):
//!
//! - `_WaitProbe`: a bare wait queue; `probe.wait()` is a `Notified` trap on it;
//! - `_spawn_joinable(coro, context=None)` spawns a task and returns a `_JoinTarget`, whose
//!   `join()` is a `Join` trap (the Python `Task` gets `await` with porto-szb.8) and whose
//!   `steps` counts the coroutine resumes (the step loop's round trips are tested with it);
//! - `_checkpoint_if_needed(value)`: what a synchronous completion returns (T2);
//! - `_current_task_token()` and `_interrupt(token, exc)`: throw `exc` into a waiting task,
//!   which gives its wait up first (T3), as cancellation will (porto-szb.36).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use porto_core::sync::{Locked, WaitNode, WaitQueue, WaitState};
use porto_core::task::TaskBodyRef;
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;

use crate::driver::pytask::{self, PyTask, Spawned};
use crate::driver::thin::{WaitSource, Waitable};
use crate::driver::trap::{Trap, TrapKind};
use crate::driver::{lane, step};

/// Adds the hooks to `module`.
pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<WaitProbe>()?;
    module.add_class::<JoinTarget>()?;
    module.add_class::<TaskToken>()?;
    module.add_function(wrap_pyfunction!(spawn_joinable, module)?)?;
    module.add_function(wrap_pyfunction!(checkpoint_if_needed, module)?)?;
    module.add_function(wrap_pyfunction!(current_task_token, module)?)?;
    module.add_function(wrap_pyfunction!(interrupt, module)?)
}

/// The probe's queue state: nothing to acquire, every wait waits for a notify.
struct ProbeState;

impl WaitState for ProbeState {
    /// A `notify_one` grant whose waiter gave up: the next waiter gets it (T3: not lost).
    fn pass_grant(waiters: &mut Locked<'_, Self>, _token: usize) {
        waiters.grant_front();
    }
}

struct ProbeQueue(WaitQueue<ProbeState>);

// SAFETY: forwards to `WaitQueue`, whose node protocol this trait describes.
unsafe impl WaitSource for ProbeQueue {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract is `WaitQueue::poll_wait`'s.
        unsafe { self.0.poll_wait(node, cx, |_| false) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: as above.
        unsafe { self.0.cancel(node) }
    }
}

/// A wait queue to test `Notified` traps with.
#[pyclass(frozen, module = "porto._core", name = "_WaitProbe")]
struct WaitProbe {
    queue: Arc<Waitable<ProbeQueue>>,
}

#[pymethods]
impl WaitProbe {
    #[new]
    fn new() -> Self {
        Self {
            queue: Waitable::new(ProbeQueue(WaitQueue::new(ProbeState))),
        }
    }

    /// A `Notified` trap: the awaiting task waits until a notify grants its wait node.
    fn wait(&self) -> Trap {
        Trap::new(TrapKind::Notified(self.queue.target()))
    }

    /// Wakes every task waiting now; returns how many.
    fn notify_all(&self) -> usize {
        self.queue.0.lock().grant_all()
    }

    /// Grants the longest waiting task; returns whether one was waiting.
    fn notify_one(&self) -> bool {
        self.queue.0.lock().grant_front()
    }

    /// How many tasks are queued (their wait node is linked).
    #[getter]
    fn waiting(&self) -> usize {
        self.queue.0.lock().len()
    }
}

/// A task spawned by `_spawn_joinable`: other tasks can await it with `join()`.
#[pyclass(frozen, module = "porto._core", name = "_JoinTarget")]
struct JoinTarget {
    task: TaskBodyRef<PyTask>,
}

#[pymethods]
impl JoinTarget {
    /// A `Join` trap: the await evaluates to the task's result, or raises its exception.
    fn join(&self) -> Trap {
        Trap::new(TrapKind::Join {
            target: self.task.clone(),
        })
    }

    /// Whether the task finished.
    #[getter]
    fn done(&self) -> bool {
        self.task.is_complete()
    }

    /// How many times the driver resumed the task's coroutine (`send` or `throw`) so far. Read
    /// it from another task of the same run.
    #[getter]
    fn steps(&self) -> PyResult<u32> {
        if !lane::is_active() || step::is_running(self.task.header()) {
            return Err(PyRuntimeError::new_err(
                "porto: _JoinTarget.steps must be read from another task of the run the target \
                 belongs to",
            ));
        }
        // SAFETY: on the target's lane (single-lane run) and not inside its poll; the reference
        // keeps the body alive.
        Ok(unsafe { PyTask::steps(self.task.body()) })
    }
}

/// Starts `coro` as a task on the running runtime and returns a handle to join it. Nothing
/// reports its exception but a join. The task runs in a copy of the current context, or in
/// `context` itself if given (so a test can hand it a context that is already entered).
#[pyfunction(name = "_spawn_joinable")]
#[pyo3(signature = (coro, /, context = None))]
fn spawn_joinable(
    coro: Bound<'_, PyAny>,
    context: Option<Bound<'_, PyAny>>,
) -> PyResult<JoinTarget> {
    let Some(handle) = lane::current() else {
        return Err(PyRuntimeError::new_err(
            "porto: _spawn_joinable() must be called from a task running under _run(); start \
             the runtime with _run(main()) and spawn from inside main",
        ));
    };
    let task = match context {
        Some(context) => {
            // SAFETY: attached (we hold a `Bound`); `context` is live.
            if unsafe { ffi::PyContext_CheckExact(context.as_ptr()) } == 0 {
                return Err(PyTypeError::new_err(
                    "_spawn_joinable: context must be a contextvars.Context; pass \
                     contextvars.copy_context() or leave it out",
                ));
            }
            pytask::spawn_in_context(&handle, coro, context.unbind(), Spawned::Joined)
        }
        None => pytask::spawn(&handle, coro, Spawned::Joined)?,
    };
    Ok(JoinTarget { task })
}

/// What a porto awaitable that completed synchronously with `value` returns (T2): a `_Ready`,
/// or a yield trap that resumes with `value` once the running task's budget is spent.
#[pyfunction(name = "_checkpoint_if_needed")]
#[pyo3(signature = (value, /))]
fn checkpoint_if_needed(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    step::checkpoint_if_needed(py, value)
}

/// The running task, for `_interrupt`. Keeps the task alive.
#[pyclass(frozen, module = "porto._core", name = "_TaskToken")]
struct TaskToken {
    waker: Waker,
}

/// A token for the task calling it.
#[pyfunction(name = "_current_task_token")]
fn current_task_token() -> PyResult<TaskToken> {
    let Some(waker) = step::current_waker() else {
        return Err(PyRuntimeError::new_err(
            "porto: _current_task_token() must be called from a porto task; call it inside \
             a coroutine that _run() runs",
        ));
    };
    Ok(TaskToken { waker })
}

/// Throws `exception` into the task behind `token` at the await it waits in (its wait is given
/// up first), or at its next wait that is not over at once. Call it from another task of the
/// same run.
#[pyfunction(name = "_interrupt")]
#[pyo3(signature = (token, exception, /))]
fn interrupt(token: &TaskToken, exception: Bound<'_, PyBaseException>) -> PyResult<()> {
    if !lane::is_active() {
        return Err(PyRuntimeError::new_err(
            "porto: _interrupt() must be called from a task of the run the target task \
             belongs to",
        ));
    }
    lane::interrupt(&token.waker, exception.unbind());
    Ok(())
}
