//! The step loop: `PyTask::poll` (11-task-model-and-trap-protocol.md §4).
//!
//! One poll resolves what the task waits for, then sends into the coroutine (`PyIter_Send`) or
//! throws into it, and dispatches on what it yields, until the coroutine finishes or has to
//! wait:
//!
//! - a `_Trap` (exact type) is taken and its [`TrapKind`] handled: `Yield` requeues the task at
//!   the back of the local queue, `Sleep` arms the task's embedded timer, `Park` waits for a
//!   wake that the trap's maker arranged through [`current_waker`];
//! - `None` (a bare `yield` in a generator-based coroutine) is a yield;
//! - anything else is an error thrown back into the coroutine, at the `await` that yielded it.
//!
//! The coroutine's `contextvars.Context` is entered for the whole poll, and the task's waker is
//! reachable through [`current_waker`] while it runs.

use std::cell::Cell;
use std::ptr;
use std::task::{Context, Poll, Waker};

use porto_core::task::Header;
use porto_core::time::Deadline;
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyStopIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::{ffi, intern};

use super::pytask::{COOP_BUDGET, Pending, PyTask, Resume, TaskOutcome};
use super::trap::{Trap, TrapKind};
use crate::py_compat::{Sent, names, send, take_exception};

thread_local! {
    /// The waker of the task this lane is polling, null between polls.
    static CURRENT_WAKER: Cell<*const Waker> = const { Cell::new(ptr::null()) };
}

/// A clone of the waker of the task running on this thread, if a task is running: what native
/// code called from a coroutine registers before handing it a [`TrapKind::Park`] trap.
pub(crate) fn current_waker() -> Option<Waker> {
    let waker = CURRENT_WAKER.get();
    // SAFETY: non-null only while `poll` runs on this thread, which borrows the waker for that
    // whole time (see `RunningWaker`).
    (!waker.is_null()).then(|| unsafe { &*waker }.clone())
}

/// Publishes the polled task's waker in `CURRENT_WAKER` until dropped.
struct RunningWaker(*const Waker);

impl RunningWaker {
    fn set(waker: &Waker) -> Self {
        Self(CURRENT_WAKER.replace(waker))
    }
}

impl Drop for RunningWaker {
    fn drop(&mut self) {
        CURRENT_WAKER.set(self.0);
    }
}

/// Polls `task`: steps its coroutine until it finishes (`Ready`) or waits (`Pending`).
pub(super) fn poll(
    task: &mut PyTask,
    header: &Header,
    py: Python<'_>,
    cx: &mut Context<'_>,
) -> Poll<()> {
    // Moved out for the poll (no refcount traffic) and put back before `complete` or `Pending`.
    let Some(coro) = task.coro.take() else {
        // porto-core never polls a completed body; nothing to do if it did.
        return Poll::Ready(());
    };
    let coro = coro.into_bound(py);
    // SAFETY: `task` owns the context and keeps it until it is freed (`complete` releases only
    // the coroutine), so it outlives the guard.
    let _context = match unsafe { EnteredContext::enter(py, task.context.as_ptr()) } {
        Ok(entered) => entered,
        Err(exception) => {
            task.coro = Some(coro.unbind());
            task.complete(py, header, TaskOutcome::Err(exception));
            return Poll::Ready(());
        }
    };
    let _waker = RunningWaker::set(cx.waker());
    task.budget = COOP_BUDGET;
    let outcome = step_until_wait(task, &coro, cx);
    task.coro = Some(coro.unbind());
    match outcome {
        Some(outcome) => {
            task.complete(py, header, outcome);
            Poll::Ready(())
        }
        None => Poll::Pending,
    }
}

/// Runs steps until the coroutine finishes (`Some(outcome)`) or the task waits (`None`).
fn step_until_wait(
    task: &mut PyTask,
    coro: &Bound<'_, PyAny>,
    cx: &mut Context<'_>,
) -> Option<TaskOutcome> {
    loop {
        match task.take_pending() {
            // A yielded task was requeued; a parked one is only polled after a wake: whoever
            // woke it ended the wait.
            Pending::None | Pending::Yield | Pending::Park => {}
            Pending::Sleep(deadline) => {
                // SAFETY: the timer is embedded in this task, which its lane polls with the
                // task's own waker.
                if unsafe { task.timer().poll(cx) }.is_pending() {
                    task.set_pending(Pending::Sleep(deadline));
                    return None;
                }
            }
            Pending::YieldThen(value) => task.set_resume(Resume::Value(value)),
            Pending::AsyncioFuture(_) => {
                unreachable!("asyncio futures are not awaited yet (the compatibility layer)")
            }
        }
        if task.budget == 0 {
            // Out of budget with a step to go (a throw after an unsupported await): let the
            // other tasks run first.
            yield_now(task, cx);
            return None;
        }
        task.budget -= 1;
        task.stats.steps = task.stats.steps.wrapping_add(1);
        let yielded = match resume(coro, task.take_resume(coro.py())) {
            Stepped::Finished(outcome) => return Some(outcome),
            Stepped::Yielded(yielded) => yielded,
        };
        match dispatch(&yielded) {
            Next::Yield => {
                yield_now(task, cx);
                return None;
            }
            Next::Sleep(deadline) => {
                task.set_pending(Pending::Sleep(deadline));
                // SAFETY: as above.
                if unsafe { task.timer().poll(cx) }.is_ready() {
                    // A deadline that already passed is still a schedule point (spec 11 §4).
                    yield_now(task, cx);
                }
                return None;
            }
            Next::Park => {
                task.set_pending(Pending::Park);
                return None;
            }
            Next::Throw(error) => task.set_resume(Resume::Throw(error.unbind())),
        }
    }
}

/// Requeues the task at the back of its lane's local queue: a wake while it runs makes the lane
/// reschedule it when this poll returns `Pending`.
fn yield_now(task: &mut PyTask, cx: &Context<'_>) {
    task.set_pending(Pending::Yield);
    cx.waker().wake_by_ref();
}

/// What one step of the coroutine did.
enum Stepped<'py> {
    /// It returned or raised: the task is done.
    Finished(TaskOutcome),
    /// It suspended, yielding this object to the driver.
    Yielded(Bound<'py, PyAny>),
}

/// Resumes the coroutine once with `resume`.
fn resume<'py>(coro: &Bound<'py, PyAny>, resume: Resume) -> Stepped<'py> {
    let py = coro.py();
    let sent = match resume {
        Resume::None => send(coro, None),
        Resume::Value(value) => send(coro, Some(value.into_bound(py))),
        Resume::Throw(exception) => return throw(coro, exception),
    };
    match sent {
        Sent::Returned(value) => Stepped::Finished(TaskOutcome::Ok(value.unbind())),
        Sent::Yielded(yielded) => Stepped::Yielded(yielded),
        Sent::Raised(exception) => Stepped::Finished(TaskOutcome::Err(exception.unbind())),
    }
}

/// Throws `exception` into the coroutine (`coro.throw(exception)`).
fn throw<'py>(coro: &Bound<'py, PyAny>, exception: Py<PyBaseException>) -> Stepped<'py> {
    let py = coro.py();
    // SAFETY: attached; `coro`, the interned name and `exception` are live for the call.
    // Returns a new reference, or NULL with an exception set.
    let result = unsafe {
        ffi::PyObject_CallMethodOneArg(coro.as_ptr(), names(py).throw.as_ptr(), exception.as_ptr())
    };
    drop(exception);
    // SAFETY: a new reference, or NULL.
    match unsafe { Bound::from_owned_ptr_or_opt(py, result) } {
        Some(yielded) => Stepped::Yielded(yielded),
        None => Stepped::Finished(thrown_outcome(take_exception(py))),
    }
}

/// The outcome of a `throw()` that did not suspend: `StopIteration` means the coroutine caught
/// the exception and returned (a coroutine cannot raise `StopIteration` itself, PEP 479).
fn thrown_outcome(raised: Bound<'_, PyBaseException>) -> TaskOutcome {
    if !raised.is_instance_of::<PyStopIteration>() {
        return TaskOutcome::Err(raised.unbind());
    }
    match raised.getattr(intern!(raised.py(), "value")) {
        Ok(value) => TaskOutcome::Ok(value.unbind()),
        Err(error) => TaskOutcome::Err(error.into_value(raised.py())),
    }
}

/// What the driver does with a yielded object.
enum Next<'py> {
    Yield,
    Sleep(Deadline),
    Park,
    /// Throw this error into the coroutine and step again.
    Throw(Bound<'py, PyBaseException>),
}

fn dispatch<'py>(yielded: &Bound<'py, PyAny>) -> Next<'py> {
    let py = yielded.py();
    if let Ok(trap) = yielded.cast_exact::<Trap>() {
        return match trap.get().take() {
            Some(TrapKind::Yield) => Next::Yield,
            Some(TrapKind::Sleep { deadline }) => Next::Sleep(deadline),
            Some(TrapKind::Park) => Next::Park,
            None => Next::Throw(
                PyRuntimeError::new_err(
                    "porto: a trap reached the driver without being awaited (or twice); \
                     `await` porto awaitables, do not `yield` them",
                )
                .into_value(py)
                .into_bound(py),
            ),
        };
    }
    if yielded.is_none() {
        return Next::Yield;
    }
    Next::Throw(unsupported_awaitable(yielded))
}

/// The `TypeError` thrown into a coroutine that awaited something porto cannot wait for.
fn unsupported_awaitable<'py>(yielded: &Bound<'py, PyAny>) -> Bound<'py, PyBaseException> {
    let py = yielded.py();
    let type_name = yielded
        .get_type()
        .fully_qualified_name()
        .map_or_else(|_| "<unknown>".to_owned(), |name| name.to_string());
    PyTypeError::new_err(format!(
        "porto: a task awaited an object porto cannot wait for (its awaitable yielded an \
         object of type {type_name}); porto tasks can await coroutines and porto awaitables. \
         asyncio futures, tasks and other event-loop objects need the asyncio compatibility \
         layer (porto.asyncio), and other libraries need their porto or anyio backend"
    ))
    .into_value(py)
    .into_bound(py)
}

/// The task's context, entered for one poll (`PyContext_Enter`/`PyContext_Exit`, spec 11 §8).
///
/// It holds the context as a pointer, not a reference, so the task stays mutable during the
/// poll; the task owns the context for at least as long.
struct EnteredContext<'py> {
    py: Python<'py>,
    context: *mut ffi::PyObject,
}

impl<'py> EnteredContext<'py> {
    /// # Safety
    /// `context` is a `contextvars.Context` that stays alive until the guard is dropped.
    unsafe fn enter(
        py: Python<'py>,
        context: *mut ffi::PyObject,
    ) -> Result<Self, Py<PyBaseException>> {
        // SAFETY: attached (we hold `py`); `context` is live (caller).
        if unsafe { ffi::PyContext_Enter(context) } < 0 {
            return Err(take_exception(py).unbind());
        }
        Ok(Self { py, context })
    }
}

impl Drop for EnteredContext<'_> {
    fn drop(&mut self) {
        // SAFETY: attached; the context is live (see `enter`), and was entered by `enter` on
        // this thread and not exited since.
        if unsafe { ffi::PyContext_Exit(self.context) } < 0 {
            let error = take_exception(self.py);
            PyErr::from_value(error.into_any()).write_unraisable(self.py, None);
        }
    }
}
