//! The step loop: `PyTask::poll` (11-task-model-and-trap-protocol.md §4).
//!
//! One poll resolves what the task waits for, then sends into the coroutine (`PyIter_Send`) or
//! throws into it, and dispatches on what it yields, until the coroutine finishes or has to
//! wait:
//!
//! - a `_Trap` (exact type) is taken and its request becomes the task's [`Pending`] wait:
//!   `Yield` and `YieldThen` requeue the task at the back of the local queue, `Sleep` arms the
//!   task's embedded timer (a deadline already passed is a yield), and every other kind is
//!   polled at once: a wait that is already over continues the loop without a scheduler round
//!   trip;
//! - `None` (a bare `yield` in a generator-based coroutine) is a yield;
//! - anything else is an error thrown back into the coroutine, at the `await` that yielded it.
//!
//! The coroutine's `contextvars.Context` is entered for the whole poll; a context that cannot be
//! entered fails the task, and its coroutine is closed. While it runs, the task's waker is
//! reachable through [`current_waker`] and its budget through [`checkpoint_if_needed`] (T2).
//!
//! The budget starts at the lane's `coop_budget` (`porto_core::coop`) and every step spends a
//! unit, a throw included: a coroutine that keeps catching the error of an unsupported await
//! yields like any other. Out of budget with a step to go, the task yields to the back of the
//! local queue.
//!
//! A woken task whose wait is still not over is where a cancellation is delivered: the wait is
//! abandoned (T3, `wait.rs`) and the exception thrown in. Until scopes deliver cancellation
//! (porto-szb.36, `scopes::effective_cancel`), the one source is [`lane::take_interrupt`].
//! porto-szb.36 also turns a `Cancelled` that belongs to a cancelling scope into
//! `TaskOutcome::Cancelled`; until then every exception is `TaskOutcome::Err`.

use std::cell::Cell;
use std::ptr;
use std::task::{Context, Poll, Waker};

use porto_core::coop;
use porto_core::task::Header;
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyStopIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::{ffi, intern};

use super::lane;
use super::pending::{Kind, Pending};
use super::pytask::{PyTask, Resume, TaskOutcome};
use super::ready::Ready;
use super::trap::{Trap, TrapKind};
use crate::py_compat::{Sent, names, send, take_exception};

thread_local! {
    /// The task this lane is polling, null between polls.
    static RUNNING: Cell<*const Running> = const { Cell::new(ptr::null()) };
}

/// What `RUNNING` publishes of the polled task: its waker and its budget. Lives in `poll`'s
/// frame, which outlives the publication (see `RunningTask`).
struct Running {
    waker: *const Waker,
    /// Steps and synchronous completions left in this poll (T2).
    budget: Cell<u32>,
}

/// The task running on this thread, if a task is running.
fn running<'a>() -> Option<&'a Running> {
    let running = RUNNING.get();
    // SAFETY: non-null only while `poll` runs on this thread, which owns the `Running` for
    // that whole time (see `RunningTask`); it is only ever shared.
    (!running.is_null()).then(|| unsafe { &*running })
}

/// A clone of the waker of the task running on this thread, if a task is running.
pub(crate) fn current_waker() -> Option<Waker> {
    // SAFETY: `poll` borrows the waker for as long as it publishes it.
    running().map(|running| unsafe { &*running.waker }.clone())
}

/// Whether the task with this header is the one running on this thread (its waker's data is
/// its header, see `lane::task_key`).
pub(crate) fn is_running(header: &Header) -> bool {
    running().is_some_and(|running| {
        // SAFETY: `poll` borrows the waker for as long as it publishes it.
        unsafe { &*running.waker }.data() == std::ptr::from_ref(header).cast()
    })
}

/// Publishes the polled task in `RUNNING` until dropped.
struct RunningTask(*const Running);

impl RunningTask {
    fn set(running: &Running) -> Self {
        Self(RUNNING.replace(running))
    }
}

impl Drop for RunningTask {
    fn drop(&mut self) {
        RUNNING.set(self.0);
    }
}

/// T2: what a Porto awaitable that completed synchronously with `value` returns instead of a
/// plain ready awaitable (spec 11 §5.3).
///
/// It spends a unit of the running task's budget, so a loop of synchronous completions cannot
/// starve the lane: once the budget is spent, the result comes back as a `Yield` trap that
/// resumes the task with `value` after the other ready tasks ran (`Pending::YieldThen`, the
/// task goes to the back of the local queue). The step that resumes it spends the next unit,
/// so a loop of them yields every `coop_budget` awaits. Otherwise, and outside a porto task (a
/// manual driver, the asyncio facade), it is a `_Ready`.
///
/// It will also raise `Cancelled` when the task's effective scope is cancelled and this
/// checkpoint has not delivered it yet: cancel delivery (porto-szb.36) hooks in here.
pub(crate) fn checkpoint_if_needed(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    if let Some(running) = running() {
        let budget = &running.budget;
        match budget.get() {
            0 => return Ok(Py::new(py, Trap::new(TrapKind::YieldThen(value)))?.into_any()),
            left => budget.set(left - 1),
        }
    }
    Ok(Py::new(py, Ready::new(value))?.into_any())
}

/// T2 for a native awaitable that completed synchronously and suspends by itself: spends one
/// unit of the running task's budget exactly as [`checkpoint_if_needed`] does, and returns
/// whether the budget was already spent (the awaitable then yields a `YieldThen` trap with its
/// result). Always `false` outside a porto task.
pub(crate) fn spend_budget() -> bool {
    running().is_some_and(|running| match running.budget.get() {
        0 => true,
        left => {
            running.budget.set(left - 1);
            false
        }
    })
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
            // The coroutine never runs again: close it, or a coroutine that never started
            // warns that it was never awaited when it is freed.
            if let Err(error) = coro.call_method0(intern!(py, "close")) {
                error.write_unraisable(py, Some(&coro));
            }
            task.coro = Some(coro.unbind());
            task.complete(py, header, TaskOutcome::Err(exception));
            return Poll::Ready(());
        }
    };
    let running = Running {
        waker: cx.waker(),
        // The lane's budget for this poll (`coop_budget`), kept here so a step does not touch a
        // thread local. Rust futures the task awaits draw on the lane's own count.
        budget: Cell::new(coop::remaining().unwrap_or(u32::MAX)),
    };
    let outcome = {
        // Published only while the coroutine steps: Python code that `complete` runs (a
        // `Task.finish`, an unraisable hook) must not see the finished task as running.
        let _running = RunningTask::set(&running);
        step_until_wait(task, &coro, &running.budget, cx)
    };
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
    budget: &Cell<u32>,
    cx: &mut Context<'_>,
) -> Option<TaskOutcome> {
    let py = coro.py();
    loop {
        if task.is_waiting() {
            match task.poll_pending(py, cx) {
                // `None` keeps a resume set before the wait (a throw delayed by the budget).
                Poll::Ready(Resume::None) => {}
                Poll::Ready(resume) => task.set_resume(resume),
                Poll::Pending => {
                    // Still waiting, unless an interrupt is due (`None`: the task waits).
                    let exception = lane::take_interrupt(cx.waker())?;
                    task.abandon_wait();
                    task.set_resume(Resume::Throw(exception));
                }
            }
        }
        if budget.get() == 0 {
            // Out of budget with a step to go (a throw after an unsupported await, a wait that
            // was over at once): let the other tasks run first. A loop that keeps catching the
            // `TypeError` of an unsupported await yields here too, so it cannot hang the lane.
            yield_now(task, cx);
            return None;
        }
        budget.set(budget.get() - 1);
        task.stats.steps = task.stats.steps.wrapping_add(1);
        let yielded = match resume(coro, task.take_resume(py)) {
            Stepped::Finished(outcome) => return Some(outcome),
            Stepped::Yielded(yielded) => yielded,
        };
        match dispatch(task, &yielded) {
            // The hot path of a switch: no payload to pack.
            Next::Wait(Pending::Yield) => {
                yield_now(task, cx);
                return None;
            }
            Next::Wait(pending) => {
                let kind = pending.kind();
                task.set_pending(pending);
                match kind {
                    Kind::Yield | Kind::YieldThen => {
                        cx.waker().wake_by_ref();
                        return None;
                    }
                    Kind::Sleep => {
                        // SAFETY: the timer is embedded in this task, which its lane polls with
                        // the task's own waker.
                        if unsafe { task.timer().poll(cx) }.is_ready() {
                            // A deadline that already passed is still a schedule point (spec 11
                            // §4): the timer is disarmed, so the wait becomes a yield.
                            task.release_wait();
                            yield_now(task, cx);
                        }
                        return None;
                    }
                    // Polled at the top of the loop: a wait that is already over continues
                    // without a round trip through the scheduler.
                    _ => {}
                }
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
    /// Wait for this.
    Wait(Pending),
    /// Throw this error into the coroutine and step again.
    Throw(Bound<'py, PyBaseException>),
}

fn dispatch<'py>(task: &PyTask, yielded: &Bound<'py, PyAny>) -> Next<'py> {
    let py = yielded.py();
    if let Ok(trap) = yielded.cast_exact::<Trap>() {
        return match trap.get().take() {
            Some(Pending::Join(target)) if task.is_same_task(target.body()) => {
                Next::Throw(error(py, SELF_JOIN))
            }
            Some(pending) => Next::Wait(pending),
            None => Next::Throw(error(py, TRAP_NOT_AWAITED)),
        };
    }
    if yielded.is_none() {
        return Next::Wait(Pending::Yield);
    }
    Next::Throw(unsupported_awaitable(yielded))
}

const TRAP_NOT_AWAITED: &str = "porto: a trap reached the driver without being awaited (or \
                                twice); `await` porto awaitables, do not `yield` them";
const SELF_JOIN: &str = "porto: a task awaited itself, which would wait forever; await the \
                         task from another task";

fn error<'py>(py: Python<'py>, message: &'static str) -> Bound<'py, PyBaseException> {
    PyRuntimeError::new_err(message)
        .into_value(py)
        .into_bound(py)
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
