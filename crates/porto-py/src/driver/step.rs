//! The step loop: `PyTask::poll` (11-task-model-and-trap-protocol.md §4).
//!
//! One poll resolves what the task waits for, then sends into the coroutine (`PyIter_Send`) or
//! throws into it, and dispatches on what it yields, until the coroutine finishes or has to
//! wait:
//!
//! - a `_Trap` (exact type) is taken and its [`TrapKind`] handled: `Yield` requeues the task at
//!   the back of the local queue, `Sleep` registers a lane timer, `Park` waits for a wake that
//!   the trap's maker arranged through [`current_waker`];
//! - `None` (a bare `yield` in a generator-based coroutine) is a yield;
//! - anything else is an error thrown back into the coroutine, at the `await` that yielded it.
//!
//! The coroutine's `contextvars.Context` is entered for the whole poll, and the task's waker is
//! reachable through [`current_waker`] while it runs.

use std::cell::Cell;
use std::future::Future;
use std::mem;
use std::ptr;
use std::task::{Context, Poll, Waker};

use porto_core::time::{Deadline, sleep_until};
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyStopIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::{ffi, intern};

use super::pytask::{Outcome, Pending, PyTask, Resume};
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
pub(super) fn poll(task: &mut PyTask, py: Python<'_>, cx: &mut Context<'_>) -> Poll<()> {
    // Moved out for the poll (no refcount traffic) and put back if the task has to wait.
    let Some(coro) = task.coro.take() else {
        // porto-core never polls a completed future; nothing to do if it did.
        return Poll::Ready(());
    };
    let coro = coro.into_bound(py);
    // SAFETY: `task` owns the context and keeps it for the whole poll (`complete` releases
    // only the coroutine), so it outlives the guard.
    let _context = match unsafe { EnteredContext::enter(py, task.context.as_ptr()) } {
        Ok(entered) => entered,
        Err(exception) => {
            task.coro = Some(coro.unbind());
            task.complete(py, Outcome::Raised(exception));
            return Poll::Ready(());
        }
    };
    let _waker = RunningWaker::set(cx.waker());
    match step_until_wait(task, &coro, cx) {
        Some(outcome) => {
            task.coro = Some(coro.unbind());
            task.complete(py, outcome);
            Poll::Ready(())
        }
        None => {
            task.coro = Some(coro.unbind());
            Poll::Pending
        }
    }
}

/// Runs steps until the coroutine finishes (`Some(outcome)`) or the task waits (`None`).
fn step_until_wait(
    task: &mut PyTask,
    coro: &Bound<'_, PyAny>,
    cx: &mut Context<'_>,
) -> Option<Outcome> {
    loop {
        match &mut task.pending {
            Pending::None => {}
            // A parked task is only polled after a wake: whoever woke it ended the wait.
            Pending::Yield | Pending::Park => task.pending = Pending::None,
            Pending::Sleep(sleep) => match sleep.as_mut().poll(cx) {
                Poll::Pending => return None,
                Poll::Ready(()) => task.pending = Pending::None,
            },
        }
        let yielded = match resume(coro, mem::replace(&mut task.resume, Resume::None)) {
            Stepped::Finished(outcome) => return Some(outcome),
            Stepped::Yielded(yielded) => yielded,
        };
        match dispatch(&yielded) {
            Next::Yield => {
                yield_now(task, cx);
                return None;
            }
            Next::Sleep(deadline) => {
                let mut sleep = Box::pin(sleep_until(deadline));
                // A deadline that already passed is still a schedule point (spec 11 §4 notes).
                if sleep.as_mut().poll(cx).is_ready() {
                    yield_now(task, cx);
                } else {
                    task.pending = Pending::Sleep(sleep);
                }
                return None;
            }
            Next::Park => {
                task.pending = Pending::Park;
                return None;
            }
            Next::Throw(error) => task.resume = Resume::Throw(error.unbind()),
        }
    }
}

/// Requeues the task at the back of its lane's local queue: a wake while it runs makes the lane
/// reschedule it when this poll returns `Pending`.
fn yield_now(task: &mut PyTask, cx: &Context<'_>) {
    task.pending = Pending::Yield;
    cx.waker().wake_by_ref();
}

/// What one step of the coroutine did.
enum Stepped<'py> {
    /// It returned or raised: the task is done.
    Finished(Outcome),
    /// It suspended, yielding this object to the driver.
    Yielded(Bound<'py, PyAny>),
}

/// Resumes the coroutine once with `resume`.
fn resume<'py>(coro: &Bound<'py, PyAny>, resume: Resume) -> Stepped<'py> {
    let py = coro.py();
    match resume {
        Resume::None => match send(coro, None) {
            Sent::Returned(value) => Stepped::Finished(Outcome::Returned(value.unbind())),
            Sent::Yielded(yielded) => Stepped::Yielded(yielded),
            Sent::Raised(exception) => Stepped::Finished(Outcome::Raised(exception.unbind())),
        },
        Resume::Throw(exception) => {
            // SAFETY: attached; `coro`, the interned name and `exception` are live for the call.
            // Returns a new reference, or NULL with an exception set.
            let result = unsafe {
                ffi::PyObject_CallMethodOneArg(
                    coro.as_ptr(),
                    names(py).throw.as_ptr(),
                    exception.as_ptr(),
                )
            };
            drop(exception);
            // SAFETY: a new reference, or NULL.
            match unsafe { Bound::from_owned_ptr_or_opt(py, result) } {
                Some(yielded) => Stepped::Yielded(yielded),
                None => Stepped::Finished(thrown_outcome(take_exception(py))),
            }
        }
    }
}

/// The outcome of a `throw()` that did not suspend: `StopIteration` means the coroutine caught
/// the exception and returned (a coroutine cannot raise `StopIteration` itself, PEP 479).
fn thrown_outcome(raised: Bound<'_, PyBaseException>) -> Outcome {
    if !raised.is_instance_of::<PyStopIteration>() {
        return Outcome::Raised(raised.unbind());
    }
    match raised.getattr(intern!(raised.py(), "value")) {
        Ok(value) => Outcome::Returned(value.unbind()),
        Err(error) => Outcome::Raised(error.into_value(raised.py())),
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
