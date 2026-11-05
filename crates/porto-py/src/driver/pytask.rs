//! The PyTask body (11-task-model-and-trap-protocol.md §2): a coroutine, its context, what to
//! resume it with and what it waits for.
//!
//! A PyTask is a porto-core task whose future is a [`PyTask`]: its `poll` runs the step loop
//! (`step.rs`). It is created only through [`spawn`], onto a runtime built by `lane.rs`, so it is
//! only ever polled on a Python lane, with the thread state attached.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use porto_core::Handle;
use porto_core::time::Sleep;
use pyo3::exceptions::{PyBaseException, PyException};
use pyo3::ffi;
use pyo3::prelude::*;

use super::{lane, step};
use crate::api::scope::Task;
use crate::py_compat::take_exception;

/// A Python coroutine running as a porto task.
pub(crate) struct PyTask {
    /// The coroutine; `None` once it finished.
    pub(super) coro: Option<Py<PyAny>>,
    /// The `contextvars.Context` every step runs in.
    pub(super) context: Py<PyAny>,
    /// What the next step sends into the coroutine.
    pub(super) resume: Resume,
    /// What the task waits for before its next step.
    pub(super) pending: Pending,
    /// Where the outcome goes.
    pub(super) completion: Completion,
}

/// Where a task's outcome goes when it finishes.
pub(crate) enum Completion {
    /// Nobody waits for it (`_spawn`). An `Exception` goes to `sys.unraisablehook`; any other
    /// `BaseException` (`KeyboardInterrupt`, `SystemExit`) stops the run (see `lane::fail`).
    Detached,
    /// The one waiter of a [`JoinSlot`]: `porto.run` waiting for `main`.
    Join(Arc<JoinSlot>),
    /// A scope's child: the outcome goes to its [`Task`] object, which tells the scope.
    Child(Py<Task>),
}

// ADR-034: the body stays within 192 bytes (excluding the coroutine).
const _: () = assert!(
    size_of::<PyTask>() <= 192,
    "ADR-034: PyTask body over budget"
);

/// The value of the next step (spec 11 §2).
pub(super) enum Resume {
    /// Send `None`: starts the coroutine, and resumes it after a yield or a sleep.
    None,
    /// Throw this exception in.
    Throw(Py<PyBaseException>),
}

/// What a task waits for between two steps (spec 11 §5.2, the kinds implemented so far).
pub(super) enum Pending {
    /// Nothing: step right away.
    None,
    /// The task yielded: it was requeued, so the next poll resumes it.
    Yield,
    /// The task sleeps until the timer fires. Boxed because a `Sleep` is pinned (it embeds its
    /// wheel entry) and a `PyTask` is moved by `&mut`; the PyTask body (porto-szb.4) embeds the
    /// entry instead.
    Sleep(Pin<Box<Sleep>>),
    /// The task waits for a wake arranged by the maker of a `Park` trap.
    Park,
}

/// How a task ended.
pub(crate) enum Outcome {
    /// The coroutine returned this value.
    Returned(Py<PyAny>),
    /// The coroutine raised this exception.
    Raised(Py<PyBaseException>),
}

impl Outcome {
    /// The value, or the exception as an error (it keeps its traceback).
    pub(crate) fn into_result(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Returned(value) => Ok(value),
            Self::Raised(exception) => Err(PyErr::from_value(exception.into_bound(py).into_any())),
        }
    }
}

/// Where a task leaves its outcome for the one waiter (the `porto.run` caller).
///
/// The lock is only held to store or take the outcome and the waker, never while calling into
/// Python (25-cpython-integration-and-memory.md P3).
#[derive(Default)]
pub(crate) struct JoinSlot {
    state: Mutex<JoinState>,
}

#[derive(Default)]
struct JoinState {
    outcome: Option<Outcome>,
    waiter: Option<Waker>,
}

impl JoinSlot {
    fn set(&self, outcome: Outcome) {
        let waiter = {
            let mut state = self.lock();
            state.outcome = Some(outcome);
            state.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    /// Waits for the outcome.
    pub(crate) fn wait(&self) -> impl Future<Output = Outcome> + '_ {
        poll_fn(|cx| {
            let mut state = self.lock();
            match state.outcome.take() {
                Some(outcome) => Poll::Ready(outcome),
                None => {
                    state.waiter = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, JoinState> {
        // Nothing panics under the lock but an allocation failure; the state stays consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Spawns `coro` as a task on the runtime behind `handle`, which MUST be a Python lane's (see
/// `lane.rs`). The task runs in a copy of the current context, like `asyncio.create_task`.
pub(crate) fn spawn(
    handle: &Handle,
    coro: Bound<'_, PyAny>,
    completion: Completion,
) -> PyResult<()> {
    let py = coro.py();
    // SAFETY: attached (we hold `py`). Returns a new reference, or NULL with an exception set.
    let context = unsafe { ffi::PyContext_CopyCurrent() };
    if context.is_null() {
        return Err(PyErr::from_value(take_exception(py).into_any()));
    }
    // SAFETY: a new, non-null reference, which the `Bound` takes over.
    let context = unsafe { Bound::from_owned_ptr(py, context) }.unbind();
    handle.spawn(PyTask {
        coro: Some(coro.unbind()),
        context,
        resume: Resume::None,
        pending: Pending::None,
        completion,
    });
    Ok(())
}

impl PyTask {
    /// Stores the outcome and releases the coroutine.
    pub(super) fn complete(&mut self, py: Python<'_>, outcome: Outcome) {
        let coro = self.coro.take();
        match (&self.completion, outcome) {
            (Completion::Join(join), outcome) => join.set(outcome),
            (Completion::Child(task), outcome) => task.get().finish(py, outcome),
            (Completion::Detached, Outcome::Returned(_)) => {}
            (Completion::Detached, Outcome::Raised(exception)) => {
                if exception.bind(py).is_instance_of::<PyException>() {
                    // Nobody to raise to: report it like an exception in `__del__`.
                    PyErr::from_value(exception.into_bound(py).into_any())
                        .write_unraisable(py, coro.as_ref().map(|coro| coro.bind(py)));
                } else {
                    // `KeyboardInterrupt`, `SystemExit`: the program asked to stop.
                    lane::fail(exception);
                }
            }
        }
    }
}

impl Future for PyTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: a PyTask is only spawned (by `spawn`) onto a Python lane's runtime, whose lane
        // thread keeps its thread state attached while it runs tasks and detaches only around
        // parks, when no task runs (`lane.rs`).
        let py = unsafe { Python::assume_attached() };
        step::poll(self.get_mut(), py, cx)
    }
}
