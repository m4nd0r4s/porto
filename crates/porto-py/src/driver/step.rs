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
//! local queue. A task out of budget while it waits yields without polling its wait: a wait
//! that resolved holds its grant (a lock, a permit) in its node until the coroutine can take
//! it, so a cancel meanwhile passes the grant on instead of leaking it (porto-szb.105).
//!
//! # Cancellation (spec 15 C1, C2, C10; spec 11 §5.2)
//!
//! While the task's effective scope is cancelled (the walk from its innermost scope, cached
//! per poll in a `CancelCache`), every checkpoint raises `porto.Cancelled`, again and again if
//! the coroutine swallows it (level-triggered):
//!
//! - a trap the coroutine yields is not waited on: `Cancelled` is thrown back in at once
//!   (a `YieldThen`, which holds a result, and a `ScopeExit` excepted);
//! - [`checkpoint_if_needed`] raises it in place of a synchronous result (T2);
//! - a woken task whose cancellable wait is still not over gives the wait up (T3, `wait.rs`)
//!   and gets it thrown in. A wait that is already over wins: its result is delivered, and the
//!   cancellation comes at the next checkpoint.
//!
//! Python code between two awaits is never interrupted. A waiting task is woken by the cancel
//! itself: its cancellable wait is filed with its innermost scope (`PyTask::watch_cancel`).
//! The test hook `_interrupt` ([`lane::take_interrupt`]) throws any exception the same way. A
//! `Cancelled` that ends the coroutine while its scope is cancelling ends the task as
//! `TaskOutcome::Cancelled`.
//!
//! The cancel stack is moved into the poll's [`Running`] record while the coroutine runs, so
//! `Scope.__aenter__` and `__aexit__` push and pop scopes on it ([`enter_cancel_scope`],
//! [`exit_cancel_scope`]) while the loop holds the task.

use std::cell::Cell;
use std::ptr;
use std::task::{Context, Poll, Waker};

use porto_core::coop;
use porto_core::scope::{CancelCache, CancelReason};
use porto_core::task::Header;
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyStopIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::{ffi, intern};

use super::lane;
use super::pending::{Kind, Pending};
use super::pytask::{CancelStack, PyScopeNode, PyScopeRef, PyTask, Resume, TaskOutcome};
use super::ready::Ready;
use super::trap::{Trap, TrapKind};
use crate::errors::Cancelled;
use crate::py_compat::{Sent, names, send, take_exception};

thread_local! {
    /// The task this lane is polling, null between polls.
    static RUNNING: Cell<*const Running> = const { Cell::new(ptr::null()) };
}

/// What `RUNNING` publishes of the polled task: its waker, its budget and its cancel stack.
/// Lives in `poll`'s frame, which outlives the publication (see `RunningTask`).
struct Running {
    waker: *const Waker,
    /// The task's id, the owner of the scopes it enters.
    task_id: u64,
    /// Steps and synchronous completions left in this poll (T2).
    budget: Cell<u32>,
    /// The task's cancel stack while its coroutine runs (moved out of the task around each
    /// step), empty otherwise. Reached through [`Running::with_stack`].
    stack: Cell<CancelStack>,
    /// The task's effective cancellation, cached for the poll: one atomic load per check
    /// while no scope anywhere was cancelled since the last one.
    cache: Cell<CancelCache>,
}

impl Running {
    /// Runs `f` on the cancel stack. `f` MUST NOT run Python code, which could reach the stack
    /// again.
    fn with_stack<R>(&self, f: impl FnOnce(&mut CancelStack) -> R) -> R {
        let mut stack = self.stack.take();
        let result = f(&mut stack);
        self.stack.set(stack);
        result
    }

    /// Whether a task whose innermost scope is `scope` is effectively cancelled.
    #[inline]
    fn is_cancelled(&self, scope: Option<&PyScopeNode>) -> bool {
        let Some(scope) = scope else {
            return false;
        };
        let mut cache = self.cache.get();
        let cancelled = cache.is_cancelled(scope);
        self.cache.set(cache);
        cancelled
    }
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
/// While the task's effective scope is cancelled it raises `Cancelled` instead (C1), before
/// spending anything: an awaitable whose fast path took something (a lock, a permit) gives it
/// back when this raises. The `YieldThen` result is always delivered: once the value is
/// stashed, the coroutine gets it, and the cancellation comes at its next checkpoint.
pub(crate) fn checkpoint_if_needed(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    if let Some(running) = running() {
        if let Some(cancelled) = running_cancellation(py, running) {
            return Err(PyErr::from_value(cancelled.into_bound(py).into_any()));
        }
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

/// The `Cancelled` to raise in the running task, if its effective scope is cancelled.
fn running_cancellation(py: Python<'_>, running: &Running) -> Option<Py<PyBaseException>> {
    let cancelled = running.with_stack(|stack| {
        let scope = stack.innermost()?;
        running.is_cancelled(Some(scope)).then(|| scope.clone())
    })?;
    Some(cancelled_exception(py, &cancelled))
}

/// Enters a new cancel scope in the task running on this thread: `make(parent, owner)` builds
/// its node (no Python code), given the task's innermost scope (`None` outside every scope) and
/// the task's id.
/// Returns the node, or `None` (and makes nothing) if no task's coroutine is running here.
pub(crate) fn enter_cancel_scope(
    make: impl FnOnce(Option<&PyScopeRef>, u64) -> PyScopeRef,
) -> PyResult<Option<PyScopeRef>> {
    let Some(running) = running() else {
        return Ok(None);
    };
    running.with_stack(|stack| {
        let scope = make(stack.innermost(), running.task_id);
        stack.enter(scope.clone())?;
        Ok(Some(scope))
    })
}

/// Exits `scope` in the task running on this thread. Returns `false`, changing nothing, if no
/// task runs here or `scope` is not its innermost entered scope.
pub(crate) fn exit_cancel_scope(scope: &PyScopeNode) -> bool {
    running().is_some_and(|running| {
        scope.owner_task() == running.task_id && running.with_stack(|stack| stack.exit(scope))
    })
}

/// The `Cancelled` delivered to a task whose effective scope, walked from `scope`, is
/// cancelled: its message says which cancellation it is.
fn cancelled_exception(py: Python<'_>, scope: &PyScopeNode) -> Py<PyBaseException> {
    let why = match scope.effective_cancellation().map(|found| found.reason) {
        Some(CancelReason::Deadline) => {
            "the deadline of its scope, or of one it is nested in, \
                                         passed"
        }
        Some(CancelReason::ChildFailed) => {
            "a task of its scope, or of one it is nested in, \
                                            failed, which cancels the scope (C4)"
        }
        Some(CancelReason::Shutdown) => "the runtime is shutting down",
        _ => "Scope.cancel() was called on its scope or on one it is nested in",
    };
    Cancelled::new_err(format!(
        "porto: the task was cancelled: {why}; let the Cancelled propagate (the cancelled scope \
         absorbs it at its `async with` exit): a task that catches it gets it again at its next \
         await"
    ))
    .into_value(py)
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
        task_id: header.id(),
        // The lane's budget for this poll (`coop_budget`), kept here so a step does not touch a
        // thread local. Rust futures the task awaits draw on the lane's own count.
        budget: Cell::new(coop::remaining().unwrap_or(u32::MAX)),
        stack: Cell::new(CancelStack::default()),
        cache: Cell::new(CancelCache::new()),
    };
    let outcome = {
        // Published only while the coroutine steps: Python code that `complete` runs (a
        // `Task.finish`, an unraisable hook) must not see the finished task as running.
        let _running = RunningTask::set(&running);
        step_until_wait(task, &coro, &running, cx)
    };
    task.coro = Some(coro.unbind());
    match outcome {
        Some(outcome) => {
            let outcome = cancelled_by_its_scope(py, task, outcome);
            task.complete(py, header, outcome);
            Poll::Ready(())
        }
        None => Poll::Pending,
    }
}

/// `outcome`, or `Cancelled` if it is a `porto.Cancelled` raised while the task's scope is
/// cancelling: the cancellation ended the task, which did not fail (spec 11 §4).
fn cancelled_by_its_scope(py: Python<'_>, task: &PyTask, outcome: TaskOutcome) -> TaskOutcome {
    match outcome {
        TaskOutcome::Err(exception)
            if exception.bind(py).is_instance_of::<Cancelled>()
                && task
                    .innermost_scope()
                    .is_some_and(|scope| scope.effective_cancellation().is_some()) =>
        {
            TaskOutcome::Cancelled(Some(exception))
        }
        outcome => outcome,
    }
}

/// Runs steps until the coroutine finishes (`Some(outcome)`) or the task waits (`None`).
fn step_until_wait(
    task: &mut PyTask,
    coro: &Bound<'_, PyAny>,
    running: &Running,
    cx: &mut Context<'_>,
) -> Option<TaskOutcome> {
    let py = coro.py();
    let budget = &running.budget;
    loop {
        if task.is_waiting() {
            if budget.get() == 0 {
                // Out of budget with a wait to resolve: yield before polling it. A wait that
                // resolved would hand its grant to a coroutine that only runs after the yield,
                // and a cancel meanwhile would lose it; unpolled, the grant stays in the node,
                // where giving the wait up passes it on (porto-szb.105).
                cx.waker().wake_by_ref();
                return None;
            }
            match task.poll_pending(py, cx) {
                // `None` keeps a resume set before the wait (a throw delayed by the budget).
                Poll::Ready(Resume::None) => {}
                Poll::Ready(resume) => task.set_resume(resume),
                Poll::Pending => {
                    let exception = match lane::take_interrupt(cx.waker()) {
                        Some(exception) => exception,
                        None => cancelled_wait(py, task, running, cx.waker())?,
                    };
                    task.abandon_wait();
                    task.set_resume(Resume::Throw(exception));
                }
            }
        }
        if budget.get() == 0 {
            // Out of budget with a step to go (a throw after an unsupported await or a
            // cancellation): let the other tasks run first. A loop that keeps catching the
            // `TypeError` of an unsupported await yields here too, so it cannot hang the lane.
            yield_now(task, cx);
            return None;
        }
        budget.set(budget.get() - 1);
        task.stats.steps = task.stats.steps.wrapping_add(1);
        let resume_with = task.take_resume(py);
        // The coroutine's Python code reaches the cancel stack through `RUNNING` (scopes it
        // enters and exits), not through the task, which this loop holds.
        running.stack.replace(task.take_cancel_stack());
        let stepped = resume(coro, resume_with);
        task.put_cancel_stack(running.stack.take());
        let yielded = match stepped {
            Stepped::Finished(outcome) => return Some(outcome),
            Stepped::Yielded(yielded) => yielded,
        };
        match dispatch(task, &yielded) {
            // The hot path of a switch: no payload to pack.
            Next::Wait(Pending::Yield) => {
                if let Some(cancelled) = cancellation(py, task, running) {
                    task.set_resume(Resume::Throw(cancelled));
                    continue;
                }
                yield_now(task, cx);
                return None;
            }
            Next::Wait(pending) => {
                let kind = pending.kind();
                if kind.is_cancellable()
                    && let Some(cancelled) = cancellation(py, task, running)
                {
                    // Not waited on: nothing is linked yet, the payload is just dropped.
                    drop(pending);
                    task.set_resume(Resume::Throw(cancelled));
                    continue;
                }
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
                            return None;
                        }
                        // Still waiting, unless the task is cancelled (`None`: it waits).
                        let cancelled = cancelled_wait(py, task, running, cx.waker())?;
                        task.abandon_wait();
                        task.set_resume(Resume::Throw(cancelled));
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

/// The `Cancelled` to throw into `task` if it is effectively cancelled.
#[inline]
fn cancellation(py: Python<'_>, task: &PyTask, running: &Running) -> Option<Py<PyBaseException>> {
    let scope = task.innermost_scope();
    if !running.is_cancelled(scope) {
        return None;
    }
    scope.map(|scope| cancelled_exception(py, scope))
}

/// For a wait that is not over: the `Cancelled` to throw in if the wait is cancellable and the
/// task is effectively cancelled, else `None`, with the wait filed with the task's scope so a
/// cancel wakes it (`PyTask::watch_cancel`, whose own check closes the race with a cancel from
/// another thread).
fn cancelled_wait(
    py: Python<'_>,
    task: &mut PyTask,
    running: &Running,
    waker: &Waker,
) -> Option<Py<PyBaseException>> {
    if !task.pending_kind.is_cancellable() {
        return None;
    }
    if let Some(cancelled) = cancellation(py, task, running) {
        return Some(cancelled);
    }
    if !task.watch_cancel(waker) {
        return None;
    }
    task.innermost_scope()
        .map(|scope| cancelled_exception(py, scope))
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
