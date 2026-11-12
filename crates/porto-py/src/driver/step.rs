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
//! While the task's effective scope is cancelled (the walk from its innermost scope, cached in
//! that scope, `ScopeNode::is_cancelled`), every checkpoint raises `porto.Cancelled`, again and again if
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
use std::ptr::{self, NonNull};
use std::task::{Context, Poll, Waker};

use porto_core::coop;
use porto_core::scope::{CancelReason, TaskEnd, TaskScope};
use porto_core::task::{Header, TaskFlags};
use pyo3::exceptions::{
    PyBaseException, PyRuntimeError, PyRuntimeWarning, PyStopIteration, PyTypeError,
};
use pyo3::prelude::*;
use pyo3::{ffi, intern};

use super::lane;
use super::pending::{Kind, Pending};
use super::pytask::{CancelStack, PyScopeNode, PyScopeRef, PyTask, Resume, TaskOutcome};
use super::ready::{ready, ready_none};
use super::trap::{Trap, TrapKind};
use crate::api::task::Task;
use crate::errors::Cancelled;
use crate::py_compat::{Sent, names, release_spare_value, send, take_exception};

thread_local! {
    /// The task this lane is polling, null between polls.
    static RUNNING: Cell<*const Running> = const { Cell::new(ptr::null()) };
}

/// What `RUNNING` publishes of the polled task: its waker, its budget and its cancel stack.
/// Lives in `poll`'s frame, which outlives the publication (see `RunningTask`).
struct Running {
    waker: *const Waker,
    /// The task's header, alive for the whole poll (the run reference).
    header: *const Header,
    /// The task's id, the owner of the scopes it enters.
    task_id: u64,
    /// The task's `porto.Task`, borrowed from the body's `py_handle` (which holds it for the
    /// whole poll): null if it has none. See [`current_task`].
    handle: Cell<*mut ffi::PyObject>,
    /// The task's `Task` struct behind `handle`, null with it: what `Task.cancel()` sets.
    task_state: Cell<*const Task>,
    /// The handle [`current_task`] made for a task that had none: the poll hands it to the
    /// body when it ends.
    made_handle: Cell<Option<Py<Task>>>,
    /// Steps and synchronous completions left in this poll (T2).
    budget: Cell<u32>,
    /// The task's cancel stack while its coroutine runs (moved out of the task around each
    /// step), empty otherwise. Reached through [`Running::with_stack`].
    stack: Cell<CancelStack>,
}

impl Drop for Running {
    fn drop(&mut self) {
        // A poll that unwound (a Rust panic in the step loop) before it gave the body the handle
        // `current_task` made: the handle points at a task that is about to be cancelled and
        // freed, and nothing else would end it (`PyTask::py_handle` was empty). Its outcome is
        // the task's, and it forgets the pointer.
        if let Some(handle) = self.made_handle.take() {
            handle.get().finish(TaskOutcome::Cancelled(None));
        }
    }
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
}

/// Whether a task whose innermost scope is `scope` is effectively cancelled: the scope's cached
/// walk (`ScopeNode::is_cancelled`), two loads and a compare while nothing was cancelled.
#[inline]
fn is_cancelled(scope: Option<&PyScopeNode>) -> bool {
    scope.is_some_and(PyScopeNode::is_cancelled)
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

/// The `porto.Task` of the task running on this thread: the one `Scope.spawn` returned, or, for
/// a task that has none (an internal spawn), a new one, kept for the task's next calls
/// (spec 11 §7). `None` if no task is running. The new handle names the task `Task-<id>`.
pub(crate) fn current_task(py: Python<'_>) -> PyResult<Option<Py<Task>>> {
    let Some(running) = running() else {
        return Ok(None);
    };
    let known = running.handle.get();
    if !known.is_null() {
        // SAFETY: the pointer is the body's `py_handle` (or the handle made below), a `Task`
        // that the body or `made_handle` holds for the whole poll.
        return Ok(Some(
            unsafe { Borrowed::from_ptr(py, known).cast_unchecked::<Task>() }
                .to_owned()
                .unbind(),
        ));
    }
    // SAFETY: `poll` publishes `Running` only while it polls the task behind `header`, alive
    // for that whole time (the run reference).
    let header = unsafe { &*running.header };
    let handle = Py::new(py, Task::new(None, header.flags().contains(TaskFlags::PIN)))?;
    // The task's pointer with the provenance of its allocation is the data of its waker (the
    // `&Header` `poll` got is only a borrow of the header).
    // SAFETY: `poll` borrows the waker for as long as it publishes `Running`; its data is the
    // header pointer of the task being polled, which is alive for the whole poll.
    let ptr = unsafe { &*running.waker }
        .data()
        .cast::<Header>()
        .cast_mut();
    // SAFETY: a task waker's data is never null, and it has the provenance of the allocation.
    unsafe { handle.get().bind_ptr(NonNull::new_unchecked(ptr)) };
    running.handle.set(handle.as_ptr());
    running.task_state.set(ptr::from_ref(handle.get()));
    running.made_handle.set(Some(handle.clone_ref(py)));
    Ok(Some(handle))
}

/// The lane the task running on this thread runs on, if a task is running.
pub(crate) fn current_lane() -> Option<u16> {
    running().map(|running| {
        // SAFETY: as in `current_task`: the header is alive for the whole poll.
        unsafe { &*running.header }.owner_lane()
    })
}

/// Moves the task running on this thread from `hidden`, the scope it was started in, into
/// `target`, the scope `Scope.start` was called on (spec 15 §3): the task counts in `target`
/// from now on (its live count, its failures, its exit wait), and `hidden`, which the scopes the
/// task entered are nested in, is adopted by `target`, so `target`'s cancel, deadline and shield
/// reach the task and the caller's no longer do. The caller waiting for `hidden` to empty is woken.
///
/// Raises `RuntimeError`, changing nothing, if no task runs here, the task was not started in
/// `hidden`, or `target` is closed.
pub(crate) fn move_running_task(hidden: &PyScopeRef, target: &PyScopeRef) -> PyResult<()> {
    let Some(running) = running() else {
        return Err(PyRuntimeError::new_err(
            "porto: Started.set() must be called from the task Scope.start() started; call it \
             inside that task's coroutine",
        ));
    };
    // SAFETY: `poll` publishes `Running` only while it polls the task behind `header`, alive for
    // that whole time (the run reference).
    let header = unsafe { &*running.header };
    // SAFETY: the Python code that called us runs inside this task's step, so this lane holds
    // RUNNING: nothing else touches the header's scope slot.
    if !unsafe { header.counts_in(hidden) } {
        return Err(PyRuntimeError::new_err(
            "porto: Started.set() must be called by the task Scope.start() started, which is the \
             one that got this `started`; do not pass it to other tasks (start them with \
             their own scope.start(...)), and call set() before the task is moved",
        ));
    }
    let counted = TaskScope::try_attach(target).ok_or_else(|| {
        PyRuntimeError::new_err(
            "porto: Started.set() found the scope Scope.start() was called on closed: its \
             `async with` block exited, so it cannot take the task over; keep the scope open \
             until start() returns",
        )
    })?;
    // Once only: `Started.set` is called once. A hidden scope with no parent (the caller was in
    // no scope) gets its first one, so `target`'s cancel, deadline and shield reach the task.
    let adopted = hidden.adopt(target);
    debug_assert!(adopted, "a hidden scope is adopted once");
    // SAFETY: as above.
    let hidden_count = unsafe { header.replace_scope(counted) };
    if let Some(hidden_count) = hidden_count {
        // Counts the task out of the hidden scope and wakes the caller waiting for it to empty.
        hidden_count.end(TaskEnd::Completed);
    }
    Ok(())
}

/// Whether the task with this header is the one running on this thread (its waker's data is
/// its header, see `lane::task_key`).
pub(crate) fn is_running(header: &Header) -> bool {
    running().is_some_and(|running| {
        // SAFETY: `poll` borrows the waker for as long as it publishes it.
        unsafe { &*running.waker }.data() == std::ptr::from_ref(header).cast()
    })
}

/// The `porto.Task` of the running task if `header` is its header: `Some(null)` if it has none
/// (this never makes one), `None` if another task runs or none does. Borrowed.
pub(crate) fn running_handle_of(header: &Header) -> Option<*mut ffi::PyObject> {
    let running = running()?;
    ptr::eq(running.header, header).then(|| running.handle.get())
}

/// Whether the task running on this thread is pinned (its header's `PIN` flag): what a task it
/// spawns inherits when the spawn does not say (spec 03: pinning is inherited by the subtree).
/// `false` outside a task.
pub(crate) fn running_is_pinned() -> bool {
    running().is_some_and(|running| {
        // SAFETY: `poll` borrows the waker for as long as it publishes it, and the data of a
        // porto-core task waker is the task's header (see `is_running`), alive while it runs.
        let header = unsafe { &*(*running.waker).data().cast::<Header>() };
        header.flags().contains(TaskFlags::PIN)
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
    if !proceed(py)? {
        return Ok(Py::new(py, Trap::new(TrapKind::YieldThen(value)))?.into_any());
    }
    // A `None` result is the shared `_Ready(None)`: no allocation.
    ready(py, value)
}

/// [`checkpoint_if_needed`] for a `None` result, which holds no `Py` to drop on the fast path
/// (dropping one reads pyo3's thread-local GIL count).
pub(crate) fn checkpoint_if_needed_none(py: Python<'_>) -> PyResult<Py<PyAny>> {
    if !proceed(py)? {
        return Ok(Py::new(py, Trap::new(TrapKind::YieldThen(py.None())))?.into_any());
    }
    ready_none(py)
}

/// The T2 decision of [`checkpoint_if_needed`] with no result object: raises `Cancelled` while
/// the running task is cancelled (before spending anything), else spends one unit of its budget
/// and returns `true`, or returns `false` if the budget was already spent (the caller then
/// suspends instead of completing). Always `true` outside a porto task.
#[inline]
pub(crate) fn proceed(py: Python<'_>) -> PyResult<bool> {
    Ok(proceed_in_task(py)?.unwrap_or(true))
}

/// [`proceed`], or `None` (spending nothing) if no porto task is running on this thread: for
/// an awaitable that checks where it runs only then, with no thread local of its own.
#[inline]
pub(crate) fn proceed_in_task(py: Python<'_>) -> PyResult<Option<bool>> {
    let Some(running) = running() else {
        return Ok(None);
    };
    if let Some(cancelled) = running_cancellation(py, running) {
        return Err(PyErr::from_value(cancelled.into_bound(py).into_any()));
    }
    let budget = &running.budget;
    match budget.get() {
        0 => Ok(Some(false)),
        left => {
            budget.set(left - 1);
            Ok(Some(true))
        }
    }
}

/// Raises `Cancelled` while the running task's effective scope is cancelled (C1), spending no
/// budget: for a call that starts something it cannot take back (a connect) before it returns
/// its awaitable. Nothing outside a porto task.
pub(crate) fn raise_if_cancelled(py: Python<'_>) -> PyResult<()> {
    match running().and_then(|running| running_cancellation(py, running)) {
        Some(cancelled) => Err(PyErr::from_value(cancelled.into_bound(py).into_any())),
        None => Ok(()),
    }
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

/// Whether `Task.cancel()` was called on the task `running` polls. While the task has no
/// implicit scope (it entered no scope), this flag is its cancellation (spec 15 §4).
#[inline]
fn task_cancel_requested(running: &Running) -> bool {
    let task = running.task_state.get();
    // SAFETY: non-null only while the body (or `made_handle`) holds the `Task` it points into,
    // which is the whole poll (see `Running::task_state`).
    !task.is_null() && unsafe { &*task }.cancel_requested()
}

/// The `Cancelled` to raise in the running task, if it is effectively cancelled: its innermost
/// scope is, or `Task.cancel()` was called and no shield the task entered stands in between.
fn running_cancellation(py: Python<'_>, running: &Running) -> Option<Py<PyBaseException>> {
    let found = running.with_stack(|stack| {
        if let Some(scope) = stack.innermost()
            && scope.is_cancelled()
        {
            return Some(Some(scope.clone()));
        }
        (task_cancel_requested(running) && !stack.has_shield()).then_some(None)
    })?;
    Some(match found {
        Some(scope) => cancelled_exception(py, &scope),
        None => task_cancelled_exception(py),
    })
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
        // The first scope a task with a handle enters: its implicit scope goes below it, so
        // `Task.cancel()` reaches everything the task runs in scopes (spec 15 §4).
        let task = running.task_state.get();
        if stack.is_at_base()
            && !task.is_null()
            // SAFETY: as in `task_cancel_requested`.
            && let Some(implicit) = unsafe { &*task }.install_implicit(stack.innermost(), running.task_id)
        {
            stack.install_base(implicit);
        }
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
    let found = scope.effective_cancellation();
    let why = match found.as_ref().map(|found| found.reason) {
        Some(CancelReason::Deadline) => "the deadline of its scope, or of one it is in, passed",
        Some(CancelReason::ChildFailed) => {
            "a task of its scope, or of one it is nested in, failed, which cancels the scope (C4)"
        }
        Some(CancelReason::Shutdown) => "the runtime is shutting down",
        Some(CancelReason::TaskCancelled) => TASK_CANCELLED,
        _ => "Scope.cancel() was called on its scope or on one it is nested in",
    };
    let cause = found.map(|found| found.scope.id());
    cancelled_with(py, why, cause)
}

/// The cause recorded for a `Cancelled` that `Task.cancel()` delivered with no scope of its own
/// to carry it: never equal to a scope's id, so no scope exit absorbs it.
const TASK_CAUSE: u64 = 1 << 63;

/// Debug mode (15 C11): remembers that the running task is being delivered the `Cancelled` of
/// the scope with id `cause` (`None`: of `Task.cancel()` alone), so that a task that goes on to
/// return normally can be reported as having swallowed it ([`warn_if_swallowed`]). A thread-local
/// read on this cold path and nothing when no `Cancelled` is raised. A task with no handle (an
/// internal spawn) is not tracked: giving it one would change what it later does (an implicit
/// scope is made for a task whose handle was cancelled), which a run without debug must not see.
fn note_cancel_delivered(cause: Option<u64>) {
    if !lane::debug() {
        return;
    }
    let Some(running) = running() else {
        return;
    };
    let task = running.task_state.get();
    // SAFETY: non-null points at the `Task` behind the running task's handle, which the body
    // (or `made_handle`) holds for the whole poll.
    if let Some(task) = unsafe { task.as_ref() } {
        task.note_cancel_delivered(cause.unwrap_or(TASK_CAUSE | running.task_id));
    }
}

/// Debug mode (15 C11): the exit of the scope with id `scope` absorbed a `Cancelled` in the
/// running task, which is how that scope's delivered cancellation is meant to end. Clears the
/// note only when that scope is the one whose cancellation was delivered.
pub(crate) fn note_cancel_absorbed(scope: u64) {
    if !lane::debug() {
        return;
    }
    let Some(running) = running() else {
        return;
    };
    let task = running.task_state.get();
    // SAFETY: as in `note_cancel_delivered`.
    if let Some(task) = unsafe { task.as_ref() } {
        task.note_cancel_absorbed(scope);
    }
}

/// Debug mode (15 C11): the task returned normally, and its handle says a `Cancelled` was
/// delivered to it and its scope's exit absorbed none since: it caught the cancellation and
/// carried on, which asks for it to be raised again. Warns once, as the task ends. One relaxed
/// load of a process-wide count gates it, so a run without debug pays nothing else.
fn warn_if_swallowed(py: Python<'_>, task: &PyTask) {
    if !lane::any_debug() {
        return;
    }
    let Some(handle) = task.py_handle.as_ref() else {
        return;
    };
    if !handle.get().swallow_pending() {
        return;
    }
    let name = handle
        .bind(py)
        .repr()
        .map_or_else(|_| "a task".to_owned(), |name| name.to_string());
    let message = std::ffi::CString::new(format!(
        "porto: cancellation was swallowed: {name} caught porto.Cancelled and finished normally \
         while its scope was cancelled (15 C11); let the Cancelled propagate, or raise it again \
         after the cleanup: the cancelled scope absorbs it at its `async with` exit"
    ))
    .expect("the warning has no NUL byte");
    if let Err(error) = PyErr::warn(py, &py.get_type::<PyRuntimeWarning>(), &message, 1) {
        // Warnings turned into errors: nobody to raise it to from here.
        error.write_unraisable(py, None);
    }
}

const TASK_CANCELLED: &str = "Task.cancel() was called on it";

/// The `Cancelled` delivered to a task that `Task.cancel()` reached while it has no implicit
/// scope to carry it.
fn task_cancelled_exception(py: Python<'_>) -> Py<PyBaseException> {
    cancelled_with(py, TASK_CANCELLED, None)
}

fn cancelled_with(py: Python<'_>, why: &str, cause: Option<u64>) -> Py<PyBaseException> {
    note_cancel_delivered(cause);
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
    let coro = coro.into_inner(py).into_bound(py);
    // SAFETY: `task` owns the context and keeps it until it is freed (`complete` releases only
    // the coroutine), so it outlives the guard.
    let _context = match unsafe { EnteredContext::enter(py, task.context_ptr()) } {
        Ok(entered) => entered,
        Err(exception) => {
            // The coroutine never runs again: close it, or a coroutine that never started
            // warns that it was never awaited when it is freed.
            if let Err(error) = coro.call_method0(intern!(py, "close")) {
                error.write_unraisable(py, Some(&coro));
            }
            task.coro = Some(coro.unbind().into());
            task.complete(py, header, TaskOutcome::Err(exception));
            return Poll::Ready(());
        }
    };
    let running = Running {
        waker: cx.waker(),
        header: ptr::from_ref(header),
        task_id: header.id(),
        handle: Cell::new(
            task.py_handle
                .as_ref()
                .map_or(ptr::null_mut(), |handle| handle.as_ptr()),
        ),
        task_state: Cell::new(
            task.py_handle
                .as_ref()
                .map_or(ptr::null(), |handle| ptr::from_ref(handle.get())),
        ),
        made_handle: Cell::new(None),
        // The lane's budget for this poll (`coop_budget`), kept here so a step does not touch a
        // thread local. Rust futures the task awaits draw on the lane's own count.
        budget: Cell::new(coop::remaining().unwrap_or(u32::MAX)),
        stack: Cell::new(CancelStack::default()),
    };
    let outcome = {
        // Published only while the coroutine steps: Python code that `complete` runs (a
        // `Task.finish`, an unraisable hook) must not see the finished task as running.
        let _running = RunningTask::set(&running);
        step_until_wait(task, &coro, &running, cx)
    };
    task.coro = Some(coro.unbind().into());
    if let Some(handle) = running.made_handle.take() {
        task.py_handle = Some(handle);
    }
    // The task gives the lane up: the last result an await returned through the spare
    // `StopIteration` must not live on in it.
    release_spare_value(py);
    match outcome {
        Some(outcome) => {
            let outcome = cancelled_by_its_scope(py, task, &running, outcome);
            if matches!(outcome, TaskOutcome::Ok(_)) {
                warn_if_swallowed(py, task);
            }
            task.complete(py, header, outcome);
            Poll::Ready(())
        }
        None => Poll::Pending,
    }
}

/// `outcome`, or `Cancelled` if it is a `porto.Cancelled` raised while the task's scope is
/// cancelling: the cancellation ended the task, which did not fail (spec 11 §4). The scope
/// whose cancel that was learns it absorbed one (C3, `cancelled_caught`): an enclosing scope's
/// cancel counts for the enclosing scope, never for the nested one the task ran in, and the
/// task's own scope (`Task.cancel()`, spec 15 §4) is such a nested one: while the scope the task
/// was spawned into, or one around it, is cancelled too (a deadline, `Scope.cancel()`), that
/// scope absorbs the `Cancelled`, so its `cancelled_caught` and its `TimeoutError` are not lost
/// to a `Task.cancel()` that came in the meantime. Only a `Task.cancel()` alone leaves every
/// scope's flags alone.
fn cancelled_by_its_scope(
    py: Python<'_>,
    task: &PyTask,
    running: &Running,
    outcome: TaskOutcome,
) -> TaskOutcome {
    match outcome {
        TaskOutcome::Err(exception) if exception.bind(py).is_instance_of::<Cancelled>() => {
            let own = task_cancel_requested(running) && !task.has_shield();
            let found = task
                .innermost_scope()
                .and_then(|scope| scope.effective_cancellation());
            let task_scope_cancelled = found.is_some();
            // The implicit scope, once made, is where the walk starts: look past it.
            let cause = match found {
                Some(found) if found.reason == CancelReason::TaskCancelled => found
                    .scope
                    .parent()
                    .and_then(|parent| parent.effective_cancellation()),
                other => other,
            };
            match cause {
                Some(cause) => {
                    cause.scope.lock_state().cancel_absorbed = true;
                    TaskOutcome::Cancelled(Some(exception))
                }
                // `Task.cancel()` alone (with no implicit scope, the flag; with one, its cancel).
                None if own || task_scope_cancelled => TaskOutcome::Cancelled(Some(exception)),
                None => TaskOutcome::Err(exception),
            }
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
    // Introspection records each step's timing; off, the loop reads no clock (spec 23 §1).
    let timed = lane::any_introspection();
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
        task.steps = task.steps.wrapping_add(1);
        let resume_with = task.take_resume(py);
        // The coroutine's Python code reaches the cancel stack through `RUNNING` (scopes it
        // enters and exits), not through the task, which this loop holds.
        running.stack.replace(task.take_cancel_stack());
        let started = timed.then(|| task.step_started());
        let stepped = resume(coro, resume_with);
        if let Some(started) = started {
            task.step_ended(started);
        }
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
    if is_cancelled(scope) {
        return scope.map(|scope| cancelled_exception(py, scope));
    }
    (task_cancel_requested(running) && !task.has_shield()).then(|| task_cancelled_exception(py))
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
