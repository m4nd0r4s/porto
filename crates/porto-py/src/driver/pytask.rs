//! The PyTask body (11-task-model-and-trap-protocol.md §2, §6): a coroutine, its context, what
//! to resume it with, what it waits for, and, once it finished, its outcome for its joiners.
//!
//! A PyTask is a porto-core task with this [`TaskBody`]: header, scheduler handle, then the
//! body, in one allocation with no output stage and no join-handle slot. Its `poll` runs the step
//! loop (`step.rs`). It is created only through [`spawn`], onto a runtime built by `lane.rs`, so
//! it is only ever polled on a Python lane, with the thread state attached.
//!
//! # Layout (25-cpython-integration-and-memory.md §5)
//!
//! The body is 64 bytes plus the two nodes it embeds for its one wait at a time: a
//! [`TaskTimer`] (56 B) and a [`WaitNode`] (48 B), 168 bytes in all (192 allowed). To fit, the
//! small enums are stored as a one-byte kind next to a one-word payload, and read back through
//! [`PyTask::take_resume`], [`PyTask::poll_pending`] (`wait.rs`) and [`PyTask::outcome`]:
//!
//! - `slot` holds the [`Resume`] payload while the task runs and the [`TaskOutcome`] payload
//!   once it finished (the two never coexist);
//! - `pending_payload` holds the [`Pending`](super::pending::Pending) payload (`pending.rs`):
//!   one word, for every kind;
//! - the cancel stack is its innermost scope plus a depth, see [`PyTask::cancel_scopes`].

use std::any::Any;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use porto_core::Handle;
use porto_core::scope::{ScopeNode, ScopeRef};
use porto_core::sync::{JoinWaiters, WaitNode};
use porto_core::task::{Header, TaskBody, TaskBodyRef, TaskFlags};
use porto_core::time::TaskTimer;
use pyo3::exceptions::{PyBaseException, PyException, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;

use super::pending::{Kind, Payload};
use super::{lane, step};
use crate::api::scope::Task;
use crate::errors::{Cancelled, RuntimeClosed};
use crate::py_compat::take_exception;

/// The cooperative budget: steps and synchronous completions a task runs per poll before it
/// yields to the lane (12-scheduler-and-lanes.md §1, `coop_budget`). It is counted in the
/// poll's frame, not in the body (`step.rs`), so `checkpoint_if_needed` reaches it.
pub(super) const COOP_BUDGET: u8 = 128;

/// The scope type of the cancel stack: scopes collect Python exceptions.
pub(crate) type PyScopeRef = ScopeRef<Py<PyBaseException>>;

/// The body of a Python task.
pub(crate) struct PyTask {
    /// The coroutine; `None` once it finished.
    pub(super) coro: Option<Py<PyAny>>,
    /// The `contextvars.Context` every step runs in.
    pub(super) context: Py<PyAny>,
    /// The payload of [`Resume`] (while running) or [`TaskOutcome`] (once finished), by
    /// `slot_kind`. Written by the lane only; joiners read it once `join` is closed.
    slot: Option<Py<PyAny>>,
    /// The payload of [`Pending`](super::pending::Pending), by `pending_kind`. Owned: released
    /// by `wait.rs`.
    pub(super) pending_payload: Payload,
    /// The innermost scope the task is in: the last one it entered, else the scope it was
    /// spawned into, its base scope (spec 15 §1.2). See [`PyTask::cancel_scopes`].
    cancel_scope: Option<PyScopeRef>,
    /// Who waits for the task to finish. Closed once the outcome is in `slot`.
    join: JoinWaiters,
    /// The `porto.Task` wrapper, created lazily: only `Scope.spawn` makes one so far.
    py_handle: Option<Py<Task>>,
    pub(super) stats: TaskStats,
    slot_kind: SlotKind,
    pub(super) pending_kind: Kind,
    /// How many cancel scopes this task entered and has not exited.
    cancel_depth: u8,
    /// The entry of a `Sleep` wait.
    timer: TaskTimer,
    /// The node of a `Notified` or `Join` wait.
    pub(super) wait_node: WaitNode,
    /// Nodes in it are pointed at: the body never moves (it is pinned in the task).
    _pinned: PhantomPinned,
}

/// Per-task counters for introspection (spec 11 §2, `TaskStats`). Timing (first poll, total
/// poll time, step start) needs a clock read per step, which the hot path cannot afford: it
/// arrives with the task registry, behind its switch (porto-szb.24).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TaskStats {
    /// Coroutine resumes (`send` or `throw`) so far.
    pub(crate) steps: u32,
}

/// The body's size without the nodes it embeds.
const BODY_WITHOUT_NODES: usize =
    size_of::<PyTask>() - size_of::<TaskTimer>() - size_of::<WaitNode>();

// ADR-034, 25 §5: the body is at most 192 bytes including its embedded timer entry and wait
// node; the rest is held to 64 bytes.
const _: () = assert!(
    BODY_WITHOUT_NODES <= 64,
    "PyTask body over 64 bytes without its nodes"
);
const _: () = assert!(
    size_of::<PyTask>() <= 192,
    "ADR-034: PyTask body over 192 bytes"
);

/// What the next step sends into the coroutine (spec 11 §2).
pub(crate) enum Resume {
    /// Send `None`: starts the coroutine, and resumes it after a yield or a sleep.
    None,
    /// Send this value: the result of the trap the coroutine awaited.
    Value(Py<PyAny>),
    /// Throw this exception in.
    Throw(Py<PyBaseException>),
}

/// How a task ended (spec 11 §2).
pub(crate) enum TaskOutcome {
    /// The coroutine returned this value.
    Ok(Py<PyAny>),
    /// The coroutine raised this exception.
    Err(Py<PyBaseException>),
    /// The task was cancelled before it finished (its runtime shut down).
    Cancelled,
}

impl TaskOutcome {
    /// A new reference to the same outcome.
    pub(crate) fn clone_ref(&self, py: Python<'_>) -> Self {
        match self {
            Self::Ok(value) => Self::Ok(value.clone_ref(py)),
            Self::Err(exception) => Self::Err(exception.clone_ref(py)),
            Self::Cancelled => Self::Cancelled,
        }
    }

    /// The value, or the exception as an error (it keeps its traceback).
    pub(crate) fn into_result(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Err(exception) => Err(PyErr::from_value(exception.into_bound(py).into_any())),
            Self::Cancelled => Err(cancelled_error()),
        }
    }
}

/// What reading the outcome of a cancelled task raises.
pub(crate) fn cancelled_error() -> PyErr {
    RuntimeClosed::new_err(
        "porto: the task was cancelled before it finished: its runtime shut down; keep \
         porto.run() running until the tasks you wait for are done",
    )
}

/// The variant stored in `slot`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum SlotKind {
    ResumeNone,
    ResumeValue,
    ResumeThrow,
    Ok,
    Err,
    Cancelled,
}

/// Who learns how a task ended.
pub(crate) enum Spawned {
    /// Nobody waits for it (`_spawn`). An `Exception` goes to `sys.unraisablehook`; any other
    /// `BaseException` (`KeyboardInterrupt`, `SystemExit`) stops the run (see `lane::fail`).
    Detached,
    /// Rust code joins it through the returned reference ([`join`]): `porto.run`'s main task.
    Joined,
    /// A scope's child: the scope counts it until it ends and records its failure (C4,
    /// [`TaskBody::take_failure`]); the outcome also goes to its [`Task`] object.
    Child {
        /// The `porto.Task` the scope's `spawn` returned.
        task: Py<Task>,
        /// The scope it is spawned into, its base scope.
        scope: PyScopeRef,
    },
}

/// Spawns `coro` as a task on the runtime behind `handle`, which MUST be a Python lane's (see
/// `lane.rs`). The task runs in a copy of the current context, like `asyncio.create_task`.
pub(crate) fn spawn(
    handle: &Handle,
    coro: Bound<'_, PyAny>,
    spawned: Spawned,
) -> PyResult<TaskBodyRef<PyTask>> {
    let py = coro.py();
    // SAFETY: attached (we hold `py`). Returns a new reference, or NULL with an exception set.
    let context = unsafe { ffi::PyContext_CopyCurrent() };
    if context.is_null() {
        return Err(PyErr::from_value(take_exception(py).into_any()));
    }
    // SAFETY: a new, non-null reference, which the `Bound` takes over.
    let context = unsafe { Bound::from_owned_ptr(py, context) }.unbind();
    let (flags, py_handle, scope) = match spawned {
        Spawned::Detached => (TaskFlags::PYTHON | TaskFlags::DETACHED_DROP, None, None),
        Spawned::Joined => (TaskFlags::PYTHON, None, None),
        Spawned::Child { task, scope } => (TaskFlags::PYTHON, Some(task), Some(scope)),
    };
    let mut body = PyTask {
        coro: Some(coro.unbind()),
        context,
        slot: None,
        pending_payload: Payload::EMPTY,
        cancel_scope: None,
        join: JoinWaiters::new(),
        py_handle,
        stats: TaskStats::default(),
        slot_kind: SlotKind::ResumeNone,
        pending_kind: Kind::None,
        cancel_depth: 0,
        timer: TaskTimer::new(),
        wait_node: WaitNode::new(0),
        _pinned: PhantomPinned,
    };
    let Some(scope) = scope else {
        return Ok(handle.spawn_body(body, flags));
    };
    // The base scope: the walk of its effective cancellation starts there (spec 15 §1.2).
    body.cancel_scope = Some(scope.clone());
    Ok(handle.spawn_body_in(&scope, body, flags))
}

impl PyTask {
    /// The resume value for the next step; leaves `Resume::None` in its place.
    pub(super) fn take_resume(&mut self, py: Python<'_>) -> Resume {
        debug_assert!(self.slot_kind.is_resume(), "resumed a finished task");
        let kind = std::mem::replace(&mut self.slot_kind, SlotKind::ResumeNone);
        match (kind, self.slot.take()) {
            (SlotKind::ResumeValue, Some(value)) => Resume::Value(value),
            (SlotKind::ResumeThrow, Some(exception)) => {
                // SAFETY: only `set_resume` stores `ResumeThrow`, with an exception.
                Resume::Throw(unsafe { exception.into_bound(py).cast_into_unchecked() }.unbind())
            }
            _ => Resume::None,
        }
    }

    pub(super) fn set_resume(&mut self, resume: Resume) {
        let (kind, payload) = match resume {
            Resume::None => (SlotKind::ResumeNone, None),
            Resume::Value(value) => (SlotKind::ResumeValue, Some(value)),
            Resume::Throw(exception) => (SlotKind::ResumeThrow, Some(exception.into_any())),
        };
        self.slot_kind = kind;
        self.slot = payload;
    }

    /// The embedded timer, for a `Sleep` wait.
    pub(super) fn timer(&mut self) -> Pin<&mut TaskTimer> {
        // SAFETY: a `&mut PyTask` only ever comes from the pinned body (`TaskBody::poll`), and
        // the timer is never moved out of it.
        unsafe { Pin::new_unchecked(&mut self.timer) }
    }

    /// The embedded wait node, for a `Notified` or `Join` wait. Only ever reached through `&`
    /// (`WaitQueue::poll_wait`'s contract: granters read it concurrently).
    pub(super) fn wait_node(&self) -> Pin<&WaitNode> {
        // SAFETY: as for the timer: the node never moves out of the pinned body.
        unsafe { Pin::new_unchecked(&self.wait_node) }
    }

    /// The scopes this task entered, innermost first (spec 11 §2, `cancel_stack`).
    ///
    /// Scopes a task enters nest (each one's parent is the scope the task was in when it
    /// entered it), so the stack is the innermost one and its first `cancel_depth - 1`
    /// ancestors: one pointer instead of a `SmallVec<[ScopeRef; 2]>` (24 bytes, and an
    /// allocation past two).
    #[allow(dead_code, reason = "read by cancel delivery (porto-szb.36)")]
    pub(super) fn cancel_scopes(&self) -> impl Iterator<Item = &ScopeNode<Py<PyBaseException>>> {
        let mut next = self.cancel_scope.as_deref();
        (0..self.cancel_depth).map_while(move |_| {
            let scope = next?;
            next = scope.parent().map(|parent| &**parent);
            Some(scope)
        })
    }

    /// Records that the task entered `scope`, whose parent MUST be the scope it was in.
    #[allow(dead_code, reason = "called by cancel scopes (porto-szb.36)")]
    pub(super) fn enter_cancel_scope(&mut self, scope: PyScopeRef) -> PyResult<()> {
        if self.cancel_depth == u8::MAX {
            return Err(PyRuntimeError::new_err(
                "porto: more than 255 cancel scopes nested in one task; flatten the nesting \
                 or move part of the work into a child task",
            ));
        }
        self.cancel_scope = Some(scope);
        self.cancel_depth += 1;
        Ok(())
    }

    /// Records that the task exited `scope`: its parent is the innermost one again (the base
    /// scope once it exited every scope it entered). Returns `false`, changing nothing, if
    /// `scope` is not its innermost one (scopes exit in the reverse order they were entered).
    #[allow(dead_code, reason = "called by cancel scopes (porto-szb.36)")]
    pub(super) fn exit_cancel_scope(&mut self, scope: &ScopeNode<Py<PyBaseException>>) -> bool {
        let Some(innermost) = self.cancel_scope.take() else {
            return false;
        };
        if !std::ptr::eq(std::ptr::from_ref(&*innermost), scope) {
            self.cancel_scope = Some(innermost);
            return false;
        }
        self.cancel_depth -= 1;
        self.cancel_scope = innermost.parent().cloned();
        true
    }

    /// The outcome, once the task finished; `None` before.
    ///
    /// # Safety
    /// `task` points at a live body whose `join` list is closed (so the outcome is published
    /// and the lane no longer writes it).
    pub(crate) unsafe fn outcome(task: NonNull<Self>, py: Python<'_>) -> Option<TaskOutcome> {
        let task = task.as_ptr();
        // SAFETY: the caller's contract; the fields are reached one by one, not through a
        // `&PyTask`.
        let (kind, slot) = unsafe { ((*task).slot_kind, &(*task).slot) };
        let object = || slot.as_ref().map(|object| object.clone_ref(py));
        match kind {
            SlotKind::Ok => object().map(TaskOutcome::Ok),
            SlotKind::Err => object().map(|error| {
                // SAFETY: only `complete` stores `Err`, with an exception.
                TaskOutcome::Err(unsafe { error.into_bound(py).cast_into_unchecked() }.unbind())
            }),
            SlotKind::Cancelled => Some(TaskOutcome::Cancelled),
            _ => None,
        }
    }

    /// The task's joiners.
    ///
    /// # Safety
    /// `task` points at a live body, which outlives the returned borrow.
    pub(crate) unsafe fn join_waiters<'a>(task: NonNull<Self>) -> &'a JoinWaiters {
        // SAFETY: the caller's contract; the list is shared with joiners by design.
        unsafe { &(*task.as_ptr()).join }
    }

    /// Stores the outcome, hands it to whoever learns of it, and wakes the joiners. The last
    /// thing the lane does to the body: joiners read the outcome once `join` is closed.
    pub(super) fn complete(&mut self, py: Python<'_>, header: &Header, outcome: TaskOutcome) {
        let coro = self.coro.take();
        if let Some(task) = self.py_handle.take() {
            task.get().finish(outcome.clone_ref(py));
        } else if header.flags().contains(TaskFlags::DETACHED_DROP)
            && let TaskOutcome::Err(exception) = &outcome
        {
            let exception = exception.clone_ref(py);
            if exception.bind(py).is_instance_of::<PyException>() {
                // Nobody to raise to: report it like an exception in `__del__`.
                PyErr::from_value(exception.into_bound(py).into_any())
                    .write_unraisable(py, coro.as_ref().map(|coro| coro.bind(py)));
            } else {
                // `KeyboardInterrupt`, `SystemExit`: the program asked to stop.
                lane::fail(exception);
            }
        }
        drop(coro);
        // Nothing is pending once the coroutine finished; give the wait up if it was left
        // half-way (a failed context enter). On the lane (only `step::poll` completes a task),
        // so a sleep's timer is unlinked too instead of keeping the task until its deadline.
        self.abandon_wait();
        // An interrupt filed for this task and never delivered goes with it.
        lane::forget_interrupt(header);
        let (kind, payload) = match outcome {
            TaskOutcome::Ok(value) => (SlotKind::Ok, Some(value)),
            TaskOutcome::Err(exception) => (SlotKind::Err, Some(exception.into_any())),
            TaskOutcome::Cancelled => (SlotKind::Cancelled, None),
        };
        self.slot_kind = kind;
        self.slot = payload;
        self.join.close();
    }
}

impl SlotKind {
    fn is_resume(self) -> bool {
        matches!(
            self,
            Self::ResumeNone | Self::ResumeValue | Self::ResumeThrow
        )
    }
}

impl TaskBody for PyTask {
    fn poll(self: Pin<&mut Self>, header: &Header, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: a PyTask is only spawned (by `spawn`) onto a Python lane's runtime, whose lane
        // thread keeps its thread state attached while it runs tasks and detaches only around
        // parks, when no task runs (`lane.rs`).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: the step loop never moves the body or its nodes (see `PyTask::timer`).
        step::poll(unsafe { self.get_unchecked_mut() }, header, py, cx)
    }

    /// The exception the coroutine raised, for the scope it was spawned into (C4): `None` if it
    /// returned, or if it raised `Cancelled` while its scope is cancelled (the scope, or one of
    /// its ancestors, is what cancelled it). The outcome stays for the joiners.
    ///
    /// A Rust panic in the step loop is not asked for here: the task fails without a payload
    /// for the scope, which is cancelled with `ChildFailed`, and the panic goes on out of
    /// `porto.run` (it is resumed on the lane), so no scope exit waits to report it.
    fn take_failure(self: Pin<&mut Self>) -> Option<Box<dyn Any + Send>> {
        if self.slot_kind != SlotKind::Err {
            return None;
        }
        // SAFETY: porto-core asks right after the poll that returned `Ready`, on the lane that
        // ran it, which is attached (see `poll`).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: only `complete` stores `Err`, with an exception.
        let exception = unsafe {
            self.slot
                .as_ref()?
                .bind(py)
                .cast_unchecked::<PyBaseException>()
        };
        let cancelled_by_its_scope = exception.is_instance_of::<Cancelled>()
            && self
                .cancel_scope
                .as_deref()
                .is_some_and(|scope| scope.effective_cancellation().is_some());
        if cancelled_by_its_scope {
            return None;
        }
        Some(Box::new(exception.clone().unbind()))
    }

    fn cancel(self: Pin<&mut Self>, _header: &Header) {
        // SAFETY: nothing is moved out; the timer and the wait node stay in place.
        let this = unsafe { self.get_unchecked_mut() };
        // Unregisters the wait (T3) with what works off the lane (see `release_wait`).
        // Dropping the coroutine closes it; off a Python thread (a wake after shutdown), pyo3
        // defers that to the next attached one.
        this.release_wait();
        drop(this.coro.take());
        drop(this.py_handle.take());
        this.slot_kind = SlotKind::Cancelled;
        this.slot = None;
        this.join.close();
    }
}

impl Drop for PyTask {
    fn drop(&mut self) {
        // A wait whose waker the task owns cannot be linked here (the link would keep the task
        // alive); this releases what the payload owns.
        self.release_wait();
    }
}

/// Waits for the task behind `task` to finish, and returns its outcome. MUST be polled on the
/// task's lane (by its `block_on`), attached.
pub(crate) fn join(task: TaskBodyRef<PyTask>) -> Join {
    Join {
        task,
        node: WaitNode::new(0),
    }
}

/// The future of [`join`]: a node on the task's [`JoinWaiters`], unlinked if the future is
/// dropped early (Ctrl-C while `porto.run` waits for main).
pub(crate) struct Join {
    task: TaskBodyRef<PyTask>,
    node: WaitNode,
}

impl Future for Join {
    type Output = TaskOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<TaskOutcome> {
        let this = self.into_ref().get_ref();
        let body = this.task.body();
        // SAFETY: the node is a field of this pinned future (`WaitNode` is `!Unpin`), used with
        // this list only; `task` keeps the list alive, and `Drop` cancels the wait.
        let joined =
            unsafe { PyTask::join_waiters(body).poll_wait(Pin::new_unchecked(&this.node), cx) };
        if joined.is_pending() {
            return Poll::Pending;
        }
        // SAFETY: polled by the lane's `block_on`, which is attached (only parks detach).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: the wait is over, so `join` is closed: the outcome is published.
        let outcome = unsafe { PyTask::outcome(body, py) };
        Poll::Ready(outcome.unwrap_or(TaskOutcome::Cancelled))
    }
}

impl Drop for Join {
    fn drop(&mut self) {
        // SAFETY: a `Join` is pinned once polled (it is `!Unpin`); its node waits on this task's
        // list or nowhere, and `task` keeps the list alive.
        unsafe {
            PyTask::join_waiters(self.task.body()).cancel(Pin::new_unchecked(&self.node));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_body_fits_its_budget() {
        println!(
            "PyTask body: {} B ({} B without the {} B timer and the {} B wait node)",
            size_of::<PyTask>(),
            BODY_WITHOUT_NODES,
            size_of::<TaskTimer>(),
            size_of::<WaitNode>(),
        );
        assert_eq!(
            size_of::<Header>(),
            56,
            "ST-11: the header is one cache line"
        );
    }
}
