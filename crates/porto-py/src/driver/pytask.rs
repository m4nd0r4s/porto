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
//! - the cancel stack is its innermost scope plus a depth, see [`CancelStack`].

use std::any::Any;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use porto_core::Handle;
use porto_core::scope::{ScopeClosed, ScopeNode, ScopeRef};
use porto_core::sync::{JoinWaiters, WaitNode};
use porto_core::task::{Header, TaskBody, TaskBodyRef, TaskFlags};
use porto_core::time::TaskTimer;
use pyo3::exceptions::{PyBaseException, PyException, PyRuntimeError};
use pyo3::ffi;
use pyo3::prelude::*;

use super::pending::{Kind, Payload};
use super::{lane, step};
use crate::api::run::close_coroutine;
use crate::api::task::Task;
use crate::errors::{Cancelled, RuntimeClosed};
use crate::py_compat::take_exception;

/// The scope type of the cancel stack: scopes collect Python exceptions.
pub(crate) type PyScopeRef = ScopeRef<Py<PyBaseException>>;
/// A scope of the cancel stack, borrowed.
pub(crate) type PyScopeNode = ScopeNode<Py<PyBaseException>>;

/// The scopes a task entered, innermost first (spec 11 §2, `cancel_stack`).
///
/// Scopes a task enters nest (each one's parent is the scope the task was in when it entered
/// it), so the stack is the innermost one and its first `depth - 1` ancestors: one pointer
/// instead of a `SmallVec<[ScopeRef; 2]>` (24 bytes, and an allocation past two). Below them
/// is the base scope (depth 0): the scope the task was spawned into, which it never exits.
#[derive(Default)]
pub(crate) struct CancelStack {
    innermost: Option<PyScopeRef>,
    depth: u8,
}

impl CancelStack {
    /// The innermost scope, where the effective-cancellation walk starts.
    pub(crate) fn innermost(&self) -> Option<&PyScopeRef> {
        self.innermost.as_ref()
    }

    /// Records that the task entered `scope`, whose parent MUST be the innermost scope.
    pub(crate) fn enter(&mut self, scope: PyScopeRef) -> PyResult<()> {
        if self.depth == u8::MAX {
            return Err(PyRuntimeError::new_err(
                "porto: more than 255 cancel scopes nested in one task; flatten the nesting \
                 or move part of the work into a child task",
            ));
        }
        self.innermost = Some(scope);
        self.depth += 1;
        Ok(())
    }

    /// Whether the task entered no scope: its innermost one is its base.
    pub(crate) fn is_at_base(&self) -> bool {
        self.depth == 0
    }

    /// Makes `base` the base scope of a task that entered no scope (`is_at_base`): its implicit
    /// scope, a child of the scope it was spawned into (spec 15 §4), which the scopes it enters
    /// from now on are nested in.
    pub(crate) fn install_base(&mut self, base: PyScopeRef) {
        debug_assert!(
            self.depth == 0,
            "a base is installed below no entered scope"
        );
        self.innermost = Some(base);
    }

    /// Whether one of the scopes the task entered (not its base) is shielded: a shield stops
    /// the cancellation of its task's implicit scope too (spec 15 §4, C8).
    pub(crate) fn has_shield(&self) -> bool {
        shield_in(self.innermost.as_deref(), self.depth)
    }

    /// Records that the task exited `scope`: its parent is the innermost one again (the base
    /// scope once it exited every scope it entered). Returns `false`, changing nothing, if
    /// `scope` is not its innermost entered one (scopes exit in the reverse order they were
    /// entered).
    pub(crate) fn exit(&mut self, scope: &PyScopeNode) -> bool {
        let Some(innermost) = self.innermost.take_if(|innermost| {
            self.depth > 0 && std::ptr::eq(std::ptr::from_ref(&**innermost), scope)
        }) else {
            return false;
        };
        self.depth -= 1;
        self.innermost = innermost.parent().cloned();
        true
    }
}

/// Whether one of the `depth` scopes from `innermost` outward is shielded.
fn shield_in(innermost: Option<&PyScopeNode>, depth: u8) -> bool {
    let mut node = innermost;
    for _ in 0..depth {
        let Some(scope) = node else {
            return false;
        };
        if scope.is_shielded() {
            return true;
        }
        node = scope.parent().map(|parent| &**parent);
    }
    false
}

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
    /// spawned into, its base scope (spec 15 §1.2). See [`CancelStack`].
    cancel_scope: Option<PyScopeRef>,
    /// Who waits for the task to finish. Closed once the outcome is in `slot`.
    join: JoinWaiters,
    /// The `porto.Task` of this task, if it has one: `Scope.spawn` makes it with the task, and
    /// `current_task()` makes one for a task that has none (an internal spawn creates none:
    /// spec 11 §7). It holds the handle strongly while the task lives and gives it its
    /// outcome when it ends. The handle points back at the task *weakly* (`Task::bind`), so
    /// the two are no cycle: a task parked forever that nothing can wake is freed with its
    /// runtime, and the handle learns it (see `Drop`).
    pub(super) py_handle: Option<Py<Task>>,
    pub(super) stats: TaskStats,
    slot_kind: SlotKind,
    pub(super) pending_kind: Kind,
    /// Who else keeps the task's waker for its current wait: the lane, its innermost scope
    /// (`wait.rs`, [`WaitLinks`](super::wait::WaitLinks)).
    pub(super) wait_links: u8,
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
    /// The task was cancelled: its coroutine raised this `Cancelled` while its scope was
    /// cancelling (spec 11 §4), or, with `None`, it was cancelled before it finished (its
    /// runtime shut down).
    Cancelled(Option<Py<PyBaseException>>),
}

impl TaskOutcome {
    /// A new reference to the same outcome.
    pub(crate) fn clone_ref(&self, py: Python<'_>) -> Self {
        match self {
            Self::Ok(value) => Self::Ok(value.clone_ref(py)),
            Self::Err(exception) => Self::Err(exception.clone_ref(py)),
            Self::Cancelled(exception) => {
                Self::Cancelled(exception.as_ref().map(|exception| exception.clone_ref(py)))
            }
        }
    }

    /// The value, or the exception as an error (it keeps its traceback).
    pub(crate) fn into_result(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Err(exception) => Err(PyErr::from_value(exception.into_bound(py).into_any())),
            Self::Cancelled(Some(exception)) => Err(PyErr::from_value(
                joined_cancelled(py, exception).into_bound(py).into_any(),
            )),
            Self::Cancelled(None) => Err(shut_down_error()),
        }
    }
}

/// What reading the outcome of a task that ended `Cancelled` raises: a fresh `porto.Cancelled`
/// per read, caused by the one the task ended with, as `asyncio.Task.result()` does (spec 15
/// §4). Re-raising the stored exception would grow its traceback with every reader.
pub(crate) fn joined_cancelled(py: Python<'_>, stored: Py<PyBaseException>) -> Py<PyBaseException> {
    let fresh = Cancelled::new_err(
        "porto: the task you waited for was cancelled; its own cancellation is the cause below",
    );
    fresh.set_cause(
        py,
        Some(PyErr::from_value(stored.into_bound(py).into_any())),
    );
    fresh.into_value(py)
}

/// What reading the outcome of a task cancelled by its runtime raises.
pub(crate) fn shut_down_error() -> PyErr {
    RuntimeClosed::new_err(
        "porto: the task was cancelled before it finished: its runtime stopped (porto.run() \
         returned, or an error stopped it); keep porto.run() running until the tasks you wait \
         for are done",
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
        /// Whether it may never migrate to another lane: the header's `PIN` flag.
        pinned: bool,
    },
}

/// Spawns `coro` as a task on the runtime behind `handle`, which MUST be a Python lane's (see
/// `lane.rs`). The task runs in a copy of the current context, like `asyncio.create_task`.
pub(crate) fn spawn(
    handle: &Handle,
    coro: Bound<'_, PyAny>,
    spawned: Spawned,
) -> PyResult<TaskBodyRef<PyTask>> {
    let context = copy_current_context(coro.py())?;
    spawn_in_context(handle, coro, context, spawned)
}

/// A copy of the current `contextvars.Context` (`PyContext_CopyCurrent`).
pub(crate) fn copy_current_context(py: Python<'_>) -> PyResult<Py<PyAny>> {
    // SAFETY: attached (we hold `py`). Returns a new reference, or NULL with an exception set.
    let context = unsafe { ffi::PyContext_CopyCurrent() };
    if context.is_null() {
        return Err(PyErr::from_value(take_exception(py).into_any()));
    }
    // SAFETY: a new, non-null reference, which the `Bound` takes over.
    Ok(unsafe { Bound::from_owned_ptr(py, context) }.unbind())
}

/// [`spawn`] in `context` itself, not a copy: every step of the task enters it. `context` MUST
/// be a `contextvars.Context`. A child of a closed scope is refused (C7): its coroutine is
/// closed, unrun, and the error says so.
pub(crate) fn spawn_in_context(
    handle: &Handle,
    coro: Bound<'_, PyAny>,
    context: Py<PyAny>,
    spawned: Spawned,
) -> PyResult<TaskBodyRef<PyTask>> {
    let py = coro.py();
    let (flags, py_handle, scope) = match spawned {
        Spawned::Detached => (TaskFlags::PYTHON | TaskFlags::DETACHED_DROP, None, None),
        Spawned::Joined => (TaskFlags::PYTHON, None, None),
        Spawned::Child {
            task,
            scope,
            pinned,
        } => {
            let flags = if pinned {
                TaskFlags::PYTHON | TaskFlags::PIN
            } else {
                TaskFlags::PYTHON
            };
            (flags, Some(task), Some(scope))
        }
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
        wait_links: 0,
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
    handle
        .spawn_body_in(&scope, body, flags)
        .map_err(|ScopeClosed(mut refused)| {
            if let Some(coro) = refused.coro.take() {
                close_coroutine(coro.bind(py));
            }
            drop(refused);
            PyRuntimeError::new_err(ScopeClosed(()).to_string())
        })
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

    /// Whether one of the scopes the task entered is shielded, see [`CancelStack::has_shield`].
    pub(super) fn has_shield(&self) -> bool {
        shield_in(self.cancel_scope.as_deref(), self.cancel_depth)
    }

    /// The innermost scope the task is in, if any: where its effective cancellation is walked
    /// from (spec 15 §1.2).
    pub(super) fn innermost_scope(&self) -> Option<&PyScopeNode> {
        self.cancel_scope.as_deref()
    }

    /// Moves the cancel stack out, for the step to run with it reachable from Python calls
    /// (`step.rs`); [`PyTask::put_cancel_stack`] puts it back.
    pub(super) fn take_cancel_stack(&mut self) -> CancelStack {
        CancelStack {
            innermost: self.cancel_scope.take(),
            depth: std::mem::take(&mut self.cancel_depth),
        }
    }

    pub(super) fn put_cancel_stack(&mut self, stack: CancelStack) {
        self.cancel_scope = stack.innermost;
        self.cancel_depth = stack.depth;
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
            SlotKind::Cancelled => Some(TaskOutcome::Cancelled(object().map(|exception| {
                // SAFETY: only `complete` stores `Cancelled` with a payload, an exception.
                unsafe { exception.into_bound(py).cast_into_unchecked() }.unbind()
            }))),
            _ => None,
        }
    }

    /// The steps the task ran so far (`TaskStats::steps`).
    ///
    /// # Safety
    /// `task` points at a live body that is not being polled (read from its lane, between its
    /// polls).
    pub(crate) unsafe fn steps(task: NonNull<Self>) -> u32 {
        // SAFETY: the caller's contract; the field is read alone, not through a `&PyTask`.
        unsafe { (*task.as_ptr()).stats.steps }
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
        // A detached task has no one to raise to even if `current_task()` made it a handle:
        // nobody reads that handle, so the failure is still reported below.
        if let Some(task) = self.py_handle.take() {
            task.get().finish(outcome.clone_ref(py));
        }
        if header.flags().contains(TaskFlags::DETACHED_DROP)
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
            TaskOutcome::Cancelled(exception) => (SlotKind::Cancelled, exception.map(Py::into_any)),
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
    /// returned, or if it was cancelled (a `Cancelled` raised while its scope was cancelling
    /// ends the task as cancelled, see `step::poll`). The outcome stays for the joiners.
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
        let exception = self.slot.as_ref()?.clone_ref(py);
        // SAFETY: only `complete` stores `Err`, with an exception.
        let exception: Py<PyBaseException> =
            unsafe { exception.into_bound(py).cast_into_unchecked() }.unbind();
        Some(Box::new(exception))
    }

    fn cancel(self: Pin<&mut Self>, _header: &Header) {
        // SAFETY: nothing is moved out; the timer and the wait node stay in place.
        let this = unsafe { self.get_unchecked_mut() };
        // Unregisters the wait (T3) with what works off the lane (see `release_wait`).
        // Dropping the coroutine closes it; off a Python thread (a wake after shutdown), pyo3
        // defers that to the next attached one.
        this.release_wait();
        drop(this.coro.take());
        if let Some(task) = this.py_handle.take() {
            // Its scope's exit, or whoever holds the `porto.Task`, learns how it ended.
            task.get().finish(TaskOutcome::Cancelled(None));
        }
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
        // Freed before it ended (idle, with nothing left to wake it): its handle must not wait
        // for an outcome that will never come, and must forget the pointer to this body, which
        // is about to go (`Task::finish` clears it under the lock its readers hold).
        if let Some(task) = self.py_handle.take() {
            task.get().finish(TaskOutcome::Cancelled(None));
        }
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
        Poll::Ready(outcome.unwrap_or(TaskOutcome::Cancelled(None)))
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
    fn the_cancel_stack_exits_innermost_first_and_keeps_its_base_scope() {
        let base = PyScopeNode::new_root(1);
        let mut stack = CancelStack {
            innermost: Some(base.clone()),
            depth: 0,
        };
        assert!(!stack.exit(&base), "the base scope is never exited");
        let outer = PyScopeNode::new_child(&base, 1, false);
        stack.enter(outer.clone()).unwrap();
        let inner = PyScopeNode::new_child(&outer, 1, false);
        stack.enter(inner.clone()).unwrap();
        assert!(!stack.exit(&outer), "not the innermost one");
        assert!(stack.exit(&inner));
        assert!(stack.exit(&outer));
        assert_eq!(stack.innermost().unwrap().id(), base.id());
        assert_eq!(stack.depth, 0);
    }

    #[test]
    fn an_implicit_scope_becomes_the_base_and_only_entered_scopes_count_for_shields() {
        let spawned_into = PyScopeNode::new_root(1);
        let mut stack = CancelStack {
            innermost: Some(spawned_into.clone()),
            depth: 0,
        };
        assert!(stack.is_at_base());
        // A shielded scope the task was spawned into is not one it entered.
        spawned_into.set_shield(true);
        assert!(!stack.has_shield());
        let implicit = PyScopeNode::new_child(&spawned_into, 2, false);
        stack.install_base(implicit.clone());
        assert!(
            stack.is_at_base(),
            "the implicit scope is the base: nothing entered"
        );
        let entered = PyScopeNode::new_child(&implicit, 2, true);
        stack.enter(entered.clone()).unwrap();
        assert!(!stack.is_at_base());
        assert!(stack.has_shield(), "a shield the task entered");
        assert!(stack.exit(&entered));
        assert!(!stack.has_shield());
        assert_eq!(stack.innermost().unwrap().id(), implicit.id());
    }

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
