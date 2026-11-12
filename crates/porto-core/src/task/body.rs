//! Tasks whose body is not a Rust future: the PyTask (`11-task-model-and-trap-protocol.md` §1,
//! §2).
//!
//! A [`TaskBody`] keeps its own outcome and its joiners, so the allocation is only the header,
//! the scheduler handle and the body: no output stage and no join-handle waker slot, which a
//! `RustTask` needs and a PyTask does not. The poll protocol is the `RustTask`'s (see
//! `harness`): poll start SCHEDULED → RUNNING, `Pending` goes idle or requeues, `Ready` sets
//! COMPLETE, notifies the scope and the scheduler and releases the run reference.

use std::any::Any;
use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

use super::flags::TaskFlags;
use super::header::Header;
use super::raw::{RawTask, TaskVTable};
use super::state::IdleAction;
use super::task_ref::TaskRef;
use super::waker::WakerRef;
use super::{Notified, Schedule};
use crate::scope::{TaskEnd, TaskScope};

/// The body of a task that is not a Rust future: it keeps its outcome and its joiners itself.
///
/// The body lives in the task allocation, which never moves, so it is pinned: it may embed
/// intrusive nodes (a timer entry, a wait node) that other threads point at.
pub trait TaskBody: Send + 'static {
    /// Runs the body until it finishes (`Ready`, which completes the task) or waits
    /// (`Pending`, after arranging a wake through `cx`). Not called again once it returned
    /// `Ready`.
    /// `header` is the task's own: its id and spawn flags.
    fn poll(self: Pin<&mut Self>, header: &Header, cx: &mut Context<'_>) -> Poll<()>;

    /// The task is cancelled instead of polled: its run-queue entry was dropped (the runtime
    /// shut down), or its last poll panicked. Release what the body runs and record the
    /// cancellation for its joiners; the task completes right after, and the body is not
    /// polled again. After a panic the body may be half-way through a step, so this must not
    /// assume a consistent resume state.
    fn cancel(self: Pin<&mut Self>, header: &Header);

    /// The body returned `Ready` in a task spawned into a scope: the failure the scope records
    /// (C4), or `None` if the body succeeded or its failure does not count (a `Cancelled` from
    /// a scope that is cancelling it). The scope keeps the payload if it is its error type `E`
    /// (see [`crate::scope::TaskEnd::Failed`]); any failure cancels the scope. Called once,
    /// before the task completes, and not after `cancel`.
    fn take_failure(self: Pin<&mut Self>) -> Option<Box<dyn Any + Send>> {
        None
    }
}

/// Header + scheduler handle + body, in one allocation. `repr(C)` keeps the header first and
/// the handle right after it, as for a `RustTask`: the body starts on the next cache line.
#[repr(C)]
struct BodyCell<B, S> {
    header: Header,
    scheduler: S,
    /// Only the lane holding RUNNING creates a `&mut` to it; anything else reaches it through
    /// [`TaskBodyRef::body`], under the body's own synchronization rules.
    body: UnsafeCell<B>,
}

/// An owned, counted reference to a task with body `B`: a [`TaskRef`] that knows where the
/// body is. One word: the body's offset (which depends on the scheduler type) is in the
/// task's vtable, so a body can store a reference to another task in a one-word slot.
pub struct TaskBodyRef<B> {
    task: TaskRef,
    _body: PhantomData<B>,
}

const _: () = assert!(size_of::<TaskBodyRef<()>>() == size_of::<usize>());

// SAFETY: the reference only reaches the header (see `TaskRef`) and a raw pointer to the body,
// which the body type synchronizes; bodies are `Send`.
unsafe impl<B: Send> Send for TaskBodyRef<B> {}
// SAFETY: as above.
unsafe impl<B: Send> Sync for TaskBodyRef<B> {}

impl<B: TaskBody> TaskBodyRef<B> {
    /// The task's pointer, with the provenance of the whole allocation: what a non-owning
    /// holder keeps to [`TaskBodyRef::try_from_ptr`] it later. The count is not touched.
    pub fn as_ptr(&self) -> NonNull<Header> {
        self.task.raw().as_ptr()
    }

    /// A new reference to the task at `ptr`, or `None` if it is being freed (its last
    /// reference was dropped): the upgrade of a non-owning pointer to a task.
    ///
    /// # Safety
    /// `ptr` MUST come from [`TaskBodyRef::as_ptr`] (or a task waker's data) of a task made by
    /// [`new_body_task`]/[`new_body_task_in`] with body type `B`, whose allocation is not freed
    /// yet: the caller keeps it from being freed while this runs, by whatever it holds that the
    /// task's body waits for before the allocation goes (the body's `Drop` runs before the
    /// memory is released, so a lock the body's `Drop` takes is enough).
    pub unsafe fn try_from_ptr(ptr: NonNull<Header>) -> Option<Self> {
        // SAFETY: the allocation is live (the caller's contract).
        let header = unsafe { ptr.as_ref() };
        if !header.state.try_ref_inc() {
            return None;
        }
        // SAFETY: the increment above made the reference the `TaskRef` takes over; `ptr` is the
        // header pointer of a live task allocation, with its provenance.
        let task = unsafe { TaskRef::from_owned(RawTask::from_raw(ptr)) };
        Some(Self {
            task,
            _body: PhantomData,
        })
    }

    /// The task's header.
    pub fn header(&self) -> &Header {
        self.task.header()
    }

    /// The task's id.
    pub fn id(&self) -> u64 {
        self.task.id()
    }

    /// A plain counted reference to the task.
    pub fn task_ref(&self) -> &TaskRef {
        &self.task
    }

    /// Whether the task completed (its body returned `Ready`, or it was cancelled).
    pub fn is_complete(&self) -> bool {
        self.header().state().load().is_complete()
    }

    /// A pointer to the body, valid while this reference lives.
    ///
    /// The lane that polls the task holds a `Pin<&mut B>` while it runs, so dereferencing the
    /// pointer is only sound for what the body itself synchronizes (atomics, cells, data it
    /// published before completion): reach single fields with `&raw const (*body).field`
    /// rather than through a `&B`.
    pub fn body(&self) -> NonNull<B> {
        let offset = self.header().vtable.body_offset;
        // SAFETY: the vtable's `body_offset` is the body's offset in this allocation (set by
        // `new_body_task`, the only maker of a `TaskBodyRef`), which is live while `self.task`
        // holds its reference.
        unsafe {
            self.task
                .raw()
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<B>()
        }
    }
}

impl<B> Clone for TaskBodyRef<B> {
    fn clone(&self) -> Self {
        Self {
            task: self.task.clone(),
            _body: PhantomData,
        }
    }
}

impl<B> fmt::Debug for TaskBodyRef<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.task.fmt(f)
    }
}

/// Allocates a task running `body`, spawned with `flags`. Returns its run-queue entry, which
/// the caller queues, and a reference to it.
pub(crate) fn new_body_task<B, S>(
    body: B,
    scheduler: S,
    flags: TaskFlags,
) -> (Notified, TaskBodyRef<B>)
where
    B: TaskBody,
    S: Schedule,
{
    new_body_task_in(body, scheduler, flags, None)
}

/// As [`new_body_task`], for a task spawned into `scope`, which hears of its end.
pub(crate) fn new_body_task_in<B, S>(
    body: B,
    scheduler: S,
    flags: TaskFlags,
    scope: Option<TaskScope>,
) -> (Notified, TaskBodyRef<B>)
where
    B: TaskBody,
    S: Schedule,
{
    let cell = Box::new(BodyCell {
        header: Header::new(vtable::<B, S>(), flags, false, scope),
        scheduler,
        body: UnsafeCell::new(body),
    });
    let ptr = NonNull::from(Box::leak(cell)).cast::<Header>();
    // SAFETY: freshly allocated and live until its last reference goes.
    let task = unsafe { RawTask::from_raw(ptr) };
    // The queue owns the first reference; this one is the `TaskBodyRef`'s. Taken before the
    // task is queued, since a queued task may run and complete at once.
    task.header().state.ref_inc();
    // SAFETY: the increment above created the reference the `TaskRef` takes over.
    let task_ref = unsafe { TaskRef::from_owned(task) };
    (
        Notified(task),
        TaskBodyRef {
            task: task_ref,
            _body: PhantomData,
        },
    )
}

fn vtable<B: TaskBody, S: Schedule>() -> &'static TaskVTable {
    // A constant expression of fn pointers: promoted to a `'static`, one per (B, S).
    &TaskVTable {
        poll: poll::<B, S>,
        drop_future: drop_body::<B, S>,
        dealloc: dealloc::<B, S>,
        schedule: schedule::<B, S>,
        try_read_output: no_join_handle,
        drop_join_handle: no_join_handle_drop,
        type_name: std::any::type_name::<B>,
        body_offset: std::mem::offset_of!(BodyCell<B, S>, body),
    }
}

/// # Safety
/// `ptr` is a live `BodyCell<B, S>`, and the caller holds a reference for as long as the
/// returned borrow lives.
unsafe fn cell<'a, B, S>(ptr: NonNull<Header>) -> &'a BodyCell<B, S> {
    // SAFETY: the caller's contract.
    unsafe { ptr.cast::<BodyCell<B, S>>().as_ref() }
}

/// # Safety
/// The caller holds RUNNING, which grants the only `&mut` to the body, for as long as the
/// result lives.
#[allow(
    clippy::mut_from_ref,
    reason = "the `&mut` comes from the `UnsafeCell`; RUNNING makes it exclusive"
)]
unsafe fn body_mut<B, S>(cell: &BodyCell<B, S>) -> Pin<&mut B> {
    // SAFETY: RUNNING excludes other `&mut` (the caller's contract); the body never moves.
    unsafe { Pin::new_unchecked(&mut *cell.body.get()) }
}

/// # Safety
/// `ptr` is a `BodyCell<B, S>` in the SCHEDULED state, and the caller hands over the queue's
/// reference.
unsafe fn poll<B: TaskBody, S: Schedule>(ptr: NonNull<Header>) {
    // SAFETY: the caller's reference keeps the allocation alive.
    let task = unsafe { RawTask::from_raw(ptr) };
    // SAFETY: as above; the borrow is not used once the run reference is released.
    let cell = unsafe { cell::<B, S>(ptr) };
    let prev = cell.header.state.transition_to_running();
    let polled = panic::catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: we hold RUNNING until the transition after this closure.
        let mut body = unsafe { body_mut(cell) };
        if prev.is_cancel_requested() {
            body.cancel(&cell.header);
            return Poll::Ready(Some(TaskEnd::Cancelled));
        }
        // SAFETY: the run reference outlives `waker`, which is not used after the poll.
        let waker = unsafe { WakerRef::new(task) };
        let polled = body
            .as_mut()
            .poll(&cell.header, &mut Context::from_waker(&waker));
        // Only a task in a scope asks its body for a failure (`None` below), outside this panic
        // guard: a body that returned `Ready` is never cancelled.
        // SAFETY: we hold RUNNING and nobody else touches the slot.
        let in_scope = unsafe { cell.header.has_scope() };
        polled.map(|()| (!in_scope).then_some(TaskEnd::Completed))
    }));
    match polled {
        Ok(Poll::Pending) => match cell.header.state.transition_to_idle() {
            IdleAction::Idle => {}
            // SAFETY: the run reference moves to the queue; the task is SCHEDULED.
            IdleAction::Reschedule => unsafe { task.schedule() },
            // SAFETY: the transition dropped the last reference.
            IdleAction::Dealloc => unsafe { task.dealloc() },
        },
        // SAFETY: still RUNNING with the run reference, which `complete` releases.
        Ok(Poll::Ready(Some(end))) => unsafe { complete::<B, S>(task, end, None) },
        // A panic in `take_failure` fails the task without a payload for the scope, and goes
        // on once the task is released, like a panic in the poll (but the body is not
        // cancelled: it already finished).
        Ok(Poll::Ready(None)) => {
            let failure = panic::catch_unwind(AssertUnwindSafe(|| {
                // SAFETY: still RUNNING.
                unsafe { body_mut(cell) }.take_failure()
            }));
            let (end, unhandled) = match failure {
                Ok(Some(failure)) => (TaskEnd::Failed(Some(failure)), None),
                Ok(None) => (TaskEnd::Completed, None),
                Err(payload) => (TaskEnd::Failed(None), Some(payload)),
            };
            // SAFETY: still RUNNING with the run reference, which `complete` releases.
            unsafe { complete::<B, S>(task, end, unhandled) }
        }
        // A body that panics completes: it is not polled again, and the panic goes on from
        // here once the task is released. It is cancelled first, so its joiners are released
        // instead of waiting forever; a panic from that cleanup is dropped (the first wins).
        Err(payload) => {
            drop(panic::catch_unwind(AssertUnwindSafe(|| {
                // SAFETY: still RUNNING: the panicked poll released nothing.
                unsafe { body_mut(cell) }.cancel(&cell.header);
            })));
            // SAFETY: as above.
            unsafe { complete::<B, S>(task, TaskEnd::Failed(None), Some(payload)) }
        }
    }
}

/// Poll end with `Ready`: set COMPLETE, tell the scope the task ended with `end`, notify the
/// scheduler, release the run reference, then resume `panic` if the body panicked (or else a
/// panic from the scope's waker).
///
/// # Safety
/// The caller holds RUNNING and the run reference, which this consumes.
unsafe fn complete<B: TaskBody, S: Schedule>(
    task: RawTask,
    end: TaskEnd,
    mut unhandled: Option<Box<dyn Any + Send>>,
) {
    // SAFETY: the run reference keeps the allocation alive until the `ref_dec` below.
    let cell = unsafe { cell::<B, S>(task.as_ptr()) };
    // SAFETY: we hold RUNNING and are completing the task.
    let scope = unsafe { cell.header.take_scope() };
    cell.header.state.transition_to_complete();
    if let Some(scope) = scope {
        let notified = panic::catch_unwind(AssertUnwindSafe(|| scope.end(end)));
        unhandled = unhandled.or(notified.err());
    }
    cell.scheduler.task_completed(&cell.header);
    if cell.header.state.ref_dec() {
        // SAFETY: the decrement released the last reference.
        unsafe { task.dealloc() };
    }
    if let Some(payload) = unhandled {
        panic::resume_unwind(payload);
    }
}

/// # Safety
/// `ptr` is a `BodyCell<B, S>` in the SCHEDULED state, and the caller hands over one
/// reference.
unsafe fn schedule<B: TaskBody, S: Schedule>(ptr: NonNull<Header>) {
    let cell = ptr.cast::<BodyCell<B, S>>().as_ptr();
    // SAFETY: the caller's reference keeps the allocation alive and moves into `Notified`.
    let task = Notified(unsafe { RawTask::from_raw(ptr) });
    // SAFETY: as in the harness's `schedule`: the scheduler handle is never mutated, and
    // `schedule_local` does not publish the task to another thread.
    let Err(task) = (unsafe { &(*cell).scheduler }).schedule_local(task) else {
        return;
    };
    // SAFETY: as above; the borrow ends once `clone` returns.
    let scheduler = unsafe { &(*cell).scheduler }.clone();
    scheduler.schedule_remote(task);
}

/// # Safety
/// `ptr` is a `BodyCell<B, S>`, the caller holds RUNNING and a reference.
unsafe fn drop_body<B: TaskBody, S: Schedule>(ptr: NonNull<Header>) {
    // SAFETY: the caller's reference keeps the allocation alive, and RUNNING grants the body.
    let cell = unsafe { cell::<B, S>(ptr) };
    // SAFETY: as above.
    unsafe { body_mut(cell) }.cancel(&cell.header);
}

/// # Safety
/// `ptr` is a `BodyCell<B, S>` whose refcount just reached zero.
unsafe fn dealloc<B: TaskBody, S: Schedule>(ptr: NonNull<Header>) {
    // SAFETY: allocated by `Box` in `new_body_task_in`, and no reference is left to observe it.
    let cell = unsafe { Box::from_raw(ptr.cast::<BodyCell<B, S>>().as_ptr()) };
    // A task that never completed still holds its scope: it ends as freed once what it owns
    // is dropped. A panic from that drop goes on after the scope is told, which happens outside
    // the unwind (a second panic there would abort).
    // SAFETY: we hold the last reference.
    let scope = unsafe { cell.header.take_scope() };
    let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| drop(cell))) else {
        drop(scope);
        return;
    };
    drop(panic::catch_unwind(AssertUnwindSafe(|| drop(scope))));
    panic::resume_unwind(payload);
}

/// A body task has no join handle: its joiners wait through the body.
unsafe fn no_join_handle(_: NonNull<Header>, _: *mut (), _: &Waker) {
    unreachable!("a task with a TaskBody has no JoinHandle");
}

unsafe fn no_join_handle_drop(_: NonNull<Header>) {
    unreachable!("a task with a TaskBody has no JoinHandle");
}

#[cfg(all(test, not(loom), target_pointer_width = "64"))]
mod layout_tests {
    use std::mem::offset_of;

    use super::BodyCell;
    use crate::sched::Handle;

    #[test]
    fn the_body_starts_on_the_line_after_the_header() {
        type Task = BodyCell<[u64; 4], Handle>;
        assert_eq!(offset_of!(Task, scheduler), 56);
        assert_eq!(offset_of!(Task, body), 64);
    }
}
