//! The concrete task allocation for a Rust future, and its vtable functions.
//!
//! Poll protocol (`11-task-model-and-trap-protocol.md` §3): poll start moves SCHEDULED →
//! RUNNING; `Pending` either goes idle or, if a wake arrived meanwhile, requeues the task with
//! the run reference; `Ready` (or a panic) drops the future in place, sets COMPLETE and
//! releases the run reference.

use std::cell::UnsafeCell;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use super::flags::TaskFlags;
use super::header::Header;
use super::raw::{RawTask, TaskVTable};
use super::state::IdleAction;
use super::task_ref::TaskRef;
use super::waker::WakerRef;
use super::{Notified, Schedule};

/// Header + scheduler handle + future, in one allocation. `repr(C)` keeps the header first,
/// so a `NonNull<Header>` and a `NonNull<Cell<F, S>>` are the same address.
#[repr(C)]
struct Cell<F, S> {
    header: Header,
    scheduler: S,
    /// `Some` until the task completes. Only the lane holding RUNNING touches it, except
    /// `dealloc`, which runs once no reference is left.
    future: UnsafeCell<Option<F>>,
}

/// Allocates a task. The returned [`Notified`] owns the only reference, as a run-queue entry.
pub(crate) fn new_task<F, S>(future: F, scheduler: S) -> Notified
where
    F: Future<Output = ()> + Send + 'static,
    S: Schedule,
{
    let cell = Box::new(Cell {
        header: Header::new(vtable::<F, S>(), TaskFlags::NONE),
        scheduler,
        future: UnsafeCell::new(Some(future)),
    });
    let ptr = NonNull::from(Box::leak(cell)).cast::<Header>();
    // SAFETY: freshly allocated, and its state holds the one reference `Notified` takes over.
    Notified(unsafe { TaskRef::from_owned(RawTask::from_raw(ptr)) })
}

fn vtable<F, S>() -> &'static TaskVTable
where
    F: Future<Output = ()> + Send + 'static,
    S: Schedule,
{
    // A constant expression of fn pointers: promoted to a `'static`, one per (F, S).
    &TaskVTable {
        poll: poll::<F, S>,
        drop_future: drop_future::<F, S>,
        dealloc: dealloc::<F, S>,
        schedule: schedule::<F, S>,
        type_name: std::any::type_name::<F>,
    }
}

/// # Safety
/// `ptr` is a `Cell<F, S>` in the SCHEDULED state, and the caller hands over the queue's
/// reference.
unsafe fn poll<F, S>(ptr: NonNull<Header>)
where
    F: Future<Output = ()> + Send + 'static,
    S: Schedule,
{
    // SAFETY: the caller's reference keeps the allocation alive.
    let task = unsafe { RawTask::from_raw(ptr) };
    task.header().state.transition_to_running();

    let cell = ptr.cast::<Cell<F, S>>().as_ptr();
    // SAFETY: RUNNING grants exclusive access to the future slot until the next transition.
    // The projection goes through a raw pointer, so it does not assert anything about the
    // header or scheduler, which other threads may read concurrently.
    let slot = unsafe { &mut *(*cell).future.get() };
    // SAFETY: the run reference outlives `waker`, which is not used after the transitions.
    let waker = unsafe { WakerRef::new(task) };
    let mut cx = Context::from_waker(&waker);

    let polled = panic::catch_unwind(AssertUnwindSafe(|| {
        let future = slot.as_mut().expect("a queued task always has its future");
        // SAFETY: the future lives in a heap allocation that never moves, and it is only
        // dropped in place (below, or in `dealloc`).
        unsafe { Pin::new_unchecked(future) }.poll(&mut cx)
    }));

    let panicked = match polled {
        Ok(Poll::Pending) => {
            match task.header().state.transition_to_idle() {
                IdleAction::Idle => {}
                // SAFETY: the run reference moves to the queue; the task is SCHEDULED again.
                IdleAction::Reschedule => unsafe { task.schedule() },
                // SAFETY: the transition dropped the last reference.
                IdleAction::Dealloc => unsafe { task.dealloc() },
            }
            return;
        }
        Ok(Poll::Ready(())) => None,
        Err(payload) => Some(payload),
    };

    // Drop the future while still RUNNING: wakes it performs on itself while dropping only
    // set NOTIFIED, which completion discards.
    // SAFETY: still RUNNING with the run reference, and `slot` is not used again. A direct
    // call: the concrete type is known here, so the vtable hop is for type-erased callers.
    let dropped = panic::catch_unwind(AssertUnwindSafe(|| unsafe { drop_future::<F, S>(ptr) }));
    if task.header().state.transition_to_complete_and_release() {
        // SAFETY: the transition dropped the last reference.
        unsafe { task.dealloc() };
    }
    if let Some(payload) = panicked.or(dropped.err()) {
        panic::resume_unwind(payload);
    }
}

/// # Safety
/// `ptr` is a `Cell<F, S>` in the SCHEDULED state, and the caller hands over one reference.
unsafe fn schedule<F, S>(ptr: NonNull<Header>)
where
    F: Future<Output = ()> + Send + 'static,
    S: Schedule,
{
    let cell = ptr.cast::<Cell<F, S>>().as_ptr();
    // SAFETY: the caller's reference keeps the allocation alive and moves into `Notified`.
    let task = Notified(unsafe { TaskRef::from_owned(RawTask::from_raw(ptr)) });
    // SAFETY: the reference now held by `task` keeps the allocation alive; the scheduler
    // handle is never mutated after construction. `schedule_local` never publishes the task
    // to another thread, so the borrow cannot outlive the allocation.
    let Err(task) = (unsafe { &(*cell).scheduler }).schedule_local(task) else {
        return;
    };
    // Off the owning lane: once the task is published another thread may run and free it,
    // so the remote path works on an owned handle, not on a borrow into the allocation.
    // SAFETY: as above; the borrow ends once `clone` returns.
    let scheduler = unsafe { &(*cell).scheduler }.clone();
    scheduler.schedule_remote(task);
}

/// # Safety
/// `ptr` is a `Cell<F, S>`, the caller holds RUNNING and a reference.
unsafe fn drop_future<F, S>(ptr: NonNull<Header>)
where
    F: Future<Output = ()> + Send + 'static,
    S: Schedule,
{
    let cell = ptr.cast::<Cell<F, S>>().as_ptr();
    // SAFETY: RUNNING grants exclusive access to the future slot; the projection goes through
    // a raw pointer, so it asserts nothing about fields other threads may read.
    unsafe { *(*cell).future.get() = None };
}

/// # Safety
/// `ptr` is a `Cell<F, S>` whose refcount just reached zero.
unsafe fn dealloc<F, S>(ptr: NonNull<Header>)
where
    F: Future<Output = ()> + Send + 'static,
    S: Schedule,
{
    // SAFETY: allocated by `Box` in `new_task`, and no reference is left to observe it.
    drop(unsafe { Box::from_raw(ptr.cast::<Cell<F, S>>().as_ptr()) });
}
