//! The concrete task allocation for a Rust future, and its vtable functions.
//!
//! Poll protocol (`11-task-model-and-trap-protocol.md` §3): poll start moves SCHEDULED →
//! RUNNING; `Pending` either goes idle or, if a wake arrived meanwhile, requeues the task with
//! the run reference; `Ready` (a panic, or CANCEL_REQ seen at poll start) drops the future in
//! place, stores the output, sets COMPLETE, wakes the joiner and releases the run reference.

use std::future::Future;
use std::mem;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

use super::flags::TaskFlags;
use super::header::Header;
use super::join::{JoinError, JoinHandle};
use super::raw::{RawTask, TaskVTable};
use super::state::IdleAction;
use super::sync::UnsafeCell;
use super::waker::WakerRef;
use super::{Notified, Schedule};
use crate::scope::{TaskEnd, TaskScope};

/// Header + scheduler handle + future or output + join waker, in one allocation (a
/// `RustTask`). `repr(C)` keeps the header first, so a `NonNull<Header>` and a
/// `NonNull<Cell<F, S>>` are the same address, and the scheduler handle right after it: an
/// 8-byte handle fills the header's cache line (§1).
#[repr(C)]
struct Cell<F: Future, S> {
    header: Header,
    scheduler: S,
    /// The future, then its output. While the task is not COMPLETE, only the lane holding
    /// RUNNING touches it. Once COMPLETE, the join handle owns it if JOIN_INTEREST was set at
    /// completion, the completing lane otherwise. `dealloc` drops what is left.
    stage: UnsafeCell<Stage<F>>,
    /// The join handle's waker. Who may touch it is decided by JOIN_WAKER, see
    /// [`super::state::State::set_join_waker`].
    join_waker: UnsafeCell<Option<Waker>>,
}

enum Stage<F: Future> {
    Running(F),
    Finished(Result<F::Output, JoinError>),
    Consumed,
}

impl<F: Future, S> Cell<F, S> {
    /// Takes the future or output out, leaving `Consumed`, so a destructor that panics
    /// afterwards leaves nothing half-dropped.
    ///
    /// # Safety
    /// The caller has exclusive access to the stage (see the field).
    unsafe fn take_stage(&self) -> Stage<F> {
        // SAFETY: the caller's contract.
        self.stage
            .with_mut(|stage| unsafe { mem::replace(&mut *stage, Stage::Consumed) })
    }
}

/// Allocates a task nobody joins. The returned [`Notified`] owns the only reference, as a
/// run-queue entry.
/// Spawns hand out join handles, so only tests make tasks without one.
#[cfg(all(test, not(loom)))]
pub(crate) fn new_task<F, S>(future: F, scheduler: S) -> Notified
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    Notified(allocate(future, scheduler, false, None))
}

/// Allocates a task and its join handle, each owning one reference.
pub(crate) fn new_joinable_task<F, S>(future: F, scheduler: S) -> (Notified, JoinHandle<F::Output>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    new_joinable_task_in(future, scheduler, None)
}

/// As [`new_joinable_task`], for a task spawned into `scope`, which hears of its end.
pub(crate) fn new_joinable_task_in<F, S>(
    future: F,
    scheduler: S,
    scope: Option<TaskScope>,
) -> (Notified, JoinHandle<F::Output>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    let task = allocate(future, scheduler, true, scope);
    // SAFETY: the allocation's output type is `F::Output`, and the state holds the join
    // handle's reference and JOIN_INTEREST.
    (Notified(task), unsafe { JoinHandle::from_raw(task) })
}

fn allocate<F, S>(future: F, scheduler: S, join_handle: bool, scope: Option<TaskScope>) -> RawTask
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    let cell = Box::new(Cell {
        header: Header::new(vtable::<F, S>(), TaskFlags::NONE, join_handle, scope),
        scheduler,
        stage: UnsafeCell::new(Stage::Running(future)),
        join_waker: UnsafeCell::new(None),
    });
    let ptr = NonNull::from(Box::leak(cell)).cast::<Header>();
    // SAFETY: freshly allocated and live until its last reference goes.
    unsafe { RawTask::from_raw(ptr) }
}

fn vtable<F, S>() -> &'static TaskVTable
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // A constant expression of fn pointers: promoted to a `'static`, one per (F, S).
    &TaskVTable {
        poll: poll::<F, S>,
        drop_future: drop_future::<F, S>,
        dealloc: dealloc::<F, S>,
        schedule: schedule::<F, S>,
        try_read_output: try_read_output::<F, S>,
        drop_join_handle: drop_join_handle::<F, S>,
        type_name: std::any::type_name::<F>,
        body_offset: 0,
    }
}

/// The cell behind a header pointer. Shared references only: fields other threads touch are
/// atomics or `UnsafeCell`s, and the scheduler handle is never mutated.
///
/// # Safety
/// `ptr` is a live `Cell<F, S>`, and the caller holds a reference for as long as the returned
/// borrow lives.
unsafe fn cell<'a, F: Future, S>(ptr: NonNull<Header>) -> &'a Cell<F, S> {
    // SAFETY: the caller's contract.
    unsafe { ptr.cast::<Cell<F, S>>().as_ref() }
}

/// # Safety
/// `ptr` is a `Cell<F, S>` in the SCHEDULED state, and the caller hands over the queue's
/// reference.
unsafe fn poll<F, S>(ptr: NonNull<Header>, on_lane: bool)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: the caller's reference keeps the allocation alive.
    let task = unsafe { RawTask::from_raw(ptr) };
    // SAFETY: as above; the borrow is not used once the run reference is released.
    let cell = unsafe { cell::<F, S>(ptr) };
    let prev = cell.header.state.transition_to_running();

    let output = if prev.is_cancel_requested() {
        Err(JoinError::cancelled(cell.header.id))
    } else {
        // SAFETY: the run reference outlives `waker`, which is not used after the poll.
        let waker = unsafe { WakerRef::new(task) };
        let mut cx = Context::from_waker(&waker);
        let polled = panic::catch_unwind(AssertUnwindSafe(|| {
            cell.stage.with_mut(|stage| {
                // SAFETY: RUNNING grants exclusive access to the stage until the next
                // transition.
                let Stage::Running(future) = (unsafe { &mut *stage }) else {
                    unreachable!("a queued task always has its future");
                };
                // SAFETY: the future lives in a heap allocation that never moves, and it is
                // only dropped in place (in `complete`, `drop_future` or `dealloc`).
                unsafe { Pin::new_unchecked(future) }.poll(&mut cx)
            })
        }));
        match polled {
            Ok(Poll::Pending) => {
                match cell.header.state.transition_to_idle() {
                    IdleAction::Idle => {}
                    // SAFETY: the run reference moves to the queue; the task is SCHEDULED.
                    IdleAction::Reschedule => unsafe { task.schedule() },
                    // SAFETY: the transition dropped the last reference.
                    IdleAction::Dealloc => unsafe { task.dealloc() },
                }
                return;
            }
            Ok(Poll::Ready(output)) => Ok(output),
            Err(payload) => Err(JoinError::panic(cell.header.id, payload)),
        }
    };
    // SAFETY: still RUNNING with the run reference, which `complete` releases.
    unsafe { complete::<F, S>(task, output, on_lane) }
}

/// Poll end with `Ready`: drop the future, store `output`, set COMPLETE, hand the output to
/// the joiner or drop it, notify the scope and the scheduler and release the run reference. A
/// panic that nobody joins (or one from a destructor, or the scope's waker) is resumed once the
/// task is released.
///
/// # Safety
/// The caller holds RUNNING and the run reference, which this consumes.
unsafe fn complete<F, S>(task: RawTask, output: Result<F::Output, JoinError>, on_lane: bool)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: the run reference keeps the allocation alive until the `ref_dec` below.
    let cell = unsafe { cell::<F, S>(task.as_ptr()) };
    // SAFETY: we hold RUNNING and are completing the task.
    let scope = unsafe { cell.header.take_scope() };
    let end = scope.as_ref().map(|_| match &output {
        Ok(_) => TaskEnd::Completed,
        Err(error) if error.is_cancelled() => TaskEnd::Cancelled,
        Err(_) => TaskEnd::Failed(None),
    });
    // Drop the future while still RUNNING: wakes it performs on itself while dropping only
    // set NOTIFIED, which completion discards. The stage is `Consumed` before the destructor
    // runs, so a panicking destructor leaves nothing half-dropped.
    // SAFETY: RUNNING grants exclusive access to the stage.
    let future = unsafe { cell.take_stage() };
    let mut unhandled = panic::catch_unwind(AssertUnwindSafe(|| drop(future))).err();
    // SAFETY: as above; the stage is `Consumed`, so nothing is dropped.
    cell.stage
        .with_mut(|stage| unsafe { stage.write(Stage::Finished(output)) });

    let snapshot = cell.header.state.transition_to_complete();
    if !snapshot.is_join_interested() {
        // Nobody will read the output: COMPLETE without JOIN_INTEREST makes it ours.
        // SAFETY: the join handle is gone, so this lane has exclusive access to the stage.
        let finished = unsafe { cell.take_stage() };
        let dropped = panic::catch_unwind(AssertUnwindSafe(|| match finished {
            Stage::Finished(Err(error)) => error.try_into_panic().ok(),
            other => {
                drop(other);
                None
            }
        }));
        let panicked = dropped.unwrap_or_else(Some);
        unhandled = panicked.or(unhandled);
    } else if snapshot.is_join_waker_set() {
        let woken = panic::catch_unwind(AssertUnwindSafe(|| {
            cell.join_waker.with(|waker| {
                // SAFETY: COMPLETE with JOIN_WAKER set lets the lane read the waker.
                let waker = unsafe { &*waker };
                waker
                    .as_ref()
                    .expect("JOIN_WAKER set without a waker")
                    .wake_by_ref();
            });
        }));
        let snapshot = cell.header.state.unset_join_waker_after_complete();
        let mut dropped = Ok(());
        if !snapshot.is_join_interested() {
            // A panicking `Waker::drop` must not skip `task_completed` and the `ref_dec` below.
            dropped = panic::catch_unwind(AssertUnwindSafe(|| {
                // SAFETY: the handle dropped while JOIN_WAKER was ours, so the waker is ours
                // to drop.
                cell.join_waker.with_mut(|waker| unsafe { *waker = None });
            }));
        }
        unhandled = unhandled.or(woken.err()).or(dropped.err());
    }
    if let (Some(scope), Some(end)) = (scope, end) {
        let notified = panic::catch_unwind(AssertUnwindSafe(|| scope.end(end)));
        unhandled = unhandled.or(notified.err());
    }
    super::unregister_completed(&cell.header, &cell.scheduler, on_lane);
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
/// `ptr` is a `Cell<F, S>` in the SCHEDULED state, and the caller hands over one reference.
unsafe fn schedule<F, S>(ptr: NonNull<Header>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    let cell = ptr.cast::<Cell<F, S>>().as_ptr();
    // SAFETY: the caller's reference keeps the allocation alive and moves into `Notified`.
    let task = Notified(unsafe { RawTask::from_raw(ptr) });
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
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: the caller's reference keeps the allocation alive.
    let cell = unsafe { cell::<F, S>(ptr) };
    // SAFETY: RUNNING grants exclusive access to the stage.
    let future = unsafe { cell.take_stage() };
    drop(future);
}

/// Writes `Poll::Ready(output)` to `dst` if the task is complete, else registers `waker` as the
/// join waker.
///
/// # Safety
/// `ptr` is a `Cell<F, S>`, the caller is its join handle (holding a reference and
/// JOIN_INTEREST), and `dst` points to a `Poll<Result<F::Output, JoinError>>`.
unsafe fn try_read_output<F, S>(ptr: NonNull<Header>, dst: *mut (), waker: &Waker)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: the join handle's reference keeps the allocation alive.
    let cell = unsafe { cell::<F, S>(ptr) };
    if !can_read_output(&cell.header, &cell.join_waker, waker) {
        return;
    }
    // SAFETY: COMPLETE was seen with JOIN_INTEREST held: the stage is the join handle's.
    let stage = unsafe { cell.take_stage() };
    let Stage::Finished(output) = stage else {
        panic!(
            "porto: JoinHandle polled again after it returned the task's output; await (or \
             poll) a JoinHandle only until it is ready"
        );
    };
    // SAFETY: the caller's contract on `dst`; the old value is a `Poll::Pending`.
    unsafe { *dst.cast::<Poll<Result<F::Output, JoinError>>>() = Poll::Ready(output) };
}

/// Whether the task is complete. If not, makes sure `waker` is the join waker the lane will
/// wake on completion. Follows Tokio's `can_read_output`.
fn can_read_output(header: &Header, slot: &UnsafeCell<Option<Waker>>, waker: &Waker) -> bool {
    let state = &header.state;
    let snapshot = state.load();
    debug_assert!(snapshot.is_join_interested());
    if snapshot.is_complete() {
        return true;
    }
    let stored = if snapshot.is_join_waker_set() {
        let same = slot.with(|stored| {
            // SAFETY: with JOIN_WAKER set, the join handle may read the waker.
            unsafe { &*stored }
                .as_ref()
                .is_some_and(|w| w.will_wake(waker))
        });
        if same {
            return false;
        }
        state
            .unset_join_waker()
            .and_then(|_| set_join_waker(state, slot, waker.clone()))
    } else {
        set_join_waker(state, slot, waker.clone())
    };
    match stored {
        Ok(_) => false,
        Err(snapshot) => {
            debug_assert!(snapshot.is_complete());
            true
        }
    }
}

/// Stores `waker` in the slot (JOIN_WAKER is clear, so the join handle owns it) and publishes
/// it. If the task completed meanwhile, takes it back out.
fn set_join_waker(
    state: &super::state::State,
    slot: &UnsafeCell<Option<Waker>>,
    waker: Waker,
) -> Result<super::state::Snapshot, super::state::Snapshot> {
    // SAFETY: JOIN_WAKER is clear: only the join handle touches the slot.
    slot.with_mut(|stored| unsafe { *stored = Some(waker) });
    let result = state.set_join_waker();
    if result.is_err() {
        // SAFETY: the bit was not set, so the slot is still the join handle's alone.
        slot.with_mut(|stored| unsafe { *stored = None });
    }
    result
}

/// The join handle is dropped: clean up what it owns, then drop its reference.
///
/// # Safety
/// `ptr` is a `Cell<F, S>`, the caller is its join handle and gives up its reference.
unsafe fn drop_join_handle<F, S>(ptr: NonNull<Header>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: the join handle's reference keeps the allocation alive until the end.
    let task = unsafe { RawTask::from_raw(ptr) };
    // SAFETY: as above; the borrow ends before the reference is dropped.
    let cell = unsafe { cell::<F, S>(ptr) };
    let action = cell.header.state.transition_to_join_handle_dropped();
    let mut dropped = Ok(());
    if action.drop_output {
        // SAFETY: COMPLETE was set while JOIN_INTEREST was held: the stage is ours.
        let output = unsafe { cell.take_stage() };
        dropped = panic::catch_unwind(AssertUnwindSafe(|| drop(output)));
    }
    if action.drop_waker {
        // Caught like the output: a panicking `Waker::drop` must not leak our reference.
        let waker_dropped = panic::catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: JOIN_WAKER is clear and JOIN_INTEREST gone: no one else touches the slot.
            cell.join_waker.with_mut(|waker| unsafe { *waker = None });
        }));
        dropped = dropped.and(waker_dropped);
    }
    // SAFETY: the join handle owned this reference.
    unsafe { task.drop_reference() };
    if let Err(payload) = dropped {
        panic::resume_unwind(payload);
    }
}

/// # Safety
/// `ptr` is a `Cell<F, S>` whose refcount just reached zero.
unsafe fn dealloc<F, S>(ptr: NonNull<Header>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // A task that never completed (or completed off its lane) may still be linked in its
    // registry: it is taken off first, or handed to the lane that owns the list.
    // SAFETY: the refcount reached zero, so the allocation is ours until the `Box` below.
    if !unsafe {
        super::unregister_before_free(ptr, std::ptr::from_ref(&cell::<F, S>(ptr).scheduler))
    } {
        return;
    }
    // SAFETY: allocated by `Box` in `allocate`, and no reference is left to observe it.
    let cell = unsafe { Box::from_raw(ptr.cast::<Cell<F, S>>().as_ptr()) };
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

#[cfg(all(test, not(loom), target_pointer_width = "64"))]
mod layout_tests {
    use std::mem::offset_of;

    use super::Cell;
    use crate::sched::Handle;

    #[test]
    fn the_scheduler_handle_fills_the_header_cache_line() {
        type Task = Cell<std::future::Ready<()>, Handle>;
        assert_eq!(size_of::<Handle>(), 8);
        assert_eq!(offset_of!(Task, scheduler), 56);
        assert_eq!(
            offset_of!(Task, stage),
            64,
            "the body starts on the next line"
        );
    }
}
