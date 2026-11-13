//! Which lane this thread is running, if any: what `spawn`, wakes and timers look up.

use std::cell::Cell;
use std::ptr;

use super::lane::Lane;
use super::shared::Handle;
use crate::task::Notified;
use crate::time::Deadline;

thread_local! {
    /// The lane whose loop (`block_on` or `run_until_idle`) is running on this thread.
    static CURRENT: Cell<*const Lane> = const { Cell::new(ptr::null()) };
}

/// In a forked child, on the thread that called `fork()` from inside a lane: forgets that this
/// thread runs a lane. The parent's lane (and the loop on this thread's stack that entered it) is
/// dead in the child, which has no other thread: a fresh runtime may be run on this thread. The
/// loop's own `Enter` guard clears the same slot if the stack ever unwinds.
pub fn forget_lane_in_forked_child() {
    // `try_with` fails only while thread locals are being destroyed: nothing to forget then.
    let _ = CURRENT.try_with(|current| current.set(ptr::null()));
}

/// Calls `f` with the lane running on this thread, if any.
fn with_lane<R>(f: impl FnOnce(&Lane) -> R) -> Option<R> {
    // `try_with` fails only while thread locals are being destroyed: not on a lane then.
    let lane = CURRENT.try_with(Cell::get).ok()?;
    // SAFETY: `CURRENT` is non-null only while an `Enter` guard borrows the lane, on this
    // thread, so the lane is alive.
    (!lane.is_null()).then(|| f(unsafe { &*lane }))
}

/// Pushes `task` onto the local queue if this thread is running one of `handle`'s lanes.
pub(crate) fn push_local(handle: &Handle, task: Notified) -> Result<(), Notified> {
    let mut task = Some(task);
    with_lane(|lane| {
        if lane.handle.same_runtime(handle)
            && let Some(task) = task.take()
        {
            lane.local.borrow_mut().push_back(task);
        }
    });
    task.map_or(Ok(()), Err)
}

/// Pushes a task that was just spawned onto the local queue, linking it into the lane's
/// registry first, if this thread is running one of `handle`'s lanes: one lookup of the lane
/// for both. Hands the task back otherwise.
pub(crate) fn push_new(handle: &Handle, task: Notified) -> Result<(), Notified> {
    let mut task = Some(task);
    with_lane(|lane| {
        if lane.handle.same_runtime(handle)
            && let Some(task) = task.take()
        {
            // SAFETY: this thread runs the lane, and the task was just spawned.
            unsafe { lane.register_new(&task) };
            lane.local.borrow_mut().push_back(task);
        }
    });
    task.map_or(Ok(()), Err)
}

/// Calls `f` with the lane of `handle`'s runtime that this thread is running, if it is one.
pub(crate) fn with_lane_of<R>(handle: &Handle, f: impl FnOnce(&Lane) -> R) -> Option<R> {
    with_lane(|lane| lane.handle.same_runtime(handle).then(|| f(lane))).flatten()
}

/// Calls `f` with the handle of the runtime running on this thread, if any (without cloning
/// it).
pub(crate) fn with_current_handle<R>(f: impl FnOnce(&Handle) -> R) -> Option<R> {
    with_lane(|lane| f(&lane.handle))
}

/// The current time on the clock of the runtime running on this thread, if any.
pub(crate) fn current_time() -> Option<Deadline> {
    with_lane(|lane| lane.shared().clock.now())
}

/// A handle to the runtime running on this thread, if any.
pub(crate) fn current_handle() -> Option<Handle> {
    with_lane(|lane| lane.handle.clone())
}

/// The lane index and tick count of the lane running on this thread, if any.
#[cfg(all(test, not(loom)))]
pub(crate) fn current_tick() -> Option<(usize, u32)> {
    with_lane(|lane| (lane.index(), lane.ticks()))
}

/// Marks this thread as running a lane until dropped.
pub(crate) struct Enter<'a> {
    lane: &'a Lane,
}

impl<'a> Enter<'a> {
    pub(crate) fn new(lane: &'a Lane) -> Self {
        CURRENT.with(|current| {
            assert!(
                current.get().is_null(),
                "porto: cannot run a runtime from inside a runtime (block_on or \
                 run_until_idle called from a task or from another block_on); spawn the work \
                 onto the current runtime instead, or run the other runtime on its own thread"
            );
            current.set(lane);
        });
        // For snapshot requests, which a lane that is not running cannot take.
        lane.remote()
            .entered
            .store(true, std::sync::atomic::Ordering::Release);
        Self { lane }
    }
}

impl Drop for Enter<'_> {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(ptr::null()));
        self.lane
            .remote()
            .entered
            .store(false, std::sync::atomic::Ordering::Release);
    }
}
