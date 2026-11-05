//! Which lane this thread is running, if any: what `spawn`, wakes and timers look up.

use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr;

use super::lane::Lane;
use super::shared::Handle;
use crate::task::Notified;
use crate::time::Deadline;

thread_local! {
    /// The lane whose loop (`block_on` or `run_until_idle`) is running on this thread.
    static CURRENT: Cell<*const Lane> = const { Cell::new(ptr::null()) };
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
    _lane: PhantomData<&'a Lane>,
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
        Self { _lane: PhantomData }
    }
}

impl Drop for Enter<'_> {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(ptr::null()));
    }
}
