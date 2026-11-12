//! [`TaskRef`]: an owned, counted reference to a task, like an `Arc` whose count lives in the
//! state word.

use std::fmt;

use super::header::Header;
use super::raw::RawTask;

/// One counted reference to a task. Cloning adds a reference, dropping removes one, and the
/// last reference to go (a `TaskRef`, a waker or a run-queue entry) frees the task.
///
/// Holding a `TaskRef` keeps the allocation (header included) alive, not the future: a task
/// that completes drops its future at once, whoever still references it.
pub struct TaskRef(RawTask);

// SAFETY: tasks are built only from `Send` futures and `Send + Sync` schedulers. Through a
// `TaskRef` only the header is reachable, and every header field is atomic, immutable after
// spawn, or only touched by the lane holding RUNNING.
unsafe impl Send for TaskRef {}
// SAFETY: as above; `&TaskRef` only hands out `&Header`, whose shared accessors are read-only.
unsafe impl Sync for TaskRef {}

impl TaskRef {
    /// Takes over one reference the caller owns.
    ///
    /// # Safety
    /// `task` must be live, and the caller must give up one reference it owns to the result.
    pub(crate) unsafe fn from_owned(task: RawTask) -> Self {
        Self(task)
    }

    /// A new reference to the task at `ptr`, or `None` if its last reference is gone already (the
    /// task is being freed): the upgrade of a non-owning pointer, for the registry's walks.
    ///
    /// # Safety
    /// `ptr` is a task header with the provenance of its allocation, which is not freed while
    /// this runs (it is linked in a registry whose lock the caller holds).
    pub(crate) unsafe fn try_new(ptr: std::ptr::NonNull<Header>) -> Option<Self> {
        // SAFETY: the caller's contract.
        let header = unsafe { ptr.as_ref() };
        if !header.state.try_ref_inc() {
            return None;
        }
        // SAFETY: the increment above created the reference the `TaskRef` takes over.
        Some(Self(unsafe { RawTask::from_raw(ptr) }))
    }

    /// Cancels the task, as [`JoinHandle::abort`](super::JoinHandle::abort) does: if it has not
    /// completed it is not polled again, and its future is dropped on whoever polls it next, which
    /// here is the thread that drops the queue entry once the scheduler is closed. Consumes this
    /// reference.
    pub(crate) fn cancel(self) {
        use super::state::WakeAction;
        match self.header().state.transition_to_cancel_requested() {
            WakeAction::DoNothing => {}
            // SAFETY: the transition added the reference the queue now owns and set SCHEDULED.
            WakeAction::Submit => unsafe { self.0.schedule() },
            WakeAction::Dealloc => unreachable!("a cancel request never drops a reference"),
        }
    }

    /// Gives the reference back to the caller, without touching the count.
    pub(crate) fn into_raw(self) -> RawTask {
        let task = self.0;
        std::mem::forget(self);
        task
    }

    /// The task's pointer, with the provenance of the whole allocation; the count is not
    /// touched.
    pub(crate) fn raw(&self) -> RawTask {
        self.0
    }

    /// The task's id.
    pub fn id(&self) -> u64 {
        self.header().id()
    }

    /// The task's header.
    pub fn header(&self) -> &Header {
        self.0.header()
    }
}

impl Clone for TaskRef {
    fn clone(&self) -> Self {
        self.header().state.ref_inc();
        Self(self.0)
    }
}

impl Drop for TaskRef {
    fn drop(&mut self) {
        // SAFETY: a `TaskRef` owns exactly one reference.
        unsafe { self.0.drop_reference() }
    }
}

impl fmt::Debug for TaskRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskRef")
            .field("id", &self.id())
            .field("type", &self.header().type_name())
            .field("state", &self.header().state())
            .finish()
    }
}
