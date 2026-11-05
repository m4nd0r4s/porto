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

    /// Gives the reference back to the caller, without touching the count.
    pub(crate) fn into_raw(self) -> RawTask {
        let task = self.0;
        std::mem::forget(self);
        task
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
