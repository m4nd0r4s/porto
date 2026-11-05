//! Tasks: a refcounted heap allocation with a [`Header`] first, polled through a vtable.
//!
//! See `11-task-model-and-trap-protocol.md` §1 and §3. A task is referenced by run-queue
//! entries ([`Notified`]), [`TaskRef`]s and [`Waker`](std::task::Waker)s, all of which count in
//! the header's state word; the last reference to go frees it. Wakes schedule an idle task
//! exactly once; a wake while the task runs makes the lane requeue it when the poll returns; a
//! wake after completion does nothing.

mod flags;
mod harness;
mod header;
mod raw;
pub mod state;
mod task_ref;
mod waker;

#[cfg(test)]
mod tests;

use std::fmt;

pub use flags::TaskFlags;
pub use header::{Header, RegistryNode, ScopeNode};
pub use raw::TaskVTable;
pub use task_ref::TaskRef;

pub(crate) use harness::new_task;

/// Where a woken task goes. Implemented by a cheap, cloneable scheduler handle that is stored
/// inside every task it spawned.
///
/// # Safety
///
/// `schedule_local` is called through a borrow of the scheduler stored inside the task's own
/// allocation. It MUST NOT run `task`, drop it, or hand it to another thread before returning:
/// any of those can free the allocation while that borrow is live. Queueing it on the current
/// thread, or returning it, is fine.
pub unsafe trait Schedule: Clone + Send + Sync + 'static {
    /// Queues `task` if the current thread is running this scheduler's lane, else returns it.
    ///
    /// `self` is borrowed from inside a task allocation, so the implementation MUST NOT hand
    /// `task` to another thread (which could run and free it while the borrow is live).
    fn schedule_local(&self, task: Notified) -> Result<(), Notified>;

    /// Queues `task` from any thread. `self` is an owned clone, so the task may already be
    /// running (or freed) on another thread by the time this returns.
    fn schedule_remote(self, task: Notified);
}

/// A run-queue entry: owns one reference to a task in the SCHEDULED state.
pub struct Notified(TaskRef);

impl Notified {
    /// Polls the task once on the current thread, consuming this queue entry.
    ///
    /// A panic in the future completes the task, frees what it owns, and is then resumed here.
    pub fn run(self) {
        let Notified(task) = self;
        let task = task.into_raw();
        // SAFETY: the queue entry owned a reference to a SCHEDULED task, which the poll takes
        // over.
        unsafe { task.poll() }
    }

    /// The task's id.
    pub fn id(&self) -> u64 {
        self.0.id()
    }

    /// The task's header.
    pub fn header(&self) -> &Header {
        self.0.header()
    }

    /// A new counted reference to the task, independent of this queue entry.
    pub fn task_ref(&self) -> TaskRef {
        self.0.clone()
    }
}

impl fmt::Debug for Notified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Notified").field(&self.0).finish()
    }
}
