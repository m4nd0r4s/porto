//! Tasks: a refcounted heap allocation with a [`Header`] first, polled through a vtable.
//!
//! See `11-task-model-and-trap-protocol.md` §1 and §3. A task is referenced by run-queue
//! entries ([`Notified`]), [`TaskRef`]s and [`Waker`](std::task::Waker)s, all of which count in
//! the header's state word; the last reference to go frees it. Wakes schedule an idle task
//! exactly once; a wake while the task runs makes the lane requeue it when the poll returns; a
//! wake after completion does nothing. A [`JoinHandle`] waits for the output.
//!
//! The loom models run with `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release task`.

mod flags;
mod harness;
mod header;
mod join;
mod raw;
pub mod state;
mod sync;
mod task_ref;
mod waker;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, not(loom)))]
mod join_tests;

#[cfg(all(test, not(loom)))]
mod proptests;

#[cfg(all(test, loom))]
mod loom_tests;

use std::fmt;
use std::mem;

pub use flags::TaskFlags;
pub use header::{Header, RegistryNode, ScopeNode};
pub use join::{JoinError, JoinHandle};
pub use raw::TaskVTable;
pub use task_ref::TaskRef;

pub(crate) use harness::new_joinable_task;
#[cfg(all(test, not(loom)))]
pub(crate) use harness::new_task;
use raw::RawTask;

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
    ///
    /// A scheduler that is shut down drops `task`, which cancels it (see [`Notified`]).
    fn schedule_remote(self, task: Notified);

    /// Called once per task, on the thread that completed it: after COMPLETE is set and the
    /// joiner woken, before the run reference is released (§3 "poll end Ready: notify
    /// scope"). The scope tree hooks in here (porto-szb.93). `self` is borrowed from inside
    /// the task, as for `schedule_local`.
    fn task_completed(&self, task: &Header) {
        let _ = task;
    }
}

/// A run-queue entry: owns one reference to a task in the SCHEDULED state.
///
/// Dropping an entry instead of running it cancels the task: the drop polls it with CANCEL_REQ
/// set, which drops the future, completes the task with [`JoinError::is_cancelled`] and wakes
/// the joiner. That is what a wake after shutdown does, since a closed scheduler drops what it
/// is handed (§3.2).
pub struct Notified(RawTask);

// SAFETY: a queue entry owns a reference to a task built from a `Send` future and output and a
// `Send + Sync` scheduler; running it on another thread is what queues are for.
unsafe impl Send for Notified {}
// SAFETY: `&Notified` only reaches the header, see `TaskRef`'s `Sync`.
unsafe impl Sync for Notified {}

impl Notified {
    /// Polls the task once on the current thread, consuming this queue entry.
    ///
    /// A panic in the future completes the task and frees what it owns. If a join handle is
    /// waiting, it gets the panic as a [`JoinError`]; if not, the panic is resumed here.
    pub fn run(self) {
        let task = self.into_raw();
        // SAFETY: the queue entry owned a reference to a SCHEDULED task, which the poll takes
        // over.
        unsafe { task.poll() }
    }

    /// The task's id.
    pub fn id(&self) -> u64 {
        self.header().id()
    }

    /// The task's header.
    pub fn header(&self) -> &Header {
        self.0.header()
    }

    /// A new counted reference to the task, independent of this queue entry.
    pub fn task_ref(&self) -> TaskRef {
        self.header().state.ref_inc();
        // SAFETY: the increment above created the reference the `TaskRef` takes over.
        unsafe { TaskRef::from_owned(self.0) }
    }

    fn into_raw(self) -> RawTask {
        let task = self.0;
        mem::forget(self);
        task
    }
}

impl Drop for Notified {
    fn drop(&mut self) {
        let task = self.0;
        let action = task.header().state.transition_to_cancel_requested();
        debug_assert_eq!(
            action,
            state::WakeAction::DoNothing,
            "a queued task is SCHEDULED"
        );
        // SAFETY: the entry owns a reference to a SCHEDULED task; the poll takes it over and,
        // seeing CANCEL_REQ, completes the task as cancelled without polling the future.
        let cancel = || unsafe { task.poll() };
        if std::thread::panicking() {
            // A second panic while unwinding aborts the process (a queue dropped during a
            // panic): the cancellation's own panic (a panicking future or waker drop) is
            // dropped instead. The task is still completed and freed.
            drop(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                cancel,
            )));
        } else {
            cancel();
        }
    }
}

impl fmt::Debug for Notified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header = self.header();
        f.debug_struct("Notified")
            .field("id", &header.id())
            .field("type", &header.type_name())
            .field("state", header.state())
            .finish()
    }
}
