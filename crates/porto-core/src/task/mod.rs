//! Tasks: a refcounted heap allocation with a [`Header`] first, polled through a vtable.
//!
//! See `11-task-model-and-trap-protocol.md` §1 and §3. A task is referenced by run-queue
//! entries ([`Notified`]), [`TaskRef`]s and [`Waker`](std::task::Waker)s, all of which count in
//! the header's state word; the last reference to go frees it. Wakes schedule an idle task
//! exactly once; a wake while the task runs makes the lane requeue it when the poll returns; a
//! wake after completion does nothing. A [`JoinHandle`] waits for the output.
//!
//! The loom models run with `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release task`.

mod body;
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
mod body_tests;

#[cfg(all(test, not(loom)))]
mod proptests;

#[cfg(all(test, not(loom)))]
mod scope_tests;

#[cfg(all(test, loom))]
mod loom_tests;

use std::fmt;
use std::mem;
use std::ptr::NonNull;

pub use body::{TaskBody, TaskBodyRef};
pub use flags::TaskFlags;
pub use header::Header;
pub use join::{JoinError, JoinHandle};
pub use raw::TaskVTable;
pub use task_ref::TaskRef;

pub(crate) use body::{new_body_task, new_body_task_in};
#[cfg(all(test, not(loom)))]
pub(crate) use harness::new_task;
pub(crate) use harness::{new_joinable_task, new_joinable_task_in};
use raw::RawTask;

use crate::registry::BeforeFree;

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

    /// Called once per completed task, on the thread that completed it: after COMPLETE is set,
    /// the joiner woken and the task's scope notified, before the run reference is released
    /// (§3 "poll end Ready"). `self` is borrowed from inside the task, as for
    /// `schedule_local`.
    ///
    /// A task freed without completing (idle when its last reference went) never calls it.
    /// Scopes do not hear of task ends through this hook: the task notifies its scope itself,
    /// on every end, freed included (see [`crate::scope::TaskScope`]).
    fn task_completed(&self, task: &Header) {
        let _ = task;
    }

    /// The runtime this scheduler is a handle to, if it is one: what [`TaskBody::release`]
    /// hands to a body that must release something on its lane.
    fn handle(&self) -> Option<&crate::Handle> {
        None
    }

    /// The registry this scheduler's tasks are linked into, with whether the calling thread is
    /// the lane that owns it (`crate::registry`); `None` for a scheduler with no registry,
    /// whose tasks are never linked.
    fn registration(&self) -> Option<Registration<'_>> {
        None
    }

    /// [`Schedule::registration`]'s registry, for a caller that knows it is on the lane thread
    /// (a lane polling its own queue) and does not need to look that up.
    fn registry(&self) -> Option<&crate::registry::TaskRegistry> {
        self.registration()
            .map(|registration| registration.registry)
    }

    /// Called after a task freed off its lane was handed to the registry's lane, which frees it
    /// at its next maintenance tick: the scheduler wakes the lane for it. `self` is an owned
    /// clone.
    fn zombie_pushed(&self) {}
}

/// Where a scheduler registers its tasks, see [`Schedule::registration`].
pub struct Registration<'a> {
    /// The registry of the lane the task belongs to.
    pub registry: &'a crate::registry::TaskRegistry,
    /// Whether the calling thread is that lane's thread.
    pub on_lane: bool,
}

/// A task completed on this thread: unlinks it from its registry if it is linked, which the
/// scheduler's lane does for a task it completes itself.
#[inline]
fn unregister_completed<S: Schedule>(header: &Header, scheduler: &S, on_lane: bool) {
    if !header.registry.is_linked_relaxed() {
        return;
    }
    if on_lane {
        // The lane polling its own queue (the hot path of every registered task): the caller's
        // word for it, no lookup.
        if let Some(registry) = scheduler.registry() {
            registry.completed(header, true);
        }
    } else {
        unregister_completed_elsewhere(header, scheduler);
    }
}

#[cold]
#[inline(never)]
fn unregister_completed_elsewhere<S: Schedule>(header: &Header, scheduler: &S) {
    if let Some(registration) = scheduler.registration() {
        registration
            .registry
            .completed(header, registration.on_lane);
    }
}

/// The last reference to the task at `ptr` is gone: takes it off its registry before it is
/// freed. Returns whether to go on freeing it: `false` means the registry handed the task to its
/// lane, which frees it later, and the caller must not touch it again.
///
/// # Safety
/// `ptr` is a task header with the provenance of its allocation, whose refcount just reached zero,
/// with scheduler `scheduler` (the one in the allocation). A pointer, not a reference: the task may
/// be freed by its lane while this runs, which a reference argument must not see.
#[inline]
unsafe fn unregister_before_free<S: Schedule>(ptr: NonNull<Header>, scheduler: *const S) -> bool {
    // SAFETY: the caller's contract.
    if !unsafe { ptr.as_ref() }.registry.is_linked() {
        return true;
    }
    // SAFETY: as above.
    unsafe { unregister_before_free_slow(ptr, scheduler) }
}

#[cold]
#[inline(never)]
unsafe fn unregister_before_free_slow<S: Schedule>(
    ptr: NonNull<Header>,
    scheduler: *const S,
) -> bool {
    // An owned handle: once the task is handed to the lane it may be freed at once, and the
    // scheduler inside it with it.
    // SAFETY: the caller's contract: the allocation is ours, so the scheduler is alive.
    let scheduler = unsafe { &*scheduler }.clone();
    let Some(registration) = scheduler.registration() else {
        return true;
    };
    // SAFETY: the caller's contract: the last reference is ours.
    match unsafe { registration.registry.before_free(ptr, registration.on_lane) } {
        BeforeFree::Proceed => true,
        BeforeFree::Deferred => {
            scheduler.zombie_pushed();
            false
        }
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
        unsafe { task.poll(false) }
    }

    /// [`Notified::run`] on the lane thread that owns the task's registry, which spares the
    /// completion a lookup of the lane (`crate::registry`).
    ///
    /// # Safety
    /// The calling thread runs the lane the task belongs to, polling its own queues.
    pub(crate) unsafe fn run_on_lane(self) {
        let task = self.into_raw();
        // SAFETY: as for `run`, and the caller's contract for the flag.
        unsafe { task.poll(true) }
    }

    /// The task's id.
    pub fn id(&self) -> u64 {
        self.header().id()
    }

    /// The task's header.
    pub fn header(&self) -> &Header {
        self.0.header()
    }

    /// The task's pointer, with the provenance of its allocation: what the registry links.
    pub(crate) fn ptr(&self) -> NonNull<Header> {
        self.0.as_ptr()
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
        let cancel = || unsafe { task.poll(false) };
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

/// Frees a task the registry handed back after it was freed off its lane (the last reference
/// went on another thread): the lane, which owns the registry's list, calls it once the task is
/// unlinked.
///
/// # Safety
/// `ptr` came from [`crate::registry::TaskRegistry::take_zombies`], whose refcount is zero.
pub(crate) unsafe fn free_zombie(ptr: NonNull<Header>) {
    // SAFETY: the caller's contract; the task is unlinked now, so `dealloc` frees it.
    unsafe { RawTask::from_raw(ptr).dealloc() }
}
