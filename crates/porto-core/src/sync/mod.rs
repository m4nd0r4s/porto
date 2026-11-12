//! Synchronization primitives (`16-sync-primitives-and-channels.md`).
//!
//! M1 starts with the shared waiting infrastructure every primitive builds on:
//!
//! - [`WaitQueue`]: a FIFO (fair) queue of intrusive [`WaitNode`]s plus the primitive's state,
//!   both behind one small mutex. Granting wakes waiters in arrival order.
//! - Cancellation: dropping a waiting future removes its node. If the waiter had **already been
//!   granted** (grant and cancel raced), the grant is passed on through
//!   [`WaitState::pass_grant`], so a lock or permit is never lost.
//!
//! On top of it:
//!
//! - [`Event`]: a flag that, once set, releases every waiter; not clearable.
//! - [`Notify`]: `notify_one` wakes one waiter or stores one permit, `notify_all` wakes the
//!   current waiters.
//!
//! - [`Mutex`]: an async lock that owns its value; fair FIFO handoff, non-reentrant.
//! - [`Semaphore`]: permits acquired and released several at a time, granted in FIFO order.
//! - [`RwLock`]: a writer-preferring read/write lock that owns its value.
//! - [`Condvar`]: waits for a notification with a [`Mutex`] released.
//! - [`Barrier`]: waits until a set number of tasks arrived; reusable.
//!
//! And [`JoinWaiters`]: the one-word intrusive list of a task's joiners, built on the same
//! nodes and closed once when the task finishes.
//!
//! And:
//!
//! - [`blocking_wait`]: waits from a thread that does not run a lane, the backing of every
//!   `blocking_*` method. On a lane thread it returns [`BlockingOnLane`] instead of stalling
//!   the lane.
//! - [`mpsc`]: bounded and unbounded multi-producer multi-consumer channels.
//! - [`oneshot`]: a channel for one value.
//! - [`broadcast`]: every receiver gets every message; a receiver that falls behind is told how
//!   many it missed.
//! - [`watch`]: a channel for the latest value, with change notification.
//! - [`SetOnce`]: an `Arc` set at most once and read with one atomic load.
//!
//! The grant-versus-cancel race is model-checked with [loom](https://docs.rs/loom):
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sync
//! ```

mod barrier;
mod blocking;
pub mod broadcast;
mod condvar;
mod event;
mod join_waiters;
pub mod mpsc;
mod mutex;
mod notify;
pub mod oneshot;
pub(crate) mod primitives;
mod rwlock;
mod semaphore;
mod send_error;
mod set_once;
mod wait_queue;
pub mod watch;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, not(loom)))]
mod event_notify_tests;

#[cfg(all(test, not(loom)))]
mod mutex_tests;

#[cfg(all(test, not(loom)))]
mod oneshot_tests;

#[cfg(all(test, not(loom)))]
mod watch_tests;

#[cfg(all(test, not(loom)))]
mod broadcast_tests;

#[cfg(test)]
mod join_waiters_tests;

#[cfg(all(test, loom))]
mod loom_tests;

#[cfg(all(test, loom))]
mod loom_event_notify_tests;

#[cfg(all(test, loom))]
mod loom_mutex_tests;

#[cfg(all(test, loom))]
mod loom_oneshot_tests;

#[cfg(all(test, loom))]
mod loom_watch_tests;

#[cfg(all(test, loom))]
mod loom_broadcast_tests;

#[cfg(all(test, not(loom)))]
mod semaphore_tests;

#[cfg(all(test, not(loom)))]
mod rwlock_tests;

#[cfg(all(test, not(loom)))]
mod condvar_barrier_tests;

#[cfg(all(test, loom))]
mod loom_semaphore_rwlock_tests;

#[cfg(all(test, loom))]
mod loom_condvar_barrier_tests;

pub use barrier::{Barrier, BarrierWait, BarrierWaitResult};
pub use blocking::{BlockingOnLane, blocking_wait};
pub use condvar::{Condvar, CondvarWait};
pub use event::{Event, EventWait};
pub use join_waiters::JoinWaiters;
pub use mutex::{Mutex, MutexGuard, MutexLock};
pub use notify::{Notified, Notify};
pub use rwlock::{RwLock, RwLockRead, RwLockReadGuard, RwLockWrite, RwLockWriteGuard};
pub use semaphore::{MAX_PERMITS, Semaphore, SemaphoreAcquire, SemaphorePermit};
pub use set_once::SetOnce;
pub use wait_queue::{Locked, Wait, WaitNode, WaitQueue, WaitState};
