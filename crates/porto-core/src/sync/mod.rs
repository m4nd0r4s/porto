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
//! And:
//!
//! - [`blocking_wait`]: waits from a thread that does not run a lane, the backing of every
//!   `blocking_*` method. On a lane thread it returns [`BlockingOnLane`] instead of stalling
//!   the lane.
//! - [`mpsc`]: bounded and unbounded multi-producer multi-consumer channels.
//!
//! The grant-versus-cancel race is model-checked with [loom](https://docs.rs/loom):
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sync
//! ```

mod blocking;
mod event;
pub mod mpsc;
mod notify;
mod primitives;
mod wait_queue;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, not(loom)))]
mod event_notify_tests;

#[cfg(all(test, loom))]
mod loom_tests;

#[cfg(all(test, loom))]
mod loom_event_notify_tests;

pub use blocking::{BlockingOnLane, blocking_wait};
pub use event::{Event, EventWait};
pub use notify::{Notified, Notify};
pub use wait_queue::{Locked, Wait, WaitNode, WaitQueue, WaitState};
