//! Time: the runtime clock ([`Clock`], [`Deadline`]) and the [`Sleep`] future.
//!
//! `14-time-and-timers.md`: each lane keeps a shard of the hierarchical timer wheel (6 levels
//! of 64 slots, 1 ms per tick by default, see `wheel`), fired before parking and every
//! `event_interval` tasks; the lane parks no longer than until the earliest timer. A timer
//! never fires early: its deadline is rounded up to a tick, and a tick fires once the clock
//! has passed it. A [`Sleep`] embeds its wheel entry and unlinks it when dropped.

mod clock;
mod deadline;
mod sleep;
pub(crate) mod wheel;

#[cfg(all(test, not(loom)))]
mod sleep_tests;
#[cfg(all(test, not(loom)))]
mod tests;

pub(crate) use clock::RuntimeClock;
pub use clock::{Clock, MonotonicClock};
pub use deadline::Deadline;
pub use sleep::{Sleep, sleep_until};
