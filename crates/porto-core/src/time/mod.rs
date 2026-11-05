//! Time: the runtime clock ([`Clock`], [`Deadline`]) and the [`Sleep`] future.
//!
//! M0 spike of `14-time-and-timers.md`: each lane keeps a min-heap of deadlines, fired before
//! parking and every `event_interval` tasks, and parks with a timeout until the earliest one.
//! A timer never fires early: an entry fires only once the clock has reached its deadline. The
//! hierarchical wheel with intrusive entries arrives in M1.

mod clock;
mod deadline;
mod sleep;

#[cfg(test)]
mod tests;

pub(crate) use clock::RuntimeClock;
pub use clock::{Clock, MonotonicClock};
pub use deadline::Deadline;
pub use sleep::{Sleep, sleep_until};
