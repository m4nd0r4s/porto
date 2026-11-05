//! The runtime clock: where [`Deadline`]s come from.
//!
//! Every runtime owns one clock (`14-time-and-timers.md` §1). By default it is a
//! [`MonotonicClock`] whose epoch is the moment the runtime was built, so `current_time()` is
//! about zero at start. Deterministic mode (`24-deterministic-testing.md` §3) installs a
//! virtual clock instead, through [`Builder::clock`]: every deadline, timer and park of that
//! runtime then follows it.
//!
//! [`Builder::clock`]: crate::Builder::clock

use std::fmt;
use std::thread;
use std::time::Instant;

use super::Deadline;

/// A source of time for one runtime. Install it with [`Builder::clock`].
///
/// Deadlines from one clock only mean something to the runtime that owns it: a deadline is
/// nanoseconds since that clock's epoch.
///
/// [`Builder::clock`]: crate::Builder::clock
pub trait Clock: Send + Sync + 'static {
    /// The current time, since this clock's epoch. MUST never go backwards.
    fn now(&self) -> Deadline;

    /// Blocks the lane thread until `deadline`, or until it is unparked
    /// ([`std::thread::Thread::unpark`]), whichever comes first. Returning early is allowed:
    /// the lane checks its timers and parks again for the rest.
    ///
    /// The default parks for the wall-clock time left. A virtual clock that autojumps advances
    /// itself to `deadline` and returns: the lane only parks when nothing else can run.
    fn park_until(&self, deadline: Deadline) {
        let timeout = deadline.saturating_duration_since(self.now());
        if !timeout.is_zero() {
            thread::park_timeout(timeout);
        }
    }
}

/// The real clock: [`Instant`], counted from an epoch.
#[derive(Debug, Clone, Copy)]
pub struct MonotonicClock {
    epoch: Instant,
}

impl MonotonicClock {
    /// A clock whose epoch is now: it reads (about) zero right away.
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    /// The instant this clock reads zero.
    pub fn epoch(&self) -> Instant {
        self.epoch
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Deadline {
        Deadline::from_duration(self.epoch.elapsed())
    }
}

/// The clock a runtime owns: the monotonic clock without a virtual call, or a custom one.
pub(crate) enum RuntimeClock {
    Monotonic(MonotonicClock),
    Custom(Box<dyn Clock>),
}

impl RuntimeClock {
    pub(crate) fn now(&self) -> Deadline {
        match self {
            Self::Monotonic(clock) => clock.now(),
            Self::Custom(clock) => clock.now(),
        }
    }

    pub(crate) fn park_until(&self, deadline: Deadline) {
        match self {
            Self::Monotonic(clock) => clock.park_until(deadline),
            Self::Custom(clock) => clock.park_until(deadline),
        }
    }
}

impl fmt::Debug for RuntimeClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Monotonic(clock) => clock.fmt(f),
            Self::Custom(_) => f.write_str("Custom"),
        }
    }
}
