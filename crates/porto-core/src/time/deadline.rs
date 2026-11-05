//! A point on the runtime clock, in eight bytes.

use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use crate::sched::current_time;

/// A point on a runtime's clock, in nanoseconds since the clock's epoch (by default the moment
/// the runtime was built, see [`Clock`]).
///
/// It is eight bytes with a niche, so `Option<Deadline>` and enums that carry one stay small
/// (a Python trap stores one inline). The clock saturates at about 584 years after its epoch,
/// which [`Deadline::NEVER`] stands for. Deadlines of different runtimes are not comparable.
///
/// [`Clock`]: super::Clock
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Deadline(NonZeroU64);

impl Deadline {
    /// A deadline that is never reached.
    pub const NEVER: Self = Self(NonZeroU64::MAX);

    /// The current time on the clock of the runtime running on this thread.
    ///
    /// # Panics
    /// Outside a runtime (a task, or inside `Runtime::block_on`). Use [`Deadline::try_now`],
    /// or [`Handle::now`](crate::Handle::now) from other threads.
    pub fn now() -> Self {
        Self::try_now().expect(
            "porto_core::time::Deadline::now called outside a runtime: the clock belongs to a \
             runtime, so read it from a task or inside Runtime::block_on, or use Handle::now",
        )
    }

    /// The current time on the clock of the runtime running on this thread, if any.
    pub fn try_now() -> Option<Self> {
        current_time()
    }

    /// The time `delay` from now, saturating at [`Deadline::NEVER`].
    ///
    /// # Panics
    /// Outside a runtime, like [`Deadline::now`].
    pub fn after(delay: Duration) -> Self {
        Self::now().saturating_add(delay)
    }

    /// The deadline `nanos` nanoseconds after the clock's epoch. Zero is rounded up to one
    /// nanosecond (the niche), which only ever makes a deadline later, never earlier.
    pub const fn from_nanos(nanos: u64) -> Self {
        match NonZeroU64::new(nanos) {
            Some(nanos) => Self(nanos),
            None => Self(NonZeroU64::MIN),
        }
    }

    /// The deadline `since_epoch` after the clock's epoch, saturating at [`Deadline::NEVER`].
    pub fn from_duration(since_epoch: Duration) -> Self {
        Self::from_nanos(saturating_nanos(since_epoch))
    }

    /// Nanoseconds since the clock's epoch.
    pub const fn as_nanos(self) -> u64 {
        self.0.get()
    }

    /// Seconds since the clock's epoch: what `porto.current_time()` returns.
    #[allow(
        clippy::cast_precision_loss,
        reason = "f64 seconds keep nanosecond precision for 104 days, microseconds for 285 years"
    )]
    pub fn as_secs_f64(self) -> f64 {
        self.as_nanos() as f64 / 1e9
    }

    /// `self + delay`, saturating at [`Deadline::NEVER`].
    pub fn saturating_add(self, delay: Duration) -> Self {
        Self::from_nanos(self.as_nanos().saturating_add(saturating_nanos(delay)))
    }

    /// How long from `earlier` to `self`; zero if `earlier` is later.
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        Duration::from_nanos(self.as_nanos().saturating_sub(earlier.as_nanos()))
    }
}

fn saturating_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

impl fmt::Debug for Deadline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::NEVER {
            f.write_str("Deadline(never)")
        } else {
            write!(f, "Deadline({:?})", Duration::from_nanos(self.as_nanos()))
        }
    }
}
