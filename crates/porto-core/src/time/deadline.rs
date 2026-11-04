//! The runtime clock: a monotonic instant in eight bytes.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// The clock's origin: the first time any deadline was read in this process.
static ORIGIN: OnceLock<Instant> = OnceLock::new();

/// A point on the runtime's monotonic clock, in nanoseconds since the clock's origin (the first
/// use of the clock in this process).
///
/// It is eight bytes with a niche, so `Option<Deadline>` and enums that carry one stay small
/// (a Python trap stores one inline). The clock saturates at about 584 years after its origin,
/// which [`Deadline::NEVER`] stands for.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Deadline(NonZeroU64);

impl Deadline {
    /// A deadline that is never reached.
    pub const NEVER: Self = Self(NonZeroU64::MAX);

    /// The current time.
    pub fn now() -> Self {
        let origin = *ORIGIN.get_or_init(Instant::now);
        Self::from_nanos(saturating_nanos(origin.elapsed()))
    }

    /// The time `delay` from now, saturating at [`Deadline::NEVER`].
    pub fn after(delay: Duration) -> Self {
        Self::now().saturating_add(delay)
    }

    /// The deadline `nanos` nanoseconds after the clock's origin. Zero is rounded up to one
    /// nanosecond (the niche), which only ever makes a deadline later, never earlier.
    pub const fn from_nanos(nanos: u64) -> Self {
        match NonZeroU64::new(nanos) {
            Some(nanos) => Self(nanos),
            None => Self(NonZeroU64::MIN),
        }
    }

    /// Nanoseconds since the clock's origin.
    pub const fn as_nanos(self) -> u64 {
        self.0.get()
    }

    /// Seconds since the clock's origin.
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
