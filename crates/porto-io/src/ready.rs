//! [`Interest`] (what a registration waits for) and [`Ready`] (what the driver observed).

use std::fmt;
use std::num::NonZeroU8;
use std::ops::{BitAnd, BitOr, BitOrAssign, Sub};

/// The readiness a registration or a waiter is interested in. Never empty.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interest(NonZeroU8);

const INTEREST_READABLE: u8 = 0b001;
const INTEREST_WRITABLE: u8 = 0b010;
const INTEREST_PRIORITY: u8 = 0b100;

impl Interest {
    /// The resource can be read, or its read half was closed.
    pub const READABLE: Self = Self(NonZeroU8::new(INTEREST_READABLE).unwrap());
    /// The resource can be written, or its write half was closed.
    pub const WRITABLE: Self = Self(NonZeroU8::new(INTEREST_WRITABLE).unwrap());
    /// Out-of-band or priority data is available (`EPOLLPRI`).
    pub const PRIORITY: Self = Self(NonZeroU8::new(INTEREST_PRIORITY).unwrap());

    /// Both interests at once.
    #[must_use]
    pub const fn add(self, other: Self) -> Self {
        match NonZeroU8::new(self.0.get() | other.0.get()) {
            Some(bits) => Self(bits),
            None => self,
        }
    }

    /// `self` without `other`, or `None` when nothing is left.
    #[must_use]
    pub const fn remove(self, other: Self) -> Option<Self> {
        match NonZeroU8::new(self.0.get() & !other.0.get()) {
            Some(bits) => Some(Self(bits)),
            None => None,
        }
    }

    /// Whether this includes [`Interest::READABLE`].
    pub const fn is_readable(self) -> bool {
        self.0.get() & INTEREST_READABLE != 0
    }

    /// Whether this includes [`Interest::WRITABLE`].
    pub const fn is_writable(self) -> bool {
        self.0.get() & INTEREST_WRITABLE != 0
    }

    /// Whether this includes [`Interest::PRIORITY`].
    pub const fn is_priority(self) -> bool {
        self.0.get() & INTEREST_PRIORITY != 0
    }
}

impl BitOr for Interest {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        self.add(other)
    }
}

impl BitOrAssign for Interest {
    fn bitor_assign(&mut self, other: Self) {
        *self = self.add(other);
    }
}

impl fmt::Debug for Interest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = [
            (self.is_readable(), "READABLE"),
            (self.is_writable(), "WRITABLE"),
            (self.is_priority(), "PRIORITY"),
        ];
        write_flags(f, &names)
    }
}

/// Readiness observed by the driver for one registration.
///
/// The closed bits are terminal: [`ScheduledIo::clear_readiness`] never clears them.
///
/// [`ScheduledIo::clear_readiness`]: crate::ScheduledIo::clear_readiness
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Ready(u8);

impl Ready {
    /// Nothing is ready.
    pub const EMPTY: Self = Self(0);
    /// A read would not block.
    pub const READABLE: Self = Self(0b00_0001);
    /// A write would not block.
    pub const WRITABLE: Self = Self(0b00_0010);
    /// The peer closed its write half: reads return EOF from now on.
    pub const READ_CLOSED: Self = Self(0b00_0100);
    /// The write half is closed: writes fail from now on.
    pub const WRITE_CLOSED: Self = Self(0b00_1000);
    /// An error is pending on the resource (`SO_ERROR`).
    pub const ERROR: Self = Self(0b01_0000);
    /// Priority data is available.
    pub const PRIORITY: Self = Self(0b10_0000);
    /// Every readiness bit.
    pub const ALL: Self = Self(0b11_1111);
    /// What a waiter observes once its registration is closed or deregistered.
    pub const CLOSED: Self = Self(Self::READ_CLOSED.0 | Self::WRITE_CLOSED.0 | Self::ERROR.0);

    /// The bits a waiter with `interest` is woken for. [`Ready::ERROR`] wakes every waiter.
    pub const fn from_interest(interest: Interest) -> Self {
        let mut bits = Self::ERROR.0;
        if interest.is_readable() {
            bits |= Self::READABLE.0 | Self::READ_CLOSED.0;
        }
        if interest.is_writable() {
            bits |= Self::WRITABLE.0 | Self::WRITE_CLOSED.0;
        }
        if interest.is_priority() {
            bits |= Self::PRIORITY.0 | Self::READ_CLOSED.0;
        }
        Self(bits)
    }

    /// Keeps the known bits of `bits` and drops the rest.
    pub(crate) const fn from_bits_truncate(bits: usize) -> Self {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "masked to the six readiness bits"
        )]
        Self((bits & Self::ALL.0 as usize) as u8)
    }

    /// The raw bits, for packing.
    pub(crate) const fn bits(self) -> usize {
        self.0 as usize
    }

    /// Whether no bit is set.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Whether every bit of `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Whether any bit of `other` is set.
    pub const fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    /// Readable or read-closed: a read would not block.
    pub const fn is_readable(self) -> bool {
        self.intersects(Self(Self::READABLE.0 | Self::READ_CLOSED.0))
    }

    /// Writable or write-closed: a write would not block.
    pub const fn is_writable(self) -> bool {
        self.intersects(Self(Self::WRITABLE.0 | Self::WRITE_CLOSED.0))
    }

    /// Whether [`Ready::READ_CLOSED`] is set.
    pub const fn is_read_closed(self) -> bool {
        self.intersects(Self::READ_CLOSED)
    }

    /// Whether [`Ready::WRITE_CLOSED`] is set.
    pub const fn is_write_closed(self) -> bool {
        self.intersects(Self::WRITE_CLOSED)
    }

    /// Whether [`Ready::ERROR`] is set.
    pub const fn is_error(self) -> bool {
        self.intersects(Self::ERROR)
    }

    /// Whether [`Ready::PRIORITY`] is set.
    pub const fn is_priority(self) -> bool {
        self.intersects(Self::PRIORITY)
    }
}

impl BitOr for Ready {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl BitOrAssign for Ready {
    fn bitor_assign(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

impl BitAnd for Ready {
    type Output = Self;

    fn bitand(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }
}

impl Sub for Ready {
    type Output = Self;

    /// The bits of `self` that are not in `other`.
    fn sub(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }
}

impl fmt::Debug for Ready {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = [
            (self.intersects(Self::READABLE), "READABLE"),
            (self.intersects(Self::WRITABLE), "WRITABLE"),
            (self.is_read_closed(), "READ_CLOSED"),
            (self.is_write_closed(), "WRITE_CLOSED"),
            (self.is_error(), "ERROR"),
            (self.is_priority(), "PRIORITY"),
        ];
        write_flags(f, &names)
    }
}

fn write_flags(f: &mut fmt::Formatter<'_>, names: &[(bool, &str)]) -> fmt::Result {
    let mut first = true;
    for &(set, name) in names {
        if set {
            if !first {
                f.write_str(" | ")?;
            }
            f.write_str(name)?;
            first = false;
        }
    }
    if first {
        f.write_str("(empty)")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interest_add_and_remove() {
        let both = Interest::READABLE | Interest::WRITABLE;
        assert!(both.is_readable() && both.is_writable() && !both.is_priority());
        assert_eq!(both.remove(Interest::WRITABLE), Some(Interest::READABLE));
        assert_eq!(Interest::READABLE.remove(Interest::READABLE), None);
        assert_eq!(format!("{both:?}"), "READABLE | WRITABLE");
    }

    #[test]
    fn from_interest_includes_closed_and_error() {
        assert_eq!(
            Ready::from_interest(Interest::READABLE),
            Ready::READABLE | Ready::READ_CLOSED | Ready::ERROR
        );
        assert_eq!(
            Ready::from_interest(Interest::WRITABLE),
            Ready::WRITABLE | Ready::WRITE_CLOSED | Ready::ERROR
        );
        assert_eq!(
            Ready::from_interest(Interest::PRIORITY),
            Ready::PRIORITY | Ready::READ_CLOSED | Ready::ERROR
        );
    }

    #[test]
    fn ready_set_operations() {
        let ready = Ready::READABLE | Ready::READ_CLOSED;
        assert!(ready.is_readable() && ready.is_read_closed() && !ready.is_writable());
        assert!(ready.contains(Ready::READABLE));
        assert_eq!(ready - Ready::READABLE, Ready::READ_CLOSED);
        assert_eq!(ready & Ready::READABLE, Ready::READABLE);
        assert_eq!(Ready::from_bits_truncate(usize::MAX), Ready::ALL);
        assert_eq!(format!("{:?}", Ready::EMPTY), "(empty)");
        assert_eq!(
            format!("{:?}", Ready::CLOSED),
            "READ_CLOSED | WRITE_CLOSED | ERROR"
        );
    }
}
