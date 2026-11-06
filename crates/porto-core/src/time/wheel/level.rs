//! One level of the wheel: 64 slots, each a FIFO list of entries with a lower bound of their
//! ticks, and a bitmap of the occupied ones.

use std::ptr::NonNull;

use super::entry::{TimerEntry, UNLINKED};
use super::{LEVELS, SLOT_BITS, SLOTS};

/// A doubly linked FIFO list of entries.
#[derive(Clone, Copy)]
pub(super) struct Slot {
    head: Option<NonNull<TimerEntry>>,
    tail: Option<NonNull<TimerEntry>>,
    /// At most the earliest `deadline_tick` in the list (`u64::MAX` when empty): exact until
    /// an entry is removed, then possibly lower. It bounds the next-deadline scan to O(1)
    /// (porto-szb.95); a stale value only wakes the lane early, to cascade the slot, which
    /// makes it exact again.
    min_tick: u64,
}

impl Slot {
    const EMPTY: Self = Self {
        head: None,
        tail: None,
        min_tick: u64::MAX,
    };
}

pub(super) struct Level {
    /// Bit `i` is set when slot `i` is non-empty.
    occupied: u64,
    slots: [Slot; SLOTS],
}

impl Level {
    pub(super) const fn new() -> Self {
        Self {
            occupied: 0,
            slots: [Slot::EMPTY; SLOTS],
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    /// The first occupied slot at or after `now`'s slot on this level, wrapping around, and
    /// the tick it starts at. Only the top level wraps: its slots form a ring that clamped
    /// entries go around (`Wheel::insert`).
    pub(super) fn next_slot(&self, level: usize, now: u64) -> Option<(usize, u64)> {
        if self.occupied == 0 {
            return None;
        }
        let shift = SLOT_BITS * level;
        let top = level == LEVELS - 1;
        // The top level is a ring: a slot at or before `now`'s holds entries one rotation
        // ahead, `now`'s own slot last of all (an entry never shares `now`'s slot on a level
        // above 0 without wrapping). Below the top, no slot is behind `now`, and level 0 may
        // hold entries due right now in `now`'s slot.
        let first = (slot_of(now, level) + usize::from(top)) % SLOTS;
        let distance = self
            .occupied
            .rotate_right(u32::try_from(first).unwrap_or(0));
        let slot = (first + distance.trailing_zeros() as usize) % SLOTS;
        let level_range = 1u64 << (shift + SLOT_BITS);
        let level_start = now & !(level_range - 1);
        let mut start = level_start + ((slot as u64) << shift);
        if top && start <= now {
            start += level_range;
        }
        debug_assert!(
            start >= now,
            "a slot behind the wheel's time on level {level}"
        );
        Some((slot, start))
    }

    /// Appends `entry` to `slot`.
    ///
    /// # Safety
    /// `entry` is live and unlinked, and the caller owns the wheel (exclusive access to every
    /// entry it links).
    pub(super) unsafe fn push_back(
        &mut self,
        level: usize,
        slot: usize,
        entry: NonNull<TimerEntry>,
    ) {
        let list = &mut self.slots[slot];
        // SAFETY: the caller owns the wheel and `entry`; the tail is linked in it.
        unsafe {
            let state = TimerEntry::state(entry);
            debug_assert_eq!((*state).location, UNLINKED);
            (*state).prev = list.tail;
            (*state).next = None;
            (*state).location = location(level, slot);
            list.min_tick = list.min_tick.min((*state).deadline_tick);
            match list.tail {
                Some(tail) => (*TimerEntry::state(tail)).next = Some(entry),
                None => list.head = Some(entry),
            }
        }
        list.tail = Some(entry);
        self.occupied |= 1 << slot;
    }

    /// Unlinks `entry` from `slot`.
    ///
    /// # Safety
    /// `entry` is linked in this level's `slot`, and the caller owns the wheel.
    pub(super) unsafe fn remove(&mut self, slot: usize, entry: NonNull<TimerEntry>) {
        let list = &mut self.slots[slot];
        // SAFETY: the caller owns the wheel; `entry` and its neighbors are linked in it.
        unsafe {
            let state = TimerEntry::state(entry);
            let (prev, next) = ((*state).prev, (*state).next);
            match prev {
                Some(prev) => (*TimerEntry::state(prev)).next = next,
                None => list.head = next,
            }
            match next {
                Some(next) => (*TimerEntry::state(next)).prev = prev,
                None => list.tail = prev,
            }
            (*state).prev = None;
            (*state).next = None;
            (*state).location = UNLINKED;
        }
        if list.head.is_none() {
            list.min_tick = u64::MAX;
            self.occupied &= !(1 << slot);
        }
    }

    pub(super) fn head(&self, slot: usize) -> Option<NonNull<TimerEntry>> {
        self.slots[slot].head
    }

    /// A lower bound of the ticks of `slot`'s entries, in O(1): their earliest tick unless an
    /// entry was removed since the slot was last empty. `u64::MAX` for an empty slot.
    pub(super) fn min_tick(&self, slot: usize) -> u64 {
        self.slots[slot].min_tick
    }

    /// Takes the whole list of `slot`, leaving it empty. The entries keep their links to each
    /// other (walk them with `next`) but are marked unlinked as they are visited by the caller.
    pub(super) fn take(&mut self, slot: usize) -> Option<NonNull<TimerEntry>> {
        self.occupied &= !(1 << slot);
        let list = std::mem::replace(&mut self.slots[slot], Slot::EMPTY);
        list.head
    }

    /// The first non-empty slot, from slot 0: for draining in no particular order.
    pub(super) fn any_slot(&self) -> Option<usize> {
        (self.occupied != 0).then(|| self.occupied.trailing_zeros() as usize)
    }
}

/// The slot `tick` falls in on `level`.
pub(super) fn slot_of(tick: u64, level: usize) -> usize {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "masked to 6 bits, the cast cannot truncate"
    )]
    let slot = ((tick >> (SLOT_BITS * level)) & (SLOTS as u64 - 1)) as usize;
    slot
}

fn location(level: usize, slot: usize) -> u16 {
    debug_assert!(
        level < LEVELS && slot < SLOTS,
        "slot {slot} on level {level}"
    );
    // At most LEVELS * SLOTS = 384, well below UNLINKED.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "level < 6 and slot < 64: at most 383"
    )]
    let location = (level * SLOTS + slot) as u16;
    location
}

/// The (level, slot) a linked entry's `location` stands for.
pub(super) fn split_location(location: u16) -> (usize, usize) {
    let location = usize::from(location);
    (location / SLOTS, location % SLOTS)
}
