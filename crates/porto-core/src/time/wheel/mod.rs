//! The hierarchical timer wheel (`14-time-and-timers.md` §2): 6 levels of 64 slots.
//!
//! The wheel counts in ticks (a lane's shard turns deadlines into ticks at its resolution, 1 ms
//! by default). A slot on level `L` covers `64^L` ticks, so the wheel spans `64^6` ticks, about
//! 2.2 years at 1 ms. An entry goes on the level of the highest 6-bit group in which its tick
//! differs from the wheel's time (`elapsed`); when the wheel reaches a slot above level 0, the
//! slot's entries cascade down to finer levels, and an entry in a level-0 slot fires.
//!
//! Entries are intrusive [`TimerEntry`] nodes in FIFO slot lists: insert and cancel are O(1),
//! and entries with the same tick fire in the order they were inserted. A tick beyond the span
//! is clamped into the top level, whose slots form a ring, and re-armed each time the wheel
//! reaches it, until the real tick is within reach. An entry never fires before its tick.
//!
//! The wheel is plain data: whoever owns it (for a lane, the holder of the shard lock) has
//! exclusive access to the entries it links.

mod entry;
mod level;

#[cfg(test)]
mod tests;

use std::fmt;
use std::ptr::NonNull;

pub(crate) use entry::TimerEntry;
use level::{Level, slot_of, split_location};

/// Levels in the wheel.
pub(crate) const LEVELS: usize = 6;
/// Bits of a tick per level: 64 slots.
const SLOT_BITS: usize = 6;
const SLOTS: usize = 1 << SLOT_BITS;
/// Ticks the wheel spans: an entry further away is clamped and re-armed.
pub(crate) const MAX_SPAN: u64 = 1 << (SLOT_BITS * LEVELS);

pub(crate) struct Wheel {
    /// The wheel's time, in ticks: every entry due at or before it has fired.
    elapsed: u64,
    /// Linked entries.
    len: usize,
    levels: [Level; LEVELS],
}

impl Wheel {
    pub(crate) const fn new() -> Self {
        Self {
            elapsed: 0,
            len: 0,
            levels: [const { Level::new() }; LEVELS],
        }
    }

    /// The wheel's time: the last `now` given to [`Wheel::pop_expired`] (or the tick being
    /// fired, while it fires).
    #[cfg(test)]
    pub(crate) fn elapsed(&self) -> u64 {
        self.elapsed
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Links `entry` to fire at `deadline_tick`: once [`Wheel::pop_expired`] is given a `now`
    /// at or after it (a tick already passed fires on the next call).
    ///
    /// # Safety
    /// `entry` is live and unlinked, the caller has exclusive access to it, and it stays live
    /// and in place until it is removed or popped. While linked, only the wheel's owner
    /// touches its state.
    pub(crate) unsafe fn insert(&mut self, entry: NonNull<TimerEntry>, deadline_tick: u64) {
        // SAFETY: the caller has exclusive access to the unlinked entry.
        unsafe { (*TimerEntry::state(entry)).deadline_tick = deadline_tick };
        // SAFETY: forwarded to the caller.
        unsafe { self.link(entry, deadline_tick) };
        self.len += 1;
    }

    /// Unlinks `entry`: O(1).
    ///
    /// # Safety
    /// `entry` is linked in this wheel.
    pub(crate) unsafe fn remove(&mut self, entry: NonNull<TimerEntry>) {
        // SAFETY: the entry is linked here, and the wheel's owner has exclusive access.
        let location = unsafe { (*TimerEntry::state(entry)).location };
        let (level, slot) = split_location(location);
        // SAFETY: `location` says where the entry is linked.
        unsafe { self.levels[level].remove(slot, entry) };
        self.len -= 1;
    }

    /// Unlinks and returns the next entry due at or before `now`, earliest tick first and in
    /// insertion order within a tick; `None` once there is none, after moving the wheel's
    /// time to `now`. Cascades the slots it passes and re-arms clamped entries on the way.
    ///
    /// Call it until it returns `None`, with the same `now`: entries inserted meanwhile at
    /// or before `now` are returned too.
    pub(crate) fn pop_expired(&mut self, now: u64) -> Option<NonNull<TimerEntry>> {
        loop {
            let Some((level, slot, start)) = self.next_expiration().filter(|&(.., s)| s <= now)
            else {
                self.elapsed = self.elapsed.max(now);
                return None;
            };
            self.elapsed = start;
            if level > 0 {
                self.cascade(level, slot);
                continue;
            }
            let entry = self.levels[0].head(slot)?;
            // SAFETY: the head of an occupied slot is linked there.
            unsafe { self.levels[0].remove(slot, entry) };
            // SAFETY: linked until just now, so the wheel's owner still has exclusive access.
            let deadline_tick = unsafe { (*TimerEntry::state(entry)).deadline_tick };
            if deadline_tick > start {
                // Not due yet (only clamped entries sit early; none reach level 0, but
                // never firing early does not rely on it).
                // SAFETY: just unlinked from this wheel, still live.
                unsafe { self.link(entry, deadline_tick) };
                continue;
            }
            self.len -= 1;
            return Some(entry);
        }
    }

    /// Unlinks and returns any entry, due or not: for draining the wheel.
    pub(crate) fn pop_any(&mut self) -> Option<NonNull<TimerEntry>> {
        let (level, slot) = self
            .levels
            .iter()
            .enumerate()
            .find_map(|(index, level)| Some((index, level.any_slot()?)))?;
        let entry = self.levels[level].head(slot)?;
        // SAFETY: the head of an occupied slot is linked there.
        unsafe { self.levels[level].remove(slot, entry) };
        self.len -= 1;
        Some(entry)
    }

    /// The earliest tick at which [`Wheel::pop_expired`] has work: the earliest entry's tick,
    /// or, for an entry beyond the span, the tick at which it is re-armed. Never later than
    /// the earliest entry's tick, so parking until then never oversleeps a timer.
    pub(crate) fn next_deadline_tick(&self) -> Option<u64> {
        let (level, slot, start) = self.next_expiration()?;
        if level == 0 {
            return Some(start);
        }
        // The earliest slot on the lowest occupied level holds the earliest entry: walk it, so
        // a park waits for that entry and not for each cascade on the way down.
        let end = start + (1 << (SLOT_BITS * level)) - 1;
        let mut earliest = end;
        let mut cursor = self.levels[level].head(slot);
        while let Some(entry) = cursor {
            // SAFETY: linked in this wheel, which the caller owns.
            let state = unsafe { &*TimerEntry::state(entry) };
            earliest = earliest.min(state.deadline_tick);
            cursor = state.next;
        }
        Some(earliest.max(start))
    }

    /// The first slot with entries, scanning from level 0: `(level, slot, start tick)`. The
    /// lowest occupied level's next slot is the earliest, since every slot of a level ends
    /// before the next level's first slot starts.
    fn next_expiration(&self) -> Option<(usize, usize, u64)> {
        self.levels.iter().enumerate().find_map(|(index, level)| {
            let (slot, start) = level.next_slot(index, self.elapsed)?;
            Some((index, slot, start))
        })
    }

    /// Moves the entries of `slot` on `level` (the wheel's time has reached its start) down to
    /// finer levels, in order.
    fn cascade(&mut self, level: usize, slot: usize) {
        let mut cursor = self.levels[level].take(slot);
        while let Some(entry) = cursor {
            // SAFETY: the taken list's entries are live and owned by the wheel; each is
            // unlinked and linked again before the next one is visited.
            unsafe {
                let state = TimerEntry::state(entry);
                cursor = (*state).next;
                (*state).prev = None;
                (*state).next = None;
                (*state).location = entry::UNLINKED;
                self.link(entry, (*state).deadline_tick);
            }
        }
    }

    /// Links `entry` in the slot for `deadline_tick`, clamped to the wheel's time and span.
    ///
    /// # Safety
    /// As for [`Wheel::insert`]; does not count the entry.
    unsafe fn link(&mut self, entry: NonNull<TimerEntry>, deadline_tick: u64) {
        let latest = self.elapsed.saturating_add(MAX_SPAN - 1);
        let when = deadline_tick.clamp(self.elapsed, latest);
        let level = level_for(self.elapsed, when);
        let slot = slot_of(when, level);
        // SAFETY: forwarded to the caller.
        unsafe { self.levels[level].push_back(level, slot, entry) };
    }
}

/// The level for an entry at `when` on a wheel at `elapsed`: the highest 6-bit group in which
/// they differ. A difference beyond the span lands on the top level (its ring).
fn level_for(elapsed: u64, when: u64) -> usize {
    let masked = ((elapsed ^ when) | (SLOTS as u64 - 1)).min(MAX_SPAN - 1);
    let significant = 63 - masked.leading_zeros() as usize;
    significant / SLOT_BITS
}

impl Default for Wheel {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Wheel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wheel")
            .field("elapsed", &self.elapsed)
            .field("len", &self.len)
            .field(
                "occupied_levels",
                &self.levels.iter().filter(|level| !level.is_empty()).count(),
            )
            .finish()
    }
}

// SAFETY: the wheel only reaches its entries (`Send + Sync`) through `&mut self` or `&self`,
// so moving it to another thread moves that exclusive access with it.
unsafe impl Send for Wheel {}
