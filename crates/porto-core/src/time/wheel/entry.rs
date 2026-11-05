//! The intrusive timer entry: the node a [`Wheel`](super::Wheel) links into its slot lists.

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomPinned;
use std::ptr::NonNull;
use std::task::Waker;

/// A node of the timer wheel, embedded in whatever waits for it (a [`Sleep`], and later one per
/// Python task, `14-time-and-timers.md` §2). 48 bytes, no allocation.
///
/// While linked, the wheel holds a pointer to the entry, so the entry MUST stay put (it is
/// `!Unpin`) and MUST be unlinked before it is dropped. Its state is only touched by whoever
/// owns the wheel it is linked in (for a lane, under the shard lock), or by its owner while it
/// is unlinked.
///
/// [`Sleep`]: crate::time::Sleep
pub(crate) struct TimerEntry {
    state: UnsafeCell<EntryState>,
    // Linked entries are pointed at: pinned, and `&mut` to it is not assumed unique.
    _pinned: PhantomPinned,
}

pub(crate) struct EntryState {
    /// The tick the entry fires at, as requested: never earlier. The slot it sits in may be
    /// earlier when the deadline is beyond the wheel's span (it is re-armed from there).
    pub(crate) deadline_tick: u64,
    /// Neighbors in the slot list.
    pub(super) prev: Option<NonNull<TimerEntry>>,
    pub(super) next: Option<NonNull<TimerEntry>>,
    /// Woken when the entry fires; the firing side takes it.
    pub(crate) waker: Option<Waker>,
    /// `level * SLOTS + slot` while linked, [`UNLINKED`] otherwise.
    pub(super) location: u16,
}

pub(super) const UNLINKED: u16 = u16::MAX;

impl TimerEntry {
    /// An unlinked entry, without a waker.
    pub(crate) const fn new() -> Self {
        Self {
            state: UnsafeCell::new(EntryState {
                deadline_tick: 0,
                prev: None,
                next: None,
                waker: None,
                location: UNLINKED,
            }),
            _pinned: PhantomPinned,
        }
    }

    /// The entry's state.
    ///
    /// # Safety
    /// `entry` points at a live entry, and the caller has exclusive access to its state for as
    /// long as it uses the pointer: it owns the wheel the entry is linked in, or the entry is
    /// unlinked and the caller owns it.
    pub(crate) unsafe fn state(entry: NonNull<Self>) -> *mut EntryState {
        // SAFETY: the caller guarantees `entry` is live; `UnsafeCell` allows writes through
        // the shared reference.
        unsafe { entry.as_ref() }.state.get()
    }

    /// Whether a wheel links the entry.
    ///
    /// # Safety
    /// As for [`TimerEntry::state`].
    pub(crate) unsafe fn is_linked(entry: NonNull<Self>) -> bool {
        // SAFETY: forwarded to the caller.
        unsafe { (*Self::state(entry)).location != UNLINKED }
    }
}

impl Default for TimerEntry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TimerEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The state may be in use by the wheel's owner on another thread: not read here.
        f.debug_struct("TimerEntry").finish_non_exhaustive()
    }
}

// SAFETY: the state (raw pointers to other entries, a `Waker`) is only touched with exclusive
// access, as `TimerEntry::state` requires; the wheel's owner serializes it.
unsafe impl Send for TimerEntry {}
// SAFETY: a shared reference gives no access to the state without the `unsafe` contract above.
unsafe impl Sync for TimerEntry {}
