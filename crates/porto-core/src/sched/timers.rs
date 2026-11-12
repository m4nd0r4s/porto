//! A lane's timer shard: a [`Wheel`] behind a mutex, in runtime-clock deadlines
//! (`14-time-and-timers.md` §2).
//!
//! The lane links entries (from `Sleep::poll`, on the lane thread) and fires them, before
//! parking and every `EVENT_INTERVAL` tasks. The mutex is there for the other side of an
//! entry's life: a `Sleep` may be dropped on any thread, at any time, and its entry is unlinked
//! then. It is uncontended on the lane's own path.
//!
//! No user code runs under the lock: wakers are cloned before it is taken, and woken or
//! dropped after it is released (a wake may arm a timer; dropping a waker may free a task whose
//! `Sleep` unlinks itself).

use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Waker;
use std::time::Duration;

use crate::time::Deadline;
use crate::time::wheel::{TimerEntry, Wheel};

/// Entries fired per lock: their wakers are called once the lock is released.
const FIRE_BATCH: usize = 32;

/// What [`Timers::arm`] did.
#[must_use = "the replaced waker must be dropped outside the lock"]
pub(crate) enum Armed {
    /// The entry is linked; this is the waker it held before, if replaced.
    Linked(Option<Waker>),
    /// No waker was given and the entry was not linked: nothing changed.
    Unlinked,
    /// The wheel was shut down ([`Timers::drop_all`], the runtime is being dropped): nothing was
    /// linked. Holds the waker that was given, for the caller to drop outside any lock.
    Closed(Option<Waker>),
}

pub(crate) struct Timers {
    shard: Mutex<Shard>,
    /// Linked entries, read without the lock to skip it when there are none. Written under
    /// the lock. The lane links new entries (`Sleep::poll`), so it never reads it as zero
    /// while one it linked is; another thread that links one (a scope deadline set there,
    /// `ScopeNode::set_deadline`) notifies the lanes after, and the notify orders the write
    /// before the lane's next look.
    armed: AtomicUsize,
}

struct Shard {
    wheel: Wheel,
    /// Set by [`Timers::drop_all`]: the runtime is going away, so nothing is linked any more
    /// (an entry linked now would never be fired or released, and its waker would keep
    /// whatever it owns alive).
    closed: bool,
    /// Nanoseconds per tick.
    resolution: u64,
}

impl Timers {
    /// The timer resolution when none is configured: 1 ms per tick.
    pub(crate) const DEFAULT_RESOLUTION: Duration = Duration::from_millis(1);
    /// The configurable range (`14-time-and-timers.md` §4).
    pub(crate) const MIN_RESOLUTION: Duration = Duration::from_micros(100);
    pub(crate) const MAX_RESOLUTION: Duration = Duration::from_millis(10);

    pub(crate) fn new(resolution: Duration) -> Self {
        Self {
            shard: Mutex::new(Shard {
                wheel: Wheel::new(),
                closed: false,
                // The builder keeps it within 100 µs..=10 ms.
                resolution: u64::try_from(resolution.as_nanos()).unwrap_or(1_000_000),
            }),
            armed: AtomicUsize::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Shard> {
        // Wheel operations do not panic and no user code runs under the lock; keep going.
        self.shard.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Arms `entry` to wake `waker` at `deadline`: links it, or moves it if it is linked at
    /// another tick. Returns the waker it replaces, for the caller to drop outside any lock.
    ///
    /// With no `waker`, only moves an entry that is linked (keeping its waker), and returns
    /// whether it did: what a reset from any thread may do, since only the lane links new
    /// entries.
    ///
    /// # Safety
    /// `entry` is live and stays in place until it is disarmed or fired; it is linked in this
    /// shard or in none; and it is disarmed (by [`Timers::disarm`] on this shard) before it is
    /// dropped or armed on another shard.
    pub(crate) unsafe fn arm(
        &self,
        entry: NonNull<TimerEntry>,
        deadline: Deadline,
        waker: Option<Waker>,
    ) -> Armed {
        let mut shard = self.lock();
        if shard.closed {
            return Armed::Closed(waker);
        }
        let tick = shard.deadline_tick(deadline);
        // SAFETY: the entry is live, and linked here or nowhere: holding the lock gives
        // exclusive access to its state either way (its owner only touches it through us).
        unsafe {
            let state = TimerEntry::state(entry);
            if TimerEntry::is_linked(entry) {
                if (*state).deadline_tick != tick {
                    shard.wheel.remove(entry);
                    shard.wheel.insert(entry, tick);
                }
            } else if waker.is_some() {
                shard.wheel.insert(entry, tick);
                self.armed.store(shard.wheel.len(), Ordering::Relaxed);
            } else {
                return Armed::Unlinked;
            }
            match waker {
                Some(waker) => Armed::Linked((*state).waker.replace(waker)),
                None => Armed::Linked(None),
            }
        }
    }

    /// Unlinks `entry` if it is linked, and takes its waker, for the caller to drop outside
    /// any lock.
    ///
    /// # Safety
    /// `entry` is live, and linked in this shard or in none.
    pub(crate) unsafe fn disarm(&self, entry: NonNull<TimerEntry>) -> Option<Waker> {
        let mut shard = self.lock();
        // SAFETY: as in `arm`.
        unsafe {
            if TimerEntry::is_linked(entry) {
                shard.wheel.remove(entry);
                self.armed.store(shard.wheel.len(), Ordering::Relaxed);
            }
            (*TimerEntry::state(entry)).waker.take()
        }
    }

    /// How many entries are linked: armed timers, sleeping tasks among them.
    pub(crate) fn armed(&self) -> usize {
        self.armed.load(Ordering::Relaxed)
    }

    /// The earliest deadline the lane must wake up for, if any timer is armed: never after
    /// the earliest timer's deadline.
    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        if self.armed.load(Ordering::Relaxed) == 0 {
            return None;
        }
        let shard = self.lock();
        let tick = shard.wheel.next_deadline_tick()?;
        Some(Deadline::from_nanos(tick.saturating_mul(shard.resolution)))
    }

    /// Wakes every timer whose deadline has passed on the runtime clock, earliest first, in
    /// batches of [`FIRE_BATCH`] per lock. Returns how many fired.
    ///
    /// Reads the clock (`now`) only when a timer is armed.
    pub(crate) fn fire_expired(&self, now: impl FnOnce() -> Deadline) -> usize {
        if self.armed.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        let now = now();
        let mut fired = 0;
        let mut wakers: [Option<Waker>; FIRE_BATCH] = [const { None }; FIRE_BATCH];
        loop {
            let mut taken = 0;
            {
                let mut shard = self.lock();
                // A tick fires once it has fully passed: `deadline_tick` rounds up, this rounds
                // down, so no timer fires before its deadline.
                let now_tick = now.as_nanos() / shard.resolution;
                while taken < FIRE_BATCH {
                    let Some(entry) = shard.wheel.pop_expired(now_tick) else {
                        break;
                    };
                    // SAFETY: just unlinked under the lock, and still live (its owner disarms
                    // it under this lock before dropping it).
                    wakers[taken] = unsafe { (*TimerEntry::state(entry)).waker.take() };
                    taken += 1;
                }
                self.armed.store(shard.wheel.len(), Ordering::Relaxed);
            }
            for waker in &mut wakers[..taken] {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
            fired += taken;
            if taken < FIRE_BATCH {
                return fired;
            }
        }
    }

    /// Unlinks every entry and drops their wakers (outside the lock). Returns whether there
    /// was any.
    pub(crate) fn drop_all(&self) -> bool {
        let mut wakers = Vec::new();
        let mut any = false;
        {
            let mut shard = self.lock();
            shard.closed = true;
            while let Some(entry) = shard.wheel.pop_any() {
                any = true;
                // SAFETY: as in `fire_expired`.
                wakers.extend(unsafe { (*TimerEntry::state(entry)).waker.take() });
            }
            self.armed.store(0, Ordering::Relaxed);
        }
        // Dropping a waker may free its task, whose destructor may arm or disarm timers.
        drop(wakers);
        any
    }
}

impl Shard {
    /// The tick `deadline` falls due at: rounded up, so it never fires early.
    fn deadline_tick(&self, deadline: Deadline) -> u64 {
        deadline.as_nanos().div_ceil(self.resolution)
    }
}
