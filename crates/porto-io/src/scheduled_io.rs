//! [`ScheduledIo`]: the per-registration readiness word and its waiters (13-io-driver.md §2.1).
//!
//! The readiness word packs, from the low bits up:
//!
//! | bits    | field                                                             |
//! |---------|-------------------------------------------------------------------|
//! | 0..8    | readiness ([`Ready`]: six bits used)                              |
//! | 8..23   | tick (15 bits), bumped by every dispatch                          |
//! | 23..31  | generation (8 bits), bumped when the slot is released             |
//!
//! It fits a 32-bit `usize`. The tick lets a consumer clear exactly the readiness it observed
//! and no newer event; the generation lets one atomic update reject events and waits that
//! belong to an earlier owner of the slot.

use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use crate::driver::{GENERATION_BITS, GENERATION_MASK};
use crate::waiters::{WaiterNode, Waiters, WakeList};
use crate::{Interest, Ready, RegistrationId};

const READY_MASK: usize = 0xFF;
const TICK_SHIFT: u32 = 8;
const TICK_BITS: u32 = 15;
const TICK_MASK: usize = (1 << TICK_BITS) - 1;
const GENERATION_SHIFT: u32 = TICK_SHIFT + TICK_BITS;
const _: () = assert!(GENERATION_SHIFT + GENERATION_BITS <= usize::BITS);

const fn pack(ready: Ready, tick: usize, generation: usize) -> usize {
    (ready.bits() & READY_MASK)
        | ((tick & TICK_MASK) << TICK_SHIFT)
        | ((generation & GENERATION_MASK) << GENERATION_SHIFT)
}

const fn unpack_ready(word: usize) -> Ready {
    Ready::from_bits_truncate(word & READY_MASK)
}

const fn unpack_tick(word: usize) -> usize {
    (word >> TICK_SHIFT) & TICK_MASK
}

const fn unpack_generation(word: usize) -> usize {
    (word >> GENERATION_SHIFT) & GENERATION_MASK
}

/// One direction of a registration, each with its own waiter slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Waits for [`Interest::READABLE`].
    Read,
    /// Waits for [`Interest::WRITABLE`].
    Write,
}

impl Direction {
    const fn mask(self) -> Ready {
        match self {
            Self::Read => Ready::from_interest(Interest::READABLE),
            Self::Write => Ready::from_interest(Interest::WRITABLE),
        }
    }
}

/// A snapshot of readiness, with the tick it was observed at. Pass it back to
/// [`ScheduledIo::clear_readiness`] after the operation hit `WouldBlock`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadyEvent {
    ready: Ready,
    tick: usize,
    generation: usize,
    /// The registration was released: closed, deregistered, or the driver shut down.
    released: bool,
}

impl ReadyEvent {
    /// The readiness observed, restricted to the waiter's interest.
    pub const fn ready(self) -> Ready {
        self.ready
    }

    /// Whether the registration was released (closed, deregistered, or its driver shut down)
    /// rather than observed ready: its readiness is then [`Ready::CLOSED`] and no operation on
    /// it can succeed.
    pub const fn is_released(self) -> bool {
        self.released
    }

    /// The same event, restricted to `ready`, to clear only part of it (one direction of a
    /// combined wait).
    #[must_use]
    pub fn restrict(self, ready: Ready) -> Self {
        Self {
            ready: self.ready & ready,
            ..self
        }
    }

    /// What a stale wait observes: the slot was released, so the resource is gone.
    const fn closed(mask: Ready, generation: usize) -> Self {
        Self {
            ready: Ready::from_bits_truncate(Ready::CLOSED.bits() & mask.bits()),
            tick: 0,
            generation,
            released: true,
        }
    }
}

/// The readiness state of one registration: an atomic readiness word and the tasks waiting
/// on it. Lives in an [`IoSlab`](crate::IoSlab) slot.
pub struct ScheduledIo {
    readiness: AtomicUsize,
    waiters: Mutex<Waiters>,
}

impl Default for ScheduledIo {
    fn default() -> Self {
        Self {
            readiness: AtomicUsize::new(0),
            waiters: Mutex::default(),
        }
    }
}

impl std::fmt::Debug for ScheduledIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let word = self.readiness.load(Ordering::Acquire);
        f.debug_struct("ScheduledIo")
            .field("ready", &unpack_ready(word))
            .field("tick", &unpack_tick(word))
            .field("generation", &unpack_generation(word))
            .finish_non_exhaustive()
    }
}

impl ScheduledIo {
    /// The slot's current generation.
    pub(crate) fn generation(&self) -> usize {
        unpack_generation(self.readiness.load(Ordering::Acquire))
    }

    /// Adds `ready` to the readiness of `generation` and bumps the tick. Returns `false`, and
    /// changes nothing, when the slot has moved to another generation.
    fn set_readiness(&self, generation: usize, ready: Ready) -> bool {
        let mut current = self.readiness.load(Ordering::Acquire);
        loop {
            if unpack_generation(current) != generation {
                return false;
            }
            let next = pack(
                unpack_ready(current) | ready,
                unpack_tick(current) + 1,
                generation,
            );
            match self.readiness.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Records readiness from the driver and wakes the waiters it concerns. Returns `false`,
    /// waking nobody, when `id` is stale.
    pub(crate) fn dispatch(&self, id: RegistrationId, ready: Ready) -> bool {
        if !self.set_readiness(id.generation(), ready) {
            return false;
        }
        self.wake(ready);
        true
    }

    /// Bumps the generation so `id` and everything carrying it goes stale, forgets the
    /// readiness, and wakes every waiter (each then observes [`Ready::CLOSED`]). Returns
    /// `false` when `id` was already stale.
    pub(crate) fn release(&self, id: RegistrationId) -> bool {
        let mut current = self.readiness.load(Ordering::Acquire);
        loop {
            if unpack_generation(current) != id.generation() {
                return false;
            }
            let next = pack(Ready::EMPTY, unpack_tick(current), id.generation() + 1);
            match self.readiness.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.wake(Ready::ALL);
        true
    }

    /// Clears the readiness in `event`, unless a newer event arrived since it was observed
    /// (the tick moved) or the slot was released. The closed bits are never cleared.
    pub fn clear_readiness(&self, event: ReadyEvent) {
        let clear = event.ready - (Ready::READ_CLOSED | Ready::WRITE_CLOSED);
        let mut current = self.readiness.load(Ordering::Acquire);
        loop {
            if unpack_generation(current) != event.generation || unpack_tick(current) != event.tick
            {
                return;
            }
            let next = pack(unpack_ready(current) - clear, event.tick, event.generation);
            match self.readiness.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    /// The current readiness for `interest`, possibly empty. A stale `id` observes
    /// [`Ready::CLOSED`].
    pub fn ready_event(&self, id: RegistrationId, interest: Interest) -> ReadyEvent {
        let mask = Ready::from_interest(interest);
        let word = self.readiness.load(Ordering::Acquire);
        event_if_ready(word, id, mask).unwrap_or(ReadyEvent {
            ready: Ready::EMPTY,
            tick: unpack_tick(word),
            generation: id.generation(),
            released: false,
        })
    }

    /// Polls for readiness in one `direction`, keeping a single waker per direction.
    pub fn poll_readiness(
        &self,
        cx: &mut Context<'_>,
        id: RegistrationId,
        direction: Direction,
    ) -> Poll<ReadyEvent> {
        let mask = direction.mask();
        if let Some(event) = event_if_ready(self.readiness.load(Ordering::Acquire), id, mask) {
            return Poll::Ready(event);
        }
        let mut waiters = self.lock_waiters();
        // A stale ID (the slot was released, perhaps reused) must not touch the waker slots: it
        // would replace the new owner's waker and that owner would never wake.
        let word = self.readiness.load(Ordering::Acquire);
        if unpack_generation(word) != id.generation() {
            drop(waiters);
            return Poll::Ready(ReadyEvent::closed(mask, id.generation()));
        }
        let slot = match direction {
            Direction::Read => &mut waiters.reader,
            Direction::Write => &mut waiters.writer,
        };
        // Taken out and dropped after the unlock: a waker's `Drop` may use this registration.
        let stale = match slot {
            Some(waker) if waker.will_wake(cx.waker()) => None,
            _ => slot.replace(cx.waker().clone()),
        };
        // A dispatch that raced with us updated the word before taking the lock to wake, so
        // with the waker stored, a second look cannot miss it.
        let poll = match event_if_ready(self.readiness.load(Ordering::Acquire), id, mask) {
            Some(event) => Poll::Ready(event),
            None => Poll::Pending,
        };
        drop(waiters);
        drop(stale);
        poll
    }

    /// A future that resolves once readiness matching `interest` is set, for any number of
    /// concurrent waiters.
    pub fn readiness(&self, id: RegistrationId, interest: Interest) -> Readiness<'_> {
        Readiness {
            io: self,
            id,
            linked: false,
            node: UnsafeCell::new(WaiterNode::new(interest)),
            release_only: false,
            _pinned: PhantomPinned,
        }
    }

    /// A future that resolves, with the closed event, once the slot of `id` is released, and
    /// never for any readiness: not even the closed bits a hang-up sets (which are never
    /// cleared, so waiting on them would resolve at every poll). A wake for readiness may
    /// poll it, and it stays pending.
    pub fn released(&self, id: RegistrationId) -> Readiness<'_> {
        let mut released = self.readiness(id, Interest::PRIORITY);
        released.release_only = true;
        released
    }

    fn lock_waiters(&self) -> MutexGuard<'_, Waiters> {
        // Wakers run outside the lock, so a poisoned lock still holds consistent waiters.
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wakes every waiter whose interest matches `ready`, in batches, outside the lock.
    fn wake(&self, ready: Ready) {
        let mut wakers = WakeList::new();
        let mut waiters = self.lock_waiters();
        loop {
            if ready.intersects(Direction::Read.mask())
                && let Some(waker) = waiters.reader.take()
            {
                wakers.push(waker);
            }
            if ready.intersects(Direction::Write.mask())
                && let Some(waker) = waiters.writer.take()
            {
                wakers.push(waker);
            }
            if waiters.list.notify_matching(ready, &mut wakers) {
                break;
            }
            drop(waiters);
            wakers.wake_all();
            waiters = self.lock_waiters();
        }
        drop(waiters);
        wakers.wake_all();
    }

    #[cfg(test)]
    pub(crate) fn has_list_waiters(&self) -> bool {
        !self.lock_waiters().list.is_empty()
    }
}

/// The event for `mask` if `word` has any of it, or the closed event if `id` is stale.
fn event_if_ready(word: usize, id: RegistrationId, mask: Ready) -> Option<ReadyEvent> {
    if unpack_generation(word) != id.generation() {
        return Some(ReadyEvent::closed(mask, id.generation()));
    }
    let ready = unpack_ready(word) & mask;
    (!ready.is_empty()).then_some(ReadyEvent {
        ready,
        tick: unpack_tick(word),
        generation: id.generation(),
        released: false,
    })
}

/// The future returned by [`ScheduledIo::readiness`]. While pending it is linked into the
/// `ScheduledIo`'s waiter list; dropping it unlinks it.
#[must_use = "futures do nothing unless awaited"]
pub struct Readiness<'a> {
    io: &'a ScheduledIo,
    id: RegistrationId,
    /// Whether `node` was linked by the last poll (it may since have been notified).
    linked: bool,
    /// Only touched with `io.waiters` locked, since the list points into it.
    node: UnsafeCell<WaiterNode>,
    /// Resolves on the release of the slot only, whatever readiness is set (see
    /// [`ScheduledIo::released`]).
    release_only: bool,
    _pinned: PhantomPinned,
}

impl Readiness<'_> {
    /// The event this wait resolves with at `word`, if it does.
    fn observe(&self, word: usize, mask: Ready) -> Option<ReadyEvent> {
        if self.release_only {
            let generation = unpack_generation(word);
            (generation != self.id.generation())
                .then(|| ReadyEvent::closed(mask, self.id.generation()))
        } else {
            event_if_ready(word, self.id, mask)
        }
    }
}

// SAFETY: `node` is only accessed with the `ScheduledIo` mutex held, so the future can move
// between threads (while not linked; once linked it is pinned) and be shared: `&Readiness`
// exposes nothing.
unsafe impl Send for Readiness<'_> {}
// SAFETY: as above.
unsafe impl Sync for Readiness<'_> {}

impl Future for Readiness<'_> {
    type Output = ReadyEvent;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ReadyEvent> {
        // SAFETY: nothing is moved out of `self`; `node` stays where it was pinned.
        let this = unsafe { self.get_unchecked_mut() };
        let io = this.io;
        let node_ptr = this.node.get();
        // SAFETY: `interest` is never written after construction.
        let mask = Ready::from_interest(unsafe { (*node_ptr).interest });

        if !this.linked
            && let Some(event) = this.observe(io.readiness.load(Ordering::Acquire), mask)
        {
            return Poll::Ready(event);
        }

        let mut waiters = io.lock_waiters();
        // SAFETY: we hold the mutex, the only other way to reach the node.
        let node = unsafe { &mut *node_ptr };
        if this.linked && !node.notified {
            match &node.waker {
                Some(waker) if waker.will_wake(cx.waker()) => {}
                _ => node.waker = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        }
        // Not linked, or notified (and so unlinked): look at the word again under the lock,
        // so a dispatch that races with linking cannot be missed.
        this.linked = false;
        if let Some(event) = this.observe(io.readiness.load(Ordering::Acquire), mask) {
            return Poll::Ready(event);
        }
        node.notified = false;
        node.waker = Some(cx.waker().clone());
        // SAFETY: we hold the mutex, the node is not linked, and it is pinned until `Drop`
        // unlinks it.
        unsafe { waiters.list.push_back(NonNull::from(node)) };
        this.linked = true;
        Poll::Pending
    }
}

impl Drop for Readiness<'_> {
    fn drop(&mut self) {
        if !self.linked {
            return;
        }
        let mut waiters = self.io.lock_waiters();
        // SAFETY: we hold the mutex.
        let node = unsafe { &mut *self.node.get() };
        if !node.notified {
            // SAFETY: linked and not notified means it is still in the list.
            unsafe { waiters.list.remove(NonNull::from(node)) };
        }
    }
}

#[cfg(test)]
mod tests;
