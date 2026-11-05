//! A bounded lock-free MPMC queue: Dmitry Vyukov's array queue, as in `crossbeam::ArrayQueue`.
//!
//! Adapted from crossbeam-queue 0.3 (MIT OR Apache-2.0, the license porto shares). It is
//! vendored rather than depended on so that it is built from `sync::primitives`: under loom,
//! the model checker sees the queue's atomics, and the channel's lost-wakeup argument (a
//! `SeqCst` fence on each side, see `chan.rs`) is checked against the real queue rather than
//! a stand-in.
//!
//! `head` and `tail` are stamps: the low bits are a slot index, the high bits a lap count
//! (`one_lap` is the smallest power of two above the capacity). Each slot carries a stamp too:
//! a slot is free for the push whose tail equals its stamp, and full for the pop whose head
//! plus one equals its stamp. A push claims a slot by moving `tail` (CAS), writes the value,
//! then publishes it by bumping the slot stamp; a pop mirrors this on `head`.

use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::Ordering;

use super::super::primitives::{AtomicUsize, UnsafeCell, fence, spin};

/// The largest capacity [`ArrayQueue::new`] accepts: the lap bits need room above the index.
pub(super) const MAX_CAPACITY: usize = usize::MAX / 4;

/// One cell of the ring.
struct Slot<T> {
    /// `tail` for the push that may fill it, or `head + 1` for the pop that may empty it.
    stamp: AtomicUsize,
    /// Initialized exactly while the stamp says the slot is full.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// Keeps `head` and `tail` on separate cache lines, so producers and consumers do not
/// invalidate each other's line on every operation.
#[repr(align(128))]
struct CachePadded<T>(T);

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// A bounded multi-producer multi-consumer FIFO queue.
pub(super) struct ArrayQueue<T> {
    /// The stamp of the next slot to pop.
    head: CachePadded<AtomicUsize>,
    /// The stamp of the next slot to push.
    tail: CachePadded<AtomicUsize>,
    buffer: Box<[Slot<T>]>,
    capacity: usize,
    /// `capacity < one_lap`, a power of two: the stamp's index bits are `one_lap - 1`.
    one_lap: usize,
}

// SAFETY: values move between threads through the queue (hence `T: Send`); a slot's value is
// only touched by the one push or pop that claimed it through the stamp protocol.
unsafe impl<T: Send> Send for ArrayQueue<T> {}
// SAFETY: as above; `&ArrayQueue` only allows push and pop, which synchronize on the stamps.
unsafe impl<T: Send> Sync for ArrayQueue<T> {}

impl<T> ArrayQueue<T> {
    /// An empty queue holding at most `capacity` values.
    ///
    /// # Panics
    /// If `capacity` is zero or above [`MAX_CAPACITY`]; the channel constructors check both
    /// first, with a message that says what to do.
    pub(super) fn new(capacity: usize) -> Self {
        assert!(
            (1..=MAX_CAPACITY).contains(&capacity),
            "ArrayQueue capacity {capacity} is outside 1..={MAX_CAPACITY}"
        );
        let one_lap = (capacity + 1).next_power_of_two();
        let buffer = (0..capacity)
            .map(|index| Slot {
                stamp: AtomicUsize::new(index),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Self {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            buffer,
            capacity,
            one_lap,
        }
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    /// The stamp after `stamp`: the next index, or index 0 of the next lap.
    fn next_stamp(&self, stamp: usize) -> usize {
        let index = stamp & (self.one_lap - 1);
        if index + 1 < self.capacity {
            stamp + 1
        } else {
            (stamp & !(self.one_lap - 1)).wrapping_add(self.one_lap)
        }
    }

    /// Appends `value`, or returns it if the queue is full.
    pub(super) fn push(&self, value: T) -> Result<(), T> {
        let mut step = 0;
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[tail & (self.one_lap - 1)];
            let stamp = slot.stamp.load(Ordering::Acquire);
            if tail == stamp {
                // The slot is free for this lap: claim it.
                match self.tail.compare_exchange_weak(
                    tail,
                    self.next_stamp(tail),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        slot.value.with_mut(|cell| {
                            // SAFETY: the CAS made this push the slot's only owner until the
                            // stamp store below hands it to one pop.
                            unsafe { (*cell).write(value) };
                        });
                        slot.stamp.store(tail + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(current) => tail = current,
                }
            } else if stamp.wrapping_add(self.one_lap) == tail + 1 {
                // The slot still holds last lap's value: full, unless a pop is moving head.
                fence(Ordering::SeqCst);
                let head = self.head.load(Ordering::Relaxed);
                if head.wrapping_add(self.one_lap) == tail {
                    return Err(value);
                }
                spin(step);
                step += 1;
                tail = self.tail.load(Ordering::Relaxed);
            } else {
                // A pop claimed the slot and is still reading it.
                spin(step);
                step += 1;
                tail = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    /// Removes the oldest value, or returns `None` if the queue is empty. A push that has
    /// claimed a slot but not written it yet is waited for, not reported as empty.
    pub(super) fn pop(&self) -> Option<T> {
        let mut step = 0;
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[head & (self.one_lap - 1)];
            let stamp = slot.stamp.load(Ordering::Acquire);
            if head + 1 == stamp {
                // The slot is full for this lap: claim it.
                match self.head.compare_exchange_weak(
                    head,
                    self.next_stamp(head),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let value = slot.value.with_mut(|cell| {
                            // SAFETY: the CAS made this pop the slot's only owner, and the
                            // `Acquire` stamp load saw the push's write.
                            unsafe { (*cell).assume_init_read() }
                        });
                        slot.stamp
                            .store(head.wrapping_add(self.one_lap), Ordering::Release);
                        return Some(value);
                    }
                    Err(current) => head = current,
                }
            } else if stamp == head {
                // The slot is empty: so is the queue, unless a push is moving tail.
                fence(Ordering::SeqCst);
                let tail = self.tail.load(Ordering::Relaxed);
                if tail == head {
                    return None;
                }
                spin(step);
                step += 1;
                head = self.head.load(Ordering::Relaxed);
            } else {
                // A push claimed the slot and is still writing it.
                spin(step);
                step += 1;
                head = self.head.load(Ordering::Relaxed);
            }
        }
    }

    /// The number of values, exact when no push or pop runs concurrently.
    pub(super) fn len(&self) -> usize {
        loop {
            let tail = self.tail.load(Ordering::SeqCst);
            let head = self.head.load(Ordering::SeqCst);
            // A consistent snapshot: tail did not move while head was read.
            if self.tail.load(Ordering::SeqCst) == tail {
                let head_index = head & (self.one_lap - 1);
                let tail_index = tail & (self.one_lap - 1);
                return if head_index < tail_index {
                    tail_index - head_index
                } else if head_index > tail_index {
                    self.capacity - head_index + tail_index
                } else if tail == head {
                    0
                } else {
                    self.capacity
                };
            }
        }
    }
}

impl<T> Drop for ArrayQueue<T> {
    fn drop(&mut self) {
        // `&mut self`: no concurrent push or pop, so this drops exactly what is left.
        while self.pop().is_some() {}
    }
}

impl<T> fmt::Debug for ArrayQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrayQueue")
            .field("len", &self.len())
            .field("capacity", &self.capacity)
            .finish()
    }
}
