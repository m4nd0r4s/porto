//! A lane's timers: a min-heap of (deadline, waker), fired in deadline order.
//!
//! Entries with equal deadlines fire in registration order. The heap is lane-local and only
//! touched by the lane thread, so it needs no synchronization.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem;
use std::task::Waker;

use crate::time::Deadline;

#[derive(Default)]
pub(crate) struct Timers {
    heap: RefCell<BinaryHeap<Entry>>,
    /// Registrations so far: the tie-breaker that keeps equal deadlines in FIFO order.
    sequence: Cell<u64>,
}

struct Entry {
    deadline: Deadline,
    sequence: u64,
    waker: Waker,
}

impl Timers {
    pub(crate) fn register(&self, deadline: Deadline, waker: Waker) {
        let sequence = self.sequence.get();
        self.sequence.set(sequence.wrapping_add(1));
        self.heap.borrow_mut().push(Entry {
            deadline,
            sequence,
            waker,
        });
    }

    /// The earliest deadline, if any timer is registered.
    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        self.heap.borrow().peek().map(|entry| entry.deadline)
    }

    /// Wakes every timer whose deadline has passed, earliest first. Returns how many fired.
    ///
    /// Reads the clock only when a timer is registered.
    pub(crate) fn fire_expired(&self) -> usize {
        if self.heap.borrow().is_empty() {
            return 0;
        }
        let now = Deadline::now();
        let mut fired = 0;
        loop {
            let entry = {
                let mut heap = self.heap.borrow_mut();
                match heap.peek() {
                    Some(entry) if entry.deadline <= now => heap.pop(),
                    _ => None,
                }
            };
            // The heap is not borrowed while the waker runs: a wake may register a timer.
            let Some(entry) = entry else { break };
            entry.waker.wake();
            fired += 1;
        }
        fired
    }

    /// Removes every entry, so the caller can drop the wakers outside any borrow. Returns
    /// whether there was any.
    pub(crate) fn drop_all(&self) -> bool {
        let entries = mem::take(&mut *self.heap.borrow_mut());
        let any = !entries.is_empty();
        // Dropping a waker may free its task, whose destructor may register or drop timers.
        drop(entries);
        any
    }
}

// `BinaryHeap` is a max-heap: the earliest (deadline, sequence) compares greatest.
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.deadline, other.sequence).cmp(&(self.deadline, self.sequence))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Entry {}
