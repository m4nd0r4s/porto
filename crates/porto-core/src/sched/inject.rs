//! The injector: an unbounded FIFO that any thread pushes onto and lanes pop from
//! (`12-scheduler-and-lanes.md` §2 `Shared.injector`).
//!
//! Wakes and spawns from threads that are not running a lane land here. It is a `VecDeque`
//! behind a mutex, as Tokio's injector is, with the length mirrored in an atomic so a lane can
//! skip the lock while the queue is empty, which is the common case on the hot path. That
//! relaxed look may miss a push in flight; the pusher's notify (see `park::Parking`) is what
//! keeps the lane from sleeping on it.
//!
//! The same queue carries a lane's deferred work (`Handle::defer`), with no wake attached.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::Ordering;

use crate::sync::primitives::{AtomicUsize, Mutex, MutexGuard};

pub(crate) struct Inject<T> {
    queue: Mutex<Queue<T>>,
    /// `queue.items.len()`, written under the lock and read without it.
    len: AtomicUsize,
}

struct Queue<T> {
    items: VecDeque<T>,
    /// Set at shutdown: later pushes hand their item back.
    closed: bool,
}

impl<T> Inject<T> {
    pub(crate) fn new() -> Self {
        Self {
            queue: Mutex::new(Queue {
                items: VecDeque::new(),
                closed: false,
            }),
            len: AtomicUsize::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Queue<T>> {
        // Only queue operations run under the lock, so poisoning can only come from an
        // allocation failure; the queue is still consistent, so keep going.
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether the queue looks empty. A push in flight may be missed: its notify, which comes
    /// after, wakes the lane.
    pub(crate) fn is_empty(&self) -> bool {
        self.len.load(Ordering::Relaxed) == 0
    }

    /// The length when last written; see [`Inject::is_empty`].
    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Queues `item` at the back, or hands it back if the queue is closed. The caller drops a
    /// handed-back item outside any lock (dropping a task entry cancels it, running its
    /// destructor).
    pub(crate) fn push(&self, item: T) -> Result<(), T> {
        let mut queue = self.lock();
        if queue.closed {
            return Err(item);
        }
        queue.items.push_back(item);
        self.len.store(queue.items.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Takes the front item, if any.
    pub(crate) fn pop(&self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let mut queue = self.lock();
        let item = queue.items.pop_front();
        self.len.store(queue.items.len(), Ordering::Relaxed);
        item
    }

    /// Takes up to `max` items from the front: the first is returned, the rest are appended to
    /// `local` in order.
    pub(crate) fn pop_batch(&self, local: &mut VecDeque<T>, max: usize) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let mut queue = self.lock();
        let first = queue.items.pop_front()?;
        let more = queue.items.len().min(max.saturating_sub(1));
        local.extend(queue.items.drain(..more));
        self.len.store(queue.items.len(), Ordering::Relaxed);
        Some(first)
    }

    /// Closes the queue and takes everything in it, for the caller to drop outside the lock.
    /// Pushes from now on hand their item back.
    pub(crate) fn close(&self) -> VecDeque<T> {
        let mut queue = self.lock();
        queue.closed = true;
        self.len.store(0, Ordering::Relaxed);
        std::mem::take(&mut queue.items)
    }

    /// Takes everything in it, leaving it open.
    pub(crate) fn take_all(&self) -> VecDeque<T> {
        if self.is_empty() {
            return VecDeque::new();
        }
        let mut queue = self.lock();
        self.len.store(0, Ordering::Relaxed);
        std::mem::take(&mut queue.items)
    }
}

impl<T> fmt::Debug for Inject<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inject")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}
