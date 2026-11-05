//! The state both ends share: the buffer, the open-handle counts and the two waiter lists.
//!
//! # No lost wakeups
//!
//! Push and pop are lock-free; the waiter lists ([`WaitQueue`]) are not. A receiver that finds
//! the buffer empty locks its list, raises `recv_hint` to the list length plus one, issues a
//! `SeqCst` fence and pops again; only if that pop fails too does it queue. A sender pushes,
//! issues a `SeqCst` fence and reads `recv_hint`, locking the list to wake one receiver only
//! if the hint is non-zero. The two fences are ordered one way or the other: either the
//! receiver's second pop sees the value, or the sender sees the hint and wakes it. Senders
//! waiting for room mirror this with `send_hint`.
//!
//! (The waiter's fence is also implied by the buffer: the array queue's empty and full checks
//! fence before reading the other end's index, and the deque's mutex orders the two sides. It
//! is kept, off the fast path, so the argument does not depend on the buffer's internals. The
//! loom models catch the removal of either fast-path fence.)
//!
//! A hint is only written under its list's lock, and is never below the list's length (a
//! cancel unlinks without lowering it, which only costs a needless lock later), so a non-zero
//! length is never hidden from the fast path.
//!
//! A wake ("grant") means "try again", not "here is a value": the woken waiter retries the
//! fast path and queues again if another task took the value first. A woken waiter that is
//! cancelled before it retried passes the wake on ([`WaitState::pass_grant`]), so each push
//! (or pop) wakes exactly one counterpart that will act on it.
//!
//! Closing never relies on hints: the last sender (or receiver) to close always locks the
//! other side's list and wakes every waiter, and waiters check the counts under that lock.

use std::collections::VecDeque;
use std::fmt;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;

use super::super::primitives::{AtomicUsize, Mutex, MutexGuard, fence};
use super::super::{Locked, WaitQueue, WaitState};
use super::array_queue::ArrayQueue;
use super::error::{TryRecvError, TrySendError};

/// Where values wait between send and receive.
#[expect(
    clippy::large_enum_variant,
    reason = "the buffer lives once per channel, inside its Arc; boxing the padded queue \
              would add a pointer chase to every send and receive"
)]
enum Buffer<T> {
    /// `channel(capacity)`: lock-free.
    Bounded(ArrayQueue<T>),
    /// `unbounded_channel()`: a locked deque. Never full, so senders never wait on it.
    Unbounded(Mutex<VecDeque<T>>),
}

impl<T> Buffer<T> {
    fn push(&self, value: T) -> Result<(), T> {
        match self {
            Self::Bounded(queue) => queue.push(value),
            Self::Unbounded(deque) => {
                lock(deque).push_back(value);
                Ok(())
            }
        }
    }

    fn pop(&self) -> Option<T> {
        match self {
            Self::Bounded(queue) => queue.pop(),
            Self::Unbounded(deque) => lock(deque).pop_front(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Bounded(queue) => queue.len(),
            Self::Unbounded(deque) => lock(deque).len(),
        }
    }

    fn capacity(&self) -> Option<usize> {
        match self {
            Self::Bounded(queue) => Some(queue.capacity()),
            Self::Unbounded(_) => None,
        }
    }
}

fn lock<T>(deque: &Mutex<VecDeque<T>>) -> MutexGuard<'_, VecDeque<T>> {
    // A deque push or pop that panics (allocation failure aside) leaves it consistent.
    deque.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The state of both waiter lists. Waiters are only told to retry, so a wake that a
/// cancelled waiter can no longer act on goes to the next one.
#[derive(Debug)]
pub(super) struct Retry;

impl WaitState for Retry {
    fn pass_grant(waiters: &mut Locked<'_, Self>, _: usize) {
        waiters.grant_front();
    }
}

/// The shared channel state, behind an `Arc` in every handle.
pub(super) struct Chan<T> {
    buffer: Buffer<T>,
    /// Open sender handles; the channel ends for receivers when it reaches zero.
    senders: AtomicUsize,
    /// Open receiver handles; sends fail once it reaches zero.
    receivers: AtomicUsize,
    /// Senders waiting for room.
    pub(super) send_waiters: WaitQueue<Retry>,
    /// Receivers waiting for a value.
    pub(super) recv_waiters: WaitQueue<Retry>,
    /// At least the length of `send_waiters`; see the module docs.
    send_hint: AtomicUsize,
    /// At least the length of `recv_waiters`; see the module docs.
    recv_hint: AtomicUsize,
}

impl<T> Chan<T> {
    /// A channel with one open sender and one open receiver.
    pub(super) fn new(capacity: Option<usize>) -> Self {
        let buffer = match capacity {
            Some(capacity) => Buffer::Bounded(ArrayQueue::new(capacity)),
            None => Buffer::Unbounded(Mutex::new(VecDeque::new())),
        };
        Self {
            buffer,
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            send_waiters: WaitQueue::new(Retry),
            recv_waiters: WaitQueue::new(Retry),
            send_hint: AtomicUsize::new(0),
            recv_hint: AtomicUsize::new(0),
        }
    }

    pub(super) fn capacity(&self) -> Option<usize> {
        self.buffer.capacity()
    }

    pub(super) fn len(&self) -> usize {
        self.buffer.len()
    }

    pub(super) fn receivers_closed(&self) -> bool {
        self.receivers.load(Ordering::Acquire) == 0
    }

    pub(super) fn senders_closed(&self) -> bool {
        self.senders.load(Ordering::Acquire) == 0
    }

    /// Pushes `value` and wakes a waiting receiver.
    pub(super) fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.push(value)?;
        self.wake_receiver();
        Ok(())
    }

    /// Pops a value and wakes a waiting sender.
    pub(super) fn try_recv(&self) -> Result<T, TryRecvError> {
        let value = self.pop()?;
        self.wake_sender();
        Ok(value)
    }

    fn push(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.receivers_closed() {
            return Err(TrySendError::Closed(value));
        }
        self.buffer.push(value).map_err(TrySendError::Full)?;
        if self.receivers_closed() {
            // The last receiver closed during the push, possibly after its drain: nobody can
            // receive what landed, so free it now rather than when the channel drops.
            self.drain();
        }
        Ok(())
    }

    /// Drops every buffered value. Only once every receiver is closed.
    fn drain(&self) {
        while self.buffer.pop().is_some() {}
    }

    fn pop(&self) -> Result<T, TryRecvError> {
        if let Some(value) = self.buffer.pop() {
            return Ok(value);
        }
        if !self.senders_closed() {
            return Err(TryRecvError::Empty);
        }
        // The last sender closed after its final push (`Acquire` on the count): one more look
        // sees every value sent.
        self.buffer.pop().ok_or(TryRecvError::Closed)
    }

    /// After a push: wakes one waiting receiver, if any.
    pub(super) fn wake_receiver(&self) {
        fence(Ordering::SeqCst);
        if self.recv_hint.load(Ordering::Relaxed) != 0 {
            let mut waiters = self.recv_waiters.lock();
            waiters.grant_front();
            self.recv_hint.store(waiters.len(), Ordering::Relaxed);
        }
    }

    /// After a pop: wakes one waiting sender, if any.
    pub(super) fn wake_sender(&self) {
        fence(Ordering::SeqCst);
        if self.send_hint.load(Ordering::Relaxed) != 0 {
            let mut waiters = self.send_waiters.lock();
            waiters.grant_front();
            self.send_hint.store(waiters.len(), Ordering::Relaxed);
        }
    }

    /// A sender's slow path, under the `send_waiters` lock: the last try before queueing.
    ///
    /// Returns `true` if the sender need not queue: `value` was pushed (it is left `None`; the
    /// caller wakes a receiver once the lock is released) or the channel is closed (`value`
    /// is put back). Returns `false`, with `value` put back, if the buffer is still full.
    pub(super) fn push_or_queue(&self, waiters: &Locked<'_, Retry>, value: &mut Option<T>) -> bool {
        self.send_hint.store(waiters.len() + 1, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        let taken = value.take().expect("a pending send holds its value");
        match self.push(taken) {
            Ok(()) => {}
            Err(TrySendError::Closed(back)) => *value = Some(back),
            Err(TrySendError::Full(back)) => {
                *value = Some(back);
                return false;
            }
        }
        self.send_hint.store(waiters.len(), Ordering::Relaxed);
        true
    }

    /// A receiver's slow path, under the `recv_waiters` lock: the last try before queueing.
    ///
    /// Returns `true` if the receiver need not queue: a value was popped into `value` (the
    /// caller wakes a sender once the lock is released) or the channel has ended. Returns
    /// `false` if the buffer is still empty.
    pub(super) fn pop_or_queue(&self, waiters: &Locked<'_, Retry>, value: &mut Option<T>) -> bool {
        self.recv_hint.store(waiters.len() + 1, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        match self.pop() {
            Ok(popped) => *value = Some(popped),
            Err(TryRecvError::Closed) => {}
            Err(TryRecvError::Empty) => return false,
        }
        self.recv_hint.store(waiters.len(), Ordering::Relaxed);
        true
    }

    pub(super) fn open_sender(&self) {
        self.senders.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn open_receiver(&self) {
        self.receivers.fetch_add(1, Ordering::Relaxed);
    }

    /// Closes one sender handle. The last one ends the channel: every waiting receiver wakes,
    /// drains what is left and then sees the end.
    pub(super) fn close_sender(&self) {
        if self.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            let mut waiters = self.recv_waiters.lock();
            waiters.grant_all();
            self.recv_hint.store(0, Ordering::Relaxed);
        }
    }

    /// Closes one receiver handle. The last one fails every waiting and future send, and
    /// drops the buffered values nobody can receive any more.
    pub(super) fn close_receiver(&self) {
        if self.receivers.fetch_sub(1, Ordering::AcqRel) == 1 {
            {
                let mut waiters = self.send_waiters.lock();
                waiters.grant_all();
                self.send_hint.store(0, Ordering::Relaxed);
            }
            // Dropped outside any lock: a value's drop may run arbitrary code. A send that
            // races with the close may still land a value; it drains it (see `push`).
            self.drain();
        }
    }
}

impl<T> fmt::Debug for Chan<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Chan")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("senders", &self.senders.load(Ordering::Relaxed))
            .field("receivers", &self.receivers.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}
