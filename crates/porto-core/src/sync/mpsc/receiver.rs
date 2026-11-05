//! The receiving end and its `recv` future.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use super::super::WaitNode;
use super::super::primitives::{Arc, AtomicBool};
use super::chan::Chan;
use super::error::{RecvError, TryRecvError};

/// The receiving end of a channel. Clone it for more consumers: each value goes to exactly
/// one receiver.
///
/// Dropping a receiver closes it. Once every receiver is closed, sends fail and the values
/// still buffered are dropped.
pub struct Receiver<T> {
    chan: Arc<Chan<T>>,
    /// Set once, by `close` (or a clone of a closed handle).
    closed: AtomicBool,
}

impl<T> Receiver<T> {
    pub(super) fn new(chan: Arc<Chan<T>>) -> Self {
        Self {
            chan,
            closed: AtomicBool::new(false),
        }
    }

    /// Receives the next value, waiting while the channel is empty.
    ///
    /// Waiting receivers are woken one per sent value, oldest first; a receiver that is not
    /// waiting yet may still take the value first. Dropping the future before it completes
    /// cancels the receive and loses no value.
    ///
    /// # Errors
    /// [`RecvError`] once every sender is closed and every buffered value was received, or
    /// if this receiver was closed.
    pub fn recv(&self) -> RecvFuture<'_, T> {
        RecvFuture {
            receiver: self,
            node: WaitNode::new(0),
            done: false,
        }
    }

    /// Receives the next value if one is buffered now.
    ///
    /// # Errors
    /// [`TryRecvError::Empty`] if nothing is buffered but senders remain,
    /// [`TryRecvError::Closed`] as for [`Receiver::recv`].
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if self.is_handle_closed() {
            return Err(TryRecvError::Closed);
        }
        self.chan.try_recv()
    }

    /// Waits for at least one value, then moves up to `limit` buffered values into `buffer`
    /// without waiting again. Returns how many were added: `0` means the channel has ended
    /// (or `limit` is `0`). Batching amortizes the per-value wake-up cost.
    pub async fn recv_many(&self, buffer: &mut Vec<T>, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        let Ok(first) = self.recv().await else {
            return 0;
        };
        buffer.push(first);
        let mut received = 1;
        while received < limit {
            let Ok(value) = self.try_recv() else { break };
            buffer.push(value);
            received += 1;
        }
        received
    }

    /// Closes this receiver, as dropping it would. Receives through it end from now on;
    /// other clones are unaffected. Closing twice does nothing.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.chan.close_receiver();
        }
    }

    /// Whether this handle was closed ([`Receiver::close`]), as opposed to the channel being
    /// closed from the other side.
    pub fn is_handle_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Waits with a caller-owned node until a `try_recv` may succeed: a value is buffered, or the channel has ended (the retry reports it). `Ready` is a hint to retry, not a
    /// reservation: another handle may take it first, and the caller then waits again.
    /// Returns `Ready` at once on a closed handle.
    ///
    /// This is the building block for waiters that keep their node elsewhere than in a
    /// future (the Python binding embeds one in each task); [`Receiver::recv`] is the usual way to
    /// wait.
    ///
    /// # Safety
    /// As for [`WaitQueue::poll_wait`](super::super::WaitQueue::poll_wait): the node is only
    /// used with this channel (through any of its handles) until a `Ready` or
    /// [`Receiver::cancel_ready`], and this handle outlives that period.
    pub unsafe fn poll_ready(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        let chan = &*self.chan;
        // SAFETY: the caller's contract.
        unsafe {
            chan.recv_waiters.poll_wait(node, cx, |waiters| {
                self.is_handle_closed() || chan.readable_or_queue(waiters)
            })
        }
    }

    /// Stops a [`Receiver::poll_ready`] wait: unlinks the node, or passes on a wake it has not
    /// observed, so the room or value it announced goes to the next waiter.
    ///
    /// # Safety
    /// As for [`Receiver::poll_ready`].
    pub unsafe fn cancel_ready(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.chan.recv_waiters.cancel(node) }
    }

    /// The channel's capacity, or `None` for an unbounded channel.
    pub fn capacity(&self) -> Option<usize> {
        self.chan.capacity()
    }

    /// The number of buffered values (a snapshot when other handles are in use).
    pub fn len(&self) -> usize {
        self.chan.len()
    }

    /// Whether no value is buffered (a snapshot when other handles are in use).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Clone for Receiver<T> {
    /// Another receiver on the same channel. A clone of a closed receiver is closed too.
    fn clone(&self) -> Self {
        let open = !self.is_handle_closed() && self.chan.open_receiver();
        Self {
            chan: self.chan.clone(),
            closed: AtomicBool::new(!open),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("chan", &self.chan)
            .field("closed", &self.is_handle_closed())
            .finish()
    }
}

/// The future returned by [`Receiver::recv`].
#[must_use = "futures do nothing unless polled"]
pub struct RecvFuture<'a, T> {
    receiver: &'a Receiver<T>,
    /// Queued in the channel's `recv_waiters` while the channel is empty.
    node: WaitNode,
    done: bool,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `node` is never moved out of the pinned future.
        let this = unsafe { self.get_unchecked_mut() };
        assert!(
            !this.done,
            "porto: a channel recv future was polled after it completed; await it only once"
        );
        // SAFETY: `node` is structurally pinned (see above).
        let node = unsafe { Pin::new_unchecked(&this.node) };
        let chan = &*this.receiver.chan;
        if node.is_pending() {
            // Queued by an earlier poll: wait for the wake (or consume it) before retrying, so
            // a wake is never left on a future that no longer needs it.
            // SAFETY: as below.
            let poll = unsafe { chan.recv_waiters.poll_wait(node, cx, |_| false) };
            if poll.is_pending() {
                return Poll::Pending;
            }
        }
        loop {
            match this.receiver.try_recv() {
                Ok(value) => {
                    this.done = true;
                    return Poll::Ready(Ok(value));
                }
                Err(TryRecvError::Closed) => {
                    this.done = true;
                    return Poll::Ready(Err(RecvError));
                }
                Err(TryRecvError::Empty) => {}
            }
            let mut value = None;
            // SAFETY: `&mut self` serializes calls on the node; it is only ever used with this
            // channel's `recv_waiters`, which the borrowed receiver keeps alive, and `Drop`
            // cancels a pending wait.
            let poll = unsafe {
                chan.recv_waiters
                    .poll_wait(node, cx, |waiters| chan.pop_or_queue(waiters, &mut value))
            };
            match (poll, value) {
                // Popped under the lock: wake a sender now that the lock is released.
                (Poll::Ready(()), Some(value)) => {
                    chan.wake_sender();
                    this.done = true;
                    return Poll::Ready(Ok(value));
                }
                // The channel ended under the lock: the retry reports it.
                (Poll::Ready(()), None) => {}
                (Poll::Pending, _) => return Poll::Pending,
            }
        }
    }
}

impl<T> Drop for RecvFuture<'_, T> {
    fn drop(&mut self) {
        // SAFETY: the node was pinned since the first poll and is dropped right after this;
        // `&mut self` serializes; it was only used with this channel's `recv_waiters`.
        unsafe {
            self.receiver
                .chan
                .recv_waiters
                .cancel(Pin::new_unchecked(&self.node));
        }
    }
}

impl<T> fmt::Debug for RecvFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvFuture")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}
