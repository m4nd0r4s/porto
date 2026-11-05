//! The sending end and its `send` future.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::super::WaitNode;
use super::super::primitives::Arc;
use super::chan::Chan;
use super::error::{SendError, TrySendError};

/// The sending end of a channel. Clone it for more producers.
///
/// Dropping a sender closes it. Once every sender is closed, receivers drain the buffer and
/// then see the end of the channel.
pub struct Sender<T> {
    chan: Arc<Chan<T>>,
    closed: bool,
}

impl<T> Sender<T> {
    pub(super) fn new(chan: Arc<Chan<T>>) -> Self {
        Self {
            chan,
            closed: false,
        }
    }

    /// Sends `value`, waiting for room while the channel is full.
    ///
    /// Waiting senders are woken one per received value, oldest first; a sender that is not
    /// waiting yet may still take the room first. Dropping the future before it completes
    /// cancels the send and drops the value.
    ///
    /// # Errors
    /// [`SendError`], with the value, if the channel is closed: every receiver is gone, or
    /// this sender was closed.
    pub fn send(&self, value: T) -> SendFuture<'_, T> {
        SendFuture {
            sender: self,
            value: Some(value),
            node: WaitNode::new(0),
        }
    }

    /// Sends `value` if there is room now.
    ///
    /// # Errors
    /// [`TrySendError::Full`] if the channel is at capacity, [`TrySendError::Closed`] as for
    /// [`Sender::send`]. Both return the value.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.closed {
            return Err(TrySendError::Closed(value));
        }
        self.chan.try_send(value)
    }

    /// Closes this sender, as dropping it would. Sends through it fail from now on; other
    /// clones are unaffected. Closing twice does nothing.
    pub fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.chan.close_sender();
        }
    }

    /// Whether sends through this sender fail: it was closed, or every receiver is gone.
    pub fn is_closed(&self) -> bool {
        self.closed || self.chan.receivers_closed()
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

impl<T> Clone for Sender<T> {
    /// Another sender on the same channel. A clone of a closed sender is closed too.
    fn clone(&self) -> Self {
        if !self.closed {
            self.chan.open_sender();
        }
        Self {
            chan: self.chan.clone(),
            closed: self.closed,
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("chan", &self.chan)
            .field("closed", &self.closed)
            .finish()
    }
}

/// The future returned by [`Sender::send`].
#[must_use = "futures do nothing unless polled"]
pub struct SendFuture<'a, T> {
    sender: &'a Sender<T>,
    /// `None` once the value was sent (or handed back).
    value: Option<T>,
    /// Queued in the channel's `send_waiters` while the channel is full.
    node: WaitNode,
}

impl<T> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `node` is never moved out of the pinned future; `value` is not pinned.
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: `node` is structurally pinned (see above).
        let node = unsafe { Pin::new_unchecked(&this.node) };
        let chan = &*this.sender.chan;
        let mut value = Some(this.value.take().expect(
            "porto: a channel send future was polled after it completed; await it only once",
        ));
        if node.is_pending() {
            // Queued by an earlier poll: wait for the wake (or consume it) before retrying, so
            // a wake is never left on a future that no longer needs it.
            // SAFETY: as below.
            let poll = unsafe { chan.send_waiters.poll_wait(node, cx, |_| false) };
            if poll.is_pending() {
                this.value = value;
                return Poll::Pending;
            }
        }
        loop {
            match this.sender.try_send(value.take().expect("held until sent")) {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(TrySendError::Closed(back)) => return Poll::Ready(Err(SendError(back))),
                Err(TrySendError::Full(back)) => value = Some(back),
            }
            // SAFETY: `&mut self` serializes calls on the node; it is only ever used with this
            // channel's `send_waiters`, which the borrowed sender keeps alive, and `Drop`
            // cancels a pending wait.
            let poll = unsafe {
                chan.send_waiters
                    .poll_wait(node, cx, |waiters| chan.push_or_queue(waiters, &mut value))
            };
            match (poll, value.is_some()) {
                // Pushed under the lock: wake a receiver now that the lock is released.
                (Poll::Ready(()), false) => {
                    chan.wake_receiver();
                    return Poll::Ready(Ok(()));
                }
                // The channel closed under the lock: the retry reports it.
                (Poll::Ready(()), true) => {}
                (Poll::Pending, _) => {
                    this.value = value;
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<T> Drop for SendFuture<'_, T> {
    fn drop(&mut self) {
        // SAFETY: the node was pinned since the first poll and is dropped right after this;
        // `&mut self` serializes; it was only used with this channel's `send_waiters`.
        unsafe {
            self.sender
                .chan
                .send_waiters
                .cancel(Pin::new_unchecked(&self.node));
        }
    }
}

impl<T> fmt::Debug for SendFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendFuture")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}
