//! A channel for one value (`16-sync-primitives-and-channels.md` §2).
//!
//! [`channel`] returns a [`Sender`] that sends at most one value and a [`Receiver`] that
//! receives it. Sending never waits (there is always room for the one value), so
//! [`Sender::send`] is a plain call.
//!
//! - The receiver sees [`RecvError`] if the sender was closed or dropped without sending.
//! - The sender sees [`SendError::Broken`], with the value, if the receiver was closed or
//!   dropped first.
//! - A handle that was itself closed (or a sender that already sent) fails with
//!   [`SendError::Closed`] / [`TryRecvError::Closed`] on its own side: the Python layer tells
//!   this apart from the other side going away (`ClosedResourceError` versus
//!   `EndOfChannel` / `BrokenResourceError`) through [`Receiver::is_handle_closed`].
//!
//! # Cancellation
//!
//! The value sits in the slot until a receive takes it, and every wake is "the slot changed":
//! a waiter that is woken and then cancelled has taken nothing, so there is no grant to pass
//! on and nothing to lose. Dropping a [`Receiver::recv`] future at any point loses no value:
//! the next receive on the channel finds it. Several tasks may wait on one receiver; the value
//! goes to the one that takes it first, and the others then see the end of the channel.
//!
//! ```
//! use porto_core::sync::oneshot;
//!
//! let rt = porto_core::Runtime::new();
//! let (tx, rx) = oneshot::channel();
//! let value = rt.block_on(async move {
//!     porto_core::spawn(async move {
//!         tx.send(42).unwrap();
//!     });
//!     rx.recv().await.unwrap()
//! });
//! assert_eq!(value, 42);
//! ```

use std::fmt;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

pub use super::mpsc::{RecvError, TryRecvError};
use super::primitives::{Arc, AtomicBool};
pub use super::send_error::SendError;
use super::{BlockingOnLane, Locked, WaitNode, WaitQueue, WaitState, blocking_wait};

/// The state behind the queue lock.
struct Slot<T> {
    /// The sent value, until a receive takes it (or the receiver closes).
    value: Option<T>,
    /// The sender sent, closed or was dropped: nothing more will arrive.
    sender_gone: bool,
    /// The receiver closed or was dropped.
    receiver_gone: bool,
}

impl<T> WaitState for Slot<T> {
    /// Unreachable: waiters are only ever woken with `grant_all` (shared wakes).
    fn pass_grant(_: &mut Locked<'_, Self>, _: usize) {}
}

/// A channel for one value: a sender, a receiver. Neither clones.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(WaitQueue::new(Slot {
        value: None,
        sender_gone: false,
        receiver_gone: false,
    }));
    (
        Sender {
            shared: shared.clone(),
            closed: AtomicBool::new(false),
        },
        Receiver {
            shared,
            closed: AtomicBool::new(false),
        },
    )
}

/// The sending end of a oneshot channel.
///
/// Dropping it (or [`close`](Sender::close)) without sending ends the channel for the
/// receiver.
pub struct Sender<T> {
    shared: Arc<WaitQueue<Slot<T>>>,
    /// Set by `close`, and by the first `send` (which uses the sender up).
    closed: AtomicBool,
}

impl<T> Sender<T> {
    /// Sends `value`, which a waiting or later receive takes. Uses the sender up: sending again
    /// fails.
    ///
    /// # Errors
    /// [`SendError::Closed`] if this sender was closed or already sent, [`SendError::Broken`]
    /// if the receiver is gone. Both return the value.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Err(SendError::Closed(value));
        }
        let mut slot = self.shared.lock();
        slot.sender_gone = true;
        if slot.receiver_gone {
            drop(slot);
            return Err(SendError::Broken(value));
        }
        slot.value = Some(value);
        slot.grant_all();
        Ok(())
    }

    /// Closes this sender without sending, as dropping it would: the receiver sees the end of
    /// the channel. Closing twice, or after sending, does nothing.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let mut slot = self.shared.lock();
            slot.sender_gone = true;
            slot.grant_all();
        }
    }

    /// Whether this handle was closed or used up, as opposed to the receiver going away.
    pub fn is_handle_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Whether a send through this sender would fail: it was closed or used up, or the receiver
    /// is gone.
    pub fn is_closed(&self) -> bool {
        self.is_handle_closed() || self.shared.lock().receiver_gone
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
            .field("closed", &self.is_handle_closed())
            .finish_non_exhaustive()
    }
}

/// The receiving end of a oneshot channel.
///
/// Dropping it (or [`close`](Receiver::close)) makes sends fail and drops a value that was
/// sent but not received.
pub struct Receiver<T> {
    shared: Arc<WaitQueue<Slot<T>>>,
    /// Set by `close`.
    closed: AtomicBool,
}

/// Whether a receive can finish now (with the value or the end): the first-poll check of a
/// wait, under the lock.
fn finished<T>(slot: &Slot<T>) -> bool {
    slot.value.is_some() || slot.sender_gone
}

impl<T> Receiver<T> {
    /// Receives the value, waiting until it is sent. Dropping the future before it completes
    /// cancels the receive and loses nothing.
    ///
    /// # Errors
    /// [`RecvError`] if the sender was closed or dropped without sending, if the value was
    /// already received, or if this receiver was closed.
    pub async fn recv(&self) -> Result<T, RecvError> {
        loop {
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Closed) => return Err(RecvError),
                Err(TryRecvError::Empty) => {}
            }
            self.shared
                .wait(0, |slot: &mut Locked<'_, Slot<T>>| {
                    self.is_handle_closed() || finished(slot)
                })
                .await;
        }
    }

    /// Receives the value if it was sent.
    ///
    /// # Errors
    /// [`TryRecvError::Empty`] if nothing was sent yet, [`TryRecvError::Closed`] as for
    /// [`Receiver::recv`].
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if self.is_handle_closed() {
            return Err(TryRecvError::Closed);
        }
        let mut slot = self.shared.lock();
        if let Some(value) = slot.value.take() {
            // The value is gone: the other tasks waiting through this handle see the end.
            slot.grant_all();
            return Ok(value);
        }
        if slot.sender_gone {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// [`Receiver::recv`] for a thread that does not run a porto lane: blocks the thread.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread; await [`Receiver::recv`] there.
    pub fn blocking_recv(&self) -> Result<Result<T, RecvError>, BlockingOnLane> {
        blocking_wait("Receiver.blocking_recv", self.recv())
    }

    /// Closes this receiver, as dropping it would: receives through it end from now on
    /// (including ones already waiting), sends fail, and a value that was sent but not
    /// received is dropped. Closing twice does nothing.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let value = {
                let mut slot = self.shared.lock();
                slot.receiver_gone = true;
                slot.grant_all();
                slot.value.take()
            };
            // Outside the lock: a value's drop may run arbitrary code.
            drop(value);
        }
    }

    /// Whether this handle was closed ([`Receiver::close`]).
    pub fn is_handle_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Waits with a caller-owned node until a `try_recv` finishes: a value was sent, or the
    /// channel ended, or this handle closed (the retry reports it). `Ready` is a hint to retry,
    /// not a reservation: another task may take the value first, and the caller then retries
    /// and sees the end.
    ///
    /// This is the building block for waiters that keep their node elsewhere than in a future
    /// (the Python binding embeds one in each task); [`Receiver::recv`] is the usual way to
    /// wait.
    ///
    /// # Safety
    /// As for [`WaitQueue::poll_wait`]: the node is only used with this channel until a `Ready`
    /// or [`Receiver::cancel_ready`], and this handle outlives that period.
    pub unsafe fn poll_ready(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe {
            self.shared
                .poll_wait(node, cx, |slot| self.is_handle_closed() || finished(slot))
        }
    }

    /// Stops a [`Receiver::poll_ready`] wait. A wake it had not observed is dropped: it only
    /// said "look again", and the value stays in the slot.
    ///
    /// # Safety
    /// As for [`Receiver::poll_ready`].
    pub unsafe fn cancel_ready(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.shared.cancel(node) }
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
            .field("closed", &self.is_handle_closed())
            .finish_non_exhaustive()
    }
}
