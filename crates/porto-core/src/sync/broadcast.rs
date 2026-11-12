//! A channel where every receiver gets every message (`16-sync-primitives-and-channels.md` §2).
//!
//! [`channel`] keeps the last `capacity` messages in a ring. [`Sender::send`] appends one and
//! never waits: when the ring is full, the oldest message is overwritten. Each [`Receiver`]
//! reads the messages in order from its own position, so every receiver sees every message
//! sent after it was made, unless it falls behind: a receiver more than `capacity` messages
//! behind gets [`TryRecvError::Lagged`] / [`RecvError::Lagged`] with the exact number of
//! messages it missed, and its next receive returns the oldest message still kept.
//!
//! Messages are shared (`Arc<T>`), so receiving one is a reference-count increment and `T`
//! need not be `Clone`.
//!
//! # Positions
//!
//! Message `i` (counting from 0) is kept while `i >= sent - capacity`, where `sent` is how many
//! messages were sent. A receiver's position is the index of the next message it reads: a
//! receiver made by [`channel`] or [`Sender::subscribe`] starts at `sent` (it sees only later
//! messages), a clone starts where its original is. When the position is below `sent -
//! capacity`, the receive reports the difference as `Lagged` and moves the position up to
//! `sent - capacity`. Counts are exact: the messages a receiver gets plus the `skipped` it is
//! told add up to everything sent since its position started.
//!
//! # Closure (§2.3)
//!
//! When every sender is closed or dropped, a receiver drains the messages still kept and then
//! gets [`RecvError::Closed`]. A send fails with [`SendError::Broken`] while no receiver is
//! open; unlike `mpsc`, that is not permanent, as [`Sender::subscribe`] makes a new receiver
//! (a publisher with no subscribers yet is normal).
//!
//! # Cancellation
//!
//! A receive moves its position only when it returns a message (or `Lagged`); a wake only says
//! "look again". A cancelled receive therefore loses nothing, and passes nothing on, because
//! every send wakes every waiting receiver.
//!
//! ```
//! use porto_core::sync::broadcast;
//!
//! let rt = porto_core::Runtime::new();
//! let (tx, rx) = broadcast::channel(8);
//! let rx2 = rx.clone();
//! let total = rt.block_on(async move {
//!     for i in 1..=3 {
//!         tx.send(i).unwrap();
//!     }
//!     drop(tx);
//!     let mut total = 0;
//!     for receiver in [rx, rx2] {
//!         while let Ok(value) = receiver.recv().await {
//!             total += *value;
//!         }
//!     }
//!     total
//! });
//! assert_eq!(total, 12, "both receivers saw 1, 2 and 3");
//! ```

use std::error::Error;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use super::primitives::{Arc, AtomicBool, AtomicU64, AtomicUsize};
pub use super::send_error::SendError;
use super::{BlockingOnLane, Locked, WaitNode, WaitQueue, WaitState, blocking_wait};

/// The largest capacity [`channel`] accepts. The ring grows as messages are sent, so a large
/// capacity costs nothing until it is used.
pub const MAX_CAPACITY: usize = usize::MAX / 4;

/// The state behind the queue lock: the ring. The count of messages sent is the queue's word.
struct Ring<T> {
    /// Message `i` is at `slots[i % capacity]`; the ring grows to `capacity` as messages are
    /// sent, so a large capacity costs nothing until it is used.
    slots: Vec<Arc<T>>,
    capacity: usize,
}

impl<T> WaitState for Ring<T> {
    /// Unreachable: waiters are only ever woken with `grant_all` (shared wakes).
    fn pass_grant(_: &mut Locked<'_, Self>, _: usize) {}
}

struct Shared<T> {
    ring: WaitQueue<Ring<T>>,
    /// Open sender handles; the channel ends for receivers when it reaches zero.
    senders: AtomicUsize,
    /// Open receiver handles; sends fail while it is zero.
    receivers: AtomicUsize,
}

impl<T> Shared<T> {
    /// How many messages were sent.
    fn sent(&self) -> u64 {
        self.ring.word().load(Ordering::Acquire)
    }

    fn senders_closed(&self) -> bool {
        self.senders.load(Ordering::Acquire) == 0
    }

    /// Wakes every waiter, so each looks again.
    fn wake_all(&self) {
        self.ring.lock().grant_all();
    }
}

/// A broadcast channel keeping the last `capacity` messages.
///
/// # Panics
/// If `capacity` is zero or above [`MAX_CAPACITY`].
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(
        (1..=MAX_CAPACITY).contains(&capacity),
        "porto: broadcast capacity must be between 1 and {MAX_CAPACITY} (got {capacity}); pass \
         how many messages the channel keeps for receivers that fall behind"
    );
    let shared = Arc::new(Shared {
        ring: WaitQueue::new(Ring {
            slots: Vec::new(),
            capacity,
        }),
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
    });
    (
        Sender {
            shared: shared.clone(),
            closed: AtomicBool::new(false),
        },
        Receiver {
            shared,
            next: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        },
    )
}

/// Counts one more open handle, unless the last one already closed.
fn open_if_any(count: &AtomicUsize) -> bool {
    let mut open = count.load(Ordering::Relaxed);
    while open != 0 {
        match count.compare_exchange_weak(open, open + 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(actual) => open = actual,
        }
    }
    false
}

/// [`Receiver::recv`] got no message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// Every sender was closed or dropped and every kept message was received, or this
    /// receiver was closed. The Python layer raises `porto.EndOfChannel` (or
    /// `porto.ClosedResourceError` for a closed handle).
    Closed,
    /// The receiver fell behind and this many messages were overwritten before it read them.
    /// The next receive returns the oldest message still kept. The Python layer raises
    /// `porto.Lagged`.
    Lagged(u64),
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str(
                "the channel has ended: every sender was closed or dropped and every message \
                 was received; stop receiving (this is the normal end of the stream)",
            ),
            Self::Lagged(skipped) => lagged(f, *skipped),
        }
    }
}

impl Error for RecvError {}

/// [`Receiver::try_recv`] got no message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// Nothing new was sent, but senders remain. The Python layer raises `porto.WouldBlock`.
    Empty,
    /// As for [`RecvError::Closed`].
    Closed,
    /// As for [`RecvError::Lagged`].
    Lagged(u64),
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => {
                f.write_str("no new message; await recv() to wait for one, or try again later")
            }
            Self::Closed => RecvError::Closed.fmt(f),
            Self::Lagged(skipped) => lagged(f, *skipped),
        }
    }
}

impl Error for TryRecvError {}

fn lagged(f: &mut fmt::Formatter<'_>, skipped: u64) -> fmt::Result {
    write!(
        f,
        "this receiver fell behind and missed {skipped} messages; keep receiving to continue \
         from the oldest message still kept, or give the channel a larger capacity if no \
         message may be lost"
    )
}

/// The sending end of a broadcast channel. Clone it for more producers.
///
/// Dropping a sender closes it. Once every sender is closed, receivers drain what is kept and
/// then see the end.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    /// Set once, by `close` (or a clone of a closed handle).
    closed: AtomicBool,
}

impl<T> Sender<T> {
    /// Sends `message` to every receiver. Never waits: if the ring is full, the oldest message
    /// is overwritten (receivers that had not read it get `Lagged`).
    ///
    /// # Errors
    /// [`SendError::Closed`] if this sender was closed, [`SendError::Broken`] if no receiver is
    /// open. Both return the message.
    pub fn send(&self, message: T) -> Result<(), SendError<T>> {
        if self.is_handle_closed() {
            return Err(SendError::Closed(message));
        }
        if self.shared.receivers.load(Ordering::Acquire) == 0 {
            return Err(SendError::Broken(message));
        }
        let message = Arc::new(message);
        let evicted = {
            let mut ring = self.shared.ring.lock();
            let sent = self.shared.sent();
            let evicted = if ring.slots.len() < ring.capacity {
                ring.slots.push(message);
                None
            } else {
                let index = usize::try_from(sent % ring.capacity as u64)
                    .expect("an index below the capacity fits usize");
                Some(std::mem::replace(&mut ring.slots[index], message))
            };
            ring.word().store(sent + 1, Ordering::Release);
            ring.grant_all();
            evicted
        };
        // Outside the lock: a message's drop may run arbitrary code (it may use this channel).
        drop(evicted);
        Ok(())
    }

    /// A new receiver that starts at the next message sent: it sees nothing that was sent
    /// before. Works while no other receiver is open.
    ///
    /// Returns `None` if this sender was closed.
    pub fn subscribe(&self) -> Option<Receiver<T>> {
        if self.is_handle_closed() {
            return None;
        }
        self.shared.receivers.fetch_add(1, Ordering::AcqRel);
        Some(Receiver {
            shared: self.shared.clone(),
            next: AtomicU64::new(self.shared.sent()),
            closed: AtomicBool::new(false),
        })
    }

    /// Closes this sender, as dropping it would; other clones are unaffected. Once every sender
    /// is closed, receivers drain what is kept and then see the end. Closing twice does
    /// nothing.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel)
            && self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1
        {
            self.shared.wake_all();
        }
    }

    /// Whether this handle was closed ([`Sender::close`]), as opposed to the receivers going
    /// away.
    pub fn is_handle_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Whether a send through this sender fails: it was closed, or no receiver is open.
    pub fn is_closed(&self) -> bool {
        self.is_handle_closed() || self.shared.receivers.load(Ordering::Acquire) == 0
    }

    /// How many messages the channel keeps for receivers that fall behind.
    pub fn capacity(&self) -> usize {
        self.shared.ring.lock().capacity
    }
}

impl<T> Clone for Sender<T> {
    /// Another sender on the same channel. A clone of a closed sender is closed too.
    fn clone(&self) -> Self {
        let open = !self.is_handle_closed() && open_if_any(&self.shared.senders);
        Self {
            shared: self.shared.clone(),
            closed: AtomicBool::new(!open),
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
            .field("closed", &self.is_handle_closed())
            .field("sent", &self.shared.sent())
            .finish_non_exhaustive()
    }
}

/// The receiving end of a broadcast channel. Clone it for another receiver at the same
/// position: each one gets every message.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The index of the next message this receiver reads. Written under the ring's lock.
    next: AtomicU64,
    /// Set once, by `close` (or a clone of a closed handle).
    closed: AtomicBool,
}

impl<T> Receiver<T> {
    /// Receives the next message, waiting while there is none. Dropping the future before it
    /// completes cancels the receive and loses nothing.
    ///
    /// # Errors
    /// [`RecvError::Lagged`] if messages were overwritten before this receiver read them (the
    /// next call continues from the oldest one kept), [`RecvError::Closed`] once every sender
    /// is closed and every kept message was received, or if this receiver was closed.
    pub async fn recv(&self) -> Result<Arc<T>, RecvError> {
        loop {
            match self.try_recv() {
                Ok(message) => return Ok(message),
                Err(TryRecvError::Closed) => return Err(RecvError::Closed),
                Err(TryRecvError::Lagged(skipped)) => return Err(RecvError::Lagged(skipped)),
                Err(TryRecvError::Empty) => {}
            }
            self.shared
                .ring
                .wait(0, |_: &mut Locked<'_, Ring<T>>| self.ready())
                .await;
        }
    }

    /// Receives the next message if one was sent.
    ///
    /// # Errors
    /// [`TryRecvError::Empty`] if nothing new was sent, otherwise as for [`Receiver::recv`].
    pub fn try_recv(&self) -> Result<Arc<T>, TryRecvError> {
        if self.is_handle_closed() {
            return Err(TryRecvError::Closed);
        }
        // `senders_closed` first: a send precedes the close, so a position at the sent count
        // read after it is really the end.
        let ended = self.shared.senders_closed();
        if self.next.load(Ordering::Acquire) == self.shared.sent() {
            return Err(if ended {
                TryRecvError::Closed
            } else {
                TryRecvError::Empty
            });
        }
        let ring = self.shared.ring.lock();
        let sent = self.shared.sent();
        let next = self.next.load(Ordering::Acquire);
        if next == sent {
            // Another call on this handle took it.
            return Err(if ended {
                TryRecvError::Closed
            } else {
                TryRecvError::Empty
            });
        }
        let oldest = sent.saturating_sub(ring.capacity as u64);
        if next < oldest {
            self.next.store(oldest, Ordering::Release);
            return Err(TryRecvError::Lagged(oldest - next));
        }
        let index = usize::try_from(next % ring.capacity as u64)
            .expect("an index below the capacity fits usize");
        let message = ring.slots[index].clone();
        self.next.store(next + 1, Ordering::Release);
        Ok(message)
    }

    /// [`Receiver::recv`] for a thread that does not run a porto lane: blocks the thread.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread; await [`Receiver::recv`] there.
    pub fn blocking_recv(&self) -> Result<Result<Arc<T>, RecvError>, BlockingOnLane> {
        blocking_wait("BroadcastReceiver.blocking_recv", self.recv())
    }

    /// Closes this receiver, as dropping it would: receives through it end from now on
    /// (including waits in progress); other receivers are unaffected. Closing twice does
    /// nothing.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.shared.receivers.fetch_sub(1, Ordering::AcqRel);
            // A task parked through this handle must see the close.
            self.shared.wake_all();
        }
    }

    /// Whether this handle was closed ([`Receiver::close`]).
    pub fn is_handle_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Whether a wait can end now: a message (or a lag) is pending, the channel ended or this
    /// handle closed.
    fn ready(&self) -> bool {
        self.is_handle_closed()
            || self.next.load(Ordering::Acquire) != self.shared.sent()
            || self.shared.senders_closed()
    }

    /// Waits with a caller-owned node until a `try_recv` may finish: a message (or a lag) is
    /// pending, the channel ended, or this handle closed (the retry reports it). `Ready` is a
    /// hint to retry, not a reservation.
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
        unsafe { self.shared.ring.poll_wait(node, cx, |_| self.ready()) }
    }

    /// Stops a [`Receiver::poll_ready`] wait. A wake it had not observed is dropped: it only
    /// said "look again".
    ///
    /// # Safety
    /// As for [`Receiver::poll_ready`].
    pub unsafe fn cancel_ready(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.shared.ring.cancel(node) }
    }
}

impl<T> Clone for Receiver<T> {
    /// Another receiver at the same position: it also gets every message from there. A clone
    /// of a closed receiver is closed too.
    fn clone(&self) -> Self {
        let open = !self.is_handle_closed();
        if open {
            self.shared.receivers.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            shared: self.shared.clone(),
            next: AtomicU64::new(self.next.load(Ordering::Acquire)),
            closed: AtomicBool::new(!open),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Nothing waits through a handle being dropped (waits borrow it), so only the count
        // closes.
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.shared.receivers.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("closed", &self.is_handle_closed())
            .field("next", &self.next.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}
