//! A channel for the latest value (`16-sync-primitives-and-channels.md` §2).
//!
//! [`channel`] starts with an initial value. A [`Sender`] replaces it with [`Sender::send`]
//! (never waits; every send is a new *version*), and every [`Receiver`] can read the current
//! value ([`Receiver::borrow`]) or wait for a version it has not seen ([`Receiver::changed`]).
//! It is the clearable replacement for an event: `watch(false)`, `send(true)` to set it,
//! `send(false)` to clear it.
//!
//! # Versions
//!
//! The value starts at version 0. Each send makes the next version. A receiver remembers the
//! newest version it has *seen*: [`Receiver::changed`] completes once there is a newer one and
//! marks it seen (so the initial value is seen from the start, and several sends between two
//! `changed()` calls coalesce into one completion). [`Receiver::borrow`] only reads;
//! [`Receiver::borrow_and_update`] reads and marks seen. A receiver made by
//! [`Sender::subscribe`] has seen the current version; a clone has seen what its original had.
//!
//! # Closure (§2.3)
//!
//! When every sender is closed or dropped, a receiver still gets a change it had not seen,
//! then [`RecvError`]; [`Receiver::borrow`] keeps returning the last value. A send fails with
//! [`SendError::Broken`] while no receiver is open; unlike `mpsc`, that is not permanent, as
//! [`Sender::subscribe`] makes a new receiver.
//!
//! # Cancellation
//!
//! Waiting changes nothing: a wake only says "look again", and the version stays until a
//! `changed()` that completes marks it seen. A cancelled waiter loses no change, and passes
//! nothing on, because every waiter is woken by each send. Several tasks may wait on one
//! receiver; each version completes one of them. Give each task its own clone to have all of
//! them see every version.
//!
//! ```
//! use porto_core::sync::watch;
//!
//! let rt = porto_core::Runtime::new();
//! let (tx, rx) = watch::channel(0);
//! let seen = rt.block_on(async move {
//!     porto_core::spawn(async move {
//!         tx.send(1).unwrap();
//!         tx.send(2).unwrap();
//!     });
//!     let mut last = 0;
//!     while rx.changed().await.is_ok() {
//!         last = *rx.borrow_and_update();
//!     }
//!     last
//! });
//! assert_eq!(seen, 2);
//! ```

use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

pub use super::mpsc::{RecvError, TryRecvError};
use super::primitives::{Arc, AtomicBool, AtomicU64, AtomicUsize};
pub use super::send_error::SendError;
use super::{BlockingOnLane, Locked, WaitNode, WaitQueue, WaitState, blocking_wait};

/// The state behind the queue lock: the latest value. Its version is the queue's word.
struct Latest<T> {
    value: T,
}

impl<T> WaitState for Latest<T> {
    /// Unreachable: waiters are only ever woken with `grant_all` (shared wakes).
    fn pass_grant(_: &mut Locked<'_, Self>, _: usize) {}
}

struct Shared<T> {
    /// The latest value (under the lock) and its version (the word: written under the lock,
    /// read without it).
    latest: WaitQueue<Latest<T>>,
    /// Open sender handles; the channel ends for receivers when it reaches zero.
    senders: AtomicUsize,
    /// Open receiver handles; sends fail while it is zero.
    receivers: AtomicUsize,
}

impl<T> Shared<T> {
    fn version(&self) -> u64 {
        self.latest.word().load(Ordering::Acquire)
    }

    fn senders_closed(&self) -> bool {
        self.senders.load(Ordering::Acquire) == 0
    }

    /// Wakes every waiter, so each looks again.
    fn wake_all(&self) {
        self.latest.lock().grant_all();
    }
}

/// A watch channel holding `initial` (version 0).
pub fn channel<T>(initial: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        latest: WaitQueue::new(Latest { value: initial }),
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
            seen: AtomicU64::new(0),
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

/// The sending end of a watch channel. Clone it for more producers.
///
/// Dropping a sender closes it. Once every sender is closed, receivers see the last value and
/// then the end.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    /// Set once, by `close` (or a clone of a closed handle).
    closed: AtomicBool,
}

impl<T> Sender<T> {
    /// Makes `value` the latest value (a new version) and wakes every waiting receiver. Never
    /// waits. The replaced value is dropped.
    ///
    /// # Errors
    /// [`SendError::Closed`] if this sender was closed, [`SendError::Broken`] if no receiver is
    /// open. Both return the value, which is not stored.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.is_handle_closed() {
            return Err(SendError::Closed(value));
        }
        if self.shared.receivers.load(Ordering::Acquire) == 0 {
            return Err(SendError::Broken(value));
        }
        let old = {
            let mut latest = self.shared.latest.lock();
            let old = std::mem::replace(&mut latest.value, value);
            let version = self.shared.version() + 1;
            latest.word().store(version, Ordering::Release);
            latest.grant_all();
            old
        };
        // Outside the lock: a value's drop may run arbitrary code (it may use this channel).
        drop(old);
        Ok(())
    }

    /// A new receiver that has seen the current version: it waits for the next send. Works
    /// while no other receiver is open.
    ///
    /// Returns `None` if this sender was closed.
    pub fn subscribe(&self) -> Option<Receiver<T>> {
        if self.is_handle_closed() {
            return None;
        }
        self.shared.receivers.fetch_add(1, Ordering::AcqRel);
        Some(Receiver {
            shared: self.shared.clone(),
            seen: AtomicU64::new(self.shared.version()),
            closed: AtomicBool::new(false),
        })
    }

    /// Closes this sender, as dropping it would; other clones are unaffected. Once every sender
    /// is closed, receivers see the last value and then the end. Closing twice does nothing.
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
            .field("version", &self.shared.version())
            .finish_non_exhaustive()
    }
}

/// The receiving end of a watch channel. Clone it for more watchers.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The newest version this receiver has seen.
    seen: AtomicU64,
    /// Set once, by `close` (or a clone of a closed handle).
    closed: AtomicBool,
}

/// The latest value, read under the channel's lock: keep it short, and do not wait or send
/// while holding it (a send blocks on the same lock).
pub struct Ref<'a, T> {
    latest: Locked<'a, Latest<T>>,
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.latest.value
    }
}

impl<T: fmt::Debug> fmt::Debug for Ref<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

impl<T> Receiver<T> {
    /// The latest value, without marking its version seen. Still works after the channel
    /// closed, and returns the last value then.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            latest: self.shared.latest.lock(),
        }
    }

    /// The latest value, marking its version seen.
    pub fn borrow_and_update(&self) -> Ref<'_, T> {
        let latest = self.shared.latest.lock();
        self.seen.fetch_max(self.shared.version(), Ordering::AcqRel);
        Ref { latest }
    }

    /// Whether a version this receiver has not seen exists.
    pub fn has_changed(&self) -> bool {
        self.shared.version() > self.seen.load(Ordering::Acquire)
    }

    /// Waits for a version newer than the last one this receiver saw, and marks it seen.
    /// Several sends before the call coalesce into one completion. Dropping the future before
    /// it completes loses nothing.
    ///
    /// # Errors
    /// [`RecvError`] once every sender is closed and no unseen version is left, or if this
    /// receiver was closed.
    pub async fn changed(&self) -> Result<(), RecvError> {
        self.changed_ref().await.map(drop)
    }

    /// [`Receiver::changed`], then the latest value (the version it marked seen, or newer).
    ///
    /// # Errors
    /// As for [`Receiver::changed`].
    pub async fn changed_ref(&self) -> Result<Ref<'_, T>, RecvError> {
        loop {
            match self.try_changed_ref() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Closed) => return Err(RecvError),
                Err(TryRecvError::Empty) => {}
            }
            self.shared
                .latest
                .wait(0, |_: &mut Locked<'_, Latest<T>>| self.ready())
                .await;
        }
    }

    /// [`Receiver::changed`] without waiting.
    ///
    /// # Errors
    /// [`TryRecvError::Empty`] if there is no unseen version, [`TryRecvError::Closed`] as for
    /// [`Receiver::changed`].
    pub fn try_changed(&self) -> Result<(), TryRecvError> {
        self.try_changed_ref().map(drop)
    }

    /// [`Receiver::try_changed`], returning the latest value.
    ///
    /// # Errors
    /// As for [`Receiver::try_changed`].
    pub fn try_changed_ref(&self) -> Result<Ref<'_, T>, TryRecvError> {
        if self.is_handle_closed() {
            return Err(TryRecvError::Closed);
        }
        // `senders_closed` first: a send precedes the close, so the version read after it sees
        // every send.
        let ended = self.shared.senders_closed();
        if self.has_changed() {
            let latest = self.shared.latest.lock();
            // Under the lock, so the version and the value read are the same one.
            let version = self.shared.version();
            if self.seen.fetch_max(version, Ordering::AcqRel) < version {
                return Ok(Ref { latest });
            }
        }
        if ended && !self.has_changed() {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// [`Receiver::changed`] for a thread that does not run a porto lane: blocks the thread.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread; await [`Receiver::changed`] there.
    pub fn blocking_changed(&self) -> Result<Result<(), RecvError>, BlockingOnLane> {
        blocking_wait("WatchReceiver.blocking_changed", self.changed())
    }

    /// Closes this receiver, as dropping it would: changes through it end from now on
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

    /// Whether a wait can end now: a version was not seen, the channel ended or this handle
    /// closed.
    fn ready(&self) -> bool {
        self.is_handle_closed() || self.has_changed() || self.shared.senders_closed()
    }

    /// Waits with a caller-owned node until a `try_changed` may finish: an unseen version
    /// exists, the channel ended, or this handle closed (the retry reports it). `Ready` is a
    /// hint to retry, not a reservation.
    ///
    /// This is the building block for waiters that keep their node elsewhere than in a future
    /// (the Python binding embeds one in each task); [`Receiver::changed`] is the usual way to
    /// wait.
    ///
    /// # Safety
    /// As for [`WaitQueue::poll_wait`]: the node is only used with this channel until a `Ready`
    /// or [`Receiver::cancel_ready`], and this handle outlives that period.
    pub unsafe fn poll_ready(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.shared.latest.poll_wait(node, cx, |_| self.ready()) }
    }

    /// Stops a [`Receiver::poll_ready`] wait. A wake it had not observed is dropped: it only
    /// said "look again".
    ///
    /// # Safety
    /// As for [`Receiver::poll_ready`].
    pub unsafe fn cancel_ready(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.shared.latest.cancel(node) }
    }
}

impl<T> Clone for Receiver<T> {
    /// Another receiver on the same channel, which has seen what this one has. A clone of a
    /// closed receiver is closed too.
    fn clone(&self) -> Self {
        let open = !self.is_handle_closed();
        if open {
            self.shared.receivers.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            shared: self.shared.clone(),
            seen: AtomicU64::new(self.seen.load(Ordering::Acquire)),
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
            .field("seen", &self.seen.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}
