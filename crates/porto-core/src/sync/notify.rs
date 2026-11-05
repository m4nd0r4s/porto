//! [`Notify`]: wakes one waiting task, or stores a permit for the next one
//! (16-sync-primitives-and-channels.md §1).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{BlockingOnLane, Locked, Wait, WaitNode, WaitQueue, WaitState, blocking_wait};

/// A wake-up signal between tasks.
///
/// - [`notify_one`](Notify::notify_one) wakes the longest-waiting task; if nobody waits, it
///   stores **one** permit (several calls still store one) that the next
///   [`notified`](Notify::notified) consumes without waiting.
/// - [`notify_all`](Notify::notify_all) wakes every task waiting at that moment and stores
///   nothing: a task that starts waiting afterwards waits for the next notification.
///
/// A [`Notified`] future starts waiting on its first poll: a `notify_all` before that is not
/// seen (a `notify_one` is, through the permit). Thread-safe: share it across lanes and threads.
///
/// If a task woken by `notify_one` is cancelled before it observes the wake, the wake goes to
/// the next waiter, or becomes the stored permit, so it is never lost. A cancelled task woken
/// by `notify_all` simply drops its wake.
///
/// ```
/// use porto_core::Runtime;
/// use porto_core::sync::Notify;
///
/// let rt = Runtime::new();
/// let notify = Notify::new();
/// notify.notify_one(); // nobody waits: stores a permit
/// rt.block_on(notify.notified()); // consumes it without waiting
/// ```
pub struct Notify {
    queue: WaitQueue<NotifyState>,
}

/// The first-poll check of a wait, as a nameable type.
type Acquire = fn(&mut Locked<'_, NotifyState>) -> bool;

/// The queue state. Invariant: a permit is stored only while nobody waits.
#[derive(Debug, Default)]
struct NotifyState {
    permit: bool,
}

impl WaitState for NotifyState {
    /// A `notify_one` wake whose waiter was cancelled: wake the next waiter, or store a permit.
    fn pass_grant(waiters: &mut Locked<'_, Self>, _: usize) {
        notify_one_locked(waiters);
    }
}

fn notify_one_locked(locked: &mut Locked<'_, NotifyState>) {
    if !locked.grant_front() {
        locked.permit = true;
    }
}

/// Consumes a stored permit, completing the wait at once (and queues the waiter otherwise).
fn take_permit(locked: &mut Locked<'_, NotifyState>) -> bool {
    // By the invariant, a stored permit means nobody is queued ahead.
    std::mem::take(&mut locked.permit)
}

impl Notify {
    /// A notify with no permit stored.
    pub fn new() -> Self {
        Self {
            queue: WaitQueue::new(NotifyState::default()),
        }
    }

    /// Wakes the task that has waited longest, or, if none waits, stores a permit for the next
    /// [`Notify::notified`]. At most one permit is stored.
    pub fn notify_one(&self) {
        notify_one_locked(&mut self.queue.lock());
    }

    /// Wakes every task waiting right now. Stores no permit, and leaves a stored one in place.
    pub fn notify_all(&self) {
        self.queue.lock().grant_all();
    }

    /// Waits for a notification: consumes a stored permit at once, otherwise waits for
    /// [`Notify::notify_one`] or [`Notify::notify_all`]. Waiting starts on the first poll.
    /// Dropping the future cancels the wait (passing a `notify_one` wake that raced with the
    /// drop on to the next waiter).
    pub fn notified(&self) -> Notified<'_> {
        Notified {
            wait: self.queue.wait(0, take_permit as Acquire),
        }
    }

    /// Consumes a stored permit without waiting; `false` if none is stored.
    pub fn try_notified(&self) -> bool {
        take_permit(&mut self.queue.lock())
    }

    /// [`Notify::notified`] with a caller-owned node, for a waiter that embeds its
    /// [`WaitNode`] (a Python task): `Ready` once notified (or at once with a stored permit,
    /// on the node's first poll), otherwise queues `node`.
    ///
    /// # Safety
    /// [`WaitQueue::poll_wait`]'s contract, with this notify as the queue.
    pub unsafe fn poll_notified_node(
        &self,
        node: Pin<&WaitNode>,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.queue.poll_wait(node, cx, take_permit) }
    }

    /// Stops `node`'s [`Notify::poll_notified_node`] wait: a `notify_one` wake it has not
    /// observed goes to the next waiter, or becomes the stored permit.
    ///
    /// # Safety
    /// [`WaitQueue::cancel`]'s contract, with this notify as the queue.
    pub unsafe fn cancel_notified_node(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.queue.cancel(node) }
    }

    /// [`Notify::notified`] for a thread that does not run a porto lane: blocks the thread
    /// until notified.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread; await [`Notify::notified`] there
    /// instead.
    pub fn blocking_notified(&self) -> Result<(), BlockingOnLane> {
        blocking_wait("Notify.blocking_notified", self.notified())
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Notify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let locked = self.queue.lock();
        f.debug_struct("Notify")
            .field("permit", &locked.permit)
            .field("waiters", &locked.len())
            .finish()
    }
}

/// The future returned by [`Notify::notified`].
#[must_use = "futures do nothing unless polled"]
#[derive(Debug)]
pub struct Notified<'a> {
    wait: Wait<'a, NotifyState, Acquire>,
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `wait` is structurally pinned: it is never moved out of `self`.
        unsafe { self.map_unchecked_mut(|this| &mut this.wait) }.poll(cx)
    }
}
