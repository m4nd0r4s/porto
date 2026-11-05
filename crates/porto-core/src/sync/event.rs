//! [`Event`]: a one-shot flag tasks wait for (16-sync-primitives-and-channels.md §1).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use super::primitives::AtomicBool;
use super::{BlockingOnLane, Locked, Wait, WaitNode, WaitQueue, WaitState, blocking_wait};

/// A flag that starts unset and, once [`set`](Event::set), stays set: every waiter, current
/// and future, is released.
///
/// It cannot be cleared (Trio semantics). Thread-safe: share it (in an `Arc`, say) across
/// lanes and threads.
///
/// ```
/// use porto_core::Runtime;
/// use porto_core::sync::Event;
///
/// let rt = Runtime::new();
/// let event = Event::new();
/// rt.block_on(async {
///     event.set();
///     event.wait().await; // already set: completes on its first poll
/// });
/// assert!(event.is_set());
/// ```
pub struct Event {
    /// Mirrors the queue state, so [`Event::is_set`] does not take the lock. Written under it.
    set: AtomicBool,
    queue: WaitQueue<EventState>,
}

/// The first-poll check of a wait, as a nameable type.
type Acquire = fn(&mut Locked<'_, EventState>) -> bool;

/// The queue state: whether the event is set.
#[derive(Debug, Default)]
struct EventState {
    set: bool,
}

impl WaitState for EventState {
    /// Unreachable: an event only grants with `grant_all`, whose wakes are never passed on.
    fn pass_grant(_: &mut Locked<'_, Self>, _: usize) {}
}

/// Completes the wait at once if the event is set (and queues the waiter otherwise).
fn is_set_locked(locked: &mut Locked<'_, EventState>) -> bool {
    locked.set
}

impl Event {
    /// An event that is not set.
    pub fn new() -> Self {
        Self {
            set: AtomicBool::new(false),
            queue: WaitQueue::new(EventState::default()),
        }
    }

    /// Sets the event and wakes every task waiting for it. Setting it again does nothing.
    pub fn set(&self) {
        if self.is_set() {
            return;
        }
        let mut locked = self.queue.lock();
        if !locked.set {
            locked.set = true;
            self.set.store(true, Ordering::Release);
            locked.grant_all();
        }
    }

    /// Whether the event is set. Once `true`, it stays `true`.
    pub fn is_set(&self) -> bool {
        self.set.load(Ordering::Acquire)
    }

    /// Waits until the event is set. Completes on its first poll, without suspending, if it
    /// already is. Dropping the future cancels the wait.
    pub fn wait(&self) -> EventWait<'_> {
        EventWait {
            wait: self.queue.wait(0, is_set_locked as Acquire),
        }
    }

    /// [`Event::wait`] with a caller-owned node, for a waiter that embeds its [`WaitNode`] (a
    /// Python task): `Ready` once the event is set, otherwise queues `node`.
    ///
    /// # Safety
    /// [`WaitQueue::poll_wait`]'s contract, with this event as the queue.
    pub unsafe fn poll_wait_node(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.queue.poll_wait(node, cx, is_set_locked) }
    }

    /// Stops `node`'s [`Event::poll_wait_node`] wait (an unobserved wake is dropped).
    ///
    /// # Safety
    /// [`WaitQueue::cancel`]'s contract, with this event as the queue.
    pub unsafe fn cancel_wait_node(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.queue.cancel(node) }
    }

    /// [`Event::wait`] for a thread that does not run a porto lane: blocks the thread until the
    /// event is set.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread; await [`Event::wait`] there instead.
    pub fn blocking_wait(&self) -> Result<(), BlockingOnLane> {
        blocking_wait("Event.blocking_wait", self.wait())
    }
}

impl Default for Event {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("set", &self.is_set())
            .field("waiters", &self.queue.lock().len())
            .finish()
    }
}

/// The future returned by [`Event::wait`].
#[must_use = "futures do nothing unless polled"]
#[derive(Debug)]
pub struct EventWait<'a> {
    wait: Wait<'a, EventState, Acquire>,
}

impl Future for EventWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `wait` is structurally pinned: it is never moved out of `self`.
        unsafe { self.map_unchecked_mut(|this| &mut this.wait) }.poll(cx)
    }
}
