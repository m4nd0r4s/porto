//! [`Condvar`]: tasks wait, with a [`Mutex`] released, until notified; the queue behind
//! `porto.Condition` (16-sync-primitives-and-channels.md §1).

use std::fmt;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{Locked, Mutex, MutexGuard, MutexLock, WaitNode, WaitQueue, WaitState};

/// A condition variable for [`Mutex`].
///
/// [`Condvar::wait`] queues the task, then releases the mutex, waits until notified, and takes
/// the mutex again. Queuing before the release means a notification sent after the release is
/// never missed. [`Condvar::notify_one`] and [`Condvar::notify`] wake the waiters that have
/// waited longest; [`Condvar::notify_all`] wakes every current waiter. Nothing is stored: a
/// notification with nobody waiting is lost, and waiters re-check their condition.
///
/// A waiter dropped after `notify_one`/`notify` chose it, before it observed the wake, passes
/// the wake on to the next waiter, so a notification is not lost to a cancelled task.
///
/// Thread-safe: share it across lanes and threads.
///
/// ```
/// use porto_core::Runtime;
/// use porto_core::sync::{Condvar, Mutex};
///
/// let rt = Runtime::new();
/// let (ready, condvar) = (Mutex::new(true), Condvar::new());
/// rt.block_on(async {
///     let mut guard = ready.lock().await;
///     while !*guard {
///         guard = condvar.wait(guard).await;
///     }
/// });
/// ```
pub struct Condvar {
    queue: WaitQueue<CondvarState>,
}

/// The queue state: none beyond the list.
#[derive(Debug)]
struct CondvarState;

impl WaitState for CondvarState {
    /// A `notify_one`/`notify` wake whose waiter gave up: it goes to the next waiter, if any.
    fn pass_grant(waiters: &mut Locked<'_, Self>, _: usize) {
        waiters.grant_front();
    }
}

/// A condvar wait never completes without a notification.
fn never(_: &mut Locked<'_, CondvarState>) -> bool {
    false
}

impl Condvar {
    /// A condvar with nobody waiting.
    pub fn new() -> Self {
        Self {
            queue: WaitQueue::new(CondvarState),
        }
    }

    /// Wakes the task that has waited longest; `false` if nobody waits.
    pub fn notify_one(&self) -> bool {
        self.queue.lock().grant_front()
    }

    /// Wakes up to `n` waiters, longest-waiting first; returns how many.
    pub fn notify(&self, n: usize) -> usize {
        let mut locked = self.queue.lock();
        (0..n).take_while(|_| locked.grant_front()).count()
    }

    /// Wakes every task waiting right now; returns how many.
    pub fn notify_all(&self) -> usize {
        self.queue.lock().grant_all()
    }

    /// How many tasks wait. A snapshot.
    pub fn waiters(&self) -> usize {
        self.queue.lock().len()
    }

    /// Queues the task, releases `guard`'s mutex, waits until notified, then takes the mutex
    /// again; the future evaluates to the new guard.
    ///
    /// Dropping the future after the release leaves the mutex unlocked (a Rust future cannot
    /// wait in its drop): code that must hold the mutex afterwards awaits the future to the end.
    pub fn wait<'a, T: ?Sized>(&'a self, guard: MutexGuard<'a, T>) -> CondvarWait<'a, T> {
        CondvarWait {
            condvar: self,
            mutex: MutexGuard::mutex(&guard),
            guard: Some(guard),
            node: WaitNode::new(0),
            relock: None,
            _pinned: PhantomPinned,
        }
    }

    /// The notification wait of [`Condvar::wait`] with a caller-owned node, for a waiter that
    /// embeds its [`WaitNode`] (a Python task): the first poll queues the node and is always
    /// `Pending` (the caller then releases its lock); `Ready` once notified.
    ///
    /// # Safety
    /// [`WaitQueue::poll_wait`]'s contract, with this condvar as the queue.
    pub unsafe fn poll_wait_node(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.queue.poll_wait(node, cx, never) }
    }

    /// Stops `node`'s [`Condvar::poll_wait_node`] wait: a `notify_one`/`notify` wake it has not
    /// observed goes to the next waiter.
    ///
    /// # Safety
    /// [`WaitQueue::cancel`]'s contract, with this condvar as the queue.
    pub unsafe fn cancel_wait_node(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.queue.cancel(node) }
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Condvar")
            .field("waiters", &self.waiters())
            .finish()
    }
}

/// The future returned by [`Condvar::wait`].
#[must_use = "futures do nothing unless polled"]
pub struct CondvarWait<'a, T: ?Sized> {
    condvar: &'a Condvar,
    mutex: &'a Mutex<T>,
    /// Held until the first poll queued the node.
    guard: Option<MutexGuard<'a, T>>,
    node: WaitNode,
    /// Set once notified: taking the mutex again.
    relock: Option<MutexLock<'a, T>>,
    _pinned: PhantomPinned,
}

impl<'a, T: ?Sized> Future for CondvarWait<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<MutexGuard<'a, T>> {
        // SAFETY: `node` and `relock` are never moved out of the pinned future.
        let this = unsafe { self.get_unchecked_mut() };
        if this.relock.is_none() {
            // SAFETY: `node` is structurally pinned; `&mut self` serializes calls on it; it is
            // only used with this condvar, which outlives the future, and `Drop` cancels a
            // pending wait.
            let node = unsafe { Pin::new_unchecked(&this.node) };
            // SAFETY: as above.
            let notified = unsafe { this.condvar.poll_wait_node(node, cx) };
            // Queued (the first poll always is): now the mutex can go.
            drop(this.guard.take());
            if notified.is_pending() {
                return Poll::Pending;
            }
            this.relock = Some(this.mutex.lock());
        }
        let relock = this.relock.as_mut().expect("set above");
        // SAFETY: `relock` is structurally pinned: set once in place, never moved out.
        unsafe { Pin::new_unchecked(relock) }.poll(cx)
    }
}

impl<T: ?Sized> Drop for CondvarWait<'_, T> {
    fn drop(&mut self) {
        // SAFETY: the node was pinned since its first poll and is dropped right after this;
        // `&mut self` serializes; it was only used with this condvar.
        unsafe {
            self.condvar
                .cancel_wait_node(Pin::new_unchecked(&self.node));
        }
    }
}

impl<T: ?Sized> fmt::Debug for CondvarWait<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CondvarWait")
            .field("node", &self.node)
            .field("relocking", &self.relock.is_some())
            .finish_non_exhaustive()
    }
}
