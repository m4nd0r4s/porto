//! [`Mutex`]: an async lock that owns its value, with fair FIFO handoff
//! (16-sync-primitives-and-channels.md §1).

use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{BlockingOnLane, Locked, Wait, WaitNode, WaitQueue, WaitState, blocking_wait};

/// An async mutual-exclusion lock around a `T`.
///
/// - **Fair:** waiters get the lock in arrival order. Unlocking with waiters hands the lock
///   straight to the first one (it stays locked in between), so a task that never waited
///   cannot barge ahead of the queue.
/// - **Non-reentrant:** locking it again from the task that holds it waits forever.
/// - **Cancel-safe:** a waiter dropped before it observed a handoff passes the lock on to the
///   next waiter, or unlocks it.
///
/// Thread-safe: share it (in an `Arc`, say) across lanes and threads.
///
/// ```
/// use porto_core::Runtime;
/// use porto_core::sync::Mutex;
///
/// let rt = Runtime::new();
/// let counter = Mutex::new(0);
/// rt.block_on(async {
///     *counter.lock().await += 1;
/// });
/// assert_eq!(counter.into_inner(), 1);
/// ```
pub struct Mutex<T: ?Sized> {
    queue: WaitQueue<LockState>,
    value: UnsafeCell<T>,
}

// SAFETY: the lock gives one holder at a time access to the value, which may live on any
// thread (`T: Send`); nothing else touches it.
unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
// SAFETY: as above: `&Mutex` only reaches `&mut T` through the lock, one holder at a time.
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

/// The first-poll check of a lock wait, as a nameable type.
type Acquire = fn(&mut Locked<'_, LockState>) -> bool;

/// The queue state. Invariant: waiters are queued only while `locked`.
#[derive(Debug, Default)]
struct LockState {
    locked: bool,
}

impl WaitState for LockState {
    /// A handoff whose waiter gave up: the lock goes to the next waiter, or is unlocked.
    fn pass_grant(waiters: &mut Locked<'_, Self>, _: usize) {
        unlock_locked(waiters);
    }
}

/// Takes the lock if it is free (and the waiter queues otherwise). Fair: by the invariant, a
/// free lock has nobody queued ahead.
fn try_lock_locked(locked: &mut Locked<'_, LockState>) -> bool {
    !std::mem::replace(&mut locked.locked, true)
}

/// Hands the lock to the first waiter, or unlocks it if nobody waits.
fn unlock_locked(locked: &mut Locked<'_, LockState>) {
    if !locked.grant_front() {
        locked.locked = false;
    }
}

impl<T> Mutex<T> {
    /// An unlocked mutex around `value`.
    pub fn new(value: T) -> Self {
        Self {
            queue: WaitQueue::new(LockState::default()),
            value: UnsafeCell::new(value),
        }
    }

    /// The value, consuming the mutex.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Waits for the lock. Completes on its first poll, without suspending, if the lock is
    /// free. Dropping the future cancels the wait (passing on a handoff that raced with the
    /// drop).
    pub fn lock(&self) -> MutexLock<'_, T> {
        MutexLock {
            mutex: self,
            wait: self.queue.wait(0, try_lock_locked as Acquire),
        }
    }

    /// Takes the lock if it is free right now.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        // The queue lock is released before a guard exists: a guard unlocks on drop.
        let acquired = try_lock_locked(&mut self.queue.lock());
        if acquired {
            Some(MutexGuard { mutex: self })
        } else {
            None
        }
    }

    /// [`Mutex::lock`] for a thread that does not run a porto lane: blocks the thread until it
    /// holds the lock.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread; await [`Mutex::lock`] there instead.
    pub fn blocking_lock(&self) -> Result<MutexGuard<'_, T>, BlockingOnLane> {
        blocking_wait("Mutex.blocking_lock", self.lock())
    }

    /// Whether the lock is held (or being handed to a waiter). A snapshot: it may change
    /// right after.
    pub fn is_locked(&self) -> bool {
        self.queue.lock().locked
    }

    /// The value, through the exclusive borrow of the mutex (no locking needed).
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }

    /// [`Mutex::lock`] with a caller-owned node, for a waiter that embeds its [`WaitNode`] (a
    /// Python task). `Ready` means the caller now holds the lock, with no guard: it unlocks it
    /// with [`Mutex::force_unlock`].
    ///
    /// # Safety
    /// [`WaitQueue::poll_wait`]'s contract, with this mutex as the queue.
    pub unsafe fn poll_lock_node(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.queue.poll_wait(node, cx, try_lock_locked) }
    }

    /// Stops `node`'s [`Mutex::poll_lock_node`] wait: a handoff it has not observed goes to
    /// the next waiter, or unlocks the mutex.
    ///
    /// # Safety
    /// [`WaitQueue::cancel`]'s contract, with this mutex as the queue.
    pub unsafe fn cancel_lock_node(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.queue.cancel(node) }
    }

    /// Unlocks a lock held without a guard (a [`MutexGuard::leak`]ed guard, a ready
    /// [`Mutex::poll_lock_node`]): hands it to the first waiter, or frees it. Returns `false`,
    /// doing nothing, if the mutex was not locked.
    ///
    /// # Safety
    /// No reference to the value obtained through the lock being released is used afterwards.
    pub unsafe fn force_unlock(&self) -> bool {
        let mut locked = self.queue.lock();
        if !locked.locked {
            return false;
        }
        unlock_locked(&mut locked);
        true
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let locked = self.queue.lock();
        f.debug_struct("Mutex")
            .field("locked", &locked.locked)
            .field("waiters", &locked.len())
            .finish_non_exhaustive()
    }
}

/// Holds a [`Mutex`] and gives access to its value; unlocks it (handing it to the first
/// waiter) when dropped.
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
}

// SAFETY: a shared guard gives `&T` only, like `&T`.
unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}

impl<T: ?Sized> MutexGuard<'_, T> {
    /// Keeps the lock held without a guard; [`Mutex::force_unlock`] releases it.
    pub fn leak(guard: Self) {
        std::mem::forget(guard);
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the guard holds the lock, so no other access exists.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as above; `&mut self` makes this access unique.
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        unlock_locked(&mut self.mutex.queue.lock());
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// The future returned by [`Mutex::lock`].
#[must_use = "futures do nothing unless polled"]
pub struct MutexLock<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
    wait: Wait<'a, LockState, Acquire>,
}

impl<'a, T: ?Sized> Future for MutexLock<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<MutexGuard<'a, T>> {
        // SAFETY: `wait` is structurally pinned: it is never moved out of `self`.
        let this = unsafe { self.get_unchecked_mut() };
        let mutex = this.mutex;
        // SAFETY: as above.
        let wait = unsafe { Pin::new_unchecked(&mut this.wait) };
        wait.poll(cx).map(|()| MutexGuard { mutex })
    }
}

impl<T: ?Sized> fmt::Debug for MutexLock<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MutexLock")
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}
