//! [`RwLock`]: an async, writer-preferring read/write lock (16-sync-primitives-and-channels.md
//! §1).

use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use super::primitives::{AtomicU64, UnsafeCell};
use super::{BlockingOnLane, Locked, Wait, WaitNode, WaitQueue, WaitState, blocking_wait};

/// An async read/write lock around a `T`: any number of readers, or one writer.
///
/// - **Writer-preferring:** a reader never takes the lock while a writer waits; it queues
///   behind the writer. So a stream of readers cannot starve a writer.
/// - **Fair:** waiters are served in arrival order, consecutive readers together. So a stream
///   of writers cannot starve the readers queued between them either.
/// - **Cancel-safe:** a waiter dropped before it observed its grant releases it (to the next
///   waiters); a waiting writer that leaves lets the readers queued behind it in.
/// - **Non-reentrant:** taking it again from a task that holds it may wait forever.
///
/// Thread-safe: share it (in an `Arc`, say) across lanes and threads.
///
/// ```
/// use porto_core::Runtime;
/// use porto_core::sync::RwLock;
///
/// let rt = Runtime::new();
/// let lock = RwLock::new(1);
/// rt.block_on(async {
///     let (a, b) = (lock.read().await, lock.read().await);
///     assert_eq!(*a + *b, 2);
///     drop((a, b));
///     *lock.write().await += 1;
/// });
/// assert_eq!(lock.into_inner(), 2);
/// ```
pub struct RwLock<T: ?Sized> {
    queue: WaitQueue<RwState>,
    value: UnsafeCell<T>,
}

// SAFETY: the lock gives either shared access to many readers (`T: Sync`) or exclusive access
// to one writer (`T: Send`), which may be on any thread.
unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
// SAFETY: as above.
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

/// The first-poll check of a wait, as a nameable type.
type Acquire = fn(&mut Locked<'_, RwState>) -> bool;

/// The queue state: none beyond the list. The lock is the queue's lock-free word
/// ([`WaitQueue::word`]): [`WRITER`] while a writer holds it, the reader count from bit 2, and
/// [`QUEUED`] while waiters may be queued.
///
/// [`QUEUED`] is set (under the queue lock) before a waiter is queued and while one is; with it
/// clear nobody waits, so the fast paths skip the queue lock: taking a free lock (for reading:
/// one without a writer) and releasing one are a compare-and-swap each. A locked operation sets
/// [`QUEUED`] first, so the counts stay put until it stores them back.
#[derive(Debug)]
struct RwState;

/// A writer holds the lock (or is being handed it).
const WRITER: u64 = 1;
/// Waiters may be queued: take the queue lock.
const QUEUED: u64 = 2;
/// One reader, in the count from bit 2.
const READER: u64 = 4;

/// The token of a waiting reader.
const READ: usize = 1;
/// The token of a waiting writer.
const WRITE: usize = 2;

impl WaitState for RwState {
    /// A grant whose waiter gave up: released, to the next waiters.
    fn pass_grant(waiters: &mut Locked<'_, Self>, token: usize) {
        let mut word = enter(waiters);
        if token == WRITE {
            word &= !WRITER;
        } else {
            word -= READER;
        }
        grant(waiters, &mut word);
        leave(waiters, word, false);
    }

    /// A writer that left may have blocked readers behind it.
    fn waiter_left(waiters: &mut Locked<'_, Self>, _: usize) {
        let mut word = enter(waiters);
        grant(waiters, &mut word);
        leave(waiters, word, false);
    }
}

/// Sets [`QUEUED`], under the queue lock, and returns the word, which no fast path changes
/// until [`leave`] stores it back.
fn enter(locked: &Locked<'_, RwState>) -> u64 {
    locked.word().fetch_or(QUEUED, Ordering::Acquire) & !QUEUED
}

/// Stores `word` back, with [`QUEUED`] if waiters are queued or `queuing` (the caller queues a
/// waiter before it releases the queue lock).
fn leave(locked: &Locked<'_, RwState>, word: u64, queuing: bool) {
    let queued = if queuing || !locked.is_empty() {
        QUEUED
    } else {
        0
    };
    locked.word().store(word | queued, Ordering::Release);
}

/// Grants from the front of the queue: one writer if the lock is free, or every reader up to
/// the first writer if no writer holds it.
fn grant(locked: &mut Locked<'_, RwState>, word: &mut u64) {
    while let Some(token) = locked.front_token() {
        if token == WRITE {
            if *word == 0 {
                *word = WRITER;
                locked.grant_front();
            }
            return;
        }
        if *word & WRITER != 0 {
            return;
        }
        *word += READER;
        locked.grant_front();
    }
}

fn try_read_fast(word: &AtomicU64) -> bool {
    let mut current = word.load(Ordering::Relaxed);
    loop {
        if current & (WRITER | QUEUED) != 0 {
            return false;
        }
        match word.compare_exchange_weak(
            current,
            current + READER,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

fn try_write_fast(word: &AtomicU64) -> bool {
    word.compare_exchange(0, WRITER, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
}

/// Takes a read lock if no writer holds it and nobody is queued; otherwise, if `queuing`,
/// leaves [`QUEUED`] set for the waiter the caller queues.
fn read_locked(locked: &mut Locked<'_, RwState>, queuing: bool) -> bool {
    let mut word = enter(locked);
    let acquired = word & WRITER == 0 && locked.is_empty();
    if acquired {
        word += READER;
    }
    leave(locked, word, queuing && !acquired);
    acquired
}

/// Takes the write lock if it is free and nobody is queued; otherwise as [`read_locked`].
fn write_locked(locked: &mut Locked<'_, RwState>, queuing: bool) -> bool {
    let mut word = enter(locked);
    let acquired = word == 0 && locked.is_empty();
    if acquired {
        word = WRITER;
    }
    leave(locked, word, queuing && !acquired);
    acquired
}

fn read_wait(locked: &mut Locked<'_, RwState>) -> bool {
    read_locked(locked, true)
}

fn write_wait(locked: &mut Locked<'_, RwState>) -> bool {
    write_locked(locked, true)
}

impl<T> RwLock<T> {
    /// An unlocked lock around `value`.
    pub fn new(value: T) -> Self {
        Self {
            queue: WaitQueue::new(RwState),
            value: UnsafeCell::new(value),
        }
    }

    /// The value, consuming the lock.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Waits for a read lock. Completes on its first poll, without suspending, if no writer
    /// holds or waits for the lock. Dropping the future cancels the wait.
    pub fn read(&self) -> RwLockRead<'_, T> {
        RwLockRead {
            lock: self,
            wait: self.queue.wait(READ, read_wait as Acquire),
        }
    }

    /// Waits for the write lock. Completes on its first poll, without suspending, if the lock
    /// is free and nobody waits. Dropping the future cancels the wait.
    pub fn write(&self) -> RwLockWrite<'_, T> {
        RwLockWrite {
            lock: self,
            wait: self.queue.wait(WRITE, write_wait as Acquire),
        }
    }

    /// Takes a read lock if no writer holds or waits for it right now.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        self.try_read_detached()
            .then(|| RwLockReadGuard { lock: self })
    }

    /// Takes the write lock if it is free and nobody waits right now.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        self.try_write_detached()
            .then(|| RwLockWriteGuard { lock: self })
    }

    /// [`RwLock::try_read`] without a guard; [`RwLock::force_read_unlock`] releases it.
    pub fn try_read_detached(&self) -> bool {
        let word = self.queue.word();
        try_read_fast(word)
            || (word.load(Ordering::Relaxed) & QUEUED != 0
                && read_locked(&mut self.queue.lock(), false))
    }

    /// [`RwLock::try_write`] without a guard; [`RwLock::force_write_unlock`] releases it.
    pub fn try_write_detached(&self) -> bool {
        let word = self.queue.word();
        try_write_fast(word)
            || (word.load(Ordering::Relaxed) & QUEUED != 0
                && write_locked(&mut self.queue.lock(), false))
    }

    /// [`RwLock::read`] for a thread that does not run a porto lane: blocks the thread until it
    /// holds a read lock.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread.
    pub fn blocking_read(&self) -> Result<RwLockReadGuard<'_, T>, BlockingOnLane> {
        blocking_wait("RwLock.blocking_read", self.read())
    }

    /// [`RwLock::write`] for a thread that does not run a porto lane.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread.
    pub fn blocking_write(&self) -> Result<RwLockWriteGuard<'_, T>, BlockingOnLane> {
        blocking_wait("RwLock.blocking_write", self.write())
    }

    /// How many readers hold the lock. A snapshot.
    pub fn readers(&self) -> usize {
        (self.queue.word().load(Ordering::Relaxed) / READER) as usize
    }

    /// Whether a writer holds the lock (or is being handed it). A snapshot.
    pub fn is_write_locked(&self) -> bool {
        self.queue.word().load(Ordering::Relaxed) & WRITER != 0
    }

    /// The value, through the exclusive borrow of the lock (no locking needed).
    pub fn get_mut(&mut self) -> &mut T {
        // SAFETY: `&mut self` rules out every other access.
        self.value.with_mut(|value| unsafe { &mut *value })
    }

    /// [`RwLock::read`] with a caller-owned node, for a waiter that embeds its [`WaitNode`] (a
    /// Python task). `Ready` means the caller holds a read lock, with no guard: it releases it
    /// with [`RwLock::force_read_unlock`]. Sets the node's token when the wait starts.
    ///
    /// # Safety
    /// [`WaitQueue::poll_wait`]'s contract, with this lock as the queue.
    pub unsafe fn poll_read_node(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        if !node.is_pending() {
            node.set_token(READ);
        }
        // SAFETY: the caller's contract.
        unsafe { self.queue.poll_wait(node, cx, read_wait) }
    }

    /// [`RwLock::poll_read_node`] for the write lock; released with
    /// [`RwLock::force_write_unlock`].
    ///
    /// # Safety
    /// As for [`RwLock::poll_read_node`].
    pub unsafe fn poll_write_node(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        if !node.is_pending() {
            node.set_token(WRITE);
        }
        // SAFETY: the caller's contract.
        unsafe { self.queue.poll_wait(node, cx, write_wait) }
    }

    /// Stops `node`'s [`RwLock::poll_read_node`] or [`RwLock::poll_write_node`] wait: a grant it
    /// has not observed is released, to the next waiters.
    ///
    /// # Safety
    /// [`WaitQueue::cancel`]'s contract, with this lock as the queue.
    pub unsafe fn cancel_node(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.queue.cancel(node) }
    }

    /// Releases a read lock held without a guard. Returns `false`, doing nothing, if no reader
    /// holds the lock.
    ///
    /// # Safety
    /// No reference to the value obtained through the read lock being released is used
    /// afterwards.
    pub unsafe fn force_read_unlock(&self) -> bool {
        let word = self.queue.word();
        let mut current = word.load(Ordering::Relaxed);
        loop {
            if current & QUEUED != 0 {
                break;
            }
            if current < READER {
                return false;
            }
            match word.compare_exchange_weak(
                current,
                current - READER,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        let mut locked = self.queue.lock();
        let mut word = enter(&locked);
        if word < READER {
            leave(&locked, word, false);
            return false;
        }
        word -= READER;
        grant(&mut locked, &mut word);
        leave(&locked, word, false);
        true
    }

    /// Releases the write lock held without a guard. Returns `false`, doing nothing, if no
    /// writer holds the lock.
    ///
    /// # Safety
    /// No reference to the value obtained through the write lock is used afterwards.
    pub unsafe fn force_write_unlock(&self) -> bool {
        let word = self.queue.word();
        match word.compare_exchange(WRITER, 0, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(current) if current & WRITER == 0 => return false,
            Err(_) => {}
        }
        let mut locked = self.queue.lock();
        let mut word = enter(&locked);
        if word & WRITER == 0 {
            leave(&locked, word, false);
            return false;
        }
        word &= !WRITER;
        grant(&mut locked, &mut word);
        leave(&locked, word, false);
        true
    }
}

#[cfg(all(test, loom))]
impl<T: ?Sized> RwLock<T> {
    /// Whether [`QUEUED`] is set, for the models that check it is clear once nobody waits.
    pub(super) fn queued_flag(&self) -> bool {
        self.queue.word().load(Ordering::Relaxed) & QUEUED != 0
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let locked = self.queue.lock();
        let word = locked.word().load(Ordering::Acquire);
        f.debug_struct("RwLock")
            .field("write_locked", &(word & WRITER != 0))
            .field("readers", &(word / READER))
            .field("waiters", &locked.len())
            .finish_non_exhaustive()
    }
}

/// A read lock on a [`RwLock`]: shared access to its value; released when dropped.
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

// SAFETY: a shared guard gives `&T` only, like `&T`.
unsafe impl<T: ?Sized + Sync> Sync for RwLockReadGuard<'_, T> {}

impl<T: ?Sized> RwLockReadGuard<'_, T> {
    /// Keeps the read lock held without a guard; [`RwLock::force_read_unlock`] releases it.
    pub fn leak(guard: Self) {
        std::mem::forget(guard);
    }
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: a read lock is held, so no writer has access.
        self.lock.value.with(|value| unsafe { &*value })
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        // SAFETY: the guard is going away with its borrows.
        let released = unsafe { self.lock.force_read_unlock() };
        debug_assert!(released, "a read guard's lock is counted");
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// The write lock on a [`RwLock`]: exclusive access to its value; released when dropped.
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

// SAFETY: a shared guard gives `&T` only, like `&T`.
unsafe impl<T: ?Sized + Sync> Sync for RwLockWriteGuard<'_, T> {}

impl<T: ?Sized> RwLockWriteGuard<'_, T> {
    /// Keeps the write lock held without a guard; [`RwLock::force_write_unlock`] releases it.
    pub fn leak(guard: Self) {
        std::mem::forget(guard);
    }
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: the write lock is held, so no other access exists.
        self.lock.value.with(|value| unsafe { &*value })
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as above; `&mut self` makes this access unique.
        self.lock.value.with_mut(|value| unsafe { &mut *value })
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        // SAFETY: the guard is going away with its borrows.
        let released = unsafe { self.lock.force_write_unlock() };
        debug_assert!(released, "a write guard holds the lock");
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// The future returned by [`RwLock::read`].
#[must_use = "futures do nothing unless polled"]
pub struct RwLockRead<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    wait: Wait<'a, RwState, Acquire>,
}

impl<'a, T: ?Sized> Future for RwLockRead<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RwLockReadGuard<'a, T>> {
        // SAFETY: `wait` is structurally pinned: it is never moved out of `self`.
        let this = unsafe { self.get_unchecked_mut() };
        let lock = this.lock;
        // SAFETY: as above.
        let wait = unsafe { Pin::new_unchecked(&mut this.wait) };
        wait.poll(cx).map(|()| RwLockReadGuard { lock })
    }
}

impl<T: ?Sized> fmt::Debug for RwLockRead<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockRead")
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}

/// The future returned by [`RwLock::write`].
#[must_use = "futures do nothing unless polled"]
pub struct RwLockWrite<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    wait: Wait<'a, RwState, Acquire>,
}

impl<'a, T: ?Sized> Future for RwLockWrite<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RwLockWriteGuard<'a, T>> {
        // SAFETY: `wait` is structurally pinned: it is never moved out of `self`.
        let this = unsafe { self.get_unchecked_mut() };
        let lock = this.lock;
        // SAFETY: as above.
        let wait = unsafe { Pin::new_unchecked(&mut this.wait) };
        wait.poll(cx).map(|()| RwLockWriteGuard { lock })
    }
}

impl<T: ?Sized> fmt::Debug for RwLockWrite<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockWrite")
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}
