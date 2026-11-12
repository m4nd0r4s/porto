//! [`Semaphore`]: a pool of permits tasks acquire and release, with fair FIFO grants
//! (16-sync-primitives-and-channels.md §1).

use std::fmt;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use super::primitives::AtomicU64;
use super::{BlockingOnLane, Locked, WaitNode, WaitQueue, WaitState, blocking_wait};

/// The most permits a semaphore counts, free and acquired together.
pub const MAX_PERMITS: usize = (1 << 31) - 1;

/// An async counting semaphore.
///
/// - **Fair:** waiters are granted in arrival order. A waiter that wants more permits than are
///   free blocks the ones behind it, even if they want fewer: nobody barges ahead of the queue,
///   so a large request is never starved by small ones.
/// - **Bounded:** it counts the permits it handed out, and [`Semaphore::release`] refuses to
///   give back more than that.
/// - **Cancel-safe:** a waiter dropped before it observed its grant gives the permits back,
///   which go to the next waiters; a waiter that leaves the front of the queue lets the ones
///   behind it be granted.
///
/// Thread-safe: share it (in an `Arc`, say) across lanes and threads.
///
/// ```
/// use porto_core::Runtime;
/// use porto_core::sync::Semaphore;
///
/// let rt = Runtime::new();
/// let semaphore = Semaphore::new(2);
/// rt.block_on(async {
///     let permit = semaphore.acquire(2).await;
///     assert_eq!(semaphore.available_permits(), 0);
///     drop(permit);
/// });
/// assert_eq!(semaphore.available_permits(), 2);
/// ```
pub struct Semaphore {
    queue: WaitQueue<SemaphoreState>,
}

/// The queue state beyond the list: permits owed by a [`Semaphore::remove_permits`] that found
/// too few free ones.
///
/// The permit counts live in the queue's lock-free word ([`WaitQueue::word`]): the free
/// permits, the acquired ones, and [`SLOW`]. [`SLOW`] is set (under the queue lock) before a
/// waiter is queued and while one is, and while a debt is owed; with it clear nobody waits, so
/// the fast paths can skip the queue lock: an acquire that finds enough free permits and a
/// release are one compare-and-swap each. A locked operation sets [`SLOW`] first, so the
/// counts stay put until it stores them back. Invariant: `free + acquired - debt` is the total.
#[derive(Debug, Default)]
struct SemaphoreState {
    debt: u64,
}

/// Waiters may be queued, or a debt is owed: take the queue lock.
const SLOW: u64 = 1;
/// The acquired permits, from bit 1.
const HELD_SHIFT: u32 = 1;
/// The free permits, from bit 32.
const FREE_SHIFT: u32 = 32;
const COUNT_MASK: u64 = MAX_PERMITS as u64;

/// The counts of the word.
#[derive(Clone, Copy, Debug)]
struct Counts {
    free: u64,
    held: u64,
}

impl Counts {
    fn of(word: u64) -> Self {
        Self {
            free: (word >> FREE_SHIFT) & COUNT_MASK,
            held: (word >> HELD_SHIFT) & COUNT_MASK,
        }
    }

    fn word(self, slow: bool) -> u64 {
        (self.free << FREE_SHIFT) | (self.held << HELD_SHIFT) | u64::from(slow)
    }
}

impl WaitState for SemaphoreState {
    /// A grant whose waiter gave up: its permits go back, to the next waiters first.
    fn pass_grant(waiters: &mut Locked<'_, Self>, permits: usize) {
        let released = release_locked(waiters, permits as u64);
        debug_assert!(
            released,
            "a granted waiter's permits are counted as acquired"
        );
    }

    /// The waiter that left may have blocked the ones behind it.
    fn waiter_left(waiters: &mut Locked<'_, Self>, _: usize) {
        let mut counts = enter(waiters);
        grant(waiters, &mut counts);
        leave(waiters, counts, false);
    }
}

/// Takes `permits` free permits without the queue lock, if nobody waits. Fair: with [`SLOW`]
/// clear nobody is queued ahead.
fn try_acquire_fast(word: &AtomicU64, permits: u64) -> bool {
    let mut current = word.load(Ordering::Relaxed);
    loop {
        let counts = Counts::of(current);
        if current & SLOW != 0 || counts.free < permits {
            return false;
        }
        let new = Counts {
            free: counts.free - permits,
            held: counts.held + permits,
        };
        match word.compare_exchange_weak(
            current,
            new.word(false),
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

/// What a lock-free release did.
enum FastRelease {
    Released,
    /// Fewer permits are acquired than the release gives back.
    TooMany,
    /// [`SLOW`] is set: release under the queue lock.
    Slow,
}

fn release_fast(word: &AtomicU64, permits: u64) -> FastRelease {
    let mut current = word.load(Ordering::Relaxed);
    loop {
        if current & SLOW != 0 {
            return FastRelease::Slow;
        }
        let counts = Counts::of(current);
        if counts.held < permits {
            return FastRelease::TooMany;
        }
        let new = Counts {
            free: counts.free + permits,
            held: counts.held - permits,
        };
        match word.compare_exchange_weak(
            current,
            new.word(false),
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => return FastRelease::Released,
            Err(actual) => current = actual,
        }
    }
}

/// Sets [`SLOW`], under the queue lock, and returns the counts, which no fast path changes
/// until [`leave`] stores them back.
fn enter(locked: &Locked<'_, SemaphoreState>) -> Counts {
    Counts::of(locked.word().fetch_or(SLOW, Ordering::Acquire))
}

/// Stores `counts` back, keeping [`SLOW`] if waiters are queued, a debt is owed, or `queuing`
/// (the caller queues a waiter before it releases the queue lock).
fn leave(locked: &Locked<'_, SemaphoreState>, counts: Counts, queuing: bool) {
    let slow = queuing || !locked.is_empty() || locked.debt > 0;
    locked.word().store(counts.word(slow), Ordering::Release);
}

/// Grants the queued waiters, in order, while the first one's request fits the free permits.
fn grant(locked: &mut Locked<'_, SemaphoreState>, counts: &mut Counts) {
    while let Some(wanted) = locked.front_token() {
        let wanted = wanted as u64;
        if wanted > counts.free {
            break;
        }
        counts.free -= wanted;
        counts.held += wanted;
        locked.grant_front();
    }
}

/// Takes `permits` if nobody is queued and enough are free; otherwise, if `queuing`, leaves
/// [`SLOW`] set for the waiter the caller queues.
fn acquire_locked(locked: &mut Locked<'_, SemaphoreState>, permits: u64, queuing: bool) -> bool {
    let mut counts = enter(locked);
    let acquired = locked.is_empty() && counts.free >= permits;
    if acquired {
        counts.free -= permits;
        counts.held += permits;
    }
    leave(locked, counts, queuing && !acquired);
    acquired
}

/// Gives `permits` back (paying a debt first) and grants the waiters they let through; `false`,
/// doing nothing, if fewer are acquired.
fn release_locked(locked: &mut Locked<'_, SemaphoreState>, permits: u64) -> bool {
    let mut counts = enter(locked);
    if counts.held < permits {
        leave(locked, counts, false);
        return false;
    }
    counts.held -= permits;
    let paid = permits.min(locked.debt);
    locked.debt -= paid;
    counts.free += permits - paid;
    grant(locked, &mut counts);
    leave(locked, counts, false);
    true
}

/// A count as a `usize`.
#[allow(
    clippy::cast_possible_truncation,
    reason = "every count is at most MAX_PERMITS, which fits in 32 bits"
)]
fn count(n: u64) -> usize {
    n as usize
}

fn to_permits(permits: usize) -> u64 {
    assert!(
        permits <= MAX_PERMITS,
        "porto: a semaphore counts at most {MAX_PERMITS} permits, got {permits}"
    );
    permits as u64
}

impl Semaphore {
    /// A semaphore with `permits` free permits.
    ///
    /// # Panics
    /// If `permits` is above [`MAX_PERMITS`].
    pub fn new(permits: usize) -> Self {
        let semaphore = Self {
            queue: WaitQueue::new(SemaphoreState::default()),
        };
        let counts = Counts {
            free: to_permits(permits),
            held: 0,
        };
        semaphore
            .queue
            .word()
            .store(counts.word(false), Ordering::Relaxed);
        semaphore
    }

    /// Waits for `permits` permits. Completes on its first poll, without suspending, if nobody
    /// waits and enough are free. Dropping the future cancels the wait (giving back a grant
    /// that raced with the drop).
    ///
    /// # Panics
    /// If `permits` is above [`MAX_PERMITS`].
    pub fn acquire(&self, permits: usize) -> SemaphoreAcquire<'_> {
        SemaphoreAcquire {
            semaphore: self,
            node: WaitNode::new(count(to_permits(permits))),
            done: false,
            _pinned: PhantomPinned,
        }
    }

    /// Takes `permits` permits if nobody waits and enough are free right now.
    pub fn try_acquire(&self, permits: usize) -> Option<SemaphorePermit<'_>> {
        self.try_acquire_detached(permits).then(|| SemaphorePermit {
            semaphore: self,
            permits,
        })
    }

    /// [`Semaphore::try_acquire`] without a guard: the caller gives the permits back with
    /// [`Semaphore::release`].
    pub fn try_acquire_detached(&self, permits: usize) -> bool {
        let permits = to_permits(permits);
        let word = self.queue.word();
        if try_acquire_fast(word, permits) {
            return true;
        }
        // A stale `SLOW` (the last waiter left) sends us here with nobody queued.
        word.load(Ordering::Relaxed) & SLOW != 0
            && acquire_locked(&mut self.queue.lock(), permits, false)
    }

    /// [`Semaphore::acquire`] for a thread that does not run a porto lane: blocks the thread
    /// until it holds the permits.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread; await [`Semaphore::acquire`] there
    /// instead.
    pub fn blocking_acquire(&self, permits: usize) -> Result<SemaphorePermit<'_>, BlockingOnLane> {
        blocking_wait("Semaphore.blocking_acquire", self.acquire(permits))
    }

    /// Gives back `permits` acquired permits held without a guard ([`SemaphorePermit::forget`],
    /// a ready [`Semaphore::poll_acquire_node`]), granting the waiters they let through.
    /// Returns `false`, doing nothing, if fewer permits are acquired.
    pub fn release(&self, permits: usize) -> bool {
        let permits = to_permits(permits);
        match release_fast(self.queue.word(), permits) {
            FastRelease::Released => true,
            FastRelease::TooMany => false,
            FastRelease::Slow => release_locked(&mut self.queue.lock(), permits),
        }
    }

    /// The free permits. A snapshot: it may change right after.
    pub fn available_permits(&self) -> usize {
        count(Counts::of(self.queue.word().load(Ordering::Relaxed)).free)
    }

    /// The acquired permits (granted to a waiter that has not observed it yet included). A
    /// snapshot.
    pub fn acquired_permits(&self) -> usize {
        count(Counts::of(self.queue.word().load(Ordering::Relaxed)).held)
    }

    /// The total: free plus acquired, less what [`Semaphore::remove_permits`] still waits to
    /// take back. A snapshot.
    pub fn total_permits(&self) -> usize {
        let locked = self.queue.lock();
        let counts = Counts::of(locked.word().load(Ordering::Acquire));
        count(counts.free + counts.held - locked.debt)
    }

    /// Raises the total by `permits`: they become free (or cancel what
    /// [`Semaphore::remove_permits`] still waits to take back), granting the waiters they let
    /// through.
    ///
    /// # Panics
    /// If the total would go above [`MAX_PERMITS`].
    pub fn add_permits(&self, permits: usize) {
        let permits = to_permits(permits);
        let mut locked = self.queue.lock();
        let mut counts = enter(&locked);
        let total = counts.free + counts.held - locked.debt;
        if total + permits > COUNT_MASK {
            leave(&locked, counts, false);
            drop(locked);
            panic!("porto: a semaphore counts at most {MAX_PERMITS} permits");
        }
        let paid = permits.min(locked.debt);
        locked.debt -= paid;
        counts.free += permits - paid;
        grant(&mut locked, &mut counts);
        leave(&locked, counts, false);
    }

    /// Lowers the total by `permits`: free ones first, the rest as they are released (the
    /// acquired permits may stay above the total until then).
    ///
    /// # Panics
    /// If `permits` is above the total.
    pub fn remove_permits(&self, permits: usize) {
        let permits = to_permits(permits);
        let mut locked = self.queue.lock();
        let mut counts = enter(&locked);
        let total = counts.free + counts.held - locked.debt;
        if permits > total {
            leave(&locked, counts, false);
            drop(locked);
            panic!("porto: cannot remove {permits} permits from a semaphore with {total}");
        }
        let taken = permits.min(counts.free);
        counts.free -= taken;
        locked.debt += permits - taken;
        leave(&locked, counts, false);
    }

    /// [`Semaphore::acquire`] with a caller-owned node, for a waiter that embeds its
    /// [`WaitNode`] (a Python task). `Ready` means the caller now holds `permits` permits,
    /// with no guard: it gives them back with [`Semaphore::release`]. The node's token is set
    /// to `permits` when the wait starts.
    ///
    /// # Safety
    /// [`WaitQueue::poll_wait`]'s contract, with this semaphore as the queue; `permits` is the
    /// same on every poll of one wait.
    pub unsafe fn poll_acquire_node(
        &self,
        node: Pin<&WaitNode>,
        permits: usize,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        let permits = to_permits(permits);
        if !node.is_pending() {
            node.set_token(count(permits));
        }
        // SAFETY: the caller's contract.
        unsafe {
            self.queue
                .poll_wait(node, cx, |locked| acquire_locked(locked, permits, true))
        }
    }

    /// Stops `node`'s [`Semaphore::poll_acquire_node`] wait: a grant it has not observed goes
    /// back, to the next waiters first.
    ///
    /// # Safety
    /// [`WaitQueue::cancel`]'s contract, with this semaphore as the queue.
    pub unsafe fn cancel_acquire_node(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.queue.cancel(node) }
    }
}

#[cfg(all(test, loom))]
impl Semaphore {
    /// Whether [`SLOW`] is set, for the models that check it is clear once nobody waits.
    pub(super) fn slow_flag(&self) -> bool {
        self.queue.word().load(Ordering::Relaxed) & SLOW != 0
    }
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let locked = self.queue.lock();
        let counts = Counts::of(locked.word().load(Ordering::Acquire));
        f.debug_struct("Semaphore")
            .field("available", &counts.free)
            .field("acquired", &counts.held)
            .field("debt", &locked.debt)
            .field("waiters", &locked.len())
            .finish()
    }
}

/// Permits acquired from a [`Semaphore`]; given back when dropped.
#[must_use = "the permits are released as soon as the guard is dropped"]
pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
    permits: usize,
}

impl SemaphorePermit<'_> {
    /// How many permits it holds.
    pub fn permits(&self) -> usize {
        self.permits
    }

    /// Keeps the permits acquired without a guard; [`Semaphore::release`] gives them back.
    pub fn forget(self) {
        std::mem::forget(self);
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        let released = self.semaphore.release(self.permits);
        debug_assert!(released, "a guard's permits are counted as acquired");
    }
}

impl fmt::Debug for SemaphorePermit<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemaphorePermit")
            .field("permits", &self.permits)
            .finish()
    }
}

/// The future returned by [`Semaphore::acquire`].
#[must_use = "futures do nothing unless polled"]
pub struct SemaphoreAcquire<'a> {
    semaphore: &'a Semaphore,
    /// Its token is the permits wanted.
    node: WaitNode,
    done: bool,
    _pinned: PhantomPinned,
}

impl<'a> Future for SemaphoreAcquire<'a> {
    type Output = SemaphorePermit<'a>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<SemaphorePermit<'a>> {
        // SAFETY: `node` is never moved out of the pinned future.
        let this = unsafe { self.get_unchecked_mut() };
        assert!(
            !this.done,
            "porto: a Semaphore acquire future was polled after it completed; await it once"
        );
        let permits = this.node.token();
        let semaphore = this.semaphore;
        let fast =
            !this.node.is_pending() && try_acquire_fast(semaphore.queue.word(), permits as u64);
        let ready = if fast {
            Poll::Ready(())
        } else {
            // SAFETY: `node` is structurally pinned; `&mut self` serializes calls on it; it is
            // only used with this semaphore, which outlives the future, and `Drop` cancels a
            // pending wait.
            unsafe { semaphore.poll_acquire_node(Pin::new_unchecked(&this.node), permits, cx) }
        };
        ready.map(|()| {
            this.done = true;
            SemaphorePermit { semaphore, permits }
        })
    }
}

impl Drop for SemaphoreAcquire<'_> {
    fn drop(&mut self) {
        // SAFETY: the node was pinned since its first poll and is dropped right after this;
        // `&mut self` serializes; it was only used with this semaphore.
        unsafe {
            self.semaphore
                .cancel_acquire_node(Pin::new_unchecked(&self.node));
        }
    }
}

impl fmt::Debug for SemaphoreAcquire<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemaphoreAcquire")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}
