//! [`Barrier`]: tasks wait until a set number of them arrived, then all go on together
//! (16-sync-primitives-and-channels.md §1).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{BlockingOnLane, Locked, Wait, WaitNode, WaitQueue, WaitState, blocking_wait};

/// A reusable barrier for `parties` tasks.
///
/// Each [`Barrier::wait`] arrives and waits; the `parties`-th arrival releases every waiter at
/// once and starts the next generation, so the barrier can be used again. That last arrival is
/// the generation's **leader**: its wait completes without suspending and reports
/// [`BarrierWaitResult::is_leader`], exactly one per generation.
///
/// A waiter dropped before the generation completes leaves it: it no longer counts as arrived,
/// so the others wait for one more arrival. One dropped after the release has nothing to undo.
///
/// Thread-safe: share it across lanes and threads.
///
/// ```
/// use porto_core::Runtime;
/// use porto_core::sync::Barrier;
///
/// let rt = Runtime::new();
/// let barrier = Barrier::new(1);
/// assert!(rt.block_on(barrier.wait()).is_leader());
/// ```
pub struct Barrier {
    queue: WaitQueue<BarrierState>,
}

/// The first-poll check of a wait, as a nameable type.
type Acquire = fn(&mut Locked<'_, BarrierState>) -> bool;

/// The queue state.
#[derive(Debug)]
struct BarrierState {
    parties: usize,
    /// Arrivals in the current generation; every one but the leader is queued.
    arrived: usize,
}

impl WaitState for BarrierState {
    /// Unreachable: a barrier releases with `grant_all`, whose wakes are never passed on.
    fn pass_grant(_: &mut Locked<'_, Self>, _: usize) {}

    /// A queued waiter left the current generation.
    fn waiter_left(waiters: &mut Locked<'_, Self>, _: usize) {
        waiters.arrived -= 1;
    }
}

/// Arrives: the last arrival releases the generation and completes at once (the leader);
/// any other is counted, for the caller to queue.
fn arrive(locked: &mut Locked<'_, BarrierState>) -> bool {
    if trip(locked) {
        return true;
    }
    locked.arrived += 1;
    false
}

/// Releases the generation if one more arrival completes it.
fn trip(locked: &mut Locked<'_, BarrierState>) -> bool {
    if locked.arrived + 1 < locked.parties {
        return false;
    }
    locked.arrived = 0;
    locked.grant_all();
    true
}

impl Barrier {
    /// A barrier for `parties` tasks.
    ///
    /// # Panics
    /// If `parties` is 0.
    pub fn new(parties: usize) -> Self {
        assert!(
            parties > 0,
            "porto: a Barrier needs at least one party; pass parties >= 1"
        );
        Self {
            queue: WaitQueue::new(BarrierState {
                parties,
                arrived: 0,
            }),
        }
    }

    /// How many tasks each generation waits for.
    pub fn parties(&self) -> usize {
        self.queue.lock().parties
    }

    /// How many tasks arrived in the current generation and wait. A snapshot.
    pub fn waiting(&self) -> usize {
        self.queue.lock().arrived
    }

    /// Arrives and waits for the generation to complete. Arrival happens on the first poll.
    /// Dropping the future before the release leaves the generation.
    pub fn wait(&self) -> BarrierWait<'_> {
        BarrierWait {
            wait: self.queue.wait(0, arrive as Acquire),
            polled: false,
        }
    }

    /// [`Barrier::wait`] for a thread that does not run a porto lane.
    ///
    /// # Errors
    /// [`BlockingOnLane`] when called on a lane thread.
    pub fn blocking_wait(&self) -> Result<BarrierWaitResult, BlockingOnLane> {
        blocking_wait("Barrier.blocking_wait", self.wait())
    }

    /// Completes the generation if one more arrival does, as its leader; otherwise arrives at
    /// nothing and returns `false` (the caller then waits with [`Barrier::poll_wait_node`],
    /// which arrives).
    pub fn try_lead(&self) -> bool {
        trip(&mut self.queue.lock())
    }

    /// [`Barrier::wait`] with a caller-owned node, for a waiter that embeds its [`WaitNode`] (a
    /// Python task). The first poll arrives: `Ready` there means it was the last arrival (the
    /// leader); a later `Ready` means the generation completed.
    ///
    /// # Safety
    /// [`WaitQueue::poll_wait`]'s contract, with this barrier as the queue.
    pub unsafe fn poll_wait_node(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.queue.poll_wait(node, cx, arrive) }
    }

    /// Stops `node`'s [`Barrier::poll_wait_node`] wait: a queued waiter leaves the generation.
    ///
    /// # Safety
    /// [`WaitQueue::cancel`]'s contract, with this barrier as the queue.
    pub unsafe fn cancel_wait_node(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.queue.cancel(node) }
    }
}

impl fmt::Debug for Barrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let locked = self.queue.lock();
        f.debug_struct("Barrier")
            .field("parties", &locked.parties)
            .field("arrived", &locked.arrived)
            .finish()
    }
}

/// What a [`Barrier::wait`] evaluates to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarrierWaitResult {
    leader: bool,
}

impl BarrierWaitResult {
    /// Whether this wait was the generation's last arrival: true for exactly one per
    /// generation.
    pub fn is_leader(&self) -> bool {
        self.leader
    }
}

/// The future returned by [`Barrier::wait`].
#[must_use = "futures do nothing unless polled"]
#[derive(Debug)]
pub struct BarrierWait<'a> {
    wait: Wait<'a, BarrierState, Acquire>,
    /// Whether a poll returned `Pending`: then the wait was released, not the leader.
    polled: bool,
}

impl Future for BarrierWait<'_> {
    type Output = BarrierWaitResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BarrierWaitResult> {
        // SAFETY: `wait` is structurally pinned: it is never moved out of `self`.
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: as above.
        let poll = unsafe { Pin::new_unchecked(&mut this.wait) }.poll(cx);
        let leader = !this.polled;
        this.polled = true;
        poll.map(|()| BarrierWaitResult { leader })
    }
}
