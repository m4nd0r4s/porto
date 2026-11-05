//! The park hook: lets an embedder run code around every park of a lane thread.
//!
//! A lane parks when it has nothing to run (until a wake, or its earliest timer). An embedder
//! that holds a per-thread resource while tasks run can release it for the duration of the park
//! and take it back afterwards. The Python bindings detach the thread state, so other Python
//! threads and the free-threaded stop-the-world GC can proceed (`25-cpython-integration-and-
//! memory.md` P2); the core itself knows nothing about what the hook does.
//!
//! [`Parking`] is the lane's half of a park: the protocol that keeps wakes from other threads
//! from being lost while the lane decides to sleep.

use std::sync::PoisonError;
use std::sync::atomic::Ordering;

use crate::sync::primitives::{AtomicUsize, Mutex, Thread};

/// Wraps every park of a lane thread. Install it with [`Runtime::with_park_hook`].
///
/// [`Runtime::with_park_hook`]: super::Runtime::with_park_hook
pub trait ParkHook: Send + 'static {
    /// Called on the lane thread instead of parking it. MUST call `park` once: it blocks until
    /// the lane is unparked or its earliest timer is due. If it returns without calling `park`,
    /// the lane does not block and checks its queues again.
    fn around_park(&self, park: &mut dyn FnMut());
}

/// No notification is "taken back" while parked.
const EMPTY: usize = 0;
/// The lane is parked, or about to call its park: a notify must unpark it.
const PARKED: usize = 1;
/// Work was published since the lane last looked: its next park returns at once.
const NOTIFIED: usize = 2;

/// A lane's parking slot: a three-state word (Tokio's parker) and the thread to unpark.
///
/// No wake is lost between a lane deciding to park and another thread publishing work for it.
/// A waker publishes its work (an injector push, a root wake) and then swaps the state to
/// NOTIFIED; only if it was PARKED does it pay for an unpark. The lane moves EMPTY to PARKED
/// with a compare-exchange before it blocks, so it either finds the NOTIFIED left by a waker
/// that came first (and does not block), or a later waker finds PARKED and unparks it; an
/// unpark that lands before the block is kept by the thread's park token (or the driver's
/// pending wake event). Each side's read-modify-write of the one word orders it against the
/// other: the lane, having consumed a NOTIFIED (Acquire), sees the work published before it.
///
/// A notify that finds the lane running costs one atomic swap and leaves NOTIFIED behind, so
/// the next park returns at once and the lane looks at its queues again.
pub(crate) struct Parking {
    state: AtomicUsize,
    /// The thread running the lane, set when it starts (`block_on`).
    thread: Mutex<Option<Thread>>,
}

impl Parking {
    pub(crate) fn new() -> Self {
        Self {
            state: AtomicUsize::new(EMPTY),
            thread: Mutex::new(None),
        }
    }

    /// Records the thread that runs the lane, before it first parks.
    pub(crate) fn set_thread(&self, thread: Thread) {
        *self.thread.lock().unwrap_or_else(PoisonError::into_inner) = Some(thread);
    }

    /// The lane side: calls `block` unless a notify arrived since the last park. Returns
    /// whether it blocked. `block` may return early (a spurious wake, an I/O event, a timer);
    /// the lane then looks at its queues and parks again.
    pub(crate) fn park(&self, block: impl FnOnce()) -> bool {
        if let Err(state) =
            self.state
                .compare_exchange(EMPTY, PARKED, Ordering::AcqRel, Ordering::Acquire)
        {
            debug_assert_eq!(state, NOTIFIED, "only the lane parks");
            // Consumes the notification. A swap, not a store: a second waker may have swapped
            // in its own NOTIFIED since the compare-exchange read the first, and reading it
            // here (Acquire) is what makes its work visible. A store would erase it, and the loom
            // models (`sched::loom_tests`) deadlock with one.
            self.state.swap(EMPTY, Ordering::AcqRel);
            return false;
        }
        block();
        // PARKED if it woke for another reason, NOTIFIED if a waker unparked it. Acquire: the
        // waker's work is visible either way.
        self.state.swap(EMPTY, Ordering::AcqRel);
        true
    }

    /// The waker side, after publishing work: marks the lane notified, and returns whether it
    /// was parked, in which case the caller MUST unpark it (its thread, or the driver it
    /// blocks in).
    pub(crate) fn notify(&self) -> bool {
        self.state.swap(NOTIFIED, Ordering::AcqRel) == PARKED
    }

    /// Unparks the lane thread, if it has started.
    pub(crate) fn unpark_thread(&self) {
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(thread) = thread {
            thread.unpark();
        }
    }
}
