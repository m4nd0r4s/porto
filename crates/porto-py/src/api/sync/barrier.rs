//! `porto.Barrier`: tasks wait until a set number of them arrived, then go on together
//! (16-sync-primitives-and-channels.md §1).

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use porto_core::sync::{Barrier as CoreBarrier, WaitNode};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::blocking;
use super::wait_op::{Grant, SyncWait};
use crate::api::time::require_lane;
use crate::driver::thin::{WaitSource, Waitable};

/// One `wait()`: its node arrives on the first poll, which completes at once for the
/// generation's last arrival, the leader.
pub(super) struct Arrival {
    barrier: Arc<CoreBarrier>,
    leader: AtomicBool,
}

impl Arrival {
    /// A wait on `barrier`.
    fn waitable(barrier: &Arc<CoreBarrier>) -> Arc<Waitable<Self>> {
        Waitable::new(Self {
            barrier: Arc::clone(barrier),
            leader: AtomicBool::new(false),
        })
    }

    /// Completes the generation now if this arrival is its last; then it leads.
    pub(super) fn try_lead(&self) -> bool {
        let led = self.barrier.try_lead();
        if led {
            self.leader.store(true, Ordering::Relaxed);
        }
        led
    }

    /// Whether this arrival led its generation.
    pub(super) fn led(&self) -> bool {
        self.leader.load(Ordering::Relaxed)
    }
}

// SAFETY: forwards to the barrier's node API, whose contract is `WaitSource`'s.
unsafe impl WaitSource for Arrival {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        let arriving = !node.is_pending();
        // SAFETY: the caller's contract.
        let poll = unsafe { self.barrier.poll_wait_node(node, cx) };
        if arriving && poll.is_ready() {
            self.leader.store(true, Ordering::Relaxed);
        }
        poll
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.barrier.cancel_wait_node(node) }
    }
}

/// `porto.Barrier(parties)`: each `await barrier.wait()` waits until `parties` tasks arrived,
/// then all go on; the barrier is then ready for the next round. The last arrival of a round
/// is its leader: its wait returns `True`, every other one `False`. A task cancelled while it
/// waits leaves the round, which then waits for one more arrival.
#[pyclass(frozen, module = "porto", name = "Barrier")]
pub(super) struct Barrier {
    barrier: Arc<CoreBarrier>,
}

#[pymethods]
impl Barrier {
    #[new]
    #[pyo3(signature = (parties, /))]
    fn new(parties: i64) -> PyResult<Self> {
        let parties = usize::try_from(parties)
            .ok()
            .filter(|&parties| parties > 0)
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "porto: Barrier({parties}) needs at least one party; pass the number of \
                     tasks that wait for each other"
                ))
            })?;
        Ok(Self {
            barrier: Arc::new(CoreBarrier::new(parties)),
        })
    }

    /// An awaitable that arrives and completes once the round's last task arrived; it
    /// evaluates to `True` for that last task (the leader), `False` for the others. Nothing
    /// happens until it is awaited.
    fn wait(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("Barrier.wait")?;
        SyncWait::new(Grant::Arrival(Arrival::waitable(&self.barrier))).into_object(py)
    }

    /// Blocks the calling thread until the round completes; `True` for its leader. Only for
    /// threads that do not run porto: on a lane thread it raises `RuntimeError`.
    fn blocking_wait(&self, py: Python<'_>) -> PyResult<bool> {
        blocking(py, "Barrier.blocking_wait", || {
            self.barrier
                .blocking_wait()
                .map(|result| result.is_leader())
        })
    }

    /// How many tasks each round waits for.
    #[getter]
    fn parties(&self) -> usize {
        self.barrier.parties()
    }

    /// How many tasks wait in the current round.
    #[getter]
    fn n_waiting(&self) -> usize {
        self.barrier.waiting()
    }

    fn __repr__(&self) -> String {
        format!(
            "<porto.Barrier waiting={}/{}>",
            self.barrier.waiting(),
            self.barrier.parties()
        )
    }
}
