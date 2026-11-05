//! The synchronization primitives (16-sync-primitives-and-channels.md §1): `porto.Event` and
//! `porto.Notify`.
//!
//! Each wraps the `porto_core::sync` primitive in a [`Waitable`](crate::driver::thin::Waitable),
//! so an `await` that has to wait is a `Notified` trap on it: the task's embedded wait node
//! joins the primitive's queue, with no allocation per wait, and a task that gives the wait up
//! (T3) unlinks the node or passes an unobserved exclusive grant on. A wait that is over at
//! once is a synchronous completion (T2, `checkpoint_if_needed`), taken before any trap.
//!
//! The objects are thread-safe: the methods that do not wait can be called from any thread,
//! and `blocking_*` waits from threads that do not run porto.

mod event;

use porto_core::sync::BlockingOnLane;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::driver::lane;

/// Adds the classes to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<event::Event>()?;
    module.add_class::<event::Notify>()
}

/// Runs a `blocking_*` wait with the thread detached from Python, after refusing a porto lane
/// thread: waiting there would stall every task on the lane.
fn blocking<R: Send>(
    py: Python<'_>,
    operation: &'static str,
    wait: impl FnOnce() -> Result<R, BlockingOnLane> + Send,
) -> PyResult<R> {
    if lane::is_active() {
        return Err(on_lane(operation));
    }
    py.detach(wait)
        .map_err(|refused| on_lane(refused.operation()))
}

fn on_lane(operation: &str) -> PyErr {
    PyRuntimeError::new_err(format!(
        "porto: {operation}() cannot be called on a porto lane thread: it would block every \
         task on the lane. Await the async form instead, or call it from a thread that does \
         not run porto"
    ))
}
