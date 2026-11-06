//! The synchronization primitives (16-sync-primitives-and-channels.md §1): `porto.Event`,
//! `porto.Notify`, `porto.Lock` and `porto.Mutex[T]` with its `MutexGuard`.
//!
//! Each wraps the `porto_core::sync` primitive in a [`Waitable`](crate::driver::thin::Waitable)
//! and waits through a lazy `_SyncWait` awaitable (`wait_op`): an `await` that has to wait is
//! a `Notified` trap on the primitive, so the task's embedded wait node joins its queue, with
//! no allocation per wait, and a task that gives the wait up (T3) unlinks the node or passes an
//! unobserved exclusive grant on. A wait that is over at once is a synchronous completion (T2),
//! and a grant is only taken in the step that hands it to the coroutine. `async with` on a
//! `Lock` or `Mutex` is the same lazy wait: `__aenter__` takes nothing, and the statement's
//! await takes a free lock on the first resume, without suspending.
//!
//! The objects are thread-safe: the methods that do not wait can be called from any thread,
//! and `blocking_*` waits from threads that do not run porto. A `blocking_*` wait runs with
//! the thread detached from Python and does not check for signals, so Ctrl-C does not
//! interrupt it: the `KeyboardInterrupt` is raised once the wait is over.

mod event;
mod lock;
mod mutex;
mod wait_op;

use porto_core::sync::BlockingOnLane;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::async_slots;
use crate::driver::freelist;
use crate::driver::lane;

/// Adds the classes to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<wait_op::SyncWait>()?;
    // Right after `add_class`, before anything can see the type (see `async_slots`).
    async_slots::install_am_send::<wait_op::SyncWait>(module.py(), wait_op::sync_wait_am_send)?;
    async_slots::install_iter_slots::<wait_op::SyncWait>(module.py(), wait_op::sync_wait_iternext)?;
    freelist::install::<wait_op::SyncWait>(module.py(), &sync_wait_freelist::SLOTS)?;
    wait_op::init_fast_new(module.py())?;
    module.add_class::<event::Event>()?;
    module.add_class::<event::Notify>()?;
    module.add_class::<lock::Lock>()?;
    lock::install_raw_methods(module.py())?;
    module.add_class::<mutex::Mutex>()?;
    mutex::install_raw_methods(module.py())?;
    module.add_class::<mutex::MutexGuard>()?;
    freelist::install::<mutex::MutexGuard>(module.py(), &guard_freelist::SLOTS)
}

freelist::gil_freelist!(sync_wait_freelist, 256, traversed: true);
freelist::gil_freelist!(guard_freelist, 64, traversed: true);

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
