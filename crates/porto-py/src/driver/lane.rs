//! The Python lane: a porto-core runtime driven by a thread that keeps its Python thread state
//! attached while it runs tasks, and detaches only while parked (12-scheduler-and-lanes.md §6,
//! 25-cpython-integration-and-memory.md P2).
//!
//! The M0 runtime has one lane, the thread that called `_run`: it is attached already, so it
//! needs no thread state of its own. On a GIL build that is lane 0, the only Python lane.
//! Detaching around parks lets other Python threads run, and on free-threaded builds lets a
//! stop-the-world GC proceed.

use std::cell::Cell;

use porto_core::{Handle, ParkHook, Runtime};
use pyo3::ffi;
use pyo3::prelude::*;

thread_local! {
    /// Whether this thread is running a Python lane (inside [`run`]).
    static ON_PYTHON_LANE: Cell<bool> = const { Cell::new(false) };
}

/// Runs `body` with a fresh single-lane Python runtime, on this (attached) thread. The runtime
/// is dropped before returning, still attached, so the tasks it frees release their Python
/// objects directly.
pub(crate) fn run<T>(_attached: Python<'_>, body: impl FnOnce(&Runtime) -> T) -> T {
    let runtime = Runtime::with_park_hook(DetachWhileParked);
    let _lane = OnPythonLane::enter();
    body(&runtime)
}

/// The runtime of the Python lane running on this thread, if any: where `_spawn` puts tasks.
pub(crate) fn current() -> Option<Handle> {
    if ON_PYTHON_LANE.get() {
        // Runtimes do not nest (porto-core refuses), so the runtime running here is the lane's.
        Handle::try_current()
    } else {
        None
    }
}

/// Marks this thread as a Python lane until dropped.
struct OnPythonLane;

impl OnPythonLane {
    fn enter() -> Self {
        ON_PYTHON_LANE.set(true);
        Self
    }
}

impl Drop for OnPythonLane {
    fn drop(&mut self) {
        ON_PYTHON_LANE.set(false);
    }
}

/// Detaches the thread state for the duration of each park.
struct DetachWhileParked;

impl ParkHook for DetachWhileParked {
    fn around_park(&self, park: &mut dyn FnMut()) {
        // SAFETY: the hook runs on the lane thread, inside `run`, which is attached. No task
        // runs while the lane is parked, so nothing touches Python until `Detached` reattaches,
        // also if `park` unwinds.
        let _detached = Detached(unsafe { ffi::PyEval_SaveThread() });
        park();
    }
}

/// A detached thread state, reattached on drop.
struct Detached(*mut ffi::PyThreadState);

impl Drop for Detached {
    fn drop(&mut self) {
        // SAFETY: the pointer came from `PyEval_SaveThread` on this thread, which has not
        // reattached since.
        unsafe { ffi::PyEval_RestoreThread(self.0) };
    }
}
