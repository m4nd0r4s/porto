//! The Python lane: a porto-core runtime driven by a thread that keeps its Python thread state
//! attached while it runs tasks, and detaches only while parked (12-scheduler-and-lanes.md §6,
//! 25-cpython-integration-and-memory.md P2).
//!
//! The M0 runtime has one lane, the thread that called `porto.run`: it is attached already, so
//! it needs no thread state of its own. On a GIL build that is lane 0, the only Python lane.
//! Detaching around parks lets other Python threads run, and on free-threaded builds lets a
//! stop-the-world GC proceed.
//!
//! The lane also keeps what belongs to one run and must not outlive it: the wakers of tasks
//! waiting for a scope's children (held here, not by the scope, so a task waiting on a scope it
//! owns is not a reference cycle), and an error that stops the run (see [`fail`]).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::{Poll, Waker};
use std::time::Duration;

use porto_core::config::RuntimeConfig;
use porto_core::time::{Deadline, sleep_until};
use porto_core::{Handle, ParkHook, Runtime};
use pyo3::exceptions::PyBaseException;
use pyo3::ffi;
use pyo3::prelude::*;

use super::pytask::TaskOutcome;
use crate::py_compat::take_exception;

/// How often the lane checks for signals (Ctrl-C) while it waits for `main`: the longest a
/// `KeyboardInterrupt` waits when every task is parked.
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(50);

thread_local! {
    /// Whether this thread is running a Python lane (inside [`run`]).
    static ON_PYTHON_LANE: Cell<bool> = const { Cell::new(false) };
    /// What the run on this thread keeps (see the module docs).
    static LANE: RefCell<LaneState> = RefCell::new(LaneState::default());
}

#[derive(Default)]
struct LaneState {
    /// The tasks waiting for a scope's children, by scope key.
    scope_waiters: HashMap<usize, Waker>,
    /// An exception that stops the run, raised out of `porto.run` in place of main's outcome.
    failure: Option<Py<PyBaseException>>,
    /// The waker of the `block_on` future, to deliver `failure` right away.
    root: Option<Waker>,
}

/// Runs `body` with a fresh single-lane Python runtime configured by `config` (of which it uses
/// `timer_resolution`), on this (attached) thread. The runtime
/// is dropped before returning, still attached, so the tasks it frees release their Python
/// objects directly. Tasks still waiting on a scope are freed after it (their coroutines are
/// closed).
pub(crate) fn run<T>(
    _attached: Python<'_>,
    config: &RuntimeConfig,
    body: impl FnOnce(&Runtime) -> T,
) -> T {
    let runtime = Runtime::builder()
        .park_hook(DetachWhileParked)
        .timer_resolution(config.timer_resolution())
        .build();
    let output = {
        let _lane = OnPythonLane::enter();
        body(&runtime)
    };
    drop(runtime);
    // Taken out first: dropping a waker may free its task and close its coroutine, which runs
    // Python code that may reach the lane state again.
    let state = LANE.take();
    drop(state);
    output
}

/// Whether this thread is running a Python lane.
pub(crate) fn is_active() -> bool {
    ON_PYTHON_LANE.get()
}

/// The runtime of the Python lane running on this thread, if any: where tasks are spawned.
pub(crate) fn current() -> Option<Handle> {
    if ON_PYTHON_LANE.get() {
        // Runtimes do not nest (porto-core refuses), so the runtime running here is the lane's.
        Handle::try_current()
    } else {
        None
    }
}

/// Wakes `waker` once the scope keyed `scope` calls [`wake_scope_waiter`]. Replaces an earlier
/// registration for the same scope.
pub(crate) fn register_scope_waiter(scope: usize, waker: Waker) {
    let previous = LANE.with_borrow_mut(|lane| lane.scope_waiters.insert(scope, waker));
    drop(previous);
}

/// Wakes the task waiting for the scope keyed `scope`, if one is.
pub(crate) fn wake_scope_waiter(scope: usize) {
    if let Some(waker) = LANE.with_borrow_mut(|lane| lane.scope_waiters.remove(&scope)) {
        waker.wake();
    }
}

/// Stops the run: `porto.run` raises `exception` instead of returning main's outcome. Used for
/// a `BaseException` that is not an `Exception` (`KeyboardInterrupt`, `SystemExit`) raised by
/// a task nobody waits for. The first one wins.
pub(crate) fn fail(exception: Py<PyBaseException>) {
    let root = LANE.with_borrow_mut(|lane| {
        if lane.failure.is_some() {
            return None;
        }
        lane.failure = Some(exception);
        lane.root.clone()
    });
    if let Some(root) = root {
        root.wake();
    }
}

/// Waits for `main`, the outcome of the run's main task, and meanwhile delivers what stops the
/// run early: a signal handler that raises (`KeyboardInterrupt` on Ctrl-C), checked every
/// [`SIGNAL_CHECK_INTERVAL`], and a [`fail`]ure. MUST be polled by the lane's `block_on`.
pub(crate) async fn main_or_interrupt(main: impl Future<Output = TaskOutcome>) -> TaskOutcome {
    let mut main = pin!(main);
    let mut ticker = pin!(sleep_until(Deadline::after(SIGNAL_CHECK_INTERVAL)));
    poll_fn(|cx| {
        if let Poll::Ready(outcome) = main.as_mut().poll(cx) {
            return Poll::Ready(outcome);
        }
        let failure = LANE.with_borrow_mut(|lane| {
            lane.root = Some(cx.waker().clone());
            lane.failure.take()
        });
        if let Some(exception) = failure {
            return Poll::Ready(TaskOutcome::Err(exception));
        }
        while ticker.as_mut().poll(cx).is_ready() {
            // SAFETY: the lane's `block_on` polls this future with the thread state attached
            // (only parks detach). Runs the Python handlers of the signals that arrived.
            if unsafe { ffi::PyErr_CheckSignals() } < 0 {
                // SAFETY: attached, see above.
                let py = unsafe { Python::assume_attached() };
                return Poll::Ready(TaskOutcome::Err(take_exception(py).unbind()));
            }
            ticker
                .as_mut()
                .reset(Deadline::after(SIGNAL_CHECK_INTERVAL));
        }
        Poll::Pending
    })
    .await
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
