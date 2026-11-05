//! The Python lane: a porto-core runtime driven by a thread that keeps its Python thread state
//! attached while it runs tasks, and detaches only while parked (12-scheduler-and-lanes.md §6,
//! 25-cpython-integration-and-memory.md P2).
//!
//! The M0 runtime has one lane, the thread that called `porto.run`: it is attached already, so
//! it needs no thread state of its own. On a GIL build that is lane 0, the only Python lane.
//! Detaching around parks lets other Python threads run, and on free-threaded builds lets a
//! stop-the-world GC proceed.
//!
//! The lane also keeps what belongs to one run and must not outlive it: an error that stops the
//! run (see [`fail`]) and the interrupts filed for waiting tasks (see [`interrupt`]).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::{Poll, Waker};
use std::time::Duration;

use porto_core::config::RuntimeConfig;
use porto_core::task::Header;
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
    /// Exceptions to throw into tasks at their next wait that is not over, by task key.
    interrupts: HashMap<usize, Py<PyBaseException>>,
    /// An exception that stops the run, raised out of `porto.run` in place of main's outcome.
    failure: Option<Py<PyBaseException>>,
    /// The waker of the `block_on` future, to deliver `failure` right away.
    root: Option<Waker>,
}

/// Runs `body` with a fresh single-lane Python runtime configured by `config` (of which it uses
/// `timer_resolution`), on this (attached) thread. The runtime
/// is dropped before returning, still attached, so the tasks it frees release their Python
/// objects directly. Tasks still waiting on a scope are freed after it (their coroutines are
/// closed). A task still waiting on a join or a sync primitive is kept alive by the waker that
/// wait holds and is not freed until the wait is woken (porto-szb.100: shutdown cancels it).
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

/// The key of the task a waker wakes: its data pointer, the task's header for porto-core's
/// task wakers.
fn task_key(waker: &Waker) -> usize {
    waker.data().addr()
}

/// Throws `exception` into the task `waker` wakes, at the `await` it is waiting in, and wakes
/// it. The wait is given up first (T3, `wait.rs`). A task that is not waiting gets it at its
/// next wait that is not over at once. A later interrupt of the same task replaces this one.
///
/// This is how a cancellation reaches a waiting task until scopes deliver it (porto-szb.36:
/// `Scope.cancel` wakes the task, and its poll finds `effective_cancel` instead of this
/// entry). The private `_interrupt` test hook files them.
pub(crate) fn interrupt(waker: &Waker, exception: Py<PyBaseException>) {
    let previous = LANE.with_borrow_mut(|lane| lane.interrupts.insert(task_key(waker), exception));
    drop(previous);
    waker.wake_by_ref();
}

/// Drops the interrupt filed for the task with this header, if any: a task that completes before
/// a wait received it must not keep the exception (and its traceback) alive, nor pass it to a
/// later task allocated at the same address.
pub(crate) fn forget_interrupt(header: &Header) {
    let key = core::ptr::from_ref(header).addr();
    let forgotten = LANE.with_borrow_mut(|lane| {
        if lane.interrupts.is_empty() {
            return None;
        }
        lane.interrupts.remove(&key)
    });
    drop(forgotten);
}

/// Takes the interrupt filed for the task `waker` wakes, if any.
pub(crate) fn take_interrupt(waker: &Waker) -> Option<Py<PyBaseException>> {
    LANE.with_borrow_mut(|lane| {
        if lane.interrupts.is_empty() {
            return None;
        }
        lane.interrupts.remove(&task_key(waker))
    })
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
