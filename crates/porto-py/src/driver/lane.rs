//! The Python lane: a porto-core runtime driven by a thread that keeps its Python thread state
//! attached while it runs tasks, and detaches only while parked (12-scheduler-and-lanes.md §6,
//! 25-cpython-integration-and-memory.md P2).
//!
//! The M0 runtime has one lane, the thread that called `porto.run`: it is attached already, so
//! it needs no thread state of its own. On a GIL build that is lane 0, the only Python lane.
//! Detaching around parks lets other Python threads run, and on free-threaded builds lets a
//! stop-the-world GC proceed.
//!
//! Each run owns an I/O driver (the `io_backend` the config selects, through mio): the idle lane
//! blocks in its `turn` until the next timer is due, detached, and a wake from another thread
//! (a spawn, an `Event.set()`, any wake of a task) interrupts it through the driver's
//! waker (12-scheduler-and-lanes.md §9).
//!
//! The lane also keeps what belongs to one run and must not outlive it: an error that stops the
//! run (see [`fail`]), the interrupts a test hook files for waiting tasks (see [`interrupt`]),
//! and the wakers of tasks whose wait would otherwise keep them alive forever once the run ends
//! (see [`keep_linked_wait`]).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::hash::{BuildHasherDefault, Hasher};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};
use std::time::Duration;

use porto_core::config::RuntimeConfig;
use porto_core::task::Header;
use porto_core::time::{Deadline, sleep_until};
use porto_core::{Handle, ParkHook, Runtime};
use porto_io::MioDriver;
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyRuntimeWarning};
use pyo3::ffi;
use pyo3::prelude::*;

use super::pytask::TaskOutcome;
use crate::py_compat::take_exception;

/// How often the lane checks for signals (Ctrl-C) while it waits for `main`: the longest a
/// `KeyboardInterrupt` waits when every task is parked.
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(50);

/// How many runs in the process are in debug mode.
static DEBUG_RUNS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// Whether this thread is running a Python lane (inside [`run`]).
    static ON_PYTHON_LANE: Cell<bool> = const { Cell::new(false) };
    /// Whether the run on this thread is in debug mode (`RuntimeConfig.debug`): read only by
    /// the cold paths that report mistakes (a swallowed `Cancelled`, 15 C11).
    static DEBUG: Cell<bool> = const { Cell::new(false) };
    /// What the run on this thread keeps (see the module docs).
    static LANE: RefCell<LaneState> = RefCell::new(LaneState::default());
}

/// The hasher of the lane's maps, whose keys are task addresses (distinct, not chosen by
/// anyone): a multiply and a rotate instead of SipHash, which showed in every spawn that waits
/// for its scope (porto-itr.7).
type ByAddress = BuildHasherDefault<AddressHasher>;

/// See [`ByAddress`].
#[derive(Default)]
struct AddressHasher(u64);

impl Hasher for AddressHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.write_u64(u64::from(byte));
        }
    }

    fn write_usize(&mut self, key: usize) {
        self.write_u64(key as u64);
    }

    fn write_u64(&mut self, key: u64) {
        // Fibonacci hashing: the multiply spreads the address bits into the high ones, the
        // rotate brings some back down for the bucket index.
        self.0 = (self.0 ^ key)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .rotate_left(26);
    }
}

#[derive(Default)]
struct LaneState {
    /// The wakers of the tasks in a linked wait, by the key `keep_linked_wait` got.
    linked_waits: HashMap<usize, Waker, ByAddress>,
    /// Exceptions to throw into tasks at their next wait that is not over, by task key.
    interrupts: HashMap<usize, Py<PyBaseException>, ByAddress>,
    /// An exception that stops the run, raised out of `porto.run` in place of main's outcome.
    failure: Option<Py<PyBaseException>>,
    /// The waker of the `block_on` future, to deliver `failure` right away.
    root: Option<Waker>,
}

/// Runs `body` with a fresh single-lane Python runtime configured by `config` (of which it uses
/// `io_backend`, `io_backend_strict`, `timer_resolution` and `coop_budget`), on this
/// (attached) thread. The runtime is dropped before returning, still attached, so the tasks it
/// frees release their Python objects directly. Then every task still in a linked wait (a join,
/// a sync primitive, a scope exit) is cancelled: its wait is given up and its coroutine closed
/// (spec 11 §3.2), so no task of the run outlives it.
///
/// # Errors
///
/// When the I/O driver cannot be opened: the `io_backend` is unavailable and
/// `io_backend_strict` is set (`RuntimeError`), or the OS refused the poller (`OSError`). A
/// `RuntimeWarning` for an unavailable `io_backend` that fell back, if warnings are errors.
pub(crate) fn run<T>(
    py: Python<'_>,
    config: &RuntimeConfig,
    body: impl FnOnce(&Runtime) -> T,
) -> PyResult<T> {
    let runtime = Runtime::builder()
        .park_hook(DetachWhileParked)
        .timer_resolution(config.timer_resolution())
        .coop_budget(config.coop_budget())
        .driver(open_driver(py, config)?)
        .build();
    let output = {
        let _lane = OnPythonLane::enter(config.debug());
        body(&runtime)
    };
    drop(runtime);
    cancel_linked_waits();
    // Taken out first: dropping a waker may free its task and close its coroutine, which runs
    // Python code that may reach the lane state again.
    let state = LANE.take();
    drop(state);
    // The run's pooled buffers go with it (13-io-driver.md §6).
    crate::io::pool::clear();
    crate::io::op::clear_run_state();
    Ok(output)
}

/// The I/O driver `config` selects, after warning about a fallback from an unavailable
/// `io_backend`.
fn open_driver(py: Python<'_>, config: &RuntimeConfig) -> PyResult<Arc<MioDriver>> {
    let (driver, fallback) = MioDriver::open(config.io_backend_config()).map_err(|error| {
        match error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<porto_io::UnavailableBackend>())
        {
            Some(unavailable) => PyRuntimeError::new_err(format!("porto: {unavailable}")),
            None => PyErr::from(std::io::Error::new(
                error.kind(),
                format!(
                    "porto: could not open the I/O driver ({error}); the runtime needs one \
                     poller and one wake handle: raise the open-file limit (`ulimit -n`) or \
                     close unused files"
                ),
            )),
        }
    })?;
    if let Some(fallback) = fallback {
        let message = std::ffi::CString::new(format!("porto: {fallback}"))
            .expect("the warning has no NUL byte");
        PyErr::warn(py, &py.get_type::<PyRuntimeWarning>(), &message, 1)?;
    }
    Ok(Arc::new(driver))
}

/// Whether the run on this thread is in debug mode.
pub(crate) fn debug() -> bool {
    DEBUG.get()
}

/// Whether any run in the process is in debug mode: one relaxed load, for the checks that sit
/// on a path every task takes (the swallowed-`Cancelled` report as a task ends).
#[inline]
pub(crate) fn any_debug() -> bool {
    DEBUG_RUNS.load(Ordering::Relaxed) != 0
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

/// Keeps `waker`, the waker of the task keyed `key`, until [`forget_linked_wait`]: the task
/// waits on something that holds its waker while the task holds it (a join, a sync primitive,
/// a scope exit), a cycle nothing outside it may ever end. If the run ends first, the lane
/// wakes the waker, which cancels the task ([`run`]).
pub(crate) fn keep_linked_wait(key: usize, waker: &Waker) {
    let previous = LANE.with_borrow_mut(|lane| lane.linked_waits.insert(key, waker.clone()));
    drop(previous);
}

/// Stops keeping the waker of the task keyed `key`: its wait ended.
///
/// Safe from any thread: a task cancelled off its lane (a wake after shutdown) finds nothing
/// to forget there, also while that thread's locals are being destroyed.
pub(crate) fn forget_linked_wait(key: usize) {
    let kept = LANE
        .try_with(|lane| lane.borrow_mut().linked_waits.remove(&key))
        .ok()
        .flatten();
    // Dropped after the borrow: the last reference to a task frees it, which runs Python code.
    drop(kept);
}

/// Cancels every task still in a linked wait, once the runtime is gone: a wake after shutdown
/// cancels its task, which gives the wait up (T3) and closes the coroutine. A cancelled task
/// may release others (the target of its join), which then are freed too.
fn cancel_linked_waits() {
    loop {
        let waiting = LANE.with_borrow_mut(|lane| std::mem::take(&mut lane.linked_waits));
        if waiting.is_empty() {
            return;
        }
        for waker in waiting.into_values() {
            waker.wake();
        }
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
/// A scope's cancellation reaches a waiting task the same way with `Cancelled` (`step.rs`);
/// this is for the private `_interrupt` test hook, which throws other exceptions, also into
/// waits a cancellation does not give up (`ScopeExit`).
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
struct OnPythonLane {
    debug: bool,
}

impl OnPythonLane {
    fn enter(debug: bool) -> Self {
        ON_PYTHON_LANE.set(true);
        DEBUG.set(debug);
        if debug {
            DEBUG_RUNS.fetch_add(1, Ordering::Relaxed);
        }
        Self { debug }
    }
}

impl Drop for OnPythonLane {
    fn drop(&mut self) {
        ON_PYTHON_LANE.set(false);
        DEBUG.set(false);
        if self.debug {
            DEBUG_RUNS.fetch_sub(1, Ordering::Relaxed);
        }
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
