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
//! run (see [`fail`]) and the interrupts a test hook files for waiting tasks (see [`interrupt`]).
//! Tasks whose wait would keep them alive forever once the run ends (a join, a sync primitive, a
//! scope exit: a waker cycle) are cancelled by the runtime's registry as it is dropped
//! (23-observability.md §1).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::hash::{BuildHasherDefault, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration;

use porto_core::config::RuntimeConfig;
use porto_core::task::Header;
use porto_core::time::{Deadline, sleep_until};
use porto_core::{Handle, ParkHook, Runtime};
use porto_io::{Driver, MioDriver};
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyRuntimeWarning};
use pyo3::ffi;
use pyo3::prelude::*;

use super::pytask::TaskOutcome;
use crate::api::threads::PythonWorkers;
use crate::py_compat::take_exception;

/// How often the lane checks for signals (Ctrl-C) while it waits for `main`: the longest a
/// `KeyboardInterrupt` waits when every task is parked.
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(50);

/// How many runs in the process are in debug mode.
static DEBUG_RUNS: AtomicUsize = AtomicUsize::new(0);

/// How many runs in the process have the introspection switch on (tasks record their timing).
static INTROSPECTION_RUNS: AtomicUsize = AtomicUsize::new(0);

/// The id of the next run: a scope remembers the run it was entered in.
static NEXT_RUN: AtomicUsize = AtomicUsize::new(1);

/// The runs in progress in the process, each with its I/O driver: what tells an object that
/// belongs to a run whether that run stopped (I4, 10 §5).
static ACTIVE_RUNS: Mutex<Vec<(usize, Arc<dyn Driver>)>> = Mutex::new(Vec::new());

fn active_runs() -> std::sync::MutexGuard<'static, Vec<(usize, Arc<dyn Driver>)>> {
    ACTIVE_RUNS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The I/O driver of the run on this thread, if one runs.
pub(crate) fn current_driver() -> Option<Arc<dyn Driver>> {
    let run = RUN.get();
    active_runs()
        .iter()
        .find(|(active, _)| *active == run)
        .map(|(_, driver)| driver.clone())
}

/// Whether the run with this id (nonzero) stopped. Runs end for good: an id is never reused.
pub(crate) fn run_has_ended(run: usize) -> bool {
    run != 0 && !active_runs().iter().any(|(active, _)| *active == run)
}

/// Whether the run that `registration` was opened in stopped.
pub(crate) fn registration_run_has_ended<S: porto_io::IoSource>(
    registration: &porto_io::Registration<S>,
) -> bool {
    !active_runs()
        .iter()
        .any(|(_, driver)| registration.is_driven_by(driver))
}

thread_local! {
    /// The id of the run on this thread, 0 outside one (from [`run`] until it returns, teardown
    /// included).
    static RUN: Cell<usize> = const { Cell::new(0) };
    /// The runtime of the run on this thread while it is being dropped: code that runs as the
    /// drop cancels a task (a `finally`) can still ask it for the tasks left (`Scope.tasks`).
    static TEARDOWN: RefCell<Option<Handle>> = const { RefCell::new(None) };
    /// The tasks whose coroutines awaited while they were closed at the end of the run: name and
    /// stack of each (see `abandon.rs`).
    static STUCK: RefCell<Vec<(String, String)>> = const { RefCell::new(Vec::new()) };
    /// Whether this thread is running a Python lane (inside [`run`]).
    static ON_PYTHON_LANE: Cell<bool> = const { Cell::new(false) };
    /// Whether the run on this thread is in debug mode (`RuntimeConfig.debug`): read only by
    /// the cold paths that report mistakes (a swallowed `Cancelled`, 15 C11).
    static DEBUG: Cell<bool> = const { Cell::new(false) };
    /// Test hook (`_arm_scheduler_panic`): the next poll of the run's `main_or_interrupt` future
    /// panics, outside any task.
    static SCHEDULER_PANIC_ARMED: Cell<bool> = const { Cell::new(false) };
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
    /// Exceptions to throw into tasks at their next wait that is not over, by task key.
    interrupts: HashMap<usize, Py<PyBaseException>, ByAddress>,
    /// An exception that stops the run, raised out of `porto.run` in place of main's outcome.
    failure: Option<Py<PyBaseException>>,
    /// The waker of the `block_on` future, to deliver `failure` right away.
    root: Option<Waker>,
}

/// Runs `body` with a fresh single-lane Python runtime configured by `config` (of which it uses
/// `io_backend`, `io_backend_strict`, `timer_resolution`, `coop_budget`, `blocking_threads` and
/// `blocking_keepalive`), on this
/// (attached) thread. The runtime is dropped before returning, still attached, so the tasks it
/// frees release their Python objects directly. Dropping it cancels every task still alive
/// (the registry, 23 §1), including those kept only by a waker cycle (a join, a sync primitive,
/// a scope exit): its wait is given up and its coroutine closed (spec 11 §3.2), so no task of
/// the run outlives it.
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
    let driver = open_driver(py, config)?;
    let runtime = Runtime::builder()
        .park_hook(DetachWhileParked::new(py))
        .timer_resolution(config.timer_resolution())
        .coop_budget(config.coop_budget())
        .blocking_threads(config.blocking_threads())
        .blocking_keepalive(config.blocking_keepalive())
        .blocking_hooks(PythonWorkers::of_current_interpreter(py))
        .driver(driver.clone())
        .introspection(config.introspection())
        .build();
    let id = NEXT_RUN.fetch_add(1, Ordering::Relaxed);
    RUN.set(id);
    let as_driver: Arc<dyn Driver> = driver;
    active_runs().push((id, as_driver));
    let _run = RunScope(id);
    let output = {
        let _lane = OnPythonLane::enter(config.debug(), config.introspection());
        let output = body(&runtime);
        // The signals the run received stop being delivered: the previous dispositions are
        // back before anything of the run is dropped.
        crate::api::signals::end_of_run();
        output
    };
    // Dropping the runtime cancels what is left: the tasks' `finally` blocks run in it, and
    // may list a scope's tasks, which the registry answers while it shuts down.
    TEARDOWN.replace(Some(runtime.handle().clone()));
    drop(runtime);
    drop(TEARDOWN.take());
    super::abandon::warn_stuck(py);
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

/// The id of the run on this thread (inside [`run`], teardown included), 0 outside one.
pub(crate) fn run_id() -> usize {
    RUN.get()
}

/// The runtime of the run on this thread while it is being dropped: its registry is closed and
/// answers walks under its lock.
pub(crate) fn teardown_handle() -> Option<Handle> {
    TEARDOWN.with_borrow(Clone::clone)
}

/// Whether any run in the process has introspection on: one relaxed load per poll, which is all
/// the hot path pays for the timing a step records only then (spec 23 §1). A run with it off
/// whose tasks a run with it on shares a process with times its steps too: harmless.
#[inline]
pub(crate) fn any_introspection() -> bool {
    INTROSPECTION_RUNS.load(Ordering::Relaxed) != 0
}

/// Whether this thread is running a Python lane.
pub(crate) fn is_active() -> bool {
    ON_PYTHON_LANE.get()
}

/// Whether the run on this thread is being dropped: tasks cancelled now are cancelled by the
/// end of the run.
pub(crate) fn tearing_down() -> bool {
    TEARDOWN.with_borrow(Option::is_some)
}

/// Records a task whose coroutine could not be closed (see `abandon.rs`).
pub(crate) fn record_stuck(name: String, stack: String) {
    STUCK.with_borrow_mut(|stuck| stuck.push((name, stack)));
}

/// Takes the tasks recorded by [`record_stuck`].
pub(crate) fn take_stuck() -> Vec<(String, String)> {
    STUCK.take()
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

/// Drives `future` on `runtime`'s lane (the run's `block_on`). A panic in a task never gets here:
/// `PyTask::poll` turns it into a `porto.RustPanic` failure of that task. One that does is a
/// panic in scheduler internals (the queues, the timers, the driver, the lane's own futures),
/// which leaves state that no task can repair: it is logged and the process aborts (ADR-027,
/// spec 25 §8), instead of unwinding into CPython or running on with a broken scheduler.
pub(crate) fn block_on<F: Future>(runtime: &Runtime, future: F) -> F::Output {
    match catch_unwind(AssertUnwindSafe(|| runtime.block_on(future))) {
        Ok(output) => output,
        Err(payload) => abort_after_scheduler_panic(&*payload),
    }
}

/// Logs a panic of the scheduler's own code and aborts the process.
#[cold]
fn abort_after_scheduler_panic(payload: &(dyn core::any::Any + Send)) -> ! {
    let panic = crate::errors::rust_panic_description(payload);
    // The panic hook already printed the message and location; this says what it means.
    eprintln!(
        "porto: fatal: a Rust panic in the scheduler ({panic}); the runtime cannot continue and \
         the process aborts. This is a bug in porto: please report it with the panic message above"
    );
    std::process::abort();
}

/// Test hook: makes the next poll of [`main_or_interrupt`] on this thread panic, which is a panic
/// in scheduler internals ([`block_on`] aborts the process).
pub(crate) fn arm_scheduler_panic() {
    SCHEDULER_PANIC_ARMED.set(true);
}

/// Waits for `main`, the outcome of the run's main task, and meanwhile delivers what stops the
/// run early: a signal handler that raises (`KeyboardInterrupt` on Ctrl-C), checked every
/// [`SIGNAL_CHECK_INTERVAL`], and a [`fail`]ure. MUST be polled by the lane's `block_on`.
pub(crate) async fn main_or_interrupt(main: impl Future<Output = TaskOutcome>) -> TaskOutcome {
    match watch_for_interrupt(main).await {
        Interrupted::Done(outcome) => outcome,
        Interrupted::Interrupt(exception) => TaskOutcome::Err(exception),
    }
}

/// What [`watch_for_interrupt`] ended with.
pub(crate) enum Interrupted<T> {
    /// The future finished.
    Done(T),
    /// An exception stopped the run first.
    Interrupt(Py<PyBaseException>),
}

/// [`main_or_interrupt`] for any future: the future's output, or what stopped the run first.
pub(crate) async fn watch_for_interrupt<T>(main: impl Future<Output = T>) -> Interrupted<T> {
    let mut main = pin!(main);
    let mut ticker = pin!(sleep_until(Deadline::after(SIGNAL_CHECK_INTERVAL)));
    poll_fn(|cx| {
        assert!(
            !SCHEDULER_PANIC_ARMED.replace(false),
            "test hook: a panic in the scheduler"
        );
        if let Poll::Ready(output) = main.as_mut().poll(cx) {
            return Poll::Ready(Interrupted::Done(output));
        }
        let failure = LANE.with_borrow_mut(|lane| {
            lane.root = Some(cx.waker().clone());
            lane.failure.take()
        });
        if let Some(exception) = failure {
            return Poll::Ready(Interrupted::Interrupt(exception));
        }
        while ticker.as_mut().poll(cx).is_ready() {
            // SAFETY: the lane's `block_on` polls this future with the thread state attached
            // (only parks detach). Runs the Python handlers of the signals that arrived.
            if unsafe { ffi::PyErr_CheckSignals() } < 0 {
                // SAFETY: attached, see above.
                let py = unsafe { Python::assume_attached() };
                return Poll::Ready(Interrupted::Interrupt(take_exception(py).unbind()));
            }
            ticker
                .as_mut()
                .reset(Deadline::after(SIGNAL_CHECK_INTERVAL));
        }
        Poll::Pending
    })
    .await
}

/// Ends the run id of [`run`] when it returns, unwinding included.
struct RunScope(usize);

impl Drop for RunScope {
    fn drop(&mut self) {
        RUN.set(0);
        drop(TEARDOWN.take());
        let driver = {
            let mut active = active_runs();
            active
                .iter()
                .position(|(id, _)| *id == self.0)
                .map(|at| active.swap_remove(at))
        };
        drop(driver);
    }
}

/// Marks this thread as a Python lane until dropped.
struct OnPythonLane {
    debug: bool,
    introspection: bool,
}

impl OnPythonLane {
    fn enter(debug: bool, introspection: bool) -> Self {
        ON_PYTHON_LANE.set(true);
        DEBUG.set(debug);
        if debug {
            DEBUG_RUNS.fetch_add(1, Ordering::Relaxed);
        }
        if introspection {
            INTROSPECTION_RUNS.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            debug,
            introspection,
        }
    }
}

impl Drop for OnPythonLane {
    fn drop(&mut self) {
        ON_PYTHON_LANE.set(false);
        DEBUG.set(false);
        if self.debug {
            DEBUG_RUNS.fetch_sub(1, Ordering::Relaxed);
        }
        if self.introspection {
            INTROSPECTION_RUNS.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// How many times a lane detached to park, in the process (test hook `_gil_stats`).
static PARK_DETACHES: AtomicU64 = AtomicU64::new(0);

/// How many times a lane released the GIL for the other Python threads (see
/// [`DetachWhileParked::on_maintenance`]), in the process (test hook `_gil_stats`).
static PERIODIC_RELEASES: AtomicU64 = AtomicU64::new(0);

/// `(park detaches, periodic releases)` so far in this process.
pub(crate) fn gil_stats() -> (u64, u64) {
    (
        PARK_DETACHES.load(Ordering::Relaxed),
        PERIODIC_RELEASES.load(Ordering::Relaxed),
    )
}

/// Maintenance passes between two looks at the clock: a pass comes every 61 ticks (a few
/// microseconds), the switch interval is milliseconds, so most passes only count.
#[cfg(not(Py_GIL_DISABLED))]
const PASSES_PER_CLOCK_READ: u32 = 32;

/// The lane's attachment policy on a GIL build (12 §6, 25 §2 P0, P2).
///
/// The lane thread is the thread that called `porto.run`, which already has the thread state
/// CPython made for it: the lane never calls `PyGILState_*` (P0), it detaches and reattaches that
/// one state explicitly (`PyEval_SaveThread`, `PyEval_RestoreThread`). It keeps the GIL across
/// consecutive Python tasks and detaches only around a park (P2: the park is where the lane
/// blocks in the driver, the only blocking call a lane makes), and, as
/// [`on_maintenance`](ParkHook::on_maintenance), for a moment every `gil_switch_interval` of
/// continuous Python execution while other Python threads exist.
///
/// On a free-threaded build there is no GIL to hand over: the periodic release is compiled out.
struct DetachWhileParked {
    /// When the lane last attached: the start of the current stretch of Python execution.
    #[cfg(not(Py_GIL_DISABLED))]
    attached_since: Cell<std::time::Instant>,
    /// The longest stretch before the other threads get the GIL (`sys.getswitchinterval()` when
    /// the run started).
    #[cfg(not(Py_GIL_DISABLED))]
    switch_interval: Duration,
    /// Maintenance passes since the clock was last read.
    #[cfg(not(Py_GIL_DISABLED))]
    passes: Cell<u32>,
}

impl DetachWhileParked {
    fn new(py: Python<'_>) -> Self {
        #[cfg(Py_GIL_DISABLED)]
        let _ = py;
        Self {
            #[cfg(not(Py_GIL_DISABLED))]
            attached_since: Cell::new(std::time::Instant::now()),
            #[cfg(not(Py_GIL_DISABLED))]
            switch_interval: switch_interval(py),
            #[cfg(not(Py_GIL_DISABLED))]
            passes: Cell::new(0),
        }
    }
}

/// `sys.getswitchinterval()`, the default `gil_switch_interval`; 5 ms if it cannot be read.
#[cfg(not(Py_GIL_DISABLED))]
fn switch_interval(py: Python<'_>) -> Duration {
    py.import("sys")
        .and_then(|sys| sys.call_method0("getswitchinterval"))
        .and_then(|seconds| seconds.extract::<f64>())
        .ok()
        .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
        .unwrap_or(Duration::from_millis(5))
}

/// `threading.active_count()`, or 1 if it cannot be read.
#[cfg(not(Py_GIL_DISABLED))]
fn other_threads_exist(py: Python<'_>) -> bool {
    static ACTIVE_COUNT: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();
    ACTIVE_COUNT
        .import(py, "threading", "active_count")
        .and_then(|count| count.call0())
        .and_then(|count| count.extract::<usize>())
        .is_ok_and(|count| count > 1)
}

impl ParkHook for DetachWhileParked {
    fn around_park(&self, park: &mut dyn FnMut()) {
        PARK_DETACHES.fetch_add(1, Ordering::Relaxed);
        {
            // SAFETY: the hook runs on the lane thread, inside `run`, which is attached. No task
            // runs while the lane is parked, so nothing touches Python until `Detached`
            // reattaches, also if `park` unwinds.
            let _detached = Detached(unsafe { ffi::PyEval_SaveThread() });
            park();
        }
        #[cfg(not(Py_GIL_DISABLED))]
        self.attached_since.set(std::time::Instant::now());
    }

    /// Periodic GIL release (12 §6): once a stretch of continuous Python execution outlasts the
    /// switch interval and another Python thread exists, the lane detaches and yields, so that
    /// thread gets the GIL even when the tasks in between never reach a bytecode that honors
    /// CPython's own drop request. With no other thread, the cost is a counter increment per
    /// pass and one clock read per [`PASSES_PER_CLOCK_READ`] passes.
    #[cfg(not(Py_GIL_DISABLED))]
    fn on_maintenance(&self) {
        let passes = self.passes.get().wrapping_add(1);
        self.passes.set(passes);
        if !passes.is_multiple_of(PASSES_PER_CLOCK_READ) {
            return;
        }
        let now = std::time::Instant::now();
        if now.duration_since(self.attached_since.get()) < self.switch_interval {
            return;
        }
        // SAFETY: maintenance runs on the lane thread, between task polls, attached.
        let py = unsafe { Python::assume_attached() };
        if other_threads_exist(py) {
            PERIODIC_RELEASES.fetch_add(1, Ordering::Relaxed);
            // SAFETY: attached (see above); the guard reattaches this thread's state, which
            // nothing else uses meanwhile.
            let _detached = Detached(unsafe { ffi::PyEval_SaveThread() });
            // Lets a thread waiting for the GIL run before this one asks for it back: the
            // waiter was signalled when the GIL was dropped.
            std::thread::yield_now();
        }
        // The next look comes after another interval, whether or not anything was released.
        self.attached_since.set(std::time::Instant::now());
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
