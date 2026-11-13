//! `porto.Runtime`, `porto.runtime()` and `porto.in_runtime()` (20 Running, 26 §2), and the
//! process-level invariants I1 and I4 (10 §5).
//!
//! A [`Runtime`] is single-use, as the lifecycle is one way: `NEW` until [`Runtime::block_on`]
//! starts it (the calling thread becomes lane 0), `RUNNING` while main runs and the root scope
//! drains, `STOPPED` after, for good. `porto.run` makes a `Runtime` for the run and starts it, so
//! `porto.runtime()` is the same object either way.
//!
//! - **I1**: one runtime runs per process at a time. A start while one runs raises
//!   `RuntimeError` that says what to do, whether the caller is a task of the running runtime, a
//!   blocking-pool worker (`to_thread`) or another thread. (`_run`, the private test entry
//!   point, only refuses a start inside a running task.)
//! - **I4**: what holds runtime state answers `porto.RuntimeClosed`, not a crash or a hang, once
//!   its runtime stopped: this module's `Runtime`, and the objects checked in `tests/python/
//!   test_runtime.py` (task handles, scopes, channels, streams).

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use porto_core::Handle;
use porto_core::config::RuntimeConfig;
use pyo3::exceptions::{PyRuntimeError, PyRuntimeWarning};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::config;
use super::lifecycle::{Cause, Lifecycle, Request};
use super::run::{closing_on_error, coroutine_from, run_claimed};
use super::scope::Scope;
use crate::driver::lane;
use crate::errors::{NoRuntimeError, RuntimeClosed};

const NEW: u8 = 0;
const RUNNING: u8 = 1;
const STOPPED: u8 = 2;

/// Whether a public run (`porto.run`, `Runtime.block_on`) is in progress in the process (I1).
static PROCESS_RUN: AtomicBool = AtomicBool::new(false);

/// How many `fork()`s this process is the child of: a runtime that started in an earlier epoch
/// than the current one was inherited by a forked child, where it does not work (25 §6).
static FORK_EPOCH: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// The runtime whose lane this thread is, from the start of its run until it ends.
    static CURRENT: RefCell<Option<Py<Runtime>>> = const { RefCell::new(None) };
    /// Whether this thread is a worker of a blocking pool (`to_thread`).
    static ON_BLOCKING_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// Marks the calling thread as a blocking-pool worker: it can never start a runtime (I1).
pub(crate) fn mark_blocking_worker() {
    ON_BLOCKING_WORKER.set(true);
}

/// An asynchronous runtime: `Runtime().block_on(main)` runs `main` on the calling thread, which
/// becomes lane 0, like `porto.run(main)`, which makes one itself. A `Runtime` runs once.
#[pyclass(module = "porto", frozen)]
pub(crate) struct Runtime {
    config: RuntimeConfig,
    state: AtomicU8,
    /// The root scope while the runtime runs.
    root: Mutex<Option<Py<Scope>>>,
    /// What asks the runtime to shut down (26 §2 steps 3-4).
    lifecycle: Arc<Lifecycle>,
    /// `porto.lifecycle.shutdown_requested()` while the runtime runs.
    shutdown_event: Mutex<Option<Py<PyAny>>>,
    /// Whether the run ended (`STOPPED`), for `shutdown()` called from another thread.
    stopped: (Mutex<bool>, Condvar),
    /// The [`FORK_EPOCH`] the run started in.
    epoch: AtomicU64,
}

impl Runtime {
    pub(crate) fn with_config(config: RuntimeConfig) -> Self {
        Self {
            config,
            state: AtomicU8::new(NEW),
            root: Mutex::new(None),
            lifecycle: Arc::new(Lifecycle::default()),
            shutdown_event: Mutex::new(None),
            stopped: (Mutex::new(false), Condvar::new()),
            epoch: AtomicU64::new(0),
        }
    }

    /// Whether this runtime was running when the process was forked: in the child it is a
    /// copy of memory whose threads, descriptors' readiness and locks do not exist (25 §6).
    fn is_poisoned(&self) -> bool {
        self.state.load(Ordering::Acquire) == RUNNING
            && self.epoch.load(Ordering::Acquire) != FORK_EPOCH.load(Ordering::Acquire)
    }

    /// `RuntimeClosed` if [`is_poisoned`](Self::is_poisoned).
    pub(crate) fn check_not_poisoned(&self) -> PyResult<()> {
        if self.is_poisoned() {
            return Err(RuntimeClosed::new_err(
                "porto: runtime does not survive fork(): this runtime was running when the \
                 process forked, and a forked child gets only the forking thread, so the \
                 runtime's lanes, pool and driver are gone. Start a fresh runtime in the child \
                 with porto.run(...), or fork before any runtime starts (the supervisor of a \
                 server does), or use subprocess or multiprocessing's 'spawn' start method",
            ));
        }
        Ok(())
    }

    pub(crate) fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub(crate) fn lifecycle(&self) -> &Arc<Lifecycle> {
        &self.lifecycle
    }

    /// The `shutdown_requested` event while the runtime runs.
    pub(crate) fn shutdown_event(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.shutdown_event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|event| event.clone_ref(py))
    }

    /// Blocks the calling thread, detached, until the run ended.
    fn wait_stopped(&self, py: Python<'_>) -> PyResult<()> {
        let (lock, condvar) = &self.stopped;
        loop {
            let stopped = py.detach(|| {
                let guard = lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let (guard, _) = condvar
                    .wait_timeout_while(guard, Duration::from_millis(100), |stopped| !*stopped)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *guard
            });
            if stopped {
                return Ok(());
            }
            py.check_signals()?;
        }
    }

    /// Takes the right to run: the runtime is `NEW`, and no other runtime runs in the process.
    /// The claim makes the runtime `RUNNING` and gives it back as `STOPPED` when dropped.
    pub(crate) fn claim(slf: &Bound<'_, Self>) -> PyResult<RunClaim> {
        refuse_nested_start()?;
        let this = slf.get();
        this.check_not_poisoned()?;
        match this
            .state
            .compare_exchange(NEW, RUNNING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(RUNNING) => return Err(another_runtime_runs()),
            Err(_) => return Err(already_ran()),
        }
        if PROCESS_RUN
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            this.state.store(NEW, Ordering::Release);
            return Err(another_runtime_runs());
        }
        this.epoch
            .store(FORK_EPOCH.load(Ordering::Acquire), Ordering::Release);
        Ok(RunClaim {
            runtime: slf.clone().unbind(),
        })
    }

    fn lock_root(&self) -> std::sync::MutexGuard<'_, Option<Py<Scope>>> {
        self.root
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Publishes the root scope and the runtime for `runtime()` and `root_scope`, on the lane
    /// thread, until the guard drops.
    ///
    /// # Errors
    /// When the `shutdown_requested` event cannot be made.
    pub(crate) fn publish(slf: &Bound<'_, Self>, root: Py<Scope>) -> PyResult<Published> {
        let event = slf
            .py()
            .import("porto._core")?
            .getattr("Event")?
            .call0()?
            .unbind();
        *slf.get()
            .shutdown_event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(event);
        *slf.get().lock_root() = Some(root);
        let previous = CURRENT.replace(Some(slf.clone().unbind()));
        drop(previous);
        Ok(Published {
            runtime: slf.clone().unbind(),
        })
    }
}

/// The right to run, held while the runtime runs.
pub(crate) struct RunClaim {
    runtime: Py<Runtime>,
}

impl Drop for RunClaim {
    fn drop(&mut self) {
        PROCESS_RUN.store(false, Ordering::Release);
        let runtime = self.runtime.get();
        runtime.state.store(STOPPED, Ordering::Release);
        let (lock, condvar) = &runtime.stopped;
        *lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        condvar.notify_all();
    }
}

/// The runtime and its root scope as seen by `runtime()` and `root_scope`.
pub(crate) struct Published {
    runtime: Py<Runtime>,
}

impl Drop for Published {
    fn drop(&mut self) {
        let current = CURRENT.take();
        let root = self.runtime.get().lock_root().take();
        let event = self
            .runtime
            .get()
            .shutdown_event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        // Dropped after the lock: dropping a scope may run Python code.
        drop(current);
        drop(root);
        drop(event);
    }
}

/// I1 for a thread that is not the lane of a running runtime: refuses a start from a blocking
/// worker, from a task of a running runtime, or while another thread's runtime runs.
pub(crate) fn refuse_nested_start() -> PyResult<()> {
    if ON_BLOCKING_WORKER.get() {
        return Err(PyRuntimeError::new_err(
            "porto: a runtime cannot be started on a blocking-pool thread (the worker of \
             `porto.to_thread`): one runtime runs per process (I1). Return what the thread \
             computed and carry on in the task that awaited `to_thread`; to call back into the \
             running runtime from a thread, use `porto.from_thread`",
        ));
    }
    // A forked child's thread that was a lane still has the parent's runtime context on its
    // stack, which is dead: a fresh runtime may start on it.
    if Handle::try_current().is_some() && !lane::is_forked_lane() {
        return Err(PyRuntimeError::new_err(
            "porto: porto.run() cannot be called from inside a running porto runtime; `await` \
             the coroutine instead, or run it as a task with `async with porto.scope() as s: \
             s.spawn(...)` (one runtime runs per process, I1)",
        ));
    }
    if PROCESS_RUN.load(Ordering::Acquire) {
        return Err(another_runtime_runs());
    }
    Ok(())
}

fn another_runtime_runs() -> PyErr {
    PyRuntimeError::new_err(
        "porto: another porto runtime is already running in this process; only one runtime \
         runs at a time (I1). Wait for it to finish, or run both programs as tasks of the one \
         runtime",
    )
}

fn already_ran() -> PyErr {
    RuntimeClosed::new_err(
        "porto: this Runtime already ran, or was shut down, and cannot start again; create a \
         new porto.Runtime() for each program (a Runtime runs once), or use porto.run(main)",
    )
}

#[pymethods]
impl Runtime {
    /// `Runtime(config=None, **overrides)`: resolves the configuration now, so an invalid one
    /// raises here, before anything runs.
    #[new]
    #[pyo3(signature = (config=None, **overrides))]
    fn new(
        py: Python<'_>,
        config: Option<Bound<'_, PyAny>>,
        overrides: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let config = config::resolve(py, config.as_ref(), overrides.as_ref())?;
        Ok(Self::with_config(config))
    }

    /// Runs `main` (an async function, or a coroutine object) to completion on the calling
    /// thread, which becomes lane 0, and returns its result or raises its exception. The
    /// runtime stops when it returns.
    #[pyo3(signature = (main, /))]
    fn block_on(slf: &Bound<'_, Self>, main: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let (coro, claim) = closing_on_error(&main, || {
            slf.get().check_not_poisoned()?;
            refuse_nested_start()?;
            if slf.get().state.load(Ordering::Acquire) != NEW {
                return Err(already_ran());
            }
            let coro = coroutine_from(&main, &PyTuple::empty(py), "Runtime.block_on")?;
            Ok((coro, Self::claim(slf)?))
        })?;
        run_claimed(slf, claim, &main, coro)
    }

    /// The root scope, a nursery that outlives the scopes `main` opens (ADR-007). Valid only
    /// while the runtime runs.
    #[getter]
    fn root_scope(&self, py: Python<'_>) -> PyResult<Py<Scope>> {
        self.check_not_poisoned()?;
        let root = self.lock_root().as_ref().map(|root| root.clone_ref(py));
        match (root, self.state.load(Ordering::Acquire)) {
            (Some(root), _) => Ok(root),
            (None, STOPPED) => Err(RuntimeClosed::new_err(
                "porto: Runtime.root_scope is only valid while the runtime runs, and this one \
                 stopped; read it inside main",
            )),
            (None, _) => Err(NoRuntimeError::new_err(
                "porto: Runtime.root_scope is only valid while the runtime runs; read it from \
                 a task of Runtime.block_on(main)",
            )),
        }
    }

    /// Stops the runtime. Nothing to do for one that never started or already stopped; it
    /// cannot start afterwards.
    ///
    /// A runtime that runs is shut down in order (26 §2): `porto.lifecycle.shutdown_requested()`
    /// is set, main gets `timeout` seconds to finish by itself, then the root scope is cancelled
    /// and given `cancel_timeout`, and what is left is abandoned. Called from another thread, it
    /// returns once the runtime stopped; called from a task of the runtime itself it returns at
    /// once, and the shutdown goes on while main continues.
    #[pyo3(signature = (*, timeout=10.0))]
    fn shutdown(&self, py: Python<'_>, timeout: f64) -> PyResult<()> {
        if !timeout.is_finite() || timeout < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "porto: Runtime.shutdown(timeout={timeout}) must be a finite number of seconds, \
                 0 or more; pass e.g. timeout=10.0"
            )));
        }
        match self
            .state
            .compare_exchange(NEW, STOPPED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(STOPPED) => Ok(()),
            Err(_) => {
                self.check_not_poisoned()?;
                self.lifecycle.request(Request {
                    cause: Cause::Shutdown,
                    graceful: Some(Duration::try_from_secs_f64(timeout).unwrap_or(Duration::MAX)),
                });
                if lane::is_active() {
                    return Ok(());
                }
                self.wait_stopped(py)
            }
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_exception))]
    fn __exit__(&self, py: Python<'_>, _exception: &Bound<'_, PyTuple>) -> PyResult<()> {
        self.shutdown(py, 10.0)
    }

    fn __repr__(&self) -> String {
        let state = match self.state.load(Ordering::Acquire) {
            NEW => "new",
            RUNNING if self.is_poisoned() => "poisoned by fork()",
            RUNNING => "running",
            _ => "stopped",
        };
        format!("<porto.Runtime {state}>")
    }
}

/// `porto.runtime()`: the runtime this code runs in.
///
/// # Errors
/// `NoRuntimeError` outside a task of a running runtime.
#[pyfunction]
fn runtime(py: Python<'_>) -> PyResult<Py<Runtime>> {
    current(py).ok_or_else(|| {
        NoRuntimeError::new_err(
            "porto: porto.runtime() must be called from code that porto.run(main) or \
             Runtime.block_on(main) runs; from another thread, use `porto.from_thread`",
        )
    })
}

/// The runtime whose lane this thread is, if any.
pub(crate) fn current(py: Python<'_>) -> Option<Py<Runtime>> {
    CURRENT.with_borrow(|runtime| runtime.as_ref().map(|runtime| runtime.clone_ref(py)))
}

/// `porto.in_runtime()`: whether this code runs on the lane of a running runtime.
#[pyfunction]
fn in_runtime() -> bool {
    lane::is_active()
}

/// `os.register_at_fork(before=...)`: forking while a runtime runs leaves the child a copy of
/// a runtime whose threads are gone (25 §6, ST-14).
#[pyfunction(name = "_before_fork")]
fn before_fork(py: Python<'_>) -> PyResult<()> {
    if !PROCESS_RUN.load(Ordering::Acquire) {
        return Ok(());
    }
    let category = py.get_type::<PyRuntimeWarning>();
    PyErr::warn(
        py,
        &category,
        c"fork() while porto runtime is running is unsafe: the child gets only the forking \
          thread, so the runtime does not survive in it (using it raises porto.RuntimeClosed); \
          fork before any runtime starts, start a fresh runtime in the child with porto.run(...), \
          or use subprocess or multiprocessing's 'spawn' start method",
        2,
    )
}

/// `os.register_at_fork(after_in_child=...)`: the child of a `fork()` taken while a runtime
/// runs inherits the process-wide I1 flag with no thread left to clear it, so every `porto.run`
/// there would be refused for good: clears it ("fresh runtimes may be created in the child",
/// 25 §6). The runtime that was running is *poisoned* (every use raises `RuntimeClosed`), the
/// signals it received are not delivered to the parent's pipe any more, and the forking thread,
/// if it was the lane, is no lane.
#[pyfunction(name = "_after_fork_in_child")]
fn after_fork_in_child() {
    PROCESS_RUN.store(false, Ordering::Release);
    FORK_EPOCH.fetch_add(1, Ordering::AcqRel);
    lane::forget_inherited_lane();
    super::signals::forget_in_child();
    #[cfg(unix)]
    porto_io::signal::forget_in_child();
}

/// The fork hooks are registered once per process: `os.register_at_fork` has no unregister.
static FORK_HOOKS: std::sync::Once = std::sync::Once::new();

/// Registers the fork hooks (26 §2 step 1, the `fork_hooks` start step). Later runs find them
/// installed.
pub(crate) fn install_fork_hooks(py: Python<'_>) -> PyResult<()> {
    let os = py.import("os")?;
    // Windows has no fork.
    if !os.hasattr("register_at_fork")? {
        return Ok(());
    }
    let mut result = Ok(());
    FORK_HOOKS.call_once(|| {
        result = (|| {
            let module = py.import("porto._core")?;
            let keywords = PyDict::new(py);
            keywords.set_item("before", module.getattr("_before_fork")?)?;
            keywords.set_item("after_in_child", module.getattr("_after_fork_in_child")?)?;
            os.call_method("register_at_fork", (), Some(&keywords))?;
            Ok(())
        })();
    });
    result
}

/// Adds the class and the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Runtime>()?;
    module.add_function(wrap_pyfunction!(runtime, module)?)?;
    module.add_function(wrap_pyfunction!(in_runtime, module)?)?;
    module.add_function(wrap_pyfunction!(before_fork, module)?)?;
    module.add_function(wrap_pyfunction!(after_fork_in_child, module)?)
}
