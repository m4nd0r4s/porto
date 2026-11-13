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
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use porto_core::Handle;
use porto_core::config::RuntimeConfig;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::config;
use super::run::{closing_on_error, coroutine_from, run_claimed};
use super::scope::Scope;
use crate::driver::lane;
use crate::errors::{NoRuntimeError, RuntimeClosed};

const NEW: u8 = 0;
const RUNNING: u8 = 1;
const STOPPED: u8 = 2;

/// Whether a public run (`porto.run`, `Runtime.block_on`) is in progress in the process (I1).
static PROCESS_RUN: AtomicBool = AtomicBool::new(false);

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
}

impl Runtime {
    pub(crate) fn with_config(config: RuntimeConfig) -> Self {
        Self {
            config,
            state: AtomicU8::new(NEW),
            root: Mutex::new(None),
        }
    }

    pub(crate) fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// Takes the right to run: the runtime is `NEW`, and no other runtime runs in the process.
    /// The claim makes the runtime `RUNNING` and gives it back as `STOPPED` when dropped.
    pub(crate) fn claim(slf: &Bound<'_, Self>) -> PyResult<RunClaim> {
        refuse_nested_start()?;
        let this = slf.get();
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
    pub(crate) fn publish(slf: &Bound<'_, Self>, root: Py<Scope>) -> Published {
        *slf.get().lock_root() = Some(root);
        let previous = CURRENT.replace(Some(slf.clone().unbind()));
        drop(previous);
        Published {
            runtime: slf.clone().unbind(),
        }
    }
}

/// The right to run, held while the runtime runs.
pub(crate) struct RunClaim {
    runtime: Py<Runtime>,
}

impl Drop for RunClaim {
    fn drop(&mut self) {
        PROCESS_RUN.store(false, Ordering::Release);
        self.runtime.get().state.store(STOPPED, Ordering::Release);
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
        // Dropped after the lock: dropping a scope may run Python code.
        drop(current);
        drop(root);
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
    if Handle::try_current().is_some() {
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
    /// cannot start afterwards. A runtime that runs cannot be shut down from here yet: return
    /// from main, or cancel the root scope.
    #[pyo3(signature = (*, timeout=10.0))]
    fn shutdown(&self, timeout: f64) -> PyResult<()> {
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
            Err(_) => Err(PyRuntimeError::new_err(
                "porto: Runtime.shutdown() of a runtime that is running is not supported yet; \
                 return from main to stop it, or cancel its root scope \
                 (`runtime.root_scope.cancel()`) and let main finish",
            )),
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_exception))]
    fn __exit__(&self, _exception: &Bound<'_, PyTuple>) -> PyResult<()> {
        self.shutdown(10.0)
    }

    fn __repr__(&self) -> String {
        let state = match self.state.load(Ordering::Acquire) {
            NEW => "new",
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
    let current =
        CURRENT.with_borrow(|runtime| runtime.as_ref().map(|runtime| runtime.clone_ref(py)));
    current.ok_or_else(|| {
        NoRuntimeError::new_err(
            "porto: porto.runtime() must be called from code that porto.run(main) or \
             Runtime.block_on(main) runs; from another thread, use `porto.from_thread`",
        )
    })
}

/// `porto.in_runtime()`: whether this code runs on the lane of a running runtime.
#[pyfunction]
fn in_runtime() -> bool {
    lane::is_active()
}

/// Adds the class and the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Runtime>()?;
    module.add_function(wrap_pyfunction!(runtime, module)?)?;
    module.add_function(wrap_pyfunction!(in_runtime, module)?)
}
