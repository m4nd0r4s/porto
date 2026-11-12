//! `porto.run(main, *args, config=None, **overrides)`, and the private `_run(coro)`,
//! `_spawn(coro)` and `_root_scope()`.
//!
//! `porto.run` runs `main` as the first task of the runtime's root scope, a nursery (spec 15
//! C12, spec 26 §2): a task spawned into the root scope (`_root_scope().spawn(...)`, the future
//! `Runtime.root_scope`) that fails cancels the root scope, and so `main` and every scope it
//! opened. Once `main` ended, the root scope is cancelled (`Shutdown`) and `porto.run` waits up
//! to `cancel_timeout` for the tasks still in it, then closes it (C7: no more spawns). It
//! returns main's result, or raises main's exception as is; when tasks of the root scope
//! failed, it raises a strict exception group of main's exception (first) and theirs.

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use porto_core::Handle;
use porto_core::config::RuntimeConfig;
use porto_core::scope::CancelReason;
use porto_core::time::{Deadline, sleep_until};
use pyo3::exceptions::{
    PyBaseException, PyException, PyRuntimeError, PyRuntimeWarning, PyTypeError,
};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyList, PyTuple, PyType};

use super::config;
use super::scope::{Scope, exception_group};
use super::task::Task;
use crate::driver::lane;
use crate::driver::pytask::{self, PyScopeNode, Spawned, TaskOutcome};
use crate::errors::NoRuntimeError;

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run, module)?)?;
    module.add_function(wrap_pyfunction!(private_run, module)?)?;
    module.add_function(wrap_pyfunction!(root_scope, module)?)?;
    module.add_function(wrap_pyfunction!(spawn, module)?)
}

thread_local! {
    /// The root scope of the `porto.run` running on this thread.
    static ROOT_SCOPE: RefCell<Option<Py<Scope>>> = const { RefCell::new(None) };
}

/// `porto.run(main, *args, config=None, **overrides)`: runs `main(*args)` (or the coroutine
/// object `main`) as the main task of a fresh runtime whose lane 0 is this thread, and returns
/// its result or raises its exception. Ctrl-C raises `KeyboardInterrupt` out of it, also while
/// every task sleeps. The configuration is resolved first, so an invalid one raises before
/// `main` is called. A coroutine object `main` that is rejected is closed.
#[pyfunction]
#[pyo3(signature = (main, /, *args, config=None, **overrides))]
fn run(
    main: Bound<'_, PyAny>,
    args: Bound<'_, PyTuple>,
    config: Option<Bound<'_, PyAny>>,
    overrides: Option<Bound<'_, PyDict>>,
) -> PyResult<Py<PyAny>> {
    let (config, coro) = closing_on_error(&main, || {
        if Handle::try_current().is_some() {
            return Err(PyRuntimeError::new_err(
                "porto: porto.run() cannot be called from inside a running porto runtime; \
                 `await` the coroutine instead, or run it as a task with `async with \
                 porto.scope() as s: s.spawn(...)`",
            ));
        }
        let config = config::resolve(main.py(), config.as_ref(), overrides.as_ref())?;
        Ok((config, coroutine_from(&main, &args, "porto.run")?))
    })?;
    run_in_root_scope(&config, &main, coro)
}

/// Runs `coro` as the main task of a fresh runtime, in its root scope (C12, see the module
/// docs). `main` names the task.
fn run_in_root_scope(
    config: &RuntimeConfig,
    main: &Bound<'_, PyAny>,
    coro: Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let py = coro.py();
    let cancel_timeout = config.cancel_timeout();
    let unstarted = coro.clone();
    let (outcome, errors) = lane::run(py, config, |runtime| {
        // Owned by no task: nobody enters or exits it.
        let node = PyScopeNode::new_root(0);
        let root = Py::new(py, Scope::root(py, node.clone()))?;
        let _published = RootScope::publish(root.clone_ref(py));
        let task = Py::new(py, Task::new(Some(main.clone().unbind()), false))?;
        let spawned = Spawned::Child {
            task: task.clone_ref(py),
            scope: node.clone(),
            pinned: false,
        };
        let main = pytask::spawn(runtime.handle(), coro, spawned)?;
        task.get().bind(&main);
        let abandoned = Cell::new(0);
        let outcome = runtime.block_on(lane::main_or_interrupt(async {
            let outcome = pytask::join(main).await;
            node.cancel(CancelReason::Shutdown);
            abandoned.set(drain(&node, Deadline::after(cancel_timeout)).await);
            outcome
        }));
        if abandoned.get() > 0 {
            warn_abandoned(py, abandoned.get(), cancel_timeout)?;
        }
        PyResult::Ok((outcome, root.get().close()))
    })
    .inspect_err(|_| {
        // The runtime did not start (its I/O driver did not open): close the coroutine, so
        // the caller sees that error and no "never awaited" warning.
        close_coroutine(&unstarted);
    })??;
    root_outcome(py, outcome, errors)
}

/// Waits until no task is left in `scope`, or `deadline` passed: the tasks still there are
/// then abandoned (dropped with the runtime), and their count is returned. MUST be polled by
/// the lane's `block_on`.
async fn drain(scope: &PyScopeNode, deadline: Deadline) -> u32 {
    let mut timeout = pin!(sleep_until(deadline));
    poll_fn(|cx| {
        let mut state = scope.lock_state();
        let previous = if state.live_children == 0 {
            state.exit_waiter.take()
        } else {
            state.exit_waiter.replace(cx.waker().clone())
        };
        let drained = state.live_children == 0;
        drop(state);
        // Dropped after the lock (see `ScopeNode::lock_state`).
        drop(previous);
        if drained || timeout.as_mut().poll(cx).is_ready() {
            return Poll::Ready(());
        }
        Poll::Pending
    })
    .await;
    let mut state = scope.lock_state();
    let waiter = state.exit_waiter.take();
    let abandoned = state.live_children;
    drop(state);
    // Dropped after the lock (see `ScopeNode::lock_state`).
    drop(waiter);
    abandoned
}

/// Warns that `count` tasks of the root scope outlived `cancel_timeout` after main ended: they
/// are dropped with the runtime, their coroutines closed with no runtime around (26 §2).
fn warn_abandoned(py: Python<'_>, count: u32, cancel_timeout: Duration) -> PyResult<()> {
    let message = format!(
        "porto.run: {count} task(s) of the root scope still ran {:.3} s (cancel_timeout) after \
         main ended and the root scope was cancelled, and were abandoned; let them exit when \
         they get porto.Cancelled (do not swallow it), or raise cancel_timeout",
        cancel_timeout.as_secs_f64()
    );
    let category = py.get_type::<PyRuntimeWarning>();
    PyErr::warn(py, &category, &std::ffi::CString::new(message)?, 1)
}

/// What `porto.run` returns or raises, given how main ended and the failures its root scope
/// recorded (main's own failure among them): main's outcome when only main failed or nothing
/// did, else a strict group of main's exception, first, and the others'.
fn root_outcome(
    py: Python<'_>,
    outcome: TaskOutcome,
    errors: Vec<Py<PyBaseException>>,
) -> PyResult<Py<PyAny>> {
    // An interrupt (Ctrl-C, or `SystemExit`) ends `porto.run` bare, also when it arrived while
    // the root scope drained after main failed: it is not one more failure to group.
    if let TaskOutcome::Err(exception) = &outcome
        && !exception.bind(py).is_instance_of::<PyException>()
    {
        return outcome.into_result(py);
    }
    let main_error = match &outcome {
        TaskOutcome::Err(exception) => Some(exception.as_ptr()),
        _ => None,
    };
    let others: Vec<_> = errors
        .into_iter()
        .filter(|error| Some(error.as_ptr()) != main_error)
        .collect();
    if others.is_empty() {
        return outcome.into_result(py);
    }
    let exceptions = PyList::empty(py);
    if let TaskOutcome::Err(exception) = outcome {
        exceptions.append(exception)?;
    }
    for error in others {
        exceptions.append(error)?;
    }
    Err(exception_group(py, "porto root scope failed", &exceptions)?)
}

/// Publishes the root scope of the running `porto.run` for `_root_scope()` until dropped.
struct RootScope;

impl RootScope {
    fn publish(scope: Py<Scope>) -> Self {
        let previous = ROOT_SCOPE.replace(Some(scope));
        drop(previous);
        Self
    }
}

impl Drop for RootScope {
    fn drop(&mut self) {
        let scope = ROOT_SCOPE.take();
        drop(scope);
    }
}

/// `_root_scope()`: the root scope of the `porto.run` running this task (C12), whose tasks
/// outlive the scopes `main` opens. The escape hatch `Runtime.root_scope` will return it.
#[pyfunction(name = "_root_scope")]
fn root_scope(py: Python<'_>) -> PyResult<Py<Scope>> {
    let scope = ROOT_SCOPE.with_borrow(|root| root.as_ref().map(|scope| scope.clone_ref(py)));
    scope.ok_or_else(|| {
        NoRuntimeError::new_err(
            "porto: _root_scope() must be called from a task running under porto.run(); \
             start the program with porto.run(main) and call it inside main",
        )
    })
}

/// Runs `coro` to completion as a task on a fresh single-lane runtime, on this thread, and
/// returns its result or raises its exception. Tasks it spawned and that are still running when
/// it returns are dropped (their coroutines are closed).
#[pyfunction(name = "_run")]
#[pyo3(signature = (coro, /))]
fn private_run(coro: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if Handle::try_current().is_some() {
        close_coroutine(&coro);
        return Err(PyRuntimeError::new_err(
            "porto: _run() cannot start a runtime inside a running porto task; await the \
             coroutine instead, or spawn it with _spawn()",
        ));
    }
    expect_coroutine(&coro, "_run")?;
    let py = coro.py();
    let unstarted = coro.clone();
    let outcome = lane::run(py, &RuntimeConfig::default(), |runtime| {
        let main = pytask::spawn(runtime.handle(), coro, Spawned::Joined)?;
        PyResult::Ok(runtime.block_on(lane::main_or_interrupt(pytask::join(main))))
    })
    .inspect_err(|_| {
        // The runtime did not start (its I/O driver did not open): close the coroutine, so
        // the caller sees that error and no "never awaited" warning.
        close_coroutine(&unstarted);
    })??;
    outcome.into_result(py)
}

/// Starts `coro` as a new task on the runtime of the task calling it, at the back of the run
/// queue. Nothing waits for it: if it raises an `Exception`, the exception goes to
/// `sys.unraisablehook`; any other `BaseException` stops the run.
#[pyfunction(name = "_spawn")]
#[pyo3(signature = (coro, /))]
fn spawn(coro: Bound<'_, PyAny>) -> PyResult<()> {
    let Some(handle) = lane::current() else {
        close_coroutine(&coro);
        return Err(NoRuntimeError::new_err(
            "porto: _spawn() must be called from a task running under _run(); start the \
             runtime with _run(main()) and spawn from inside main",
        ));
    };
    expect_coroutine(&coro, "_spawn")?;
    pytask::spawn(&handle, coro, Spawned::Detached)?;
    Ok(())
}

/// Runs `body`; if it fails, closes `rejected` (when it is a coroutine object) before the
/// error propagates, so the caller sees that error and no later "coroutine ... was never
/// awaited" warning.
pub(crate) fn closing_on_error<T>(
    rejected: &Bound<'_, PyAny>,
    body: impl FnOnce() -> PyResult<T>,
) -> PyResult<T> {
    body().inspect_err(|_| close_coroutine(rejected))
}

/// Closes `object` if it is a coroutine object that porto will not run. Closing a coroutine
/// that never started runs none of its code; an error from a non-native one's `close()` goes
/// to `sys.unraisablehook`, so it never hides the error being raised.
pub(crate) fn close_coroutine(object: &Bound<'_, PyAny>) {
    let py = object.py();
    // An exception may be set already (the rejection being raised): keep it for the caller.
    let pending = PyErr::take(py);
    let closed = match is_coroutine(object) {
        Ok(true) => object.call_method0(pyo3::intern!(py, "close")).map(drop),
        Ok(false) => Ok(()),
        Err(error) => Err(error),
    };
    if let Err(error) = closed {
        error.write_unraisable(py, Some(object));
    }
    if let Some(pending) = pending {
        pending.restore(py);
    }
}

/// `collections.abc.Coroutine`, for coroutine objects that are not native coroutines (Cython).
static COROUTINE_ABC: PyOnceLock<Py<PyType>> = PyOnceLock::new();
/// `porto._bridge.await_awaitable`: wraps an awaitable that is not a coroutine.
static AWAIT_AWAITABLE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn is_coroutine(object: &Bound<'_, PyAny>) -> PyResult<bool> {
    // SAFETY: attached (we hold a `Bound`); the check only reads the object's type.
    if unsafe { ffi::PyCoro_CheckExact(object.as_ptr()) } != 0 {
        return Ok(true);
    }
    // SAFETY: as above. A plain function (`s.spawn(work, ...)`) is never a coroutine: skip the
    // ABC's `isinstance`, which costs more than the rest of a spawn.
    if unsafe { ffi::PyFunction_Check(object.as_ptr()) } != 0 {
        return Ok(false);
    }
    let coroutine_abc = COROUTINE_ABC.import(object.py(), "collections.abc", "Coroutine")?;
    object.is_instance(coroutine_abc)
}

fn is_awaitable(object: &Bound<'_, PyAny>) -> PyResult<bool> {
    object.hasattr(pyo3::intern!(object.py(), "__await__"))
}

/// A coroutine running `awaitable`, which is awaitable but not a coroutine.
fn wrap_awaitable<'py>(awaitable: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let py = awaitable.py();
    AWAIT_AWAITABLE
        .import(py, "porto._bridge", "await_awaitable")?
        .call1((awaitable,))
}

/// The coroutine for `main` and `args` (ADR-010): `main` itself if it is a coroutine object (and
/// `args` is empty), else `main(*args)`, which must return an awaitable. Other awaitables are
/// wrapped in a coroutine. `function` names the API in error messages.
pub(crate) fn coroutine_from<'py>(
    main: &Bound<'py, PyAny>,
    args: &Bound<'py, PyTuple>,
    function: &str,
) -> PyResult<Bound<'py, PyAny>> {
    if is_coroutine(main)? {
        if !args.is_empty() {
            return Err(PyTypeError::new_err(format!(
                "porto: {function}() got a coroutine object and arguments; pass either the \
                 coroutine object, {function}(work(1)), or the async function and its \
                 arguments, {function}(work, 1)"
            )));
        }
        return Ok(main.clone());
    }
    if main.is_callable() {
        let result = main.call1(args)?;
        if is_coroutine(&result)? {
            return Ok(result);
        }
        if is_awaitable(&result)? {
            return wrap_awaitable(&result);
        }
        let name = match main.getattr(pyo3::intern!(main.py(), "__qualname__")) {
            Ok(name) => name.str()?.to_string(),
            Err(_) => main.repr()?.to_string(),
        };
        let type_name = result.get_type().fully_qualified_name()?;
        return Err(PyTypeError::new_err(format!(
            "porto: {function}() expects an async function, but {name}() returned \
             {type_name}, which is not awaitable; define it with `async def`"
        )));
    }
    if is_awaitable(main)? && args.is_empty() {
        return wrap_awaitable(main);
    }
    let type_name = main.get_type().fully_qualified_name()?;
    Err(PyTypeError::new_err(format!(
        "porto: {function}() expects a coroutine object or an async function, got {type_name}"
    )))
}

/// Checks that `object` is a coroutine object, with a hint for the usual mistake: passing the
/// async function instead of calling it.
fn expect_coroutine(object: &Bound<'_, PyAny>, function: &str) -> PyResult<()> {
    if is_coroutine(object)? {
        return Ok(());
    }
    let type_name = object.get_type().fully_qualified_name()?;
    let hint = if object.is_callable() {
        format!("; call the async function: {function}(main()), not {function}(main)")
    } else {
        String::new()
    };
    Err(PyTypeError::new_err(format!(
        "porto: {function}() expects a coroutine object, got {type_name}{hint}"
    )))
}
