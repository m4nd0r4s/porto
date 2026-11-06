//! `porto.run(main, *args, config=None, **overrides)`, and the private `_run(coro)` and
//! `_spawn(coro)`.

use porto_core::Handle;
use porto_core::config::RuntimeConfig;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};

use super::config;
use crate::driver::lane;
use crate::driver::pytask::{self, Spawned};
use crate::errors::NoRuntimeError;

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run, module)?)?;
    module.add_function(wrap_pyfunction!(private_run, module)?)?;
    module.add_function(wrap_pyfunction!(spawn, module)?)
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
    run_main_task(&config, coro)
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
    run_main_task(&RuntimeConfig::default(), coro)
}

fn run_main_task(config: &RuntimeConfig, coro: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let py = coro.py();
    let outcome = lane::run(py, config, |runtime| {
        let main = pytask::spawn(runtime.handle(), coro, Spawned::Joined)?;
        PyResult::Ok(runtime.block_on(lane::main_or_interrupt(pytask::join(main))))
    })?;
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
