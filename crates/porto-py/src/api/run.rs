//! `porto.run(main, *args)`, and the private `_run(coro)` and `_spawn(coro)`.

use std::sync::Arc;

use porto_core::Handle;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyTuple, PyType};

use crate::driver::lane;
use crate::driver::pytask::{self, Completion, JoinSlot};

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run, module)?)?;
    module.add_function(wrap_pyfunction!(private_run, module)?)?;
    module.add_function(wrap_pyfunction!(spawn, module)?)
}

/// `porto.run(main, *args)`: runs `main(*args)` (or the coroutine object `main`) as the main task
/// of a fresh runtime whose lane 0 is this thread, and returns its result or raises its
/// exception. Ctrl-C raises `KeyboardInterrupt` out of it, also while every task sleeps.
#[pyfunction]
#[pyo3(signature = (main, /, *args))]
fn run(main: Bound<'_, PyAny>, args: Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
    if Handle::try_current().is_some() {
        return Err(PyRuntimeError::new_err(
            "porto: porto.run() cannot be called from inside a running porto runtime; `await` \
             the coroutine instead, or run it as a task with `async with porto.scope() as s: \
             s.spawn(...)`",
        ));
    }
    let coro = coroutine_from(&main, &args, "porto.run")?;
    run_main_task(coro)
}

/// Runs `coro` to completion as a task on a fresh single-lane runtime, on this thread, and
/// returns its result or raises its exception. Tasks it spawned and that are still running when
/// it returns are dropped (their coroutines are closed).
#[pyfunction(name = "_run")]
#[pyo3(signature = (coro, /))]
fn private_run(coro: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if Handle::try_current().is_some() {
        return Err(PyRuntimeError::new_err(
            "porto: _run() cannot start a runtime inside a running porto task; await the \
             coroutine instead, or spawn it with _spawn()",
        ));
    }
    expect_coroutine(&coro, "_run")?;
    run_main_task(coro)
}

fn run_main_task(coro: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let py = coro.py();
    let join = Arc::new(JoinSlot::default());
    let outcome = lane::run(py, |runtime| {
        pytask::spawn(runtime.handle(), coro, Completion::Join(join.clone()))?;
        PyResult::Ok(runtime.block_on(lane::main_or_interrupt(join.wait())))
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
        return Err(PyRuntimeError::new_err(
            "porto: _spawn() must be called from a task running under _run(); start the \
             runtime with _run(main()) and spawn from inside main",
        ));
    };
    expect_coroutine(&coro, "_spawn")?;
    pytask::spawn(&handle, coro, Completion::Detached)
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
