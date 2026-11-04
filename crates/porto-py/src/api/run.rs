//! `_run(coro)` and `_spawn(coro)`: run coroutines as porto tasks.

use std::sync::Arc;

use porto_core::Handle;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

use crate::driver::lane;
use crate::driver::pytask::{self, JoinSlot};

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run, module)?)?;
    module.add_function(wrap_pyfunction!(spawn, module)?)
}

/// Runs `coro` to completion as a task on a fresh single-lane runtime, on this thread, and
/// returns its result or raises its exception. Tasks it spawned and that are still running when
/// it returns are dropped (their coroutines are closed).
#[pyfunction(name = "_run")]
#[pyo3(signature = (coro, /))]
fn run(coro: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let py = coro.py();
    if Handle::try_current().is_some() {
        return Err(PyRuntimeError::new_err(
            "porto: _run() cannot start a runtime inside a running porto task; await the \
             coroutine instead, or spawn it with _spawn()",
        ));
    }
    expect_coroutine(&coro, "_run")?;
    let join = Arc::new(JoinSlot::default());
    let outcome = lane::run(py, |runtime| {
        pytask::spawn(runtime.handle(), coro, Some(join.clone()))?;
        PyResult::Ok(runtime.block_on(join.wait()))
    })?;
    outcome.into_result(py)
}

/// Starts `coro` as a new task on the runtime of the task calling it, at the back of the run
/// queue. Nothing waits for it: if it raises, the exception goes to `sys.unraisablehook`.
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
    pytask::spawn(&handle, coro, None)
}

/// `collections.abc.Coroutine`, for coroutine objects that are not native coroutines (Cython).
static COROUTINE_ABC: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// Checks that `object` is a coroutine object, with a hint for the usual mistake: passing the
/// async function instead of calling it.
fn expect_coroutine(object: &Bound<'_, PyAny>, function: &str) -> PyResult<()> {
    // SAFETY: attached (we hold a `Bound`); the check only reads the object's type.
    if unsafe { ffi::PyCoro_CheckExact(object.as_ptr()) } != 0 {
        return Ok(());
    }
    let coroutine_abc = COROUTINE_ABC.import(object.py(), "collections.abc", "Coroutine")?;
    if object.is_instance(coroutine_abc)? {
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
