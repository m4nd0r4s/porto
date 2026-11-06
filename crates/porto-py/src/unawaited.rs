//! The `RuntimeWarning` for a porto awaitable that is freed without ever being awaited, the
//! counterpart of CPython's "coroutine ... was never awaited".
//!
//! `porto.sleep()`, `porto.checkpoint()`, `Event.wait()`, `Lock.acquire()`, `tx.send(...)`
//! and the other awaitables that are not coroutines do nothing until they are awaited, so a
//! forgotten `await` would otherwise be silent. Each awaitable already records whether it
//! was resumed; its `Drop` reads that and calls [`warn_never_awaited`], so the awaited path
//! pays nothing for the warning. An awaitable that was thrown into or closed is finished,
//! not forgotten, and does not warn.

use pyo3::exceptions::PyRuntimeWarning;
use pyo3::prelude::*;
use pyo3::types::PyString;

/// Warns that the awaitable `call` returned was freed without being awaited. `call` is how
/// the user wrote it, `porto.sleep(...)`.
///
/// Called from `Drop`, so it must not fail: an exception that is already set is kept, and a
/// warning turned into an error (`-W error`) goes to `sys.unraisablehook`, as CPython does
/// for coroutines. The warning points at the Python code running when the awaitable is
/// freed: for `porto.sleep(1)` as a statement, that is the line of the call.
#[cold]
#[inline(never)]
pub(crate) fn warn_never_awaited(call: &str) {
    Python::attach(|py| {
        let pending = PyErr::take(py);
        let message = format!(
            "porto: {call} was never awaited, so it did nothing; add `await` in front of the call"
        );
        if let Err(error) = warn(py, &message) {
            error.write_unraisable(py, Some(PyString::new(py, call).as_any()));
        }
        if let Some(pending) = pending {
            pending.restore(py);
        }
    });
}

fn warn(py: Python<'_>, message: &str) -> PyResult<()> {
    let message = std::ffi::CString::new(message)?;
    PyErr::warn(py, &py.get_type::<PyRuntimeWarning>(), &message, 1)
}
