//! The `am_send` slot for pyclasses, which pyo3 0.30 does not expose.
//!
//! `PyIter_Send` (and the 3.11 `SEND` opcode) calls `tp_as_async->am_send` when a type has one:
//! the callee returns `PYGEN_RETURN` with the value in `*presult`, and no `StopIteration` is
//! created. pyo3 builds every pyclass with `PyType_FromSpec`, so the type object is a
//! `PyHeapTypeObject` and `tp_as_async` points at its embedded `as_async` table, which is filled
//! from the spec's slots (a class that defines `__await__` gets `am_await` there). We set
//! `am_send` in that table once, at module init, right after the type is created.
//!
//! Why this is sound:
//! - the table belongs to the type object (it is not a shared static), and stays alive as long
//!   as the type does; the module holds the type for the life of the interpreter;
//! - it runs before the module object is returned to the importer, so no instance exists and no
//!   other thread can reach the type yet (pyo3 refuses to initialise the module twice);
//! - the classes are final (no `subclass`), so no subclass copied the slot before it was set;
//! - CPython does not cache `am_send`: every `PyIter_Send` reads the slot.
//!
//! The alternative, building the whole type with `PyType_FromSpec` and a `Py_am_send` slot by
//! hand, would also mean redoing pyo3's method table, freelist and layout code for both types.

use core::ffi::CStr;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pyo3::exceptions::{PyImportError, PyStopIteration};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::type_object::PyTypeInfo;

/// Sets `am_send` on the type of `T`. Call it once, from module init, after `add_class::<T>`.
pub fn install_am_send<T: PyTypeInfo>(py: Python<'_>, send: ffi::sendfunc) -> PyResult<()> {
    let type_object = T::type_object(py);
    let type_ptr = type_object.as_type_ptr();
    // SAFETY: `type_ptr` is a live type object (`type_object` holds a strong reference). We
    // only read the `tp_as_async` pointer field.
    let async_methods = unsafe { (*type_ptr).tp_as_async };
    if async_methods.is_null() {
        return Err(PyImportError::new_err(format!(
            "porto._core: {} has no async slot table, so am_send cannot be installed; \
             give the class an `__await__` method",
            type_object.name()?
        )));
    }
    // SAFETY: `async_methods` points at the `as_async` table embedded in the heap type object
    // (see the module docs), which is valid and writable. Nothing reads it concurrently: the
    // type is not reachable from Python until module init returns.
    unsafe { (*async_methods).am_send = Some(send) };
    Ok(())
}

/// `StopIteration(value)`, the slow-path way to finish an `await` with `value` (from `send` and
/// `__next__`). The value goes in a 1-tuple so a tuple value is not spread into the arguments.
pub(crate) fn stop_iteration(value: Bound<'_, PyAny>) -> PyErr {
    PyStopIteration::new_err((value.unbind(),))
}

/// Runs the body of a raw `am_send` slot, turning a Rust panic into a `RuntimeError` instead of
/// unwinding into CPython (25-cpython-integration-and-memory.md §8).
pub(crate) fn guard_send(body: impl FnOnce() -> ffi::PySendResult) -> ffi::PySendResult {
    catch_unwind(AssertUnwindSafe(body)).unwrap_or_else(|_| {
        const MESSAGE: &CStr = c"porto._core: a Rust panic escaped am_send; this is a porto bug, \
                                 please report it";
        // SAFETY: slots are called with an attached thread state; both pointers are valid
        // (a static exception type and a NUL-terminated static string).
        unsafe { ffi::PyErr_SetString(ffi::PyExc_RuntimeError, MESSAGE.as_ptr()) };
        ffi::PySendResult::PYGEN_ERROR
    })
}
