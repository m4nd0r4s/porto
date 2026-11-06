//! The `am_send` slot for pyclasses, which pyo3 0.30 does not expose, and raw `tp_iternext` and
//! `am_await` slots for the awaitables on the hot path.
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
//!
//! From 3.12 the `SEND` opcode resumes an awaitable that is not a generator through
//! `tp_iternext` (`send(None)`), not `am_send`, and `GET_AWAITABLE` calls `am_await` first: two
//! slot calls per `await`, three for one that suspends. pyo3's slot trampolines cost a few ns each
//! (its GIL count, a thread local that macOS reaches through a function call), so
//! [`install_iter_slots`] replaces them for `_Trap`, `_Ready` and `_SyncWait` with raw functions
//! guarded by [`guard_slot`]. The same reasoning as for `am_send` applies to writing the slots;
//! the Python-visible `__next__` and `__await__` (method wrappers made from pyo3's functions when
//! the type was created) keep working and do the same. A raw slot only drops `Bound` references
//! (a plain `Py_DECREF`), never a `Py<T>`, whose drop defers to pyo3's pool when pyo3 does not
//! know the thread is attached.

use core::ffi::CStr;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pyo3::exceptions::{PyImportError, PyStopIteration};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::type_object::PyTypeInfo;

use crate::errors;
use crate::py_compat::raise_stop_iteration;

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

/// Sets `tp_iternext` and `am_await` on the type of `T` (see the module docs). Call it once, from
/// module init, after `add_class::<T>`; `T` MUST define `__next__` and `__await__` (returning
/// itself), which the slots replace.
pub fn install_iter_slots<T: PyTypeInfo>(py: Python<'_>, next: ffi::iternextfunc) -> PyResult<()> {
    let type_object = T::type_object(py);
    let type_ptr = type_object.as_type_ptr();
    // SAFETY: as in `install_am_send`: a live heap type that nothing else can reach yet.
    let async_methods = unsafe { (*type_ptr).tp_as_async };
    if async_methods.is_null() {
        return Err(PyImportError::new_err(format!(
            "porto._core: {} has no async slot table, so am_await cannot be installed; \
             give the class an `__await__` method",
            type_object.name()?
        )));
    }
    // SAFETY: as in `install_am_send`; `tp_iternext` is a field of the type object itself.
    unsafe {
        (*async_methods).am_await = Some(await_self);
        (*type_ptr).tp_iternext = Some(next);
    }
    Ok(())
}

/// `am_await` for an awaitable that is its own iterator: `__await__` returns itself.
///
/// # Safety
///
/// CPython calls it as `tp_as_async->am_await`, attached, with `slf` a live object.
unsafe extern "C" fn await_self(slf: *mut ffi::PyObject) -> *mut ffi::PyObject {
    // SAFETY: `slf` is live (slot contract); the new reference is the slot's result.
    unsafe { ffi::Py_INCREF(slf) };
    slf
}

/// What a raw `tp_iternext` returns: an object to yield, the end of iteration with `value` as
/// the result of the `await`, or an error.
pub(crate) enum IterNext<'py> {
    /// Yield this object to the driver (a new reference).
    Yield(Bound<'py, PyAny>),
    /// The await evaluates to this.
    Return(Bound<'py, PyAny>),
}

/// Runs the body of a raw `tp_iternext` slot: a yield is returned, a `None` result ends the
/// iteration with no exception (nothing is allocated), any other result raises
/// `StopIteration(value)`. A Rust panic is raised as `porto.RustPanic`, like [`guard_send`].
pub(crate) fn guard_next(
    body: impl for<'py> FnOnce(Python<'py>) -> PyResult<IterNext<'py>>,
) -> *mut ffi::PyObject {
    catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: slots are called with an attached thread state.
        let py = unsafe { Python::assume_attached() };
        match body(py) {
            Ok(IterNext::Yield(yielded)) => yielded.into_ptr(),
            Ok(IterNext::Return(value)) if value.is_none() => core::ptr::null_mut(),
            Ok(IterNext::Return(value)) => {
                raise_stop_iteration(&value);
                core::ptr::null_mut()
            }
            Err(error) => {
                error.restore(py);
                core::ptr::null_mut()
            }
        }
    }))
    .unwrap_or_else(|payload| {
        raise_panic(&*payload);
        core::ptr::null_mut()
    })
}

/// `StopIteration(value)`, the slow-path way to finish an `await` with `value` (from `send` and
/// `__next__`). The value goes in a 1-tuple so a tuple value is not spread into the arguments.
pub(crate) fn stop_iteration(value: Bound<'_, PyAny>) -> PyErr {
    PyStopIteration::new_err((value.unbind(),))
}

/// Runs the body of a raw `am_send` slot, turning a Rust panic into a `porto.RustPanic` raised
/// into the awaiting task instead of unwinding into CPython (25-cpython-integration-and-memory.md
/// §8, ADR-027).
pub(crate) fn guard_send(body: impl FnOnce() -> ffi::PySendResult) -> ffi::PySendResult {
    catch_unwind(AssertUnwindSafe(body)).unwrap_or_else(|payload| {
        raise_panic(&*payload);
        ffi::PySendResult::PYGEN_ERROR
    })
}

/// Raises a Rust panic that a raw slot caught as `porto.RustPanic`.
fn raise_panic(payload: &(dyn std::any::Any + Send)) {
    // SAFETY: slots are called with an attached thread state.
    let py = unsafe { Python::assume_attached() };
    // Raising `RustPanic` imports `porto.errors`, which panics if the import fails (an
    // interpreter shutting down); a panic must not unwind out of the slot either.
    let raised = catch_unwind(AssertUnwindSafe(|| {
        errors::rust_panic(payload, None).restore(py);
    }));
    if raised.is_err() {
        const MESSAGE: &CStr = c"porto._core: a Rust panic escaped a porto slot; this is a porto \
                                 bug, please report it";
        // SAFETY: attached (see above); both pointers are valid (a static exception type and a
        // NUL-terminated static string).
        unsafe { ffi::PyErr_SetString(ffi::PyExc_RuntimeError, MESSAGE.as_ptr()) };
    }
}
