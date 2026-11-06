//! Methods of a pyclass as plain `METH_NOARGS` / `METH_FASTCALL` C functions, for the calls on
//! the hottest paths (`async with lock:`'s `__aenter__` and `__aexit__`, porto-itr.7).
//!
//! pyo3's method trampoline counts the GIL (a thread local that macOS reaches through a function
//! call) and looks the type object up to check `self`; a `METH_FASTCALL` method also parses its
//! arguments against a description. Together that was several ns per call on paths of tens. A
//! method descriptor made by [`install`] checks `self`'s type itself (CPython refuses to call it
//! on anything else), so the C function only casts it.
//!
//! [`install`] puts the descriptor in the type's dict, in place of the pyo3 method of the same
//! name if there is one, at module init: the same reasoning as for the async slots
//! (`async_slots`) applies. `PyType_Modified` drops any lookup the type cached.

use core::ffi::CStr;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pyo3::exceptions::PyImportError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::type_object::PyTypeInfo;

/// A `PyMethodDef`, which CPython requires to be a static.
pub(crate) struct MethodDef(pub(crate) ffi::PyMethodDef);

// SAFETY: the definition is immutable and its pointers are to statics (a C function and
// NUL-terminated strings); CPython only reads it.
unsafe impl Sync for MethodDef {}

/// Makes `def` a method of `T`. Call it once from module init, after `add_class::<T>`.
pub(crate) fn install<T: PyTypeInfo>(py: Python<'_>, def: &'static MethodDef) -> PyResult<()> {
    let type_object = T::type_object(py);
    let type_ptr = type_object.as_type_ptr();
    // SAFETY: attached; `def` is a static, as `PyDescr_NewMethod` requires, and its name a
    // NUL-terminated static string. `type_ptr` is a live heap type that nothing outside module
    // init can reach yet (see the module docs), whose dict it owns.
    unsafe {
        let descriptor = Bound::from_owned_ptr_or_err(
            py,
            ffi::PyDescr_NewMethod(type_ptr, (&raw const def.0).cast_mut()),
        )?;
        let dict = (*type_ptr).tp_dict;
        if dict.is_null() {
            return Err(PyImportError::new_err(format!(
                "porto._core: {} has no dict to add a method to; this is a porto bug, please \
                 report it",
                type_object.name()?
            )));
        }
        if ffi::PyDict_SetItemString(dict, def.0.ml_name, descriptor.as_ptr()) < 0 {
            return Err(PyErr::fetch(py));
        }
        ffi::PyType_Modified(type_ptr);
    }
    Ok(())
}

/// Runs the body of a raw method: its result as a new reference, or NULL with the error set.
/// A Rust panic is raised as `porto.RustPanic` instead of unwinding into CPython.
pub(crate) fn guard(body: impl FnOnce(Python<'_>) -> PyResult<Py<PyAny>>) -> *mut ffi::PyObject {
    catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: CPython calls methods attached.
        //
        // pyo3 does not count the thread as attached here (no trampoline raised its count), so
        // a `Py` the body drops is not released at once: pyo3 parks it in its reference pool
        // and releases it the next time it attaches the thread (the next pyo3 method call). A
        // `Bound` is released at once. Neither is unsound, but an object dropped as a `Py`
        // lives a little longer (its `__del__` or a weakref callback runs later).
        let py = unsafe { Python::assume_attached() };
        match body(py) {
            Ok(result) => result.into_ptr(),
            Err(error) => {
                error.restore(py);
                core::ptr::null_mut()
            }
        }
    }))
    .unwrap_or_else(|payload| {
        crate::async_slots::raise_panic(&*payload);
        core::ptr::null_mut()
    })
}

/// The three positional arguments of an `__aexit__(exc_type, exc, traceback)` called with
/// `METH_FASTCALL`, or `TypeError` naming `method`.
///
/// # Safety
/// Attached; `args` points at `nargs` live objects (the `METH_FASTCALL` contract).
pub(crate) unsafe fn aexit_args<'a, 'py>(
    py: Python<'py>,
    method: &CStr,
    args: *const *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
) -> PyResult<[Borrowed<'a, 'py, PyAny>; 3]> {
    if nargs != 3 {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "{}() takes exactly 3 positional arguments (exc_type, exc, traceback), {nargs} given",
            method.to_string_lossy()
        )));
    }
    // SAFETY: the caller's contract; three arguments, live for the call.
    Ok(core::array::from_fn(|index| unsafe {
        Borrowed::from_ptr(py, *args.add(index))
    }))
}
