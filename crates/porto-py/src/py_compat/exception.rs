//! Taking the raised exception out of the thread state, on every supported version.
//!
//! 3.12+ keeps only normalized exceptions in the thread state; 3.11 keeps the
//! `(type, value, traceback)` triple a C extension set with `PyErr_Restore` as it is, so it is
//! normalized here. The two disagree on one corner: a value that is an instance of a *subclass* of
//! the type is kept by 3.11's normalization, while 3.12's `PyErr_Restore` wraps it in a new
//! instance of the type (`tests/python/test_py_compat.py`).

use pyo3::exceptions::{PyBaseException, PySystemError};
use pyo3::ffi;
use pyo3::prelude::*;

/// Takes the raised exception out of the thread state and returns it normalized, with its
/// traceback attached. If none is set (a C API broke its contract), returns a `SystemError`
/// saying so, rather than losing the failure.
///
/// 3.12+ uses `PyErr_GetRaisedException`; 3.11 uses `PyErr_Fetch` + `PyErr_NormalizeException`
/// and puts the traceback back on the exception (11-task-model-and-trap-protocol.md §4).
pub(crate) fn take_exception(py: Python<'_>) -> Bound<'_, PyBaseException> {
    // SAFETY: attached (we hold `py`). Takes ownership of the raised exception, if any.
    let raised = unsafe { take_raised(py) };
    raised.unwrap_or_else(|| {
        PySystemError::new_err(
            "porto: a C API call failed without setting an exception; this is a porto bug, \
             please report it",
        )
        .into_value(py)
        .into_bound(py)
    })
}

/// # Safety
/// The thread state must be attached.
#[cfg(Py_3_12)]
unsafe fn take_raised(py: Python<'_>) -> Option<Bound<'_, PyBaseException>> {
    // SAFETY: attached (caller). Returns a new reference to an exception instance, or NULL.
    let raised = unsafe { ffi::PyErr_GetRaisedException() };
    // SAFETY: a new reference or NULL, and always an exception instance.
    unsafe { Bound::from_owned_ptr_or_opt(py, raised).map(|any| any.cast_into_unchecked()) }
}

/// # Safety
/// The thread state must be attached.
#[cfg(not(Py_3_12))]
unsafe fn take_raised(py: Python<'_>) -> Option<Bound<'_, PyBaseException>> {
    let mut kind = core::ptr::null_mut();
    let mut value = core::ptr::null_mut();
    let mut traceback = core::ptr::null_mut();
    // SAFETY: attached (caller); the out-pointers are valid. We own the three references.
    unsafe { ffi::PyErr_Fetch(&raw mut kind, &raw mut value, &raw mut traceback) };
    if kind.is_null() {
        return None;
    }
    // SAFETY: the triple came from PyErr_Fetch; normalizing replaces it with owned references
    // where `value` is an instance of `kind`.
    unsafe { ffi::PyErr_NormalizeException(&raw mut kind, &raw mut value, &raw mut traceback) };
    // SAFETY: we own the references; `from_owned_ptr_or_opt` takes them over (NULL stays None).
    let (kind, value, traceback) = unsafe {
        (
            Bound::from_owned_ptr_or_opt(py, kind),
            Bound::from_owned_ptr_or_opt(py, value),
            Bound::from_owned_ptr_or_opt(py, traceback),
        )
    };
    drop(kind);
    let value = value?;
    if let Some(traceback) = traceback {
        // SAFETY: `value` is a normalized exception instance and `traceback` a traceback;
        // the call does not steal either reference.
        unsafe { ffi::PyException_SetTraceback(value.as_ptr(), traceback.as_ptr()) };
    }
    // SAFETY: normalized, so an exception instance.
    Some(unsafe { value.cast_into_unchecked() })
}
