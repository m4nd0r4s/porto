//! `PyIter_Send` with steal semantics (25-cpython-integration-and-memory.md §4).
//!
//! `PyIter_Send` borrows the value it sends. The step driver already owns that value (the result
//! of a wait, moved out of the task), so the wrapper takes it by value and releases it after the
//! call: one reference travels from the producer into the coroutine, with no `Py_INCREF` and
//! `Py_DECREF` pair around the call. `None` is sent as the `Py_None` singleton, with no refcount
//! traffic at all.
//!
//! On a return, `PyIter_Send` hands the value back without a `StopIteration` when the outermost
//! iterator implements `am_send` (coroutines and porto's awaitables do). Inside a chain of
//! coroutines that is only true on 3.11: from 3.12 the `SEND` opcode resumes a non-generator
//! awaitable through `tp_iternext`/`send`, so an inner `_Ready` still raises a `StopIteration`
//! that the frame catches (11-task-model-and-trap-protocol.md §5.1, errata in porto-szb.87).

use core::ptr;

use pyo3::exceptions::PyBaseException;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyNone;

use super::take_exception;

/// What one `PyIter_Send` did.
pub(crate) enum Sent<'py> {
    /// The iterator finished, returning this value.
    Returned(Bound<'py, PyAny>),
    /// The iterator suspended, yielding this object.
    Yielded(Bound<'py, PyAny>),
    /// The iterator raised this exception (normalized, with its traceback).
    Raised(Bound<'py, PyBaseException>),
}

/// Resumes `iterator` with `value` (`None` sends Python `None`), taking ownership of `value`:
/// the caller's reference is released after the call, so no extra reference is created for it.
pub(crate) fn send<'py>(
    iterator: &Bound<'py, PyAny>,
    value: Option<Bound<'py, PyAny>>,
) -> Sent<'py> {
    let py = iterator.py();
    let argument = value
        .as_ref()
        .map_or_else(|| PyNone::get(py).as_ptr(), Bound::as_ptr);
    let mut result = ptr::null_mut();
    // SAFETY: attached (we hold `py`); `iterator` and `argument` stay alive for the call
    // (`value` is dropped only after it), and `result` is a valid out-pointer. `PyIter_Send`
    // borrows both arguments and writes a new reference to `result` unless it fails.
    let status = unsafe { ffi::PyIter_Send(iterator.as_ptr(), argument, &raw mut result) };
    drop(value);
    match status {
        ffi::PySendResult::PYGEN_ERROR => Sent::Raised(take_exception(py)),
        // SAFETY: on PYGEN_RETURN and PYGEN_NEXT, `result` is a new, non-null reference.
        status => match (status, unsafe { Bound::from_owned_ptr(py, result) }) {
            (ffi::PySendResult::PYGEN_RETURN, value) => Sent::Returned(value),
            (_, yielded) => Sent::Yielded(yielded),
        },
    }
}
