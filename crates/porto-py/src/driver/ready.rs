//! `_Ready`: an awaitable that already holds its value (11-task-model §6).
//!
//! Hot primitives that can complete synchronously return one of these instead of a trap, so the
//! await never suspends. Through `am_send` the value comes back as `PYGEN_RETURN`, with no
//! `StopIteration`; so does a `None` through `__next__` (the end of iteration, which is how the
//! 3.12+ `SEND` opcode resumes it). Other values through `__next__`, and `send`, raise
//! `StopIteration(value)`. It has no state: awaiting it again returns the same value, so one
//! `_Ready(None)` is shared ([`ready_none`]) and a `None` result allocates nothing.

use pyo3::ffi;
use pyo3::prelude::*;

use pyo3::sync::PyOnceLock;

use crate::async_slots::{IterNext, guard_next, guard_send, stop_iteration};
use crate::throw_args::thrown_exception;

/// An awaitable whose result is already known.
#[pyclass(frozen, freelist = 1024, module = "porto._core", name = "_Ready")]
pub struct Ready {
    value: Py<PyAny>,
}

/// The shared `_Ready(None)`.
static READY_NONE: PyOnceLock<Py<Ready>> = PyOnceLock::new();

/// A ready awaitable that evaluates to `None`: the shared one, so no allocation.
pub(crate) fn ready_none(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let shared = READY_NONE.get_or_try_init(py, || Py::new(py, Ready::new(py.None())))?;
    Ok(shared.clone_ref(py).into_any())
}

/// A ready awaitable that evaluates to `value`; the shared one for `None`.
pub(crate) fn ready(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    if value.is_none(py) {
        return ready_none(py);
    }
    Ok(Py::new(py, Ready::new(value))?.into_any())
}

impl Ready {
    /// A ready awaitable that evaluates to `value`.
    pub fn new(value: Py<PyAny>) -> Self {
        Self { value }
    }

    /// The value the await evaluates to.
    pub fn value(&self) -> &Py<PyAny> {
        &self.value
    }
}

#[pymethods]
impl Ready {
    fn __await__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __iter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if self.value.is_none(py) {
            return Ok(None);
        }
        Err(stop_iteration(self.value.bind(py).clone()))
    }

    #[pyo3(signature = (_value, /))]
    fn send(&self, py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        Err(stop_iteration(self.value.bind(py).clone()))
    }

    #[pyo3(signature = (typ, val = None, tb = None, /))]
    fn throw(
        &self,
        typ: &Bound<'_, PyAny>,
        val: Option<&Bound<'_, PyAny>>,
        tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        Err(thrown_exception(typ, val, tb))
    }

    fn close(&self) {}

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "<porto._core._Ready {}>",
            self.value.bind(py).repr()?
        ))
    }
}

/// `am_send` for `_Ready`: returns the value as `PYGEN_RETURN`, whatever was sent.
///
/// # Safety
///
/// CPython calls it as `tp_as_async->am_send` of `_Ready`, with an attached thread state, `slf` a
/// live `_Ready` and `presult` writable.
pub(crate) unsafe extern "C" fn ready_am_send(
    slf: *mut ffi::PyObject,
    _arg: *mut ffi::PyObject,
    presult: *mut *mut ffi::PyObject,
) -> ffi::PySendResult {
    guard_send(|| {
        // SAFETY: the caller is attached (am_send contract).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: `slf` is live for the call, and the slot is only installed on `_Ready`.
        let ready = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<Ready>() };
        // SAFETY: `presult` is writable (am_send contract); it receives a new reference.
        unsafe { *presult = ready.get().value.clone_ref(py).into_ptr() };
        ffi::PySendResult::PYGEN_RETURN
    })
}

/// `tp_iternext` for `_Ready` (see `async_slots`): the end of iteration with its value.
///
/// # Safety
///
/// CPython calls it as `tp_iternext` of `_Ready`, attached, with `slf` a live `_Ready`.
pub(crate) unsafe extern "C" fn ready_iternext(slf: *mut ffi::PyObject) -> *mut ffi::PyObject {
    guard_next(|py| {
        // SAFETY: `slf` is live for the call, and the slot is only installed on `_Ready`.
        let ready = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<Ready>() };
        Ok(IterNext::Return(ready.get().value.bind(py).clone()))
    })
}
