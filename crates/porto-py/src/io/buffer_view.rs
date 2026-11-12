//! Views of the Python buffers an I/O operation sends from or receives into
//! (13-io-driver.md §5, §6): [`PyBufferRef`] and [`PyBufferMut`].
//!
//! A view is taken with `PyObject_GetBuffer(PyBUF_SIMPLE)` (plus `PyBUF_WRITABLE` for a
//! receive), so the bytes are one contiguous run, and the exporter keeps them where they are
//! while the view lives (a `bytearray` refuses to resize, a `porto.Buffer` too). The view is
//! released on drop, which MUST happen on a Python lane, attached (25 P1): debug builds assert
//! it. A release build that drops one detached anyway attaches to release it.

use std::mem::MaybeUninit;
use std::ptr::NonNull;

use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::prelude::*;

/// A Python buffer viewed as one contiguous run of bytes, released on drop.
struct View(NonNull<ffi::Py_buffer>);

// SAFETY: the view is only read by the operation that owns it, polled on its lane; the exporter
// keeps the memory alive and in place until `PyBuffer_Release`, which `Drop` calls attached
// (P1). Moving the owning future between threads moves the view, nothing else touches it.
unsafe impl Send for View {}

impl View {
    /// A view of `object`'s bytes, writable if asked. `what` names the argument in the error.
    fn new(object: &Bound<'_, PyAny>, writable: bool, what: &str) -> PyResult<Self> {
        let py = object.py();
        let flags = if writable {
            ffi::PyBUF_SIMPLE | ffi::PyBUF_WRITABLE
        } else {
            ffi::PyBUF_SIMPLE
        };
        // Boxed, so the view keeps its address until it is released, as CPython expects of
        // the struct it filled.
        let view: *mut ffi::Py_buffer =
            Box::into_raw(Box::new(MaybeUninit::<ffi::Py_buffer>::zeroed())).cast();
        // SAFETY: attached (we hold `object`); `view` is a writable `Py_buffer`.
        if unsafe { ffi::PyObject_GetBuffer(object.as_ptr(), view, flags) } != 0 {
            // SAFETY: from `Box::into_raw` above; the failed call left nothing to release.
            drop(unsafe { Box::from_raw(view.cast::<MaybeUninit<ffi::Py_buffer>>()) });
            let cause = PyErr::fetch(py);
            let kind = if writable {
                "a writable bytes-like object (a bytearray, a memoryview of one, or a \
                 porto.Buffer)"
            } else {
                "a bytes-like object (bytes, bytearray, memoryview or porto.Buffer)"
            };
            let error = PyTypeError::new_err(format!(
                "porto: {what} must be {kind}, not {}; pass one, or convert the value first",
                object.get_type().name()?
            ));
            error.set_cause(py, Some(cause));
            return Err(error);
        }
        Ok(Self(
            NonNull::new(view).expect("Box::into_raw is never null"),
        ))
    }

    fn raw(&self) -> &ffi::Py_buffer {
        // SAFETY: filled by `PyObject_GetBuffer`, live until `Drop`.
        unsafe { self.0.as_ref() }
    }

    fn len(&self) -> usize {
        usize::try_from(self.raw().len).expect("a buffer's length is never negative")
    }
}

impl Drop for View {
    fn drop(&mut self) {
        let attached = thread_is_attached();
        debug_assert!(
            attached,
            "porto: a buffer view was released off a Python lane (25 P1)"
        );
        let release = || {
            // SAFETY: attached (checked, or attached just now); the view was filled by
            // `PyObject_GetBuffer` and is released once.
            unsafe { ffi::PyBuffer_Release(self.0.as_ptr()) };
        };
        if attached {
            release();
        } else {
            Python::attach(|_| release());
        }
        // SAFETY: from `Box::into_raw` in `new` (as a `MaybeUninit`, same layout), freed once,
        // after the release.
        drop(unsafe { Box::from_raw(self.0.as_ptr().cast::<MaybeUninit<ffi::Py_buffer>>()) });
    }
}

/// Whether this thread has an attached Python thread state (it holds the GIL on a GIL build).
pub(crate) fn thread_is_attached() -> bool {
    // SAFETY: a plain query of this thread's state, callable detached.
    unsafe { ffi::PyGILState_Check() == 1 }
}

/// A read-only view: what a send writes out.
pub(crate) struct PyBufferRef(View);

impl PyBufferRef {
    /// A view of `object`, a bytes-like object. `what` names the argument in the error.
    pub(crate) fn new(object: &Bound<'_, PyAny>, what: &str) -> PyResult<Self> {
        View::new(object, false, what).map(Self)
    }

    /// The bytes.
    pub(crate) fn bytes(&self) -> &[u8] {
        let len = self.0.len();
        if len == 0 {
            return &[];
        }
        // SAFETY: a `PyBUF_SIMPLE` view is `len` contiguous bytes at `buf`, kept in place by
        // the exporter while the view lives. On a GIL build no Python code runs during the
        // attempt (it runs on the lane, attached); a free-threaded build's other threads can
        // write the bytes meanwhile, as they can during `socket.send`, and the kernel then
        // sends whichever bytes it copied.
        unsafe { std::slice::from_raw_parts(self.0.raw().buf.cast::<u8>(), len) }
    }
}

/// A writable view: what a receive fills.
pub(crate) struct PyBufferMut(View);

impl PyBufferMut {
    /// A writable view of `object`. `what` names the argument in the error.
    pub(crate) fn new(object: &Bound<'_, PyAny>, what: &str) -> PyResult<Self> {
        View::new(object, true, what).map(Self)
    }

    /// The bytes, to receive into.
    pub(crate) fn spare(&mut self) -> &mut [MaybeUninit<u8>] {
        let len = self.0.len();
        if len == 0 {
            return &mut [];
        }
        // SAFETY: a writable `PyBUF_SIMPLE` view is `len` contiguous writable bytes at `buf`,
        // kept in place while the view lives; `&mut self` makes this the only Rust access.
        unsafe { std::slice::from_raw_parts_mut(self.0.raw().buf.cast(), len) }
    }
}
