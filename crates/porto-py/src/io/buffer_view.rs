//! Views of the Python buffers an I/O operation sends from or receives into
//! (13-io-driver.md §5, §6): [`PyBufferRef`] and [`PyBufferMut`].
//!
//! A view is taken with `PyObject_GetBuffer(PyBUF_SIMPLE)` (plus `PyBUF_WRITABLE` for a
//! receive), so the bytes are one contiguous run, and the exporter keeps them where they are
//! while the view lives (a `bytearray` refuses to resize, a `porto.Buffer` too).
//!
//! The `Py_buffer` lives in a small Python object, `_BufferView`, whose deallocation releases
//! it: an operation holds a `Py<_BufferView>`, so dropping the operation is a decref like any
//! other `Py` it could hold (25 P1). On a Python lane that releases the view at once; where no
//! thread state is attached (a task cancelled or freed off its lane, a wake after shutdown)
//! pyo3 defers the decref to the next attached thread, and the release happens there.

use std::mem::MaybeUninit;
use std::ptr::NonNull;

use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::prelude::*;

/// Owns one filled `Py_buffer` and releases it when the object is deallocated (attached).
#[pyclass(frozen, module = "porto._core", name = "_BufferView")]
pub(crate) struct ExportedView(NonNull<ffi::Py_buffer>);

// SAFETY: the `Py_buffer` is read through `&self` only (its `buf` and `len`, never written after
// `PyObject_GetBuffer` filled it), and released once, by `Drop`, which CPython runs attached
// when the object is deallocated, on whichever thread drops the last reference.
unsafe impl Send for ExportedView {}
// SAFETY: as above.
unsafe impl Sync for ExportedView {}

impl Drop for ExportedView {
    fn drop(&mut self) {
        debug_assert!(
            thread_is_attached(),
            "porto: a buffer view was released without an attached thread state (25 P1)"
        );
        // SAFETY: the object is being deallocated, attached; the view was filled by
        // `PyObject_GetBuffer` and is released once, here.
        unsafe { ffi::PyBuffer_Release(self.0.as_ptr()) };
        // SAFETY: from `Box::into_raw` in `View::new` (as a `MaybeUninit`, same layout), freed
        // once, after the release.
        drop(unsafe { Box::from_raw(self.0.as_ptr().cast::<MaybeUninit<ffi::Py_buffer>>()) });
    }
}

/// A Python buffer viewed as one contiguous run of bytes, released when dropped (deferred to an
/// attached thread if need be).
struct View {
    /// Held only for its drop, which releases the view (deferred if detached).
    _owner: Py<ExportedView>,
    data: *mut u8,
    len: usize,
}

// SAFETY: `_owner` is a `Py` (Send); `data` points into the exporter's memory, kept alive and in
// place by `_owner`, and only the operation that owns the view reads or writes it.
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
        let view = NonNull::new(view).expect("Box::into_raw is never null");
        // SAFETY: filled by `PyObject_GetBuffer` just now.
        let (data, len) = unsafe { (view.as_ref().buf.cast::<u8>(), view.as_ref().len) };
        let len = usize::try_from(len).expect("a buffer's length is never negative");
        // From here on the object owns the view; if it cannot be made, its drop releases it.
        let owner = Py::new(py, ExportedView(view))?;
        Ok(Self {
            _owner: owner,
            data,
            len,
        })
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
        let len = self.0.len;
        if len == 0 {
            return &[];
        }
        // SAFETY: a `PyBUF_SIMPLE` view is `len` contiguous bytes at `buf`, kept in place by
        // the exporter while the view lives. On a GIL build no Python code runs during the
        // attempt (it runs on the lane, attached); a free-threaded build's other threads can
        // write the bytes meanwhile, as they can during `socket.send`, and the kernel then
        // sends whichever bytes it copied.
        unsafe { std::slice::from_raw_parts(self.0.data, len) }
    }
}

/// A writable view: what a receive fills.
pub(crate) struct PyBufferMut(View);

impl PyBufferMut {
    /// A writable view of `object`. `what` names the argument in the error.
    pub(crate) fn new(object: &Bound<'_, PyAny>, what: &str) -> PyResult<Self> {
        View::new(object, true, what).map(Self)
    }

    /// Whether the view holds no bytes.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.len == 0
    }

    /// The bytes, to receive into.
    pub(crate) fn spare(&mut self) -> &mut [MaybeUninit<u8>] {
        let len = self.0.len;
        if len == 0 {
            return &mut [];
        }
        // SAFETY: a writable `PyBUF_SIMPLE` view is `len` contiguous writable bytes at `buf`,
        // kept in place while the view lives; `&mut self` makes this the only Rust access.
        unsafe { std::slice::from_raw_parts_mut(self.0.data.cast(), len) }
    }
}

#[cfg(test)]
mod tests {
    use pyo3::types::PyByteArray;

    use super::*;

    #[test]
    fn a_view_pins_its_exporter_until_released_on_the_lane() {
        Python::initialize();
        Python::attach(|py| {
            let array = PyByteArray::new(py, b"pinned");
            let view = PyBufferRef::new(array.as_any(), "data").unwrap();
            assert_eq!(view.bytes(), b"pinned");
            assert!(array.resize(1).is_err(), "a viewed bytearray cannot resize");
            drop(view);
            array.resize(1).unwrap();
        });
    }

    #[test]
    fn a_writable_view_receives_into_the_exporter() {
        Python::initialize();
        Python::attach(|py| {
            let array = PyByteArray::new(py, b"....");
            let mut view = PyBufferMut::new(array.as_any(), "buffer").unwrap();
            view.spare()[0].write(b'!');
            drop(view);
            assert_eq!(array.to_vec(), b"!...");
            let error = PyBufferMut::new(pyo3::types::PyBytes::new(py, b"x").as_any(), "buf")
                .err()
                .unwrap();
            assert!(error.to_string().contains("writable bytes-like object"));
        });
    }

    /// P1: a view held across a lane hop (dropped on a thread with no Python thread state, as a
    /// task cancelled off its lane does) is released at the next attach, not detached.
    #[test]
    fn a_view_dropped_off_a_python_lane_is_released_at_the_next_attach() {
        Python::initialize();
        let array = Python::attach(|py| {
            let array = PyByteArray::new(py, b"hop");
            let view = PyBufferRef::new(array.as_any(), "data").unwrap();
            py.detach(|| std::thread::spawn(move || drop(view)).join())
                .expect("dropping a view off the lane must not panic");
            array.unbind()
        });
        Python::attach(|py| {
            let array = array.bind(py);
            array
                .resize(1)
                .expect("the deferred release ran when this thread attached");
        });
    }
}
