//! `porto.Buffer`: a Rust-owned, growable byte buffer that exposes the buffer protocol for
//! reading and writing, its storage taken from the lane's pool (13-io-driver.md §6).
//!
//! A receive fills one without copying (`read_buffer`, `read_into`); `bytes(buf)` copies on
//! demand. While a view is exported (a `memoryview`, or a pending operation's view) the buffer
//! cannot change size, like a `bytearray`, so the view's memory never moves.

use std::ffi::c_int;

use pyo3::exceptions::{PyBufferError, PyMemoryError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::buffer_view::PyBufferRef;
use super::pool::PooledBytes;

/// A growable byte buffer from the lane's pool.
#[pyclass(module = "porto", name = "Buffer")]
pub(crate) struct Buffer {
    bytes: PooledBytes,
    /// Views exported and not released yet.
    exports: usize,
}

impl Buffer {
    /// A buffer holding `bytes`.
    pub(crate) fn from_pooled(bytes: PooledBytes) -> Self {
        Self { bytes, exports: 0 }
    }

    fn check_not_exported(&self, change: &str) -> PyResult<()> {
        if self.exports == 0 {
            return Ok(());
        }
        Err(PyBufferError::new_err(format!(
            "porto: cannot {change} a Buffer while a view of it is exported (a memoryview, or \
             an I/O operation still using it); release the memoryview (`with memoryview(buf) \
             as view:`) or await the operation first"
        )))
    }

    /// Changes the length to `size`, zero-filling new bytes.
    fn set_len(&mut self, size: usize) -> PyResult<()> {
        self.bytes
            .reserve_total(size)
            .map_err(|_| out_of_memory(size))?;
        self.bytes.vec_mut().resize(size, 0);
        Ok(())
    }
}

fn out_of_memory(size: usize) -> PyErr {
    PyMemoryError::new_err(format!(
        "porto: could not allocate a Buffer of {size} bytes; ask for a smaller size"
    ))
}

#[pymethods]
impl Buffer {
    /// A buffer of `size` zero bytes.
    #[new]
    #[pyo3(signature = (size = 0, /))]
    fn new(size: usize) -> PyResult<Self> {
        let mut buffer =
            Self::from_pooled(PooledBytes::with_capacity(size).map_err(|_| out_of_memory(size))?);
        buffer.set_len(size)?;
        Ok(buffer)
    }

    fn __len__(&self) -> usize {
        self.bytes.len()
    }

    /// A copy of the contents.
    fn __bytes__<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.bytes)
    }

    /// How many bytes the buffer holds before it has to move to larger storage.
    #[getter]
    fn capacity(&self) -> usize {
        self.bytes.vec().capacity()
    }

    /// Changes the length to `size`: truncates, or appends zero bytes.
    fn resize(&mut self, size: usize) -> PyResult<()> {
        self.check_not_exported("resize")?;
        self.set_len(size)
    }

    /// Appends the bytes of `data`, a bytes-like object (this buffer itself included).
    fn extend(slf: &Bound<'_, Self>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        slf.borrow().check_not_exported("extend")?;
        // A copy of the bytes to append, or a view of them: taken before the buffer is borrowed
        // for the change, since viewing this buffer borrows it too.
        let own_copy = data.is(slf).then(|| slf.borrow().bytes.to_vec());
        let view = match own_copy {
            Some(_) => None,
            None => Some(PyBufferRef::new(data, "Buffer.extend()'s data")?),
        };
        let appended = own_copy
            .as_deref()
            .or_else(|| view.as_ref().map(PyBufferRef::bytes))
            .unwrap_or_default();
        let mut this = slf.borrow_mut();
        let total = this.bytes.len() + appended.len();
        this.bytes
            .reserve_total(total)
            .map_err(|_| out_of_memory(total))?;
        this.bytes.vec_mut().extend_from_slice(appended);
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "porto.Buffer(len={}, capacity={})",
            self.bytes.len(),
            self.bytes.vec().capacity()
        )
    }

    /// Exports the contents, readable and writable.
    ///
    /// # Safety
    ///
    /// CPython calls it as `bf_getbuffer`, attached, with `view` a writable `Py_buffer`.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let mut this = slf.borrow_mut();
        let len =
            ffi::Py_ssize_t::try_from(this.bytes.len()).expect("a Vec's length fits an isize");
        let data = this.bytes.as_mut_ptr().cast();
        // SAFETY: `view` is writable (the caller's contract); `data` points to `len` bytes that
        // stay in place while the view is exported (resizing is refused until it is released).
        // `PyBuffer_FillInfo` takes a reference to `slf` for the view.
        if unsafe { ffi::PyBuffer_FillInfo(view, slf.as_ptr(), data, len, 0, flags) } != 0 {
            return Err(PyErr::fetch(slf.py()));
        }
        this.exports += 1;
        Ok(())
    }

    /// Releases a view exported by `__getbuffer__`.
    ///
    /// # Safety
    ///
    /// CPython calls it as `bf_releasebuffer`, attached, once per exported view.
    unsafe fn __releasebuffer__(&mut self, _view: *mut ffi::Py_buffer) {
        self.exports -= 1;
    }
}
