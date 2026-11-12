//! `porto.Buffer`: a Rust-owned, growable byte buffer that exposes the buffer protocol for
//! reading and writing, its storage taken from the lane's pool (13-io-driver.md §6).
//!
//! A receive fills one without copying (`read_buffer`, `read_into`); `bytes(buf)` copies on
//! demand. While a view is exported (a `memoryview`, or a pending operation's view) the buffer
//! cannot change size, like a `bytearray`, so the view's memory never moves.
//!
//! The class is frozen: the storage is behind a short lock that is never held while Python code
//! runs (P3), and the count of exported views is atomic, so `memoryview(buf)` and its release
//! never fail on a free-threaded build because another thread is reading the buffer. A change
//! of size checks the count under the lock, after everything that may run Python code (taking a
//! view of the data to append runs its `__buffer__`, which may export this buffer).

use std::ffi::c_int;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::{Mutex, MutexGuard};
use pyo3::exceptions::{PyBufferError, PyMemoryError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::buffer_view::PyBufferRef;
use super::pool::PooledBytes;

/// A growable byte buffer from the lane's pool.
#[pyclass(frozen, module = "porto", name = "Buffer")]
pub(crate) struct Buffer {
    bytes: Mutex<PooledBytes>,
    /// Views exported and not released yet. Incremented with `bytes` locked, so a change of
    /// size that sees 0 under the lock cannot race with a new export.
    exports: AtomicUsize,
}

impl Buffer {
    /// A buffer holding `bytes`.
    pub(crate) fn from_pooled(bytes: PooledBytes) -> Self {
        Self {
            bytes: Mutex::new(bytes),
            exports: AtomicUsize::new(0),
        }
    }

    /// The storage, locked for a change of size: refused while a view is exported.
    fn lock_for_resize(&self, change: &str) -> PyResult<MutexGuard<'_, PooledBytes>> {
        let bytes = self.bytes.lock();
        if self.exports.load(Ordering::Acquire) == 0 {
            return Ok(bytes);
        }
        Err(PyBufferError::new_err(format!(
            "porto: cannot {change} a Buffer while a view of it is exported (a memoryview, or \
             an I/O operation still using it); release the memoryview (`with memoryview(buf) \
             as view:`) or await the operation first"
        )))
    }
}

/// Changes the length of `bytes` to `size`, zero-filling new bytes.
fn set_len(bytes: &mut PooledBytes, size: usize) -> PyResult<()> {
    bytes.reserve_total(size).map_err(|_| out_of_memory(size))?;
    bytes.vec_mut().resize(size, 0);
    Ok(())
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
        let mut bytes = PooledBytes::with_capacity(size).map_err(|_| out_of_memory(size))?;
        set_len(&mut bytes, size)?;
        Ok(Self::from_pooled(bytes))
    }

    fn __len__(&self) -> usize {
        self.bytes.lock().len()
    }

    /// A copy of the contents.
    fn __bytes__<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let copy = self.bytes.lock().to_vec();
        PyBytes::new(py, &copy)
    }

    /// How many bytes the buffer holds before it has to move to larger storage.
    #[getter]
    fn capacity(&self) -> usize {
        self.bytes.lock().vec().capacity()
    }

    /// Changes the length to `size`: truncates, or appends zero bytes.
    fn resize(&self, size: usize) -> PyResult<()> {
        set_len(&mut *self.lock_for_resize("resize")?, size)
    }

    /// Appends the bytes of `data`, a bytes-like object (this buffer itself included).
    fn extend(slf: &Bound<'_, Self>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let this = slf.get();
        // Fail early, before running any of `data`'s code; checked again below.
        drop(this.lock_for_resize("extend")?);
        // The bytes to append: a copy of this buffer's own, or a view of `data`'s, taken
        // without the lock (it may run Python code, which may use this buffer).
        let own_copy = data.is(slf).then(|| this.bytes.lock().to_vec());
        let view = match own_copy {
            Some(_) => None,
            None => Some(PyBufferRef::new(data, "Buffer.extend()'s data")?),
        };
        let appended = own_copy
            .as_deref()
            .or_else(|| view.as_ref().map(PyBufferRef::bytes))
            .unwrap_or_default();
        // Again under the lock: taking the view may have exported this buffer (a `__buffer__`
        // that returns `memoryview(buf)`, or keeps one), whose memory must not move.
        let mut bytes = this.lock_for_resize("extend")?;
        let total = bytes.len() + appended.len();
        bytes
            .reserve_total(total)
            .map_err(|_| out_of_memory(total))?;
        bytes.vec_mut().extend_from_slice(appended);
        drop(bytes);
        // The view is released after the lock (its release may run Python code).
        drop(view);
        Ok(())
    }

    fn __repr__(&self) -> String {
        let bytes = self.bytes.lock();
        format!(
            "porto.Buffer(len={}, capacity={})",
            bytes.len(),
            bytes.vec().capacity()
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
        let this = slf.get();
        let mut bytes = this.bytes.lock();
        let len = ffi::Py_ssize_t::try_from(bytes.len()).expect("a Vec's length fits an isize");
        let data = bytes.as_mut_ptr().cast();
        // SAFETY: `view` is writable (the caller's contract); `data` points to `len` bytes that
        // stay in place while the view is exported (a change of size is refused until it is
        // released). `PyBuffer_FillInfo` runs no Python code and takes a reference to `slf`.
        if unsafe { ffi::PyBuffer_FillInfo(view, slf.as_ptr(), data, len, 0, flags) } != 0 {
            drop(bytes);
            return Err(PyErr::fetch(slf.py()));
        }
        // Under the lock, so a change of size that holds it sees this export.
        this.exports.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Releases a view exported by `__getbuffer__`.
    ///
    /// # Safety
    ///
    /// CPython calls it as `bf_releasebuffer`, attached, once per exported view.
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
        self.exports.fetch_sub(1, Ordering::AcqRel);
    }
}
