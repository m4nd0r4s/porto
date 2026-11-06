//! Building the objects of a hot pyclass without pyo3's generic path, on GIL builds
//! (porto-szb.117).
//!
//! `Py::new` looks the type object up, goes through pyo3's initializer and reaches the type's
//! `tp_alloc` through `object.__new__`'s machinery, moving the value several times on the way;
//! for the trap of every `porto.checkpoint()` and the wait of every `async with lock:` that was
//! a few ns of a budget of tens. [`FastNew::object`] calls `tp_alloc` (the lock-free freelist,
//! `freelist.rs`), writes the value where pyo3 keeps it and zeroes what pyo3 keeps after it,
//! then hands the object to pyo3, which borrows, drops and frees it as one of its own.
//!
//! [`FastNew::init`] measures the layout on a probe object made by `Py::new`, and only enables
//! the fast path if it is the one this relies on: the value right after the object header (at
//! `size_of::<PyObject>()` rounded up to the value's alignment), and everything after it up to
//! `tp_basicsize` (a borrow flag, nothing for a frozen class) zero in a fresh object, so zeroing
//! it is pyo3's own initialization. A pyo3 upgrade that changes either falls back to `Py::new`
//! instead of corrupting memory, and so does a free-threaded build.

use core::marker::PhantomData;
use core::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use pyo3::PyClass;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pyclass_init::PyClassInitializer;

/// The fast constructor of one pyclass `T`: a `static`, enabled by [`FastNew::init`].
pub(crate) struct FastNew<T> {
    /// `T`'s type object, once `init` enabled the fast path; null until then.
    type_ptr: AtomicPtr<ffi::PyTypeObject>,
    /// How many bytes follow the value up to `tp_basicsize`, all zero in a fresh object.
    tail: AtomicUsize,
    _value: PhantomData<fn(T)>,
}

impl<T: PyClass + Into<PyClassInitializer<T>>> FastNew<T> {
    /// Where the value starts: right after the object header.
    const VALUE_OFFSET: usize = size_of::<ffi::PyObject>().next_multiple_of(align_of::<T>());

    /// Disabled until [`FastNew::init`].
    pub(crate) const fn new() -> Self {
        Self {
            type_ptr: AtomicPtr::new(core::ptr::null_mut()),
            tail: AtomicUsize::new(0),
            _value: PhantomData,
        }
    }

    /// Enables the fast path if `T`'s layout allows it (see the module docs), measured on an
    /// object made from `probe`, which must not warn when dropped. Call it once from module
    /// init, after the type's slots are installed.
    pub(crate) fn init(&self, py: Python<'_>, probe: T) -> PyResult<()> {
        if cfg!(Py_GIL_DISABLED) {
            return Ok(());
        }
        let probe = Bound::new(py, probe)?;
        let object = probe.as_ptr();
        let offset = {
            let value = probe.borrow();
            core::ptr::from_ref::<T>(&value).addr() - object.addr()
        };
        // SAFETY: attached; `probe` is a live `T` object, so its type is live.
        let basic_size = unsafe { (*ffi::Py_TYPE(object)).tp_basicsize };
        let Some(tail) = usize::try_from(basic_size)
            .ok()
            .and_then(|size| size.checked_sub(Self::VALUE_OFFSET + size_of::<T>()))
        else {
            return Ok(());
        };
        // SAFETY: the probe's memory is `tp_basicsize` bytes long, and nothing borrows it now.
        let tail_is_zero = unsafe {
            core::slice::from_raw_parts(
                object.cast::<u8>().add(Self::VALUE_OFFSET + size_of::<T>()),
                tail,
            )
        }
        .iter()
        .all(|byte| *byte == 0);
        if offset == Self::VALUE_OFFSET && tail_is_zero {
            self.tail.store(tail, Ordering::Relaxed);
            self.type_ptr
                .store(T::type_object_raw(py), Ordering::Release);
        }
        Ok(())
    }

    /// `value` as a new `T` object.
    pub(crate) fn object(&self, py: Python<'_>, value: T) -> PyResult<Py<T>> {
        let type_ptr = self.type_ptr.load(Ordering::Acquire);
        if type_ptr.is_null() {
            return Py::new(py, value);
        }
        // SAFETY: attached; `init` checked that a `T` object is its header, the value at
        // `VALUE_OFFSET` and `tail` bytes that a fresh object has zeroed (see the module docs),
        // which pyo3 reads, drops and frees like any `T` it made: `tp_alloc` returns a fresh
        // object of the type with one reference, the writes initialize all of it past the
        // header, and the new reference becomes the `Py`.
        unsafe {
            let alloc = (*type_ptr).tp_alloc.unwrap_or(ffi::PyType_GenericAlloc);
            let object = alloc(type_ptr, 0);
            if object.is_null() {
                return Err(PyErr::fetch(py));
            }
            let value_ptr = object.byte_add(Self::VALUE_OFFSET).cast::<T>();
            value_ptr.write(value);
            value_ptr
                .add(1)
                .cast::<u8>()
                .write_bytes(0, self.tail.load(Ordering::Relaxed));
            Ok(Bound::from_owned_ptr(py, object)
                .cast_into_unchecked()
                .unbind())
        }
    }
}
