//! A freelist without a lock for the awaitables on the hot path, on GIL builds (porto-szb.117).
//!
//! pyo3's `#[pyclass(freelist = N)]` keeps freed objects for reuse, but takes a mutex on every
//! allocation and every free (it must: on a free-threaded build two threads free objects at
//! once) and looks the type object up each time. That was a fifth of `await porto.checkpoint()`
//! on macOS. On a GIL build every allocation and free of a Python object happens while attached,
//! so the GIL already serializes them: [`install`] replaces the type's `tp_alloc` and `tp_free`
//! with functions over a plain stack ([`gil_freelist!`]).
//!
//! pyo3's freelist also hands a reused object back without the collector tracking it (its
//! `tp_dealloc` stopped that), so a reference cycle through one was never collected. Every class
//! declared with a pyo3 freelist therefore goes through [`install`]: on a GIL build ours tracks
//! a reused object again, and a free-threaded build keeps pyo3's freelist behind a `tp_alloc`
//! that does the same ([`TrackingAlloc`]). Only for a class with a `__traverse__`, though
//! ([`gil_freelist!`]'s `traversed`): the collector cannot see through one without it (`_Trap`,
//! `_Ready`, `_ChannelOp`), so tracking it would only cost time on the hottest allocations
//! (2 ns of a 60 ns `checkpoint`).
//!
//! Only the storage changes: pyo3 still runs `Drop` (so a trap freed unawaited still warns,
//! `unawaited.rs`) and releases the type reference, exactly as with its own freelist. The same
//! reasoning as for `am_send` (`async_slots`) applies to writing the slots at module init.

use core::cell::UnsafeCell;
use core::ffi::c_void;
use std::sync::OnceLock;

use pyo3::exceptions::PyImportError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::type_object::PyTypeInfo;

/// Freed objects of one type, at most `N`; only reached while attached on a GIL build.
pub(crate) struct FreeList<const N: usize> {
    slots: UnsafeCell<([*mut ffi::PyObject; N], usize)>,
    /// Whether a reused object is tracked by the collector again (see the module docs).
    track_reused: bool,
}

// SAFETY: only `alloc` and `free` reach the cell, and CPython calls them as slots, attached;
// on a GIL build one thread is attached at a time, so the accesses never overlap. The module
// installs them only on GIL builds (`install`).
unsafe impl<const N: usize> Sync for FreeList<N> {}

impl<const N: usize> FreeList<N> {
    /// An empty freelist; `track_reused` as in the module docs.
    pub(crate) const fn new(track_reused: bool) -> Self {
        Self {
            slots: UnsafeCell::new(([core::ptr::null_mut(); N], 0)),
            track_reused,
        }
    }

    /// `tp_alloc`: a freed object of `subtype` again, else a new one.
    ///
    /// # Safety
    /// Called as `tp_alloc` of the one type this freelist serves, attached, on a GIL build.
    pub(crate) unsafe fn alloc(
        &self,
        subtype: *mut ffi::PyTypeObject,
        nitems: ffi::Py_ssize_t,
    ) -> *mut ffi::PyObject {
        if nitems == 0 {
            // SAFETY: see the `Sync` impl: no other access overlaps this one.
            let (slots, len) = unsafe { &mut *self.slots.get() };
            if *len > 0 {
                *len -= 1;
                let object = slots[*len];
                // SAFETY: a freed object of this type (only `free` pushes, for this type),
                // whose memory is still allocated; `PyObject_Init` makes it a live object of
                // `subtype` with one reference, as pyo3's freelist does. Its `tp_dealloc`
                // stopped the collector tracking it: track it again, as `PyType_GenericAlloc`
                // does for a new one, or reference cycles through it are never collected. The
                // caller writes the value before anything can run the collector (no Python
                // code or allocation in between), so a traversal never sees the old one.
                unsafe {
                    let object = ffi::PyObject_Init(object, subtype);
                    if self.track_reused && ffi::PyType_IS_GC(subtype) != 0 {
                        ffi::PyObject_GC_Track(object.cast());
                    }
                    return object;
                }
            }
        }
        // SAFETY: attached; the generic allocation pyo3 falls back to.
        unsafe { ffi::PyType_GenericAlloc(subtype, nitems) }
    }

    /// `tp_free`: keeps the memory for reuse, or frees it once the list is full.
    ///
    /// # Safety
    /// Called as `tp_free` of the one type this freelist serves, attached, on a GIL build, with
    /// `object` an instance being freed (its `Drop` already ran).
    pub(crate) unsafe fn free(&self, object: *mut c_void) {
        if object.is_null() {
            return;
        }
        let object = object.cast::<ffi::PyObject>();
        // SAFETY: see the `Sync` impl.
        let (slots, len) = unsafe { &mut *self.slots.get() };
        if *len < N {
            slots[*len] = object;
            *len += 1;
            return;
        }
        // SAFETY: `object` is live memory of an instance; the inverse of `PyType_GenericAlloc`,
        // chosen as pyo3 does.
        unsafe {
            if ffi::PyType_IS_GC(ffi::Py_TYPE(object)) != 0 {
                ffi::PyObject_GC_Del(object.cast());
            } else {
                ffi::PyObject_Free(object.cast());
            }
        }
    }
}

/// A free-threaded build's `tp_alloc` for a class with a pyo3 freelist: pyo3's own, then the
/// collector tracks the object if it came from the freelist (see the module docs).
pub(crate) struct TrackingAlloc {
    /// The `tp_alloc` pyo3 installed, saved by [`install`] before it is replaced.
    pyo3_alloc: OnceLock<ffi::allocfunc>,
}

impl TrackingAlloc {
    /// Not installed yet.
    pub(crate) const fn new() -> Self {
        Self {
            pyo3_alloc: OnceLock::new(),
        }
    }

    /// `tp_alloc`: pyo3's, tracked.
    ///
    /// # Safety
    /// Called as `tp_alloc` of the one type this serves, attached, after [`install`].
    pub(crate) unsafe fn alloc(
        &self,
        subtype: *mut ffi::PyTypeObject,
        nitems: ffi::Py_ssize_t,
    ) -> *mut ffi::PyObject {
        let alloc = self
            .pyo3_alloc
            .get()
            .copied()
            .unwrap_or(ffi::PyType_GenericAlloc);
        // SAFETY: the caller's contract; pyo3's slot for this type. The object it returns is
        // only visible to this thread until it is returned, so tracking it races with nothing.
        unsafe {
            let object = alloc(subtype, nitems);
            if !object.is_null() && ffi::PyObject_GC_IsTracked(object) == 0 {
                ffi::PyObject_GC_Track(object.cast());
            }
            object
        }
    }
}

/// The slots [`install`] puts on one type, made by [`gil_freelist!`].
pub(crate) struct Slots {
    /// GIL build: `tp_alloc` over the [`FreeList`].
    pub(crate) alloc: ffi::allocfunc,
    /// GIL build: `tp_free` over the [`FreeList`].
    pub(crate) free: ffi::freefunc,
    /// Free-threaded build: `tp_alloc` over [`Slots::tracking`].
    pub(crate) tracking_alloc: ffi::allocfunc,
    pub(crate) tracking: &'static TrackingAlloc,
    /// Whether the class has a `__traverse__`, so a reused object is tracked again.
    pub(crate) traversed: bool,
}

/// Defines module `$name` with a [`FreeList`] of `$capacity`, its `alloc` and `free` slots, and
/// `SLOTS` for [`install`]. `traversed`: the class has a `__traverse__` (see the module docs).
macro_rules! gil_freelist {
    ($name:ident, $capacity:expr, traversed: $traversed:expr) => {
        pub(crate) mod $name {
            use pyo3::ffi;

            static LIST: $crate::driver::freelist::FreeList<$capacity> =
                $crate::driver::freelist::FreeList::new($traversed);

            static TRACKING: $crate::driver::freelist::TrackingAlloc =
                $crate::driver::freelist::TrackingAlloc::new();

            /// What `freelist::install` puts on the type.
            pub(crate) static SLOTS: $crate::driver::freelist::Slots =
                $crate::driver::freelist::Slots {
                    alloc,
                    free,
                    tracking_alloc,
                    tracking: &TRACKING,
                    traversed: $traversed,
                };

            /// `tp_alloc` on a free-threaded build: pyo3's, tracked.
            ///
            /// # Safety
            /// As for `alloc`.
            unsafe extern "C" fn tracking_alloc(
                subtype: *mut ffi::PyTypeObject,
                nitems: ffi::Py_ssize_t,
            ) -> *mut ffi::PyObject {
                // SAFETY: the caller's contract.
                unsafe { TRACKING.alloc(subtype, nitems) }
            }

            /// `tp_alloc` over the freelist.
            ///
            /// # Safety
            /// Installed by `freelist::install` on one type only; CPython's slot contract.
            unsafe extern "C" fn alloc(
                subtype: *mut ffi::PyTypeObject,
                nitems: ffi::Py_ssize_t,
            ) -> *mut ffi::PyObject {
                // SAFETY: the caller's contract.
                unsafe { LIST.alloc(subtype, nitems) }
            }

            /// `tp_free` over the freelist.
            ///
            /// # Safety
            /// As for `alloc`.
            unsafe extern "C" fn free(object: *mut core::ffi::c_void) {
                // SAFETY: the caller's contract.
                unsafe { LIST.free(object) }
            }
        }
    };
}

pub(crate) use gil_freelist;

/// Puts `slots` (from [`gil_freelist!`], one module per type) on `T`: the [`FreeList`]'s
/// `tp_alloc` and `tp_free` on a GIL build, pyo3's freelist behind a [`TrackingAlloc`] on a
/// free-threaded one. Call it once, from module init, right after `add_class::<T>`, before any
/// instance exists; `T` MUST be final.
pub(crate) fn install<T: PyTypeInfo>(py: Python<'_>, slots: &'static Slots) -> PyResult<()> {
    let type_object = T::type_object(py);
    let type_ptr = type_object.as_type_ptr();
    // SAFETY: a live heap type that nothing outside module init can reach yet (see
    // `async_slots`); no instance exists, so none was allocated by the slot being replaced.
    unsafe {
        if (*type_ptr).tp_alloc.is_none() || (*type_ptr).tp_free.is_none() {
            return Err(PyImportError::new_err(format!(
                "porto._core: {} has no tp_alloc/tp_free to replace; this is a porto bug, \
                 please report it",
                type_object.name()?
            )));
        }
        if cfg!(Py_GIL_DISABLED) {
            if !slots.traversed {
                return Ok(());
            }
            if let Some(pyo3_alloc) = (*type_ptr).tp_alloc {
                slots.tracking.pyo3_alloc.get_or_init(|| pyo3_alloc);
            }
            (*type_ptr).tp_alloc = Some(slots.tracking_alloc);
        } else {
            (*type_ptr).tp_alloc = Some(slots.alloc);
            (*type_ptr).tp_free = Some(slots.free);
        }
    }
    Ok(())
}
