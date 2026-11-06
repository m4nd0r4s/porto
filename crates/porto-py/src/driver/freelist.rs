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
//! `tp_dealloc` stopped that), so a reference cycle through one would never be collected. The
//! freelist cannot track it again itself: `tp_alloc` returns before the new value is written,
//! and the body still holds the old one, whose `Py` fields were released when it was dropped.
//! A debug CPython's `PyObject_GC_Track` traverses the object at once to validate it, so it
//! would follow those dangling pointers (porto-txa). Neither freelist tracks a reused object;
//! whoever writes the value tracks it afterwards: `FastNew::object` (`fast_new.rs`) for the
//! classes it builds, [`new_tracked`] for the others. Every class with a `__traverse__` that
//! uses a pyo3 freelist (`_SyncWait`, `MutexGuard`, `_ScopeExit`) MUST be built through one of
//! the two (`tests/python/test_gc_cycles.py` checks each). A class without one (`_Trap`,
//! `_Ready`, `_ChannelOp`) holds nothing the collector could follow, so it is not tracked again:
//! that would only cost time on the hottest allocations (2 ns of a 60 ns `checkpoint`).
//!
//! Only the storage changes: pyo3 still runs `Drop` (so a trap freed unawaited still warns,
//! `unawaited.rs`) and releases the type reference, exactly as with its own freelist. The same
//! reasoning as for `am_send` (`async_slots`) applies to writing the slots at module init.

use core::cell::UnsafeCell;
use core::ffi::c_void;

use pyo3::PyClass;
use pyo3::exceptions::PyImportError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pyclass_init::PyClassInitializer;
use pyo3::type_object::PyTypeInfo;

/// Freed objects of one type, at most `N`; only reached while attached on a GIL build.
pub(crate) struct FreeList<const N: usize> {
    slots: UnsafeCell<([*mut ffi::PyObject; N], usize)>,
}

// SAFETY: only `alloc` and `free` reach the cell, and CPython calls them as slots, attached;
// on a GIL build one thread is attached at a time, so the accesses never overlap. The module
// installs them only on GIL builds (`install`).
unsafe impl<const N: usize> Sync for FreeList<N> {}

impl<const N: usize> FreeList<N> {
    /// An empty freelist.
    pub(crate) const fn new() -> Self {
        Self {
            slots: UnsafeCell::new(([core::ptr::null_mut(); N], 0)),
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
                // stopped the collector tracking it, and it stays untracked until its value is
                // written (see the module docs).
                return unsafe { ffi::PyObject_Init(object, subtype) };
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

/// `value` as a new `T` object, tracked by the collector once the value is written, whether it
/// came from a freelist or not (see the module docs). For a class with a `__traverse__` and a
/// freelist that [`FastNew`](super::fast_new::FastNew) does not build.
pub(crate) fn new_tracked<T: PyClass>(
    py: Python<'_>,
    value: impl Into<PyClassInitializer<T>>,
) -> PyResult<Py<T>> {
    let object = Py::new(py, value)?;
    // SAFETY: attached; `object` is a new `T` object with its value written, which only this
    // thread can reach yet.
    unsafe { track_reused(object.as_ptr()) };
    Ok(object)
}

/// Tracks `object` by the collector if its type is one the collector traverses and `object` is
/// not tracked yet, because it came from a freelist (see the module docs).
///
/// # Safety
/// Attached; `object` is live, its value fully written, and no other thread can reach it yet.
pub(crate) unsafe fn track_reused(object: *mut ffi::PyObject) {
    // SAFETY: the caller's contract: a debug build's traversal sees only the new value, and
    // nothing races with the tracking.
    unsafe {
        if ffi::PyType_IS_GC(ffi::Py_TYPE(object)) != 0 && ffi::PyObject_GC_IsTracked(object) == 0 {
            ffi::PyObject_GC_Track(object.cast());
        }
    }
}

/// The slots [`install`] puts on one type, made by [`gil_freelist!`].
pub(crate) struct Slots {
    /// GIL build: `tp_alloc` over the [`FreeList`].
    pub(crate) alloc: ffi::allocfunc,
    /// GIL build: `tp_free` over the [`FreeList`].
    pub(crate) free: ffi::freefunc,
}

/// Defines module `$name` with a [`FreeList`] of `$capacity`, its `alloc` and `free` slots, and
/// `SLOTS` for [`install`].
macro_rules! gil_freelist {
    ($name:ident, $capacity:expr) => {
        pub(crate) mod $name {
            use pyo3::ffi;

            static LIST: $crate::driver::freelist::FreeList<$capacity> =
                $crate::driver::freelist::FreeList::new();

            /// What `freelist::install` puts on the type.
            pub(crate) static SLOTS: $crate::driver::freelist::Slots =
                $crate::driver::freelist::Slots { alloc, free };

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
/// `tp_alloc` and `tp_free` on a GIL build; a free-threaded build keeps pyo3's freelist. Call it
/// once, from module init, right after `add_class::<T>`, before any instance exists; `T` MUST be
/// final.
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
        if !cfg!(Py_GIL_DISABLED) {
            (*type_ptr).tp_alloc = Some(slots.alloc);
            (*type_ptr).tp_free = Some(slots.free);
        }
    }
    Ok(())
}
