//! Raising `StopIteration(value)` from a raw `tp_iternext` without allocating an exception per
//! `await` (porto-itr.7).
//!
//! From 3.12 the `SEND` opcode resumes a non-generator awaitable through `tp_iternext`, so an
//! awaitable that finishes with a value other than `None` must raise `StopIteration(value)`;
//! the frame takes the value out of it and drops it at once. Creating one costs an exception
//! object and an argument tuple, a third of an uncontended `async with mutex as guard:`.
//!
//! On a GIL build, porto keeps one spare `StopIteration` and raises it again whenever nobody
//! else holds it (its reference count is the spare's own): its value, arguments and the state a
//! raise leaves (traceback, context, cause, notes, attributes) are reset first, so it is
//! indistinguishable from a fresh `StopIteration(value)`. The GIL serializes every use, and a
//! count of one means no other reference exists to observe the reset. Free-threaded builds,
//! where another thread could take a reference between the check and the reset, always
//! allocate, and so does 3.11, where `SEND` goes through `am_send` and never needs one.

use pyo3::ffi;
use pyo3::prelude::*;

/// Raises `StopIteration(value)`: sets it as the current exception.
pub(crate) fn raise_stop_iteration(value: &Bound<'_, PyAny>) {
    #[cfg(all(Py_3_12, not(Py_GIL_DISABLED)))]
    {
        // SAFETY: attached (we hold a `Bound`), on a GIL build: see the module docs.
        if unsafe { spare::raise_again(value.as_ptr()) } {
            return;
        }
    }
    raise_new(value);
}

/// Raises a fresh `StopIteration(value)`.
fn raise_new(value: &Bound<'_, PyAny>) {
    // SAFETY: attached; `StopIteration` and `value` are live. A new reference, or NULL with an
    // exception set (which is then the one raised, a `MemoryError`).
    let exception = unsafe { ffi::PyObject_CallOneArg(ffi::PyExc_StopIteration, value.as_ptr()) };
    if exception.is_null() {
        return;
    }
    #[cfg(all(Py_3_12, not(Py_GIL_DISABLED)))]
    // SAFETY: attached, on a GIL build; `exception` is a live `StopIteration`.
    unsafe {
        spare::keep(exception);
    }
    // SAFETY: attached; steals the new reference.
    #[cfg(Py_3_12)]
    unsafe {
        ffi::PyErr_SetRaisedException(exception);
    }
    // SAFETY: attached; `exception` is a live instance, raised as itself; the reference is ours.
    #[cfg(not(Py_3_12))]
    unsafe {
        ffi::PyErr_SetObject(ffi::PyExc_StopIteration, exception);
        ffi::Py_DECREF(exception);
    }
}

#[cfg(all(Py_3_12, not(Py_GIL_DISABLED)))]
mod spare {
    use core::ptr;
    use core::sync::atomic::{AtomicPtr, Ordering};

    use pyo3::ffi;

    /// The spare `StopIteration` (one strong reference), or null. Only touched while attached,
    /// which on a GIL build means one thread at a time; the atomic only keeps Rust's rules.
    static SPARE: AtomicPtr<ffi::PyObject> = AtomicPtr::new(ptr::null_mut());

    /// Makes `exception` the spare. The previous one, which someone else still held (else it
    /// would have been raised again), is let go: a spare caught in a reference cycle must not
    /// keep every later raise allocating until the collector runs.
    ///
    /// # Safety
    /// Attached, on a GIL build; `exception` is a live `StopIteration`.
    pub(super) unsafe fn keep(exception: *mut ffi::PyObject) {
        // SAFETY: the caller's contract; the spare owns this new reference.
        unsafe { ffi::Py_INCREF(exception) };
        let previous = SPARE.swap(exception, Ordering::Relaxed);
        // SAFETY: the reference the spare owned; released last, as it can run code.
        unsafe { ffi::Py_XDECREF(previous) };
    }

    /// Raises the spare as `StopIteration(value)` if nobody else holds it; returns whether it
    /// did.
    ///
    /// # Safety
    /// Attached, on a GIL build; `value` is live.
    pub(super) unsafe fn raise_again(value: *mut ffi::PyObject) -> bool {
        let spare = SPARE.load(Ordering::Relaxed);
        // SAFETY: the spare is live (it owns a reference); reading its count is a plain load.
        if spare.is_null() || unsafe { ffi::Py_REFCNT(spare) } != 1 {
            return false;
        }
        let exception = spare.cast::<ffi::PyStopIterationObject>();
        // SAFETY: the spare is a `StopIteration` made by `StopIteration(value)`; its fields are
        // readable while it lives.
        let args = unsafe { (*exception).args };
        // SAFETY: `args` is live when non-null (the spare owns it).
        let args_reusable = !args.is_null()
            && unsafe { ffi::PyTuple_CheckExact(args) } != 0
            && unsafe { ffi::Py_REFCNT(args) } == 1
            && unsafe { ffi::PyTuple_GET_SIZE(args) } == 1;
        if !args_reusable {
            return false;
        }
        // SAFETY: attached; nobody else references the spare or its tuple (count one), and the
        // reference taken first keeps it so while the old values are released (which can run
        // code that raises again: that gets a fresh exception). Every field written holds a
        // new reference, and every value replaced is released after the writes.
        unsafe {
            ffi::Py_INCREF(spare);
            ffi::Py_INCREF(value);
            ffi::Py_INCREF(value);
            let tuple_item = (*args.cast::<ffi::PyTupleObject>()).ob_item.as_mut_ptr();
            let released = [
                ptr::replace(tuple_item, value),
                ptr::replace(&raw mut (*exception).value, value),
                ptr::replace(&raw mut (*exception).dict, ptr::null_mut()),
                ptr::replace(&raw mut (*exception).notes, ptr::null_mut()),
                ptr::replace(&raw mut (*exception).traceback, ptr::null_mut()),
                ptr::replace(&raw mut (*exception).context, ptr::null_mut()),
                ptr::replace(&raw mut (*exception).cause, ptr::null_mut()),
            ];
            // A C `char` (pyo3-ffi spells it as a Rust `char`): write the one byte.
            (&raw mut (*exception).suppress_context)
                .cast::<u8>()
                .write(0);
            for object in released {
                ffi::Py_XDECREF(object);
            }
            // Steals the reference taken above.
            ffi::PyErr_SetRaisedException(spare);
        }
        true
    }
}
