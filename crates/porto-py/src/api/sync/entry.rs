//! The lazy entry of `async with lock:` and `async with mutex as guard:` (porto-szb.115,
//! porto-szb.118), and of the other primitives' `async with` (a semaphore's permit, a
//! limiter's token, a read or write lock: `sync_entry`): one small frozen object per entry,
//! its own awaitable.
//!
//! It follows `_SyncWait`'s rules (`wait_op`) exactly: nothing is taken until it is awaited, the
//! first resume spends one unit of the task's budget (T2, `Cancelled` first if the task is
//! cancelled) and takes a free lock without suspending, a held lock is waited for with a
//! `Notified` trap on the task's embedded wait node, and an entry dropped without ever being
//! resumed warns. What it saves on the hottest path of the M1 sync gates is everything around
//! that:
//!
//! - its state is one atomic byte ([`Entry`]), so the class is frozen: no borrow flag, and no
//!   `Grant` holding its own `Arc` of the lock (two atomic counts per entry). The lock is
//!   reached through the object being entered (an [`Enterable`]), held by a [`Strong`];
//! - for a `Mutex`, the entry is the guard: the await evaluates to the entry itself, so an
//!   entry is one object, not a wait and then a guard;
//! - its dealloc is a raw slot ([`install_dealloc`]), which skips pyo3's trampoline.

use core::mem::ManuallyDrop;
use core::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use pyo3::PyClass;
use pyo3::exceptions::{PyImportError, PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pyclass_init::PyClassInitializer;
use pyo3::type_object::PyTypeInfo;

use super::lock::LockSource;
use crate::api::time::require_lane;
use crate::driver::fast_new::{FastNew, Unawaited};
use crate::driver::step;
use crate::driver::thin::{WaitTarget, Waitable};
use crate::driver::trap::{Trap, TrapKind};

/// What an entry takes and how: a lock, a permit, a limiter token, a read or write lock.
pub(super) trait Enterable {
    /// Runs on the first resume, after the T2 check and before any attempt: an error that makes
    /// the wait pointless (a borrower that already holds a token). Takes nothing.
    fn check(&self, py: Python<'_>) -> PyResult<()> {
        let _ = py;
        Ok(())
    }

    /// Takes the grant if it is available now, without waiting.
    fn try_take(&self) -> bool;

    /// The handle a waiting task parks on; its grant is what [`Enterable::try_take`] takes.
    fn target(&self) -> WaitTarget;

    /// Runs in the step that resumes the task with the grant, taken at once or handed over: it
    /// records the holder. If it fails, it gives the grant back and the await raises.
    fn taken(&self, py: Python<'_>) -> PyResult<()> {
        let _ = py;
        Ok(())
    }
}

impl Enterable for Arc<Waitable<LockSource>> {
    fn try_take(&self) -> bool {
        self.try_lock()
    }

    fn target(&self) -> WaitTarget {
        Waitable::target(self)
    }
}

/// A strong reference to an object, released with a plain `Py_DECREF`.
///
/// `Py<T>`'s drop asks pyo3 whether the thread is attached (a thread local, a function call on
/// macOS) and defers the decrement to a pool if pyo3 does not know. A `Strong` is only dropped
/// while attached: it lives in an entry object, which CPython frees attached.
pub(super) struct Strong<T>(ManuallyDrop<Py<T>>);

impl<T> Strong<T> {
    /// A new reference to `object`.
    pub(super) fn new(object: &Bound<'_, T>) -> Self {
        Self(ManuallyDrop::new(object.clone().unbind()))
    }

    /// The reference.
    pub(super) fn py(&self) -> &Py<T> {
        &self.0
    }
}

impl<T> Drop for Strong<T> {
    fn drop(&mut self) {
        // SAFETY: only dropped while attached (see the type docs), and the reference is ours:
        // the `Py` is never dropped itself.
        unsafe { ffi::Py_DECREF(self.0.as_ptr()) };
    }
}

/// Never resumed: dropping it warns.
const FRESH: u8 = 0;
/// Resumed, and yielded because the budget was spent: the next resume tries again.
const TRYING: u8 = 1;
/// Yielded a `Notified` trap: the driver resumes it once the lock was handed to the task.
const WAITING: u8 = 2;
/// The lock was taken and the await finished.
const ENTERED: u8 = 3;
/// Thrown into, closed, or ended by an error.
const DONE: u8 = 4;
/// A resume is running (another one at the same time is refused).
const BUSY: u8 = 5;

/// Where an entry is: one of the phases above.
pub(super) struct Entry {
    phase: AtomicU8,
}

/// What one resume of an entry does.
pub(super) enum Step {
    /// Suspend, yielding this trap to the driver.
    Yield(Py<PyAny>),
    /// The task now holds the lock: the await finishes.
    Entered,
}

impl Entry {
    /// A fresh entry.
    pub(super) const fn new() -> Self {
        Self {
            phase: AtomicU8::new(FRESH),
        }
    }

    /// Whether it was never resumed, thrown into or closed: dropping it means the `await` was
    /// forgotten.
    pub(super) fn never_awaited(&self) -> bool {
        self.phase.load(Ordering::Relaxed) == FRESH
    }

    /// Ends it without taking anything (`throw`, `close`). The driver gave a wait up before
    /// throwing in (T3), so nothing is held.
    pub(super) fn finish(&self) {
        self.phase.store(DONE, Ordering::Relaxed);
    }

    /// Ends an entered entry, for the `__aexit__` of an entry object that is its own context
    /// manager (`RwLock.read()`); `false`, changing nothing, if it is not entered.
    pub(super) fn exit(&self) -> bool {
        self.phase
            .compare_exchange(ENTERED, DONE, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Resumes the entry of `lock` with `sent` (`None` from `__next__`), as `_SyncWait` does.
    /// `call` names the method that made it (`Lock.__aenter__`), for the error if it is
    /// awaited outside a porto runtime.
    pub(super) fn resume(
        &self,
        py: Python<'_>,
        lock: &impl Enterable,
        sent: &Bound<'_, PyAny>,
        call: &str,
    ) -> PyResult<Step> {
        let phase = self.take_phase();
        let (next, step) = match phase {
            BUSY => return Err(concurrent_resume_error()),
            FRESH if !sent.is_none() => (FRESH, Err(not_started_error())),
            FRESH | TRYING => match proceed(py, call).and_then(|go| Ok((go, lock.check(py)?))) {
                Err(error) => (DONE, Err(error)),
                // Take nothing before the schedule point: a task closed there holds nothing.
                Ok((false, ())) => (TRYING, suspend(py, TrapKind::Yield)),
                Ok((true, ())) if lock.try_take() => entered(py, lock),
                Ok((true, ())) => (WAITING, suspend(py, TrapKind::Notified(lock.target()))),
            },
            // The driver resumes a `Notified` wait only once the lock was handed over.
            WAITING => entered(py, lock),
            finished => (finished, Err(reused_error())),
        };
        self.phase.store(next, Ordering::Release);
        step
    }

    /// The phase, leaving [`BUSY`] in its place so a resume from another thread at the same
    /// time is refused. On a GIL build only one thread is attached at a time and a resume runs
    /// no Python code, so no other resume can start before the phase is stored back: a plain
    /// load and store do.
    fn take_phase(&self) -> u8 {
        if cfg!(Py_GIL_DISABLED) {
            self.phase.swap(BUSY, Ordering::Acquire)
        } else {
            let phase = self.phase.load(Ordering::Acquire);
            self.phase.store(BUSY, Ordering::Relaxed);
            phase
        }
    }
}

/// The grant is taken: record it ([`Enterable::taken`]) and finish the await.
fn entered(py: Python<'_>, lock: &impl Enterable) -> (u8, PyResult<Step>) {
    match lock.taken(py) {
        Ok(()) => (ENTERED, Ok(Step::Entered)),
        Err(error) => (DONE, Err(error)),
    }
}

/// T2 for the first resume (`step::proceed`), which is also where an entry finds out whether it
/// runs in a porto task: outside one, it must at least be on a porto lane (a manual driver),
/// else it raises `NoRuntimeError` naming `call`, as a call outside `porto.run()` does.
fn proceed(py: Python<'_>, call: &str) -> PyResult<bool> {
    match step::proceed_in_task(py)? {
        Some(proceed) => Ok(proceed),
        None => require_lane(call).map(|()| true),
    }
}

/// Yields a trap for `kind` to the driver.
fn suspend(py: Python<'_>, kind: TrapKind) -> PyResult<Step> {
    Ok(Step::Yield(Py::new(py, Trap::suspended(kind))?.into_any()))
}

fn not_started_error() -> PyErr {
    PyTypeError::new_err(
        "can't send a non-None value to a porto `async with` entry that has not started; the \
         first resume of an awaitable must be send(None)",
    )
}

fn reused_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: an `async with` entry of a porto primitive (a Lock, Mutex, Semaphore, \
         CapacityLimiter or RwLock) was awaited again after it finished; make a new entry for \
         each `async with` (the statement does it for you)",
    )
}

/// Two callers resumed the same entry at once.
fn concurrent_resume_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: an `async with` entry of a porto primitive was resumed from two places at \
         once; await each entry from one task only",
    )
}

/// Replaces `T`'s `tp_dealloc` with [`dealloc::<T>`] if `fast_new` found the layout it relies
/// on (the value right after the header, nothing after it to drop); else pyo3's stays, as on a
/// free-threaded build. Call it once from module init, after `fast_new.init` and before any
/// instance exists; `T` MUST be a final, frozen pyclass with no `__dict__` or `__weakref__`.
pub(super) fn install_dealloc<T>(py: Python<'_>, fast_new: &FastNew<T>) -> PyResult<()>
where
    T: PyClass + PyTypeInfo + Into<PyClassInitializer<T>> + Unawaited,
{
    if !fast_new.is_enabled() {
        return Ok(());
    }
    let type_object = T::type_object(py);
    let type_ptr = type_object.as_type_ptr();
    // SAFETY: a live heap type that nothing outside module init can reach yet (see
    // `async_slots`); no instance exists, so none was made for the slot being replaced.
    unsafe {
        if (*type_ptr).tp_free.is_none()
            || (*type_ptr).tp_dictoffset != 0
            || (*type_ptr).tp_weaklistoffset != 0
        {
            return Err(PyImportError::new_err(format!(
                "porto._core: {} cannot take a raw tp_dealloc; this is a porto bug, please \
                 report it",
                type_object.name()?
            )));
        }
        (*type_ptr).tp_dealloc = Some(dealloc::<T>);
    }
    Ok(())
}

/// `tp_dealloc` for a frozen pyclass `T`: what pyo3's does (stop tracking, drop the value, free
/// the memory, release the type), without its trampoline.
///
/// # Safety
/// CPython calls it as `tp_dealloc` of `T` (installed by [`install_dealloc`]), attached, with
/// `object` a `T` object whose last reference is gone.
unsafe extern "C" fn dealloc<T: PyClass>(object: *mut ffi::PyObject) {
    // A panic must not unwind into CPython; the entries' `Drop` does not panic.
    let abort = AbortOnUnwind;
    // SAFETY: the caller's contract. A frozen pyclass keeps its value right after the header
    // (at the offset `fast_new` checks) and nothing after it that needs dropping; the type's
    // `tp_free` matches its `tp_alloc`, and the object holds a reference to its heap type.
    unsafe {
        let type_ptr = ffi::Py_TYPE(object);
        if ffi::PyType_IS_GC(type_ptr) != 0 {
            ffi::PyObject_GC_UnTrack(object.cast());
        }
        let offset = size_of::<ffi::PyObject>().next_multiple_of(align_of::<T>());
        core::ptr::drop_in_place(object.byte_add(offset).cast::<T>());
        if let Some(free) = (*type_ptr).tp_free {
            free(object.cast());
        }
        ffi::Py_DECREF(type_ptr.cast());
    }
    core::mem::forget(abort);
}

/// Aborts the process if dropped during an unwind (see [`dealloc`]).
struct AbortOnUnwind;

impl Drop for AbortOnUnwind {
    fn drop(&mut self) {
        std::process::abort();
    }
}
