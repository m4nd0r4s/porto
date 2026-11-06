//! `porto.Semaphore`: permits acquired and released several at a time, granted in FIFO order
//! (16-sync-primitives-and-channels.md §1).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::{MAX_PERMITS, Semaphore as CoreSemaphore, WaitNode};
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;

use super::blocking;
use super::entry::Enterable;
use super::sync_entry::SyncEntry;
use super::wait_op::{Grant, SyncWait};
use crate::api::time::require_lane;
use crate::driver::ready::ready_none;
use crate::driver::step::checkpoint_if_needed_none;
use crate::driver::thin::{WaitSource, WaitTarget, Waitable};
use crate::raw_methods::{self, MethodDef};

/// The semaphore, waited on one permit at a time (`async with`, `acquire()`,
/// `CapacityLimiter`); a wait for more is a [`ManyPermits`].
pub(super) struct SemaphoreSource(pub(super) CoreSemaphore);

// SAFETY: forwards to the semaphore's node API, whose contract is `WaitSource`'s. A ready poll
// means the waiting task holds one permit.
unsafe impl WaitSource for SemaphoreSource {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.0.poll_acquire_node(node, 1, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.0.cancel_acquire_node(node) }
    }
}

impl SemaphoreSource {
    /// A semaphore with `permits` free permits, to wait on.
    pub(super) fn waitable(permits: usize) -> Arc<Waitable<Self>> {
        Waitable::new(Self(CoreSemaphore::new(permits)))
    }
}

impl Enterable for Arc<Waitable<SemaphoreSource>> {
    fn try_take(&self) -> bool {
        self.0.try_acquire_detached(1)
    }

    fn target(&self) -> WaitTarget {
        Waitable::target(self)
    }
}

/// One wait for several permits: the count the task's node waits for.
pub(super) struct ManyPermits {
    semaphore: Arc<Waitable<SemaphoreSource>>,
    permits: usize,
}

// SAFETY: as for `SemaphoreSource`; every poll of one wait passes the same count.
unsafe impl WaitSource for ManyPermits {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.semaphore.0.poll_acquire_node(node, self.permits, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.semaphore.0.cancel_acquire_node(node) }
    }
}

/// The target of a wait for `permits` permits: the semaphore itself for one, else a wait of
/// its own (one allocation, only when the task has to wait).
pub(super) fn permits_target(
    semaphore: &Arc<Waitable<SemaphoreSource>>,
    permits: usize,
) -> WaitTarget {
    if permits == 1 {
        return semaphore.target();
    }
    Waitable::new(ManyPermits {
        semaphore: Arc::clone(semaphore),
        permits,
    })
    .target()
}

/// `porto.Semaphore(value)`: `value` permits, acquired and released several at a time.
/// Waiters are granted in arrival order: one that wants more permits than are free blocks the
/// ones behind it. Bounded: releasing more than was acquired raises `ValueError`.
#[pyclass(frozen, module = "porto", name = "Semaphore")]
pub(super) struct Semaphore {
    pub(super) semaphore: Arc<Waitable<SemaphoreSource>>,
    /// The permits it was made with: no acquire can want more.
    value: usize,
}

#[pymethods]
impl Semaphore {
    #[new]
    #[pyo3(signature = (value, /))]
    fn new(value: i64) -> PyResult<Self> {
        let permits = usize::try_from(value)
            .ok()
            .filter(|&permits| (1..=MAX_PERMITS).contains(&permits))
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "porto: Semaphore({value}) needs 1 to {MAX_PERMITS} permits. A porto \
                     semaphore is bounded (releasing more than was acquired raises), so a \
                     Semaphore(0) could never be acquired; to signal between tasks, use \
                     porto.Event or porto.Notify"
                ))
            })?;
        Ok(Self {
            semaphore: SemaphoreSource::waitable(permits),
            value: permits,
        })
    }

    /// An awaitable that completes once the task holds `permits` permits: without suspending
    /// if nobody waits and enough are free. Nothing happens until it is awaited; the permits are
    /// only taken in the step that resumes the task with them.
    #[pyo3(signature = (permits = 1, /))]
    fn acquire(&self, py: Python<'_>, permits: i64) -> PyResult<Py<SyncWait>> {
        let permits = self.count("acquire", permits)?;
        require_lane("Semaphore.acquire")?;
        SyncWait::new(Grant::Permits(Arc::clone(&self.semaphore), permits)).into_object(py)
    }

    /// Takes `permits` permits if nobody waits and enough are free right now; returns whether
    /// it did. Never waits.
    #[pyo3(signature = (permits = 1, /))]
    fn try_acquire(&self, permits: i64) -> PyResult<bool> {
        let permits = self.count("try_acquire", permits)?;
        Ok(self.semaphore.0.try_acquire_detached(permits))
    }

    /// Gives back `permits` acquired permits, granting the tasks that waited longest. Raises
    /// `ValueError` if fewer are acquired.
    #[pyo3(signature = (permits = 1, /))]
    fn release(&self, permits: i64) -> PyResult<()> {
        let permits = self.count("release", permits)?;
        release(&self.semaphore, permits)
    }

    /// The free permits.
    #[getter]
    fn available(&self) -> usize {
        self.semaphore.0.available_permits()
    }

    /// Blocks the calling thread until it holds `permits` permits; give them back with
    /// `release()`. Only for threads that do not run porto: on a lane thread it raises
    /// `RuntimeError`.
    #[pyo3(signature = (permits = 1, /))]
    fn blocking_acquire(&self, py: Python<'_>, permits: i64) -> PyResult<()> {
        let permits = self.count("blocking_acquire", permits)?;
        blocking(py, "Semaphore.blocking_acquire", || {
            self.semaphore
                .0
                .blocking_acquire(permits)
                .map(|permit| permit.forget())
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "<porto.Semaphore available={}/{}>",
            self.semaphore.0.available_permits(),
            self.value
        )
    }
}

impl Semaphore {
    /// `permits` as a count for `method`: at least 1 and at most the semaphore's value (more
    /// could never be granted).
    fn count(&self, method: &str, permits: i64) -> PyResult<usize> {
        usize::try_from(permits)
            .ok()
            .filter(|&permits| (1..=self.value).contains(&permits))
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "porto: Semaphore.{method}({permits}) on a Semaphore({}): the count must be \
                     from 1 to {} (no task can hold more permits than the semaphore has)",
                    self.value, self.value
                ))
            })
    }
}

/// Gives `permits` back; `ValueError` if fewer are acquired.
pub(super) fn release(semaphore: &SemaphoreSource, permits: usize) -> PyResult<()> {
    if semaphore.0.release(permits) {
        return Ok(());
    }
    Err(PyValueError::new_err(format!(
        "porto: Semaphore.release({permits}) gives back more permits than are acquired ({}); \
         release once per acquire, with the same count (`async with semaphore:` does it for \
         you)",
        semaphore.0.acquired_permits()
    )))
}

/// `Semaphore.__aenter__` and `Semaphore.__aexit__`, as raw methods (`raw_methods`).
pub(super) fn install_raw_methods(py: Python<'_>) -> PyResult<()> {
    raw_methods::install::<Semaphore>(py, &SEMAPHORE_AENTER)?;
    raw_methods::install::<Semaphore>(py, &SEMAPHORE_AEXIT)
}

static SEMAPHORE_AENTER: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"__aenter__".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: semaphore_aenter,
    },
    ml_flags: ffi::METH_NOARGS,
    ml_doc: c"__aenter__($self, /)\n--\n\nWaits for one permit: `async with semaphore:`.".as_ptr(),
});

static SEMAPHORE_AEXIT: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"__aexit__".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunctionFast: semaphore_aexit,
    },
    ml_flags: ffi::METH_FASTCALL,
    ml_doc: c"__aexit__($self, exc_type, exc, traceback, /)\n--\n\nGives the permit back.".as_ptr(),
});

/// `Semaphore.__aenter__` (`METH_NOARGS`): a lazy entry (`sync_entry`) that takes one permit.
///
/// # Safety
/// CPython calls it as a `METH_NOARGS` method of `Semaphore`, attached: `slf` is a live
/// `Semaphore`.
unsafe extern "C" fn semaphore_aenter(
    slf: *mut ffi::PyObject,
    _args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    raw_methods::guard(|py| {
        // SAFETY: the caller's contract (the method descriptor checked the type).
        let semaphore = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<Semaphore>() };
        Ok(SyncEntry::permit(&semaphore)?.into_any())
    })
}

/// `Semaphore.__aexit__(exc_type, exc, traceback)` (`METH_FASTCALL`): gives the permit back,
/// then the exit's checkpoint (T2), unless an exception leaves the block.
///
/// # Safety
/// CPython calls it as a `METH_FASTCALL` method of `Semaphore`, attached: `slf` is a live
/// `Semaphore` and `args` points at `nargs` live objects.
unsafe extern "C" fn semaphore_aexit(
    slf: *mut ffi::PyObject,
    args: *mut *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
) -> *mut ffi::PyObject {
    raw_methods::guard(|py| {
        // SAFETY: the caller's contract.
        let [_, exc, _] =
            unsafe { raw_methods::aexit_args(py, c"Semaphore.__aexit__", args, nargs)? };
        // SAFETY: the caller's contract (the method descriptor checked the type).
        let semaphore = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<Semaphore>() };
        release(&semaphore.get().semaphore, 1)?;
        exit_checkpoint(py, &exc)
    })
}

/// The checkpoint at the end of an `async with` block (T2), skipped while an exception leaves
/// it: a `Cancelled` raised there would replace (and a cancelled scope absorb) the original.
pub(super) fn exit_checkpoint(py: Python<'_>, exc: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if !exc.is_none() {
        return ready_none(py);
    }
    checkpoint_if_needed_none(py)
}
