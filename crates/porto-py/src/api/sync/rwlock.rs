//! `porto.RwLock`: a writer-preferring read/write lock (16-sync-primitives-and-channels.md §1).
//!
//! `async with lock.read():` and `async with lock.write():` take it through a lazy entry
//! (`sync_entry`) that is its own context manager: `read()` and `write()` make it, `__aenter__`
//! returns it, the await takes the lock like `Lock`'s entry, and `__aexit__` releases it.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::{RwLock as CoreRwLock, RwLockReadGuard, RwLockWriteGuard, WaitNode};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::blocking;
use super::entry::Enterable;
use super::sync_entry::SyncEntry;
use super::wait_op::{Grant, SyncWait};
use crate::api::time::require_lane;
use crate::driver::thin::{WaitSource, WaitTarget, Waitable};

/// The lock, waited on for reading. Held without guards: a Python holder is not a Rust scope.
pub(super) struct ReadSource(pub(super) CoreRwLock<()>);

// SAFETY: forwards to the lock's node API, whose contract is `WaitSource`'s. A ready poll means
// the waiting task holds a read lock.
unsafe impl WaitSource for ReadSource {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.0.poll_read_node(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.0.cancel_node(node) }
    }
}

/// The same lock, waited on for writing.
pub(super) struct WriteSource(pub(super) Arc<Waitable<ReadSource>>);

// SAFETY: as for `ReadSource`; a ready poll means the waiting task holds the write lock.
unsafe impl WaitSource for WriteSource {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.0.0.poll_write_node(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.0.0.cancel_node(node) }
    }
}

impl Enterable for Arc<Waitable<ReadSource>> {
    fn try_take(&self) -> bool {
        self.0.try_read_detached()
    }

    fn target(&self) -> WaitTarget {
        Waitable::target(self)
    }
}

impl Enterable for Arc<Waitable<WriteSource>> {
    fn try_take(&self) -> bool {
        self.0.0.try_write_detached()
    }

    fn target(&self) -> WaitTarget {
        Waitable::target(self)
    }
}

/// `porto.RwLock()`: any number of readers, or one writer. Writer-preferring: a reader never
/// takes it while a writer waits. Waiters are served in arrival order (consecutive readers
/// together), so neither side starves. Not reentrant, and it does not record its holders.
#[pyclass(frozen, module = "porto", name = "RwLock")]
pub(super) struct RwLock {
    pub(super) read: Arc<Waitable<ReadSource>>,
    pub(super) write: Arc<Waitable<WriteSource>>,
}

#[pymethods]
impl RwLock {
    #[new]
    pub(super) fn new() -> Self {
        let read = Waitable::new(ReadSource(CoreRwLock::new(())));
        let write = Waitable::new(WriteSource(Arc::clone(&read)));
        Self { read, write }
    }

    /// `async with lock.read():` holds a read lock for the block.
    fn read(slf: &Bound<'_, Self>) -> PyResult<Py<SyncEntry>> {
        SyncEntry::read(slf)
    }

    /// `async with lock.write():` holds the write lock for the block.
    fn write(slf: &Bound<'_, Self>) -> PyResult<Py<SyncEntry>> {
        SyncEntry::write(slf)
    }

    /// An awaitable that completes once the task holds a read lock; release it with
    /// `release_read()`.
    fn acquire_read(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("RwLock.acquire_read")?;
        SyncWait::new(Grant::Read(Arc::clone(&self.read))).into_object(py)
    }

    /// An awaitable that completes once the task holds the write lock; release it with
    /// `release_write()`.
    fn acquire_write(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("RwLock.acquire_write")?;
        SyncWait::new(Grant::Write(Arc::clone(&self.write))).into_object(py)
    }

    /// Takes a read lock if no writer holds or waits for it right now; returns whether it did.
    fn try_acquire_read(&self) -> bool {
        self.read.0.try_read_detached()
    }

    /// Takes the write lock if it is free and nobody waits right now; returns whether it did.
    fn try_acquire_write(&self) -> bool {
        self.read.0.try_write_detached()
    }

    /// Releases a read lock. Raises `RuntimeError` if no reader holds it.
    fn release_read(&self) -> PyResult<()> {
        release_read(&self.read)
    }

    /// Releases the write lock. Raises `RuntimeError` if no writer holds it.
    fn release_write(&self) -> PyResult<()> {
        release_write(&self.read)
    }

    /// How many readers hold the lock.
    #[getter]
    fn readers(&self) -> usize {
        self.read.0.readers()
    }

    /// Whether a writer holds the lock (or is being handed it).
    fn write_locked(&self) -> bool {
        self.read.0.is_write_locked()
    }

    /// Blocks the calling thread until it holds a read lock. Only for threads that do not run
    /// porto: on a lane thread it raises `RuntimeError`.
    fn blocking_acquire_read(&self, py: Python<'_>) -> PyResult<()> {
        blocking(py, "RwLock.blocking_acquire_read", || {
            self.read.0.blocking_read().map(RwLockReadGuard::leak)
        })
    }

    /// Blocks the calling thread until it holds the write lock. Only for threads that do not
    /// run porto.
    fn blocking_acquire_write(&self, py: Python<'_>) -> PyResult<()> {
        blocking(py, "RwLock.blocking_acquire_write", || {
            self.read.0.blocking_write().map(RwLockWriteGuard::leak)
        })
    }

    fn __repr__(&self) -> String {
        if self.read.0.is_write_locked() {
            return "<porto.RwLock write-locked>".to_owned();
        }
        match self.read.0.readers() {
            0 => "<porto.RwLock unlocked>".to_owned(),
            readers => format!("<porto.RwLock read-locked by {readers}>"),
        }
    }
}

/// Releases a read lock; `RuntimeError` if no reader holds it.
pub(super) fn release_read(lock: &ReadSource) -> PyResult<()> {
    // SAFETY: the value is `()`: no reference to it outlives the lock.
    if unsafe { lock.0.force_read_unlock() } {
        return Ok(());
    }
    Err(PyRuntimeError::new_err(
        "porto: RwLock.release_read() was called while no reader holds the lock; release once \
         per acquire_read() (`async with lock.read():` does it for you)",
    ))
}

/// Releases the write lock; `RuntimeError` if no writer holds it.
pub(super) fn release_write(lock: &ReadSource) -> PyResult<()> {
    // SAFETY: as above.
    if unsafe { lock.0.force_write_unlock() } {
        return Ok(());
    }
    Err(PyRuntimeError::new_err(
        "porto: RwLock.release_write() was called while no writer holds the lock; release once \
         per acquire_write() (`async with lock.write():` does it for you)",
    ))
}
