//! `porto.Condition`: tasks wait, with a `porto.Lock` released, until notified
//! (16-sync-primitives-and-channels.md §1).
//!
//! `wait()` and `wait_for(predicate)` return a lazy `_ConditionWait`. Its first resume (T2:
//! `Cancelled` first if the task is cancelled) checks that the lock is held and yields a
//! `Notified` trap on a wait of its own (`Released`): the driver's poll queues the task's node
//! on the condvar and only then releases the lock, so a notification sent after the release is
//! never missed. Once notified, it takes the lock back; if the lock is held, it waits for it
//! with a `NotifiedShielded` trap, which a cancellation does not give up: the lock is always
//! held again when the await ends, normally or with an exception (a `Cancelled` thrown into
//! the notification wait included). A `Cancelled` that arrives before the lock was released
//! leaves at once: the lock was never let go.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use porto_core::sync::{Condvar, WaitNode};
use pyo3::PyTraverseError;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::gc::PyVisit;
use pyo3::prelude::*;

use super::lock::{Lock, LockEntry, LockLost, LockSource, release, release_then_checkpoint};
use super::wait_op::{Grant, SyncWait, Then};
use crate::api::time::require_lane;
use crate::async_slots::stop_iteration;
use crate::driver::step;
use crate::driver::thin::{WaitSource, Waitable};
use crate::driver::trap::{Trap, TrapKind};
use crate::throw_args::thrown_exception;

/// One notification wait: queues the task on the condvar, then releases the lock.
pub(super) struct Released {
    condvar: Arc<Condvar>,
    lock: Arc<Waitable<LockSource>>,
    /// Whether the lock was released, which the wait must undo when it ends.
    released: AtomicBool,
}

// SAFETY: forwards to the condvar's node API, whose contract is `WaitSource`'s.
unsafe impl WaitSource for Released {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        let queuing = !node.is_pending();
        // SAFETY: the caller's contract.
        let poll = unsafe { self.condvar.poll_wait_node(node, cx) };
        if queuing {
            // Queued: a notification from now on reaches the task, so the lock can go.
            self.released.store(true, Ordering::Relaxed);
            self.lock.unlock();
        }
        poll
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.condvar.cancel_wait_node(node) }
    }
}

/// `porto.Condition(lock=None)`: a `porto.Lock` (a new one by default) plus a queue of tasks
/// waiting for a notification. Use it as the lock (`async with condition:`) and `await
/// condition.wait()` inside.
#[pyclass(frozen, module = "porto", name = "Condition")]
pub(super) struct Condition {
    lock: Py<Lock>,
    condvar: Arc<Condvar>,
}

impl Condition {
    fn lock_source(&self) -> &Arc<Waitable<LockSource>> {
        &self.lock.get().lock
    }

    fn require_held(&self, method: &str) -> PyResult<()> {
        if self.lock_source().is_locked() {
            return Ok(());
        }
        Err(PyRuntimeError::new_err(format!(
            "porto: Condition.{method}() needs the condition's lock held; call it inside \
             `async with condition:`"
        )))
    }

    fn wait_object(
        slf: &Bound<'_, Self>,
        predicate: Option<Py<PyAny>>,
        call: &'static str,
    ) -> PyResult<Py<ConditionWait>> {
        require_lane(call.trim_end_matches("()"))?;
        Py::new(
            slf.py(),
            ConditionWait {
                condition: slf.clone().unbind(),
                predicate,
                state: State::Fresh,
                started: false,
                waited: false,
                call,
            },
        )
    }
}

#[pymethods]
impl Condition {
    #[new]
    #[pyo3(signature = (lock = None))]
    fn new(py: Python<'_>, lock: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let lock = match lock {
            None => Py::new(py, Lock::new())?,
            Some(lock) => lock
                .cast::<Lock>()
                .map_err(|_| {
                    PyTypeError::new_err(format!(
                        "porto: Condition(lock) takes a porto.Lock, got {}; pass a porto.Lock \
                         or nothing (the condition then makes its own)",
                        lock.get_type()
                            .name()
                            .map_or_else(|_| "?".into(), |n| n.to_string())
                    ))
                })?
                .clone()
                .unbind(),
        };
        Ok(Self {
            lock,
            condvar: Arc::new(Condvar::new()),
        })
    }

    /// `async with condition:` takes the lock, like `async with lock:`.
    fn __aenter__(&self, py: Python<'_>) -> PyResult<Py<LockEntry>> {
        Lock::enter(self.lock.bind(py))
    }

    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __aexit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        exc: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        release_then_checkpoint(py, self.lock_source(), exc)
    }

    /// An awaitable that completes once the task holds the lock.
    fn acquire(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("Condition.acquire")?;
        SyncWait::with(
            Grant::Locked(Arc::clone(self.lock_source())),
            Then::Nothing,
            "Condition.acquire()",
        )
        .into_object(py)
    }

    /// Takes the lock if it is free right now; returns whether it did.
    fn try_acquire(&self) -> bool {
        self.lock_source().try_lock()
    }

    /// Releases the lock. Raises `RuntimeError` if it is not held.
    fn release(&self) -> PyResult<()> {
        release(self.lock_source())
    }

    /// Whether the lock is held.
    fn locked(&self) -> bool {
        self.lock_source().is_locked()
    }

    /// The lock the condition uses.
    #[getter]
    fn lock(&self, py: Python<'_>) -> Py<Lock> {
        self.lock.clone_ref(py)
    }

    /// An awaitable that releases the lock, waits until notified, and takes the lock again,
    /// also when it is cancelled. Raises `RuntimeError` if the lock is not held.
    fn wait(slf: &Bound<'_, Self>) -> PyResult<Py<ConditionWait>> {
        Self::wait_object(slf, None, "Condition.wait()")
    }

    /// An awaitable that waits (as `wait()`) until `predicate()` is true, and evaluates to its
    /// result; it checks first, and again after each notification, with the lock held.
    #[pyo3(signature = (predicate, /))]
    fn wait_for(slf: &Bound<'_, Self>, predicate: Py<PyAny>) -> PyResult<Py<ConditionWait>> {
        Self::wait_object(slf, Some(predicate), "Condition.wait_for()")
    }

    /// Wakes up to `n` tasks waiting, longest-waiting first. Needs the lock held.
    #[pyo3(signature = (n = 1, /))]
    fn notify(&self, n: i64) -> PyResult<()> {
        let n = usize::try_from(n).map_err(|_| {
            PyValueError::new_err(format!(
                "porto: Condition.notify({n}): the number of tasks to wake cannot be negative"
            ))
        })?;
        self.require_held("notify")?;
        self.condvar.notify(n);
        Ok(())
    }

    /// Wakes every task waiting. Needs the lock held.
    fn notify_all(&self) -> PyResult<()> {
        self.require_held("notify_all")?;
        self.condvar.notify_all();
        Ok(())
    }

    fn __repr__(&self) -> String {
        let state = if self.locked() { "locked" } else { "unlocked" };
        format!(
            "<porto.Condition {state}, waiters={}>",
            self.condvar.waiters()
        )
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.lock)
    }
}

/// Where a `_ConditionWait` is.
enum State {
    /// Not started, or yielded for the budget before it started.
    Fresh,
    /// Waits for a notification.
    Waiting(Arc<Waitable<Released>>),
    /// Waits for the lock again; then raises this, if set.
    Relocking(Option<PyErr>),
    Done,
}

/// The awaitable of `Condition.wait()` and `Condition.wait_for()` (see the module docs).
#[pyclass(module = "porto._core", name = "_ConditionWait")]
pub(super) struct ConditionWait {
    condition: Py<Condition>,
    /// `wait_for()`'s predicate.
    predicate: Option<Py<PyAny>>,
    state: State,
    started: bool,
    /// Whether it waited for a notification at least once.
    waited: bool,
    call: &'static str,
}

/// What one resume does.
enum Stepped {
    Yield(Py<PyAny>),
    Return(Py<PyAny>),
}

impl ConditionWait {
    fn resume(&mut self, py: Python<'_>, sent: &Bound<'_, PyAny>) -> PyResult<Stepped> {
        if !self.started && !sent.is_none() {
            return Err(PyTypeError::new_err(
                "can't send a non-None value to a porto wait that has not started; the first \
                 resume of an awaitable must be send(None)",
            ));
        }
        self.started = true;
        match std::mem::replace(&mut self.state, State::Done) {
            State::Fresh => {
                if !step::proceed(py)? {
                    self.state = State::Fresh;
                    return suspend(py, TrapKind::Yield);
                }
                self.condition.get().require_held(
                    self.call
                        .trim_start_matches("Condition.")
                        .trim_end_matches("()"),
                )?;
                self.check_or_wait(py)
            }
            // The driver resumes a `Notified` wait only once it was notified.
            State::Waiting(_) => self.relock(py, None),
            State::Relocking(Some(error)) => Err(error),
            State::Relocking(None) => self.check_or_wait(py),
            State::Done => Err(PyRuntimeError::new_err(
                "porto: a porto wait was awaited again after it finished; call wait() or \
                 wait_for() again for each await",
            )),
        }
    }

    /// Takes the lock back, then raises `error` or goes on; waits for the lock (shielded) if
    /// it is held.
    fn relock(&mut self, py: Python<'_>, error: Option<PyErr>) -> PyResult<Stepped> {
        let lock = Arc::clone(self.condition.get().lock_source());
        if !lock.try_lock() {
            self.state = State::Relocking(error);
            return suspend(py, TrapKind::NotifiedShielded(lock.target()));
        }
        match error {
            Some(error) => Err(error),
            None => self.check_or_wait(py),
        }
    }

    /// With the lock held: done (a `wait()` that was notified, a true predicate), or a new
    /// notification wait.
    fn check_or_wait(&mut self, py: Python<'_>) -> PyResult<Stepped> {
        match &self.predicate {
            None if self.waited => return Ok(Stepped::Return(py.None())),
            None => {}
            Some(predicate) => {
                let result = predicate.bind(py).call0()?;
                if result.is_truthy()? {
                    return Ok(Stepped::Return(result.unbind()));
                }
            }
        }
        self.waited = true;
        let condition = self.condition.get();
        let wait = Waitable::new(Released {
            condvar: Arc::clone(&condition.condvar),
            lock: Arc::clone(condition.lock_source()),
            released: AtomicBool::new(false),
        });
        let target = wait.target();
        self.state = State::Waiting(wait);
        suspend(py, TrapKind::Notified(target))
    }

    /// An exception thrown in (the driver gave the wait up first, T3): it leaves once the lock
    /// is held again.
    fn thrown(&mut self, py: Python<'_>, error: PyErr) -> PyResult<Stepped> {
        match std::mem::replace(&mut self.state, State::Done) {
            State::Waiting(wait) if wait.released.load(Ordering::Relaxed) => {
                self.relock(py, Some(error))
            }
            // Thrown into the shielded lock wait anyway: keep the first exception.
            State::Relocking(first) => self.relock(py, Some(first.unwrap_or(error))),
            _ => Err(error),
        }
    }
}

fn suspend(py: Python<'_>, kind: TrapKind) -> PyResult<Stepped> {
    Ok(Stepped::Yield(
        Py::new(py, Trap::suspended(kind))?.into_any(),
    ))
}

/// Never resumed, thrown into or closed: the `await` was forgotten (see `unawaited.rs`).
impl Drop for ConditionWait {
    fn drop(&mut self) {
        if matches!(self.state, State::Fresh) && !self.started {
            crate::unawaited::warn_never_awaited(self.call);
        }
    }
}

#[pymethods]
impl ConditionWait {
    fn __await__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __iter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.resume(py, &py.None().into_bound(py))? {
            Stepped::Yield(trap) => Ok(Some(trap)),
            Stepped::Return(value) if value.is_none(py) => Ok(None),
            Stepped::Return(value) => Err(stop_iteration(value.into_bound(py))),
        }
    }

    #[pyo3(signature = (value, /))]
    fn send(&mut self, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = value.py();
        match self.resume(py, value)? {
            Stepped::Yield(trap) => Ok(trap),
            Stepped::Return(value) => Err(stop_iteration(value.into_bound(py))),
        }
    }

    #[pyo3(signature = (typ, val = None, tb = None, /))]
    fn throw(
        &mut self,
        typ: &Bound<'_, PyAny>,
        val: Option<&Bound<'_, PyAny>>,
        tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let py = typ.py();
        match self.thrown(py, thrown_exception(typ, val, tb))? {
            Stepped::Yield(trap) => Ok(trap),
            Stepped::Return(value) => Err(stop_iteration(value.into_bound(py))),
        }
    }

    /// Closed (the task is being freed, `GeneratorExit`): nothing can wait any more. A released
    /// lock is taken back if it is free, so the enclosing `async with` releases what it holds.
    /// One another task holds (or was handed, when the task gave an unobserved grant up) stays
    /// with it: the close raises `LockLost` in place of `GeneratorExit`, so that exit releases
    /// nothing (see [`LockLost`]).
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let released = match std::mem::replace(&mut self.state, State::Done) {
            State::Waiting(wait) => wait.released.load(Ordering::Relaxed),
            State::Relocking(_) => true,
            State::Fresh | State::Done => false,
        };
        let lock = self.condition.get().lock_source();
        if !released || lock.try_lock() {
            return Ok(());
        }
        Err(LockLost::new_err(py, Arc::clone(lock)))
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.condition)?;
        if let Some(predicate) = &self.predicate {
            visit.call(predicate)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        let predicate = self.predicate.take();
        drop(predicate);
    }

    fn __repr__(&self) -> String {
        let state = match self.state {
            State::Fresh if !self.started => "fresh",
            State::Fresh | State::Waiting(_) => "waiting",
            State::Relocking(_) => "relocking",
            State::Done => "done",
        };
        format!("<porto._core._ConditionWait {state}>")
    }
}
