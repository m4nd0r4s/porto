//! `_SyncWait`: the awaitable behind `Event.wait()`, `Notify.notified()`, `Lock.acquire()`,
//! `Semaphore.acquire()`, `CapacityLimiter.acquire()`, `RwLock.acquire_read()`/`acquire_write()`
//! and `Barrier.wait()` (16-sync-primitives-and-channels.md §1). `async with` on a `Lock` or `Mutex` follows the
//! same rules with a lighter object of its own (`entry`).
//!
//! Nothing happens until it is awaited, so a `notified()` created and never awaited consumes
//! no permit, and an `acquire()` never awaited takes no lock. A grant is only ever taken in the
//! step that hands it to the coroutine, so a task closed or cancelled at any suspension point
//! cannot hold a grant it never saw, and neither can one that an exception stops between the
//! call and the await (3.14 runs hooks in between):
//!
//! - the first resume spends one unit of the task's budget (T2). If that ran the budget out,
//!   it takes nothing: it yields a `Yield` trap and tries again on the next resume, after the
//!   other ready tasks ran;
//! - otherwise it tries the primitive without waiting (`try_notified`, `is_set`, `try_lock`).
//!   Success returns at once, in this same step;
//! - otherwise it yields a `Notified` trap: the driver parks the task's embedded wait node on
//!   the primitive, and the grant stays in the node until the coroutine is resumed with it. A
//!   task that gives the wait up first (T3) cancels the node, which passes a grant it had not
//!   observed on to the next waiter (or back to the primitive).
//!
//! The one window left is in the driver: a wait that resolves when the step loop's budget is
//! spent yields before resuming the coroutine (porto-szb.105). It never opens here: the
//! `Notified` trap is only yielded with budget left.

use std::sync::Arc;

use pyo3::PyTraverseError;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::gc::PyVisit;
use pyo3::prelude::*;

use super::barrier::Arrival;
use super::event::{EventSource, NotifySource};
use super::limiter::{TokenGrant, traverse_grant};
use super::lock::LockSource;
use super::rwlock::{ReadSource, WriteSource, release_read, release_write};
use super::semaphore::{SemaphoreSource, permits_target};
use crate::async_slots::{IterNext, guard_next, guard_send, stop_iteration};
use crate::driver::fast_new::{FastNew, Unawaited};
use crate::driver::step;
use crate::driver::thin::{WaitTarget, Waitable};
use crate::driver::trap::{Trap, TrapKind};
use crate::throw_args::thrown_exception;

/// The primitive an awaitable waits on, and how to take its grant without waiting.
pub(super) enum Grant {
    /// `Event.wait()`: the event is set. Nothing is consumed.
    EventSet(Arc<Waitable<EventSource>>),
    /// `Notify.notified()`: a permit or a `notify_one` wake, consumed.
    Notified(Arc<Waitable<NotifySource>>),
    /// `Lock.acquire()`: the lock, held until `release()`.
    Locked(Arc<Waitable<LockSource>>),
    /// `Semaphore.acquire(permits)`: that many permits, held until `release(permits)`.
    Permits(Arc<Waitable<SemaphoreSource>>, usize),
    /// `CapacityLimiter.acquire()`: a token, recorded for its borrower.
    Token(TokenGrant),
    /// `RwLock.acquire_read()`: a read lock.
    Read(Arc<Waitable<ReadSource>>),
    /// `RwLock.acquire_write()`: the write lock.
    Write(Arc<Waitable<WriteSource>>),
    /// `Barrier.wait()`: the round completed; the await evaluates to whether it led.
    Arrival(Arc<Waitable<Arrival>>),
}

impl Grant {
    /// Before any attempt: an error that makes the wait pointless. Takes nothing.
    fn check(&self, py: Python<'_>) -> PyResult<()> {
        match self {
            Self::Token(token) => token.check(py),
            _ => Ok(()),
        }
    }

    /// Takes the grant if it is available now.
    fn try_take(&self) -> bool {
        match self {
            Self::EventSet(event) => event.0.is_set(),
            Self::Notified(notify) => notify.0.try_notified(),
            Self::Locked(lock) => lock.try_lock(),
            Self::Permits(semaphore, permits) => semaphore.0.try_acquire_detached(*permits),
            Self::Token(token) => token.try_take(),
            Self::Read(lock) => lock.0.try_read_detached(),
            Self::Write(lock) => lock.0.0.try_write_detached(),
            Self::Arrival(arrival) => arrival.try_lead(),
        }
    }

    /// In the step that resumes the coroutine with the grant: records its holder. On error
    /// the grant is given back.
    fn taken(&self, py: Python<'_>) -> PyResult<()> {
        match self {
            Self::Token(token) => token.taken(py),
            _ => Ok(()),
        }
    }

    /// Gives a taken grant back: the coroutine is not resumed with it after all.
    fn give_back(&self, py: Python<'_>) {
        match self {
            Self::EventSet(_) | Self::Arrival(_) => {}
            Self::Notified(notify) => notify.0.notify_one(),
            Self::Locked(lock) => {
                lock.unlock();
            }
            Self::Permits(semaphore, permits) => {
                semaphore.0.release(*permits);
            }
            Self::Token(token) => token.give_back(py),
            Self::Read(lock) => {
                let _ = release_read(lock);
            }
            Self::Write(lock) => {
                let _ = release_write(&lock.0);
            }
        }
    }

    /// The handle a waiting task parks on.
    fn target(&self) -> WaitTarget {
        match self {
            Self::EventSet(event) => event.target(),
            Self::Notified(notify) => notify.target(),
            Self::Locked(lock) => lock.target(),
            Self::Permits(semaphore, permits) => permits_target(semaphore, *permits),
            Self::Token(token) => token.target(),
            Self::Read(lock) => lock.target(),
            Self::Write(lock) => lock.target(),
            Self::Arrival(arrival) => arrival.target(),
        }
    }

    /// What the await evaluates to when nothing else is asked for ([`Then::Nothing`]).
    fn value(&self, py: Python<'_>) -> Py<PyAny> {
        match self {
            Self::Arrival(arrival) => pyo3::types::PyBool::new(py, arrival.led())
                .to_owned()
                .into_any()
                .unbind(),
            _ => py.None(),
        }
    }
}

/// Where the awaitable is.
enum State {
    /// Not granted yet: the next resume tries (the first one, or after a budget yield).
    Trying(Grant),
    /// Yielded a `Notified` trap: the driver resumes it once the wait node was granted.
    Waiting(Grant),
    /// Returned, raised, thrown into or closed.
    Done,
}

/// What the await evaluates to once the grant is taken, made in the same step.
pub(super) enum Then {
    /// `None`.
    Nothing,
    /// The result of calling this; if it raises, the grant is given back and the await raises
    /// (`Lock._acquire_then`).
    Call(Py<PyAny>),
}

/// An awaitable wait on a sync primitive.
#[pyclass(freelist = 256, module = "porto._core", name = "_SyncWait")]
pub(crate) struct SyncWait {
    state: State,
    /// Whether it has been resumed: a first resume must send `None`.
    started: bool,
    then: Then,
    /// The call that returned this wait, as the user wrote it (for the never-awaited warning).
    call: &'static str,
}

/// What one resume does.
enum Stepped {
    /// Suspend, yielding this trap to the driver.
    Yield(Py<PyAny>),
    /// The await evaluates to this.
    Return(Py<PyAny>),
}

impl SyncWait {
    /// The wait of `wait()`, `notified()` or `acquire()`; the await evaluates to `None`.
    pub(super) fn new(grant: Grant) -> Self {
        let call = match &grant {
            Grant::EventSet(_) => "Event.wait()",
            Grant::Notified(_) => "Notify.notified()",
            Grant::Locked(_) => "Lock.acquire()",
            Grant::Permits(..) => "Semaphore.acquire()",
            Grant::Token(token) => token.call(),
            Grant::Read(_) => "RwLock.acquire_read()",
            Grant::Write(_) => "RwLock.acquire_write()",
            Grant::Arrival(_) => "Barrier.wait()",
        };
        Self::with(grant, Then::Nothing, call)
    }

    /// A wait for `grant` that `call` returned and that evaluates to `then`'s result.
    pub(super) fn with(grant: Grant, then: Then, call: &'static str) -> Self {
        Self {
            state: State::Trying(grant),
            started: false,
            then,
            call,
        }
    }

    /// This wait as a new `_SyncWait` object, over the freelist (`fast_new.rs`).
    pub(super) fn into_object(self, py: Python<'_>) -> PyResult<Py<Self>> {
        SYNC_WAITS.object(py, self)
    }

    /// The grant is taken: what the await evaluates to.
    fn granted(&mut self, py: Python<'_>, grant: &Grant) -> PyResult<Stepped> {
        grant.taken(py)?;
        let result = match std::mem::replace(&mut self.then, Then::Nothing) {
            Then::Nothing => return Ok(Stepped::Return(grant.value(py))),
            Then::Call(then) => then.call0(py),
        };
        result
            .map(Stepped::Return)
            .inspect_err(|_| grant.give_back(py))
    }

    /// Resumes the awaitable with `sent`.
    fn resume(&mut self, py: Python<'_>, sent: &Bound<'_, PyAny>) -> PyResult<Stepped> {
        if !self.started && !sent.is_none() {
            return Err(not_started_error());
        }
        self.started = true;
        match std::mem::replace(&mut self.state, State::Done) {
            State::Trying(grant) => {
                if budget_ran_out(py)? {
                    // Take nothing before the schedule point: a task closed there holds nothing.
                    self.state = State::Trying(grant);
                    return suspend(py, TrapKind::Yield);
                }
                grant.check(py)?;
                if grant.try_take() {
                    return self.granted(py, &grant);
                }
                let target = grant.target();
                self.state = State::Waiting(grant);
                suspend(py, TrapKind::Notified(target))
            }
            // The driver resumes a `Notified` wait only once it was granted.
            State::Waiting(grant) => self.granted(py, &grant),
            State::Done => Err(reused_error()),
        }
    }
}

/// `_SyncWait` objects made without pyo3's generic path.
static SYNC_WAITS: FastNew<SyncWait> = FastNew::tracked();

/// Enables [`SyncWait::into_object`]'s fast path if the layout allows it. Call it once from
/// module init, after the type's slots are installed.
pub(super) fn init_fast_new(py: Python<'_>) -> PyResult<()> {
    // A finished wait: dropping it does not warn.
    let probe = SyncWait {
        state: State::Done,
        started: false,
        then: Then::Nothing,
        call: "",
    };
    SYNC_WAITS.init(py, probe)
}

impl Unawaited for SyncWait {
    fn unwatch(&mut self) {
        // Finished rather than forgotten; the grant was never taken.
        self.state = State::Done;
    }
}

/// Never resumed, thrown into or closed: the `await` was forgotten (see `unawaited.rs`).
impl Drop for SyncWait {
    fn drop(&mut self) {
        if let State::Trying(_) = &self.state
            && !self.started
        {
            crate::unawaited::warn_never_awaited(self.call);
        }
    }
}

/// Spends one unit of the running task's budget (T2); whether it was already spent. Raises
/// `Cancelled` instead while the task is cancelled (C1).
fn budget_ran_out(py: Python<'_>) -> PyResult<bool> {
    Ok(!step::proceed(py)?)
}

/// Yields a trap for `kind` to the driver.
fn suspend(py: Python<'_>, kind: TrapKind) -> PyResult<Stepped> {
    Ok(Stepped::Yield(
        Py::new(py, Trap::suspended(kind))?.into_any(),
    ))
}

fn not_started_error() -> PyErr {
    PyTypeError::new_err(
        "can't send a non-None value to a porto wait that has not started; the first resume \
         of an awaitable must be send(None)",
    )
}

fn reused_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: a porto wait was awaited again after it finished; call wait(), notified() or \
         acquire() again for each await",
    )
}

/// The borrow failed: two callers resumed the same wait at once.
fn concurrent_resume_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: a porto wait was resumed from two places at once; await each wait from one \
         task only",
    )
}

#[pymethods]
impl SyncWait {
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

    /// The driver gave the wait up before throwing in (T3); the wait is over.
    #[pyo3(signature = (typ, val = None, tb = None, /))]
    fn throw(
        &mut self,
        typ: &Bound<'_, PyAny>,
        val: Option<&Bound<'_, PyAny>>,
        tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        self.state = State::Done;
        Err(thrown_exception(typ, val, tb))
    }

    fn close(&mut self) {
        self.state = State::Done;
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let State::Trying(Grant::Token(token)) | State::Waiting(Grant::Token(token)) =
            &self.state
        {
            traverse_grant(token, &visit)?;
        }
        match &self.then {
            Then::Nothing => Ok(()),
            Then::Call(then) => visit.call(then),
        }
    }

    fn __clear__(&mut self) {
        let then = std::mem::replace(&mut self.then, Then::Nothing);
        drop(then);
    }

    fn __repr__(&self) -> &'static str {
        match &self.state {
            State::Trying(_) if !self.started => "<porto._core._SyncWait fresh>",
            State::Trying(_) | State::Waiting(_) => "<porto._core._SyncWait waiting>",
            State::Done => "<porto._core._SyncWait done>",
        }
    }
}

/// `am_send` for `_SyncWait`: the same steps as `send`, with no `StopIteration` on completion.
///
/// # Safety
///
/// CPython calls it as `tp_as_async->am_send` of `_SyncWait`, with an attached thread state,
/// `slf` a live `_SyncWait`, `arg` a live object and `presult` writable.
pub(crate) unsafe extern "C" fn sync_wait_am_send(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
    presult: *mut *mut ffi::PyObject,
) -> ffi::PySendResult {
    guard_send(|| {
        // SAFETY: the caller is attached (am_send contract).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: `slf` and `arg` are live, borrowed for the duration of the call; the slot is
        // only installed on `_SyncWait`, so `slf` is one.
        let (slf, arg) = unsafe { (Borrowed::from_ptr(py, slf), Borrowed::from_ptr(py, arg)) };
        // SAFETY: see above.
        let wait = unsafe { slf.cast_unchecked::<SyncWait>() };
        let stepped = match wait.try_borrow_mut() {
            Ok(mut wait) => wait.resume(py, &arg),
            Err(_) => Err(concurrent_resume_error()),
        };
        let (result, status) = match stepped {
            Ok(Stepped::Yield(trap)) => (trap, ffi::PySendResult::PYGEN_NEXT),
            Ok(Stepped::Return(value)) => (value, ffi::PySendResult::PYGEN_RETURN),
            Err(error) => {
                error.restore(py);
                return ffi::PySendResult::PYGEN_ERROR;
            }
        };
        // SAFETY: `presult` is writable (am_send contract); it receives a new reference.
        unsafe { *presult = result.into_ptr() };
        status
    })
}

/// `tp_iternext` for `_SyncWait` (see `async_slots`): the same steps as `__next__`.
///
/// # Safety
///
/// CPython calls it as `tp_iternext` of `_SyncWait`, attached, with `slf` a live `_SyncWait`.
pub(crate) unsafe extern "C" fn sync_wait_iternext(slf: *mut ffi::PyObject) -> *mut ffi::PyObject {
    guard_next(|py| {
        // SAFETY: `slf` is live for the call, and the slot is only installed on `_SyncWait`.
        let wait = unsafe { Borrowed::from_ptr(py, slf).cast_unchecked::<SyncWait>() };
        let mut wait = wait
            .try_borrow_mut()
            .map_err(|_| concurrent_resume_error())?;
        Ok(match wait.resume(py, &py.None().into_bound(py))? {
            Stepped::Yield(trap) => IterNext::Yield(trap.into_bound(py)),
            Stepped::Return(value) => IterNext::Return(value.into_bound(py)),
        })
    })
}
