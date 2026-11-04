//! `_Trap`: the one object a Porto awaitable yields to the driver (11-task-model §5.1).
//!
//! `await trap` runs `trap.__await__()` (the trap itself), then resumes it:
//!
//! 1. the first resume (`send(None)`, `__next__` or `am_send`) suspends: the trap yields itself,
//!    and that object travels up the coroutine chain to the driver;
//! 2. the driver resumes the outer coroutine with a value; the chain hands it to the trap, which
//!    returns it, so the `await` evaluates to that value (`am_send` returns `PYGEN_RETURN`, the
//!    Python `send` raises `StopIteration(value)`);
//! 3. any later resume raises `RuntimeError("trap reused")`: a trap is awaited exactly once.
//!
//! `throw(...)` re-raises the exception at the `await`.
//!
//! The driver reads the request with [`Trap::take`], which succeeds once, and only for a trap
//! that suspended through an `await`. The payload is an immutable [`TrapKind`] and the only
//! mutable state is one atomic, so the class stays `frozen` and sound on free-threaded builds
//! (25-cpython-integration-and-memory.md §3). Spec 11 §5.1 sketches the payload as an
//! `UnsafeCell<Option<TrapKind>>` moved out by the driver; the kinds that exist so far are
//! `Copy`, so nothing needs moving. A kind that owns data (`Future`, `Join`) can keep this
//! design by moving its payload out only after winning the SUSPENDED → TAKEN exchange.

use core::sync::atomic::{AtomicU8, Ordering};

use porto_core::time::Deadline;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;

use crate::async_slots::{guard_send, stop_iteration};
use crate::throw_args::thrown_exception;

/// Not awaited yet: the next resume suspends.
const FRESH: u8 = 0;
/// Yielded to the driver: the next resume returns the value sent back.
const SUSPENDED: u8 = 1;
/// Taken by the driver (after SUSPENDED): the next resume returns the value sent back.
const TAKEN: u8 = 2;
/// Resumed, thrown into or closed: any further resume is a reuse.
const DONE: u8 = 3;

/// What a trap asks the driver to do (11-task-model-and-trap-protocol.md §5.2). Later kinds
/// (`Notified`, `Io`, `Join`, `ScopeExit`, `Future`) arrive with the features that need them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrapKind {
    /// A schedule point: requeue the task at the back of its lane's local queue.
    Yield,
    /// Resume the task once the runtime clock reaches `deadline`, never earlier. A deadline
    /// that has already passed is a [`TrapKind::Yield`].
    Sleep {
        /// When to resume.
        deadline: Deadline,
    },
}

/// A trap: a request from a coroutine to the driver, plus the value the driver sends back.
#[pyclass(frozen, freelist = 512, module = "porto._core", name = "_Trap")]
pub struct Trap {
    state: AtomicU8,
    kind: TrapKind,
}

// 25-cpython-integration-and-memory.md §5: a trap is at most 48 bytes with its object header.
const _: () = assert!(
    size_of::<Trap>() <= 16,
    "the trap payload must stay two words"
);

/// What one resume of a trap does.
enum Step {
    /// Yield the trap itself to the driver.
    Suspend,
    /// Return the value sent in as the result of the `await`.
    Complete,
    /// A non-None value was sent before the trap suspended.
    NotStarted,
    /// The trap already completed.
    Reused,
}

impl Trap {
    /// A fresh trap asking for `kind`.
    pub fn new(kind: TrapKind) -> Self {
        Self {
            state: AtomicU8::new(FRESH),
            kind,
        }
    }

    /// A trap that yields to the scheduler once (`porto.checkpoint`).
    pub fn yield_now() -> Self {
        Self::new(TrapKind::Yield)
    }

    /// A trap that resumes the task at `deadline` (`porto.sleep`, `porto.sleep_until`).
    pub fn sleep_until(deadline: Deadline) -> Self {
        Self::new(TrapKind::Sleep { deadline })
    }

    /// What the trap asks for.
    pub fn kind(&self) -> TrapKind {
        self.kind
    }

    /// The driver's side: returns the request of a trap that a coroutine suspended on, once.
    ///
    /// `None` if the trap did not reach the driver through an `await` (a generator yielded it
    /// directly, or it was taken already): the driver then throws an error into the coroutine.
    pub fn take(&self) -> Option<TrapKind> {
        self.state
            .compare_exchange(SUSPENDED, TAKEN, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| self.kind)
    }

    fn resume(&self, value_is_none: bool) -> Step {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let (next, step) = match state {
                FRESH if value_is_none => (SUSPENDED, Step::Suspend),
                FRESH => return Step::NotStarted,
                SUSPENDED | TAKEN => (DONE, Step::Complete),
                _ => return Step::Reused,
            };
            match self
                .state
                .compare_exchange_weak(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return step,
                Err(actual) => state = actual,
            }
        }
    }
}

fn not_started_error() -> PyErr {
    PyTypeError::new_err(
        "can't send a non-None value to a trap that has not started; \
         the first resume of an awaitable must be send(None)",
    )
}

fn reused_error() -> PyErr {
    PyRuntimeError::new_err(
        "trap reused: a porto trap can be awaited only once; create a new trap for each await",
    )
}

#[pymethods]
impl Trap {
    /// What the trap asks for: `"yield"` or `"sleep"`.
    #[getter(kind)]
    fn py_kind(&self) -> &'static str {
        match self.kind {
            TrapKind::Yield => "yield",
            TrapKind::Sleep { .. } => "sleep",
        }
    }

    /// For a sleep, when it ends, in seconds on the runtime clock (`_monotonic()`).
    #[getter(deadline)]
    fn py_deadline(&self) -> Option<f64> {
        match self.kind {
            TrapKind::Yield => None,
            TrapKind::Sleep { deadline } => Some(deadline.as_secs_f64()),
        }
    }

    fn __await__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __iter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __next__(slf: &Bound<'_, Self>) -> PyResult<Option<Py<Self>>> {
        match slf.get().resume(true) {
            Step::Suspend => Ok(Some(slf.clone().unbind())),
            // `next()` sends None, so the await evaluates to None: plain exhaustion.
            Step::Complete => Ok(None),
            Step::NotStarted => Err(not_started_error()),
            Step::Reused => Err(reused_error()),
        }
    }

    #[pyo3(signature = (value, /))]
    fn send(slf: &Bound<'_, Self>, value: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        match slf.get().resume(value.is_none()) {
            Step::Suspend => Ok(slf.clone().into_any().unbind()),
            Step::Complete => Err(stop_iteration(value)),
            Step::NotStarted => Err(not_started_error()),
            Step::Reused => Err(reused_error()),
        }
    }

    #[pyo3(signature = (typ, val = None, tb = None, /))]
    fn throw(
        &self,
        typ: &Bound<'_, PyAny>,
        val: Option<&Bound<'_, PyAny>>,
        tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        self.state.store(DONE, Ordering::Release);
        Err(thrown_exception(typ, val, tb))
    }

    fn close(&self) {
        self.state.store(DONE, Ordering::Release);
    }

    fn __repr__(&self) -> String {
        let state = match self.state.load(Ordering::Acquire) {
            FRESH => "fresh",
            SUSPENDED => "suspended",
            TAKEN => "taken",
            _ => "done",
        };
        match self.kind {
            TrapKind::Yield => format!("<porto._core._Trap yield {state}>"),
            TrapKind::Sleep { deadline } => format!(
                "<porto._core._Trap sleep until {:.6}s {state}>",
                deadline.as_secs_f64()
            ),
        }
    }
}

/// `am_send` for `_Trap`: the same steps as `send`, with no `StopIteration` on completion.
///
/// # Safety
///
/// CPython calls it as `tp_as_async->am_send` of `_Trap`, with an attached thread state, `slf` a
/// live `_Trap`, `arg` a live object and `presult` writable.
pub(crate) unsafe extern "C" fn trap_am_send(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
    presult: *mut *mut ffi::PyObject,
) -> ffi::PySendResult {
    guard_send(|| {
        // SAFETY: the caller is attached (am_send contract).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: `slf` and `arg` are live, borrowed for the duration of the call; the slot is
        // only installed on `_Trap`, so `slf` is a `_Trap`.
        let (slf, arg) = unsafe { (Borrowed::from_ptr(py, slf), Borrowed::from_ptr(py, arg)) };
        // SAFETY: see above, `slf` is an instance of `_Trap`.
        let trap = unsafe { slf.cast_unchecked::<Trap>() };
        let (result, status) = match trap.get().resume(arg.is_none()) {
            Step::Suspend => (slf.to_owned().into_ptr(), ffi::PySendResult::PYGEN_NEXT),
            Step::Complete => (arg.to_owned().into_ptr(), ffi::PySendResult::PYGEN_RETURN),
            Step::NotStarted => {
                not_started_error().restore(py);
                return ffi::PySendResult::PYGEN_ERROR;
            }
            Step::Reused => {
                reused_error().restore(py);
                return ffi::PySendResult::PYGEN_ERROR;
            }
        };
        // SAFETY: `presult` is writable (am_send contract); it receives a new reference.
        unsafe { *presult = result };
        status
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trap_kind_is_one_word() {
        assert_eq!(size_of::<TrapKind>(), 8);
    }

    #[test]
    fn the_driver_takes_a_trap_once_and_only_after_it_suspended() {
        let trap = Trap::sleep_until(Deadline::from_nanos(7));
        assert_eq!(trap.take(), None, "a fresh trap was not awaited");
        assert!(matches!(trap.resume(true), Step::Suspend));
        assert_eq!(
            trap.take(),
            Some(TrapKind::Sleep {
                deadline: Deadline::from_nanos(7)
            })
        );
        assert_eq!(trap.take(), None, "taken twice");
        assert!(matches!(trap.resume(false), Step::Complete));
        assert!(matches!(trap.resume(true), Step::Reused));
        assert_eq!(trap.take(), None);
    }
}
