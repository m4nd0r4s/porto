//! `_Trap`: the one object a Porto awaitable yields to the driver (11-task-model §5.1).
//!
//! `await trap` runs `trap.__await__()` (the trap itself), then resumes it:
//!
//! 1. the first resume (`send(None)`, `__next__` or `am_send`) suspends: the trap yields itself,
//!    and that object travels up the coroutine chain to the driver;
//! 2. the driver resumes the outer coroutine with a value; the chain hands it to the trap, which
//!    returns it, so the `await` evaluates to that value (`am_send` returns `PYGEN_RETURN`, the
//!    Python `send` raises `StopIteration(value)`);
//! 3. any later resume raises `RuntimeError("trap reused")`: a trap is taken exactly once.
//!
//! `throw(...)` re-raises the exception at the `await`. The state is an atomic, so the class
//! stays `frozen` and safe on free-threaded builds (25-cpython-integration-and-memory.md §3).

use core::sync::atomic::{AtomicU8, Ordering};

use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;

use crate::async_slots::{guard_send, stop_iteration};
use crate::throw_args::thrown_exception;

/// Not awaited yet: the next resume suspends.
const FRESH: u8 = 0;
/// Yielded to the driver: the next resume returns the value sent back.
const SUSPENDED: u8 = 1;
/// Resumed, thrown into or closed: any further resume is a reuse.
const DONE: u8 = 2;

/// A trap: a request from a coroutine to the driver, plus the value the driver sends back.
#[pyclass(frozen, freelist = 512, module = "porto._core", name = "_Trap")]
pub struct Trap {
    state: AtomicU8,
    kind: u32,
    args: Py<PyAny>,
}

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
    /// A trap of `kind` with its arguments. Kinds are defined by the driver (porto-itr.4).
    pub fn new(kind: u32, args: Py<PyAny>) -> Self {
        Self {
            state: AtomicU8::new(FRESH),
            kind,
            args,
        }
    }

    /// The trap kind, as given to [`Trap::new`].
    pub fn kind(&self) -> u32 {
        self.kind
    }

    /// The trap arguments, as given to [`Trap::new`].
    pub fn args(&self) -> &Py<PyAny> {
        &self.args
    }

    fn resume(&self, value_is_none: bool) -> Step {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let (next, step) = match state {
                FRESH if value_is_none => (SUSPENDED, Step::Suspend),
                FRESH => return Step::NotStarted,
                SUSPENDED => (DONE, Step::Complete),
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
    /// The trap kind (an integer the driver dispatches on).
    #[getter(kind)]
    fn py_kind(&self) -> u32 {
        self.kind
    }

    /// The trap arguments.
    #[getter(args)]
    fn py_args(&self, py: Python<'_>) -> Py<PyAny> {
        self.args.clone_ref(py)
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
            _ => "done",
        };
        format!("<porto._core._Trap kind={} {state}>", self.kind)
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
