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
//! that suspended through an `await`. The request is stored packed (`pending.rs`): a kind byte
//! and one payload word, moved out by the driver only after it won the SUSPENDED → TAKEN
//! exchange, so it is read by one thread exactly once; the getters read only the kind and a
//! sleep's (`Copy`) deadline. The rest of the state is one atomic, so the class stays `frozen`
//! and sound on free-threaded builds (25-cpython-integration-and-memory.md §3).

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU8, Ordering};

use porto_core::task::TaskBodyRef;
use porto_core::time::Deadline;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;

use super::pending::{AbiFuture, IoWait, Kind, Payload, Pending};
use super::pytask::{PyScopeRef, PyTask, Resume};
use super::thin::{BoxedFuture, WaitTarget};
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
/// The phase bits above; the payload bit below.
const PHASE: u8 = 0b0011;
/// Set with TAKEN: the payload was moved out, so dropping the trap must not drop it again.
const MOVED: u8 = 0b0100;

/// What a trap asks the driver to do (11-task-model-and-trap-protocol.md §5.2).
pub(crate) enum TrapKind {
    /// A schedule point: requeue the task at the back of its lane's local queue.
    Yield,
    /// A schedule point, then resume with the value: a synchronous completion after the
    /// budget ran out (T2, [`checkpoint_if_needed`](super::step::checkpoint_if_needed)).
    YieldThen(Py<PyAny>),
    /// Resume the task once the runtime clock reaches `deadline`, never earlier. A deadline
    /// that has already passed is a [`TrapKind::Yield`].
    Sleep {
        /// When to resume.
        deadline: Deadline,
    },
    /// Wait until a sync primitive grants the task's embedded wait node (spec 16).
    #[allow(
        dead_code,
        reason = "made by porto.Event and porto.Notify (porto-szb.94)"
    )]
    Notified(WaitTarget),
    /// Run an I/O operation, re-attempted on readiness (spec 13 §5).
    #[allow(dead_code, reason = "made by the socket APIs (porto-szb.62)")]
    Io(IoWait),
    /// Wait for another task; the await evaluates to its result, or raises its exception.
    Join {
        /// The task to wait for.
        target: TaskBodyRef<PyTask>,
    },
    /// Wait until a scope's children finished (spec 15 §1.1).
    ScopeExit {
        /// The scope whose children the task waits for.
        scope: PyScopeRef,
    },
    /// Wait for a Rust future; the await evaluates to what it resolves to.
    #[allow(
        dead_code,
        reason = "made by Rust-backed awaitables (to_thread, porto-szb.26)"
    )]
    Future(BoxedFuture<Resume>),
    /// Wait for a future from a third-party extension (porto-abi, M3).
    #[allow(dead_code, reason = "uninhabited until porto-abi (M3)")]
    AbiFuture(AbiFuture),
}

impl From<TrapKind> for Pending {
    fn from(kind: TrapKind) -> Self {
        match kind {
            TrapKind::Yield => Self::Yield,
            TrapKind::YieldThen(value) => Self::YieldThen(value),
            TrapKind::Sleep { deadline } => Self::Sleep(deadline),
            TrapKind::Notified(wait) => Self::Notified(wait),
            TrapKind::Io(io) => Self::Io(io),
            TrapKind::Join { target } => Self::Join(target),
            TrapKind::ScopeExit { scope } => Self::ScopeExit(scope),
            TrapKind::Future(future) => Self::Future(future),
            TrapKind::AbiFuture(never) => match never {},
        }
    }
}

/// A trap: a request from a coroutine to the driver, plus the value the driver sends back.
#[pyclass(frozen, freelist = 512, module = "porto._core", name = "_Trap")]
pub struct Trap {
    state: AtomicU8,
    kind: Kind,
    /// The packed request; moved out once, by the driver (see the module docs).
    payload: UnsafeCell<Payload>,
}

// SAFETY: the payload is reached by one thread at a time: `take` moves it out only after
// winning the SUSPENDED → TAKEN exchange (once), `Drop` has `&mut self`, and the getters read
// only a sleep's `Copy` deadline, which nothing writes after `new`. Every payload type is
// `Send`, so moving it out on another thread is sound.
unsafe impl Sync for Trap {}

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
    pub(crate) fn new(kind: TrapKind) -> Self {
        let (kind, payload) = Pending::from(kind).pack();
        Self {
            state: AtomicU8::new(FRESH),
            kind,
            payload: UnsafeCell::new(payload),
        }
    }

    /// A trap that a native awaitable yields to the driver itself, instead of awaiting it: it
    /// is past its first resume, so the driver can take it. The awaitable keeps its own state
    /// and never resumes the trap.
    pub(crate) fn suspended(kind: TrapKind) -> Self {
        let trap = Self::new(kind);
        trap.state.store(SUSPENDED, Ordering::Relaxed);
        trap
    }

    /// A trap that yields to the scheduler once (`porto.checkpoint`).
    pub fn yield_now() -> Self {
        Self::new(TrapKind::Yield)
    }

    /// A trap that resumes the task at `deadline` (`porto.sleep`, `porto.sleep_until`).
    pub fn sleep_until(deadline: Deadline) -> Self {
        Self::new(TrapKind::Sleep { deadline })
    }

    /// The deadline of a sleep trap.
    fn deadline(&self) -> Option<Deadline> {
        // SAFETY: a sleep's payload is its deadline, `Copy` and never written after `new`;
        // `take` only copies it.
        (self.kind == Kind::Sleep).then(|| unsafe { (*self.payload.get()).deadline })
    }

    /// The driver's side: returns the request of a trap that a coroutine suspended on, once.
    ///
    /// `None` if the trap did not reach the driver through an `await` (a generator yielded it
    /// directly, or it was taken already): the driver then throws an error into the coroutine.
    pub(crate) fn take(&self) -> Option<Pending> {
        self.state
            .compare_exchange(
                SUSPENDED,
                TAKEN | MOVED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .ok()?;
        // SAFETY: winning the exchange makes this the one move out of the payload, which was
        // packed with `self.kind`; MOVED keeps `Drop` from dropping it again.
        Some(unsafe { Pending::unpack(self.kind, &*self.payload.get()) })
    }

    fn resume(&self, value_is_none: bool) -> Step {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let (next, step) = match state & PHASE {
                FRESH if value_is_none => (SUSPENDED, Step::Suspend),
                FRESH => return Step::NotStarted,
                SUSPENDED | TAKEN => (DONE | (state & MOVED), Step::Complete),
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

    /// Marks the trap finished (thrown into or closed), keeping MOVED.
    fn finish(&self) {
        self.state.fetch_or(DONE, Ordering::AcqRel);
    }
}

impl Drop for Trap {
    fn drop(&mut self) {
        if self.kind.owns_payload() && *self.state.get_mut() & MOVED == 0 {
            // SAFETY: not moved out, so the payload is live and packed with `self.kind`; it is
            // never read again. A trap's request is never linked anywhere, so dropping it is
            // all there is to undo.
            drop(unsafe { Pending::unpack(self.kind, self.payload.get_mut()) });
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
    /// What the trap asks for: `"yield"`, `"sleep"`, `"notified"`, `"io"`,
    /// `"join"`, `"scope_exit"` or `"future"`.
    #[getter(kind)]
    fn py_kind(&self) -> &'static str {
        self.kind.name()
    }

    /// For a sleep, when it ends, in seconds on the runtime clock (`porto.current_time()`).
    #[getter(deadline)]
    fn py_deadline(&self) -> Option<f64> {
        self.deadline().map(Deadline::as_secs_f64)
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
        self.finish();
        Err(thrown_exception(typ, val, tb))
    }

    fn close(&self) {
        self.finish();
    }

    fn __repr__(&self) -> String {
        let state = match self.state.load(Ordering::Acquire) & PHASE {
            FRESH => "fresh",
            SUSPENDED => "suspended",
            TAKEN => "taken",
            _ => "done",
        };
        match self.deadline() {
            Some(deadline) => format!(
                "<porto._core._Trap sleep until {:.6}s {state}>",
                deadline.as_secs_f64()
            ),
            None => format!("<porto._core._Trap {} {state}>", self.kind.name()),
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

    fn deadline_of(pending: Option<Pending>) -> Option<Deadline> {
        match pending {
            Some(Pending::Sleep(deadline)) => Some(deadline),
            _ => None,
        }
    }

    #[test]
    fn the_copy_kinds_round_trip_through_the_packed_payload() {
        let sleep = Trap::sleep_until(Deadline::from_nanos(7));
        assert_eq!(sleep.deadline(), Some(Deadline::from_nanos(7)));
        assert!(matches!(sleep.resume(true), Step::Suspend));
        assert_eq!(deadline_of(sleep.take()), Some(Deadline::from_nanos(7)));
        // The getters still read the kind and the deadline once the trap was taken.
        assert_eq!(sleep.deadline(), Some(Deadline::from_nanos(7)));
        let trap = Trap::yield_now();
        assert_eq!(trap.kind.name(), "yield");
        assert_eq!(trap.deadline(), None);
        assert!(matches!(trap.resume(true), Step::Suspend));
        assert!(trap.take().is_some());
    }

    #[test]
    fn the_driver_takes_a_trap_once_and_only_after_it_suspended() {
        let trap = Trap::sleep_until(Deadline::from_nanos(7));
        assert!(trap.take().is_none(), "a fresh trap was not awaited");
        assert!(matches!(trap.resume(true), Step::Suspend));
        assert_eq!(deadline_of(trap.take()), Some(Deadline::from_nanos(7)));
        assert!(trap.take().is_none(), "taken twice");
        assert!(matches!(trap.resume(false), Step::Complete));
        assert!(matches!(trap.resume(true), Step::Reused));
        assert!(trap.take().is_none());
        assert_eq!(trap.state.load(Ordering::Acquire), DONE | MOVED);
    }

    #[test]
    fn a_trap_drops_a_payload_it_still_owns_exactly_once() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;

        struct Probe(Arc<AtomicUsize>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let future_trap = |drops: &Arc<AtomicUsize>| {
            let probe = Probe(drops.clone());
            Trap::new(TrapKind::Future(BoxedFuture::new(async move {
                drop(probe);
                Resume::None
            })))
        };
        let drops = Arc::new(AtomicUsize::new(0));
        // Never awaited: the trap drops the future.
        drop(future_trap(&drops));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        // Taken: the driver owns the future, the trap must not drop it.
        let trap = future_trap(&drops);
        assert!(matches!(trap.resume(true), Step::Suspend));
        let taken = trap.take();
        trap.finish();
        drop(trap);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(taken);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}
