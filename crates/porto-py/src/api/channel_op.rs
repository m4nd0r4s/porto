//! `_ChannelOp`: the awaitable behind `tx.send(v)`, `rx.recv()`, `rx.recv_many(max)` and
//! `async for v in rx` (16-sync-primitives-and-channels.md §2.1-§2.2).
//!
//! Nothing happens until it is awaited, so an operation created and never awaited neither
//! sends nor loses a value. Each resume tries the operation without waiting:
//!
//! - it completed: the await returns its result (`PYGEN_RETURN` through `am_send`). A
//!   completion on the first try is synchronous, so it spends the task's budget (T2): when the
//!   budget runs out it yields a `YieldThen` trap instead, and returns what that sends back;
//! - it would wait: it yields a `Notified` trap whose target is the handle; the driver parks
//!   the task's embedded wait node on the channel's waiter list (`Sender::poll_ready`). A wake
//!   means "retry": the next resume tries again, and waits again if another task was faster;
//! - the channel is closed: the await raises.
//!
//! T3: a waiting task that is cancelled gives its node up (the driver cancels it before
//! throwing in), and a wake it had not observed yet goes to the next waiter, so no value or
//! room is lost. The retry runs in the same step as the wake, so a wake that was observed is
//! always acted on.

use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;

use porto_core::sync::mpsc::{TryRecvError, TrySendError};

use super::channel::{
    PyObj, RecvSide, SendSide, collect_many, recv_closed_error, send_closed_error,
};
use crate::async_slots::{guard_send, stop_iteration};
use crate::driver::thin::{WaitTarget, Waitable};
use crate::driver::trap::{Trap, TrapKind};
use crate::driver::{lane, step};
use crate::errors::NoRuntimeError;
use crate::throw_args::thrown_exception;

/// What the awaitable does.
pub(super) enum Op {
    /// `tx.send(value)`; `value` is `None` only while a try holds it.
    Send {
        side: Arc<Waitable<SendSide>>,
        value: Option<PyObj>,
    },
    /// `rx.recv()`.
    Recv { side: Arc<Waitable<RecvSide>> },
    /// `rx.__anext__()`: the end of the channel is `StopAsyncIteration`.
    Next { side: Arc<Waitable<RecvSide>> },
    /// `rx.recv_many(max)`.
    RecvMany {
        side: Arc<Waitable<RecvSide>>,
        max: usize,
    },
}

impl Op {
    /// The call that returned this operation, as the user wrote it.
    fn call_name(&self) -> &'static str {
        match self {
            Self::Send { .. } => "Sender.send(...)",
            Self::Recv { .. } => "Receiver.recv()",
            Self::Next { .. } => "anext(receiver)",
            Self::RecvMany { .. } => "Receiver.recv_many(...)",
        }
    }

    /// Tries the operation without waiting: `Some(result)` once done, `None` to wait.
    fn attempt(&mut self, py: Python<'_>) -> PyResult<Option<PyObj>> {
        match self {
            Self::Send { side, value } => {
                let tx = &side.0;
                match tx.try_send(
                    value
                        .take()
                        .expect("a send holds its value until it is done"),
                ) {
                    Ok(()) => Ok(Some(py.None())),
                    Err(TrySendError::Full(back)) => {
                        *value = Some(back);
                        Ok(None)
                    }
                    Err(TrySendError::Closed(_)) => Err(send_closed_error(tx)),
                }
            }
            Self::Recv { side } => match side.0.try_recv() {
                Ok(value) => Ok(Some(value)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Closed) => Err(recv_closed_error(&side.0)),
            },
            Self::Next { side } => match side.0.try_recv() {
                Ok(value) => Ok(Some(value)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Closed) if side.0.is_handle_closed() => {
                    Err(recv_closed_error(&side.0))
                }
                Err(TryRecvError::Closed) => Err(PyStopAsyncIteration::new_err(())),
            },
            Self::RecvMany { side, max } => match side.0.try_recv() {
                Ok(first) => collect_many(py, &side.0, first, *max).map(Some),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Closed) => Err(recv_closed_error(&side.0)),
            },
        }
    }

    /// The handle a waiting task parks on.
    fn target(&self) -> WaitTarget {
        match self {
            Self::Send { side, .. } => side.target(),
            Self::Recv { side } | Self::Next { side } | Self::RecvMany { side, .. } => {
                side.target()
            }
        }
    }
}

/// Where the awaitable is. The operation (and the handle it keeps open) is released as soon
/// as it is no longer needed, so a finished `_ChannelOp` that is kept around does not hold
/// the channel open.
enum State {
    /// Not resumed yet.
    Fresh(Op),
    /// Yielded a `Notified` trap; the next resume retries.
    Waiting(Op),
    /// Yielded a `YieldThen` trap with the result (T2); the next resume returns what it sends.
    Delivering,
    /// Returned, raised, thrown into or closed.
    Done,
}

/// An awaitable channel operation.
#[pyclass(freelist = 256, module = "porto._core", name = "_ChannelOp")]
pub(crate) struct ChannelOp {
    state: State,
}

/// What one resume does.
enum Stepped {
    /// Suspend, yielding this trap to the driver.
    Yield(Py<PyAny>),
    /// The await evaluates to this.
    Return(Py<PyAny>),
}

impl ChannelOp {
    pub(super) fn new(op: Op) -> Self {
        Self {
            state: State::Fresh(op),
        }
    }

    /// Resumes the awaitable with `sent`.
    fn resume(&mut self, py: Python<'_>, sent: &Bound<'_, PyAny>) -> PyResult<Stepped> {
        let (mut op, first) = match std::mem::replace(&mut self.state, State::Done) {
            State::Fresh(_) if !sent.is_none() => return Err(not_started_error()),
            State::Fresh(op) => (op, true),
            State::Waiting(op) => (op, false),
            State::Delivering => return Ok(Stepped::Return(sent.clone().unbind())),
            State::Done => return Err(reused_error()),
        };
        // An error leaves the state `Done`.
        match op.attempt(py)? {
            Some(result) if first && step::spend_budget() => {
                self.state = State::Delivering;
                suspend(py, TrapKind::YieldThen(result))
            }
            Some(result) => Ok(Stepped::Return(result)),
            None => {
                if !lane::is_active() {
                    return Err(no_runtime_error());
                }
                let target = op.target();
                self.state = State::Waiting(op);
                suspend(py, TrapKind::Notified(target))
            }
        }
    }
}

/// Never resumed, thrown into or closed: the `await` was forgotten (see `unawaited.rs`).
impl Drop for ChannelOp {
    fn drop(&mut self) {
        if let State::Fresh(op) = &self.state {
            crate::unawaited::warn_never_awaited(op.call_name());
        }
    }
}

/// Yields a trap for `kind` to the driver.
fn suspend(py: Python<'_>, kind: TrapKind) -> PyResult<Stepped> {
    Ok(Stepped::Yield(
        Py::new(py, Trap::suspended(kind))?.into_any(),
    ))
}

fn not_started_error() -> PyErr {
    PyTypeError::new_err(
        "can't send a non-None value to a channel operation that has not started; the first \
         resume of an awaitable must be send(None)",
    )
}

fn reused_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: a channel operation was awaited again after it finished; call tx.send(...) or \
         rx.recv() again for each await",
    )
}

fn no_runtime_error() -> PyErr {
    NoRuntimeError::new_err(
        "porto: a channel operation had to wait outside a porto task; await it from code that \
         porto.run(main) runs, or use try_send/try_recv (or blocking_send/blocking_recv from \
         another thread)",
    )
}

/// The borrow failed: two callers resumed the same operation at once.
fn concurrent_resume_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: a channel operation was resumed from two places at once; await each \
         operation from one task only",
    )
}

#[pymethods]
impl ChannelOp {
    fn __await__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __iter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.resume(py, &py.None().into_bound(py))? {
            Stepped::Yield(trap) => Ok(trap),
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

    /// The driver gave the wait up before throwing in (T3); the operation is over.
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

    fn __repr__(&self) -> &'static str {
        match &self.state {
            State::Fresh(_) => "<porto._core._ChannelOp fresh>",
            State::Waiting(_) => "<porto._core._ChannelOp waiting>",
            State::Delivering => "<porto._core._ChannelOp delivering>",
            State::Done => "<porto._core._ChannelOp done>",
        }
    }
}

/// `am_send` for `_ChannelOp`: the same steps as `send`, with no `StopIteration` on completion.
///
/// # Safety
///
/// CPython calls it as `tp_as_async->am_send` of `_ChannelOp`, with an attached thread state,
/// `slf` a live `_ChannelOp`, `arg` a live object and `presult` writable.
pub(crate) unsafe extern "C" fn channel_op_am_send(
    slf: *mut ffi::PyObject,
    arg: *mut ffi::PyObject,
    presult: *mut *mut ffi::PyObject,
) -> ffi::PySendResult {
    guard_send(|| {
        // SAFETY: the caller is attached (am_send contract).
        let py = unsafe { Python::assume_attached() };
        // SAFETY: `slf` and `arg` are live, borrowed for the duration of the call; the slot is
        // only installed on `_ChannelOp`, so `slf` is one.
        let (slf, arg) = unsafe { (Borrowed::from_ptr(py, slf), Borrowed::from_ptr(py, arg)) };
        // SAFETY: see above.
        let op = unsafe { slf.cast_unchecked::<ChannelOp>() };
        let stepped = match op.try_borrow_mut() {
            Ok(mut op) => op.resume(py, &arg),
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
