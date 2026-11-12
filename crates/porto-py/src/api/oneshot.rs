//! `porto.oneshot[T]()`: `OneshotSender` and `OneshotReceiver` over
//! `porto_core::sync::oneshot` with `T = PyObj` (16-sync-primitives-and-channels.md §2).
//!
//! Sending never waits, so `tx.send(value)` is a plain call, not an awaitable; the receiver's
//! `recv()`, `async for` and `try_recv()` follow the multi-producer channel's receiver
//! ([`channel`](super::channel)): a wait parks the task's embedded wait node on the channel
//! through a `Notified` trap ([`channel_op`](super::channel_op)).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::oneshot::{self, SendError, TryRecvError};
use porto_core::sync::{WaitNode, blocking_wait};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

use super::channel::{PyObj, empty_error, generic_alias};
use super::channel_op::{ChannelOp, Op};
use crate::driver::ready::Ready;
use crate::driver::thin::{WaitSource, Waitable};
use crate::errors::{BrokenResourceError, ClosedResourceError, EndOfChannel};

/// Adds the handles and the constructor behind `porto.oneshot`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<OneshotSender>()?;
    module.add_class::<OneshotReceiver>()?;
    module.add_function(wrap_pyfunction!(new_oneshot, module)?)
}

/// `porto.oneshot[T]()`.
#[pyfunction(name = "_oneshot")]
fn new_oneshot() -> (OneshotSender, OneshotReceiver) {
    let (tx, rx) = oneshot::channel();
    (
        OneshotSender { tx },
        OneshotReceiver {
            side: Waitable::new(OneshotRecvSide(rx)),
        },
    )
}

/// The receiving side as a wait source: a task waiting for the value parks on it.
pub(super) struct OneshotRecvSide(pub(super) oneshot::Receiver<PyObj>);

// SAFETY: forwards to `Receiver::poll_ready`/`cancel_ready`, whose contract is `WaitSource`'s:
// the node is used with this channel only while it waits, and the `WaitTarget` that the
// waiting task holds keeps this handle (so the channel) alive until then.
unsafe impl WaitSource for OneshotRecvSide {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract (see the impl).
        unsafe { self.0.poll_ready(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: as above.
        unsafe { self.0.cancel_ready(node) }
    }
}

/// The sending end of a oneshot channel: sends one value, then is used up.
#[pyclass(frozen, module = "porto._core", name = "OneshotSender")]
pub(crate) struct OneshotSender {
    tx: oneshot::Sender<PyObj>,
}

#[pymethods]
impl OneshotSender {
    /// Sends `value`: never waits, so it is not awaited. Uses the sender up. Raises
    /// `porto.BrokenResourceError` if the receiver is gone, `porto.ClosedResourceError` if this
    /// sender was closed or already sent.
    #[pyo3(signature = (value, /))]
    fn send(&self, value: PyObj) -> PyResult<()> {
        self.tx.send(value).map_err(|error| match error {
            SendError::Closed(_) => ClosedResourceError::new_err(
                "porto: this oneshot sender was closed or already sent its value, so it cannot \
                 send again; a oneshot channel carries exactly one value: create another with \
                 porto.oneshot[T]()",
            ),
            SendError::Broken(_) => BrokenResourceError::new_err(
                "porto: the receiver of this oneshot channel was closed or dropped, so nothing \
                 can receive the value; keep the receiver open until the value is sent",
            ),
        })
    }

    /// Closes this sender without sending: the receiver sees `porto.EndOfChannel`. Closing
    /// twice, or after sending, does nothing.
    fn close(&self) {
        self.tx.close();
    }

    /// Whether a send would fail: this sender was closed or used up, or the receiver is gone.
    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn __aenter__(slf: &Bound<'_, Self>) -> Ready {
        Ready::new(slf.clone().into_any().unbind())
    }

    /// Closes the sender on the way out of `async with`.
    #[pyo3(signature = (*_exc_info))]
    fn __aexit__(&self, py: Python<'_>, _exc_info: &Bound<'_, PyTuple>) -> Ready {
        self.close();
        Ready::new(py.None())
    }

    fn __repr__(&self) -> String {
        let state = if self.tx.is_handle_closed() {
            " closed"
        } else {
            ""
        };
        format!("<porto.OneshotSender{state}>")
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias(cls, item)
    }
}

/// The receiving end of a oneshot channel: receives the one value.
#[pyclass(frozen, module = "porto._core", name = "OneshotReceiver")]
pub(crate) struct OneshotReceiver {
    side: Arc<Waitable<OneshotRecvSide>>,
}

#[pymethods]
impl OneshotReceiver {
    /// `await rx.recv()`: the value, waiting until it is sent. Raises `porto.EndOfChannel` if
    /// the sender was closed or dropped without sending, or the value was already received.
    fn recv(&self) -> ChannelOp {
        ChannelOp::new(Op::OneshotRecv {
            side: self.side.clone(),
        })
    }

    /// The value if it was sent; raises `porto.WouldBlock` if not yet, `porto.EndOfChannel` if
    /// the channel ended.
    fn try_recv(&self) -> PyResult<PyObj> {
        let rx = &self.side.0;
        match rx.try_recv() {
            Ok(value) => Ok(value),
            Err(TryRecvError::Empty) => Err(empty_error()),
            Err(TryRecvError::Closed) => Err(recv_closed_error(rx)),
        }
    }

    /// The value, from a thread that does not run porto, blocking it until it is sent.
    /// Detaches from the Python thread state while it waits.
    fn blocking_recv(&self, py: Python<'_>) -> PyResult<PyObj> {
        let rx = &self.side.0;
        match py.detach(|| blocking_wait("OneshotReceiver.blocking_recv", rx.recv())) {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(recv_closed_error(rx)),
            Err(on_lane) => Err(PyRuntimeError::new_err(format!("porto: {on_lane}"))),
        }
    }

    /// Closes this receiver: sends fail from now on, and a value not yet received is dropped.
    fn close(&self) {
        self.side.0.close();
    }

    fn __aiter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    /// `async for value in rx`: yields the one value, then ends.
    fn __anext__(&self) -> ChannelOp {
        ChannelOp::new(Op::OneshotNext {
            side: self.side.clone(),
        })
    }

    fn __repr__(&self) -> String {
        let state = if self.side.0.is_handle_closed() {
            " closed"
        } else {
            ""
        };
        format!("<porto.OneshotReceiver{state}>")
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias(cls, item)
    }
}

/// Why a receive through `rx` failed: this handle was closed, or the channel ended.
pub(super) fn recv_closed_error(rx: &oneshot::Receiver<PyObj>) -> PyErr {
    if rx.is_handle_closed() {
        ClosedResourceError::new_err(
            "porto: this oneshot receiver was closed, so it cannot receive; create a new \
             channel with porto.oneshot[T]() instead",
        )
    } else {
        EndOfChannel::new_err(
            "porto: the oneshot channel ended: its sender was closed or dropped without \
             sending, or the value was already received; stop receiving",
        )
    }
}
