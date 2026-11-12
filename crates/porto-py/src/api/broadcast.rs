//! `porto.broadcast[T](capacity)`: `BroadcastSender` and `BroadcastReceiver` over
//! `porto_core::sync::broadcast` with `T = PyObj` (16-sync-primitives-and-channels.md §2).
//!
//! Sending never waits, so `tx.send(value)` is a plain call. Messages are shared in the ring
//! (`Arc<PyObj>`): a receive takes a counted reference under the channel's lock, with no
//! Python code run, and turns it into a new Python reference once the lock is released. A
//! receiver that fell behind raises `porto.Lagged(skipped)`; waits park the task's embedded
//! wait node on the channel through a `Notified` trap ([`channel_op`](super::channel_op)).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::broadcast::{self, MAX_CAPACITY, RecvError, SendError, TryRecvError};
use porto_core::sync::{WaitNode, blocking_wait};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyInt, PyTuple, PyType};

use super::channel::{PyObj, empty_error, generic_alias};
use super::channel_op::{ChannelOp, Op};
use crate::driver::ready::Ready;
use crate::driver::thin::{WaitSource, Waitable};
use crate::errors::{BrokenResourceError, ClosedResourceError, EndOfChannel, Lagged};

/// Adds the handles and the constructor behind `porto.broadcast`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<BroadcastSender>()?;
    module.add_class::<BroadcastReceiver>()?;
    module.add_function(wrap_pyfunction!(new_broadcast, module)?)
}

/// `porto.broadcast[T](capacity)`. The capacity is checked here, so the core constructor's
/// assertion never fires.
#[pyfunction(name = "_broadcast")]
#[pyo3(signature = (capacity, /))]
fn new_broadcast(capacity: &Bound<'_, PyAny>) -> PyResult<(BroadcastSender, BroadcastReceiver)> {
    let capacity = broadcast_capacity(capacity)?;
    let (tx, rx) = broadcast::channel(capacity);
    Ok((BroadcastSender { tx }, BroadcastReceiver::wrap(rx)))
}

/// The capacity of `porto.broadcast`: an `int` from 1 to the core limit.
fn broadcast_capacity(capacity: &Bound<'_, PyAny>) -> PyResult<usize> {
    if !capacity.is_instance_of::<PyInt>() || capacity.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(format!(
            "porto: the broadcast capacity must be an int, got {}; pass how many messages the \
             channel keeps for receivers that fall behind, as in porto.broadcast[T](64)",
            capacity.get_type().name()?
        )));
    }
    match capacity.extract::<usize>() {
        Ok(capacity @ 1..=MAX_CAPACITY) => Ok(capacity),
        _ => Err(PyValueError::new_err(format!(
            "porto: the broadcast capacity must be between 1 and {MAX_CAPACITY}, got {capacity}; \
             pass how many messages the channel keeps for receivers that fall behind"
        ))),
    }
}

/// The receiving side as a wait source: a task waiting for a message parks on it.
pub(super) struct BroadcastRecvSide(pub(super) broadcast::Receiver<PyObj>);

// SAFETY: forwards to `Receiver::poll_ready`/`cancel_ready`, whose contract is `WaitSource`'s:
// the node is used with this channel only while it waits, and the `WaitTarget` that the
// waiting task holds keeps this handle (so the channel) alive until then.
unsafe impl WaitSource for BroadcastRecvSide {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract (see the impl).
        unsafe { self.0.poll_ready(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: as above.
        unsafe { self.0.cancel_ready(node) }
    }
}

/// The sending end of a broadcast channel. Clone it for more producers.
#[pyclass(frozen, module = "porto._core", name = "BroadcastSender")]
pub(crate) struct BroadcastSender {
    tx: broadcast::Sender<PyObj>,
}

#[pymethods]
impl BroadcastSender {
    /// Sends `message` to every receiver: never waits, so it is not awaited. When the channel
    /// keeps `capacity` messages already, the oldest is overwritten and receivers that had not
    /// read it get `porto.Lagged`. Raises `porto.BrokenResourceError` if no receiver is open
    /// (`subscribe()` makes one), `porto.ClosedResourceError` if this sender was closed.
    #[pyo3(signature = (message, /))]
    fn send(&self, message: PyObj) -> PyResult<()> {
        self.tx.send(message).map_err(|error| match error {
            SendError::Closed(_) => ClosedResourceError::new_err(
                "porto: this broadcast sender was closed (or cloned from a sender that was \
                 closed), so it cannot send any more; send through a clone that is still open, \
                 or close the sender only once sending is done",
            ),
            SendError::Broken(_) => BrokenResourceError::new_err(
                "porto: every receiver of this broadcast channel was closed or dropped, so \
                 nobody would see the message (it was not kept); call tx.subscribe() to make \
                 a new receiver, or stop sending",
            ),
        })
    }

    /// A new receiver that starts at the next message sent: it sees nothing sent before. Works
    /// while no other receiver is open.
    fn subscribe(&self) -> PyResult<BroadcastReceiver> {
        self.tx
            .subscribe()
            .map(BroadcastReceiver::wrap)
            .ok_or_else(|| {
                ClosedResourceError::new_err(
                    "porto: this broadcast sender was closed, so it cannot make receivers; \
                     subscribe through a sender (or receiver clone) that is still open",
                )
            })
    }

    /// Another sender on the same channel. A clone of a closed sender is closed.
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }

    /// Closes this sender; other clones are unaffected. Once every sender is closed, receivers
    /// drain what is kept and then see the end.
    fn close(&self) {
        self.tx.close();
    }

    /// Whether sends through this sender fail: it was closed, or no receiver is open.
    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// How many messages the channel keeps for receivers that fall behind.
    #[getter]
    fn capacity(&self) -> usize {
        self.tx.capacity()
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
        let state = if self.tx.is_closed() { " closed" } else { "" };
        format!(
            "<porto.BroadcastSender capacity={}{state}>",
            self.tx.capacity()
        )
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias(cls, item)
    }
}

/// The receiving end of a broadcast channel. Clone it for another receiver at the same
/// position: each one gets every message.
#[pyclass(frozen, module = "porto._core", name = "BroadcastReceiver")]
pub(crate) struct BroadcastReceiver {
    side: Arc<Waitable<BroadcastRecvSide>>,
}

impl BroadcastReceiver {
    fn wrap(rx: broadcast::Receiver<PyObj>) -> Self {
        Self {
            side: Waitable::new(BroadcastRecvSide(rx)),
        }
    }
}

#[pymethods]
impl BroadcastReceiver {
    /// `await rx.recv()`: the next message, waiting while there is none. Raises
    /// `porto.Lagged(skipped)` if messages were overwritten before this receiver read them (the
    /// next call continues from the oldest one kept), `porto.EndOfChannel` once every sender is
    /// closed and every kept message was received.
    fn recv(&self) -> ChannelOp {
        ChannelOp::new(Op::BroadcastRecv {
            side: self.side.clone(),
        })
    }

    /// The next message if one was sent; raises `porto.WouldBlock` if nothing new was, and the
    /// errors of `recv`.
    fn try_recv(&self, py: Python<'_>) -> PyResult<PyObj> {
        let rx = &self.side.0;
        match rx.try_recv() {
            Ok(message) => Ok(message.clone_ref(py)),
            Err(TryRecvError::Empty) => Err(empty_error()),
            Err(TryRecvError::Closed) => Err(recv_closed_error(rx)),
            Err(TryRecvError::Lagged(skipped)) => Err(Lagged::new_err(skipped)),
        }
    }

    /// The next message, from a thread that does not run porto, blocking it while there is none.
    /// Detaches from the Python thread state while it waits.
    fn blocking_recv(&self, py: Python<'_>) -> PyResult<PyObj> {
        let rx = &self.side.0;
        match py.detach(|| blocking_wait("BroadcastReceiver.blocking_recv", rx.recv())) {
            Ok(Ok(message)) => Ok(message.clone_ref(py)),
            Ok(Err(RecvError::Closed)) => Err(recv_closed_error(rx)),
            Ok(Err(RecvError::Lagged(skipped))) => Err(Lagged::new_err(skipped)),
            Err(on_lane) => Err(PyRuntimeError::new_err(format!("porto: {on_lane}"))),
        }
    }

    /// Another receiver on the same channel, at this receiver's position: it also gets every
    /// message from there. A clone of a closed receiver is closed.
    fn clone(&self) -> Self {
        Self::wrap(self.side.0.clone())
    }

    /// Closes this receiver; other receivers are unaffected. Waits through it raise
    /// `porto.ClosedResourceError`.
    fn close(&self) {
        self.side.0.close();
    }

    fn __aiter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    /// `async for message in rx`: like `recv()`, but the end of the channel ends the loop. A
    /// lag raises `porto.Lagged` out of the loop; catch it around a new `async for` (or use
    /// `recv()` in a loop) to carry on from the oldest message kept.
    fn __anext__(&self) -> ChannelOp {
        ChannelOp::new(Op::BroadcastNext {
            side: self.side.clone(),
        })
    }

    fn __repr__(&self) -> String {
        let state = if self.side.0.is_handle_closed() {
            " closed"
        } else {
            ""
        };
        format!("<porto.BroadcastReceiver{state}>")
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
pub(super) fn recv_closed_error(rx: &broadcast::Receiver<PyObj>) -> PyErr {
    if rx.is_handle_closed() {
        ClosedResourceError::new_err(
            "porto: this broadcast receiver was closed (or cloned from a receiver that was \
             closed), so it cannot receive any more; receive through a receiver that is still \
             open, or make one with tx.subscribe()",
        )
    } else {
        EndOfChannel::new_err(
            "porto: the broadcast channel ended: every sender was closed or dropped and every \
             kept message was received; stop receiving (async for stops by itself)",
        )
    }
}
