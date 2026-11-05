//! `porto.channel[T](capacity)` and `porto.unbounded_channel[T]()`: the `Sender` and
//! `Receiver` handles over `porto_core::sync::mpsc` with `T = PyObj`
//! (16-sync-primitives-and-channels.md §2.1-§2.3).
//!
//! Each Python handle owns one core handle, shared (`Arc<Waitable<_>>`) with the waits in
//! progress on it: a task that waits to send or receive parks its embedded wait node on the
//! channel's waiter list through a `Notified` trap whose target is the handle
//! ([`channel_op`](super::channel_op)), so a wait allocates nothing but the trap. When the
//! last reference to a handle goes (the Python object and any wait on it), the core handle
//! drops, which closes it (§2.3: dropping is closing, on refcount release).
//!
//! The `blocking_*` methods are for threads that do not run porto: they detach from the
//! Python thread state while they wait, and refuse with `RuntimeError` on a lane thread.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::mpsc::{self, MAX_CAPACITY, TryRecvError, TrySendError};
use porto_core::sync::{WaitNode, blocking_wait};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyInt, PyList, PyTuple, PyType};

use super::channel_op::{ChannelOp, Op};
use crate::driver::ready::Ready;
use crate::driver::thin::{WaitSource, Waitable};
use crate::errors::{BrokenResourceError, ClosedResourceError, EndOfChannel, WouldBlock};

/// What a channel carries: any Python object, moved between tasks (and lanes) without a copy.
pub(super) type PyObj = Py<PyAny>;

/// Adds the handles and the constructors behind `porto.channel` and `porto.unbounded_channel`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Sender>()?;
    module.add_class::<Receiver>()?;
    module.add_function(wrap_pyfunction!(new_channel, module)?)?;
    module.add_function(wrap_pyfunction!(new_unbounded_channel, module)?)
}

/// `porto.channel[T](capacity)`: a bounded channel. The capacity is checked here, so the core
/// constructor's assertions never fire.
#[pyfunction(name = "_channel")]
#[pyo3(signature = (capacity, /))]
fn new_channel(capacity: &Bound<'_, PyAny>) -> PyResult<(Sender, Receiver)> {
    let capacity = channel_capacity(capacity)?;
    Ok(pair(mpsc::channel(capacity)))
}

/// `porto.unbounded_channel[T]()`.
#[pyfunction(name = "_unbounded_channel")]
fn new_unbounded_channel() -> (Sender, Receiver) {
    pair(mpsc::unbounded_channel())
}

fn pair((tx, rx): (mpsc::Sender<PyObj>, mpsc::Receiver<PyObj>)) -> (Sender, Receiver) {
    (Sender::wrap(tx), Receiver::wrap(rx))
}

/// The capacity of `porto.channel`: an `int` from 1 to the core limit.
fn channel_capacity(capacity: &Bound<'_, PyAny>) -> PyResult<usize> {
    if !capacity.is_instance_of::<PyInt>() || capacity.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(format!(
            "porto: the channel capacity must be an int, got {}; pass the number of values the \
             channel may buffer, as in porto.channel[T](64)",
            capacity.get_type().name()?
        )));
    }
    match capacity.extract::<usize>() {
        Ok(capacity @ 1..=MAX_CAPACITY) => Ok(capacity),
        _ => Err(PyValueError::new_err(format!(
            "porto: the channel capacity must be between 1 and {MAX_CAPACITY}, got {capacity}; \
             pass the number of values the channel may buffer (it is allocated up front), or \
             use porto.unbounded_channel[T]() for no limit"
        ))),
    }
}

/// The sending side as a wait source: a task waiting for room parks on it.
pub(super) struct SendSide(pub(super) mpsc::Sender<PyObj>);

// SAFETY: forwards to `Sender::poll_ready`/`cancel_ready`, whose contract is `WaitSource`'s:
// the node is used with this channel only while it waits, and the `WaitTarget` that the
// waiting task holds keeps this handle (so the channel) alive until then.
unsafe impl WaitSource for SendSide {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract (see the impl).
        unsafe { self.0.poll_ready(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: as above.
        unsafe { self.0.cancel_ready(node) }
    }
}

/// The receiving side as a wait source: a task waiting for a value parks on it.
pub(super) struct RecvSide(pub(super) mpsc::Receiver<PyObj>);

// SAFETY: as for `SendSide`, with `Receiver::poll_ready`/`cancel_ready`.
unsafe impl WaitSource for RecvSide {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract (see the impl).
        unsafe { self.0.poll_ready(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: as above.
        unsafe { self.0.cancel_ready(node) }
    }
}

/// The sending end of a channel. Clone it for more producers; close it (or drop it) when done.
#[pyclass(frozen, module = "porto._core", name = "Sender")]
pub(crate) struct Sender {
    side: Arc<Waitable<SendSide>>,
}

impl Sender {
    fn wrap(tx: mpsc::Sender<PyObj>) -> Self {
        Self {
            side: Waitable::new(SendSide(tx)),
        }
    }
}

#[pymethods]
impl Sender {
    /// `await tx.send(value)`: sends, waiting for room while the channel is full.
    #[pyo3(signature = (value, /))]
    fn send(&self, value: PyObj) -> ChannelOp {
        ChannelOp::new(Op::Send {
            side: self.side.clone(),
            value: Some(value),
        })
    }

    /// Sends `value` if there is room now; raises `porto.WouldBlock` if the channel is full.
    #[pyo3(signature = (value, /))]
    fn try_send(&self, value: PyObj) -> PyResult<()> {
        let tx = &self.side.0;
        match tx.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(full_error(tx)),
            Err(TrySendError::Closed(_)) => Err(send_closed_error(tx)),
        }
    }

    /// Sends `value` from a thread that does not run porto, blocking it while the channel is
    /// full. Detaches from the Python thread state while it waits.
    #[pyo3(signature = (value, /))]
    fn blocking_send(&self, py: Python<'_>, value: PyObj) -> PyResult<()> {
        let tx = &self.side.0;
        // The unsent value comes back out of `detach`, so it is dropped attached.
        match py.detach(|| blocking_wait("Sender.blocking_send", tx.send(value))) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(send_closed_error(tx)),
            Err(on_lane) => Err(PyRuntimeError::new_err(format!("porto: {on_lane}"))),
        }
    }

    /// Another sender on the same channel. A clone of a closed sender is closed.
    fn clone(&self) -> Self {
        Self::wrap(self.side.0.clone())
    }

    /// Closes this sender: sends through it fail from now on; other clones are unaffected.
    /// Once every sender is closed, receivers drain the buffer and then see the end.
    fn close(&self) {
        self.side.0.close();
    }

    /// Whether sends through this sender fail: it was closed, or every receiver is gone.
    fn is_closed(&self) -> bool {
        self.side.0.is_closed()
    }

    /// The channel's capacity, `None` for an unbounded channel.
    #[getter]
    fn capacity(&self) -> Option<usize> {
        self.side.0.capacity()
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
        handle_repr("Sender", self.side.0.capacity(), self.side.0.is_closed())
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias(cls, item)
    }
}

/// The receiving end of a channel. Clone it for more consumers: each value goes to one.
#[pyclass(frozen, module = "porto._core", name = "Receiver")]
pub(crate) struct Receiver {
    side: Arc<Waitable<RecvSide>>,
}

impl Receiver {
    fn wrap(rx: mpsc::Receiver<PyObj>) -> Self {
        Self {
            side: Waitable::new(RecvSide(rx)),
        }
    }
}

#[pymethods]
impl Receiver {
    /// `await rx.recv()`: the next value, waiting while the channel is empty. Raises
    /// `porto.EndOfChannel` once every sender is closed and the buffer is drained.
    fn recv(&self) -> ChannelOp {
        ChannelOp::new(Op::Recv {
            side: self.side.clone(),
        })
    }

    /// `await rx.recv_many(max)`: waits for at least one value, then returns up to `max` of
    /// the buffered values as a list, without waiting again.
    #[pyo3(signature = (max, /))]
    fn recv_many(&self, max: usize) -> PyResult<ChannelOp> {
        if max == 0 {
            return Err(PyValueError::new_err(
                "porto: recv_many(max) needs max >= 1, got 0; pass how many values one call may \
                 return at most",
            ));
        }
        Ok(ChannelOp::new(Op::RecvMany {
            side: self.side.clone(),
            max,
        }))
    }

    /// The next value if one is buffered now; raises `porto.WouldBlock` if the channel is
    /// empty, `porto.EndOfChannel` if it ended.
    fn try_recv(&self) -> PyResult<PyObj> {
        let rx = &self.side.0;
        match rx.try_recv() {
            Ok(value) => Ok(value),
            Err(TryRecvError::Empty) => Err(empty_error()),
            Err(TryRecvError::Closed) => Err(recv_closed_error(rx)),
        }
    }

    /// The next value, from a thread that does not run porto, blocking it while the channel is
    /// empty. Detaches from the Python thread state while it waits.
    fn blocking_recv(&self, py: Python<'_>) -> PyResult<PyObj> {
        let rx = &self.side.0;
        match py.detach(|| blocking_wait("Receiver.blocking_recv", rx.recv())) {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(recv_closed_error(rx)),
            Err(on_lane) => Err(PyRuntimeError::new_err(format!("porto: {on_lane}"))),
        }
    }

    /// Another receiver on the same channel. A clone of a closed receiver is closed.
    fn clone(&self) -> Self {
        Self::wrap(self.side.0.clone())
    }

    /// Closes this receiver; other clones are unaffected. Once every receiver is closed, sends
    /// raise `porto.BrokenResourceError` and the buffered values are dropped.
    fn close(&self) {
        self.side.0.close();
    }

    fn __aiter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    /// `async for value in rx`: like `recv()`, but the end of the channel ends the loop.
    fn __anext__(&self) -> ChannelOp {
        ChannelOp::new(Op::Next {
            side: self.side.clone(),
        })
    }

    fn __repr__(&self) -> String {
        handle_repr(
            "Receiver",
            self.side.0.capacity(),
            self.side.0.is_handle_closed(),
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

/// `cls[item]` at runtime, so `porto.Sender[int]` works in annotations that are evaluated.
fn generic_alias<'py>(
    cls: &Bound<'py, PyType>,
    item: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    cls.py()
        .import("types")?
        .getattr("GenericAlias")?
        .call1((cls, item))
}

fn handle_repr(kind: &str, capacity: Option<usize>, closed: bool) -> String {
    let capacity = capacity.map_or_else(|| "unbounded".to_owned(), |n| format!("capacity={n}"));
    let state = if closed { " closed" } else { "" };
    format!("<porto.{kind} {capacity}{state}>")
}

pub(super) fn full_error(tx: &mpsc::Sender<PyObj>) -> PyErr {
    WouldBlock::new_err(format!(
        "porto: the channel is full (capacity {}); await tx.send(value) to wait for room, or \
         retry try_send later",
        tx.capacity().unwrap_or_default()
    ))
}

pub(super) fn empty_error() -> PyErr {
    WouldBlock::new_err(
        "porto: the channel is empty; await rx.recv() to wait for a value, or retry try_recv \
         later",
    )
}

/// Why a send through `tx` failed: this handle was closed, or every receiver is gone.
pub(super) fn send_closed_error(tx: &mpsc::Sender<PyObj>) -> PyErr {
    if tx.is_handle_closed() {
        ClosedResourceError::new_err(
            "porto: this sender was closed, so it cannot send any more; send through a clone \
             that is still open, or close the sender only once sending is done",
        )
    } else {
        BrokenResourceError::new_err(
            "porto: every receiver of this channel was closed or dropped, so nothing can \
             receive the value; keep a receiver open while sending, or stop sending when this \
             is raised",
        )
    }
}

/// Why a receive through `rx` failed: this handle was closed, or the channel ended.
pub(super) fn recv_closed_error(rx: &mpsc::Receiver<PyObj>) -> PyErr {
    if rx.is_handle_closed() {
        ClosedResourceError::new_err(
            "porto: this receiver was closed, so it cannot receive any more; receive through a \
             clone that is still open, or close the receiver only once receiving is done",
        )
    } else {
        EndOfChannel::new_err(
            "porto: the channel ended: every sender was closed or dropped and every value was \
             received; stop receiving (async for stops by itself)",
        )
    }
}

/// Up to `max` values as a list: `first`, then what is buffered now, without waiting.
pub(super) fn collect_many(
    py: Python<'_>,
    rx: &mpsc::Receiver<PyObj>,
    first: PyObj,
    max: usize,
) -> PyResult<PyObj> {
    let list = PyList::empty(py);
    list.append(first)?;
    while list.len() < max {
        let Ok(value) = rx.try_recv() else { break };
        list.append(value)?;
    }
    Ok(list.into_any().unbind())
}
