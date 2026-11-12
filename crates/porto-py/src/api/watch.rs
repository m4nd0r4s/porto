//! `porto.watch[T](initial)`: `WatchSender` and `WatchReceiver` over `porto_core::sync::watch`
//! with `T = PyObj` (16-sync-primitives-and-channels.md §2).
//!
//! Sending never waits, so `tx.send(value)` is a plain call. A receiver reads the latest value
//! with `borrow()` (a new reference to the object, taken under the channel's lock, with no
//! Python code run) and waits for a newer version with `await rx.changed()`, which parks the
//! task's embedded wait node on the channel through a `Notified` trap
//! ([`channel_op`](super::channel_op)).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::WaitNode;
use porto_core::sync::watch::{self, SendError, TryRecvError};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

use super::channel::{PyObj, generic_alias};
use super::channel_op::{ChannelOp, Op};
use crate::driver::ready::Ready;
use crate::driver::thin::{WaitSource, Waitable};
use crate::errors::{BrokenResourceError, ClosedResourceError, EndOfChannel};

/// Adds the handles and the constructor behind `porto.watch`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<WatchSender>()?;
    module.add_class::<WatchReceiver>()?;
    module.add_function(wrap_pyfunction!(new_watch, module)?)
}

/// `porto.watch[T](initial)`.
#[pyfunction(name = "_watch")]
#[pyo3(signature = (initial, /))]
fn new_watch(initial: PyObj) -> (WatchSender, WatchReceiver) {
    let (tx, rx) = watch::channel(initial);
    (WatchSender { tx }, WatchReceiver::wrap(rx))
}

/// The receiving side as a wait source: a task waiting for a change parks on it.
pub(super) struct WatchRecvSide(pub(super) watch::Receiver<PyObj>);

// SAFETY: forwards to `Receiver::poll_ready`/`cancel_ready`, whose contract is `WaitSource`'s:
// the node is used with this channel only while it waits, and the `WaitTarget` that the
// waiting task holds keeps this handle (so the channel) alive until then.
unsafe impl WaitSource for WatchRecvSide {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract (see the impl).
        unsafe { self.0.poll_ready(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: as above.
        unsafe { self.0.cancel_ready(node) }
    }
}

/// The sending end of a watch channel. Clone it for more producers.
#[pyclass(frozen, module = "porto._core", name = "WatchSender")]
pub(crate) struct WatchSender {
    tx: watch::Sender<PyObj>,
}

#[pymethods]
impl WatchSender {
    /// Makes `value` the latest value (a new version) and wakes the waiting receivers: never
    /// waits, so it is not awaited. Raises `porto.BrokenResourceError` if no receiver is open
    /// (`subscribe()` makes one), `porto.ClosedResourceError` if this sender was closed.
    #[pyo3(signature = (value, /))]
    fn send(&self, value: PyObj) -> PyResult<()> {
        self.tx.send(value).map_err(|error| match error {
            SendError::Closed(_) => ClosedResourceError::new_err(
                "porto: this watch sender was closed (or cloned from a sender that was closed), \
                 so it cannot send any more; send through a clone that is still open, or close \
                 the sender only once sending is done",
            ),
            SendError::Broken(_) => BrokenResourceError::new_err(
                "porto: every receiver of this watch channel was closed or dropped, so nothing \
                 can see the value (it was not stored); call tx.subscribe() to make a new \
                 receiver, or stop sending",
            ),
        })
    }

    /// A new receiver that has seen the current version: it waits for the next send. Works while
    /// no other receiver is open.
    fn subscribe(&self) -> PyResult<WatchReceiver> {
        self.tx.subscribe().map(WatchReceiver::wrap).ok_or_else(|| {
            ClosedResourceError::new_err(
                "porto: this watch sender was closed, so it cannot make receivers; subscribe \
                 through a sender (or receiver clone) that is still open",
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
    /// see the last value and then the end.
    fn close(&self) {
        self.tx.close();
    }

    /// Whether sends through this sender fail: it was closed, or no receiver is open.
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
        let state = if self.tx.is_closed() { " closed" } else { "" };
        format!("<porto.WatchSender{state}>")
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias(cls, item)
    }
}

/// The receiving end of a watch channel. Clone it for more watchers.
#[pyclass(frozen, module = "porto._core", name = "WatchReceiver")]
pub(crate) struct WatchReceiver {
    side: Arc<Waitable<WatchRecvSide>>,
}

impl WatchReceiver {
    fn wrap(rx: watch::Receiver<PyObj>) -> Self {
        Self {
            side: Waitable::new(WatchRecvSide(rx)),
        }
    }

    /// Fails with `ClosedResourceError` if this handle was closed.
    fn check_open(&self) -> PyResult<()> {
        if self.side.0.is_handle_closed() {
            return Err(closed_error());
        }
        Ok(())
    }
}

#[pymethods]
impl WatchReceiver {
    /// The latest value, without marking it seen. After the channel closed it is the last
    /// value. Raises `porto.ClosedResourceError` if this receiver was closed.
    fn borrow(&self, py: Python<'_>) -> PyResult<PyObj> {
        self.check_open()?;
        Ok(self.side.0.borrow().clone_ref(py))
    }

    /// The latest value, marking its version seen (the next `changed()` waits for a newer one).
    fn borrow_and_update(&self, py: Python<'_>) -> PyResult<PyObj> {
        self.check_open()?;
        Ok(self.side.0.borrow_and_update().clone_ref(py))
    }

    /// Whether a version this receiver has not seen exists.
    fn has_changed(&self) -> PyResult<bool> {
        self.check_open()?;
        Ok(self.side.0.has_changed())
    }

    /// `await rx.changed()`: waits for a version newer than the last this receiver saw, and
    /// marks it seen; sends before the call coalesce into one completion. Raises
    /// `porto.EndOfChannel` once every sender is closed and no unseen version is left.
    fn changed(&self) -> ChannelOp {
        ChannelOp::new(Op::WatchChanged {
            side: self.side.clone(),
        })
    }

    /// `changed()` without waiting: raises `porto.WouldBlock` if there is no unseen version.
    fn try_changed(&self) -> PyResult<()> {
        let rx = &self.side.0;
        match rx.try_changed() {
            Ok(()) => Ok(()),
            Err(TryRecvError::Empty) => Err(empty_changed_error()),
            Err(TryRecvError::Closed) => Err(recv_closed_error(rx)),
        }
    }

    /// `changed()` from a thread that does not run porto, blocking it until a newer version
    /// exists. Detaches from the Python thread state while it waits.
    fn blocking_changed(&self, py: Python<'_>) -> PyResult<()> {
        let rx = &self.side.0;
        match py.detach(|| rx.blocking_changed()) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(recv_closed_error(rx)),
            Err(on_lane) => Err(PyRuntimeError::new_err(format!("porto: {on_lane}"))),
        }
    }

    /// Another receiver on the same channel, which has seen what this one has.
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

    /// `async for value in rx`: waits for each new version and yields the latest value; the
    /// end of the channel ends the loop. The initial value is not yielded.
    fn __anext__(&self) -> ChannelOp {
        ChannelOp::new(Op::WatchNext {
            side: self.side.clone(),
        })
    }

    fn __repr__(&self) -> String {
        let state = if self.side.0.is_handle_closed() {
            " closed"
        } else {
            ""
        };
        format!("<porto.WatchReceiver{state}>")
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias(cls, item)
    }
}

fn closed_error() -> PyErr {
    ClosedResourceError::new_err(
        "porto: this watch receiver was closed (or cloned from a receiver that was closed), so \
         it cannot be used any more; use a receiver that is still open, or make one with \
         tx.subscribe()",
    )
}

fn empty_changed_error() -> PyErr {
    crate::errors::WouldBlock::new_err(
        "porto: no newer value has been sent; await rx.changed() to wait for one, or retry \
         try_changed later",
    )
}

/// Why a wait for a change through `rx` failed: this handle was closed, or the channel ended.
pub(super) fn recv_closed_error(rx: &watch::Receiver<PyObj>) -> PyErr {
    if rx.is_handle_closed() {
        closed_error()
    } else {
        EndOfChannel::new_err(
            "porto: the watch channel ended: every sender was closed or dropped and every \
             change was seen; stop waiting (rx.borrow() still returns the last value)",
        )
    }
}
