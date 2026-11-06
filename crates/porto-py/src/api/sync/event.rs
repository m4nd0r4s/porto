//! `porto.Event` and `porto.Notify`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::{self as core_sync, WaitNode};
use pyo3::prelude::*;

use super::blocking;
use super::wait_op::{Grant, SyncWait};
use crate::api::time::require_lane;
use crate::driver::thin::{WaitSource, Waitable};

pub(super) struct EventSource(pub(super) core_sync::Event);

// SAFETY: forwards to the event's node API, whose contract is `WaitSource`'s.
unsafe impl WaitSource for EventSource {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.0.poll_wait_node(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.0.cancel_wait_node(node) }
    }
}

/// `porto.Event()`: a flag that starts unset and, once set, stays set; every waiter, current
/// and future, is released. Not clearable (Trio semantics).
#[pyclass(frozen, module = "porto", name = "Event")]
pub(super) struct Event {
    event: Arc<Waitable<EventSource>>,
}

#[pymethods]
impl Event {
    #[new]
    fn new() -> Self {
        Self {
            event: Waitable::new(EventSource(core_sync::Event::new())),
        }
    }

    /// Sets the event and wakes every task waiting for it. Setting it again does nothing.
    fn set(&self) {
        self.event.0.set();
    }

    /// Whether the event is set.
    fn is_set(&self) -> bool {
        self.event.0.is_set()
    }

    /// An awaitable that completes once the event is set: without suspending if it is.
    /// Nothing happens until it is awaited.
    fn wait(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("Event.wait")?;
        SyncWait::new(Grant::EventSet(Arc::clone(&self.event))).into_object(py)
    }

    /// Blocks the calling thread until the event is set. Only for threads that do not run
    /// porto: on a lane thread it raises `RuntimeError`. Runs detached, without signal checks:
    /// Ctrl-C does not interrupt it.
    fn blocking_wait(&self, py: Python<'_>) -> PyResult<()> {
        blocking(py, "Event.blocking_wait", || self.event.0.blocking_wait())
    }

    fn __repr__(&self) -> String {
        let state = if self.event.0.is_set() {
            "set"
        } else {
            "unset"
        };
        format!("<porto.Event {state}>")
    }
}

pub(super) struct NotifySource(pub(super) core_sync::Notify);

// SAFETY: forwards to the notify's node API, whose contract is `WaitSource`'s.
unsafe impl WaitSource for NotifySource {
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the caller's contract.
        unsafe { self.0.poll_notified_node(node, cx) }
    }

    unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: the caller's contract.
        unsafe { self.0.cancel_notified_node(node) }
    }
}

/// `porto.Notify()`: `notify_one()` wakes the longest waiting task or stores one permit for
/// the next waiter; `notify_all()` wakes the tasks waiting now and stores nothing.
#[pyclass(frozen, module = "porto", name = "Notify")]
pub(super) struct Notify {
    notify: Arc<Waitable<NotifySource>>,
}

#[pymethods]
impl Notify {
    #[new]
    fn new() -> Self {
        Self {
            notify: Waitable::new(NotifySource(core_sync::Notify::new())),
        }
    }

    /// Wakes the task that has waited longest or, if none waits, stores a permit for the next
    /// `notified()` (at most one).
    fn notify_one(&self) {
        self.notify.0.notify_one();
    }

    /// Wakes every task waiting now. Stores no permit.
    fn notify_all(&self) {
        self.notify.0.notify_all();
    }

    /// An awaitable that completes when notified: at once if a permit is stored (consuming
    /// it), otherwise when a `notify_one` or `notify_all` comes after the `await` started.
    /// Nothing happens until it is awaited: an un-awaited one consumes no permit.
    fn notified(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("Notify.notified")?;
        SyncWait::new(Grant::Notified(Arc::clone(&self.notify))).into_object(py)
    }

    /// Blocks the calling thread until notified. Only for threads that do not run porto: on a
    /// lane thread it raises `RuntimeError`. Runs detached, without signal checks: Ctrl-C does
    /// not interrupt it.
    fn blocking_notified(&self, py: Python<'_>) -> PyResult<()> {
        blocking(py, "Notify.blocking_notified", || {
            self.notify.0.blocking_notified()
        })
    }

    fn __repr__(&self) -> &'static str {
        "<porto.Notify>"
    }
}
