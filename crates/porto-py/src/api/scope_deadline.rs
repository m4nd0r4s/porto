//! A scope's deadline from Python (15-structured-concurrency-and-cancellation.md C3, C9):
//! `porto.scope(timeout=, deadline=)`, `porto.timeout()`, `porto.move_on_after()` and the
//! read/write `Scope.deadline`.
//!
//! Deadlines are `float` seconds on the runtime clock (`porto.current_time()`), `inf` for none.
//! Until the block is entered, the scope keeps what it was given here: a `timeout=` counts from
//! the entry, which turns it into a deadline (the earlier of the two when both are given). From
//! then on the scope's node holds the deadline and arms its timer (`ScopeNode::set_deadline`):
//! once it passes, the scope is cancelled with `CancelReason::Deadline`. Entering and setting
//! the deadline take the same lock, so a deadline set from another thread while the block is
//! entered is never lost or overtaken by the one given before.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use porto_core::scope::ScopeNode;
use porto_core::time::Deadline;
use porto_core::{Handle, WeakHandle};
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;

use super::time::deadline_at;
use crate::driver::pytask::{PyScopeNode, PyScopeRef};

/// The deadline part of a `Scope`.
pub(super) struct ScopeDeadline {
    state: Mutex<Pending>,
}

struct Pending {
    /// The absolute deadline given (`deadline=` or set before the entry), `inf` for none.
    deadline: f64,
    /// `timeout=`: seconds from the entry.
    timeout: Option<f64>,
    /// The runtime the block was entered on: the node's deadline is set on its clock from
    /// then on. `None` before the entry, and for the root scope, which has no deadline. Weak: a
    /// scope that outlives its run must not keep the runtime's state alive, and a deadline set
    /// after the run ended has nothing to arm on (it is recorded, inert).
    entered_on: Option<WeakHandle>,
}

impl ScopeDeadline {
    /// `timeout` (seconds from the entry) and `deadline` (on the runtime clock), checked by
    /// [`seconds`] already.
    pub(super) fn new(timeout: Option<f64>, deadline: Option<f64>) -> Self {
        Self {
            state: Mutex::new(Pending {
                deadline: deadline.unwrap_or(f64::INFINITY),
                timeout,
                entered_on: None,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Pending> {
        // Only plain values are written under it, and no Python code runs under it.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The block was entered on `handle`'s runtime: arms the deadline given so far on `node`
    /// (a deadline already passed cancels it now).
    pub(super) fn enter(&self, node: &PyScopeRef, handle: &Handle) {
        let mut pending = self.lock();
        let now = handle.now();
        let mut deadline = absolute(now, pending.deadline);
        if let Some(timeout) = pending.timeout {
            deadline = deadline.min(after(now, timeout));
        }
        pending.entered_on = Some(handle.downgrade());
        // Under the lock, so a `deadline` set meanwhile from another thread comes after it.
        ScopeNode::set_deadline(node, handle, deadline);
    }

    /// `Scope.deadline`: the node's once entered, else the deadline given (a `timeout=` is
    /// not counted until the entry).
    pub(super) fn get(&self, node: Option<&PyScopeNode>) -> f64 {
        let pending = self.lock();
        match (
            node,
            pending.entered_on.as_ref().and_then(WeakHandle::upgrade),
        ) {
            (Some(node), Some(_)) => seconds_of(node.deadline()),
            _ => pending.deadline,
        }
    }

    /// `Scope.deadline = seconds`: re-arms the timer of an entered scope (C9); before the
    /// entry it replaces both `deadline=` and `timeout=`.
    pub(super) fn set(&self, node: Option<&PyScopeRef>, seconds: f64) {
        let mut pending = self.lock();
        let handle = pending.entered_on.as_ref().map(WeakHandle::upgrade);
        match (node, handle) {
            (Some(node), Some(Some(handle))) => {
                let deadline = absolute(handle.now(), seconds);
                ScopeNode::set_deadline(node, &handle, deadline);
            }
            // Entered, but its runtime is gone: no clock and no wheel, so the deadline is only
            // recorded (and reads back), and nothing is armed or kept alive.
            (Some(_), Some(None)) => pending.deadline = seconds,
            _ => {
                pending.deadline = seconds;
                pending.timeout = None;
            }
        }
    }
}

/// Checks a number of seconds given to `function` as `argument`: NaN is no time at all.
pub(super) fn seconds(function: &str, argument: &str, value: f64) -> PyResult<f64> {
    if value.is_nan() {
        return Err(PyValueError::new_err(format!(
            "porto: {function} got {argument}=NaN, which is no point in time; pass a number of \
             seconds (0 or less expires at once, inf never does), or leave it out for no \
             deadline"
        )));
    }
    Ok(value)
}

/// The deadline `seconds` from `now`: `now` itself for 0 or less, never for `inf` (or a delay
/// beyond the clock).
fn after(now: Deadline, seconds: f64) -> Deadline {
    if seconds <= 0.0 {
        return now;
    }
    Duration::try_from_secs_f64(seconds).map_or(Deadline::NEVER, |delay| now.saturating_add(delay))
}

/// The deadline at `seconds` on the runtime clock (not NaN), rounded up: never early.
fn absolute(now: Deadline, seconds: f64) -> Deadline {
    if seconds == f64::INFINITY {
        return Deadline::NEVER;
    }
    deadline_at(now, seconds)
}

/// A deadline as `Scope.deadline` reads it: seconds on the runtime clock, `inf` for none.
fn seconds_of(deadline: Deadline) -> f64 {
    if deadline == Deadline::NEVER {
        f64::INFINITY
    } else {
        deadline.as_secs_f64()
    }
}

/// What a scope whose deadline cut its block short raises instead of absorbing the
/// `Cancelled` (C3, `raise_on_timeout=True`), chained to that `Cancelled` if the block let one
/// out.
pub(super) fn timeout_error(py: Python<'_>, cancelled: &Bound<'_, PyAny>) -> PyResult<PyErr> {
    let error = PyTimeoutError::new_err(
        "porto: the scope's deadline passed before its block finished (porto.timeout(), or \
         porto.scope(timeout=..., deadline=...)); give it more time, or use \
         porto.move_on_after() to go on without raising",
    );
    if !cancelled.is_none() {
        error.value(py).setattr("__context__", cancelled)?;
    }
    Ok(error)
}

/// What setting the root scope's deadline raises: it has none.
pub(super) fn root_has_no_deadline() -> PyErr {
    PyRuntimeError::new_err(
        "porto: the root scope has no deadline; put one on a scope inside main: \
         `async with porto.timeout(seconds):`",
    )
}
