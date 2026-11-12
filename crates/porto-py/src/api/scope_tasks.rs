//! `Scope.tasks` (20-python-api-reference.md "Structured concurrency"): the tasks a scope lists,
//! derived from the lane's task registry (23-observability.md §1).
//!
//! A scope keeps no list of its own tasks. Reading `Scope.tasks` walks the registry of the lane
//! the caller runs on, which holds every live task, and keeps the Python tasks that count in
//! the scope (the header's scope slot, which `Scope.start`'s move changes) and have a
//! `porto.Task` handle, in spawn order. Reading is O(live tasks); spawning a task into a scope,
//! moving it, and its end cost nothing for it, which is why this replaced the intrusive
//! per-scope list of PR #62 (porto-szb.43, porto-szb.142).
//!
//! The walk runs on the lane's thread, which is the only one that links, unlinks and frees tasks,
//! while the callback only reads: so a task seen is alive for the callback, and a task other
//! than the running one is not being polled, which is what reading its body's `py_handle` needs.
//! The running one (the reader itself, when it is a child of the scope) is read through the
//! step driver's record of it instead, since its poll holds the body.

use pyo3::Borrowed;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::driver::pytask::{PyScopeRef, PyTask};
use crate::driver::{lane, step};
use crate::errors::NoRuntimeError;
use porto_core::task::TaskFlags;

/// The `porto.Task` of every live task spawned into `scope` (or moved into it), oldest first.
///
/// # Errors
/// `NoRuntimeError` when the caller is not on the lane of the run (a task, or `main`): the lane
/// owns the registry.
pub(super) fn scope_tasks<'py>(
    py: Python<'py>,
    scope: &PyScopeRef,
) -> PyResult<Bound<'py, PyTuple>> {
    let handle = lane::current().ok_or_else(not_on_the_lane)?;
    let mut found: Vec<(u64, Bound<'py, PyAny>)> = Vec::new();
    let walked = handle.with_live_tasks(|task| {
        if !task.flags().contains(TaskFlags::PYTHON) || !task.is_in_scope(scope) {
            return;
        }
        let object = match step::running_handle_of(task.header()) {
            Some(running) => running,
            // SAFETY: a `PYTHON` task's body is a `PyTask`, and a task that is not the running
            // one is between polls (the walk is on the lane, whose polls do not nest).
            None => unsafe { PyTask::handle_ptr(task.body::<PyTask>()) },
        };
        if object.is_null() {
            // An internal spawn: it has no handle to list.
            return;
        }
        // SAFETY: a borrowed pointer to the `porto.Task` the body (or the running poll) holds
        // for as long as the task lives; the new reference outlives the walk.
        let object = unsafe { Borrowed::from_ptr(py, object) }.to_owned();
        found.push((task.id(), object));
    });
    if !walked {
        return Err(not_on_the_lane());
    }
    found.sort_unstable_by_key(|(id, _)| *id);
    PyTuple::new(py, found.into_iter().map(|(_, object)| object))
}

fn not_on_the_lane() -> PyErr {
    NoRuntimeError::new_err(
        "porto: Scope.tasks of an open scope must be read from a task running under \
         porto.run(): the tasks live on the run's lane; read it inside the scope's block, or \
         from any task of the same run",
    )
}
