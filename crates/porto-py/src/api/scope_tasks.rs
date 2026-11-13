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

use porto_core::LiveTask;
use pyo3::Borrowed;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::driver::pytask::{PyScopeRef, PyTask};
use crate::driver::{lane, step};
use crate::errors::NoRuntimeError;
use porto_core::task::TaskFlags;

/// The `porto.Task` of every live task spawned into `scope` (or moved into it), oldest first.
/// `run` is the id of the run that entered the scope.
///
/// The walk stops once it has seen as many tasks of the scope as the scope counts
/// (`live_children`), and a scope with none skips it, so the cost is the distance to the scope's
/// oldest live task from the newest in the registry: O(live tasks of the run) at worst (about 30 ns
/// each), a few tasks for a scope of recent children.
///
/// While the runtime is dropped (a `finally` of a task it cancels reads `tasks`) the registry is
/// closed and walked under its lock instead.
///
/// # Errors
/// `NoRuntimeError` when the caller is not in a run (the lane owns the registry), and
/// `RuntimeError` when the scope belongs to another run.
pub(super) fn scope_tasks<'py>(
    py: Python<'py>,
    scope: &PyScopeRef,
    run: usize,
) -> PyResult<Bound<'py, PyTuple>> {
    let current = lane::run_id();
    if current == 0 {
        return Err(not_on_the_lane());
    }
    if run != current {
        return Err(PyRuntimeError::new_err(
            "porto: this scope was entered in another porto.run(), so its tasks are not in this \
             run's registry: read Scope.tasks from a task of the run that entered the scope",
        ));
    }
    let live = scope.lock_state().live_children;
    if live == 0 {
        return Ok(PyTuple::empty(py));
    }
    let handle = lane::current()
        .or_else(lane::teardown_handle)
        .ok_or_else(not_on_the_lane)?;
    let mut found: Vec<(u64, Bound<'py, PyAny>)> = Vec::new();
    let mut in_scope = 0;
    let mut visit = |task: LiveTask<'_>| {
        if !task.flags().contains(TaskFlags::PYTHON) || !task.is_in_scope(scope) {
            return true;
        }
        in_scope += 1;
        let object = match step::running_handle_of(task.header()) {
            Some(running) => running,
            // SAFETY: a `PYTHON` task's body is a `PyTask`, and a task that is not the running
            // one is between polls (the walk is on the lane, whose polls do not nest; at
            // shutdown the registry skips tasks another thread polls).
            None => unsafe { PyTask::handle_ptr(task.body::<PyTask>()) },
        };
        // An internal spawn has no handle to list.
        if !object.is_null() {
            // SAFETY: a borrowed pointer to the `porto.Task` the body (or the running poll)
            // holds for as long as the task lives; the new reference outlives the walk.
            let object = unsafe { Borrowed::from_ptr(py, object) }.to_owned();
            found.push((task.id(), object));
        }
        in_scope < live
    };
    let walked = handle.with_live_tasks_while(&mut visit)
        || handle.with_live_tasks_at_shutdown(step::running_header(), &mut visit);
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
