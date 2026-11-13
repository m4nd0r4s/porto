//! Closing the coroutines of the tasks that are still alive when a run ends (26 §2 step 5, 12 §11).
//!
//! Dropping the runtime cancels every task left, and a cancelled Python task *closes* its
//! coroutine: `GeneratorExit` is raised at the `await` it waits in, so a synchronous `finally`
//! runs. A coroutine that awaits again while closing (an `await` in a `finally`, a swallowed
//! `GeneratorExit`) cannot finish: CPython raises `RuntimeError` from `close()`. Such a task is
//! dropped, and the run warns once, with the name and the stack of each ([`warn_stuck`]).

use pyo3::exceptions::{PyRuntimeError, PyRuntimeWarning};
use pyo3::prelude::*;

use super::lane;
use crate::api::task::Task;

/// The most frames of one task's stack the warning lists.
const MAX_FRAMES: usize = 12;

/// Closes `coro`, the coroutine of a task cancelled by the end of the run. Outside the end of a
/// run (the lane is not tearing down) it is just dropped, as before.
///
/// A coroutine that awaits while closing is recorded for [`warn_stuck`] and then *leaked*: its
/// finalizer would close it again, fail the same way, and report the same failure to
/// `sys.unraisablehook`. Whatever the leaked coroutine's frame references stays alive until the
/// process ends.
pub(crate) fn close_for_teardown(task: Option<&Py<Task>>, coro: Py<PyAny>) {
    if !lane::tearing_down() {
        drop(coro);
        return;
    }
    // SAFETY: the lane thread tears the run down (`lane::run`), and keeps its thread state
    // attached while it does.
    let py = unsafe { Python::assume_attached() };
    let bound = coro.bind(py);
    match bound.call_method0(pyo3::intern!(py, "close")) {
        Ok(_) => drop(coro),
        Err(error) if is_ignored_generator_exit(py, &error) => {
            lane::record_stuck(task_name(py, task, bound), describe_stack(bound));
            std::mem::forget(coro);
        }
        Err(error) => {
            // What the finalizer would have done with it.
            error.write_unraisable(py, Some(bound));
            drop(coro);
        }
    }
}

fn is_ignored_generator_exit(py: Python<'_>, error: &PyErr) -> bool {
    error.is_instance_of::<PyRuntimeError>(py)
        && error
            .value(py)
            .to_string()
            .contains("ignored GeneratorExit")
}

/// The task's name, else the coroutine's.
fn task_name(py: Python<'_>, task: Option<&Py<Task>>, coro: &Bound<'_, PyAny>) -> String {
    let named = task
        .and_then(|task| task.bind(py).getattr(pyo3::intern!(py, "name")).ok())
        .and_then(|name| name.extract::<String>().ok());
    named
        .or_else(|| {
            coro.getattr(pyo3::intern!(py, "__qualname__"))
                .ok()
                .and_then(|name| name.extract::<String>().ok())
        })
        .unwrap_or_else(|| "<unnamed task>".to_owned())
}

/// Where the coroutine waits: `file:line in function` of its frame, then of the coroutine it
/// awaits, and so on, outermost first.
fn describe_stack(coro: &Bound<'_, PyAny>) -> String {
    let mut frames = Vec::new();
    let mut current = Some(coro.clone());
    while let Some(object) = current.take() {
        if frames.len() >= MAX_FRAMES {
            frames.push("...".to_owned());
            break;
        }
        if let Some(frame) = first_attribute(&object, &["cr_frame", "gi_frame", "ag_frame"])
            && let Some(text) = describe_frame(&frame)
        {
            frames.push(text);
        }
        current = first_attribute(&object, &["cr_await", "gi_yieldfrom", "ag_await"]);
    }
    if frames.is_empty() {
        "no frame".to_owned()
    } else {
        frames.join(" -> ")
    }
}

fn first_attribute<'py>(object: &Bound<'py, PyAny>, names: &[&str]) -> Option<Bound<'py, PyAny>> {
    names
        .iter()
        .find_map(|name| object.getattr(*name).ok().filter(|value| !value.is_none()))
}

fn describe_frame(frame: &Bound<'_, PyAny>) -> Option<String> {
    let code = frame.getattr("f_code").ok()?;
    let file = code.getattr("co_filename").ok()?;
    let function = code.getattr("co_name").ok()?;
    let line = frame.getattr("f_lineno").ok()?;
    Some(format!("{file}:{line} in {function}"))
}

/// Warns, once, about the tasks [`close_for_teardown`] recorded: names and stacks.
pub(crate) fn warn_stuck(py: Python<'_>) {
    let stuck = lane::take_stuck();
    if stuck.is_empty() {
        return;
    }
    let mut message = format!(
        "porto.run: {} task(s) were still alive when the runtime stopped and could not be \
         closed, because they await while their coroutine is closed (an `await` in a `finally` \
         or `__aexit__`, or a swallowed GeneratorExit); the tasks were dropped without \
         finishing. Make them end on porto.Cancelled before cancel_timeout, with their cleanup \
         shielded (`with porto.shield():`) if it must await. Tasks:",
        stuck.len()
    );
    for (name, stack) in stuck {
        message.push_str(&format!("\n  - {name}: {stack}"));
    }
    let category = py.get_type::<PyRuntimeWarning>();
    let warned = std::ffi::CString::new(message.replace('\0', "?"))
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        .and_then(|message| PyErr::warn(py, &category, &message, 1));
    if let Err(error) = warned {
        // Warnings turned into errors: nobody to raise to, the run is over.
        error.write_unraisable(py, None);
    }
}
