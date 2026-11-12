//! The engine behind `porto.to_thread` (`19-threads-and-blocking.md` §1, §2): a Python callable
//! run on the runtime's blocking pool, and the worker threads that can run it.
//!
//! `_run_in_thread(fn, args, kwargs, context)` returns a trap that, when the task awaits it,
//! queues `context.run(fn, *args, **kwargs)` on a pool worker and waits for the result through
//! the job's join handle: the worker completing the job wakes the task on the lane's usual wake
//! path. Nothing starts until the await, so a trap that is never awaited runs nothing. The
//! public `porto.to_thread` (`python/porto/_threads.py`) adds the copy of the caller's context
//! and the cancellation policy around it.
//!
//! # Worker threads (25 P0, P1)
//!
//! Each worker creates one `PyThreadState` when it starts ([`PythonWorkers`]) and deletes it
//! when it exits. A Python job attaches it for the job only (`Python::attach` finds the
//! thread's state, so `PyGILState_Ensure` neither makes nor deletes one) and detaches after, so
//! an idle worker costs the interpreter nothing: no GIL held, and a free-threaded stop-the-world
//! pause does not wait for it. The references a job owns while it runs are plain `Py`s: the
//! worker is attached. Everything that crosses to or from the lane is a [`PyDeferred`]: the
//! callable, its arguments and the context travel to the worker as such, and the result (or the
//! exception, with its traceback) travels back as such, so a job that is dropped unrun, or whose
//! awaiter is gone, is released on the lane and never by the worker.
//!
//! A job that finishes after its runtime ended (the pool waits one second, then leaves its
//! threads to finish alone) still delivers nothing: its result has no lane to route to and is
//! released where it is, attached, by the worker.

use std::any::Any;
use std::mem;

use porto_core::{BlockingStats, Handle, WeakHandle, WorkerHooks};
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::time::require_lane;
use crate::driver::lane;
use crate::driver::pytask::{Resume, shut_down_error};
use crate::driver::thin::BoxedFuture;
use crate::driver::trap::{Trap, TrapKind};
use crate::errors::{NoRuntimeError, RuntimeClosed, rust_panic};
use crate::pyref::{PyDeferred, interpreter_is_finalizing};

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run_in_thread, module)?)?;
    module.add_function(wrap_pyfunction!(blocking_stats, module)?)
}

/// `_run_in_thread(fn, args, kwargs, context)`: an awaitable that runs `context.run(fn, *args,
/// **kwargs)` on a blocking-pool thread, and evaluates to its result or raises its exception
/// (with the traceback of the worker's frames). The job cannot be interrupted: dropping the
/// await (a cancel that is not shielded) leaves it running and discards its result.
///
/// `context` MUST be a `contextvars.Context` that nothing else enters while the job runs
/// (`porto.to_thread` passes a fresh copy).
#[pyfunction(name = "_run_in_thread")]
#[pyo3(signature = (func, args, kwargs, context, /))]
fn run_in_thread(
    func: Bound<'_, PyAny>,
    args: Bound<'_, PyTuple>,
    kwargs: Option<Bound<'_, PyDict>>,
    context: Bound<'_, PyAny>,
) -> PyResult<Trap> {
    require_lane("to_thread")?;
    if !func.is_callable() {
        return Err(PyTypeError::new_err(format!(
            "porto: to_thread needs a callable to run, not {}; pass a function (and its \
             arguments after it), for example porto.to_thread(open_file, path)",
            func.get_type().name()?
        )));
    }
    let handle = lane::current().ok_or_else(|| {
        NoRuntimeError::new_err("porto: to_thread was called outside a porto runtime")
    })?;
    let job = PythonJob {
        func: PyDeferred::new(func.unbind(), &handle),
        args: PyDeferred::new(args.unbind(), &handle),
        kwargs: kwargs.map(|kwargs| PyDeferred::new(kwargs.unbind(), &handle)),
        context: PyDeferred::new(context.unbind(), &handle),
    };
    let home = handle.downgrade();
    // Polled by the awaiting task, on the lane: the job starts at the first poll.
    let future = async move {
        let join = match handle.try_spawn_blocking(move || job.run(&home)) {
            Ok(join) => join,
            Err(error) => return Resume::Throw(thrown(&RuntimeClosed::new_err(error.to_string()))),
        };
        let outcome = join.await;
        // SAFETY: polled by the task's step, on the lane, attached.
        let py = unsafe { Python::assume_attached() };
        match outcome {
            Ok(Some(Outcome::Returned(value))) => Resume::Value(value.into_inner(py)),
            Ok(Some(Outcome::Raised(exception))) => Resume::Throw(exception.into_inner(py)),
            // The runtime ended before the job ran, or the interpreter is finalizing.
            Ok(None) => Resume::Throw(thrown(&shut_down_error())),
            // Cancelled while queued: the pool closed, or no worker thread could be started for
            // it (another submitter's start failed with no worker left, spec 19 §1).
            Err(error) if error.is_cancelled() => Resume::Throw(thrown(&not_run_error())),
            Err(error) => {
                let payload = error.into_panic();
                Resume::Throw(thrown(&rust_panic(&*payload, None)))
            }
        }
    };
    Ok(Trap::new(TrapKind::Future(BoxedFuture::new(future))))
}

/// The exception object of `error`, to throw into the coroutine.
fn thrown(error: &PyErr) -> Py<PyBaseException> {
    // SAFETY: only called from the future above, polled on the lane, attached.
    let py = unsafe { Python::assume_attached() };
    error.clone_ref(py).into_value(py)
}

/// What a job did.
enum Outcome {
    Returned(PyDeferred),
    Raised(PyDeferred<PyBaseException>),
}

/// `fn`, its arguments and the context to run it in: owned for the lane until the worker takes
/// them.
struct PythonJob {
    func: PyDeferred,
    args: PyDeferred<PyTuple>,
    kwargs: Option<PyDeferred<PyDict>>,
    context: PyDeferred,
}

impl PythonJob {
    /// Runs the job on the worker thread: attaches its thread state for the call and detaches
    /// after. `None` when the interpreter is finalizing (the job is leaked, 25 §7).
    fn run(self, home: &WeakHandle) -> Option<Outcome> {
        if interpreter_is_finalizing() {
            // Attaching now would hang this thread or end it mid-frame; leak what the job owns.
            mem::forget(self);
            return None;
        }
        Some(Python::attach(|py| self.call(py, home)))
    }

    fn call(self, py: Python<'_>, home: &WeakHandle) -> Outcome {
        let context = self.context.into_inner(py).into_bound(py);
        let func = self.func.into_inner(py).into_bound(py);
        let args = self.args.into_inner(py).into_bound(py);
        let kwargs = self
            .kwargs
            .map(|kwargs| kwargs.into_inner(py).into_bound(py));
        // `Context.run(fn, *args, **kwargs)`.
        let items: Vec<Bound<'_, PyAny>> = std::iter::once(func).chain(args.iter()).collect();
        let call = PyTuple::new(py, items)
            .and_then(|call| context.call_method(intern!(py, "run"), call, kwargs.as_ref()));
        match call {
            Ok(value) => Outcome::Returned(PyDeferred::with_home(value.unbind(), home.clone())),
            // `into_value` keeps the traceback: the frames of this thread are in it.
            Err(error) => {
                Outcome::Raised(PyDeferred::with_home(error.into_value(py), home.clone()))
            }
        }
    }
}

/// Makes each pool worker a Python thread (25 P0): one `PyThreadState` for the thread's
/// lifetime.
pub(crate) struct PythonWorkers {
    interpreter: *mut ffi::PyInterpreterState,
}

// SAFETY: the interpreter pointer is only handed to `PyThreadState_New`, which may be called
// from any thread, and the interpreter outlives the runtime (the run holds it attached).
unsafe impl Send for PythonWorkers {}
// SAFETY: as above.
unsafe impl Sync for PythonWorkers {}

impl PythonWorkers {
    /// The workers of the interpreter this thread is attached to.
    pub(crate) fn of_current_interpreter(_py: Python<'_>) -> Self {
        Self {
            // SAFETY: attached (the token says so).
            interpreter: unsafe { ffi::PyInterpreterState_Get() },
        }
    }
}

impl WorkerHooks for PythonWorkers {
    fn thread_start(&self) -> Box<dyn Any> {
        if interpreter_is_finalizing() {
            return Box::new(WorkerThreadState(std::ptr::null_mut()));
        }
        // SAFETY: a live interpreter (the runtime is alive, so its run is, attached); the
        // call needs no attached thread. The new state is bound to this thread and detached.
        Box::new(WorkerThreadState(unsafe {
            ffi::PyThreadState_New(self.interpreter)
        }))
    }
}

/// The worker's thread state, deleted when its thread exits.
struct WorkerThreadState(*mut ffi::PyThreadState);

impl Drop for WorkerThreadState {
    fn drop(&mut self) {
        if self.0.is_null() || interpreter_is_finalizing() {
            // Finalization deletes every thread state of the interpreter itself.
            return;
        }
        // SAFETY: the state is this thread's own, made by `PyThreadState_New` in
        // `thread_start` and detached (no job runs here any more). `PyEval_RestoreThread`
        // attaches it; `PyThreadState_DeleteCurrent` clears nothing, so clear first, then it
        // deletes the state and releases the thread, as `threading` does at a thread's end.
        unsafe {
            ffi::PyEval_RestoreThread(self.0);
            ffi::PyThreadState_Clear(self.0);
            ffi::PyThreadState_DeleteCurrent();
        }
    }
}

/// `_blocking_stats()`: `(threads, idle, queued)` of this run's blocking pool (the
/// `blocking_threads` and `blocking_queue_depth` gauges of spec 23). A private hook for tests.
#[pyfunction(name = "_blocking_stats")]
fn blocking_stats() -> PyResult<(usize, usize, usize)> {
    let handle: Handle = lane::current().ok_or_else(|| {
        NoRuntimeError::new_err(
            "porto: _blocking_stats() was called outside porto.run(); call it from a task",
        )
    })?;
    let BlockingStats {
        threads,
        idle,
        queued,
    } = handle.blocking_stats();
    Ok((threads, idle, queued))
}

/// A blocking job cancelled before it ran: the runtime stopped, or the OS refused the worker
/// thread it needed.
fn not_run_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: the blocking job was cancelled before it ran: either its runtime stopped (keep \
         porto.run() running until the jobs you wait for are done) or no worker thread could be \
         started for it (the OS refused a new thread: lower RuntimeConfig.blocking_threads or \
         free system resources, then try again)",
    )
}
