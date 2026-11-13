//! `porto.scope()` and `Scope`: the nursery and cancel scope (15-structured-concurrency-
//! and-cancellation.md §1-§2).
//!
//! A scope is a node of porto-core's scope tree (`ScopeNode`), made when its block is entered,
//! nested in the entering task's innermost scope (a root for a task in none), and pushed on that
//! task's cancel stack until its exit (`step::enter_cancel_scope`). Its children are spawned into
//! the node, which counts them (`live_children`) and records their failures, each of which
//! cancels the scope with `ChildFailed` (C4). `__aexit__` waits until no child is left (C5) on a
//! `ScopeExit` trap, which files the owner's waker as the node's `exit_waiter`; the last child to
//! end wakes it. The failures are raised at exit as a strict exception group, the body's
//! exception first (ADR-009). Spawning into a closed scope raises (C7).
//!
//! `Scope.cancel()` cancels the node, from any thread (C10): the tasks in it and in the scopes
//! nested in it get `Cancelled` at their next checkpoint, and the waiting ones are woken
//! (`step.rs`). A `Cancelled` the body lets out is absorbed at the exit when this scope itself
//! is cancelled (C3, `cancelled_caught`); one from an enclosing scope goes on. A child ended by
//! this scope's cancel counts as absorbed too (Trio's rule: a `Cancelled` counts for the scope
//! whose cancel raised it), so `cancelled_caught` is true when only the children were reached.
//!
//! A scope can have a deadline (`porto.scope(timeout=, deadline=)`, `porto.timeout()`,
//! `porto.move_on_after()`, the read/write `Scope.deadline`; `scope_deadline`): once it passed,
//! the scope is cancelled as `scope.cancel()` would, with the reason `Deadline` (C9). The exit
//! absorbs the `Cancelled` it caused like any other, but with `raise_on_timeout=True` (the
//! default, and `porto.timeout()`) it raises the builtin `TimeoutError` instead (C3), and
//! `timed_out` turns true either way.
//!
//! A shielded scope (`porto.scope(shield=True)`, `porto.shield()`, the read/write
//! `Scope.shield`) stops the walk: the cancels and deadlines of the scopes it is in do not reach
//! it, its own do (C8). Entered with sync `with` (`with porto.shield(timeout=5):` in a
//! `finally:`), a scope is a cancel scope only: entering and leaving it do not await, and
//! spawning into it raises.
//!
//! `Scope.start()` (§3) runs a task in a hidden scope the caller waits in until the task calls
//! `started.set(value)`, which moves it into this scope (`started.rs`). `Task.cancel()` (§4) is
//! in `task.rs`.
//!
//! `Scope.name` is the name `porto.scope(name=)` gave (`""` for none, `"root"` for the root
//! scope); it names the scope in the exception group its exit raises. `Scope.tasks` is a
//! snapshot of the tasks spawned into it (`Scope.spawn`, or moved in by `started.set`) that
//! have not ended: it is derived from the lane's task registry (`scope_tasks`), so a scope keeps
//! no list of its own and spawning and exiting pay nothing for it.
//!
//! Not yet: spawning from other lanes or threads (C10).

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};

use porto_core::Handle;
use porto_core::scope::{CancelReason, ScopeNode};
use porto_core::sync::SetOnce;
use pyo3::exceptions::{
    PyBaseException, PyGeneratorExit, PyRuntimeError, PyTypeError, PyValueError,
};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyDict, PyList, PyString, PyTuple, PyType};

use super::run::{closing_on_error, coroutine_from};
use super::scope_deadline::{self, ScopeDeadline, seconds};
use super::scope_exit;
use super::scope_exit::ScopeExit;
use super::scope_tasks::scope_tasks;
use super::started::Started;
use super::task::Task;
use crate::async_slots;
use crate::driver::pytask::{self, PyScopeNode, PyScopeRef, Spawned};
use crate::driver::ready::{Ready, ready, ready_none};
use crate::driver::step::{self, checkpoint_if_needed, checkpoint_if_needed_none};
use crate::driver::trap::{Trap, TrapKind};
use crate::driver::{freelist, lane};
use crate::errors::{Cancelled, NoRuntimeError, RuntimeClosed};

/// Adds the classes and functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Scope>()?;
    module.add_class::<ScopeExit>()?;
    module.add_class::<Started>()?;
    // Right after `add_class`, before anything can see the type (see `async_slots`).
    async_slots::install_iter_slots::<ScopeExit>(module.py(), scope_exit::scope_exit_iternext)?;
    freelist::install::<ScopeExit>(module.py(), &exit_freelist::SLOTS)?;
    module.add_function(wrap_pyfunction!(scope, module)?)?;
    module.add_function(wrap_pyfunction!(timeout, module)?)?;
    module.add_function(wrap_pyfunction!(move_on_after, module)?)?;
    module.add_function(wrap_pyfunction!(shield, module)?)?;
    module.add_function(wrap_pyfunction!(hidden_scope, module)?)
}

freelist::gil_freelist!(exit_freelist, 64);

/// `porto.scope(*, timeout=None, deadline=None, shield=False, name=None, raise_on_timeout=True)`:
/// a new scope, to enter with `async with`. `timeout` counts seconds from the entry, `deadline`
/// is a time on the runtime clock; with both, the earlier wins. `shield=True` keeps the cancels
/// of the scopes it is in out (C8). `name` (a `str`) names the scope (`Scope.name`, and the
/// message of the exception group its exit raises).
#[pyfunction]
#[pyo3(signature = (*, timeout=None, deadline=None, shield=false, name=None, raise_on_timeout=true))]
fn scope(
    timeout: Option<f64>,
    deadline: Option<f64>,
    shield: bool,
    name: Option<Bound<'_, PyAny>>,
    raise_on_timeout: bool,
) -> PyResult<Scope> {
    let name = scope_name(name)?;
    let timeout = timeout
        .map(|value| seconds("porto.scope()", "timeout", value))
        .transpose()?;
    let deadline = deadline
        .map(|value| seconds("porto.scope()", "deadline", value))
        .transpose()?;
    Ok(Scope::new(timeout, deadline, raise_on_timeout)
        .shielded(shield)
        .named(name))
}

/// The `name=` of `porto.scope()`, checked: a `str`, or `None` for none.
fn scope_name(name: Option<Bound<'_, PyAny>>) -> PyResult<Option<Py<PyString>>> {
    match name {
        None => Ok(None),
        Some(name) if name.is_none() => Ok(None),
        Some(name) => match name.cast_into::<PyString>() {
            Ok(name) => {
                // A name the exit cannot put in the failure group's message (a lone surrogate)
                // is refused here, where the mistake is made.
                if name.to_str().is_err() {
                    return Err(PyValueError::new_err(
                        "porto: porto.scope() name= must be text that can be encoded (it has a \
                         lone surrogate); pass a str without \\ud800-\\udfff escapes, or leave it \
                         out for an unnamed scope",
                    ));
                }
                Ok(Some(name.unbind()))
            }
            Err(error) => Err(PyTypeError::new_err(format!(
                "porto: porto.scope() name= must be a str, got {}; pass name=str(...), or leave \
                 it out for an unnamed scope",
                error.into_inner().get_type().fully_qualified_name()?
            ))),
        },
    }
}

/// `_hidden_scope()`: the scope `Scope.start` runs a task in until it reports: a nursery whose
/// exit raises a single failure bare.
#[pyfunction(name = "_hidden_scope")]
fn hidden_scope() -> Scope {
    Scope::new(None, None, false).bare()
}

/// `porto.shield(*, timeout=None)`: `porto.scope(shield=True, timeout=timeout)` (C8), also
/// usable with sync `with` in a `finally:` block that awaits cleanup.
#[pyfunction]
#[pyo3(signature = (*, timeout=None))]
fn shield(timeout: Option<f64>) -> PyResult<Scope> {
    let timeout = timeout
        .map(|value| seconds("porto.shield()", "timeout", value))
        .transpose()?;
    Ok(Scope::new(timeout, None, true).shielded(true))
}

/// `porto.timeout(seconds)`: a scope whose block raises `TimeoutError` if it is still running
/// `seconds` after it was entered.
#[pyfunction]
#[pyo3(signature = (seconds))]
fn timeout(seconds: f64) -> PyResult<Scope> {
    let seconds = scope_deadline::seconds("porto.timeout()", "seconds", seconds)?;
    Ok(Scope::new(Some(seconds), None, true))
}

/// `porto.move_on_after(seconds)`: a scope whose block is cancelled `seconds` after it was
/// entered, and which then goes on quietly (`timed_out` tells).
#[pyfunction]
#[pyo3(signature = (seconds))]
fn move_on_after(seconds: f64) -> PyResult<Scope> {
    let seconds = scope_deadline::seconds("porto.move_on_after()", "seconds", seconds)?;
    Ok(Scope::new(Some(seconds), None, false))
}

/// A nursery: every task spawned into it finishes before its `async with` block exits.
#[pyclass(frozen, module = "porto._core", name = "Scope")]
pub struct Scope {
    /// The scope's node in the tree, set when the block is entered.
    node: SetOnce<PyScopeNode>,
    /// The id of the run that entered the scope (`lane::run_id`), set with the node: `tasks` only
    /// answers in that run, whose registry holds the scope's tasks.
    run: AtomicUsize,
    /// [`ScopeFlags`]. A cancel before the block is entered reaches the node through them:
    /// `cancel` sets `CANCEL_REQUESTED` and `__aenter__` sets `ENTERED` (after `node`), both
    /// with one read-modify-write, so the later one sees the other and cancels the node.
    flags: AtomicU8,
    /// The reason the first `cancel()` gave, if any, for the repr.
    cancel_reason: OnceLock<String>,
    /// Orders the writers of `Scope.shield`, so the flag here and the node's bit end up in
    /// step (each writer sets both under it). No Python code runs under it.
    shield_lock: Mutex<()>,
    /// The deadline given, armed on the node once the block is entered (C9).
    deadline: ScopeDeadline,
    /// Whether the exit raises `TimeoutError` when the deadline cut the block short (C3).
    raise_on_timeout: bool,
    /// Whether the exit raises the one failure of a scope that has a single, as it is, instead
    /// of a group: the hidden scope of `Scope.start` (15 §3), whose child's exception must
    /// reach the caller of `start` bare.
    bare_error: bool,
    /// The name `porto.scope(name=)` gave, `None` for an unnamed scope.
    name: Option<Py<PyString>>,
}

/// The bits of `Scope::flags`.
struct ScopeFlags;

impl ScopeFlags {
    /// `node` is set: the block was entered.
    const ENTERED: u8 = 1;
    /// Its `async with` block exited.
    const CLOSED: u8 = 2;
    /// `cancel()` was called.
    const CANCEL_REQUESTED: u8 = 4;
    /// The exit absorbed a `Cancelled` this scope caused (C3).
    const CANCELLED_CAUGHT: u8 = 8;
    /// ... and the scope's own deadline caused it.
    const TIMED_OUT: u8 = 16;
    /// The runtime's root scope, which has no deadline.
    const ROOT: u8 = 32;
    /// Shielded, as asked before the block was entered (the node's own bit from then on).
    const SHIELD: u8 = 64;
    /// Entered with sync `with`: a cancel scope only, which cannot spawn (C8).
    const SYNC: u8 = 128;
}

/// Where a scope is in its life.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Created, not entered yet.
    New,
    /// Inside its `async with` block, or waiting for children at its exit.
    Open,
    /// Its `async with` block exited.
    Closed,
}

/// `porto._bridge.start`: the coroutine behind `Scope.start`.
static START: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// `BaseExceptionGroup`, built in since 3.11.
static BASE_EXCEPTION_GROUP: PyOnceLock<Py<PyType>> = PyOnceLock::new();

#[pymethods]
impl Scope {
    /// Starts `coro_or_fn` (a coroutine object, or an async function called with `args`) as a
    /// child task of this scope, and returns its `Task`. It runs in a copy of the current
    /// context, or in `context` itself; `name` names it (default: the function's
    /// `__qualname__`); `pin=True` keeps it on its lane, and without `pin` it is pinned if the
    /// spawning task is.
    // `**kwargs` is only there to refuse an unknown keyword with an error that says what to
    // do; the signature shown (`inspect.signature`) is the one 20 gives.
    #[pyo3(
        signature = (coro_or_fn, /, *args, name=None, pin=None, context=None, **kwargs),
        text_signature = "($self, coro_or_fn, /, *args, name=None, pin=None, context=None)"
    )]
    fn spawn(
        &self,
        coro_or_fn: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
        name: Option<Bound<'_, PyAny>>,
        pin: Option<Bound<'_, PyAny>>,
        context: Option<Bound<'_, PyAny>>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<Task>> {
        // A rejected coroutine object is closed: the error is the one thing reported.
        closing_on_error(&coro_or_fn, || {
            let options =
                if name.is_none() && pin.is_none() && context.is_none() && kwargs.is_none() {
                    SpawnOptions::default()
                } else {
                    SpawnOptions::parse(name, pin, context, kwargs)?
                };
            self.spawn_child(&coro_or_fn, &args, options)
        })
    }

    /// `await s.start(fn, *args, name=None)`: spawns `fn(*args, started=Started())` into a
    /// hidden scope the caller waits in, waits until the task calls `started.set(value)`, and
    /// returns `value`; the task is then a child of this scope (15 §3, Trio's
    /// `nursery.start`). What the task raises before `set` is raised from `start` as it is,
    /// and cancelling the caller meanwhile cancels the task. Calls `fn` with keyword
    /// `started` only.
    #[pyo3(
        signature = (fn_, /, *args, name=None, **kwargs),
        text_signature = "($self, fn, /, *args, name=None)"
    )]
    fn start<'py>(
        slf: &Bound<'py, Self>,
        fn_: Bound<'py, PyAny>,
        args: Bound<'py, PyTuple>,
        name: Option<Bound<'py, PyAny>>,
        kwargs: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        if let Some(kwargs) = kwargs.filter(|kwargs| !kwargs.is_empty()) {
            let names = kwargs.keys().str()?;
            return Err(PyTypeError::new_err(format!(
                "porto: Scope.start() got keyword arguments it does not take: {names}; it \
                 passes only positional arguments to the function and `started=` itself: wrap \
                 the function, s.start(functools.partial(work, key=value)) (start itself takes \
                 name=)"
            )));
        }
        if let Some(name) = &name
            && !name.is_none()
            && !name.is_instance_of::<PyString>()
        {
            return Err(PyTypeError::new_err(format!(
                "porto: Scope.start() name= must be a str, got {}; pass name=str(...), or leave \
                 it out to name the task after its function",
                name.get_type().fully_qualified_name()?
            )));
        }
        let start = START.import(py, "porto._bridge", "start")?;
        start.call1((slf, fn_, args, name))
    }

    /// For `Scope.start`: the `Started` that will move a task into this scope. Raises what
    /// `spawn` raises for a scope tasks cannot be spawned into.
    #[pyo3(name = "_new_started")]
    fn new_started(slf: &Bound<'_, Self>) -> PyResult<Started> {
        let this = slf.get();
        if !lane::is_active() {
            return Err(this.without_runtime(
                "porto: Scope.start() must be called from a task running under porto.run(); \
                 start the program with porto.run(main) and open the scope inside main",
            ));
        }
        Ok(Started::new_for(this.open_node()?))
    }

    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let this = slf.get();
        if !lane::is_active() {
            return Err(this.without_runtime(
                "porto: `async with porto.scope()` only works in tasks of a porto runtime; \
                 start the program with porto.run(main) and open the scope inside main",
            ));
        }
        if this.node.get().is_some() {
            return Err(entered_twice());
        }
        let py = slf.py();
        let value = slf.clone().into_any().unbind();
        if this.has(ScopeFlags::SHIELD) {
            // C8: a shield is entered before anything else, so the cancellation it shields
            // from is never raised at its entry (`finally: async with porto.shield():` in a
            // cancelled task). T2 still holds: the entry spends budget, and yields once the
            // budget is spent.
            this.enter(ScopeFlags::ENTERED)?;
            if step::spend_budget() {
                return Ok(Py::new(py, Trap::new(TrapKind::YieldThen(value)))?.into_any());
            }
            return ready(py, value);
        }
        // T2: entering completes synchronously. The checkpoint belongs to the enclosing scope
        // (a cancellation there raises here, before this scope exists); the scope is entered
        // as its effect.
        let entered = checkpoint_if_needed(py, value)?;
        this.enter(ScopeFlags::ENTERED)?;
        Ok(entered)
    }

    /// `with porto.shield():` (C8): the scope as a cancel scope only, entered and exited
    /// without awaiting, for a `finally:` block that must await cleanup. It cannot spawn.
    fn __enter__(slf: &Bound<'_, Self>) -> PyResult<Py<Self>> {
        let this = slf.get();
        if !lane::is_active() {
            return Err(this.without_runtime(
                "porto: `with porto.shield()` only works in tasks of a porto runtime; start the \
                 program with porto.run(main) and open the scope inside main",
            ));
        }
        if this.node.get().is_some() {
            return Err(entered_twice());
        }
        this.enter(ScopeFlags::ENTERED | ScopeFlags::SYNC)?;
        Ok(slf.clone().unbind())
    }

    /// Leaves a scope entered with `with`: absorbs a `Cancelled` it caused (C3), or raises
    /// `TimeoutError` if its deadline cut the block short.
    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        exc: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        if !self.has(ScopeFlags::SYNC) {
            return Err(PyRuntimeError::new_err(
                "porto: Scope.__exit__ leaves a scope entered with `with`; this one was not: \
                 exit it the way it was entered (`async with` for `async with`), and do not \
                 call __exit__ yourself",
            ));
        }
        let node = self.node.get();
        // As in `__aexit__`: the owner closed at shutdown has no cancel stack to pop.
        if exc.is_instance_of::<PyGeneratorExit>() {
            if let Some(node) = node {
                step::exit_cancel_scope(node);
            }
            drop(self.close());
            return Ok(false);
        }
        if let Some(node) = node
            && !step::exit_cancel_scope(node)
        {
            return Err(PyRuntimeError::new_err(
                "porto: a scope must be exited by the task that entered it, innermost first; \
                 use `with porto.shield():` and do not call __exit__ yourself",
            ));
        }
        // No child can run (spawn is refused), so nothing to wait for or cancel.
        self.finish(exc)
    }

    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __aexit__<'py>(
        slf: &Bound<'py, Self>,
        _exc_type: &Bound<'py, PyAny>,
        exc: &Bound<'py, PyAny>,
        _traceback: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let this = slf.get();
        // The owner is being closed (its runtime is shutting down): it cannot wait any more, its
        // children are dropped with the runtime, and `GeneratorExit` must go through, so the
        // exit must not yield (no checkpoint). The coroutine may be closed outside its task's
        // poll, where there is no cancel stack to pop: it goes with the task.
        if exc.is_instance_of::<PyGeneratorExit>() {
            if let Some(node) = this.node.get() {
                step::exit_cancel_scope(node);
            }
            let errors = this.close();
            drop(errors);
            return Ok(ready_none(py)?.into_bound(py));
        }
        // The exit runs outside the scope: waiting for the children (C5) is not cancelled by
        // it, and its checkpoint belongs to the enclosing scope.
        if let Some(node) = this.node.get()
            && !step::exit_cancel_scope(node)
        {
            return Err(PyRuntimeError::new_err(
                "porto: a scope must be exited by the task that entered it, innermost first; \
                 use `async with porto.scope() as s:` and do not call __aexit__ yourself",
            ));
        }
        // The block raised: like a failing child, its exception cancels the scope (C4), so
        // the children end and the exit raises it instead of waiting for them. A `Cancelled`
        // that a cancellation already reaching this scope raised needs no new one.
        if !exc.is_none()
            && let Some(node) = this.node.get()
            && !(exc.is_instance_of::<Cancelled>() && node.effective_cancellation().is_some())
        {
            node.cancel(CancelReason::ChildFailed);
        }
        if this.live_children() == 0 {
            let absorbed = this.finish(exc)?;
            if !exc.is_none() {
                // The block raised: no checkpoint, so nothing replaces its exception.
                let absorbed = PyBool::new(py, absorbed).to_owned().into_any().unbind();
                return Ok(Bound::new(py, Ready::new(absorbed))?.into_any());
            }
            // T2: no child to wait for, so the exit completes synchronously.
            return Ok(checkpoint_if_needed_none(py)?.into_bound(py));
        }
        // The waiting part: a native awaitable, not a coroutine (porto-itr.7), from a freelist.
        Ok(freelist::new_tracked(
            py,
            ScopeExit::new(slf.clone().unbind(), exc.clone().unbind()),
        )?
        .into_bound(py)
        .into_any())
    }

    /// Cancels the scope: every task in it, and in the scopes nested in it, gets
    /// `porto.Cancelled` at its next checkpoint, and the waiting ones are woken (C1, C10).
    /// Safe from any thread. Cancelling again does nothing; cancelling a closed scope affects
    /// no task, but `cancel_called` still turns true.
    #[pyo3(signature = (reason = None))]
    fn cancel(&self, reason: Option<String>) {
        let flags = self
            .flags
            .fetch_or(ScopeFlags::CANCEL_REQUESTED, Ordering::AcqRel);
        if flags & ScopeFlags::CANCEL_REQUESTED == 0
            && let Some(reason) = reason
        {
            let _first = self.cancel_reason.set(reason);
        }
        if flags & ScopeFlags::ENTERED != 0
            && let Some(node) = self.node.get()
        {
            node.cancel(CancelReason::Explicit);
        }
    }

    /// Whether the scope was cancelled: `cancel()` was called, or a child failed (C4).
    #[getter]
    fn cancel_called(&self) -> bool {
        self.node
            .get()
            .is_some_and(|node| node.cancel_called().is_some())
            || self.has(ScopeFlags::CANCEL_REQUESTED)
    }

    /// Whether the exit absorbed a `Cancelled` this scope's cancellation raised (C3), in the body
    /// or in a child task.
    #[getter]
    fn cancelled_caught(&self) -> bool {
        self.has(ScopeFlags::CANCELLED_CAUGHT)
    }

    /// Whether the scope's deadline cut its block short: the exit absorbed a `Cancelled` the
    /// deadline caused (C3). It raised `TimeoutError` then, unless `raise_on_timeout=False`.
    #[getter]
    fn timed_out(&self) -> bool {
        self.has(ScopeFlags::TIMED_OUT)
    }

    /// When the scope is cancelled by its deadline, in seconds on the runtime clock
    /// (`porto.current_time()`); `inf` for none. A `timeout=` counts from the entry: until then
    /// this reads the `deadline=` given, or `inf`.
    #[getter(deadline)]
    fn get_deadline(&self) -> f64 {
        self.deadline.get(self.node.get())
    }

    /// Whether the scope is shielded from the cancellation of the scopes it is in (C8): their
    /// cancels and deadlines do not reach it, its own still do.
    #[getter(shield)]
    fn get_shield(&self) -> bool {
        match self.node.get() {
            Some(node) if self.has(ScopeFlags::ENTERED) => node.is_shielded(),
            _ => self.has(ScopeFlags::SHIELD),
        }
    }

    /// Shields the scope, or stops shielding it, from any thread: the tasks in it are
    /// re-evaluated at once, and unshielding wakes the waiting ones an outer cancel now reaches.
    #[setter(shield)]
    fn set_shield(&self, shield: bool) -> PyResult<()> {
        if self.has(ScopeFlags::ROOT) {
            return Err(PyRuntimeError::new_err(
                "porto: the root scope has no enclosing scope to be shielded from; shield a \
                 scope inside main: `with porto.shield():`",
            ));
        }
        let _ordered = self
            .shield_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let flags = if shield {
            self.flags.fetch_or(ScopeFlags::SHIELD, Ordering::AcqRel)
        } else {
            self.flags.fetch_and(!ScopeFlags::SHIELD, Ordering::AcqRel)
        };
        if flags & ScopeFlags::ENTERED != 0
            && let Some(node) = self.node.get()
        {
            node.set_shield(shield);
        }
        Ok(())
    }

    /// Moves the deadline (C9): the timer re-arms, and a time already passed cancels the scope
    /// now. A scope already cancelled stays cancelled, whatever its new deadline.
    #[setter(deadline)]
    fn set_deadline(&self, value: f64) -> PyResult<()> {
        let value = seconds("Scope.deadline", "deadline", value)?;
        if self.has(ScopeFlags::ROOT) {
            return Err(scope_deadline::root_has_no_deadline());
        }
        self.deadline.set(self.node.get_arc().as_ref(), value);
        Ok(())
    }

    /// How many tasks are filed with the scope as waiting in a cancellable wait (C10): 0 before
    /// it is entered. For tests: every way a wait ends takes its task out (porto-szb.114).
    fn _waiter_count(&self) -> usize {
        self.node
            .get()
            .map_or(0, |node| node.lock_state().waiter_count())
    }

    /// The name `porto.scope(name=...)` gave, `""` for an unnamed scope, `"root"` for the root
    /// scope.
    #[getter]
    fn name<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        match &self.name {
            Some(name) => name.bind(py).clone(),
            None => PyString::intern(py, ""),
        }
    }

    /// The tasks spawned into this scope that have not ended, in spawn order: a snapshot (a
    /// tuple), taken now. Tasks of nested scopes are not listed, nor is a task `start()` has
    /// not yet moved here (before `started.set`).
    ///
    /// Read it from a task of the run that entered the scope. It walks the run's task registry,
    /// so it costs up to O(live tasks of the run), about 30 ns per task: do not read it in a hot
    /// loop (spec 20).
    #[getter]
    fn tasks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        // Not entered, or closed (every child ended, C5): nothing to list.
        let (Phase::Open, Some(node)) = (self.phase(), self.node.get_arc()) else {
            return Ok(PyTuple::empty(py));
        };
        scope_tasks(py, &node, self.run.load(Ordering::Relaxed))
    }

    fn __repr__(&self) -> String {
        let phase = match self.phase() {
            Phase::New => "new",
            Phase::Open => "open",
            Phase::Closed => "closed",
        };
        let Some(node) = self.node.get() else {
            return format!("<porto.Scope {phase}>");
        };
        let reason = self.cancel_reason.get();
        let cancelled = match (node.cancel_called(), reason) {
            (None, _) => String::new(),
            (Some(CancelReason::Explicit), Some(reason)) => format!(", cancelled: {reason:?}"),
            (Some(CancelReason::Explicit), None) => ", cancelled".to_owned(),
            (Some(CancelReason::Deadline), _) => ", cancelled: deadline".to_owned(),
            (Some(CancelReason::Parent), _) => ", cancelled: parent".to_owned(),
            (Some(CancelReason::ChildFailed), _) => ", cancelled: a child failed".to_owned(),
            (Some(CancelReason::Shutdown), _) => ", cancelled: shutdown".to_owned(),
            (Some(CancelReason::TaskCancelled), _) => ", cancelled: its task".to_owned(),
        };
        let state = node.lock_state();
        let exit = if state.exit_waiter.is_some() {
            ", exit waiting"
        } else {
            ""
        };
        format!(
            "<porto.Scope {phase}{cancelled}, {} live tasks{exit}>",
            state.live_children
        )
    }
}

impl Scope {
    /// The `Scope` of a runtime's root scope, `node` (C12): open from the start, entered by no
    /// task. `porto.run` closes it once its children are done.
    pub(crate) fn root(py: Python<'_>, node: PyScopeRef) -> Self {
        let mut scope = Self::new(None, None, false);
        scope.name = Some(PyString::intern(py, "root").unbind());
        scope.run.store(lane::run_id(), Ordering::Relaxed);
        let _unset = scope.node.set(node);
        scope
            .flags
            .store(ScopeFlags::ENTERED | ScopeFlags::ROOT, Ordering::Release);
        scope
    }

    /// Marks a scope not entered yet as shielded.
    fn shielded(self, shield: bool) -> Self {
        if shield {
            self.flags.fetch_or(ScopeFlags::SHIELD, Ordering::Relaxed);
        }
        self
    }

    fn new(timeout: Option<f64>, deadline: Option<f64>, raise_on_timeout: bool) -> Self {
        Self {
            node: SetOnce::new(),
            run: AtomicUsize::new(0),
            flags: AtomicU8::new(0),
            cancel_reason: OnceLock::new(),
            shield_lock: Mutex::new(()),
            deadline: ScopeDeadline::new(timeout, deadline),
            raise_on_timeout,
            bare_error: false,
            name: None,
        }
    }

    /// Gives the scope its name, if any.
    fn named(mut self, name: Option<Py<PyString>>) -> Self {
        self.name = name;
        self
    }

    /// A scope whose exit raises a single failure bare, see `bare_error`.
    fn bare(mut self) -> Self {
        self.bare_error = true;
        self
    }

    /// [`Scope::spawn`], without closing a rejected coroutine.
    fn spawn_child(
        &self,
        coro_or_fn: &Bound<'_, PyAny>,
        args: &Bound<'_, PyTuple>,
        options: SpawnOptions<'_>,
    ) -> PyResult<Py<Task>> {
        let py = coro_or_fn.py();
        let Some(handle) = lane::current() else {
            return Err(self.without_runtime(
                "porto: Scope.spawn() must be called from a task running under porto.run(); \
                 start the program with porto.run(main) and open the scope inside main",
            ));
        };
        let node = self.open_node()?;
        let coro = coroutine_from(coro_or_fn, args, "Scope.spawn")?;
        let pinned = options.pin.unwrap_or_else(step::running_is_pinned);
        let label = options.name.unwrap_or_else(|| coro_or_fn.clone()).unbind();
        let task = Py::new(py, Task::new(Some(label), pinned))?;
        let spawned = Spawned::Child {
            task: task.clone_ref(py),
            scope: node,
            pinned,
        };
        let body = match options.context {
            None => pytask::spawn(&handle, coro, spawned)?,
            Some(context) => pytask::spawn_in_context(&handle, coro, context.unbind(), spawned)?,
        };
        task.get().bind(&body);
        Ok(task)
    }

    /// What a scope raises when it is used where no runtime runs: `RuntimeClosed` if it was
    /// entered in a run that stopped (I4: it belongs to that runtime), else `NoRuntimeError`
    /// with `message`.
    fn without_runtime(&self, message: &'static str) -> PyErr {
        let run = self.run.load(Ordering::Relaxed);
        if lane::run_has_ended(run) {
            return RuntimeClosed::new_err(
                "porto: this scope belongs to a runtime that stopped (porto.run() returned), \
                 so nothing can be spawned into it or entered again; open a new scope in the \
                 run that needs one",
            );
        }
        NoRuntimeError::new_err(message)
    }

    /// The node of a scope that tasks can be spawned into: entered with `async with` and not
    /// closed. Errors say what to do, as `spawn` and `start` raise them.
    pub(crate) fn open_node(&self) -> PyResult<PyScopeRef> {
        if self.has(ScopeFlags::SYNC) {
            return Err(PyRuntimeError::new_err(
                "porto: a scope entered with sync `with` is only a cancel scope and cannot \
                 spawn tasks; enter it with `async with` to spawn into it",
            ));
        }
        match (self.phase(), self.node.get_arc()) {
            (Phase::Open, Some(node)) => Ok(node),
            (Phase::Closed, _) => Err(scope_closed()),
            _ => Err(PyRuntimeError::new_err(
                "porto: the scope is not entered yet; spawn inside its block: \
                 `async with porto.scope() as s: s.spawn(...)`",
            )),
        }
    }

    /// The scope's node, once entered.
    pub(crate) fn node(&self) -> Option<PyScopeRef> {
        self.node.get_arc()
    }

    fn phase(&self) -> Phase {
        if self.has(ScopeFlags::CLOSED) {
            Phase::Closed
        } else if self.has(ScopeFlags::ENTERED) {
            Phase::Open
        } else {
            Phase::New
        }
    }

    fn has(&self, flag: u8) -> bool {
        self.flags.load(Ordering::Acquire) & flag != 0
    }

    /// While children run: what the owner waits on at the exit, a `ScopeExit` request that
    /// resumes it once the last child ended. `None` once none runs.
    pub(super) fn exit_wait(&self) -> Option<TrapKind> {
        if self.live_children() == 0 {
            return None;
        }
        let scope = self.node.get_arc()?;
        Some(TrapKind::ScopeExit { scope })
    }

    /// Enters the block in the running task: makes the node (shielded if asked), publishes it
    /// with `entered` (`ENTERED`, and `SYNC` for `with`), then applies what was asked before:
    /// a `cancel()`, a shield change, the deadline.
    fn enter(&self, entered: u8) -> PyResult<()> {
        let shield = self.has(ScopeFlags::SHIELD);
        let node = step::enter_cancel_scope(|parent, owner| match parent {
            Some(parent) => ScopeNode::new_child(parent, owner, shield),
            None => {
                let root = ScopeNode::new_root(owner);
                root.set_shield(shield);
                root
            }
        })?
        .ok_or_else(|| {
            PyRuntimeError::new_err(
                "porto: a scope must be entered by a porto task, inside its coroutine: \
                 `async with porto.scope() as s:` in code that porto.run() runs",
            )
        })?;
        self.run.store(lane::run_id(), Ordering::Relaxed);
        if self.node.set(node.clone()).is_err() {
            // Another task entered it at the same time.
            step::exit_cancel_scope(&node);
            return Err(entered_twice());
        }
        let ordered = self
            .shield_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let flags = self.flags.fetch_or(entered, Ordering::AcqRel);
        if (flags & ScopeFlags::SHIELD != 0) != shield {
            // The shield changed between the node's making and its publication.
            node.set_shield(!shield);
        }
        drop(ordered);
        if flags & ScopeFlags::CANCEL_REQUESTED != 0 {
            // Cancelled before it was entered.
            node.cancel(CancelReason::Explicit);
        }
        if let Some(handle) = Handle::try_current() {
            self.deadline.enter(&node, &handle);
        }
        Ok(())
    }

    /// Children spawned and not ended.
    fn live_children(&self) -> u32 {
        self.node
            .get()
            .map_or(0, |node| node.lock_state().live_children)
    }

    /// Closes the scope, here and in its node (where a spawn from anywhere is refused, C7), and
    /// returns the children's failures.
    pub(crate) fn close(&self) -> Vec<Py<PyBaseException>> {
        self.flags.fetch_or(ScopeFlags::CLOSED, Ordering::AcqRel);
        let Some(node) = self.node.get() else {
            return Vec::new();
        };
        // Taken out under the lock, dropped by the caller after it.
        node.close()
    }

    /// Closes the scope and raises the children's failures, after `exc` unless the scope
    /// absorbs it. Returns whether it absorbs `exc`: a `Cancelled` while this scope itself is
    /// cancelled (C3; one from an enclosing scope's cancellation goes on). `cancelled_caught`
    /// turns true then, or when a child ended by this scope's cancel was dropped here; if the
    /// scope's deadline was the cancel, `timed_out` does too (when the block's exception, if
    /// any, is that `Cancelled`), and with `raise_on_timeout` the exit raises `TimeoutError`
    /// (unless children failed: their group goes on instead; and a body exception that is not
    /// the `Cancelled` always goes on).
    pub(super) fn finish(&self, exc: &Bound<'_, PyAny>) -> PyResult<bool> {
        let py = exc.py();
        let node = self.node.get();
        let reason = node.and_then(|node| node.cancel_called());
        let absorbed = !exc.is_none() && exc.is_instance_of::<Cancelled>() && reason.is_some();
        let child_absorbed = node.is_some_and(|node| node.lock_state().cancel_absorbed);
        let caught = absorbed || child_absorbed;
        if absorbed {
            // The cancellation this task was delivered ends here, as it should (15 C11).
            if let Some(node) = node {
                step::note_cancel_absorbed(node.id());
            }
        }
        // The deadline timed the block out only if it cut it short: nothing came out of it, or
        // the `Cancelled` it caused did. Any other exception leaving the block (a cleanup bug
        // after the `Cancelled`, `KeyboardInterrupt`) is the body's own and goes on, as C4 says,
        // however many children the deadline also ended.
        let timed_out = reason == Some(CancelReason::Deadline)
            && (absorbed || (child_absorbed && exc.is_none()));
        if caught {
            let flags = if timed_out {
                ScopeFlags::CANCELLED_CAUGHT | ScopeFlags::TIMED_OUT
            } else {
                ScopeFlags::CANCELLED_CAUGHT
            };
            self.flags.fetch_or(flags, Ordering::AcqRel);
        }
        let mut errors = self.close();
        if errors.is_empty() {
            if timed_out && self.raise_on_timeout {
                return Err(scope_deadline::timeout_error(py, exc)?);
            }
            return Ok(absorbed);
        }
        if self.bare_error && errors.len() == 1 && (exc.is_none() || absorbed) {
            let error = errors.remove(0);
            return Err(PyErr::from_value(error.into_bound(py).into_any()));
        }
        let exceptions = PyList::empty(py);
        if !exc.is_none() && !absorbed {
            exceptions.append(exc)?;
        }
        for error in errors {
            exceptions.append(error)?;
        }
        // Never `?` here: the children's failures were drained by `close` above, and an error
        // from the name would replace them.
        let message = match &self.name {
            Some(name) if !name.bind(py).to_string_lossy().is_empty() => {
                format!("porto scope '{}' failed", name.bind(py).to_string_lossy())
            }
            _ => "porto scope failed".to_owned(),
        };
        Err(exception_group(py, &message, &exceptions)?)
    }
}

/// A strict exception group of `exceptions` (ADR-009): `BaseExceptionGroup(...)` returns an
/// `ExceptionGroup` when every member is an `Exception`.
pub(crate) fn exception_group(
    py: Python<'_>,
    message: &str,
    exceptions: &Bound<'_, PyList>,
) -> PyResult<PyErr> {
    let group = BASE_EXCEPTION_GROUP
        .import(py, "builtins", "BaseExceptionGroup")?
        .call1((message, exceptions))?;
    Ok(PyErr::from_value(group))
}

/// The keyword options of [`Scope::spawn`], checked.
#[derive(Default)]
struct SpawnOptions<'py> {
    /// A `str`.
    name: Option<Bound<'py, PyAny>>,
    pin: Option<bool>,
    /// A `contextvars.Context`.
    context: Option<Bound<'py, PyAny>>,
}

impl<'py> SpawnOptions<'py> {
    /// Checks the options `Scope.spawn` got, with an error that says what to pass instead.
    fn parse(
        name: Option<Bound<'py, PyAny>>,
        pin: Option<Bound<'py, PyAny>>,
        context: Option<Bound<'py, PyAny>>,
        kwargs: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Self> {
        if let Some(kwargs) = kwargs.filter(|kwargs| !kwargs.is_empty()) {
            let names = kwargs.keys().str()?;
            return Err(PyTypeError::new_err(format!(
                "porto: Scope.spawn() got keyword arguments it does not take: {names}; it \
                 passes only positional arguments to the function: spawn a coroutine object, \
                 s.spawn(work(key=value)), or a partial, \
                 s.spawn(functools.partial(work, key=value)) (spawn itself takes name=, pin= \
                 and context=)"
            )));
        }
        if let Some(name) = &name
            && !name.is_instance_of::<PyString>()
        {
            return Err(PyTypeError::new_err(format!(
                "porto: Scope.spawn() name= must be a str, got {}; pass name=str(...), or \
                 leave it out to name the task after its function",
                name.get_type().fully_qualified_name()?
            )));
        }
        let pin = match pin {
            None => None,
            Some(pin) => match pin.cast::<PyBool>() {
                Ok(pin) => Some(pin.is_true()),
                Err(_) => {
                    return Err(PyTypeError::new_err(format!(
                        "porto: Scope.spawn() pin= must be True, False or None, got {}; pass \
                         pin=True to keep the task on its lane, or leave it out to inherit the \
                         spawning task's pinning",
                        pin.get_type().fully_qualified_name()?
                    )));
                }
            },
        };
        if let Some(context) = &context
            // SAFETY: attached (we hold a `Bound`); the check only reads the object's type.
            && unsafe { ffi::PyContext_CheckExact(context.as_ptr()) } == 0
        {
            return Err(PyTypeError::new_err(format!(
                "porto: Scope.spawn() context= must be a contextvars.Context, got {}; pass \
                 contextvars.copy_context(), or leave it out to run the task in a copy of the \
                 current context",
                context.get_type().fully_qualified_name()?
            )));
        }
        Ok(Self { name, pin, context })
    }
}

/// What spawning into a closed scope raises (C7).
pub(crate) fn scope_closed() -> PyErr {
    PyRuntimeError::new_err(
        "porto: scope is closed: its `async with` block has exited, so it cannot spawn more \
         tasks; spawn from inside the block, or open a new scope",
    )
}

fn entered_twice() -> PyErr {
    PyRuntimeError::new_err(
        "porto: a scope can be entered only once; open a new one with \
         `async with porto.scope() as s:` for each block",
    )
}
