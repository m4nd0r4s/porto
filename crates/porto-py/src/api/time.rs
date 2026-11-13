//! The runtime clock (`porto.current_time`, alias `porto.monotonic`) and the trap constructors
//! behind `porto.sleep`, `porto.sleep_until`, `porto.sleep_forever` and `porto.checkpoint`
//! (`14-time-and-timers.md` §3).
//!
//! A sleep whose deadline is already reached (`sleep(seconds <= 0)`, `sleep_until` of a past
//! time) is a pure checkpoint: the driver turns a sleep trap with a passed deadline into a yield,
//! cancellation check included. An infinite one (`inf`, `sleep_forever`) only ends by
//! cancellation. NaN is refused: it is no point in time.

use std::time::Duration;

use porto_core::time::Deadline;
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;

use crate::driver::lane;
use crate::driver::trap::{self, SleepCall, Trap};
use crate::errors::NoRuntimeError;

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("checkpoint", checkpoint_function(module)?)?;
    module.add_function(wrap_pyfunction!(sleep, module)?)?;
    module.add_function(wrap_pyfunction!(sleep_until, module)?)?;
    module.add_function(wrap_pyfunction!(sleep_forever, module)?)?;
    module.add_function(wrap_pyfunction!(armed_timers, module)?)?;
    module.add_function(wrap_pyfunction!(current_time, module)?)?;
    // The same function object, so `porto.monotonic is porto.current_time`.
    module.add("monotonic", module.getattr("current_time")?)?;
    module.add_function(wrap_pyfunction!(yield_trap, module)?)?;
    module.add_function(wrap_pyfunction!(trap_yield, module)?)?;
    module.add_function(wrap_pyfunction!(current_lane, module)?)?;
    module.add_function(wrap_pyfunction!(run_id, module)?)?;
    module.add_function(wrap_pyfunction!(sleep_trap, module)?)
}

/// `porto.checkpoint()`: an awaitable that yields to the other tasks once. It is a trap, not a
/// coroutine, so a checkpoint costs one small allocation from a freelist (on a GIL build one
/// without a lock, `driver::freelist`).
fn checkpoint(py: Python<'_>) -> PyResult<Py<Trap>> {
    require_lane("checkpoint")?;
    trap::fast_new::object(py, Trap::yield_now().watched())
}

/// The `PyMethodDef` of `porto.checkpoint`, a static as CPython requires.
struct MethodDef(ffi::PyMethodDef);

// SAFETY: the definition is immutable and its pointers are to statics (a C function, two
// NUL-terminated strings); CPython only reads it.
unsafe impl Sync for MethodDef {}

static CHECKPOINT_DEF: MethodDef = MethodDef(ffi::PyMethodDef {
    ml_name: c"checkpoint".as_ptr(),
    ml_meth: ffi::PyMethodDefPointer {
        PyCFunction: checkpoint_noargs,
    },
    ml_flags: ffi::METH_NOARGS,
    ml_doc: c"checkpoint()\n--\n\nAn awaitable that yields to the other tasks once.".as_ptr(),
});

/// `porto.checkpoint` as a plain `METH_NOARGS` builtin rather than a pyo3 function: it runs once
/// per loop iteration of a task that yields, and pyo3's call trampoline (its GIL count, a thread
/// local that macOS reaches through a function call) was a measurable part of it
/// (porto-szb.117). The same `PyCFunction` type and behaviour, errors included.
fn checkpoint_function<'py>(module: &Bound<'py, PyModule>) -> PyResult<Bound<'py, PyAny>> {
    let py = module.py();
    // SAFETY: attached; the definition is a static, as `PyCFunction_NewEx` requires, and the
    // module name is a live `str`. A new reference, or NULL with an exception set.
    unsafe {
        let function = ffi::PyCFunction_NewEx(
            (&raw const CHECKPOINT_DEF.0).cast_mut(),
            module.as_ptr(),
            module.name()?.as_ptr(),
        );
        Bound::from_owned_ptr_or_err(py, function)
    }
}

/// The C function behind `porto.checkpoint` (`METH_NOARGS`).
///
/// # Safety
///
/// CPython calls it as a `METH_NOARGS` function, attached.
unsafe extern "C" fn checkpoint_noargs(
    _module: *mut ffi::PyObject,
    _args: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    std::panic::catch_unwind(|| {
        // SAFETY: called attached (the caller's contract).
        let py = unsafe { Python::assume_attached() };
        match checkpoint(py) {
            Ok(trap) => trap.into_ptr(),
            Err(error) => {
                error.restore(py);
                std::ptr::null_mut()
            }
        }
    })
    .unwrap_or_else(|payload| {
        // SAFETY: as above.
        let py = unsafe { Python::assume_attached() };
        crate::errors::rust_panic(&*payload, None).restore(py);
        std::ptr::null_mut()
    })
}

/// `porto.sleep(seconds)`: an awaitable that resumes the task `seconds` from now, never earlier.
/// `seconds <= 0` is a checkpoint (it yields once); `inf` never resumes.
#[pyfunction]
#[pyo3(signature = (seconds, /))]
fn sleep(seconds: f64) -> PyResult<Trap> {
    require_lane("sleep")?;
    Ok(sleep_trap(seconds)?.watched_sleep(SleepCall::Sleep))
}

/// `porto.sleep_until(deadline)`: an awaitable that resumes the task once `current_time()`
/// reaches `deadline`, never earlier. A deadline already passed is a checkpoint; `inf` never
/// resumes.
#[pyfunction]
#[pyo3(signature = (deadline, /))]
fn sleep_until(deadline: f64) -> PyResult<Trap> {
    require_lane("sleep_until")?;
    if deadline.is_nan() {
        return Err(PyValueError::new_err(
            "porto: porto.sleep_until() got a NaN deadline, which is no point in time; pass a \
             time on the runtime clock, such as porto.current_time() + 5 (or call \
             porto.sleep_forever() to wait until cancelled)",
        ));
    }
    let now = now("sleep_until")?;
    Ok(Trap::sleep_until(deadline_at(now, deadline)).watched_sleep(SleepCall::SleepUntil))
}

/// `porto.sleep_forever()`: an awaitable that never resumes the task; only a cancellation ends
/// it.
#[pyfunction]
fn sleep_forever() -> PyResult<Trap> {
    require_lane("sleep_forever")?;
    Ok(Trap::sleep_until(Deadline::NEVER).watched_sleep(SleepCall::SleepForever))
}

/// Test hook: how many timers are armed on the running runtime's wheel: sleeping tasks and the
/// runtime's own (the lane's signal check). A cancelled sleep leaves none behind.
#[pyfunction(name = "_armed_timers")]
fn armed_timers() -> PyResult<usize> {
    porto_core::Handle::try_current()
        .map(|handle| handle.armed_timers())
        .ok_or_else(|| {
            NoRuntimeError::new_err(
                "porto: _armed_timers() was called outside porto.run(); call it from a task",
            )
        })
}

/// Fails unless a porto task is running on this thread: porto awaitables only work there.
pub(crate) fn require_lane(function: &str) -> PyResult<()> {
    if lane::is_active() {
        return Ok(());
    }
    Err(NoRuntimeError::new_err(format!(
        "porto: porto.{function}() was called outside porto.run(); porto awaitables only work \
         in tasks of a porto runtime: start the program with porto.run(main) and await it from \
         there"
    )))
}

/// `porto.current_time()`: the runtime clock, in seconds since the runtime started (virtual
/// seconds in deterministic mode). Sleep deadlines are measured on it.
#[pyfunction]
fn current_time() -> PyResult<f64> {
    Ok(now("current_time")?.as_secs_f64())
}

/// The current time on the runtime clock; fails outside a runtime, which owns the clock.
fn now(function: &str) -> PyResult<Deadline> {
    Deadline::try_now().ok_or_else(|| {
        NoRuntimeError::new_err(format!(
            "porto: porto.{function}() was called outside porto.run(); the runtime clock starts \
             with the runtime, so call it from code that porto.run(main) runs (for a clock \
             outside porto, use time.monotonic())"
        ))
    })
}

/// A trap that yields to the scheduler once: `await _yield_trap()` is `porto.checkpoint()`.
#[pyfunction(name = "_yield_trap")]
fn yield_trap() -> Trap {
    Trap::yield_now()
}

/// `porto.lowlevel.trap_yield()`: the trap behind `porto.checkpoint()`, for a library that
/// drives traps by hand: awaiting it yields to the other tasks once, with the cancellation check
/// of every checkpoint. A trap freed without being awaited emits `RuntimeWarning` ("never
/// awaited").
#[pyfunction]
fn trap_yield() -> PyResult<Trap> {
    require_lane("lowlevel.trap_yield")?;
    Ok(Trap::yield_now().watched())
}

/// `porto.lowlevel.current_lane()`: the index of the lane the calling task runs on (0: a runtime
/// has one lane until M2). Raises `porto.NoRuntimeError` outside a task of a porto runtime.
#[pyfunction]
fn current_lane() -> PyResult<u16> {
    crate::driver::step::current_lane().ok_or_else(|| {
        NoRuntimeError::new_err(
            "porto: porto.lowlevel.current_lane() was called outside a task of a porto runtime; \
             call it from code that porto.run(main) runs",
        )
    })
}

/// `_run_id()`: the process-unique id of the running `porto.run`, which a cache that belongs to
/// one run (the resolver's, 17 §4) keys itself with so that it never outlives the run.
#[pyfunction(name = "_run_id")]
fn run_id() -> PyResult<usize> {
    require_lane("_run_id")?;
    Ok(crate::driver::lane::run_id())
}

/// A trap that resumes the task `delay` seconds from now, never earlier. `inf` never resumes;
/// a delay of zero or less still yields once.
#[pyfunction(name = "_sleep_trap")]
#[pyo3(signature = (delay, /))]
fn sleep_trap(delay: f64) -> PyResult<Trap> {
    if delay.is_nan() {
        return Err(PyValueError::new_err(
            "porto: porto.sleep() got NaN seconds, which is no delay; pass a number of seconds \
             (0 or less just yields to the other tasks), or call porto.sleep_forever() to wait \
             until cancelled",
        ));
    }
    let now = now("sleep")?;
    if delay <= 0.0 {
        // Already reached: a checkpoint.
        return Ok(Trap::sleep_until(now));
    }
    // Only an overflow is left: a delay too long for the clock never ends.
    let deadline = Duration::try_from_secs_f64(delay)
        .map_or(Deadline::NEVER, |delay| now.saturating_add(delay));
    Ok(Trap::sleep_until(deadline))
}

/// The deadline for `seconds` on the runtime clock (not NaN), at `now` if that has passed.
/// Rounded up to the nanosecond, so `current_time()` at or after it never reads below
/// `seconds`: never early on the clock the caller measured with.
pub(crate) fn deadline_at(now: Deadline, seconds: f64) -> Deadline {
    if seconds <= now.as_secs_f64() {
        return now;
    }
    let Ok(since_epoch) = Duration::try_from_secs_f64(seconds) else {
        // Beyond the clock (or `inf`): never reached.
        return Deadline::NEVER;
    };
    let deadline = Deadline::from_duration(since_epoch);
    if deadline.as_secs_f64() < seconds {
        deadline.saturating_add(Duration::from_nanos(1))
    } else {
        deadline
    }
}
