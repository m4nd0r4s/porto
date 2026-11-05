//! The runtime clock (`porto.current_time`, alias `porto.monotonic`) and the trap constructors
//! behind `porto.sleep` and `porto.checkpoint`.

use std::time::Duration;

use porto_core::time::Deadline;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::driver::lane;
use crate::driver::trap::Trap;
use crate::errors::NoRuntimeError;

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(checkpoint, module)?)?;
    module.add_function(wrap_pyfunction!(sleep, module)?)?;
    module.add_function(wrap_pyfunction!(current_time, module)?)?;
    // The same function object, so `porto.monotonic is porto.current_time`.
    module.add("monotonic", module.getattr("current_time")?)?;
    module.add_function(wrap_pyfunction!(yield_trap, module)?)?;
    module.add_function(wrap_pyfunction!(sleep_trap, module)?)
}

/// `porto.checkpoint()`: an awaitable that yields to the other tasks once. It is a trap, not a
/// coroutine, so a checkpoint costs one small allocation from a freelist.
#[pyfunction]
fn checkpoint() -> PyResult<Trap> {
    require_lane("checkpoint")?;
    Ok(Trap::yield_now())
}

/// `porto.sleep(seconds)`: an awaitable that resumes the task `seconds` from now, never earlier.
/// `sleep(0)` yields once; `inf` never resumes.
#[pyfunction]
#[pyo3(signature = (seconds, /))]
fn sleep(seconds: f64) -> PyResult<Trap> {
    require_lane("sleep")?;
    sleep_trap(seconds)
}

/// Fails unless a porto task is running on this thread: porto awaitables only work there.
fn require_lane(function: &str) -> PyResult<()> {
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

/// A trap that resumes the task `delay` seconds from now, never earlier. `inf` never resumes;
/// a delay of zero still yields once.
#[pyfunction(name = "_sleep_trap")]
#[pyo3(signature = (delay, /))]
fn sleep_trap(delay: f64) -> PyResult<Trap> {
    if delay.is_nan() || delay < 0.0 {
        return Err(PyValueError::new_err(format!(
            "porto: the sleep delay must be a number of seconds >= 0, got {delay}; use 0 to \
             just yield to other tasks"
        )));
    }
    let now = now("sleep")?;
    // Only an overflow is left: a delay too long for the clock never ends.
    let deadline = Duration::try_from_secs_f64(delay)
        .map_or(Deadline::NEVER, |delay| now.saturating_add(delay));
    Ok(Trap::sleep_until(deadline))
}
