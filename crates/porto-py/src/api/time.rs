//! The runtime clock and the trap constructors behind `porto.sleep` and `porto.checkpoint`.

use std::time::Duration;

use porto_core::time::Deadline;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::driver::lane;
use crate::driver::trap::Trap;

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(checkpoint, module)?)?;
    module.add_function(wrap_pyfunction!(sleep, module)?)?;
    module.add_function(wrap_pyfunction!(monotonic, module)?)?;
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
    Err(PyRuntimeError::new_err(format!(
        "porto: porto.{function}() was called outside porto.run(); porto awaitables only work          in tasks of a porto runtime: start the program with porto.run(main) and await it from          there"
    )))
}

/// The runtime clock, in seconds since its origin: what sleep deadlines are measured on.
#[pyfunction(name = "_monotonic")]
fn monotonic() -> f64 {
    Deadline::now().as_secs_f64()
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
    // Only an overflow is left: a delay too long for the clock never ends.
    let deadline = Duration::try_from_secs_f64(delay).map_or(Deadline::NEVER, Deadline::after);
    Ok(Trap::sleep_until(deadline))
}
