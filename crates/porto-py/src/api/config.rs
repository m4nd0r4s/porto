//! `porto.run`'s configuration (26-errors-config-lifecycle.md §1): resolves the Python
//! `RuntimeConfig` (`porto._config.resolve`) and converts the fields the runtime uses into a
//! porto-core [`RuntimeConfig`]. Also the private `_physical_cores` and `_auto_lanes` that
//! `porto._config.resolve_lanes` calls.

use std::time::Duration;

use porto_core::config::{IoBackend, Lanes, RuntimeConfig, physical_cores};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;

/// Adds the functions to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(private_physical_cores, module)?)?;
    module.add_function(wrap_pyfunction!(private_auto_lanes, module)?)
}

/// `porto._config.resolve`.
static RESOLVE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// The configuration of `porto.run(..., config=config, **overrides)`: the overrides over
/// `config` over the `PORTO_*` variables over the defaults. Raises what `RuntimeConfig` raises
/// for an invalid value.
///
/// The M1 single-lane runtime uses `io_backend`, `io_backend_strict`, `timer_resolution`,
/// `coop_budget`, `blocking_threads`, `blocking_keepalive` (the blocking pool, spec 19 §1),
/// `cancel_timeout` (how long `porto.run` waits for the root scope's tasks, C12), `debug`
/// (the swallowed-`Cancelled` warning, C11) and `handle_signals` (whether the signal receiver
/// start step applies, 26 §2) only; the other fields are checked (in Python) and
/// not carried yet.
pub(crate) fn resolve(
    py: Python<'_>,
    config: Option<&Bound<'_, PyAny>>,
    overrides: Option<&Bound<'_, PyDict>>,
) -> PyResult<RuntimeConfig> {
    let overrides = match overrides {
        Some(overrides) => overrides.clone(),
        None => PyDict::new(py),
    };
    let resolved = RESOLVE
        .import(py, "porto._config", "resolve")?
        .call1((config, overrides))?;
    let timer_resolution: f64 = resolved
        .getattr(pyo3::intern!(py, "timer_resolution"))?
        .extract()?;
    let coop_budget: u32 = resolved
        .getattr(pyo3::intern!(py, "coop_budget"))?
        .extract()
        .map_err(|_| {
            PyValueError::new_err(format!(
                "porto: coop_budget must be at most {}; pass a smaller budget (the default is \
                 128)",
                u32::MAX
            ))
        })?;
    // Checked in Python: a finite number of seconds, at least 0. One too long for a `Duration`
    // waits forever.
    let cancel_timeout: f64 = resolved
        .getattr(pyo3::intern!(py, "cancel_timeout"))?
        .extract()?;
    let cancel_timeout = Duration::try_from_secs_f64(cancel_timeout).unwrap_or(Duration::MAX);
    let io_backend: String = resolved
        .getattr(pyo3::intern!(py, "io_backend"))?
        .extract()?;
    let io_backend: IoBackend =
        io_backend
            .parse()
            .map_err(|error: porto_core::config::UnknownName| {
                PyValueError::new_err(error.to_string())
            })?;
    let io_backend_strict: bool = resolved
        .getattr(pyo3::intern!(py, "io_backend_strict"))?
        .extract()?;
    let debug: bool = resolved.getattr(pyo3::intern!(py, "debug"))?.extract()?;
    let handle_signals: bool = resolved
        .getattr(pyo3::intern!(py, "handle_signals"))?
        .extract()?;
    // Checked in Python: at least 1. One too large for the platform's `usize` is no limit.
    let blocking_threads: usize = resolved
        .getattr(pyo3::intern!(py, "blocking_threads"))?
        .extract()
        .unwrap_or(usize::MAX);
    // Checked in Python: a finite number of seconds, at least 0. One too long for a `Duration`
    // keeps an idle thread forever.
    let blocking_keepalive: f64 = resolved
        .getattr(pyo3::intern!(py, "blocking_keepalive"))?
        .extract()?;
    let blocking_keepalive =
        Duration::try_from_secs_f64(blocking_keepalive).unwrap_or(Duration::MAX);
    RuntimeConfig::builder()
        .debug(debug)
        .handle_signals(handle_signals)
        .io_backend(io_backend)
        .io_backend_strict(io_backend_strict)
        .timer_resolution(Duration::from_secs_f64(timer_resolution))
        .coop_budget(coop_budget)
        .blocking_threads(blocking_threads)
        .blocking_keepalive(blocking_keepalive)
        .cancel_timeout(cancel_timeout)
        .build()
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

/// `_physical_cores()`: the machine's physical core count (logical if unknown).
#[pyfunction(name = "_physical_cores")]
fn private_physical_cores() -> usize {
    physical_cores()
}

/// `_auto_lanes(gil_build, cores)`: what `lanes="auto"` resolves to on a machine with `cores`
/// physical cores (12-scheduler-and-lanes.md §7).
#[pyfunction(name = "_auto_lanes")]
#[pyo3(signature = (gil_build, cores, /))]
fn private_auto_lanes(gil_build: bool, cores: usize) -> usize {
    Lanes::Auto.resolve_with_cores(gil_build, cores)
}
