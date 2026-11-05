//! `porto.run`'s configuration (26-errors-config-lifecycle.md §1): resolves the Python
//! `RuntimeConfig` (`porto._config.resolve`) and converts the fields the runtime uses into a
//! porto-core [`RuntimeConfig`]. Also the private `_physical_cores` and `_auto_lanes` that
//! `porto._config.resolve_lanes` calls.

use std::time::Duration;

use porto_core::config::{Lanes, RuntimeConfig, physical_cores};
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
/// The M1 single-lane runtime uses `timer_resolution` only; the other fields are checked (in
/// Python) and not carried yet.
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
    RuntimeConfig::builder()
        .timer_resolution(Duration::from_secs_f64(timer_resolution))
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
