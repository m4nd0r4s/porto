//! Python bindings for Porto: the `porto._core` extension module.
//!
//! See the [Python API reference](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/20-python-api-reference.md)
//! and [CPython integration](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/25-cpython-integration-and-memory.md).

use pyo3::prelude::*;

pub mod async_slots;
pub mod interned;
pub mod ready;
mod throw_args;
pub mod trap;

pub use ready::Ready;
pub use trap::Trap;

/// The `porto._core` module: the trap protocol's awaitables, `_Trap` and `_Ready`.
///
/// `gil_used = false` (the pyo3 0.30 default, stated on purpose): the module supports
/// free-threaded CPython 3.14t, so importing it does not re-enable the GIL (ADR-024).
#[pymodule(gil_used = false)]
#[pyo3(name = "_core")]
fn core_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_class::<Trap>()?;
    module.add_class::<Ready>()?;
    // The types exist now (`add_class` created them) and nothing outside this function can see
    // them yet, so this is the one point where their `am_send` slot can be set (see async_slots).
    async_slots::install_am_send::<Trap>(py, trap::trap_am_send)?;
    async_slots::install_am_send::<Ready>(py, ready::ready_am_send)?;
    interned::names(py);
    Ok(())
}
