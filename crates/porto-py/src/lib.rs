//! Python bindings for Porto: the `porto._core` extension module.
//!
//! See the [Python API reference](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/20-python-api-reference.md)
//! and [CPython integration](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/25-cpython-integration-and-memory.md).

use pyo3::prelude::*;

mod api;
pub mod async_slots;
pub mod driver;
mod py_compat;
pub mod testing;
mod throw_args;

pub use driver::ready::Ready;
pub use driver::trap::{Trap, TrapKind};
pub use py_compat::interned;

/// The `porto._core` module: the public API (`run`, `scope`, `Scope`, `Task`, `sleep`,
/// `checkpoint`, `current_time`, `monotonic`), the trap protocol's awaitables (`_Trap`, `_Ready`), the step driver's private
/// entry points (`_run`, `_spawn`, the trap constructors) and test hooks.
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
    async_slots::install_am_send::<Trap>(py, driver::trap::trap_am_send)?;
    async_slots::install_am_send::<Ready>(py, driver::ready::ready_am_send)?;
    py_compat::names(py);
    api::run::register(module)?;
    api::scope::register(module)?;
    api::time::register(module)?;
    testing::register(module)
}
