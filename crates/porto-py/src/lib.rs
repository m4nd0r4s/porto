//! Python bindings for Porto: the `porto._core` extension module.
//!
//! See the [Python API reference](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/20-python-api-reference.md)
//! and [CPython integration](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/25-cpython-integration-and-memory.md).

use pyo3::prelude::*;

/// The `porto._core` module. For now it only exposes `__version__`.
#[pymodule]
#[pyo3(name = "_core")]
fn core_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))
}
