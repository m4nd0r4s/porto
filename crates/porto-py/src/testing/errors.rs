//! Test hooks for the Rust-to-Python error paths of [`crate::errors`]:
//!
//! - `_raise_rust_error_chain(depth)` raises a `porto.PortoError` built from a Rust error with a
//!   `depth`-level source chain, through `errors::from_rust_error`;
//! - `_raise_rust_panic(message)` panics with `message`, catches it and raises what
//!   `errors::rust_panic` makes of it.

use core::fmt;
use std::error::Error;
use std::panic::catch_unwind;

use pyo3::prelude::*;

use crate::errors::{self, PortoError};

/// Adds the hooks to `module`.
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(raise_rust_error_chain, module)?)?;
    module.add_function(wrap_pyfunction!(raise_rust_panic, module)?)
}

/// A Rust error `depth` levels deep: `level 0` is caused by `level 1`, ... down to
/// `level {depth}`, which has no source.
#[derive(Debug)]
pub struct ErrorChain {
    level: usize,
    source: Option<Box<ErrorChain>>,
}

impl ErrorChain {
    /// The chain from `level 0` down to `level {depth}`.
    pub fn new(depth: usize) -> Self {
        let deepest = Self {
            level: depth,
            source: None,
        };
        (0..depth).rev().fold(deepest, |source, level| Self {
            level,
            source: Some(Box::new(source)),
        })
    }
}

impl fmt::Display for ErrorChain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "level {}", self.level)
    }
}

impl Error for ErrorChain {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

/// Raises `PortoError("level 0")` with one `caused by: level N` note per level below it.
#[pyfunction(name = "_raise_rust_error_chain")]
#[pyo3(signature = (depth, /))]
fn raise_rust_error_chain(py: Python<'_>, depth: usize) -> PyResult<()> {
    Err(errors::from_rust_error::<PortoError>(
        py,
        &ErrorChain::new(depth),
    ))
}

/// Panics with `message` in Rust and raises the resulting `porto.RustPanic`.
#[pyfunction(name = "_raise_rust_panic")]
#[pyo3(signature = (message, /))]
fn raise_rust_panic(message: &str) -> PyResult<()> {
    let payload = catch_unwind(|| panic!("{message}")).unwrap_err();
    Err(errors::rust_panic(&*payload, Some(file!())))
}
