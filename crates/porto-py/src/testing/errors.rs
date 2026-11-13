//! Test hooks for the Rust-to-Python error paths of [`crate::errors`]:
//!
//! - `_raise_rust_error_chain(depth)` raises a `porto.PortoError` built from a Rust error with a
//!   `depth`-level source chain, through `errors::from_rust_error`;
//! - `_raise_rust_panic(message)` panics with `message`, catches it and raises what
//!   `errors::rust_panic` makes of it.

use core::fmt;
use std::error::Error;
use std::future::poll_fn;
use std::panic::catch_unwind;
use std::task::Poll;

use pyo3::prelude::*;

use crate::driver::pytask::Resume;
use crate::driver::thin::BoxedFuture;
use crate::driver::trap::{Trap, TrapKind};
use crate::errors::{self, PortoError};

/// Adds the hooks to `module`.
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(raise_rust_error_chain, module)?)?;
    module.add_function(wrap_pyfunction!(raise_rust_panic, module)?)?;
    module.add_function(wrap_pyfunction!(panicking_future, module)?)?;
    module.add_function(wrap_pyfunction!(panic_in_pyfunction, module)?)?;
    module.add_function(wrap_pyfunction!(arm_scheduler_panic, module)?)
}

/// A trap that waits for a Rust future which panics when polled: awaiting it makes the step
/// loop of the awaiting task panic (outside any `#[pyfunction]`, so pyo3 does not catch it).
#[pyfunction(name = "_panicking_future")]
#[pyo3(signature = (message, /))]
fn panicking_future(message: String) -> Trap {
    Trap::new(TrapKind::Future(BoxedFuture::new(poll_fn(
        move |_cx| -> Poll<Resume> { panic!("{message}") },
    ))))
}

/// Panics inside a `#[pyfunction]`: pyo3 catches it and raises `pyo3_runtime.PanicException`,
/// which a task that does not handle it fails with as `porto.RustPanic`.
#[pyfunction(name = "_panic_in_pyfunction")]
#[pyo3(signature = (message, /))]
fn panic_in_pyfunction(message: &str) {
    panic!("{message}");
}

/// Makes the lane's next `main_or_interrupt` poll panic: a panic in scheduler internals, which
/// aborts the process.
#[pyfunction(name = "_arm_scheduler_panic")]
fn arm_scheduler_panic() {
    crate::driver::lane::arm_scheduler_panic();
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
