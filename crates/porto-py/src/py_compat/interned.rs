//! Interned attribute and method names, created once per interpreter
//! (25-cpython-integration-and-memory.md §4).
//!
//! The step driver looks these up on every throw and on foreign awaitables, so they are interned
//! `str` objects held in a [`PyOnceLock`]: no string is built or hashed on the hot path.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyString;

/// The names the runtime calls or reads on Python objects.
pub struct Names {
    /// `coro.throw(exc)`: delivers cancellation and errors into a coroutine.
    pub throw: Py<PyString>,
    /// `obj.send(value)`: resumes a foreign iterator.
    pub send: Py<PyString>,
    /// `obj.close()`: closes a coroutine or iterator that will not be resumed.
    pub close: Py<PyString>,
    /// `fut._asyncio_future_blocking`: marks an asyncio future yielded to the driver (§22).
    pub asyncio_future_blocking: Py<PyString>,
}

static NAMES: PyOnceLock<Names> = PyOnceLock::new();

/// The interned names, created on first use.
pub fn names(py: Python<'_>) -> &'static Names {
    NAMES.get_or_init(py, || Names {
        throw: PyString::intern(py, "throw").unbind(),
        send: PyString::intern(py, "send").unbind(),
        close: PyString::intern(py, "close").unbind(),
        asyncio_future_blocking: PyString::intern(py, "_asyncio_future_blocking").unbind(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_created_once_and_interned() {
        Python::initialize();
        Python::attach(|py| {
            let first = names(py);
            let second = names(py);
            assert!(core::ptr::eq(first, second));
            assert!(first.throw.is(PyString::intern(py, "throw")));
            assert_eq!(
                first.asyncio_future_blocking.bind(py),
                "_asyncio_future_blocking"
            );
        });
    }
}
