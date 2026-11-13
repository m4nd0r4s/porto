//! The exceptions of `porto.errors` as Rust types, and the one way a Rust error or panic becomes
//! a Python exception (20-python-api-reference.md, Exceptions; 26-errors-config-lifecycle.md §3).
//!
//! The classes are defined in Python (`python/porto/errors.py`), which gives them the multiple
//! inheritance the spec asks for (`NoRuntimeError(PortoError, RuntimeError)`) and real
//! attributes. Each Rust type here imports its class the first time it is raised, so raising one
//! looks like raising a builtin: `NoRuntimeError::new_err("porto: ...")`.
//!
//! A Rust `std::error::Error` becomes a Python exception through [`from_rust_error`] only: the
//! message is the error's `Display`, and its `source()` chain becomes `caused by: ...` notes
//! (ST-19), so nothing the Rust side knew about the failure is lost on the way. A panic becomes a
//! [`RustPanic`] through [`rust_panic`].

use core::any::Any;
use core::cell::RefCell;
use core::iter::successors;
use std::error::Error;
use std::panic;
use std::sync::Once;

use pyo3::exceptions::PyBaseException;
use pyo3::panic::PanicException;
use pyo3::prelude::*;
use pyo3::{PyTypeInfo, import_exception};

import_exception!(porto.errors, PortoError);
import_exception!(porto.errors, NoRuntimeError);
import_exception!(porto.errors, RuntimeClosed);
import_exception!(porto.errors, WouldBlock);
import_exception!(porto.errors, EndOfChannel);
import_exception!(porto.errors, BrokenResourceError);
import_exception!(porto.errors, BusyResourceError);
import_exception!(porto.errors, ClosedResourceError);
import_exception!(porto.errors, IncompleteReadError);
import_exception!(porto.errors, LimitOverrunError);
import_exception!(porto.errors, Lagged);
import_exception!(porto.errors, InvalidStateError);
import_exception!(porto.errors, RustPanic);
import_exception!(porto.errors, Cancelled);
import_exception!(porto.errors, Terminated);

/// The Python exception of class `T` for `error`: its message is `error`'s `Display`, and each
/// error in its `source()` chain adds one PEP 678 note `caused by: {source}`, outermost first.
///
/// `error`'s `Display` is what the user reads first, so it must say what to do (CLAUDE.md); the
/// notes say why.
pub fn from_rust_error<T: PyTypeInfo>(py: Python<'_>, error: &(dyn Error + 'static)) -> PyErr {
    let exception = PyErr::new::<T, _>(error.to_string());
    add_cause_notes(py, &exception, error);
    exception
}

/// Adds one `caused by: {source}` note to `exception` for each error in `error`'s source chain.
pub fn add_cause_notes(py: Python<'_>, exception: &PyErr, error: &(dyn Error + 'static)) {
    for cause in successors(error.source(), |&cause| cause.source()) {
        if let Err(failure) = exception.add_note(py, format!("caused by: {cause}")) {
            // Only a `MemoryError` gets here. The exception itself still says what went wrong,
            // so it is raised without the rest of its notes, and the failure is reported.
            failure.write_unraisable(py, Some(exception.value(py).as_any()));
            return;
        }
    }
}

/// The [`RustPanic`] for a caught panic: `payload` is what `catch_unwind` returned, `location`
/// the `file:line:column` of the panic when a panic hook recorded it.
pub fn rust_panic(payload: &(dyn Any + Send), location: Option<&str>) -> PyErr {
    RustPanic::new_err((panic_message(payload), location.map(str::to_owned)))
}

thread_local! {
    /// Where the last panic on this thread happened, recorded by the hook of
    /// [`install_panic_hook`] before the unwind starts.
    static LAST_PANIC_LOCATION: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Chains a panic hook that records where each panic happened (`file:line:column`), so the
/// [`RustPanic`] of a task that panicked can say where (spec 25 §8). The previous hook still
/// runs, so the message and the location are logged to stderr as usual. Idempotent.
pub fn install_panic_hook() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            let location = info.location().map(ToString::to_string);
            LAST_PANIC_LOCATION.with(|slot| {
                if let Ok(mut slot) = slot.try_borrow_mut() {
                    *slot = location;
                }
            });
            previous(info);
        }));
    });
}

/// Takes the location of the last panic on this thread, if the hook recorded one. The slot is
/// per thread: a `PanicException` that a `to_thread` job raised came from another thread, so the
/// `RustPanic` made from it on the lane may carry no location (or an older one).
pub fn take_panic_location() -> Option<String> {
    LAST_PANIC_LOCATION.with_borrow_mut(Option::take)
}

/// The [`RustPanic`] for a panic that `catch_unwind` caught on this thread, with the location the
/// panic hook recorded for it.
pub fn caught_panic(payload: &(dyn Any + Send)) -> PyErr {
    rust_panic(payload, take_panic_location().as_deref())
}

/// `exception` as `porto.RustPanic` if it is the `PanicException` that pyo3 raises for a panic
/// in a `#[pyfunction]`/`#[pymethods]` body (pyo3 catches the unwind at every such entry point
/// and raises it in the caller, a `BaseException` outside `porto.errors`): the original stays as
/// `__cause__`. Any other exception is returned as it is.
pub fn panic_exception_as_rust_panic<'py>(
    py: Python<'py>,
    exception: Bound<'py, PyBaseException>,
) -> Bound<'py, PyBaseException> {
    if !exception.is_instance_of::<PanicException>() {
        return exception;
    }
    let message = exception
        .str()
        .map_or_else(|_| "a Rust panic".to_owned(), |text| text.to_string());
    let converted = RustPanic::new_err((message, take_panic_location()));
    converted.set_cause(py, Some(PyErr::from_value(exception.into_any())));
    converted.into_value(py).into_bound(py)
}

/// `message` or `message at location`, for a log line about a panic that is not turned into a
/// Python exception.
pub fn rust_panic_description(payload: &(dyn Any + Send)) -> String {
    let message = panic_message(payload);
    match take_panic_location() {
        Some(location) => format!("{message} at {location}"),
        None => message,
    }
}

/// The message of a panic: `panic!` payloads are a `&str` or a `String`; anything else came from
/// `std::panic::panic_any`.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "a panic whose payload is not a string (std::panic::panic_any)".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use pyo3::exceptions::PyRuntimeError;
    use pyo3::types::PyList;

    use super::*;
    use crate::testing::errors::ErrorChain;

    fn notes(py: Python<'_>, exception: &PyErr) -> Vec<String> {
        match exception.value(py).getattr("__notes__") {
            Ok(notes) => notes.cast_into::<PyList>().unwrap().extract().unwrap(),
            Err(_) => Vec::new(),
        }
    }

    #[test]
    fn each_level_of_the_source_chain_becomes_one_note_outermost_first() {
        Python::initialize();
        Python::attach(|py| {
            let exception = from_rust_error::<PyRuntimeError>(py, &ErrorChain::new(3));
            assert!(exception.is_instance_of::<PyRuntimeError>(py));
            assert_eq!(exception.value(py).to_string(), "level 0");
            assert_eq!(
                notes(py, &exception),
                [
                    "caused by: level 1",
                    "caused by: level 2",
                    "caused by: level 3"
                ]
            );
        });
    }

    #[test]
    fn an_error_without_a_source_gets_no_note() {
        Python::initialize();
        Python::attach(|py| {
            let exception = from_rust_error::<PyRuntimeError>(py, &ErrorChain::new(0));
            assert!(notes(py, &exception).is_empty());
        });
    }

    #[test]
    fn the_panic_message_is_read_from_str_and_string_payloads() {
        let literal = catch_unwind(|| panic!("literal")).unwrap_err();
        let formatted = catch_unwind(|| panic!("formatted {}", 7)).unwrap_err();
        let other = catch_unwind(|| std::panic::panic_any(7_u8)).unwrap_err();
        assert_eq!(panic_message(&*literal), "literal");
        assert_eq!(panic_message(&*formatted), "formatted 7");
        assert!(panic_message(&*other).contains("panic_any"));
    }
}
