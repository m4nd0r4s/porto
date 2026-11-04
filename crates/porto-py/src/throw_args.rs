//! The exception for `throw(typ[, val[, tb]])`, with the generator `throw` signature.
//!
//! A coroutine suspended on one of our awaitables forwards `coro.throw(...)` to the awaitable's
//! `throw` method with the arguments it got, so both forms arrive here: `throw(exc)` and the
//! legacy `throw(type, value, traceback)`.

use pyo3::exceptions::{PyBaseException, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

/// Builds the exception that `throw(typ, val, tb)` raises, as a generator would.
pub(crate) fn thrown_exception(
    typ: &Bound<'_, PyAny>,
    val: Option<&Bound<'_, PyAny>>,
    tb: Option<&Bound<'_, PyAny>>,
) -> PyErr {
    match build(typ, val, tb) {
        Ok(exception) => PyErr::from_value(exception),
        Err(error) => error,
    }
}

fn build<'py>(
    typ: &Bound<'py, PyAny>,
    val: Option<&Bound<'py, PyAny>>,
    tb: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let val = val.filter(|value| !value.is_none());
    let exception = if typ.is_instance_of::<PyBaseException>() {
        if val.is_some() {
            return Err(PyTypeError::new_err(
                "throw(): an exception instance takes no separate value; \
                 pass throw(exc) or throw(ExcType, value)",
            ));
        }
        typ.clone()
    } else if let Ok(exception_type) = typ.cast::<PyType>()
        && exception_type.is_subclass_of::<PyBaseException>()?
    {
        match val {
            None => exception_type.call0()?,
            Some(value) if value.is_instance(exception_type)? => value.clone(),
            Some(value) => match value.cast::<PyTuple>() {
                Ok(args) => exception_type.call1(args)?,
                Err(_) => exception_type.call1((value,))?,
            },
        }
    } else {
        return Err(PyTypeError::new_err(format!(
            "throw(): exceptions must be classes or instances deriving from BaseException, \
             not {}",
            typ.get_type().name()?
        )));
    };
    if let Some(traceback) = tb.filter(|traceback| !traceback.is_none()) {
        exception.call_method1("with_traceback", (traceback,))?;
    }
    Ok(exception)
}
