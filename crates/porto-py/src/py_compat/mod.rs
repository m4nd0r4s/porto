//! The CPython layer: what differs between the versions porto supports (3.11, 3.12, 3.13, 3.14
//! and free-threaded 3.14t) and the rules every C API call in porto-py follows
//! (25-cpython-integration-and-memory.md §1, §3, §4).
//!
//! - [`take_exception`]: the raised exception, normalized, on every version;
//! - [`names`]: interned attribute names, created once per interpreter;
//! - [`send`]: `PyIter_Send` that takes ownership of the value it sends;
//! - [`raise_stop_iteration`]: `StopIteration(value)` for a raw `tp_iternext`, reusing a spare
//!   exception on GIL builds.
//!
//! Rules for the rest of the crate, which free-threaded builds make load-bearing:
//!
//! - **Strong references only.** Use the `*Ref` C APIs (`PyDict_GetItemRef`,
//!   `PyList_GetItemRef`, `PyWeakref_GetRef`, ...), never the ones that return a borrowed
//!   reference: on 3.14t another thread can drop the object between the call and its use.
//!   `scripts/check_borrowed_refs.py` (CI lint job) rejects them in this crate.
//! - **Frozen pyclasses.** Every `#[pyclass]` is `frozen`; mutable state lives in atomics or a
//!   `Mutex`, so no pymethod takes `&mut self` and pyo3's runtime borrow flag never fails.
//! - **Critical sections for user containers.** Iterating a dict or list a user can mutate from
//!   another thread holds the container's critical section for the whole walk
//!   (`PyDictMethods::locked_for_each`, or `pyo3::sync::critical_section::with_critical_section`
//!   around a manual walk); pyo3's plain iterators only lock per item. porto-py iterates no user
//!   container today; the first users are Douro's handler signature analysis (M6).
//! - **`gil_used = false`.** The module declares `Py_MOD_GIL_NOT_USED`, so importing it on 3.14t
//!   leaves the GIL off (`tests/python/test_free_threading.py`).

mod exception;
pub mod interned;
mod send;
mod stop_iteration;

pub(crate) use exception::take_exception;
pub use interned::names;
pub(crate) use send::{Sent, send};
pub(crate) use stop_iteration::raise_stop_iteration;
