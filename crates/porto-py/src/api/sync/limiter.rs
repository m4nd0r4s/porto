//! `porto.CapacityLimiter`: a semaphore whose tokens are borrowed by a task (or another
//! borrower) one at a time, with AnyIO's API (16-sync-primitives-and-channels.md §1).
//!
//! The tokens are the permits of a `Semaphore` (`SemaphoreSource`), so waiting, fairness and
//! cancellation are the semaphore's. Beside it, the limiter records who borrowed each token:
//! a borrower holds at most one, and only a borrower gives one back. The default borrower is
//! the running porto task, recorded by its waker, which keeps the task's memory (and so its
//! identity) alive while it holds the token; any other object is recorded by identity.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::Waker;

use porto_core::sync::MAX_PERMITS;
use pyo3::PyTraverseError;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::gc::PyVisit;
use pyo3::prelude::*;
use pyo3::types::PyFloat;

use super::entry::Enterable;
use super::semaphore::{SemaphoreSource, exit_checkpoint};
use super::sync_entry::SyncEntry;
use super::wait_op::{Grant, SyncWait};
use crate::api::time::require_lane;
use crate::driver::step::current_waker;
use crate::driver::thin::{WaitTarget, Waitable};
use crate::errors::WouldBlock;

/// The total a limiter reports as `math.inf`.
const UNLIMITED: usize = MAX_PERMITS;

/// Who holds a token.
enum Holder {
    /// A porto task, by its waker (whose data is the task, and which keeps it alive).
    Task(#[allow(dead_code, reason = "held to keep the task's identity alive")] Waker),
    /// Any other object, by identity.
    Object(Py<PyAny>),
}

/// A borrower named by the caller, or the running task.
pub(super) enum Borrower {
    Current,
    Object(Py<PyAny>),
}

impl Borrower {
    /// Its key in the borrower map and what to record, or `RuntimeError` for the running task
    /// when no porto task runs here.
    fn resolve(&self, py: Python<'_>, method: &str) -> PyResult<(usize, Holder)> {
        match self {
            Self::Current => {
                let waker = current_waker().ok_or_else(|| no_task_error(method))?;
                Ok((waker.data() as usize, Holder::Task(waker)))
            }
            Self::Object(object) => Ok((
                object.as_ptr() as usize,
                Holder::Object(object.clone_ref(py)),
            )),
        }
    }
}

fn no_task_error(method: &str) -> PyErr {
    PyRuntimeError::new_err(format!(
        "porto: CapacityLimiter.{method}() borrows for the running porto task, and none runs \
         here; call it from a task of porto.run(), or name the borrower with the \
         *_on_behalf_of(borrower) form"
    ))
}

/// The limiter's state, shared with its waits.
pub(super) struct LimiterCore {
    semaphore: Arc<Waitable<SemaphoreSource>>,
    /// The borrowers, by key ([`Borrower::resolve`]).
    borrowers: Mutex<HashMap<usize, Holder>>,
    /// Serializes changes of the total (`total_tokens = ...`).
    total: Mutex<usize>,
}

impl LimiterCore {
    fn borrowers(&self) -> std::sync::MutexGuard<'_, HashMap<usize, Holder>> {
        self.borrowers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// `RuntimeError` if `key` already borrows a token.
    fn check_not_borrowing(&self, key: usize) -> PyResult<()> {
        if self.borrowers().contains_key(&key) {
            return Err(already_borrowing_error());
        }
        Ok(())
    }

    /// Records that `borrower` took a token (already acquired from the semaphore); on error
    /// the token goes back.
    fn record(&self, py: Python<'_>, borrower: &Borrower, method: &str) -> PyResult<()> {
        let recorded = borrower.resolve(py, method).and_then(|(key, holder)| {
            let mut borrowers = self.borrowers();
            if borrowers.contains_key(&key) {
                drop(borrowers);
                drop(holder);
                return Err(already_borrowing_error());
            }
            borrowers.insert(key, holder);
            Ok(())
        });
        if recorded.is_err() {
            self.semaphore.0.release(1);
        }
        recorded
    }

    /// Takes a token for `borrower` now, or raises `WouldBlock`.
    fn acquire_nowait(&self, py: Python<'_>, borrower: &Borrower, method: &str) -> PyResult<()> {
        let (key, _) = borrower.resolve(py, method)?;
        self.check_not_borrowing(key)?;
        if !self.semaphore.0.try_acquire_detached(1) {
            return Err(WouldBlock::new_err(
                "porto: no CapacityLimiter token is free right now (or tasks wait for one); \
                 await acquire() to wait for one",
            ));
        }
        self.record(py, borrower, method)
    }

    /// Gives back `borrower`'s token, granting the task that waited longest.
    fn release(&self, py: Python<'_>, borrower: &Borrower, method: &str) -> PyResult<()> {
        let (key, _) = borrower.resolve(py, method)?;
        let removed = self.borrowers().remove(&key);
        // Dropped after the unlock: dropping an object can run arbitrary code.
        let Some(holder) = removed else {
            return Err(PyRuntimeError::new_err(format!(
                "porto: CapacityLimiter.{method}(): this borrower does not hold any of this \
                 limiter's tokens; release only what was acquired, by the same borrower (the \
                 task that acquired it, or the object passed to acquire_on_behalf_of())"
            )));
        };
        drop(holder);
        let released = self.semaphore.0.release(1);
        debug_assert!(released, "a recorded borrower holds a token");
        Ok(())
    }
}

fn already_borrowing_error() -> PyErr {
    PyRuntimeError::new_err(
        "porto: this borrower already holds one of this CapacityLimiter's tokens; a borrower \
         holds at most one (release it first, or borrow on behalf of another object with \
         acquire_on_behalf_of())",
    )
}

/// The token wait of `acquire()` and `acquire_on_behalf_of()`, for `_SyncWait`.
pub(super) struct TokenGrant {
    limiter: Arc<LimiterCore>,
    borrower: Borrower,
    method: &'static str,
}

impl TokenGrant {
    pub(super) fn check(&self, py: Python<'_>) -> PyResult<()> {
        let (key, _) = self.borrower.resolve(py, self.method)?;
        self.limiter.check_not_borrowing(key)
    }

    pub(super) fn try_take(&self) -> bool {
        self.limiter.semaphore.0.try_acquire_detached(1)
    }

    pub(super) fn give_back(&self, py: Python<'_>) {
        // Taken and recorded: release both. A failure means it was not recorded, which
        // `record` already undid.
        let _ = self.limiter.release(py, &self.borrower, self.method);
    }

    pub(super) fn target(&self) -> WaitTarget {
        self.limiter.semaphore.target()
    }

    pub(super) fn taken(&self, py: Python<'_>) -> PyResult<()> {
        self.limiter.record(py, &self.borrower, self.method)
    }

    pub(super) fn call(&self) -> &'static str {
        match self.borrower {
            Borrower::Current => "CapacityLimiter.acquire()",
            Borrower::Object(_) => "CapacityLimiter.acquire_on_behalf_of()",
        }
    }

    fn traverse(&self, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
        match &self.borrower {
            Borrower::Current => Ok(()),
            Borrower::Object(object) => visit.call(object),
        }
    }
}

/// Visits a token grant's borrower (`_SyncWait.__traverse__`).
pub(super) fn traverse_grant(
    grant: &TokenGrant,
    visit: &PyVisit<'_>,
) -> Result<(), PyTraverseError> {
    grant.traverse(visit)
}

/// The running task's token, for `async with limiter:` (`sync_entry`).
pub(super) struct CurrentTask<'a>(pub(super) &'a LimiterCore);

impl Enterable for CurrentTask<'_> {
    fn check(&self, py: Python<'_>) -> PyResult<()> {
        let (key, _) = Borrower::Current.resolve(py, "__aenter__")?;
        self.0.check_not_borrowing(key)
    }

    fn try_take(&self) -> bool {
        self.0.semaphore.0.try_acquire_detached(1)
    }

    fn target(&self) -> WaitTarget {
        self.0.semaphore.target()
    }

    fn taken(&self, py: Python<'_>) -> PyResult<()> {
        self.0.record(py, &Borrower::Current, "__aenter__")
    }
}

/// `porto.CapacityLimiter(total_tokens)`: at most `total_tokens` borrowers at a time, each
/// holding one token (AnyIO's API). Tokens are granted in arrival order.
#[pyclass(frozen, module = "porto", name = "CapacityLimiter")]
pub(super) struct CapacityLimiter {
    pub(super) core: Arc<LimiterCore>,
}

/// A total from Python: an `int` of at least 1, or `math.inf`.
fn parse_total(total: &Bound<'_, PyAny>) -> PyResult<usize> {
    if let Ok(float) = total.cast::<PyFloat>() {
        if float.value() == f64::INFINITY {
            return Ok(UNLIMITED);
        }
        return Err(PyTypeError::new_err(format!(
            "porto: CapacityLimiter total_tokens must be an int or math.inf, got {float}"
        )));
    }
    let Ok(tokens) = total.extract::<i64>() else {
        return Err(PyTypeError::new_err(format!(
            "porto: CapacityLimiter total_tokens must be an int or math.inf, got {}",
            total.get_type().name()?
        )));
    };
    usize::try_from(tokens)
        .ok()
        .filter(|&tokens| (1..UNLIMITED).contains(&tokens))
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "porto: CapacityLimiter total_tokens must be from 1 to {} (or math.inf), got \
                 {tokens}",
                UNLIMITED - 1
            ))
        })
}

/// A total for Python: `math.inf` for an unlimited one.
fn total_object(py: Python<'_>, total: usize) -> PyResult<Py<PyAny>> {
    if total == UNLIMITED {
        return Ok(PyFloat::new(py, f64::INFINITY).into_any().unbind());
    }
    Ok(total.into_pyobject(py)?.into_any().unbind())
}

#[pymethods]
impl CapacityLimiter {
    #[new]
    #[pyo3(signature = (total_tokens, /))]
    fn new(total_tokens: &Bound<'_, PyAny>) -> PyResult<Self> {
        let total = parse_total(total_tokens)?;
        Ok(Self {
            core: Arc::new(LimiterCore {
                semaphore: SemaphoreSource::waitable(total),
                borrowers: Mutex::new(HashMap::new()),
                total: Mutex::new(total),
            }),
        })
    }

    /// The most tokens borrowed at once: an `int`, or `math.inf`. Raising it grants waiting
    /// tasks; lowering it takes tokens back as they are released.
    #[getter]
    fn get_total_tokens(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let total = *self
            .core
            .total
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        total_object(py, total)
    }

    #[setter]
    fn set_total_tokens(&self, total_tokens: &Bound<'_, PyAny>) -> PyResult<()> {
        let new = parse_total(total_tokens)?;
        let mut total = self
            .core
            .total
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let semaphore = &self.core.semaphore.0;
        if new > *total {
            semaphore.add_permits(new - *total);
        } else {
            semaphore.remove_permits(*total - new);
        }
        *total = new;
        Ok(())
    }

    /// How many tokens are borrowed (including one being handed to a waiting task).
    #[getter]
    fn borrowed_tokens(&self) -> usize {
        self.core.semaphore.0.acquired_permits()
    }

    /// How many tokens are free: an `int`, or `math.inf`.
    #[getter]
    fn available_tokens(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let total = *self
            .core
            .total
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if total == UNLIMITED {
            return total_object(py, UNLIMITED);
        }
        total_object(py, self.core.semaphore.0.available_permits())
    }

    /// An awaitable that completes once the running task borrows a token. Raises
    /// `RuntimeError` if the task already holds one.
    fn acquire(&self, py: Python<'_>) -> PyResult<Py<SyncWait>> {
        require_lane("CapacityLimiter.acquire")?;
        self.token_wait(py, Borrower::Current, "acquire")
    }

    /// An awaitable that completes once `borrower` borrows a token. Raises `RuntimeError` if
    /// it already holds one.
    #[pyo3(signature = (borrower, /))]
    fn acquire_on_behalf_of(&self, py: Python<'_>, borrower: Py<PyAny>) -> PyResult<Py<SyncWait>> {
        require_lane("CapacityLimiter.acquire_on_behalf_of")?;
        self.token_wait(py, Borrower::Object(borrower), "acquire_on_behalf_of")
    }

    /// Borrows a token for the running task now; raises `porto.WouldBlock` if none is free.
    fn acquire_nowait(&self, py: Python<'_>) -> PyResult<()> {
        self.core
            .acquire_nowait(py, &Borrower::Current, "acquire_nowait")
    }

    /// Borrows a token for `borrower` now; raises `porto.WouldBlock` if none is free.
    #[pyo3(signature = (borrower, /))]
    fn acquire_on_behalf_of_nowait(&self, py: Python<'_>, borrower: Py<PyAny>) -> PyResult<()> {
        self.core.acquire_nowait(
            py,
            &Borrower::Object(borrower),
            "acquire_on_behalf_of_nowait",
        )
    }

    /// Gives back the running task's token. Raises `RuntimeError` if it holds none.
    fn release(&self, py: Python<'_>) -> PyResult<()> {
        self.core.release(py, &Borrower::Current, "release")
    }

    /// Gives back `borrower`'s token. Raises `RuntimeError` if it holds none.
    #[pyo3(signature = (borrower, /))]
    fn release_on_behalf_of(&self, py: Python<'_>, borrower: Py<PyAny>) -> PyResult<()> {
        self.core
            .release(py, &Borrower::Object(borrower), "release_on_behalf_of")
    }

    /// `async with limiter:` borrows a token for the running task, lazily (like a `Lock`).
    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<SyncEntry>> {
        SyncEntry::token(slf)
    }

    #[pyo3(signature = (_exc_type, exc, _traceback, /))]
    fn __aexit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        exc: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        self.core.release(py, &Borrower::Current, "__aexit__")?;
        exit_checkpoint(py, exc)
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        // The collector runs with the other threads stopped; one stopped while holding the map
        // (it never runs Python code under it) is skipped rather than waited for.
        let Ok(borrowers) = self.core.borrowers.try_lock() else {
            return Ok(());
        };
        for holder in borrowers.values() {
            if let Holder::Object(object) = holder {
                visit.call(object)?;
            }
        }
        Ok(())
    }

    fn __clear__(&self) {
        let objects: Vec<_> = {
            let mut borrowers = self.core.borrowers();
            let keys: Vec<_> = borrowers
                .iter()
                .filter(|(_, holder)| matches!(holder, Holder::Object(_)))
                .map(|(key, _)| *key)
                .collect();
            keys.into_iter()
                .filter_map(|key| borrowers.remove(&key))
                .collect()
        };
        drop(objects);
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let total = self.get_total_tokens(py)?;
        Ok(format!(
            "<porto.CapacityLimiter borrowed={}/{}>",
            self.borrowed_tokens(),
            total.bind(py).repr()?
        ))
    }
}

impl CapacityLimiter {
    fn token_wait(
        &self,
        py: Python<'_>,
        borrower: Borrower,
        method: &'static str,
    ) -> PyResult<Py<SyncWait>> {
        SyncWait::new(Grant::Token(TokenGrant {
            limiter: Arc::clone(&self.core),
            borrower,
            method,
        }))
        .into_object(py)
    }
}
