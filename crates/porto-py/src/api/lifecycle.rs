//! The shutdown request of a runtime (26 §2 steps 3-4) and `porto.lifecycle.shutdown_requested()`.
//!
//! A shutdown starts from a request: `Runtime.shutdown()` (any thread) or a signal the runtime
//! handles (the signal receiver's task on the lane). The request only records what was asked
//! and wakes the lane; `porto.run` (`run.rs`) walks the states: it sets the runtime's
//! `shutdown_requested` event, gives main up to the graceful period to finish, cancels the root
//! scope, and waits up to `cancel_timeout`.
//!
//! A second request of the interrupt kind (a second Ctrl-C) *forces* the shutdown: the waits
//! stop, and what is still alive is abandoned at once.

use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pyo3::prelude::*;

use super::runtime;
use crate::errors::NoRuntimeError;

/// Why the runtime shuts down; what `porto.run` raises after the shutdown, if anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cause {
    /// `Runtime.shutdown()`: `porto.run` returns or raises what main ended with.
    Shutdown,
    /// SIGINT: `porto.run` raises `KeyboardInterrupt` after the shutdown.
    Interrupt,
    /// SIGTERM: `porto.run` raises `porto.Terminated` after the shutdown.
    Terminate,
}

/// A request to shut down.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Request {
    pub(crate) cause: Cause,
    /// How long main may take to finish by itself; `None` for the configuration's
    /// `graceful_timeout`.
    pub(crate) graceful: Option<Duration>,
}

#[derive(Default)]
struct State {
    request: Option<Request>,
    forced: bool,
    /// The waker of the lane's supervising future, which polls [`Lifecycle::poll_requested`] and
    /// [`Lifecycle::poll_forced`].
    waker: Option<Waker>,
}

/// What asks a runtime to shut down, and what the lane has been told so far. Thread-safe: a
/// request may come from any thread.
#[derive(Default)]
pub(crate) struct Lifecycle {
    state: Mutex<State>,
}

impl Lifecycle {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Asks the runtime to shut down. The first request starts the shutdown; later ones are
    /// ignored, except that an interrupt after the first forces it.
    pub(crate) fn request(&self, request: Request) {
        let waker = {
            let mut state = self.lock();
            match state.request {
                None => state.request = Some(request),
                Some(_) if request.cause == Cause::Interrupt => state.forced = true,
                Some(_) => return,
            }
            state.waker.clone()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// The request, once there is one.
    pub(crate) fn poll_requested(&self, cx: &Context<'_>) -> Poll<Request> {
        let mut state = self.lock();
        match state.request {
            Some(request) => Poll::Ready(request),
            None => {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// Ready once a second interrupt forced the shutdown.
    pub(crate) fn poll_forced(&self, cx: &Context<'_>) -> Poll<()> {
        let mut state = self.lock();
        if state.forced {
            return Poll::Ready(());
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// What asked for the shutdown, if something did, even after main ended.
    pub(crate) fn cause(&self) -> Option<Cause> {
        self.lock().request.map(|request| request.cause)
    }

    /// Whether the shutdown was forced.
    pub(crate) fn is_forced(&self) -> bool {
        self.lock().forced
    }
}

/// `porto.lifecycle.shutdown_requested()`: the event that is set once the runtime was asked to
/// shut down, for libraries to stop taking new work while main finishes (26 §2 step 3).
#[pyfunction(name = "_shutdown_requested")]
fn shutdown_requested(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let runtime = runtime::current(py).ok_or_else(|| {
        NoRuntimeError::new_err(
            "porto: porto.lifecycle.shutdown_requested() must be called from code that \
             porto.run(main) or Runtime.block_on(main) runs; call it inside main",
        )
    })?;
    runtime.get().check_not_poisoned()?;
    let event = runtime.get().shutdown_event(py);
    event.ok_or_else(|| {
        NoRuntimeError::new_err(
            "porto: porto.lifecycle.shutdown_requested() was called while the runtime is not \
             running; call it from a task of the running runtime",
        )
    })
}

/// Adds the function to `module`.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(shutdown_requested, module)?)
}
