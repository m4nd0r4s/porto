//! `porto.signals.receive` (18-fs-process-signals.md §3): the glue between the self-pipe of
//! porto-io and Python.
//!
//! One [`Hub`] per run keeps the pipe and who wants which signal. A Rust task on the lane
//! reads the signal numbers that arrive (from any thread: the handler only writes the pipe) and
//! calls the subscribers, which run on the lane. A subscriber is a Python callable for
//! `porto.signals.receive`, or Rust for the runtime's default handling of SIGINT and SIGTERM
//! (`driver/startup.rs`).
//!
//! The hub belongs to the lane thread of the run: only code on that lane subscribes, and the
//! run's end ([`end_of_run`]) puts every signal's previous disposition back.
//!
//! On Windows there is no self-pipe yet: `porto.signals.receive` raises `NotImplementedError`
//! (Python's own SIGINT handling stays in charge there).

#[cfg(unix)]
pub(crate) use unix::{Callback, end_of_run, subscribe, unsubscribe};

#[cfg(not(unix))]
pub(crate) fn end_of_run() {}

/// Adds the functions to `module`.
pub(crate) fn register(module: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    #[cfg(unix)]
    {
        unix::register(module)
    }
    #[cfg(not(unix))]
    {
        use pyo3::prelude::*;
        module.add_function(wrap_pyfunction!(not_available, module)?)
    }
}

#[cfg(not(unix))]
#[pyo3::pyfunction(name = "_signals_subscribe", signature = (*_args))]
fn not_available(_args: &pyo3::Bound<'_, pyo3::types::PyTuple>) -> pyo3::PyResult<()> {
    Err(pyo3::exceptions::PyNotImplementedError::new_err(
        "porto: porto.signals.receive() is not available on Windows yet (there is no \
         self-pipe for signals there); use signal.signal(...) with a handler that calls \
         loop-safe code, or run on Linux or macOS",
    ))
}

#[cfg(unix)]
mod unix {
    use std::cell::RefCell;
    use std::io;
    use std::rc::Rc;

    use porto_core::Handle;
    use porto_io::signal::{Signals, Subscription};
    use pyo3::exceptions::{PyOSError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    use crate::api::time::require_lane;
    use crate::driver::lane;

    /// What runs, on the lane, when a signal arrives.
    pub(crate) type Callback = Rc<dyn Fn(Python<'_>, i32)>;

    struct Entry {
        id: u64,
        signums: Vec<i32>,
        callback: Callback,
        /// Restore the dispositions when dropped.
        _subscriptions: Vec<Subscription>,
    }

    struct Hub {
        signals: Signals,
        entries: Vec<Entry>,
        next_id: u64,
    }

    thread_local! {
        static HUB: RefCell<Option<Hub>> = const { RefCell::new(None) };
    }

    /// Opens the hub of this run if it is not open yet, and starts the task that delivers.
    fn open<'a>(hub: &'a mut Option<Hub>, handle: &Handle) -> PyResult<&'a mut Hub> {
        if hub.is_none() {
            let driver = lane::current_driver().ok_or_else(not_in_a_run)?;
            let signals = Signals::new(driver).map_err(|error| {
                PyOSError::new_err(format!(
                    "porto: cannot open the signal pipe ({error}); the process may be out of \
                     file descriptors: close some, or raise the limit (ulimit -n)"
                ))
            })?;
            let delivering = signals.clone();
            // The task is dropped with the runtime; nothing waits for it.
            drop(handle.spawn(async move {
                // The pipe closes only when the driver shuts down, at the end of the run.
                while let Ok(signum) = delivering.next().await {
                    deliver(signum);
                }
            }));
            *hub = Some(Hub {
                signals,
                entries: Vec::new(),
                next_id: 1,
            });
        }
        hub.as_mut().ok_or_else(not_in_a_run)
    }

    fn not_in_a_run() -> PyErr {
        crate::errors::NoRuntimeError::new_err(
            "porto: signals can only be received in a task of a running runtime; call it from \
             code that porto.run(main) runs",
        )
    }

    /// Calls the subscribers of `signum`, outside the hub's borrow: a subscriber may subscribe
    /// or unsubscribe.
    fn deliver(signum: i32) {
        // SAFETY: the delivering task runs on the lane thread, which is attached while it polls.
        let py = unsafe { Python::assume_attached() };
        let callbacks: Vec<Callback> = HUB.with_borrow(|hub| {
            hub.iter()
                .flat_map(|hub| &hub.entries)
                .filter(|entry| entry.signums.contains(&signum))
                .map(|entry| entry.callback.clone())
                .collect()
        });
        for callback in callbacks {
            callback(py, signum);
        }
    }

    /// Starts calling `callback` with each of `signums` that arrives, on the lane of the run
    /// `handle` belongs to, until [`unsubscribe`] or the end of the run. Returns the
    /// subscription's id.
    ///
    /// # Errors
    /// `ValueError` for a number that cannot be received, `OSError` if the OS refuses.
    pub(crate) fn subscribe(handle: &Handle, signums: &[i32], callback: Callback) -> PyResult<u64> {
        HUB.with_borrow_mut(|hub| {
            let hub = open(hub, handle)?;
            let mut subscriptions = Vec::with_capacity(signums.len());
            for &signum in signums {
                subscriptions.push(hub.signals.subscribe(signum).map_err(|error| {
                    if error.kind() == io::ErrorKind::InvalidInput {
                        PyValueError::new_err(format!(
                            "porto: signal {signum} cannot be received ({error}); SIGKILL and \
                             SIGSTOP cannot be caught, pass catchable signals such as \
                             signal.SIGHUP or signal.SIGUSR1"
                        ))
                    } else {
                        PyOSError::new_err(format!(
                            "porto: cannot receive signal {signum} ({error})"
                        ))
                    }
                })?);
            }
            let id = hub.next_id;
            hub.next_id += 1;
            hub.entries.push(Entry {
                id,
                signums: signums.to_vec(),
                callback,
                _subscriptions: subscriptions,
            });
            Ok(id)
        })
    }

    /// Stops the subscription `id`; the signals it alone received get their previous
    /// disposition back. Nothing happens for an id that ended already.
    pub(crate) fn unsubscribe(id: u64) {
        let entry = HUB.with_borrow_mut(|hub| {
            let hub = hub.as_mut()?;
            let at = hub.entries.iter().position(|entry| entry.id == id)?;
            Some(hub.entries.remove(at))
        });
        drop(entry);
    }

    /// The run ended: every subscription goes, the previous dispositions come back.
    pub(crate) fn end_of_run() {
        let hub = HUB.take();
        drop(hub);
    }

    /// `_signals_subscribe(signums, callback)`: what `porto.signals.receive` is built on.
    #[pyclass(frozen, module = "porto._core", name = "_SignalSubscription")]
    struct SignalSubscription {
        id: u64,
    }

    #[pymethods]
    impl SignalSubscription {
        /// Stops the delivery; the previous disposition of each signal comes back unless
        /// another subscription still receives it. Closing again does nothing.
        fn close(&self) {
            unsubscribe(self.id);
        }
    }

    #[pyfunction(name = "_signals_subscribe", signature = (signums, callback))]
    fn signals_subscribe(
        signums: &Bound<'_, PyTuple>,
        callback: Py<PyAny>,
    ) -> PyResult<SignalSubscription> {
        require_lane("signals.receive")?;
        let signums: Vec<i32> = signums.extract()?;
        let callback: Callback = Rc::new(move |py, signum| {
            if let Err(error) = callback.call1(py, (signum,)) {
                error.write_unraisable(py, Some(callback.bind(py)));
            }
        });
        let handle = lane::current().ok_or_else(not_in_a_run)?;
        let id = subscribe(&handle, &signums, callback)?;
        Ok(SignalSubscription { id })
    }

    pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
        module.add_class::<SignalSubscription>()?;
        module.add_function(wrap_pyfunction!(signals_subscribe, module)?)
    }
}
