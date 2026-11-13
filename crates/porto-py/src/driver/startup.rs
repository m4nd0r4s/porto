//! The steps of a runtime's start that other features fill in (26 §2 step 1), installed in the
//! order the spec lists and undone in reverse when the run stops (step 6).
//!
//! Each step is an extension point: it says when it applies, and what it installs. The owners
//! (the asyncgen hooks of 11 §9, the import hook of 25 §6) fill their step in; until then it
//! installs nothing, so a run does exactly what it did before. Filled in: the fork hooks (25 §6)
//! and the signal receiver (18 §3), the default handling of SIGINT and SIGTERM. What ran, in which order, is kept for the `_startup_trace` test hook.

use std::sync::{Arc, Mutex};

use porto_core::Handle;
use porto_core::config::RuntimeConfig;
use pyo3::prelude::*;

use crate::api::lifecycle::Lifecycle;

/// What a step may look at when it decides and installs.
pub(crate) struct StartContext<'a, 'py> {
    pub(crate) py: Python<'py>,
    pub(crate) config: &'a RuntimeConfig,
    /// Whether the thread that starts the runtime (lane 0) is the process's main thread, the only
    /// one Python delivers signals to.
    pub(crate) on_main_thread: bool,
    /// What the signal receiver asks to shut the runtime down.
    #[cfg_attr(not(unix), allow(dead_code, reason = "read by the Unix signal step"))]
    pub(crate) lifecycle: &'a Arc<Lifecycle>,
    /// The runtime being started.
    #[cfg_attr(not(unix), allow(dead_code, reason = "read by the Unix signal step"))]
    pub(crate) handle: &'a Handle,
}

/// Undoes what one step installed.
type Uninstall = Box<dyn FnOnce(Python<'_>)>;

/// One step of the start.
struct Step {
    name: &'static str,
    /// Whether the step applies to this run.
    applies: fn(&StartContext<'_, '_>) -> bool,
    /// Installs the step; the result undoes it, if there is anything to undo.
    install: fn(&StartContext<'_, '_>) -> PyResult<Option<Uninstall>>,
}

fn nothing_to_install(_: &StartContext<'_, '_>) -> PyResult<Option<Uninstall>> {
    Ok(None)
}

/// The default handling of SIGINT and SIGTERM (18 §3): each asks the runtime to shut down. A
/// signal whose disposition is not Python's default (an ignored SIGINT, a program's own handler)
/// is left alone.
///
/// SIGINT is Ctrl-C: the shutdown starts with no graceful period (main is cancelled at once)
/// and `porto.run` raises `KeyboardInterrupt` after it; a second SIGINT forces it. SIGTERM is
/// a request to stop: main gets `graceful_timeout` to finish, and `porto.run` raises
/// `porto.Terminated` after the shutdown.
#[cfg(unix)]
fn install_signal_receiver(context: &StartContext<'_, '_>) -> PyResult<Option<Uninstall>> {
    use std::rc::Rc;
    use std::time::Duration;

    use crate::api::lifecycle::{Cause, Request};
    use crate::api::signals;

    // Like `asyncio.Runner`: only a signal that has Python's default handling is taken. An
    // ignored SIGINT (`nohup`, `cmd &`) or a handler the program installed stays as it is.
    let py = context.py;
    let signal = py.import("signal")?;
    let current = |signum: i32| signal.call_method1("getsignal", (signum,));
    let take_sigint = current(libc::SIGINT)?.is(&signal.getattr("default_int_handler")?);
    let take_sigterm = current(libc::SIGTERM)?.eq(signal.getattr("SIG_DFL")?)?;
    let mut ids = Vec::new();
    for (wanted, signum, cause, graceful) in [
        (
            take_sigint,
            libc::SIGINT,
            Cause::Interrupt,
            Some(Duration::ZERO),
        ),
        (take_sigterm, libc::SIGTERM, Cause::Terminate, None),
    ] {
        if !wanted {
            continue;
        }
        let lifecycle = context.lifecycle.clone();
        let callback: signals::Callback = Rc::new(move |_, _| {
            lifecycle.request(Request { cause, graceful });
        });
        // SIGINT chains: a lane stuck in synchronous code still gets the second Ctrl-C as a
        // `KeyboardInterrupt` (18 §3).
        let chained = signum == libc::SIGINT;
        match signals::subscribe(context.handle, &[signum], callback, chained) {
            Ok(id) => ids.push(id),
            Err(error) => {
                ids.iter().for_each(|id| signals::unsubscribe(*id));
                return Err(error);
            }
        }
    }
    if ids.is_empty() {
        return Ok(None);
    }
    Ok(Some(Box::new(move |_| {
        ids.iter().for_each(|id| signals::unsubscribe(*id));
    })))
}

/// Windows has no self-pipe for signals yet (18 §3): Python's own handling stays.
#[cfg(not(unix))]
fn install_signal_receiver(_: &StartContext<'_, '_>) -> PyResult<Option<Uninstall>> {
    Ok(None)
}

/// In spec order: 26 §2 step 1.
const STEPS: [Step; 4] = [
    Step {
        name: "asyncgen_hooks",
        applies: |_| true,
        install: nothing_to_install,
    },
    Step {
        name: "fork_hooks",
        applies: |_| true,
        install: |context| {
            crate::api::runtime::install_fork_hooks(context.py)?;
            // Registered for the life of the process: `os.register_at_fork` cannot undo it.
            Ok(None)
        },
    },
    Step {
        name: "signal_receiver",
        // Signals reach the main thread only; a runtime started elsewhere has no receiver.
        applies: |context| context.config.handle_signals() && context.on_main_thread,
        install: install_signal_receiver,
    },
    Step {
        name: "import_hook",
        // Only a free-threaded build needs to guard the import of extensions (25 §6).
        applies: |_| cfg!(Py_GIL_DISABLED),
        install: nothing_to_install,
    },
];

/// What the last run in the process installed and removed, by step name (`install:name`,
/// `uninstall:name`), for tests.
static TRACE: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn trace(event: String) {
    TRACE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(event);
}

/// The steps the last run installed, then removed, in order.
pub(crate) fn last_trace() -> Vec<String> {
    TRACE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// What the start installed; removes it, last first, when dropped.
pub(crate) struct Installed {
    undo: Vec<(&'static str, Option<Uninstall>)>,
}

/// Runs the steps that apply to this run, in order. If one fails, the ones before it are removed
/// and the error is returned.
pub(crate) fn install(
    py: Python<'_>,
    config: &RuntimeConfig,
    on_main_thread: bool,
    lifecycle: &Arc<Lifecycle>,
    handle: &Handle,
) -> PyResult<Installed> {
    TRACE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    let context = StartContext {
        py,
        config,
        on_main_thread,
        lifecycle,
        handle,
    };
    let mut installed = Installed { undo: Vec::new() };
    for step in &STEPS {
        if !(step.applies)(&context) {
            continue;
        }
        let undo = (step.install)(&context)?;
        trace(format!("install:{}", step.name));
        installed.undo.push((step.name, undo));
    }
    Ok(installed)
}

impl Drop for Installed {
    fn drop(&mut self) {
        while let Some((name, undo)) = self.undo.pop() {
            if let Some(undo) = undo {
                // SAFETY: dropped on the lane thread at the end of the run, which is attached.
                undo(unsafe { Python::assume_attached() });
            }
            trace(format!("uninstall:{name}"));
        }
    }
}

/// Whether the calling thread is the interpreter's main thread (`threading.main_thread()`).
pub(crate) fn on_main_thread(py: Python<'_>) -> bool {
    let check = || -> PyResult<bool> {
        let threading = py.import("threading")?;
        let main = threading.call_method0("main_thread")?;
        let current = threading.call_method0("current_thread")?;
        Ok(main.is(&current))
    };
    // A failing import is not a reason to stop the run: assume the main thread, where it
    // matters, and let the signal step find out.
    check().unwrap_or(true)
}
