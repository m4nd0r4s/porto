//! The steps of a runtime's start that other features fill in (26 §2 step 1), installed in the
//! order the spec lists and undone in reverse when the run stops (step 6).
//!
//! Each step is an extension point: it says when it applies, and what it installs. The owners
//! (the asyncgen hooks of 11 §9, the fork hooks of 25 §3, the signal receiver of 18, the import
//! hook of 25 §6) fill their step in; until then it installs nothing, so a run does exactly what
//! it did before. What ran, in which order, is kept for the `_startup_trace` test hook.

use std::sync::Mutex;

use porto_core::config::RuntimeConfig;
use pyo3::prelude::*;

/// What a step may look at when it decides and installs.
pub(crate) struct StartContext<'a, 'py> {
    #[allow(dead_code, reason = "read by the steps once their owners fill them in")]
    pub(crate) py: Python<'py>,
    pub(crate) config: &'a RuntimeConfig,
    /// Whether the thread that starts the runtime (lane 0) is the process's main thread, the only
    /// one Python delivers signals to.
    pub(crate) on_main_thread: bool,
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
        install: nothing_to_install,
    },
    Step {
        name: "signal_receiver",
        // Signals reach the main thread only; a runtime started elsewhere has no receiver.
        applies: |context| context.config.handle_signals() && context.on_main_thread,
        install: nothing_to_install,
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
) -> PyResult<Installed> {
    TRACE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    let context = StartContext {
        py,
        config,
        on_main_thread,
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
