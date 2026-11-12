//! The thread-state rules (25-cpython-integration-and-memory.md P1, ST-21): a reference owned by
//! a lane is dropped attached, and one dropped anywhere else is released on the lane.

use std::future::poll_fn;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::thread;

use porto_core::config::RuntimeConfig;
use pyo3::prelude::*;

use crate::driver::lane;
use crate::driver::pytask::{self, Spawned};
use crate::driver::thin::BoxedFuture;
use crate::driver::trap::{Trap, TrapKind};
use crate::pyref::{PyDeferred, PyObj};

/// `events` collects the thread each `Guard` was finalized on; `victim(trap)` holds one while it
/// awaits `trap`.
const MODULE: &std::ffi::CStr = cr#"
import threading

events = []

def ident():
    return threading.get_ident()

class Guard:
    def __del__(self):
        events.append(threading.get_ident())

async def victim(trap):
    guard = Guard()
    await trap
"#;

/// A module of its own per test: the tests run in parallel in one interpreter, and a module
/// of the same name would be executed again over the first one's `events`.
fn module<'py>(py: Python<'py>, name: &std::ffi::CStr) -> Bound<'py, PyModule> {
    PyModule::from_code(py, MODULE, c"pyref_tests.py", name).expect("the module compiles")
}

fn events(module: &Bound<'_, PyModule>) -> Vec<u64> {
    module
        .getattr("events")
        .unwrap()
        .extract()
        .expect("a list of thread ids")
}

fn lane_ident(module: &Bound<'_, PyModule>) -> u64 {
    module.call_method0("ident").unwrap().extract().unwrap()
}

#[cfg(debug_assertions)]
#[test]
fn dropping_a_lane_owned_reference_without_a_thread_state_panics_in_a_debug_build() {
    Python::initialize();
    let owned = Python::attach(|py| PyObj::new(py.None()));
    // A thread Python did not make, never attached: what a Rust-only I/O thread is.
    let outcome = thread::spawn(move || drop(owned)).join();
    let payload = outcome.expect_err("the drop must assert");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("no attached thread state"),
        "unexpected panic: {message}"
    );
}

#[test]
fn dropping_a_lane_owned_reference_attached_is_fine() {
    Python::initialize();
    Python::attach(|py| drop(PyObj::new(py.None())));
}

#[test]
fn a_deferred_reference_dropped_on_a_foreign_thread_is_released_on_the_lane() {
    Python::initialize();
    Python::attach(|py| {
        let module = module(py, c"pyref_tests_a_deferred_reference_dropped_on_a_foreig");
        let lane_thread = lane_ident(&module);
        let guard_class = module.getattr("Guard").unwrap();
        lane::run(py, &RuntimeConfig::default(), |runtime| {
            const OBJECTS: usize = 500;
            let handle = runtime.handle().clone();
            let deferred: Vec<PyDeferred> = (0..OBJECTS)
                .map(|_| {
                    let guard = guard_class.call0().unwrap().unbind();
                    PyDeferred::new(guard, &handle)
                })
                .collect();
            // Dropped detached, off the lane: queued, not released.
            py.detach(|| thread::spawn(move || drop(deferred)).join().unwrap());
            assert_eq!(events(&module), Vec::<u64>::new(), "released off the lane");
            // The maintenance tick releases them, on this thread.
            runtime.run_until_idle();
            let finalized = events(&module);
            assert_eq!(finalized.len(), OBJECTS, "leaked: {finalized:?}");
            assert!(finalized.iter().all(|&thread| thread == lane_thread));
        })
        .unwrap();
    });
}

#[test]
fn a_deferred_reference_dropped_on_the_lane_is_released_at_once() {
    Python::initialize();
    Python::attach(|py| {
        let module = module(py, c"pyref_tests_a_deferred_reference_dropped_on_the_lane");
        let guard_class = module.getattr("Guard").unwrap();
        lane::run(py, &RuntimeConfig::default(), |runtime| {
            let guard = guard_class.call0().unwrap().unbind();
            // Not the current lane before `block_on` or `run_until_idle` enters it, so run
            // the drop inside one.
            let deferred = PyDeferred::new(guard, runtime.handle());
            runtime.block_on(async move { drop(deferred) });
            assert_eq!(events(&module).len(), 1);
        })
        .unwrap();
    });
}

#[test]
fn work_queued_when_the_runtime_ends_is_released_when_it_is_dropped() {
    Python::initialize();
    Python::attach(|py| {
        let module = module(py, c"pyref_tests_work_queued_when_the_runtime_ends_is_rel");
        let guard_class = module.getattr("Guard").unwrap();
        lane::run(py, &RuntimeConfig::default(), |runtime| {
            let guard = guard_class.call0().unwrap().unbind();
            let deferred = PyDeferred::new(guard, runtime.handle());
            // Queued from a foreign thread, and never run by a maintenance tick.
            py.detach(|| thread::spawn(move || drop(deferred)).join().unwrap());
            assert!(events(&module).is_empty());
        })
        .unwrap();
        // The runtime's drop ran what was queued, attached, before `run` returned.
        assert_eq!(events(&module).len(), 1);
    });
}

#[test]
fn a_deferred_reference_taken_back_is_not_queued() {
    Python::initialize();
    Python::attach(|py| {
        let module = module(py, c"pyref_tests_a_deferred_reference_taken_back");
        let guard_class = module.getattr("Guard").unwrap();
        lane::run(py, &RuntimeConfig::default(), |runtime| {
            let guard = guard_class.call0().unwrap().unbind();
            let deferred = PyDeferred::new(guard, runtime.handle());
            let taken = deferred.into_inner(py);
            runtime.run_until_idle();
            assert!(events(&module).is_empty(), "taken back, still alive");
            drop(taken);
            assert_eq!(events(&module).len(), 1);
        })
        .unwrap();
    });
}

#[test]
fn a_deferred_reference_released_after_the_run_is_dropped_where_it_is() {
    Python::initialize();
    Python::attach(|py| {
        let module = module(py, c"pyref_tests_a_deferred_reference_released_after_the_");
        let guard_class = module.getattr("Guard").unwrap();
        let deferred = lane::run(py, &RuntimeConfig::default(), |runtime| {
            let guard = guard_class.call0().unwrap().unbind();
            PyDeferred::new(guard, runtime.handle())
        })
        .unwrap();
        assert!(events(&module).is_empty(), "the run does not own it");
        // The runtime is gone: no lane to route to, and this thread is attached.
        drop(deferred);
        assert_eq!(events(&module).len(), 1);
    });
}

/// A trap whose wait parks, with its waker left in `slot`.
fn parked_trap(slot: &Arc<Mutex<Option<Waker>>>) -> Trap {
    let slot = slot.clone();
    Trap::new(TrapKind::Future(BoxedFuture::new(poll_fn(move |cx| {
        *slot.lock().unwrap() = Some(cx.waker().clone());
        Poll::<pytask::Resume>::Pending
    }))))
}

#[test]
fn a_task_whose_last_waker_is_dropped_on_a_foreign_thread_is_freed_on_the_lane() {
    Python::initialize();
    Python::attach(|py| {
        let module = module(py, c"pyref_tests_a_task_whose_last_waker_is_dropped_on_a_");
        let lane_thread = lane_ident(&module);
        lane::run(py, &RuntimeConfig::default(), |runtime| {
            let slot = Arc::new(Mutex::new(None));
            let trap = Py::new(py, parked_trap(&slot)).unwrap();
            let coro = module.getattr("victim").unwrap().call1((trap,)).unwrap();
            // Detached: nobody joins it, and the waker in `slot` is the last reference.
            pytask::spawn(runtime.handle(), coro, Spawned::Detached).unwrap();
            runtime.run_until_idle();
            let waker = slot.lock().unwrap().take().expect("the task parked");
            assert!(events(&module).is_empty());
            // The foreign thread frees the task: it has no attached thread state.
            py.detach(|| thread::spawn(move || drop(waker)).join().unwrap());
            assert!(
                events(&module).is_empty(),
                "the coroutine was released off the lane"
            );
            runtime.run_until_idle();
            assert_eq!(events(&module), vec![lane_thread], "released on the lane");
        })
        .unwrap();
    });
}

#[test]
fn a_task_freed_on_the_lane_releases_its_coroutine_at_once() {
    Python::initialize();
    Python::attach(|py| {
        let module = module(py, c"pyref_tests_a_task_freed_on_the_lane_releases_its_co");
        lane::run(py, &RuntimeConfig::default(), |runtime| {
            let slot = Arc::new(Mutex::new(None));
            let trap = Py::new(py, parked_trap(&slot)).unwrap();
            let coro = module.getattr("victim").unwrap().call1((trap,)).unwrap();
            pytask::spawn(runtime.handle(), coro, Spawned::Detached).unwrap();
            runtime.run_until_idle();
            let waker = slot.lock().unwrap().take().expect("the task parked");
            runtime.block_on(async move { drop(waker) });
            assert_eq!(events(&module).len(), 1, "released on the lane, at once");
        })
        .unwrap();
    });
}
