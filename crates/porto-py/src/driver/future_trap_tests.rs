//! The `Future` and `Io` traps, end to end: a coroutine awaits a trap carrying a Rust future,
//! run as a PyTask on a Python lane (11-task-model-and-trap-protocol.md §5.2, §5.3 T1, T3).

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;

use porto_core::config::RuntimeConfig;
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;

use super::lane;
use super::pending::IoWait;
use super::pytask::{self, Resume, Spawned};
use super::thin::BoxedFuture;
use super::trap::{Trap, TrapKind};

/// `main(trap)`: awaits the trap; returns `("value", v)` or `("raised", repr(exc))`.
const MAIN: &std::ffi::CStr = cr#"
async def main(trap):
    try:
        return ("value", await trap)
    except BaseException as error:
        return ("raised", repr(error))
"#;

/// Runs `main(trap)` as the main task of a fresh Python lane; returns what it returned.
fn run_main(trap: TrapKind) -> (String, String) {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::from_code(py, MAIN, c"future_trap_tests.py", c"future_trap_tests")
            .expect("the test module compiles");
        let trap = Py::new(py, Trap::new(trap)).expect("a trap");
        let coro = module.getattr("main").unwrap().call1((trap,)).unwrap();
        let outcome = lane::run(py, &RuntimeConfig::default(), |runtime| {
            let main = pytask::spawn(runtime.handle(), coro, Spawned::Joined).unwrap();
            runtime.block_on(pytask::join(main))
        })
        .expect("the I/O driver opens");
        let (tag, value): (String, Bound<'_, PyAny>) = outcome
            .into_result(py)
            .unwrap()
            .into_bound(py)
            .extract()
            .unwrap();
        let value = if tag == "value" {
            value.str().unwrap().to_string()
        } else {
            value.extract().unwrap()
        };
        (tag, value)
    })
}

fn value(text: &'static str) -> Resume {
    // SAFETY: futures are polled by the step loop, on the lane, attached.
    let py = unsafe { Python::assume_attached() };
    Resume::Value(pyo3::types::PyString::new(py, text).into_any().unbind())
}

#[test]
fn a_ready_future_resumes_the_await_with_its_value() {
    let trap = TrapKind::Future(BoxedFuture::new(async { value("at once") }));
    assert_eq!(run_main(trap), ("value".to_owned(), "at once".to_owned()));
}

#[test]
fn a_future_completed_by_another_thread_wakes_the_task() {
    let done = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let future = {
        let done = done.clone();
        let started = started.clone();
        poll_fn(move |cx: &mut Context<'_>| {
            if done.load(Ordering::Acquire) {
                return Poll::Ready(value("from a thread"));
            }
            if !started.swap(true, Ordering::AcqRel) {
                let done = done.clone();
                let waker = cx.waker().clone();
                thread::spawn(move || {
                    thread::sleep(std::time::Duration::from_millis(10));
                    done.store(true, Ordering::Release);
                    waker.wake();
                });
            }
            Poll::Pending
        })
    };
    let trap = TrapKind::Future(BoxedFuture::new(future));
    assert_eq!(
        run_main(trap),
        ("value".to_owned(), "from a thread".to_owned())
    );
}

#[test]
fn a_future_can_throw_into_the_coroutine() {
    let trap = TrapKind::Future(BoxedFuture::new(async {
        // SAFETY: polled on the lane, attached.
        let py = unsafe { Python::assume_attached() };
        Resume::Throw(PyValueError::new_err("from the future").into_value(py))
    }));
    assert_eq!(
        run_main(trap),
        (
            "raised".to_owned(),
            "ValueError('from the future')".to_owned()
        )
    );
}

/// Pending until dropped; interrupts its own task on the first poll, as a cancellation does.
struct InterruptedWait {
    dropped: Arc<AtomicBool>,
    /// The waker the future registered: what a T3 drop must release.
    registered: Arc<Mutex<Option<Waker>>>,
}

impl Future for InterruptedWait {
    type Output = Resume;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Resume> {
        let mut registered = self.registered.lock().unwrap();
        if registered.is_none() {
            *registered = Some(cx.waker().clone());
            // SAFETY: polled on the lane, attached.
            let py = unsafe { Python::assume_attached() };
            lane::interrupt(cx.waker(), PyKeyError::new_err("cancelled").into_value(py));
        }
        Poll::Pending
    }
}

impl Drop for InterruptedWait {
    fn drop(&mut self) {
        // T3: dropping the wait unregisters its waker.
        self.registered.lock().unwrap().take();
        self.dropped.store(true, Ordering::Release);
    }
}

fn interrupted_wait() -> (InterruptedWait, Arc<AtomicBool>, Arc<Mutex<Option<Waker>>>) {
    let dropped = Arc::new(AtomicBool::new(false));
    let registered = Arc::new(Mutex::new(None));
    let wait = InterruptedWait {
        dropped: dropped.clone(),
        registered: registered.clone(),
    };
    (wait, dropped, registered)
}

#[test]
fn an_interrupted_future_is_dropped_before_the_exception_is_thrown() {
    let (wait, dropped, registered) = interrupted_wait();
    let trap = TrapKind::Future(BoxedFuture::new(wait));
    assert_eq!(
        run_main(trap),
        ("raised".to_owned(), "KeyError('cancelled')".to_owned())
    );
    assert!(
        dropped.load(Ordering::Acquire),
        "the future outlived its wait"
    );
    assert!(
        registered.lock().unwrap().is_none(),
        "a waker stayed registered"
    );
}

#[test]
fn an_interrupted_io_wait_drops_its_operation() {
    let (wait, dropped, registered) = interrupted_wait();
    let trap = TrapKind::Io(IoWait::new(wait));
    assert_eq!(
        run_main(trap),
        ("raised".to_owned(), "KeyError('cancelled')".to_owned())
    );
    assert!(dropped.load(Ordering::Acquire));
    assert!(registered.lock().unwrap().is_none());
}

#[test]
fn an_io_wait_resumes_with_the_operations_result() {
    let trap = TrapKind::Io(IoWait::new(async { value("read") }));
    assert_eq!(run_main(trap), ("value".to_owned(), "read".to_owned()));
}
