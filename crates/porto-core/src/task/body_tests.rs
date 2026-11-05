//! Tests of tasks with a [`TaskBody`]: completion, wakes, cancellation by shutdown, panics,
//! and joining through the body's [`JoinWaiters`].

use std::future::poll_fn;
use std::marker::PhantomPinned;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use super::{Header, TaskBody, TaskFlags};
use crate::Runtime;
use crate::sync::{JoinWaiters, WaitNode};

/// What a test body does on each poll.
enum Script {
    /// Finish after this many polls, each `Pending` one waking itself.
    YieldTimes(usize),
    /// Park on the first poll, leaving its waker in `parked`; finish on the next.
    Park,
    /// Panic.
    Panic,
}

struct TestBody {
    script: Script,
    polls: Arc<AtomicUsize>,
    cancels: Arc<AtomicUsize>,
    parked: Arc<std::sync::Mutex<Option<Waker>>>,
    /// The outcome, written before `join` is closed.
    outcome: AtomicUsize,
    join: JoinWaiters,
    _pinned: PhantomPinned,
}

impl TestBody {
    fn new(script: Script) -> Self {
        Self {
            script,
            polls: Arc::default(),
            cancels: Arc::default(),
            parked: Arc::default(),
            outcome: AtomicUsize::new(0),
            join: JoinWaiters::new(),
            _pinned: PhantomPinned,
        }
    }

    fn finish(&self, outcome: usize) {
        self.outcome.store(outcome, Ordering::Relaxed);
        self.join.close();
    }
}

impl TaskBody for TestBody {
    fn poll(self: Pin<&mut Self>, _: &Header, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: nothing is moved out.
        let this = unsafe { self.get_unchecked_mut() };
        let polls = this.polls.fetch_add(1, Ordering::Relaxed) + 1;
        match this.script {
            Script::YieldTimes(times) if polls <= times => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Script::Park if polls == 1 => {
                *this.parked.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
            Script::Panic => panic!("the body panicked"),
            _ => {
                this.finish(polls);
                Poll::Ready(())
            }
        }
    }

    fn cancel(self: Pin<&mut Self>, _: &Header) {
        self.cancels.fetch_add(1, Ordering::Relaxed);
        self.finish(usize::MAX);
    }
}

#[test]
fn a_body_task_runs_to_completion_and_its_joiner_reads_the_outcome() {
    let rt = Runtime::new();
    let task = rt
        .handle()
        .spawn_body(TestBody::new(Script::YieldTimes(3)), TaskFlags::PYTHON);
    assert!(task.header().flags().contains(TaskFlags::PYTHON));
    let body = task.body();
    let node = Box::pin(WaitNode::new(0));
    rt.block_on(poll_fn(|cx| {
        // SAFETY: the node is pinned in a box that outlives the wait, used by this future
        // only; `task` keeps the body (and its list) alive.
        unsafe { (*body.as_ptr()).join.poll_wait(node.as_ref(), cx) }
    }));
    assert!(task.is_complete());
    // SAFETY: published before the close that ended the wait.
    let outcome = unsafe { (*body.as_ptr()).outcome.load(Ordering::Relaxed) };
    assert_eq!(outcome, 4, "three yields and the final poll");
}

#[test]
fn a_parked_body_resumes_on_a_wake_from_another_thread() {
    let rt = Runtime::new();
    let body = TestBody::new(Script::Park);
    let parked = body.parked.clone();
    let task = rt.handle().spawn_body(body, TaskFlags::NONE);
    rt.run_until_idle();
    let waker = parked.lock().unwrap().take().expect("the body parked");
    std::thread::spawn(move || waker.wake()).join().unwrap();
    rt.run_until_idle();
    assert!(task.is_complete());
}

#[test]
fn shutting_down_cancels_a_queued_body_once() {
    let rt = Runtime::new();
    let body = TestBody::new(Script::YieldTimes(usize::MAX));
    let (polls, cancels) = (body.polls.clone(), body.cancels.clone());
    let task = rt.handle().spawn_body(body, TaskFlags::NONE);
    drop(rt);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(cancels.load(Ordering::Relaxed), 1);
    assert!(task.is_complete());
    // SAFETY: the task completed: its outcome is published.
    let outcome = unsafe { (*task.body().as_ptr()).outcome.load(Ordering::Relaxed) };
    assert_eq!(outcome, usize::MAX);
}

#[test]
fn a_parked_body_is_freed_with_its_last_waker() {
    let rt = Runtime::new();
    let body = TestBody::new(Script::Park);
    let (parked, polls) = (body.parked.clone(), body.polls.clone());
    let task = rt.handle().spawn_body(body, TaskFlags::NONE);
    rt.run_until_idle();
    let freed = Arc::downgrade(&polls);
    drop(polls);
    drop(task);
    assert!(freed.upgrade().is_some(), "the parked waker keeps it");
    drop(parked.lock().unwrap().take());
    assert!(freed.upgrade().is_none(), "the last waker did not free it");
}

#[test]
fn a_panicking_body_completes_and_the_panic_reaches_the_lane() {
    let rt = Runtime::new();
    let task = rt
        .handle()
        .spawn_body(TestBody::new(Script::Panic), TaskFlags::NONE);
    let panicked = panic::catch_unwind(AssertUnwindSafe(|| rt.run_until_idle()));
    assert!(panicked.is_err());
    assert!(task.is_complete());
    assert_eq!(rt.run_until_idle(), 0, "the runtime stays usable");
}
