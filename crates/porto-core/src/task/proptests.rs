//! Property test: whatever order wakes, polls, aborts, join-handle polls and drops come in, a
//! task is never polled while it is RUNNING, never queued twice, never polled after it
//! completed, and freed exactly once.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use proptest::prelude::*;

use super::{JoinError, JoinHandle, Notified, Schedule, new_joinable_task};

/// Never on a lane: every wake lands in a shared queue the test drains.
#[derive(Clone, Default)]
struct Queue(Arc<Mutex<VecDeque<Notified>>>);

// SAFETY: `schedule_local` always returns the task untouched.
unsafe impl Schedule for Queue {
    fn schedule_local(&self, task: Notified) -> Result<(), Notified> {
        Err(task)
    }
    fn schedule_remote(self, task: Notified) {
        self.0.lock().unwrap().push_back(task);
    }
}

impl Queue {
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    fn pop(&self) -> Option<Notified> {
        self.0.lock().unwrap().pop_front()
    }
}

/// What the future does on one poll.
#[derive(Clone, Copy, Debug)]
enum OnPoll {
    Pending,
    PendingKeepWaker,
    PendingWakeSelf,
    Ready,
}

/// One step of the test.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// Pop the queue entry and run it.
    Run,
    /// Pop the queue entry and drop it (what a closed scheduler does).
    DropQueued,
    WakeByRef(usize),
    WakeByVal(usize),
    CloneWaker(usize),
    DropWaker(usize),
    Abort,
    PollJoin,
    DropJoin,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => Just(Op::Run),
        1 => Just(Op::DropQueued),
        2 => (0..4usize).prop_map(Op::WakeByRef),
        2 => (0..4usize).prop_map(Op::WakeByVal),
        1 => (0..4usize).prop_map(Op::CloneWaker),
        1 => (0..4usize).prop_map(Op::DropWaker),
        1 => Just(Op::Abort),
        2 => Just(Op::PollJoin),
        1 => Just(Op::DropJoin),
    ]
}

fn on_poll() -> impl Strategy<Value = OnPoll> {
    prop_oneof![
        Just(OnPoll::Pending),
        Just(OnPoll::PendingKeepWaker),
        Just(OnPoll::PendingWakeSelf),
        Just(OnPoll::Ready),
    ]
}

/// What the test observed of the future, shared with it.
#[derive(Default)]
struct Probe {
    /// Polls in progress: never above one.
    active: AtomicUsize,
    polls: AtomicUsize,
    returned_ready: AtomicUsize,
    drops: AtomicUsize,
    wakers: Mutex<Vec<Waker>>,
}

struct Tracked(Arc<Probe>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn run_case(script: Vec<OnPoll>, ops: Vec<Op>) {
    let queue = Queue::default();
    let probe = Arc::new(Probe::default());
    let tracked = Tracked(probe.clone());
    let mut script = script.into_iter();
    let future = poll_fn(move |cx: &mut Context<'_>| {
        let probe = &tracked.0;
        assert_eq!(
            probe.active.fetch_add(1, Ordering::SeqCst),
            0,
            "polled while RUNNING"
        );
        assert_eq!(
            probe.returned_ready.load(Ordering::SeqCst),
            0,
            "polled after Ready"
        );
        probe.polls.fetch_add(1, Ordering::SeqCst);
        let result = match script.next().unwrap_or(OnPoll::Pending) {
            OnPoll::Pending => Poll::Pending,
            OnPoll::PendingKeepWaker => {
                probe.wakers.lock().unwrap().push(cx.waker().clone());
                Poll::Pending
            }
            OnPoll::PendingWakeSelf => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            OnPoll::Ready => {
                probe.returned_ready.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(7_u32)
            }
        };
        probe.active.fetch_sub(1, Ordering::SeqCst);
        result
    });
    let (task, join) = new_joinable_task(future, queue.clone());
    queue.clone().schedule_remote(task);
    let mut join: Option<JoinHandle<u32>> = Some(join);
    let mut output: Option<Result<u32, JoinError>> = None;
    let mut cancelled = false;
    let noop = Waker::noop();

    for op in ops {
        match op {
            Op::Run => {
                if let Some(task) = queue.pop() {
                    task.run();
                }
            }
            Op::DropQueued => {
                if let Some(task) = queue.pop() {
                    cancelled = true;
                    drop(task);
                }
            }
            Op::WakeByRef(i) => {
                let wakers = probe.wakers.lock().unwrap();
                if let Some(waker) = wakers.get(i) {
                    let waker = waker.clone();
                    drop(wakers);
                    waker.wake_by_ref();
                }
            }
            Op::WakeByVal(i) => {
                let mut wakers = probe.wakers.lock().unwrap();
                if i < wakers.len() {
                    let waker = wakers.swap_remove(i);
                    drop(wakers);
                    waker.wake();
                }
            }
            Op::CloneWaker(i) => {
                let mut wakers = probe.wakers.lock().unwrap();
                if let Some(waker) = wakers.get(i).cloned() {
                    wakers.push(waker);
                }
            }
            Op::DropWaker(i) => {
                let mut wakers = probe.wakers.lock().unwrap();
                if i < wakers.len() {
                    let waker = wakers.swap_remove(i);
                    drop(wakers);
                    drop(waker);
                }
            }
            Op::Abort => {
                if let Some(join) = &join {
                    cancelled |= !join.is_finished();
                    join.abort();
                }
            }
            Op::PollJoin => {
                if output.is_none()
                    && let Some(handle) = &mut join
                    && let Poll::Ready(result) =
                        Pin::new(handle).poll(&mut Context::from_waker(noop))
                {
                    output = Some(result);
                    join = None;
                }
            }
            Op::DropJoin => join = None,
        }
        assert!(queue.len() <= 1, "a task is never queued twice");
        if probe.drops.load(Ordering::SeqCst) > 0 {
            assert_eq!(queue.len(), 0, "a completed task is never queued");
        }
        assert!(probe.drops.load(Ordering::SeqCst) <= 1);
    }

    match output {
        Some(Ok(value)) => {
            assert_eq!(value, 7);
            assert_eq!(probe.returned_ready.load(Ordering::SeqCst), 1);
        }
        Some(Err(error)) => {
            assert!(error.is_cancelled(), "{error}");
            assert!(
                cancelled,
                "cancelled without an abort or a dropped queue entry"
            );
            assert_eq!(probe.returned_ready.load(Ordering::SeqCst), 0);
        }
        None => {}
    }

    // Tear down: every reference goes, so the task must be freed (the scheduler clone it
    // holds is dropped with it).
    drop(join);
    while let Some(task) = queue.pop() {
        drop(task);
    }
    probe.wakers.lock().unwrap().clear();
    assert_eq!(
        probe.drops.load(Ordering::SeqCst),
        1,
        "the future is dropped once"
    );
    assert_eq!(Arc::strong_count(&queue.0), 1, "the task is freed");
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 8 } else { 2048 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn a_task_is_never_polled_while_running(
        script in prop::collection::vec(on_poll(), 0..6),
        ops in prop::collection::vec(op(), 0..40),
    ) {
        run_case(script, ops);
    }
}
