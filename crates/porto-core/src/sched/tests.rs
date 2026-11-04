//! Single-lane runtime tests.

#![allow(
    clippy::waker_clone_wake,
    reason = "the by-value wake path is under test"
)]

use std::future::{Future, poll_fn};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;

use super::{Runtime, spawn};

/// A one-way flag a task (or thread) sets and a future awaits.
#[derive(Clone, Default)]
struct Signal(Arc<Mutex<(bool, Option<Waker>)>>);

impl Signal {
    fn set(&self) {
        let waker = {
            let mut inner = self.0.lock().unwrap();
            inner.0 = true;
            inner.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let signal = self.clone();
        poll_fn(move |cx| {
            let mut inner = signal.0.lock().unwrap();
            if inner.0 {
                Poll::Ready(())
            } else {
                inner.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        })
    }
}

/// Pending once, waking itself while running: one round trip through the run queue.
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn yield_now() -> YieldNow {
    YieldNow(false)
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn iterations(normal: usize) -> usize {
    if cfg!(miri) { 5 } else { normal }
}

#[test]
fn block_on_returns_the_output() {
    let rt = Runtime::new();
    assert_eq!(rt.block_on(async { 41 + 1 }), 42);
}

#[test]
fn spawned_tasks_run_in_fifo_order() {
    let rt = Runtime::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    for i in 0..10 {
        let order = order.clone();
        rt.spawn(async move { order.lock().unwrap().push(i) });
    }
    assert_eq!(rt.run_until_idle(), 10);
    assert_eq!(*order.lock().unwrap(), (0..10).collect::<Vec<_>>());
}

#[test]
fn tasks_spawn_tasks_and_yield() {
    let rt = Runtime::new();
    let done = Signal::default();
    let count = Arc::new(AtomicUsize::new(0));
    let tasks = iterations(100);
    {
        let (done, count) = (done.clone(), count.clone());
        rt.spawn(async move {
            for _ in 0..tasks {
                let (done, count) = (done.clone(), count.clone());
                spawn(async move {
                    yield_now().await;
                    if count.fetch_add(1, Ordering::SeqCst) + 1 == tasks {
                        done.set();
                    }
                });
            }
        });
    }
    rt.block_on(done.wait());
    assert_eq!(count.load(Ordering::SeqCst), tasks);
}

#[test]
fn self_wake_while_running_polls_once_per_wake() {
    let rt = Runtime::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let polls_in_task = polls.clone();
    rt.spawn(poll_fn(move |cx| {
        if polls_in_task.fetch_add(1, Ordering::SeqCst) == 0 {
            cx.waker().wake_by_ref();
            cx.waker().clone().wake();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }));
    assert_eq!(
        rt.run_until_idle(),
        2,
        "two wakes during one poll run the task once more"
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[test]
fn wakes_from_another_thread_reach_a_parked_lane() {
    let rt = Runtime::new();
    let rounds = iterations(1000);
    let (to_helper, from_task) = mpsc::channel::<Waker>();
    let helper = thread::spawn(move || {
        for waker in from_task {
            waker.wake();
        }
    });
    let done = Signal::default();
    let polls = Arc::new(AtomicUsize::new(0));
    {
        let (done, polls) = (done.clone(), polls.clone());
        rt.spawn(poll_fn(move |cx| {
            let n = polls.fetch_add(1, Ordering::SeqCst);
            if n == rounds {
                done.set();
                return Poll::Ready(());
            }
            to_helper.send(cx.waker().clone()).unwrap();
            Poll::Pending
        }));
    }
    rt.block_on(done.wait());
    assert_eq!(
        polls.load(Ordering::SeqCst),
        rounds + 1,
        "one poll per remote wake"
    );
    helper.join().unwrap();
}

#[test]
fn handle_spawn_from_another_thread_unparks_the_lane() {
    let rt = Runtime::new();
    let handle = rt.handle().clone();
    let done = Signal::default();
    let done_in_task = done.clone();
    let spawner = thread::spawn(move || handle.spawn(async move { done_in_task.set() }));
    rt.block_on(done.wait());
    spawner.join().unwrap();
}

#[test]
fn block_on_future_woken_from_another_thread() {
    let rt = Runtime::new();
    let signal = Signal::default();
    let setter = {
        let signal = signal.clone();
        thread::spawn(move || signal.set())
    };
    rt.block_on(signal.wait());
    setter.join().unwrap();
}

#[test]
fn dropping_the_runtime_frees_queued_and_parked_tasks() {
    let drops = Arc::new(AtomicUsize::new(0));
    let parked: Arc<Mutex<Option<Waker>>> = Arc::default();
    let rt = Runtime::new();
    {
        let guard = DropCounter(drops.clone());
        let parked = parked.clone();
        rt.spawn(poll_fn(move |cx| {
            let _owned = &guard;
            *parked.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }));
    }
    rt.run_until_idle();
    let guard = DropCounter(drops.clone());
    rt.spawn(async move { drop(guard) });
    drop(rt);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the queued task is freed with the runtime"
    );
    // Waking the parked task after shutdown frees it instead of queueing it forever.
    parked.lock().unwrap().take().unwrap().wake();
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn task_panic_is_resumed_by_block_on_and_the_runtime_survives() {
    let rt = Runtime::new();
    rt.spawn(async { panic!("task failed") });
    let result = panic::catch_unwind(AssertUnwindSafe(|| rt.block_on(Signal::default().wait())));
    assert!(result.is_err());
    assert_eq!(rt.block_on(async { 7 }), 7);
}

#[test]
#[should_panic(expected = "cannot run a runtime from inside a runtime")]
fn nested_block_on_panics_with_a_hint() {
    let rt = Runtime::new();
    let inner = Runtime::new();
    rt.block_on(async { inner.block_on(async {}) });
}

#[test]
#[should_panic(expected = "porto_core::spawn called outside a runtime")]
fn spawn_outside_a_runtime_panics_with_a_hint() {
    spawn(async {});
}

#[test]
fn a_task_that_keeps_yielding_does_not_starve_remote_spawns() {
    let rt = Runtime::new();
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = stop.clone();
        rt.spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                yield_now().await;
            }
        });
    }
    let done = Signal::default();
    let handle = rt.handle().clone();
    let (done_in_task, stop_in_task) = (done.clone(), stop.clone());
    // Lands in the inbox before the lane starts: the yielding task alone keeps the local
    // queue busy, so only the periodic inbox check can let this one run.
    thread::spawn(move || {
        handle.spawn(async move {
            stop_in_task.store(true, Ordering::SeqCst);
            done_in_task.set();
        });
    })
    .join()
    .unwrap();
    rt.block_on(done.wait());
    assert!(stop.load(Ordering::SeqCst));
}
