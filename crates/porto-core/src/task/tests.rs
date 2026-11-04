//! Task-core tests against a mock scheduler that records what gets queued.

#![allow(
    clippy::waker_clone_wake,
    reason = "the by-value wake path is under test"
)]

use std::collections::VecDeque;
use std::future::poll_fn;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::task::{Poll, Waker};
use std::thread;

use super::{Notified, Schedule, new_task};

/// Never on a lane: every wake goes through `schedule_remote` into a shared queue.
#[derive(Clone, Default)]
struct MockScheduler(Arc<Mutex<VecDeque<Notified>>>);

impl Schedule for MockScheduler {
    fn schedule_local(&self, task: Notified) -> Result<(), Notified> {
        Err(task)
    }
    fn schedule_remote(self, task: Notified) {
        self.0.lock().unwrap().push_back(task);
    }
}

impl MockScheduler {
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    fn pop(&self) -> Option<Notified> {
        self.0.lock().unwrap().pop_front()
    }
}

/// Counts drops of the value it is moved into, i.e. drops of the future that owns it.
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counter() -> (Arc<AtomicUsize>, DropCounter) {
    let count = Arc::new(AtomicUsize::new(0));
    (count.clone(), DropCounter(count))
}

type Slot = Arc<Mutex<Option<Waker>>>;

/// Spawns `poll` as a task on a fresh mock scheduler. The future owns a `DropCounter`.
fn spawn(
    mut poll: impl FnMut(&Waker) -> Poll<()> + Send + 'static,
) -> (MockScheduler, Notified, Arc<AtomicUsize>) {
    let sched = MockScheduler::default();
    let (drops, guard) = counter();
    let future = poll_fn(move |cx| {
        let _owned = &guard;
        poll(cx.waker())
    });
    let task = new_task(future, sched.clone());
    (sched, task, drops)
}

/// A future that parks its waker in `slot` and stays pending.
fn park_in(slot: &Slot) -> impl FnMut(&Waker) -> Poll<()> + Send + 'static {
    let slot = slot.clone();
    move |waker| {
        *slot.lock().unwrap() = Some(waker.clone());
        Poll::Pending
    }
}

#[test]
fn wake_while_running_reschedules_exactly_once() {
    let polls = Arc::new(AtomicUsize::new(0));
    let polls_in_task = polls.clone();
    let (sched, task, drops) = spawn(move |waker| {
        if polls_in_task.fetch_add(1, Ordering::SeqCst) == 0 {
            waker.wake_by_ref();
            waker.wake_by_ref();
            waker.clone().wake();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    });

    task.run();
    assert_eq!(
        sched.len(),
        1,
        "three wakes during one poll requeue the task once"
    );
    let requeued = sched.pop().unwrap();
    let state = requeued.header().state().load();
    assert!(state.is_scheduled() && !state.is_running() && !state.is_notified());

    requeued.run();
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(sched.len(), 0);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "completion drops the future"
    );
}

#[test]
fn wake_after_complete_is_a_noop() {
    let slot = Slot::default();
    let slot_in_task = slot.clone();
    let (sched, task, drops) = spawn(move |waker| {
        *slot_in_task.lock().unwrap() = Some(waker.clone());
        Poll::Ready(())
    });

    task.run();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let waker = slot.lock().unwrap().take().unwrap();
    waker.wake_by_ref();
    assert_eq!(sched.len(), 0, "a complete task is never queued again");
    // The by-value wake drops the last reference: Miri's leak check proves the free.
    waker.wake();
    assert_eq!(sched.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn last_reference_drop_frees_the_task() {
    let slot = Slot::default();
    let (sched, task, drops) = spawn(park_in(&slot));

    task.run();
    assert_eq!(sched.len(), 0);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "the parked waker keeps the task alive"
    );
    let waker = slot.lock().unwrap().take().unwrap();
    let clone = waker.clone();
    drop(waker);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(clone);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the last waker frees the future"
    );
}

#[test]
fn pending_task_without_wakers_is_freed() {
    let (sched, task, drops) = spawn(|_| Poll::Pending);
    task.run();
    assert_eq!(sched.len(), 0);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "nothing can wake it, so it is freed"
    );
}

#[test]
fn dropping_a_queued_task_frees_it() {
    let (_sched, task, drops) = spawn(|_| Poll::Ready(()));
    drop(task);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn wakes_of_an_idle_task_schedule_it_once() {
    let slot = Slot::default();
    let (sched, task, drops) = spawn(park_in(&slot));
    task.run();

    let waker = slot.lock().unwrap().take().unwrap();
    waker.wake_by_ref();
    waker.wake_by_ref();
    waker.clone().wake();
    assert_eq!(sched.len(), 1);
    drop(waker);
    drop(sched.pop());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn concurrent_wakes_from_other_threads_schedule_once() {
    const THREADS: usize = 4;
    let rounds = if cfg!(miri) { 3 } else { 200 };
    let slot = Slot::default();
    let (sched, task, drops) = spawn(park_in(&slot));
    task.run();

    for _ in 0..rounds {
        let waker = slot.lock().unwrap().take().unwrap();
        let barrier = Arc::new(Barrier::new(THREADS));
        let wakers: Vec<thread::JoinHandle<()>> = (0..THREADS)
            .map(|i| {
                let waker = waker.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    if i % 2 == 0 {
                        waker.wake_by_ref();
                    } else {
                        waker.wake();
                    }
                })
            })
            .collect();
        drop(waker);
        for handle in wakers {
            handle.join().unwrap();
        }
        assert_eq!(sched.len(), 1, "racing wakes queue the task exactly once");
        // Run it again: it parks a fresh waker for the next round.
        sched.pop().unwrap().run();
    }
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(slot.lock().unwrap().take());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn wake_from_another_thread_during_poll_requeues_once() {
    let rounds = if cfg!(miri) { 3 } else { 200 };
    for _ in 0..rounds {
        let (sched, task, drops) = spawn(|waker| {
            let waker = waker.clone();
            // Races with the end of this poll: either NOTIFIED (requeue at poll end) or an
            // idle wake (requeue from the other thread). Both must queue it exactly once.
            thread::spawn(move || waker.wake()).join().unwrap();
            Poll::Pending
        });
        task.run();
        assert_eq!(sched.len(), 1);
        drop(sched.pop());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn panic_in_poll_completes_the_task_and_resumes_the_panic() {
    let slot = Slot::default();
    let slot_in_task = slot.clone();
    let (sched, task, drops) = spawn(move |waker| {
        *slot_in_task.lock().unwrap() = Some(waker.clone());
        panic!("boom");
    });
    let result = panic::catch_unwind(AssertUnwindSafe(|| task.run()));
    assert!(result.is_err());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the future is dropped on panic"
    );
    let waker = slot.lock().unwrap().take().unwrap();
    waker.wake();
    assert_eq!(sched.len(), 0, "a panicked task is complete");
}

#[test]
fn ids_increase_in_spawn_order() {
    let a = new_task(async {}, MockScheduler::default());
    let b = new_task(async {}, MockScheduler::default());
    assert!(b.id() > a.id());
}
