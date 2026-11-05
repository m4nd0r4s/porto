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

use super::{Notified, Schedule, TaskFlags, new_task};

/// Never on a lane: every wake goes through `schedule_remote` into a shared queue.
#[derive(Clone, Default)]
struct MockScheduler(Arc<Mutex<VecDeque<Notified>>>);

// SAFETY: `schedule_local` always returns the task untouched.
unsafe impl Schedule for MockScheduler {
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
        let waker_thread = Arc::new(Mutex::new(None));
        let waker_thread_in_task = waker_thread.clone();
        let (sched, task, drops) = spawn(move |waker| {
            let waker = waker.clone();
            // Not joined here: the wake races with the end of this poll. It lands either
            // while RUNNING (requeued when the poll returns) or after (queued by the waking
            // thread). Both must queue the task exactly once.
            *waker_thread_in_task.lock().unwrap() = Some(thread::spawn(move || waker.wake()));
            Poll::Pending
        });
        task.run();
        let waker_thread = waker_thread.lock().unwrap().take().unwrap();
        waker_thread.join().unwrap();
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

fn refs(task: &Notified) -> u32 {
    task.header().state().load().ref_count()
}

#[test]
fn task_refs_count_in_the_state_word() {
    let (_sched, task, drops) = spawn(|_| Poll::Ready(()));
    assert_eq!(refs(&task), 1, "the queue entry");
    let a = task.task_ref();
    let b = a.clone();
    assert_eq!(refs(&task), 3);
    assert_eq!((a.id(), b.id()), (task.id(), task.id()));
    drop(a);
    assert_eq!(refs(&task), 2);
    drop(b);
    assert_eq!(refs(&task), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(task);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the last reference frees the task"
    );
}

#[test]
fn a_task_ref_outlives_the_queue_entry() {
    let (_sched, task, drops) = spawn(|_| Poll::Ready(()));
    let held = task.task_ref();
    drop(task);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(held.header().state().load().ref_count(), 1);
    drop(held);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn completion_drops_the_future_but_a_task_ref_keeps_the_header() {
    let (_sched, task, drops) = spawn(|_| Poll::Ready(()));
    let held = task.task_ref();
    let id = task.id();
    task.run();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the future goes at completion"
    );
    let state = held.header().state().load();
    assert!(state.is_complete() && !state.is_running() && !state.is_scheduled());
    assert_eq!(state.ref_count(), 1);
    assert_eq!(held.id(), id);
}

#[test]
fn a_task_ref_becomes_a_waker_that_owns_its_reference() {
    let slot = Slot::default();
    let (sched, task, drops) = spawn(park_in(&slot));
    let waker = task.task_ref().into_waker();
    task.run();
    drop(slot.lock().unwrap().take());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "our waker keeps the task alive"
    );
    waker.wake();
    let requeued = sched.pop().expect("the wake queued the idle task");
    assert_eq!(requeued.header().state().load().ref_count(), 1);
    drop(requeued);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn task_ref_clones_from_many_threads_balance_out() {
    let (_sched, task, drops) = spawn(|_| Poll::Ready(()));
    let per_thread = if cfg!(miri) { 20 } else { 1000 };
    let start = Arc::new(Barrier::new(4));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let (held, start) = (task.task_ref(), start.clone());
            thread::spawn(move || {
                start.wait();
                for _ in 0..per_thread {
                    drop(held.clone());
                }
            })
        })
        .collect();
    for handle in threads {
        handle.join().unwrap();
    }
    assert_eq!(refs(&task), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}

#[test]
fn rust_tasks_have_no_flags_and_name_their_future() {
    let task = new_task(async {}, MockScheduler::default());
    assert_eq!(task.header().flags(), TaskFlags::NONE);
    assert!(
        task.header().type_name().contains("tests"),
        "type name: {}",
        task.header().type_name()
    );
}

#[test]
fn ids_increase_in_spawn_order() {
    let a = new_task(async {}, MockScheduler::default());
    let b = new_task(async {}, MockScheduler::default());
    assert!(b.id() > a.id());
}

#[test]
fn wake_and_waker_drop_race_with_completion_free_the_task_once() {
    let rounds = if cfg!(miri) { 3 } else { 200 };
    for _ in 0..rounds {
        let waker_thread = Arc::new(Mutex::new(None));
        let waker_thread_in_task = waker_thread.clone();
        let (sched, task, drops) = spawn(move |waker| {
            let (by_ref, by_val) = (waker.clone(), waker.clone());
            // Both run while this poll completes the task and the lane drops its reference:
            // the last of the three references must free the task, exactly once.
            *waker_thread_in_task.lock().unwrap() = Some(thread::spawn(move || {
                by_ref.wake_by_ref();
                drop(by_ref);
                by_val.wake();
            }));
            Poll::Ready(())
        });
        task.run();
        let waker_thread = waker_thread.lock().unwrap().take().unwrap();
        waker_thread.join().unwrap();
        assert_eq!(sched.len(), 0, "a completed task is never queued");
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
