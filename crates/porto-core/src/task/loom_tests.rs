//! loom models of the task state machine (`11-task-model-and-trap-protocol.md` §3): wake vs
//! poll end, wake vs completion, abort vs poll, and the join handle (poll and drop) vs
//! completion.
//!
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release task`

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

use super::{JoinError, JoinHandle, Notified, Schedule, new_joinable_task};

/// Never on a lane: every wake lands in a shared queue the model drains.
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
    /// Runs queued tasks until none is left. Returns how many ran.
    fn drain(&self) -> usize {
        let mut ran = 0;
        loop {
            let next = self.0.lock().unwrap().pop_front();
            let Some(task) = next else { return ran };
            task.run();
            ran += 1;
        }
    }
    /// Whether every task holding a clone of this queue has been freed.
    fn tasks_freed(&self) -> bool {
        Arc::strong_count(&self.0) == 1
    }
}

/// Counts drops of whatever owns it.
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A waker that counts its wakes.
#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_join<T>(join: &mut JoinHandle<T>, waker: &Waker) -> Poll<Result<T, JoinError>> {
    Pin::new(join).poll(&mut Context::from_waker(waker))
}

/// A wake from another thread races with the end of the poll that saw no event: the wake
/// lands while SCHEDULED (the poll then sees the event), while RUNNING (NOTIFIED requeues the
/// task) or after (the waker queues it). The event is published under a lock, as §3.1
/// requires; either way the task completes and is freed once.
#[test]
fn wake_vs_poll_end() {
    loom::model(|| {
        let queue = Queue::default();
        let event = Arc::new(Mutex::new(false));
        let event_in_task = event.clone();
        let (task, join) = new_joinable_task(
            poll_fn(move |_| {
                if *event_in_task.lock().unwrap() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
            queue.clone(),
        );
        let waker = task.task_ref().into_waker();
        let waker_thread = thread::spawn(move || {
            *event.lock().unwrap() = true;
            waker.wake();
        });
        task.run();
        waker_thread.join().unwrap();
        queue.drain();
        assert!(join.is_finished(), "the wake was lost");
        drop(join);
        assert!(queue.tasks_freed());
    });
}

/// The boundary of §3.1: a `wake_by_ref` that finds the task SCHEDULED writes nothing (a
/// by-value wake still drops its reference with a release RMW), so an event
/// published with a plain `Release` store before it may be missed by the poll it relies on
/// (the store-buffer pattern). Wakers MUST publish through a lock or a read-modify-write the
/// future also uses, as in `wake_vs_poll_end`. If wakes ever become release operations, this
/// model fails and §3.1 can promise more.
#[test]
#[should_panic(expected = "the wake was lost")]
fn a_plain_store_before_a_redundant_wake_is_not_published() {
    use loom::sync::atomic::AtomicBool;
    loom::model(|| {
        let queue = Queue::default();
        let event = Arc::new(AtomicBool::new(false));
        let event_in_task = event.clone();
        let (task, join) = new_joinable_task(
            poll_fn(move |_| {
                if event_in_task.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
            queue.clone(),
        );
        let waker = task.task_ref().into_waker();
        let waker_thread = thread::spawn(move || {
            event.store(true, Ordering::Release);
            waker.wake_by_ref();
            waker
        });
        task.run();
        let waker = waker_thread.join().unwrap();
        queue.drain();
        let finished = join.is_finished();
        drop((waker, join));
        queue.drain();
        assert!(finished, "the wake was lost");
    });
}

/// A wake (and the drop of the last waker) races with completion: the task is never queued
/// after COMPLETE, and whichever reference goes last frees it.
#[test]
fn wake_and_last_waker_drop_vs_completion() {
    loom::model(|| {
        let queue = Queue::default();
        let polls = Arc::new(AtomicUsize::new(0));
        let polls_in_task = polls.clone();
        let (task, join) = new_joinable_task(
            poll_fn(move |_| {
                polls_in_task.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(())
            }),
            queue.clone(),
        );
        drop(join);
        let waker = task.task_ref().into_waker();
        let waker_thread = thread::spawn(move || waker.wake());
        task.run();
        waker_thread.join().unwrap();
        assert_eq!(queue.drain(), 0, "a complete task is never queued");
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(queue.tasks_freed());
    });
}

/// An abort races with a poll: wherever it lands, the task ends up cancelled (its future
/// never completes on its own) and the joiner sees it.
#[test]
fn abort_vs_poll() {
    loom::model(|| {
        let queue = Queue::default();
        let drops = Arc::new(AtomicUsize::new(0));
        let guard = DropCounter(drops.clone());
        let (task, join) = new_joinable_task(
            poll_fn(move |_| -> Poll<()> {
                let _owned = &guard;
                Poll::Pending
            }),
            queue.clone(),
        );
        let join = Arc::new(join);
        let aborter = {
            let join = join.clone();
            thread::spawn(move || join.abort())
        };
        task.run();
        aborter.join().unwrap();
        queue.drain();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let Ok(mut join) = Arc::try_unwrap(join) else {
            panic!("the aborter dropped its clone");
        };
        assert!(matches!(
            poll_join(&mut join, Waker::noop()),
            Poll::Ready(Err(error)) if error.is_cancelled()
        ));
        drop(join);
        assert!(queue.tasks_freed());
    });
}

/// The join handle polls (registering its waker) while the task completes: either it gets
/// the output at once, or it is woken exactly once and gets it on the next poll.
#[test]
fn join_poll_vs_completion() {
    loom::model(|| {
        let queue = Queue::default();
        let (task, join) = new_joinable_task(poll_fn(|_| Poll::Ready(5_u8)), queue.clone());
        let joiner = thread::spawn(move || {
            let wakes = std::sync::Arc::new(CountingWaker::default());
            let waker = Waker::from(wakes.clone());
            let mut join = join;
            match poll_join(&mut join, &waker) {
                Poll::Ready(output) => (None, Some(output.unwrap()), wakes),
                Poll::Pending => (Some(join), None, wakes),
            }
        });
        task.run();
        let (join, output, wakes) = joiner.join().unwrap();
        if let Some(mut join) = join {
            assert_eq!(
                wakes.0.load(Ordering::SeqCst),
                1,
                "the joiner was not woken"
            );
            let Poll::Ready(Ok(output)) = poll_join(&mut join, Waker::noop()) else {
                panic!("woken but not ready");
            };
            assert_eq!(output, 5);
        } else {
            assert_eq!(output, Some(5));
        }
        assert!(queue.tasks_freed());
        assert_eq!(
            std::sync::Arc::strong_count(&wakes),
            1,
            "the join waker leaked"
        );
    });
}

/// The join handle is dropped (after registering a waker) while the task completes: the
/// output and the waker are each dropped exactly once, by whichever side owns them.
#[test]
fn join_drop_vs_completion() {
    loom::model(|| {
        let queue = Queue::default();
        let output_drops = Arc::new(AtomicUsize::new(0));
        let output = DropCounter(output_drops.clone());
        let mut output = Some(output);
        let (task, mut join) = new_joinable_task(
            poll_fn(move |_| Poll::Ready(output.take().unwrap())),
            queue.clone(),
        );
        let wakes = std::sync::Arc::new(CountingWaker::default());
        assert!(poll_join(&mut join, &Waker::from(wakes.clone())).is_pending());
        let dropper = thread::spawn(move || drop(join));
        task.run();
        dropper.join().unwrap();
        assert_eq!(output_drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            std::sync::Arc::strong_count(&wakes),
            1,
            "the join waker leaked"
        );
        assert!(queue.tasks_freed());
    });
}
