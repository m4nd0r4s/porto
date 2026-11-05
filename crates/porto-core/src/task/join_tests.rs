//! Join handle and abort tests against a mock scheduler.

use std::future::{Future, poll_fn};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::tests::{MockScheduler, counter};
use super::{JoinHandle, Notified, new_joinable_task};

/// A waker that counts its wakes.
#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting() -> (Arc<CountingWaker>, Waker) {
    let count = Arc::new(CountingWaker::default());
    (count.clone(), Waker::from(count))
}

fn poll_join<T>(join: &mut JoinHandle<T>, waker: &Waker) -> Poll<Result<T, super::JoinError>> {
    Pin::new(join).poll(&mut Context::from_waker(waker))
}

/// Spawns a joinable task whose future runs `poll` and owns a drop counter.
fn spawn<T: Send + 'static>(
    mut poll: impl FnMut(&mut Context<'_>) -> Poll<T> + Send + 'static,
) -> (MockScheduler, Notified, JoinHandle<T>, Arc<AtomicUsize>) {
    let sched = MockScheduler::default();
    let (drops, guard) = counter();
    let future = poll_fn(move |cx| {
        let _owned = &guard;
        poll(cx)
    });
    let (task, join) = new_joinable_task(future, sched.clone());
    (sched, task, join, drops)
}

#[test]
fn join_returns_the_output() {
    let (_sched, task, mut join, drops) = spawn(|_| Poll::Ready(42));
    assert!(!join.is_finished());
    task.run();
    assert!(join.is_finished());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the future goes at completion"
    );
    let Poll::Ready(Ok(42)) = poll_join(&mut join, Waker::noop()) else {
        panic!("the output is ready");
    };
}

#[test]
fn completion_wakes_the_registered_join_waker() {
    let (_sched, task, mut join, _drops) = spawn(|_| Poll::Ready("done"));
    let (wakes, waker) = counting();
    assert!(poll_join(&mut join, &waker).is_pending());
    assert!(join.task.header().state().load().is_join_waker_set());
    task.run();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        poll_join(&mut join, &waker),
        Poll::Ready(Ok("done"))
    ));
    drop((join, waker));
    assert_eq!(Arc::strong_count(&wakes), 1, "the join waker was dropped");
}

#[test]
fn a_new_join_waker_replaces_the_old_one() {
    let (_sched, task, mut join, _drops) = spawn(|_| Poll::Ready(()));
    let (first, first_waker) = counting();
    let (second, second_waker) = counting();
    assert!(poll_join(&mut join, &first_waker).is_pending());
    assert!(
        poll_join(&mut join, &first_waker).is_pending(),
        "same waker: kept"
    );
    assert!(poll_join(&mut join, &second_waker).is_pending());
    assert_eq!(Arc::strong_count(&first), 2, "the old waker was dropped");
    task.run();
    assert_eq!(first.0.load(Ordering::SeqCst), 0);
    assert_eq!(second.0.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_the_join_handle_detaches_and_the_lane_drops_the_output() {
    let (output_drops, output) = counter();
    let mut output = Some(output);
    let (_sched, task, mut join, drops) = spawn(move |_| Poll::Ready(output.take().unwrap()));
    let (wakes, waker) = counting();
    assert!(poll_join(&mut join, &waker).is_pending());
    drop(join);
    assert_eq!(Arc::strong_count(&wakes), 2, "the handle dropped its waker");
    task.run();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        output_drops.load(Ordering::SeqCst),
        1,
        "nobody joins: the lane drops it"
    );
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
}

#[test]
fn a_join_handle_dropped_after_completion_drops_the_output() {
    let (output_drops, output) = counter();
    let mut output = Some(output);
    let (_sched, task, join, _drops) = spawn(move |_| Poll::Ready(output.take().unwrap()));
    task.run();
    assert_eq!(
        output_drops.load(Ordering::SeqCst),
        0,
        "kept for the join handle"
    );
    drop(join);
    assert_eq!(output_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn abort_of_an_idle_task_cancels_it_without_polling() {
    let polls = Arc::new(AtomicUsize::new(0));
    let polls_in_task = polls.clone();
    let parked: Arc<Mutex<Option<Waker>>> = Arc::default();
    let parked_in_task = parked.clone();
    let (sched, task, mut join, drops) = spawn(move |cx| -> Poll<()> {
        polls_in_task.fetch_add(1, Ordering::SeqCst);
        *parked_in_task.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    });
    task.run();
    let (wakes, waker) = counting();
    assert!(poll_join(&mut join, &waker).is_pending());

    join.abort();
    assert_eq!(sched.len(), 1, "abort queues the idle task");
    join.abort();
    assert_eq!(sched.len(), 1, "once");
    sched.pop().unwrap().run();
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "a cancelled task is not polled again"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    let Poll::Ready(Err(error)) = poll_join(&mut join, &waker) else {
        panic!("the task is cancelled");
    };
    assert!(error.is_cancelled() && !error.is_panic());
    assert_eq!(error.id(), join.id());
    parked.lock().unwrap().take().unwrap().wake();
    assert_eq!(sched.len(), 0, "a cancelled task is complete");
}

#[test]
fn abort_during_a_poll_cancels_at_the_next_one() {
    let handle: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::default();
    let handle_in_task = handle.clone();
    let (sched, task, join, drops) = spawn(move |_| {
        handle_in_task.lock().unwrap().as_ref().unwrap().abort();
        Poll::Pending
    });
    *handle.lock().unwrap() = Some(join);
    task.run();
    assert_eq!(sched.len(), 1, "the abort requeues the running task");
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    sched.pop().unwrap().run();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut join = handle.lock().unwrap().take().unwrap();
    assert!(matches!(poll_join(&mut join, Waker::noop()), Poll::Ready(Err(e)) if e.is_cancelled()));
}

#[test]
fn a_poll_that_completes_wins_over_an_abort_it_raced_with() {
    let handle: Arc<Mutex<Option<JoinHandle<u8>>>> = Arc::default();
    let handle_in_task = handle.clone();
    let (sched, task, join, _drops) = spawn(move |_| {
        handle_in_task.lock().unwrap().as_ref().unwrap().abort();
        Poll::Ready(3)
    });
    *handle.lock().unwrap() = Some(join);
    task.run();
    assert_eq!(sched.len(), 0);
    let mut join = handle.lock().unwrap().take().unwrap();
    assert!(matches!(
        poll_join(&mut join, Waker::noop()),
        Poll::Ready(Ok(3))
    ));
    join.abort();
}

#[test]
fn dropping_the_queue_entry_cancels_the_task_for_the_joiner() {
    let (_sched, task, mut join, drops) = spawn(|_| Poll::Ready(1));
    let (wakes, waker) = counting();
    assert!(poll_join(&mut join, &waker).is_pending());
    drop(task);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    let Poll::Ready(Err(error)) = poll_join(&mut join, &waker) else {
        panic!("the task is cancelled");
    };
    assert!(error.is_cancelled());
    assert!(error.to_string().contains("was cancelled"), "{error}");
}

#[test]
fn a_panic_goes_to_the_joiner_instead_of_the_lane() {
    let (_sched, task, mut join, drops) = spawn(|_| -> Poll<()> { panic!("boom") });
    task.run();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let Poll::Ready(Err(error)) = poll_join(&mut join, Waker::noop()) else {
        panic!("the task panicked");
    };
    assert!(error.is_panic());
    assert!(error.to_string().contains("panicked: boom"), "{error}");
    assert_eq!(
        format!("{error:?}"),
        format!("JoinError::Panic({}, \"boom\")", error.id())
    );
    let payload = error.into_panic();
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
}

#[test]
fn a_panic_whose_join_handle_is_gone_is_resumed_on_the_lane() {
    let (_sched, task, join, drops) = spawn(|_| -> Poll<()> { panic!("unjoined") });
    drop(join);
    let result = panic::catch_unwind(AssertUnwindSafe(|| task.run()));
    assert!(result.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
#[should_panic(expected = "JoinHandle polled again after it returned")]
fn polling_a_join_handle_after_ready_panics_with_a_hint() {
    let (_sched, task, mut join, _drops) = spawn(|_| Poll::Ready(()));
    task.run();
    assert!(poll_join(&mut join, Waker::noop()).is_ready());
    let _ = poll_join(&mut join, Waker::noop());
}

#[test]
#[should_panic(expected = "check JoinError::is_panic first")]
fn into_panic_of_a_cancellation_panics_with_a_hint() {
    let (_sched, task, mut join, _drops) = spawn(|_| Poll::Ready(()));
    drop(task);
    let Poll::Ready(Err(error)) = poll_join(&mut join, Waker::noop()) else {
        unreachable!()
    };
    error.into_panic();
}

#[test]
fn the_scheduler_hears_of_each_completion_once() {
    let (sched, task, join, _drops) = spawn(|_| Poll::Ready(()));
    task.run();
    assert_eq!(sched.completed(), 1);
    drop(join);

    let (sched, task, _join, _drops) = spawn(|_| Poll::Ready(()));
    drop(task);
    assert_eq!(
        sched.completed(),
        1,
        "a cancellation completes the task too"
    );
}

#[test]
fn join_from_another_thread_sees_the_output() {
    let rounds = if cfg!(miri) { 3 } else { 100 };
    for i in 0..rounds {
        let (_sched, task, join, _drops) = spawn(move |_| Poll::Ready(i));
        let joiner = std::thread::spawn(move || {
            let (wakes, waker) = counting();
            let mut join = join;
            loop {
                if let Poll::Ready(output) = poll_join(&mut join, &waker) {
                    return output.unwrap();
                }
                while wakes.0.load(Ordering::SeqCst) == 0 {
                    std::thread::yield_now();
                }
                wakes.0.store(0, Ordering::SeqCst);
            }
        });
        task.run();
        assert_eq!(joiner.join().unwrap(), i);
    }
}
