//! Event and Notify tests: permits, broadcast scope, cancellation, tasks, blocking waits.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::{Event, Notify};
use crate::Runtime;

/// Counts its wakes.
#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl CountingWaker {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (Arc<CountingWaker>, Waker) {
    let counter = Arc::new(CountingWaker::default());
    (counter.clone(), Waker::from(counter))
}

fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn primitives_and_futures_are_thread_safe() {
    assert_send_sync::<Event>();
    assert_send_sync::<Notify>();
    assert_send_sync::<super::EventWait<'static>>();
    assert_send_sync::<super::Notified<'static>>();
}

// --- Event ---

#[test]
fn event_starts_unset() {
    let event = Event::new();
    assert!(!event.is_set());
    let mut wait = pin!(event.wait());
    assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());
}

#[test]
fn event_wait_after_set_completes_without_suspending() {
    let event = Event::new();
    event.set();
    assert!(event.is_set());
    let (wakes, waker) = counting_waker();
    let mut wait = pin!(event.wait());
    assert!(poll_once(wait.as_mut(), &waker).is_ready());
    assert_eq!(wakes.count(), 0, "it never registered a waker");
}

#[test]
fn event_set_wakes_every_waiter_once() {
    let event = Event::new();
    let mut waits: Vec<_> = (0..3).map(|_| Box::pin(event.wait())).collect();
    let wakers: Vec<_> = (0..3).map(|_| counting_waker()).collect();
    for (wait, (_, waker)) in waits.iter_mut().zip(&wakers) {
        assert!(poll_once(wait.as_mut(), waker).is_pending());
    }
    event.set();
    event.set(); // idempotent: no second wake
    for (wait, (wakes, waker)) in waits.iter_mut().zip(&wakers) {
        assert_eq!(wakes.count(), 1);
        assert!(poll_once(wait.as_mut(), waker).is_ready());
    }
    assert!(event.is_set(), "an event cannot be cleared");
}

#[test]
fn event_cancelled_waiter_does_not_disturb_the_others() {
    let event = Event::new();
    let mut a = Box::pin(event.wait());
    let mut b = Box::pin(event.wait());
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(b.as_mut(), Waker::noop()).is_pending());
    drop(a);
    event.set();
    assert!(poll_once(b.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn event_waiter_cancelled_after_set_passes_nothing_on() {
    let event = Event::new();
    let mut a = Box::pin(event.wait());
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    event.set();
    drop(a); // woken, but never observed it
    let mut late = pin!(event.wait());
    assert!(poll_once(late.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn event_releases_tasks() {
    let rt = Runtime::new();
    let event = Arc::new(Event::new());
    let released = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let event = event.clone();
        let released = released.clone();
        rt.spawn(async move {
            event.wait().await;
            released.fetch_add(1, Ordering::SeqCst);
        });
    }
    rt.run_until_idle();
    assert_eq!(released.load(Ordering::SeqCst), 0);
    event.set();
    rt.run_until_idle();
    assert_eq!(released.load(Ordering::SeqCst), 3);
}

#[test]
fn event_blocking_wait_from_a_plain_thread() {
    let event = Arc::new(Event::new());
    let waiter = {
        let event = event.clone();
        thread::spawn(move || event.blocking_wait())
    };
    thread::sleep(Duration::from_millis(20));
    event.set();
    assert_eq!(waiter.join().unwrap(), Ok(()));
}

#[test]
fn event_blocking_wait_on_a_lane_is_an_error() {
    let rt = Runtime::new();
    let event = Event::new();
    event.set();
    let error = rt.block_on(async { event.blocking_wait() }).unwrap_err();
    assert_eq!(error.operation(), "Event.blocking_wait");
    assert!(error.to_string().contains("Await the async form instead"));
}

// --- Notify ---

#[test]
fn notify_one_stores_one_permit_consumed_once() {
    let notify = Notify::new();
    notify.notify_one();
    notify.notify_one(); // still one permit
    let mut first = pin!(notify.notified());
    assert!(poll_once(first.as_mut(), Waker::noop()).is_ready());
    let mut second = pin!(notify.notified());
    assert!(
        poll_once(second.as_mut(), Waker::noop()).is_pending(),
        "the permit was consumed"
    );
}

#[test]
fn notify_one_wakes_the_longest_waiter_only() {
    let notify = Notify::new();
    let mut a = Box::pin(notify.notified());
    let mut b = Box::pin(notify.notified());
    let (a_wakes, a_waker) = counting_waker();
    let (b_wakes, b_waker) = counting_waker();
    assert!(poll_once(a.as_mut(), &a_waker).is_pending());
    assert!(poll_once(b.as_mut(), &b_waker).is_pending());
    notify.notify_one();
    assert_eq!((a_wakes.count(), b_wakes.count()), (1, 0));
    assert!(poll_once(a.as_mut(), &a_waker).is_ready());
    assert!(poll_once(b.as_mut(), &b_waker).is_pending());
    // The wake went to a waiter, so no permit was stored.
    let mut late = pin!(notify.notified());
    assert!(poll_once(late.as_mut(), Waker::noop()).is_pending());
}

#[test]
fn notify_all_wakes_only_current_waiters() {
    let notify = Notify::new();
    let mut waits: Vec<_> = (0..3).map(|_| Box::pin(notify.notified())).collect();
    for wait in &mut waits {
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());
    }
    // Created before notify_all but not polled yet: not a current waiter.
    let mut unpolled = pin!(notify.notified());
    notify.notify_all();
    for wait in &mut waits {
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_ready());
    }
    assert!(poll_once(unpolled.as_mut(), Waker::noop()).is_pending());
    let mut late = pin!(notify.notified());
    assert!(
        poll_once(late.as_mut(), Waker::noop()).is_pending(),
        "notify_all stores no permit"
    );
}

#[test]
fn notify_all_with_nobody_waiting_stores_nothing() {
    let notify = Notify::new();
    notify.notify_all();
    let mut wait = pin!(notify.notified());
    assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());
}

#[test]
fn notify_all_leaves_a_stored_permit() {
    let notify = Notify::new();
    notify.notify_one();
    notify.notify_all();
    let mut wait = pin!(notify.notified());
    assert!(poll_once(wait.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn notify_one_wake_of_a_cancelled_waiter_goes_to_the_next() {
    let notify = Notify::new();
    let mut a = Box::pin(notify.notified());
    let mut b = Box::pin(notify.notified());
    let (b_wakes, b_waker) = counting_waker();
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(b.as_mut(), &b_waker).is_pending());
    notify.notify_one(); // wakes a ...
    drop(a); // ... which is cancelled before it observes the wake
    assert_eq!(b_wakes.count(), 1);
    assert!(poll_once(b.as_mut(), &b_waker).is_ready());
}

#[test]
fn notify_one_wake_of_the_only_waiter_cancelled_becomes_a_permit() {
    let notify = Notify::new();
    let mut a = Box::pin(notify.notified());
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    notify.notify_one();
    drop(a);
    let mut late = pin!(notify.notified());
    assert!(poll_once(late.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn notify_all_wake_of_a_cancelled_waiter_is_dropped() {
    let notify = Notify::new();
    let mut a = Box::pin(notify.notified());
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    notify.notify_all();
    drop(a);
    let mut late = pin!(notify.notified());
    assert!(
        poll_once(late.as_mut(), Waker::noop()).is_pending(),
        "a broadcast wake does not turn into a permit"
    );
}

#[test]
fn notify_wakes_tasks_in_arrival_order() {
    let rt = Runtime::new();
    let notify = Arc::new(Notify::new());
    let order = Arc::new(Mutex::new(Vec::new()));
    for i in 0..3 {
        let notify = notify.clone();
        let order = order.clone();
        rt.spawn(async move {
            notify.notified().await;
            order.lock().unwrap().push(i);
        });
    }
    rt.run_until_idle();
    for expected in 1..=3 {
        notify.notify_one();
        rt.run_until_idle();
        assert_eq!(order.lock().unwrap().len(), expected);
    }
    assert_eq!(*order.lock().unwrap(), [0, 1, 2]);
}

#[test]
fn notify_blocking_notified_from_a_plain_thread() {
    let notify = Arc::new(Notify::new());
    let waiter = {
        let notify = notify.clone();
        thread::spawn(move || notify.blocking_notified())
    };
    thread::sleep(Duration::from_millis(20));
    // Whether or not the waiter is queued yet, the wake reaches it (directly or as a permit).
    notify.notify_one();
    assert_eq!(waiter.join().unwrap(), Ok(()));
}

#[test]
fn notify_blocking_notified_on_a_lane_is_an_error() {
    let rt = Runtime::new();
    let notify = Notify::new();
    let error = rt
        .block_on(async { notify.blocking_notified() })
        .unwrap_err();
    assert_eq!(error.operation(), "Notify.blocking_notified");
}
