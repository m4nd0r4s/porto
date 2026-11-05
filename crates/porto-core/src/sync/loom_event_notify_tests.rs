//! Loom models of Event and Notify. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sync`.
//!
//! The notifier runs on a spawned thread; the waiter polls and drops its future on the model's
//! main thread (the futures borrow the primitive, so they cannot move to a `'static` thread).

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use loom::sync::Arc;
use loom::thread;

use super::{Event, Notify};

/// Counts its wakes. Plain std atomics: it is only read after every thread joined.
#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl CountingWaker {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for CountingWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (std::sync::Arc<CountingWaker>, Waker) {
    let counter = std::sync::Arc::new(CountingWaker::default());
    (counter.clone(), Waker::from(counter))
}

fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

/// Whether a fresh `notified()` completes on its first poll, i.e. a permit is stored.
fn has_permit(notify: &Notify) -> bool {
    poll_once(pin!(notify.notified()), Waker::noop()).is_ready()
}

fn spawn_with<T: Send + Sync + 'static>(
    shared: &Arc<T>,
    f: impl FnOnce(&T) + Send + 'static,
) -> thread::JoinHandle<()> {
    let shared = shared.clone();
    thread::spawn(move || f(&shared))
}

/// `notify_one` races with the first poll of `notified()`: the waiter gets the notification
/// exactly once, either as the permit (ready at once) or as a wake, and no permit is left.
#[test]
fn notify_one_vs_first_poll_delivers_once() {
    loom::model(|| {
        let notify = Arc::new(Notify::new());
        let notifier = spawn_with(&notify, Notify::notify_one);
        let (wakes, waker) = counting_waker();
        let mut wait = pin!(notify.notified());
        let ready_at_once = poll_once(wait.as_mut(), &waker).is_ready();
        notifier.join().unwrap();

        if ready_at_once {
            assert_eq!(wakes.count(), 0);
        } else {
            assert_eq!(wakes.count(), 1);
            assert!(poll_once(wait.as_mut(), &waker).is_ready());
        }
        assert!(!has_permit(&notify), "the permit was consumed once");
    });
}

/// Two `notify_one`s race while one task waits: it is woken once, and one permit is stored.
#[test]
fn two_notify_ones_wake_the_waiter_and_store_one_permit() {
    loom::model(|| {
        let notify = Arc::new(Notify::new());
        let (wakes, waker) = counting_waker();
        let mut wait = pin!(notify.notified());
        assert!(poll_once(wait.as_mut(), &waker).is_pending());

        let first = spawn_with(&notify, Notify::notify_one);
        let second = spawn_with(&notify, Notify::notify_one);
        first.join().unwrap();
        second.join().unwrap();

        assert_eq!(wakes.count(), 1);
        assert!(poll_once(wait.as_mut(), &waker).is_ready());
        assert!(has_permit(&notify));
        assert!(!has_permit(&notify), "only one permit");
    });
}

/// `notify_one` races with the cancel of the first waiter: whichever wins, the second waiter
/// is woken exactly once and no permit is stored.
#[test]
fn notify_one_vs_cancel_passes_the_wake_on() {
    loom::model(|| {
        let notify = Arc::new(Notify::new());
        let mut a = Box::pin(notify.notified());
        let mut b = Box::pin(notify.notified());
        let (b_wakes, b_waker) = counting_waker();
        assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
        assert!(poll_once(b.as_mut(), &b_waker).is_pending());

        let notifier = spawn_with(&notify, Notify::notify_one);
        drop(a);
        notifier.join().unwrap();

        assert_eq!(b_wakes.count(), 1);
        assert!(poll_once(b.as_mut(), &b_waker).is_ready());
        assert!(!has_permit(&notify));
    });
}

/// `notify_one` races with the cancel of the only waiter: the notification ends up as the
/// stored permit either way.
#[test]
fn notify_one_vs_cancel_of_the_only_waiter_stores_the_permit() {
    loom::model(|| {
        let notify = Arc::new(Notify::new());
        let mut a = Box::pin(notify.notified());
        assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());

        let notifier = spawn_with(&notify, Notify::notify_one);
        drop(a);
        notifier.join().unwrap();

        assert!(has_permit(&notify));
    });
}

/// `notify_all` races with the cancel of one waiter: the other is woken once, and the
/// cancelled one's wake (if it got one) is dropped, not stored as a permit.
#[test]
fn notify_all_vs_cancel_stores_no_permit() {
    loom::model(|| {
        let notify = Arc::new(Notify::new());
        let mut a = Box::pin(notify.notified());
        let mut b = Box::pin(notify.notified());
        let (b_wakes, b_waker) = counting_waker();
        assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
        assert!(poll_once(b.as_mut(), &b_waker).is_pending());

        let notifier = spawn_with(&notify, Notify::notify_all);
        drop(a);
        notifier.join().unwrap();

        assert_eq!(b_wakes.count(), 1);
        assert!(poll_once(b.as_mut(), &b_waker).is_ready());
        assert!(!has_permit(&notify));
    });
}

/// `set` races with the first poll of `wait()`: the waiter completes at once or is woken
/// exactly once, and later waiters complete at once.
#[test]
fn event_set_vs_wait() {
    loom::model(|| {
        let event = Arc::new(Event::new());
        let setter = spawn_with(&event, Event::set);
        let (wakes, waker) = counting_waker();
        let mut wait = pin!(event.wait());
        let ready_at_once = poll_once(wait.as_mut(), &waker).is_ready();
        setter.join().unwrap();

        if ready_at_once {
            assert_eq!(wakes.count(), 0);
        } else {
            assert_eq!(wakes.count(), 1);
            assert!(poll_once(wait.as_mut(), &waker).is_ready());
        }
        assert!(event.is_set());
        assert!(poll_once(pin!(event.wait()), Waker::noop()).is_ready());
    });
}
