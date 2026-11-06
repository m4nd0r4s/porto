//! Loom models of Condvar notification and Barrier arrival. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sync`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use loom::sync::Arc;
use loom::thread;

use super::{Barrier, Condvar, Mutex, MutexGuard, WaitNode};

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

fn counting_waker() -> (std::sync::Arc<CountingWaker>, Waker) {
    let counter = std::sync::Arc::new(CountingWaker::default());
    (counter.clone(), Waker::from(counter))
}

fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

/// A waiter queues and releases the mutex while another thread takes the mutex and notifies:
/// queuing before the release means the notification always reaches it.
#[test]
fn condvar_notify_after_the_release_is_never_lost() {
    loom::model(|| {
        let state = Arc::new((Mutex::new(false), Condvar::new()));
        let guard = state.0.try_lock().expect("free");
        let (wakes, waker) = counting_waker();
        let mut wait = Box::pin(state.1.wait(guard));
        assert!(poll_once(wait.as_mut(), &waker).is_pending());

        let notifier = {
            let state = state.clone();
            thread::spawn(move || {
                // The waiter released the mutex before its first poll returned.
                let mut guard = state.0.try_lock().expect("released by the waiter");
                *guard = true;
                drop(guard);
                assert!(state.1.notify_one());
            })
        };
        notifier.join().unwrap();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        let Poll::Ready(guard) = poll_once(wait.as_mut(), &waker) else {
            panic!("notified and the mutex is free");
        };
        assert!(*guard);
    });
}

/// `notify_one` races the cancel of the waiter it picks: the notification goes to the waiter
/// behind it instead.
#[test]
fn condvar_cancel_of_a_notified_waiter_passes_the_wake_on() {
    loom::model(|| {
        let condvar = Arc::new(Condvar::new());
        let a = Arc::new(Box::pin(WaitNode::new(0)));
        let mut noop = Context::from_waker(Waker::noop());
        // SAFETY: `a` is pinned, used with this condvar only, and cancelled below.
        assert!(unsafe { condvar.poll_wait_node(a.as_ref().as_ref(), &mut noop) }.is_pending());
        let b = Box::pin(WaitNode::new(0));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: as above, resolved below.
        assert!(unsafe { condvar.poll_wait_node(b.as_ref(), &mut cx) }.is_pending());

        let notifier = {
            let condvar = condvar.clone();
            thread::spawn(move || assert!(condvar.notify_one()))
        };
        // SAFETY: nothing else uses `a` concurrently.
        unsafe { condvar.cancel_wait_node(a.as_ref().as_ref()) };
        notifier.join().unwrap();

        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "the notification was lost"
        );
        // SAFETY: as above.
        assert!(unsafe { condvar.poll_wait_node(b.as_ref(), &mut cx) }.is_ready());
    });
}

/// A queued waiter is cancelled while the last party arrives on another thread: either the
/// generation completes with the waiter (released) or without it (the arrival waits), never
/// both or neither.
#[test]
fn barrier_cancel_vs_the_last_arrival_counts_each_party_once() {
    loom::model(|| {
        let barrier = Arc::new(Barrier::new(2));
        let a = Arc::new(Box::pin(WaitNode::new(0)));
        let mut noop = Context::from_waker(Waker::noop());
        // SAFETY: `a` is pinned, used with this barrier only, and cancelled below.
        assert!(unsafe { barrier.poll_wait_node(a.as_ref().as_ref(), &mut noop) }.is_pending());

        let arrival = {
            let barrier = barrier.clone();
            thread::spawn(move || {
                let node = Box::pin(WaitNode::new(0));
                let mut cx = Context::from_waker(Waker::noop());
                // SAFETY: the node is pinned, used with this barrier only, and cancelled
                // before it is dropped.
                let led = unsafe { barrier.poll_wait_node(node.as_ref(), &mut cx) }.is_ready();
                // SAFETY: as above.
                unsafe { barrier.cancel_wait_node(node.as_ref()) };
                led
            })
        };
        // SAFETY: nothing else uses `a` concurrently.
        unsafe { barrier.cancel_wait_node(a.as_ref().as_ref()) };
        let led = arrival.join().unwrap();

        // Led: `a` was still queued and the generation completed, releasing it. Otherwise `a`
        // left first, so the arrival waited (and then left too). Never a stale count.
        assert_eq!(barrier.waiting(), 0);
        assert!(!a.is_pending());
        assert!(!barrier.try_lead(), "a fresh generation needs two arrivals");
        let _ = led;
    });
}

/// A guard is dropped on another thread while the condvar wait relocks: the mutex ends up held
/// by the waiter.
#[test]
fn condvar_relock_waits_for_the_holder() {
    loom::model(|| {
        let state = Arc::new((Mutex::new(()), Condvar::new()));
        let guard = state.0.try_lock().expect("free");
        let mut wait = Box::pin(state.1.wait(guard));
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());
        MutexGuard::leak(state.0.try_lock().expect("released by the waiter"));
        assert!(state.1.notify_one());
        let (wakes, waker) = counting_waker();
        let relocked = poll_once(wait.as_mut(), &waker);
        assert!(relocked.is_pending(), "the mutex is held");
        let unlocker = {
            let state = state.clone();
            // SAFETY: the leaked guard gave access to `()` only.
            thread::spawn(move || assert!(unsafe { state.0.force_unlock() }))
        };
        unlocker.join().unwrap();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        assert!(poll_once(wait.as_mut(), &waker).is_ready());
    });
}
