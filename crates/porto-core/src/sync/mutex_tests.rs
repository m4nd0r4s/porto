//! Mutex tests: fast path, FIFO handoff, no barging, cancellation, node API, blocking lock.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::{Mutex, WaitNode};
use crate::Runtime;

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

fn counting_waker() -> (Arc<CountingWaker>, Waker) {
    let counter = Arc::new(CountingWaker::default());
    (counter.clone(), Waker::from(counter))
}

fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

/// Pending once (waking itself), then ready.
struct YieldOnce(bool);

impl Future for YieldOnce {
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

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn the_mutex_and_its_future_are_thread_safe() {
    assert_send_sync::<Mutex<Vec<u8>>>();
    assert_send_sync::<super::MutexLock<'static, Vec<u8>>>();
}

#[test]
fn an_uncontended_lock_completes_on_its_first_poll() {
    let mutex = Mutex::new(1);
    let (wakes, waker) = counting_waker();
    let Poll::Ready(mut guard) = poll_once(pin!(mutex.lock()), &waker) else {
        panic!("a free mutex locks at once");
    };
    *guard += 1;
    assert!(mutex.is_locked());
    assert!(mutex.try_lock().is_none());
    drop(guard);
    assert!(!mutex.is_locked());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    assert_eq!(mutex.into_inner(), 2);
}

#[test]
fn waiters_get_the_lock_in_arrival_order_and_nobody_barges() {
    let mutex = Mutex::new(Vec::new());
    let held = mutex.try_lock().expect("free");
    let mut waits: Vec<_> = (0..3).map(|_| Box::pin(mutex.lock())).collect();
    for wait in &mut waits {
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());
    }
    drop(held);
    // Handed to the first waiter: still locked, so a newcomer cannot take it.
    assert!(mutex.try_lock().is_none());
    for (i, wait) in waits.iter_mut().enumerate() {
        let Poll::Ready(mut guard) = poll_once(wait.as_mut(), Waker::noop()) else {
            panic!("waiter {i} was handed the lock");
        };
        guard.push(i);
    }
    drop(waits);
    assert_eq!(mutex.into_inner(), vec![0, 1, 2]);
}

#[test]
fn a_waiter_cancelled_after_the_handoff_passes_the_lock_on() {
    let mutex = Mutex::new(());
    let held = mutex.try_lock().expect("free");
    let mut a = Box::pin(mutex.lock());
    let mut b = Box::pin(mutex.lock());
    let (b_wakes, b_waker) = counting_waker();
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(b.as_mut(), &b_waker).is_pending());
    drop(held); // hands the lock to `a`
    drop(a); // which never observed it
    assert_eq!(b_wakes.0.load(Ordering::SeqCst), 1);
    assert!(poll_once(b.as_mut(), &b_waker).is_ready());
}

#[test]
fn the_only_waiter_cancelled_after_the_handoff_unlocks_the_mutex() {
    let mutex = Mutex::new(());
    let held = mutex.try_lock().expect("free");
    let mut a = Box::pin(mutex.lock());
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    drop(held);
    drop(a);
    assert!(!mutex.is_locked(), "the lock did not leak");
}

#[test]
fn a_queued_waiter_cancelled_before_the_handoff_leaves_the_queue() {
    let mutex = Mutex::new(());
    let held = mutex.try_lock().expect("free");
    let mut a = Box::pin(mutex.lock());
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    drop(a);
    drop(held);
    assert!(!mutex.is_locked());
}

#[test]
fn the_node_api_locks_without_a_guard_and_force_unlock_hands_off() {
    let mutex = Mutex::new(());
    let node = pin!(WaitNode::new(0));
    let other = pin!(WaitNode::new(0));
    let mut cx = Context::from_waker(Waker::noop());
    // SAFETY: each node is pinned, polled from this thread only, and cancelled or ready before
    // it is dropped.
    unsafe {
        assert!(mutex.poll_lock_node(node.as_ref(), &mut cx).is_ready());
        assert!(mutex.poll_lock_node(other.as_ref(), &mut cx).is_pending());
        assert!(mutex.force_unlock());
        assert!(mutex.is_locked(), "handed to the waiting node");
        assert!(mutex.poll_lock_node(other.as_ref(), &mut cx).is_ready());
        assert!(mutex.force_unlock());
        assert!(!mutex.force_unlock(), "already unlocked");
        mutex.cancel_lock_node(other.as_ref()); // idle: nothing to do
    }
    super::MutexGuard::leak(mutex.try_lock().expect("free"));
    // SAFETY: the leaked guard gave access to `()` only.
    assert!(unsafe { mutex.force_unlock() });
}

#[test]
fn tasks_serialize_on_the_mutex() {
    let rt = Runtime::new();
    let mutex = Arc::new(Mutex::new(0usize));
    for _ in 0..4 {
        let mutex = mutex.clone();
        rt.spawn(async move {
            for _ in 0..10 {
                let mut guard = mutex.lock().await;
                let seen = *guard;
                YieldOnce(false).await;
                *guard = seen + 1;
            }
        });
    }
    rt.run_until_idle();
    assert_eq!(*mutex.try_lock().expect("free"), 40);
}

#[test]
fn blocking_lock_from_a_plain_thread() {
    let mutex = Arc::new(Mutex::new(0));
    let held = mutex.try_lock().expect("free");
    let locker = {
        let mutex = mutex.clone();
        thread::spawn(move || {
            *mutex.blocking_lock().expect("not a lane thread") += 1;
        })
    };
    thread::sleep(Duration::from_millis(20));
    drop(held);
    locker.join().unwrap();
    assert_eq!(*mutex.try_lock().expect("free"), 1);
}

#[test]
fn blocking_lock_on_a_lane_is_refused() {
    let rt = Runtime::new();
    let mutex = Mutex::new(());
    let refused = rt.block_on(async { mutex.blocking_lock().map(drop) });
    let error = refused.expect_err("a lane thread must not block");
    assert_eq!(error.operation(), "Mutex.blocking_lock");
}
