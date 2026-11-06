//! Condvar and Barrier tests.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use super::{Barrier, Condvar, Mutex, WaitNode};
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

fn wakes(counter: &CountingWaker) -> usize {
    counter.0.load(Ordering::SeqCst)
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn the_types_are_thread_safe() {
    assert_send_sync::<Condvar>();
    assert_send_sync::<Barrier>();
    assert_send_sync::<super::CondvarWait<'static, Vec<u8>>>();
    assert_send_sync::<super::BarrierWait<'static>>();
}

#[test]
fn a_wait_releases_the_mutex_and_takes_it_again_once_notified() {
    let (mutex, condvar) = (Mutex::new(0), Condvar::new());
    let guard = mutex.try_lock().expect("free");
    let (wakes_of, waker) = counting_waker();
    let mut wait = Box::pin(condvar.wait(guard));
    assert!(poll_once(wait.as_mut(), &waker).is_pending());
    assert!(!mutex.is_locked(), "released while waiting");
    assert_eq!(condvar.waiters(), 1);

    let mut other = mutex.try_lock().expect("free while the waiter waits");
    *other += 1;
    assert!(condvar.notify_one());
    assert_eq!(wakes(&wakes_of), 1);
    // Notified, but the mutex is held: the wait queues for it.
    assert!(poll_once(wait.as_mut(), &waker).is_pending());
    drop(other);
    let Poll::Ready(guard) = poll_once(wait.as_mut(), &waker) else {
        panic!("relocked");
    };
    assert_eq!(*guard, 1);
}

#[test]
fn notify_wakes_the_longest_waiting_first_and_stores_nothing() {
    let (mutex, condvar) = (Mutex::new(()), Condvar::new());
    assert!(!condvar.notify_one(), "nobody waits: lost");
    let wakers: Vec<_> = (0..3).map(|_| counting_waker()).collect();
    let mut waits = Vec::new();
    for (_, waker) in &wakers {
        let mut wait = Box::pin(condvar.wait(mutex.try_lock().expect("free")));
        assert!(poll_once(wait.as_mut(), waker).is_pending());
        waits.push(wait);
    }
    assert_eq!(condvar.notify(2), 2);
    let woken: Vec<_> = wakers.iter().map(|(w, _)| wakes(w)).collect();
    assert_eq!(woken, [1, 1, 0]);
    assert_eq!(condvar.notify_all(), 1);
    assert_eq!(wakes(&wakers[2].0), 1);
    assert_eq!(condvar.notify_all(), 0);
}

#[test]
fn a_cancelled_notified_waiter_passes_the_notification_on() {
    let (mutex, condvar) = (Mutex::new(()), Condvar::new());
    let mut first = Box::pin(condvar.wait(mutex.try_lock().expect("free")));
    assert!(poll_once(first.as_mut(), Waker::noop()).is_pending());
    let (wakes_of, waker) = counting_waker();
    let mut second = Box::pin(condvar.wait(mutex.try_lock().expect("free")));
    assert!(poll_once(second.as_mut(), &waker).is_pending());
    assert!(condvar.notify_one());
    drop(first);
    assert_eq!(wakes(&wakes_of), 1);
    assert!(poll_once(second.as_mut(), &waker).is_ready());
}

#[test]
fn the_node_api_queues_on_the_first_poll() {
    let condvar = Condvar::new();
    let node = Box::pin(WaitNode::new(0));
    let mut cx = Context::from_waker(Waker::noop());
    // SAFETY: the node is pinned, used with this condvar only, and resolved below.
    unsafe {
        assert!(condvar.poll_wait_node(node.as_ref(), &mut cx).is_pending());
        assert_eq!(condvar.waiters(), 1);
        condvar.cancel_wait_node(node.as_ref());
    }
    assert_eq!(condvar.waiters(), 0);
}

#[test]
fn a_condvar_hands_work_between_threads() {
    let state = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let rt = Runtime::new();
    let producer = {
        let state = state.clone();
        thread::spawn(move || {
            for _ in 0..100 {
                *state.0.blocking_lock().expect("off lane") += 1;
                state.1.notify_all();
            }
        })
    };
    rt.block_on(async {
        let mut guard = state.0.lock().await;
        while *guard < 100 {
            guard = state.1.wait(guard).await;
        }
    });
    producer.join().unwrap();
}

#[test]
fn the_last_arrival_leads_and_releases_everyone() {
    let barrier = Barrier::new(3);
    let (wakes_of, waker) = counting_waker();
    let mut a = Box::pin(barrier.wait());
    let mut b = Box::pin(barrier.wait());
    assert!(poll_once(a.as_mut(), &waker).is_pending());
    assert!(poll_once(b.as_mut(), &waker).is_pending());
    assert_eq!(barrier.waiting(), 2);
    let Poll::Ready(last) = poll_once(pin!(barrier.wait()), Waker::noop()) else {
        panic!("the third arrival completes the generation");
    };
    assert!(last.is_leader());
    assert_eq!(wakes(&wakes_of), 2);
    for wait in [&mut a, &mut b] {
        let Poll::Ready(result) = poll_once(wait.as_mut(), &waker) else {
            panic!("released");
        };
        assert!(!result.is_leader());
    }
    assert_eq!(barrier.waiting(), 0);
}

#[test]
fn the_barrier_is_reusable_with_one_leader_per_generation() {
    let rt = Runtime::new();
    let barrier = Arc::new(Barrier::new(4));
    let leaders = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..3)
        .map(|_| {
            let (barrier, leaders) = (barrier.clone(), leaders.clone());
            thread::spawn(move || {
                for _ in 0..50 {
                    if barrier.blocking_wait().expect("off lane").is_leader() {
                        leaders.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();
    rt.block_on(async {
        for _ in 0..50 {
            if barrier.wait().await.is_leader() {
                leaders.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(leaders.load(Ordering::SeqCst), 50);
}

#[test]
fn a_cancelled_waiter_leaves_its_generation() {
    let barrier = Barrier::new(2);
    let mut a = Box::pin(barrier.wait());
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    drop(a);
    assert_eq!(barrier.waiting(), 0);
    // Two more arrivals are needed, not one.
    let mut b = Box::pin(barrier.wait());
    assert!(poll_once(b.as_mut(), Waker::noop()).is_pending());
    assert!(barrier.try_lead());
    assert!(poll_once(b.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn try_lead_arrives_only_if_it_completes_the_generation() {
    let barrier = Barrier::new(2);
    assert!(!barrier.try_lead());
    assert_eq!(barrier.waiting(), 0);
    assert!(Barrier::new(1).try_lead());
}

#[test]
#[should_panic(expected = "at least one party")]
fn a_barrier_for_nobody_panics() {
    let _ = Barrier::new(0);
}
