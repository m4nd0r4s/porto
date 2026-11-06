//! Semaphore tests: fast path, multi-permit FIFO fairness, bounded release, cancellation,
//! total changes, node API, blocking acquire.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::{MAX_PERMITS, Semaphore, WaitNode};
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
fn the_semaphore_and_its_future_are_thread_safe() {
    assert_send_sync::<Semaphore>();
    assert_send_sync::<super::SemaphoreAcquire<'static>>();
}

#[test]
fn an_uncontended_acquire_completes_on_its_first_poll() {
    let semaphore = Semaphore::new(3);
    let (wakes_of, waker) = counting_waker();
    let Poll::Ready(permit) = poll_once(pin!(semaphore.acquire(2)), &waker) else {
        panic!("free permits are taken at once");
    };
    assert_eq!(permit.permits(), 2);
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(semaphore.acquired_permits(), 2);
    drop(permit);
    assert_eq!(semaphore.available_permits(), 3);
    assert_eq!(semaphore.acquired_permits(), 0);
    assert_eq!(wakes(&wakes_of), 0);
}

#[test]
fn a_large_request_at_the_front_blocks_smaller_ones_behind_it() {
    let semaphore = Semaphore::new(3);
    let held = semaphore.try_acquire(2).expect("free");
    let (big_wakes, big_waker) = counting_waker();
    let (small_wakes, small_waker) = counting_waker();
    let mut big = Box::pin(semaphore.acquire(3));
    let mut small = Box::pin(semaphore.acquire(1));
    assert!(poll_once(big.as_mut(), &big_waker).is_pending());
    // One permit is free, but the request ahead wants three: no barging.
    assert!(poll_once(small.as_mut(), &small_waker).is_pending());
    assert!(semaphore.try_acquire(1).is_none());
    assert_eq!(semaphore.available_permits(), 1);

    drop(held);
    assert_eq!(wakes(&big_wakes), 1);
    assert_eq!(wakes(&small_wakes), 0);
    let Poll::Ready(big_permit) = poll_once(big.as_mut(), &big_waker) else {
        panic!("the front request was granted");
    };
    drop(big_permit);
    assert_eq!(wakes(&small_wakes), 1);
    let Poll::Ready(small_permit) = poll_once(small.as_mut(), &small_waker) else {
        panic!("granted next");
    };
    assert_eq!(semaphore.available_permits(), 2);
    drop(small_permit);
}

#[test]
fn a_release_grants_every_front_waiter_it_can_in_order() {
    let semaphore = Semaphore::new(4);
    semaphore.try_acquire(4).expect("free").forget();
    let wakers: Vec<_> = (0..3).map(|_| counting_waker()).collect();
    let mut waits: Vec<_> = [1, 2, 2]
        .into_iter()
        .map(|n| Box::pin(semaphore.acquire(n)))
        .collect();
    for (wait, (_, waker)) in waits.iter_mut().zip(&wakers) {
        assert!(poll_once(wait.as_mut(), waker).is_pending());
    }
    assert!(semaphore.release(3));
    let woken: Vec<_> = wakers.iter().map(|(w, _)| wakes(w)).collect();
    assert_eq!(woken, [1, 1, 0], "1 + 2 fit in 3; the last 2 waits");
    assert_eq!(semaphore.available_permits(), 0);
    assert!(semaphore.release(1));
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(wakes(&wakers[2].0), 0);
    for wait in &mut waits[..2] {
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_ready());
    }
}

#[test]
fn releasing_more_than_was_acquired_is_refused() {
    let semaphore = Semaphore::new(2);
    assert!(!semaphore.release(1), "nothing acquired");
    semaphore.try_acquire(1).expect("free").forget();
    assert!(!semaphore.release(2));
    assert_eq!(semaphore.available_permits(), 1);
    assert!(semaphore.release(1));
    assert_eq!(semaphore.available_permits(), 2);
}

#[test]
fn a_waiter_leaving_the_front_lets_the_ones_behind_it_in() {
    let semaphore = Semaphore::new(2);
    let held = semaphore.try_acquire(1).expect("free");
    let mut big = Box::pin(semaphore.acquire(2));
    let (wakes_of, waker) = counting_waker();
    let mut small = Box::pin(semaphore.acquire(1));
    assert!(poll_once(big.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(small.as_mut(), &waker).is_pending());
    drop(big);
    assert_eq!(wakes(&wakes_of), 1);
    assert!(poll_once(small.as_mut(), &waker).is_ready());
    drop(held);
}

#[test]
fn a_cancelled_granted_waiter_passes_its_permits_on() {
    let semaphore = Semaphore::new(2);
    semaphore.try_acquire(2).expect("free").forget();
    let mut first = Box::pin(semaphore.acquire(2));
    let (wakes_of, waker) = counting_waker();
    let mut second = Box::pin(semaphore.acquire(2));
    assert!(poll_once(first.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(second.as_mut(), &waker).is_pending());
    assert!(semaphore.release(2));
    // `first` was granted but is dropped before it saw it.
    drop(first);
    assert_eq!(wakes(&wakes_of), 1);
    let Poll::Ready(permit) = poll_once(second.as_mut(), &waker) else {
        panic!("granted");
    };
    drop(permit);
    assert_eq!(semaphore.acquired_permits(), 0);
    assert_eq!(semaphore.available_permits(), 2);
}

#[test]
fn a_cancelled_granted_waiter_with_nobody_behind_returns_its_permits() {
    let semaphore = Semaphore::new(1);
    semaphore.try_acquire(1).expect("free").forget();
    let mut waiter = Box::pin(semaphore.acquire(1));
    assert!(poll_once(waiter.as_mut(), Waker::noop()).is_pending());
    assert!(semaphore.release(1));
    drop(waiter);
    assert_eq!(semaphore.available_permits(), 1);
    // The fast path works again: nobody is queued.
    assert!(semaphore.try_acquire(1).is_some());
}

#[test]
fn adding_permits_grants_waiters_and_removing_them_waits_for_releases() {
    let semaphore = Semaphore::new(1);
    let held = semaphore.try_acquire(1).expect("free");
    let (wakes_of, waker) = counting_waker();
    let mut waiter = Box::pin(semaphore.acquire(2));
    assert!(poll_once(waiter.as_mut(), &waker).is_pending());
    semaphore.add_permits(1);
    assert_eq!(semaphore.total_permits(), 2);
    assert_eq!(wakes(&wakes_of), 0, "one free, two wanted");
    drop(held);
    assert_eq!(wakes(&wakes_of), 1);
    let Poll::Ready(permit) = poll_once(waiter.as_mut(), &waker) else {
        panic!("granted");
    };
    // Two acquired; lowering the total to 1 takes one back on release.
    semaphore.remove_permits(1);
    assert_eq!(semaphore.total_permits(), 1);
    assert_eq!(semaphore.acquired_permits(), 2);
    assert!(semaphore.try_acquire(1).is_none());
    drop(permit);
    assert_eq!(semaphore.available_permits(), 1);
    assert_eq!(semaphore.total_permits(), 1);
    semaphore.add_permits(2);
    assert_eq!(semaphore.available_permits(), 3);
}

#[test]
fn a_debt_is_cancelled_by_added_permits() {
    let semaphore = Semaphore::new(2);
    semaphore.try_acquire(2).expect("free").forget();
    semaphore.remove_permits(2);
    assert_eq!(semaphore.total_permits(), 0);
    semaphore.add_permits(1);
    assert_eq!(semaphore.total_permits(), 1);
    assert_eq!(semaphore.available_permits(), 0);
    assert!(semaphore.release(2));
    assert_eq!(semaphore.available_permits(), 1);
}

#[test]
#[should_panic(expected = "cannot remove 3 permits")]
fn removing_more_than_the_total_panics() {
    Semaphore::new(2).remove_permits(3);
}

#[test]
#[should_panic(expected = "at most")]
fn more_than_max_permits_panics() {
    let _ = Semaphore::new(MAX_PERMITS + 1);
}

#[test]
fn the_node_api_sets_the_token_and_returns_the_permits() {
    let semaphore = Semaphore::new(3);
    semaphore.try_acquire(3).expect("free").forget();
    let node = Box::pin(WaitNode::new(0));
    let (wakes_of, waker) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    // SAFETY: the node is pinned, used with this semaphore only, and resolved below.
    assert!(unsafe { semaphore.poll_acquire_node(node.as_ref(), 2, &mut cx) }.is_pending());
    assert_eq!(node.token(), 2);
    assert!(semaphore.release(1));
    assert_eq!(wakes(&wakes_of), 0);
    assert!(semaphore.release(1));
    assert_eq!(wakes(&wakes_of), 1);
    // SAFETY: as above.
    assert!(unsafe { semaphore.poll_acquire_node(node.as_ref(), 2, &mut cx) }.is_ready());
    assert_eq!(semaphore.acquired_permits(), 3);
    // A cancel of a resolved node does nothing.
    // SAFETY: as above.
    unsafe { semaphore.cancel_acquire_node(node.as_ref()) };
    assert!(semaphore.release(3));
}

#[test]
fn acquires_run_on_the_runtime() {
    let rt = Runtime::new();
    let semaphore = Semaphore::new(2);
    rt.block_on(async {
        let a = semaphore.acquire(1).await;
        let b = semaphore.acquire(1).await;
        assert_eq!(semaphore.available_permits(), 0);
        drop((a, b));
    });
    assert_eq!(semaphore.available_permits(), 2);
}

#[test]
fn blocking_acquire_waits_for_a_release_on_another_thread() {
    let semaphore = Arc::new(Semaphore::new(1));
    semaphore.try_acquire(1).expect("free").forget();
    let releaser = {
        let semaphore = semaphore.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            assert!(semaphore.release(1));
        })
    };
    let permit = semaphore.blocking_acquire(1).expect("not on a lane");
    assert_eq!(permit.permits(), 1);
    drop(permit);
    releaser.join().unwrap();
}

#[test]
fn concurrent_acquires_never_exceed_the_permits() {
    let semaphore = Arc::new(Semaphore::new(3));
    let inside = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let (semaphore, inside) = (semaphore.clone(), inside.clone());
            thread::spawn(move || {
                for i in 0..200 {
                    let permit = semaphore.blocking_acquire(1 + i % 2).expect("off lane");
                    let now = inside.fetch_add(permit.permits(), Ordering::SeqCst);
                    assert!(now + permit.permits() <= 3);
                    inside.fetch_sub(permit.permits(), Ordering::SeqCst);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(semaphore.available_permits(), 3);
}
