//! Watch channel tests: versions, coalescing, closure, cancellation, blocking waits.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::watch::{self, RecvError, SendError, TryRecvError};
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

/// Lets the other tasks run once.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

struct Tracked(Arc<AtomicUsize>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn handles_are_thread_safe() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<watch::Sender<u32>>();
    assert_send_sync::<watch::Receiver<u32>>();
}

#[test]
fn the_initial_value_is_readable_and_counts_as_seen() {
    let (_tx, rx) = watch::channel(5);
    assert_eq!(*rx.borrow(), 5);
    assert!(!rx.has_changed());
    assert_eq!(rx.try_changed(), Err(TryRecvError::Empty));
    let (_wakes, waker) = counting_waker();
    assert!(poll_once(pin!(rx.changed()).as_mut(), &waker).is_pending());
}

#[test]
fn a_send_makes_a_new_version_that_changed_returns_for_once() {
    let (tx, rx) = watch::channel(0);
    tx.send(1).unwrap();
    assert!(rx.has_changed());
    assert_eq!(*rx.borrow(), 1);
    assert!(rx.has_changed(), "borrow does not mark the version seen");
    assert_eq!(rx.try_changed(), Ok(()));
    assert!(!rx.has_changed());
    assert_eq!(rx.try_changed(), Err(TryRecvError::Empty));
}

#[test]
fn several_sends_coalesce_into_one_changed() {
    let (tx, rx) = watch::channel(0);
    for value in 1..=5 {
        tx.send(value).unwrap();
    }
    assert_eq!(*rx.try_changed_ref().unwrap(), 5, "the latest value");
    assert_eq!(rx.try_changed(), Err(TryRecvError::Empty), "only once");
    tx.send(6).unwrap();
    assert_eq!(rx.try_changed(), Ok(()));
}

#[test]
fn borrow_and_update_marks_the_version_seen() {
    let (tx, rx) = watch::channel("a");
    tx.send("b").unwrap();
    assert_eq!(*rx.borrow_and_update(), "b");
    assert!(!rx.has_changed());
    assert_eq!(rx.try_changed(), Err(TryRecvError::Empty));
}

#[test]
fn changed_waits_and_each_send_wakes_it() {
    let (tx, rx) = watch::channel(0);
    let (wakes, waker) = counting_waker();
    let mut changed = pin!(rx.changed());
    assert!(poll_once(changed.as_mut(), &waker).is_pending());
    tx.send(1).unwrap();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(poll_once(changed.as_mut(), &waker), Poll::Ready(Ok(())));
}

#[test]
fn every_clone_sees_every_version_from_where_it_was_made() {
    let (tx, rx) = watch::channel(0);
    tx.send(1).unwrap();
    let late = rx.clone();
    assert!(late.has_changed(), "a clone has seen what its original had");
    assert_eq!(rx.try_changed(), Ok(()));
    let after = rx.clone();
    assert!(!after.has_changed());
    tx.send(2).unwrap();
    assert!(rx.has_changed() && late.has_changed() && after.has_changed());
    let fresh = tx.subscribe().unwrap();
    assert!(
        !fresh.has_changed(),
        "a subscriber has seen the current version"
    );
    assert_eq!(*fresh.borrow(), 2);
}

#[test]
fn closing_all_senders_delivers_the_unseen_change_then_the_end() {
    let (tx, rx) = watch::channel(0);
    tx.send(9).unwrap();
    drop(tx);
    assert_eq!(rx.try_changed(), Ok(()));
    assert_eq!(rx.try_changed(), Err(TryRecvError::Closed));
    assert_eq!(*rx.borrow(), 9, "the last value stays readable");
    let mut again = pin!(rx.changed());
    assert_eq!(
        poll_once(again.as_mut(), Waker::noop()),
        Poll::Ready(Err(RecvError))
    );
}

#[test]
fn closing_the_last_sender_wakes_a_waiting_changed() {
    let (tx, rx) = watch::channel(0);
    let (wakes, waker) = counting_waker();
    let mut changed = pin!(rx.changed());
    assert!(poll_once(changed.as_mut(), &waker).is_pending());
    let other = tx.clone();
    tx.close();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0, "another sender is open");
    drop(other);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll_once(changed.as_mut(), &waker),
        Poll::Ready(Err(RecvError))
    );
}

#[test]
fn a_clone_of_a_closed_sender_is_closed() {
    let (tx, rx) = watch::channel(0);
    tx.close();
    let clone = tx.clone();
    assert!(clone.is_handle_closed());
    assert_eq!(clone.send(1), Err(SendError::Closed(1)));
    assert_eq!(rx.try_changed(), Err(TryRecvError::Closed));
    assert!(tx.subscribe().is_none());
}

#[test]
fn send_with_no_receiver_is_broken_until_someone_subscribes() {
    let (tx, rx) = watch::channel(0);
    assert!(!tx.is_closed());
    drop(rx);
    assert!(tx.is_closed());
    assert_eq!(tx.send(1), Err(SendError::Broken(1)));
    let rx = tx.subscribe().unwrap();
    assert!(!tx.is_closed());
    assert_eq!(*rx.borrow(), 0, "the broken send stored nothing");
    tx.send(2).unwrap();
    assert_eq!(rx.try_changed(), Ok(()));
}

#[test]
fn a_closed_receiver_ends_its_waits_and_is_told_apart() {
    let (tx, rx) = watch::channel(0);
    let other = rx.clone();
    let (wakes, waker) = counting_waker();
    let mut changed = pin!(rx.changed());
    assert!(poll_once(changed.as_mut(), &waker).is_pending());
    rx.close();
    assert!(rx.is_handle_closed());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll_once(changed.as_mut(), &waker),
        Poll::Ready(Err(RecvError))
    );
    assert!(!tx.is_closed(), "another receiver is open");
    assert!(!other.is_handle_closed());
    assert!(
        rx.clone().is_handle_closed(),
        "a clone of a closed receiver is closed"
    );
}

#[test]
fn the_replaced_value_is_dropped_by_the_send_and_the_last_by_the_channel() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = watch::channel(Tracked(drops.clone()));
    tx.send(Tracked(drops.clone())).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1, "the initial value");
    drop(rx);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(tx);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn a_returned_value_is_not_stored_or_dropped() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = watch::channel(Tracked(drops.clone()));
    drop(rx);
    let Err(SendError::Broken(back)) = tx.send(Tracked(drops.clone())) else {
        panic!("expected Broken");
    };
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(back);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(tx);
    assert_eq!(drops.load(Ordering::SeqCst), 2, "the initial value");
}

#[test]
fn a_cancelled_changed_loses_no_change() {
    let (tx, rx) = watch::channel(0);
    {
        let (_wakes, waker) = counting_waker();
        let mut changed = pin!(rx.changed());
        assert!(poll_once(changed.as_mut(), &waker).is_pending());
        tx.send(1).unwrap();
        // Woken, then cancelled before it looked.
    }
    assert!(rx.has_changed());
    assert_eq!(rx.try_changed(), Ok(()));
}

#[test]
fn two_waits_on_one_receiver_split_the_versions() {
    let (tx, rx) = watch::channel(0);
    let (_a, waker_a) = counting_waker();
    let (_b, waker_b) = counting_waker();
    let mut first = pin!(rx.changed());
    let mut second = pin!(rx.changed());
    assert!(poll_once(first.as_mut(), &waker_a).is_pending());
    assert!(poll_once(second.as_mut(), &waker_b).is_pending());
    tx.send(1).unwrap();
    assert_eq!(poll_once(second.as_mut(), &waker_b), Poll::Ready(Ok(())));
    assert!(
        poll_once(first.as_mut(), &waker_a).is_pending(),
        "one version, one completion"
    );
    tx.send(2).unwrap();
    assert_eq!(poll_once(first.as_mut(), &waker_a), Poll::Ready(Ok(())));
}

#[test]
fn the_event_replacement_sets_and_clears() {
    let (tx, rx) = watch::channel(false);
    let rt = Runtime::new();
    let waited = rt.block_on(async move {
        crate::spawn(async move {
            tx.send(true).unwrap();
            tx.send(false).unwrap();
            tx.send(true).unwrap();
        });
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
        *rx.borrow()
    });
    assert!(waited);
}

#[test]
fn tasks_wait_for_changes_on_the_runtime() {
    let rt = Runtime::new();
    let (tx, rx) = watch::channel(0);
    let total = rt.block_on(async move {
        let watcher = crate::spawn(async move {
            let mut last = 0;
            while rx.changed().await.is_ok() {
                last = *rx.borrow_and_update();
            }
            last
        });
        for value in 1..=3 {
            tx.send(value).unwrap();
            yield_now().await;
        }
        drop(tx);
        watcher.await.unwrap()
    });
    assert_eq!(total, 3);
}

#[test]
fn blocking_changed_waits_for_a_send_from_another_thread() {
    let (tx, rx) = watch::channel(0);
    let sender = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send(1).unwrap();
        tx
    });
    assert_eq!(rx.blocking_changed().unwrap(), Ok(()));
    assert_eq!(*rx.borrow(), 1);
    drop(sender.join().unwrap());
    assert_eq!(rx.blocking_changed().unwrap(), Err(RecvError));
}

#[test]
fn blocking_changed_is_refused_on_a_lane() {
    let rt = Runtime::new();
    let refused = Arc::new(Mutex::new(None));
    let out = refused.clone();
    rt.block_on(async move {
        let (_tx, rx) = watch::channel(0);
        *out.lock().unwrap() = Some(rx.blocking_changed().is_err());
    });
    assert_eq!(*refused.lock().unwrap(), Some(true));
}
