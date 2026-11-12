//! Broadcast channel tests: fan-out, lag counts, closure, cancellation, blocking waits.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::broadcast::{self, RecvError, SendError, TryRecvError};
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

struct Tracked(Arc<AtomicUsize>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// The message of a receive that completed with one.
fn received<T>(poll: Poll<Result<Arc<T>, RecvError>>) -> Arc<T> {
    match poll {
        Poll::Ready(Ok(message)) => message,
        Poll::Ready(Err(_)) => panic!("the receive failed"),
        Poll::Pending => panic!("the receive is pending"),
    }
}

/// What a receiver sees until nothing is pending: messages, and lags as `Err(skipped)`.
fn drain<T: Copy>(rx: &broadcast::Receiver<T>) -> Vec<Result<T, u64>> {
    let mut seen = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(message) => seen.push(Ok(*message)),
            Err(TryRecvError::Lagged(skipped)) => seen.push(Err(skipped)),
            Err(TryRecvError::Empty | TryRecvError::Closed) => return seen,
        }
    }
}

#[test]
fn handles_are_thread_safe() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<broadcast::Sender<u32>>();
    assert_send_sync::<broadcast::Receiver<u32>>();
}

#[test]
#[should_panic(expected = "capacity must be between 1 and")]
fn a_zero_capacity_panics_and_says_what_to_pass() {
    let _ = broadcast::channel::<u8>(0);
}

#[test]
#[should_panic(expected = "capacity must be between 1 and")]
fn a_capacity_above_the_limit_panics() {
    let _ = broadcast::channel::<u8>(broadcast::MAX_CAPACITY + 1);
}

#[test]
fn every_receiver_gets_every_message_in_order() {
    let (tx, rx) = broadcast::channel(8);
    let second = rx.clone();
    let third = tx.subscribe().unwrap();
    for value in 1..=5 {
        tx.send(value).unwrap();
    }
    let all: Vec<_> = (1..=5).map(Ok).collect();
    assert_eq!(drain(&rx), all);
    assert_eq!(drain(&second), all);
    assert_eq!(drain(&third), all);
}

#[test]
fn a_subscriber_sees_only_later_messages_and_a_clone_starts_where_its_original_is() {
    let (tx, rx) = broadcast::channel(8);
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(*rx.try_recv().unwrap(), 1);
    let clone = rx.clone();
    let subscriber = tx.subscribe().unwrap();
    tx.send(3).unwrap();
    assert_eq!(drain(&clone), [Ok(2), Ok(3)]);
    assert_eq!(drain(&subscriber), [Ok(3)]);
    assert_eq!(drain(&rx), [Ok(2), Ok(3)]);
}

#[test]
fn a_slow_receiver_is_told_exactly_how_many_it_missed() {
    let (tx, rx) = broadcast::channel(3);
    for value in 1..=10 {
        tx.send(value).unwrap();
    }
    // Kept: 8, 9, 10. Missed: 1..=7.
    assert_eq!(rx.try_recv(), Err(TryRecvError::Lagged(7)));
    assert_eq!(drain(&rx), [Ok(8), Ok(9), Ok(10)]);
}

#[test]
fn lag_counts_are_exact_across_repeated_lags() {
    let (tx, rx) = broadcast::channel(2);
    let mut received = 0u64;
    let mut skipped = 0u64;
    let mut sent = 0u64;
    for burst in [1u64, 2, 3, 5, 2, 7, 1] {
        for _ in 0..burst {
            sent += 1;
            tx.send(sent).unwrap();
        }
        for item in drain(&rx) {
            match item {
                Ok(_) => received += 1,
                Err(missed) => skipped += missed,
            }
        }
    }
    assert_eq!(
        received + skipped,
        sent,
        "everything sent is received or counted"
    );
    assert!(skipped > 0);
}

#[test]
fn a_receiver_exactly_at_capacity_behind_is_not_lagged() {
    let (tx, rx) = broadcast::channel(3);
    for value in 1..=3 {
        tx.send(value).unwrap();
    }
    assert_eq!(drain(&rx), [Ok(1), Ok(2), Ok(3)]);
    tx.send(4).unwrap();
    tx.send(5).unwrap();
    tx.send(6).unwrap();
    assert_eq!(drain(&rx), [Ok(4), Ok(5), Ok(6)]);
    tx.send(7).unwrap();
    tx.send(8).unwrap();
    tx.send(9).unwrap();
    tx.send(10).unwrap();
    assert_eq!(drain(&rx), [Err(1), Ok(8), Ok(9), Ok(10)]);
}

#[test]
fn a_capacity_of_one_keeps_only_the_latest() {
    let (tx, rx) = broadcast::channel(1);
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(drain(&rx), [Err(1), Ok(2)]);
}

#[test]
fn recv_waits_and_each_send_wakes_every_waiter() {
    let (tx, rx) = broadcast::channel(4);
    let other = rx.clone();
    let (wakes_a, waker_a) = counting_waker();
    let (wakes_b, waker_b) = counting_waker();
    let mut first = pin!(rx.recv());
    let mut second = pin!(other.recv());
    assert!(poll_once(first.as_mut(), &waker_a).is_pending());
    assert!(poll_once(second.as_mut(), &waker_b).is_pending());
    tx.send("hi").unwrap();
    assert_eq!(wakes_a.0.load(Ordering::SeqCst), 1);
    assert_eq!(wakes_b.0.load(Ordering::SeqCst), 1);
    assert_eq!(*received(poll_once(first.as_mut(), &waker_a)), "hi");
    assert_eq!(*received(poll_once(second.as_mut(), &waker_b)), "hi");
}

#[test]
fn recv_reports_a_lag_then_continues() {
    let (tx, rx) = broadcast::channel(2);
    for value in 1..=5 {
        tx.send(value).unwrap();
    }
    let mut first = pin!(rx.recv());
    assert_eq!(
        poll_once(first.as_mut(), Waker::noop()),
        Poll::Ready(Err(RecvError::Lagged(3)))
    );
    let mut second = pin!(rx.recv());
    assert_eq!(*received(poll_once(second.as_mut(), Waker::noop())), 4);
}

#[test]
fn closing_all_senders_ends_every_receiver_after_it_drains() {
    let (tx, rx) = broadcast::channel(4);
    let other = rx.clone();
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    let second_sender = tx.clone();
    tx.close();
    assert_eq!(rx.try_recv().map(|m| *m), Ok(1), "another sender is open");
    drop(second_sender);
    assert_eq!(drain(&rx), [Ok(2)]);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    assert_eq!(drain(&other), [Ok(1), Ok(2)]);
    assert_eq!(other.try_recv(), Err(TryRecvError::Closed));
    let mut ended = pin!(other.recv());
    assert_eq!(
        poll_once(ended.as_mut(), Waker::noop()),
        Poll::Ready(Err(RecvError::Closed))
    );
}

#[test]
fn the_last_sender_closing_wakes_waiting_receivers() {
    let (tx, rx) = broadcast::channel::<u8>(4);
    let (wakes, waker) = counting_waker();
    let mut recv = pin!(rx.recv());
    assert!(poll_once(recv.as_mut(), &waker).is_pending());
    drop(tx);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll_once(recv.as_mut(), &waker),
        Poll::Ready(Err(RecvError::Closed))
    );
}

#[test]
fn send_with_no_receiver_is_broken_until_someone_subscribes() {
    let (tx, rx) = broadcast::channel(2);
    drop(rx);
    assert!(tx.is_closed());
    assert_eq!(tx.send(1), Err(SendError::Broken(1)));
    let rx = tx.subscribe().unwrap();
    assert!(!tx.is_closed());
    tx.send(2).unwrap();
    assert_eq!(drain(&rx), [Ok(2)], "the broken send stored nothing");
}

#[test]
fn a_closed_sender_cannot_send_or_subscribe_and_its_clones_are_closed() {
    let (tx, _rx) = broadcast::channel(2);
    tx.close();
    tx.close();
    assert_eq!(tx.send(1), Err(SendError::Closed(1)));
    assert!(tx.subscribe().is_none());
    assert!(tx.clone().is_handle_closed());
}

#[test]
fn a_closed_receiver_ends_its_waits_and_leaves_the_others() {
    let (tx, rx) = broadcast::channel(2);
    let other = rx.clone();
    let (wakes, waker) = counting_waker();
    let mut recv = pin!(rx.recv());
    assert!(poll_once(recv.as_mut(), &waker).is_pending());
    rx.close();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll_once(recv.as_mut(), &waker),
        Poll::Ready(Err(RecvError::Closed))
    );
    assert!(rx.is_handle_closed());
    assert!(rx.clone().is_handle_closed());
    assert!(!tx.is_closed());
    tx.send(1).unwrap();
    assert_eq!(drain(&other), [Ok(1)]);
}

#[test]
fn messages_are_dropped_when_overwritten_and_with_the_channel() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = broadcast::channel(2);
    for _ in 0..3 {
        tx.send(Tracked(drops.clone())).unwrap();
    }
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the oldest was overwritten"
    );
    let held = {
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Lagged(1))));
        rx.try_recv().unwrap()
    };
    tx.send(Tracked(drops.clone())).unwrap();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the overwritten message is still held by a receiver"
    );
    drop(held);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    drop(tx);
    drop(rx);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn a_returned_message_is_not_kept() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = broadcast::channel(2);
    drop(rx);
    let Err(SendError::Broken(back)) = tx.send(Tracked(drops.clone())) else {
        panic!("expected Broken");
    };
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(back);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_cancelled_recv_loses_nothing() {
    let (tx, rx) = broadcast::channel(4);
    {
        let (_wakes, waker) = counting_waker();
        let mut recv = pin!(rx.recv());
        assert!(poll_once(recv.as_mut(), &waker).is_pending());
        tx.send(7).unwrap();
        // Woken, then cancelled before it looked.
    }
    assert_eq!(drain(&rx), [Ok(7)]);
}

#[test]
fn a_huge_capacity_costs_nothing_until_it_is_used() {
    let (tx, rx) = broadcast::channel(broadcast::MAX_CAPACITY);
    tx.send(1).unwrap();
    assert_eq!(drain(&rx), [Ok(1)]);
    assert_eq!(tx.capacity(), broadcast::MAX_CAPACITY);
}

#[test]
fn tasks_receive_every_message_on_the_runtime() {
    let rt = Runtime::new();
    let (tx, rx) = broadcast::channel(64);
    let second = rx.clone();
    let totals = rt.block_on(async move {
        let a = crate::spawn(async move {
            let mut total = 0;
            while let Ok(message) = rx.recv().await {
                total += *message;
            }
            total
        });
        let b = crate::spawn(async move {
            let mut total = 0;
            while let Ok(message) = second.recv().await {
                total += *message;
            }
            total
        });
        for value in 1..=10 {
            tx.send(value).unwrap();
        }
        drop(tx);
        (a.await.unwrap(), b.await.unwrap())
    });
    assert_eq!(totals, (55, 55));
}

#[test]
fn blocking_recv_waits_for_a_send_from_another_thread() {
    let (tx, rx) = broadcast::channel(4);
    let sender = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send(5).unwrap();
        tx
    });
    assert_eq!(rx.blocking_recv().unwrap().map(|m| *m), Ok(5));
    drop(sender.join().unwrap());
    assert_eq!(
        rx.blocking_recv().unwrap().map(|m| *m),
        Err(RecvError::Closed)
    );
}

#[test]
fn blocking_recv_is_refused_on_a_lane() {
    let rt = Runtime::new();
    let refused = Arc::new(Mutex::new(None));
    let out = refused.clone();
    rt.block_on(async move {
        let (_tx, rx) = broadcast::channel::<u8>(1);
        *out.lock().unwrap() = Some(rx.blocking_recv().is_err());
    });
    assert_eq!(*refused.lock().unwrap(), Some(true));
}
