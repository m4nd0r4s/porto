//! Oneshot channel tests: send and receive, the dropped and closed sides, cancellation.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::oneshot::{self, RecvError, SendError, TryRecvError};
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

/// Counts how many of its values were dropped.
struct Tracked(Arc<AtomicUsize>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn handles_are_thread_safe() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<oneshot::Sender<u32>>();
    assert_send_sync::<oneshot::Receiver<u32>>();
}

#[test]
fn a_value_sent_first_is_received_without_waiting() {
    let (tx, rx) = oneshot::channel();
    tx.send(7).unwrap();
    let mut recv = pin!(rx.recv());
    assert_eq!(poll_once(recv.as_mut(), Waker::noop()), Poll::Ready(Ok(7)));
}

#[test]
fn recv_waits_and_the_send_wakes_it_once() {
    let (tx, rx) = oneshot::channel();
    let (wakes, waker) = counting_waker();
    let mut recv = pin!(rx.recv());
    assert!(poll_once(recv.as_mut(), &waker).is_pending());
    tx.send("hi").unwrap();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(poll_once(recv.as_mut(), &waker), Poll::Ready(Ok("hi")));
}

#[test]
fn try_recv_is_empty_until_sent_then_the_channel_has_ended() {
    let (tx, rx) = oneshot::channel();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    tx.send(1).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn a_dropped_sender_ends_the_channel_for_the_receiver() {
    let (tx, rx) = oneshot::channel::<u8>();
    let (wakes, waker) = counting_waker();
    let mut recv = pin!(rx.recv());
    assert!(poll_once(recv.as_mut(), &waker).is_pending());
    drop(tx);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll_once(recv.as_mut(), &waker),
        Poll::Ready(Err(RecvError))
    );
}

#[test]
fn a_closed_sender_ends_the_channel_and_cannot_send() {
    let (tx, rx) = oneshot::channel();
    tx.close();
    tx.close();
    assert!(tx.is_handle_closed());
    assert_eq!(tx.send(1), Err(SendError::Closed(1)));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn a_dropped_receiver_breaks_the_send_and_returns_the_value() {
    let (tx, rx) = oneshot::channel();
    drop(rx);
    assert!(tx.is_closed());
    assert_eq!(tx.send(5), Err(SendError::Broken(5)));
    assert_eq!(
        tx.send(6),
        Err(SendError::Closed(6)),
        "the sender is used up"
    );
}

#[test]
fn a_closed_receiver_ends_a_waiting_receive_and_fails_sends() {
    let (tx, rx) = oneshot::channel::<u8>();
    let (wakes, waker) = counting_waker();
    let mut recv = pin!(rx.recv());
    assert!(poll_once(recv.as_mut(), &waker).is_pending());
    rx.close();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll_once(recv.as_mut(), &waker),
        Poll::Ready(Err(RecvError))
    );
    assert!(rx.is_handle_closed());
    assert_eq!(tx.send(1), Err(SendError::Broken(1)));
}

#[test]
fn sending_twice_fails_with_closed_and_returns_the_value() {
    let (tx, _rx) = oneshot::channel();
    tx.send(1).unwrap();
    assert_eq!(tx.send(2), Err(SendError::Closed(2)));
    assert!(tx.is_handle_closed());
}

#[test]
fn a_value_not_received_is_dropped_when_the_receiver_closes_or_the_channel_drops() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = oneshot::channel();
    tx.send(Tracked(drops.clone())).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    rx.close();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "dropped when the receiver closed"
    );

    let (tx, rx) = oneshot::channel();
    tx.send(Tracked(drops.clone())).unwrap();
    drop(tx);
    drop(rx);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn a_returned_value_is_not_dropped_by_the_channel() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = oneshot::channel();
    drop(rx);
    let Err(SendError::Broken(back)) = tx.send(Tracked(drops.clone())) else {
        panic!("expected Broken");
    };
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(back);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn a_cancelled_receive_loses_no_value() {
    let (tx, rx) = oneshot::channel();
    {
        let (_wakes, waker) = counting_waker();
        let mut recv = pin!(rx.recv());
        assert!(poll_once(recv.as_mut(), &waker).is_pending());
        tx.send(9).unwrap();
        // Woken, then cancelled before it looked: the value is still in the slot.
    }
    assert_eq!(rx.try_recv(), Ok(9));
}

#[test]
fn two_receives_on_one_receiver_split_the_value_and_the_end() {
    let (tx, rx) = oneshot::channel();
    let (_wakes_a, waker_a) = counting_waker();
    let (_wakes_b, waker_b) = counting_waker();
    let mut first = pin!(rx.recv());
    let mut second = pin!(rx.recv());
    assert!(poll_once(first.as_mut(), &waker_a).is_pending());
    assert!(poll_once(second.as_mut(), &waker_b).is_pending());
    tx.send(3).unwrap();
    assert_eq!(poll_once(second.as_mut(), &waker_b), Poll::Ready(Ok(3)));
    assert_eq!(
        poll_once(first.as_mut(), &waker_a),
        Poll::Ready(Err(RecvError))
    );
}

#[test]
fn a_closed_handle_is_told_apart_from_the_other_side_going_away() {
    let (tx, rx) = oneshot::channel::<u8>();
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    assert!(
        !rx.is_handle_closed(),
        "the other side went away, not this handle"
    );
    rx.close();
    assert!(rx.is_handle_closed());
}

#[test]
fn tasks_send_and_receive_on_the_runtime() {
    let rt = Runtime::new();
    let (tx, rx) = oneshot::channel();
    let got = rt.block_on(async move {
        crate::spawn(async move {
            tx.send(String::from("done")).unwrap();
        });
        rx.recv().await.unwrap()
    });
    assert_eq!(got, "done");
}

#[test]
fn blocking_recv_waits_for_a_send_from_another_thread() {
    let (tx, rx) = oneshot::channel();
    let sender = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send(11).unwrap();
    });
    assert_eq!(rx.blocking_recv().unwrap(), Ok(11));
    sender.join().unwrap();
}

#[test]
fn blocking_recv_ends_when_the_sender_is_dropped() {
    let (tx, rx) = oneshot::channel::<u8>();
    let dropper = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        drop(tx);
    });
    assert_eq!(rx.blocking_recv().unwrap(), Err(RecvError));
    dropper.join().unwrap();
}

#[test]
fn blocking_recv_is_refused_on_a_lane() {
    let rt = Runtime::new();
    let refused = Arc::new(Mutex::new(None));
    let out = refused.clone();
    rt.block_on(async move {
        let (_tx, rx) = oneshot::channel::<u8>();
        *out.lock().unwrap() = Some(rx.blocking_recv().is_err());
    });
    assert_eq!(*refused.lock().unwrap(), Some(true));
}
