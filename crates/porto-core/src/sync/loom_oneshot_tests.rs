//! Loom models of the oneshot channel. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release oneshot`.
//!
//! Waiting goes through `loom::future::block_on`, so a lost wake-up shows up as a deadlock
//! that loom reports. The waiter polls (and drops) its futures on the model's main thread:
//! they borrow the receiver.

use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use loom::future::block_on;
use loom::model::Builder;
use loom::sync::Arc;
use loom::thread;

use super::oneshot::{self, RecvError, SendError, TryRecvError};

/// Counts its wakes. Plain std atomics: it is only read after every thread joined.
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

/// A send races with a receive that waits: the receiver gets the value exactly once, and
/// never hangs.
#[test]
fn send_vs_waiting_receive_delivers_once() {
    loom::model(|| {
        let (tx, rx) = oneshot::channel();
        let sender = thread::spawn(move || tx.send(1u32).unwrap());
        assert_eq!(block_on(rx.recv()), Ok(1));
        sender.join().unwrap();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    });
}

/// A dropped sender races with a waiting receive: it ends, it does not hang.
#[test]
fn dropped_sender_vs_waiting_receive_ends() {
    loom::model(|| {
        let (tx, rx) = oneshot::channel::<u32>();
        let dropper = thread::spawn(move || drop(tx));
        assert_eq!(block_on(rx.recv()), Err(RecvError));
        dropper.join().unwrap();
    });
}

/// A send races with the receiver closing: either the send fails with its value back, or it
/// lands and the close drops it. No value is lost or dropped twice (the counter proves it).
#[test]
fn send_vs_receiver_close_drops_the_value_exactly_once() {
    struct Counted(Arc<AtomicUsize>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    loom::model(|| {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = oneshot::channel();
        let sent = drops.clone();
        let sender = thread::spawn(move || match tx.send(Counted(sent)) {
            Ok(()) => true,
            Err(SendError::Broken(back)) => {
                drop(back);
                false
            }
            Err(SendError::Closed(_)) => unreachable!("a fresh sender"),
        });
        rx.close();
        let _ = sender.join().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

/// A receive is cancelled (dropped) at every point of a race with the send: the value is
/// never lost; a later receive gets it.
#[test]
fn a_cancelled_receive_loses_nothing() {
    loom::model(|| {
        let (tx, rx) = oneshot::channel();
        let sender = thread::spawn(move || tx.send(7u32).unwrap());
        let wake = std::sync::Arc::new(CountingWaker::default());
        let waker = Waker::from(wake);
        {
            let mut recv = pin!(rx.recv());
            let first = recv.as_mut().poll(&mut Context::from_waker(&waker));
            if let Poll::Ready(value) = first {
                // Won the race outright: it is the value.
                assert_eq!(value, Ok(7));
                sender.join().unwrap();
                return;
            }
            // Cancelled here, possibly after the send woke it.
        }
        sender.join().unwrap();
        assert_eq!(block_on(rx.recv()), Ok(7));
    });
}

/// Two receives on one receiver race for the value: exactly one gets it, the other ends. Three
/// threads, so the search runs under a preemption bound of 2 (`LOOM_MAX_PREEMPTIONS`
/// overrides it): unbounded, it takes minutes.
#[test]
fn two_receives_take_the_value_once() {
    let mut builder = Builder::new();
    builder.preemption_bound.get_or_insert(2);
    builder.check(|| {
        let (tx, rx) = oneshot::channel();
        let rx = Arc::new(rx);
        let other = rx.clone();
        let taker = thread::spawn(move || block_on(other.recv()));
        let sender = thread::spawn(move || tx.send(5u32).unwrap());
        let mine = block_on(rx.recv());
        let theirs = taker.join().unwrap();
        sender.join().unwrap();
        let results = [mine, theirs];
        assert_eq!(results.iter().filter(|r| **r == Ok(5)).count(), 1);
        assert_eq!(results.iter().filter(|r| **r == Err(RecvError)).count(), 1);
    });
}
