//! Loom models of the broadcast channel. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release broadcast`.
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

use crate::loom_support::cap_preemptions;

use super::broadcast::{self, RecvError, SendError, TryRecvError};

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

/// Receives until the channel ends: messages as `Ok`, lags as `Err(skipped)`.
fn receive_all(rx: &broadcast::Receiver<u32>) -> Vec<Result<u32, u64>> {
    let mut seen = Vec::new();
    loop {
        match block_on(rx.recv()) {
            Ok(message) => seen.push(Ok(*message)),
            Err(RecvError::Lagged(skipped)) => seen.push(Err(skipped)),
            Err(RecvError::Closed) => return seen,
        }
    }
}

/// A send races with a waiting receive on each of two receivers: both get the message, once,
/// and neither hangs. Three threads, so the search runs under a preemption bound of 2
/// (a lower `LOOM_MAX_PREEMPTIONS` lowers it): unbounded, it takes minutes.
#[test]
fn send_vs_two_waiting_receives_delivers_to_both() {
    let mut builder = Builder::new();
    cap_preemptions(&mut builder, 2);
    builder.check(|| {
        let (tx, rx) = broadcast::channel(2);
        let second = tx.subscribe().unwrap();
        let sender = thread::spawn(move || {
            tx.send(1u32).unwrap();
            tx
        });
        let other = thread::spawn(move || *block_on(second.recv()).unwrap());
        assert_eq!(*block_on(rx.recv()).unwrap(), 1);
        assert_eq!(other.join().unwrap(), 1);
        let _tx = sender.join().unwrap();
    });
}

/// Two sends into a ring of one race a receiver: it sees the messages in order, and the lag
/// count plus the messages received add up to what was sent, exactly.
#[test]
fn lag_counts_are_exact_under_a_racing_sender() {
    loom::model(|| {
        let (tx, rx) = broadcast::channel(1);
        let sender = thread::spawn(move || {
            tx.send(1u32).unwrap();
            tx.send(2u32).unwrap();
        });
        let seen = receive_all(&rx);
        sender.join().unwrap();
        let skipped: u64 = seen.iter().filter_map(|item| item.err()).sum();
        let received: Vec<u32> = seen.iter().filter_map(|item| item.ok()).collect();
        assert_eq!(skipped + received.len() as u64, 2, "{seen:?}");
        assert!(received.is_empty() || received.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            received.last(),
            Some(&2),
            "the newest message is always kept"
        );
    });
}

/// The last sender sends and closes while a receiver waits: it gets the message, then the end;
/// never the end first, never a hang.
#[test]
fn send_then_close_vs_waiting_receive_delivers_then_ends() {
    loom::model(|| {
        let (tx, rx) = broadcast::channel(2);
        let sender = thread::spawn(move || {
            tx.send(1u32).unwrap();
            drop(tx);
        });
        assert_eq!(receive_all(&rx), [Ok(1)]);
        sender.join().unwrap();
    });
}

/// A receive is cancelled (dropped) at every point of a race with a send: the message is not
/// lost; a later receive gets it.
#[test]
fn a_cancelled_recv_loses_nothing() {
    loom::model(|| {
        let (tx, rx) = broadcast::channel(2);
        let sender = thread::spawn(move || {
            tx.send(7u32).unwrap();
            tx
        });
        let waker = Waker::from(std::sync::Arc::new(CountingWaker::default()));
        {
            let mut recv = pin!(rx.recv());
            if let Poll::Ready(result) = recv.as_mut().poll(&mut Context::from_waker(&waker)) {
                assert_eq!(*result.unwrap(), 7);
                let _tx = sender.join().unwrap();
                return;
            }
        }
        let _tx = sender.join().unwrap();
        assert_eq!(*block_on(rx.recv()).unwrap(), 7);
    });
}

/// A receiver closes while a sender sends: the send lands or fails with its message back, and
/// every message is dropped exactly once (the counter proves it).
#[test]
fn receiver_close_vs_send_drops_the_message_exactly_once() {
    struct Counted(Arc<AtomicUsize>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    loom::model(|| {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = broadcast::channel(1);
        let sent = drops.clone();
        let sender = thread::spawn(move || {
            match tx.send(Counted(sent.clone())) {
                Ok(()) => {}
                Err(SendError::Broken(back)) => drop(back),
                Err(SendError::Closed(_)) => unreachable!("a fresh sender"),
            }
            // A second message overwrites the first when it landed.
            let _ = tx.send(Counted(sent));
            tx
        });
        rx.close();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Closed)));
        let tx = sender.join().unwrap();
        drop(rx);
        drop(tx);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    });
}
