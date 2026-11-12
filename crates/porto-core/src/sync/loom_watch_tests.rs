//! Loom models of the watch channel. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release watch`.
//!
//! Waiting goes through `loom::future::block_on`, so a lost wake-up shows up as a deadlock
//! that loom reports. The waiter polls (and drops) its futures on the model's main thread:
//! they borrow the receiver.

use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use loom::future::block_on;
use loom::sync::Arc;
use loom::thread;

use super::watch::{self, RecvError, TryRecvError};

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

/// A send races with a waiting `changed`: it completes, never hangs, and the value read after
/// it is the sent one.
#[test]
fn send_vs_waiting_changed_delivers() {
    loom::model(|| {
        let (tx, rx) = watch::channel(0u32);
        let sender = thread::spawn(move || {
            tx.send(1).unwrap();
            tx
        });
        assert_eq!(block_on(rx.changed()), Ok(()));
        assert_eq!(*rx.borrow(), 1);
        let _tx = sender.join().unwrap();
        assert!(!rx.has_changed());
    });
}

/// The last sender sends and closes while a receiver waits: the receiver gets the change, then
/// the end; never the end first, never a hang.
#[test]
fn send_then_close_vs_waiting_changed_delivers_then_ends() {
    loom::model(|| {
        let (tx, rx) = watch::channel(0u32);
        let sender = thread::spawn(move || {
            tx.send(1).unwrap();
            drop(tx);
        });
        assert_eq!(block_on(rx.changed()), Ok(()));
        assert_eq!(block_on(rx.changed()), Err(RecvError));
        assert_eq!(*rx.borrow(), 1);
        sender.join().unwrap();
    });
}

/// A `changed` wait is cancelled (dropped) at every point of a race with a send: the change is
/// not lost; a later `changed` returns it.
#[test]
fn a_cancelled_changed_loses_no_change() {
    loom::model(|| {
        let (tx, rx) = watch::channel(0u32);
        let sender = thread::spawn(move || {
            tx.send(1).unwrap();
            tx
        });
        let waker = Waker::from(std::sync::Arc::new(CountingWaker::default()));
        {
            let mut changed = pin!(rx.changed());
            if let Poll::Ready(result) = changed.as_mut().poll(&mut Context::from_waker(&waker)) {
                assert_eq!(result, Ok(()));
                let _tx = sender.join().unwrap();
                return;
            }
        }
        let _tx = sender.join().unwrap();
        assert_eq!(block_on(rx.changed()), Ok(()));
    });
}

/// Two sends race with `changed_ref`: the value it returns and the version it marks seen are the
/// same one, so a send it did not return is still reported as a change, and nothing is
/// reported twice.
#[test]
fn changed_ref_marks_exactly_the_version_it_returns() {
    loom::model(|| {
        let (tx, rx) = watch::channel(0u32);
        let sender = thread::spawn(move || {
            tx.send(1).unwrap();
            tx.send(2).unwrap();
            tx
        });
        let seen = *block_on(rx.changed_ref()).unwrap();
        let _tx = sender.join().unwrap();
        // Every send is done: the change report is for the versions after `seen` only.
        assert_eq!(rx.has_changed(), seen < 2);
        if seen < 2 {
            assert_eq!(*rx.try_changed_ref().unwrap(), 2);
        }
        assert_eq!(rx.try_changed().unwrap_err(), TryRecvError::Empty);
    });
}

/// One send, two concurrent `try_changed` on one receiver: exactly one reports it.
#[test]
fn one_version_completes_one_changed() {
    loom::model(|| {
        let (tx, rx) = watch::channel(0u32);
        let rx = Arc::new(rx);
        let other = rx.clone();
        let sender = thread::spawn(move || {
            tx.send(1).unwrap();
            tx
        });
        let taker = thread::spawn(move || other.try_changed().is_ok());
        let mine = rx.try_changed().is_ok();
        let theirs = taker.join().unwrap();
        let _tx = sender.join().unwrap();
        // Whatever missed the send finds it afterwards: in total exactly once.
        let late = rx.try_changed().is_ok();
        assert_eq!(
            usize::from(mine) + usize::from(theirs) + usize::from(late),
            1
        );
    });
}

/// A receiver closes while a sender sends: the send either lands (a receiver was open) or
/// fails with its value back; the closed handle reports the end of its waits.
#[test]
fn receiver_close_vs_send_and_waiting_changed() {
    loom::model(|| {
        let (tx, rx) = watch::channel(0u32);
        let kept = tx.subscribe().unwrap();
        let sender = thread::spawn(move || {
            let _ = tx.send(1);
            tx
        });
        rx.close();
        assert_eq!(block_on(rx.changed()), Err(RecvError));
        let _tx = sender.join().unwrap();
        assert_eq!(block_on(kept.changed()), Ok(()));
    });
}
