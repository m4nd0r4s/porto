//! Loom models of the channel: close races and the send/receive wake-up handshake. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release mpsc`.
//!
//! Waiting goes through `loom::future::block_on`, so a lost wake-up shows up as a deadlock
//! that loom reports.
//!
//! The models run under a preemption bound of 3 (two of them 2 and 1; a lower
//! `LOOM_MAX_PREEMPTIONS` lowers them further): the array queue spins while a peer is half-way
//! through a push or pop, so the unbounded search does not finish. Bounded search is loom's documented practice; most
//! concurrency bugs need two or three preemptions.

use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;

use loom::future::block_on;
use loom::model::Builder;
use loom::thread;

use crate::loom_support::cap_preemptions;

use super::super::WaitNode;
use super::{
    Receiver, RecvError, SendError, Sender, TryRecvError, TrySendError, channel, unbounded_channel,
};

/// Explores `f` under the bound described in the module docs.
fn model(f: impl Fn() + Sync + Send + 'static) {
    model_bounded(3, f);
}

/// Explores `f` with at most `preemptions` preemptions, fewer if `LOOM_MAX_PREEMPTIONS` is
/// lower (`crate::loom_support`).
fn model_bounded(preemptions: usize, f: impl Fn() + Sync + Send + 'static) {
    let mut builder = Builder::new();
    cap_preemptions(&mut builder, preemptions);
    // Each spin of the queue on a preempted peer is a branch of the path.
    builder.max_branches = 10_000;
    builder.check(f);
}

/// Receives until the channel ends.
fn receive_all(rx: &Receiver<u32>) -> Vec<u32> {
    let mut received = Vec::new();
    while let Ok(value) = block_on(rx.recv()) {
        received.push(value);
    }
    received
}

/// mpsc close races: the last sender sends and closes while a receiver waits. The receiver
/// gets the value, then the end; it never hangs and never sees the end first.
#[test]
fn mpsc_close_races_send_then_close_vs_waiting_receiver() {
    model(|| {
        let (tx, rx) = channel(1);
        let sender = thread::spawn(move || {
            tx.try_send(1).unwrap();
            drop(tx);
        });
        assert_eq!(receive_all(&rx), [1]);
        sender.join().unwrap();
    });
}

/// mpsc close races: one of two senders closes while the other sends. The channel only ends
/// after the second sender is gone, and the value is never lost.
#[test]
fn mpsc_close_races_one_sender_closes_while_another_sends() {
    model(|| {
        let (tx, rx) = channel(1);
        let tx2 = tx.clone();
        let closer = thread::spawn(move || drop(tx));
        let sender = thread::spawn(move || {
            block_on(tx2.send(7)).unwrap();
        });
        assert_eq!(receive_all(&rx), [7]);
        closer.join().unwrap();
        sender.join().unwrap();
    });
}

/// mpsc close races: the last receiver closes while a sender waits for room. The send never
/// hangs: it fails and hands the value back, or (if it passed the closed check just before
/// the close made room) lands, and then nothing is left buffered for nobody.
#[test]
fn mpsc_close_races_receiver_closes_while_sender_waits() {
    model(|| {
        let (tx, rx) = channel(1);
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || {
            let sent = block_on(tx.send(2));
            (sent, tx)
        });
        drop(rx);
        let (sent, tx) = sender.join().unwrap();
        assert!(matches!(sent, Ok(()) | Err(SendError(2))), "{sent:?}");
        assert!(tx.is_closed());
        assert_eq!(tx.len(), 0, "values nobody can receive are dropped");
    });
}

/// mpsc close races: a sender waits for room while one receiver makes room and closes and the
/// other closes too. The send settles (lands or fails with its value) and never hangs.
#[test]
fn mpsc_close_races_receive_and_close_vs_waiting_sender() {
    model(|| {
        let (tx, rx) = channel(1);
        let rx2 = rx.clone();
        tx.try_send(1).unwrap();
        let receiver = thread::spawn(move || rx2.try_recv());
        let sender = thread::spawn(move || block_on(tx.send(2)));
        drop(rx);
        assert_eq!(receiver.join().unwrap(), Ok(1));
        match sender.join().unwrap() {
            Ok(()) | Err(SendError(2)) => {}
            Err(SendError(other)) => panic!("the send handed back {other}, not its value"),
        }
    });
}

/// send/recv wake-up: with a capacity of one, the second send waits for the receiver's first
/// receive and the receiver waits for each send. Both sides wake; the values arrive in order.
#[test]
fn mpsc_send_recv_wakeup() {
    model(|| {
        let (tx, rx) = channel(1);
        let sender = thread::spawn(move || {
            block_on(tx.send(1)).unwrap();
            block_on(tx.send(2)).unwrap();
        });
        assert_eq!(block_on(rx.recv()), Ok(1));
        assert_eq!(block_on(rx.recv()), Ok(2));
        sender.join().unwrap();
        assert_eq!(block_on(rx.recv()), Err(RecvError));
    });
}

/// send/recv wake-up with two receivers: one send wakes one of them, the sender's close wakes
/// the other; nobody hangs.
///
/// Three threads that all touch the queue: at a bound of 3 a receiver spinning on a peer's
/// half-finished pop exceeds loom's path budget, so this one model runs at 2.
#[test]
fn mpsc_send_recv_wakeup_two_receivers() {
    model_bounded(2, || {
        let (tx, rx) = channel(1);
        let rx2 = rx.clone();
        let other = thread::spawn(move || block_on(rx2.recv()));
        let sender = thread::spawn(move || block_on(tx.send(5)).unwrap());
        let mine = block_on(rx.recv());
        sender.join().unwrap();
        let theirs = other.join().unwrap();
        assert!(
            matches!(
                (mine, theirs),
                (Ok(5), Err(RecvError)) | (Err(RecvError), Ok(5))
            ),
            "one receiver gets the value, the other the end: {mine:?}, {theirs:?}"
        );
    });
}

/// Receives the way the Python binding does: with a node that no `recv` future owns, waited on
/// with `poll_ready`, retrying `try_recv` after each wake.
fn recv_with_node(rx: &Receiver<u32>) -> Result<u32, RecvError> {
    let node = pin!(WaitNode::new(0));
    let received = block_on(poll_fn(|cx| {
        // SAFETY: the node lives in this frame, is only used with this channel, and is
        // cancelled below before the frame ends.
        if node.is_pending() && unsafe { rx.poll_ready(node.as_ref(), cx) }.is_pending() {
            return Poll::Pending;
        }
        loop {
            match rx.try_recv() {
                Ok(value) => return Poll::Ready(Ok(value)),
                Err(TryRecvError::Closed) => return Poll::Ready(Err(RecvError)),
                Err(TryRecvError::Empty) => {}
            }
            // SAFETY: as above.
            if unsafe { rx.poll_ready(node.as_ref(), cx) }.is_pending() {
                return Poll::Pending;
            }
        }
    }));
    // SAFETY: as above.
    unsafe { rx.cancel_ready(node.as_ref()) };
    received
}

/// Sends the way the Python binding does; see `recv_with_node`.
fn send_with_node(tx: &Sender<u32>, value: u32) -> Result<(), SendError<u32>> {
    let node = pin!(WaitNode::new(0));
    let mut value = Some(value);
    let sent = block_on(poll_fn(|cx| {
        // SAFETY: as in `recv_with_node`.
        if node.is_pending() && unsafe { tx.poll_ready(node.as_ref(), cx) }.is_pending() {
            return Poll::Pending;
        }
        loop {
            match tx.try_send(value.take().expect("held until sent")) {
                Ok(()) => return Poll::Ready(Ok(())),
                Err(TrySendError::Closed(back)) => return Poll::Ready(Err(SendError(back))),
                Err(TrySendError::Full(back)) => value = Some(back),
            }
            // SAFETY: as above.
            if unsafe { tx.poll_ready(node.as_ref(), cx) }.is_pending() {
                return Poll::Pending;
            }
        }
    }));
    // SAFETY: as above.
    unsafe { tx.cancel_ready(node.as_ref()) };
    sent
}

/// External-node waits: a receiver waiting with `poll_ready` sees every send (its look at the
/// buffer and the sender's hint check cannot both miss), then the end.
#[test]
fn mpsc_ready_wait_receiver_sees_every_send() {
    model(|| {
        let (tx, rx) = channel(1);
        let sender = thread::spawn(move || {
            block_on(tx.send(1)).unwrap();
            block_on(tx.send(2)).unwrap();
        });
        assert_eq!(recv_with_node(&rx), Ok(1));
        assert_eq!(recv_with_node(&rx), Ok(2));
        sender.join().unwrap();
        assert_eq!(recv_with_node(&rx), Err(RecvError));
    });
}

/// External-node waits: a sender waiting for room with `poll_ready` sees every receive.
#[test]
fn mpsc_ready_wait_sender_sees_every_receive() {
    model(|| {
        let (tx, rx) = channel(1);
        let sender = thread::spawn(move || {
            send_with_node(&tx, 1).unwrap();
            send_with_node(&tx, 2).unwrap();
        });
        assert_eq!(block_on(rx.recv()), Ok(1));
        assert_eq!(block_on(rx.recv()), Ok(2));
        sender.join().unwrap();
    });
}

/// External-node waits: the last receiver closes while a sender waits for room; the send
/// settles and never hangs.
#[test]
fn mpsc_ready_wait_sender_vs_receiver_close() {
    model(|| {
        let (tx, rx) = channel(1);
        tx.try_send(1).unwrap();
        let sender = thread::spawn(move || send_with_node(&tx, 2));
        drop(rx);
        let sent = sender.join().unwrap();
        assert!(matches!(sent, Ok(()) | Err(SendError(2))), "{sent:?}");
    });
}

/// Handle close vs a parked waiter: another thread closes the receiving handle a node waiter
/// is parked on (Python's `rx.close()` from another task) while the sender stays open and
/// idle. The waiter wakes and sees the end.
#[test]
fn mpsc_handle_close_wakes_parked_node_receiver() {
    model(|| {
        let (tx, rx) = channel::<u32>(1);
        let rx = Arc::new(rx);
        let closer = {
            let rx = rx.clone();
            thread::spawn(move || rx.close())
        };
        assert_eq!(recv_with_node(&rx), Err(RecvError));
        closer.join().unwrap();
        drop(tx);
    });
}

/// Handle close vs a parked waiter: as above, for a sender waiting for room on a full channel.
#[test]
fn mpsc_handle_close_wakes_parked_node_sender() {
    model(|| {
        let (tx, rx) = channel::<u32>(1);
        tx.try_send(1).unwrap();
        let tx = Arc::new(tx);
        let closer = {
            let tx = tx.clone();
            thread::spawn(move || tx.close())
        };
        assert_eq!(send_with_node(&tx, 2), Err(SendError(2)));
        closer.join().unwrap();
        drop(rx);
    });
}

/// Handle close vs a parked waiter: as above, for a `recv` future.
#[test]
fn mpsc_handle_close_wakes_parked_recv_future() {
    model(|| {
        let (tx, rx) = channel::<u32>(1);
        let rx = Arc::new(rx);
        let closer = {
            let rx = rx.clone();
            thread::spawn(move || rx.close())
        };
        assert_eq!(block_on(rx.recv()), Err(RecvError));
        closer.join().unwrap();
        drop(tx);
    });
}

/// Handle close vs a wake it holds: two receivers wait, a send wakes one of them and its
/// handle closes before it retries. The value still reaches the other receiver while the
/// sender is open: the close does not swallow the wake.
#[test]
fn mpsc_handle_close_passes_on_the_wake_it_holds() {
    model_bounded(2, || {
        let (tx, rx) = unbounded_channel::<u32>();
        let rx = Arc::new(rx);
        let rx2 = (*rx).clone();
        let closed = {
            let rx = rx.clone();
            thread::spawn(move || recv_with_node(&rx))
        };
        let other = thread::spawn(move || recv_with_node(&rx2));
        tx.try_send(7).unwrap();
        rx.close();
        if closed.join().unwrap() == Ok(7) {
            drop(tx);
            assert_eq!(other.join().unwrap(), Err(RecvError));
        } else {
            assert_eq!(other.join().unwrap(), Ok(7));
            drop(tx);
        }
    });
}

/// As above, for senders: two senders wait for room, a receive wakes one of them and its
/// handle closes before it retries. The other sender still gets the room.
///
/// Three threads spinning on a bounded queue: at a bound of 2 the search exceeds loom's
/// path budget, so this model runs at 1.
#[test]
fn mpsc_handle_close_passes_on_the_room_it_holds() {
    model_bounded(1, || {
        let (tx, rx) = channel::<u32>(1);
        tx.try_send(0).unwrap();
        let tx = Arc::new(tx);
        let tx2 = (*tx).clone();
        let closed = {
            let tx = tx.clone();
            thread::spawn(move || send_with_node(&tx, 1))
        };
        let other = thread::spawn(move || send_with_node(&tx2, 2));
        assert_eq!(rx.try_recv(), Ok(0));
        tx.close();
        match closed.join().unwrap() {
            // Its send landed before the close: the other one waits for the next receive.
            Ok(()) => {
                assert_eq!(rx.try_recv(), Ok(1));
                assert_eq!(other.join().unwrap(), Ok(()));
            }
            // It gave up: the room it was woken for goes to the other send.
            Err(SendError(1)) => assert_eq!(other.join().unwrap(), Ok(())),
            Err(SendError(other)) => panic!("the send handed back {other}, not its value"),
        }
        assert_eq!(rx.try_recv(), Ok(2));
    });
}
