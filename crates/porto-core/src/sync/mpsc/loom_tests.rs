//! Loom models of the channel: close races and the send/receive wake-up handshake. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release mpsc`.
//!
//! Waiting goes through `loom::future::block_on`, so a lost wake-up shows up as a deadlock
//! that loom reports.
//!
//! The models run under a preemption bound of 3 (one of them 2; `LOOM_MAX_PREEMPTIONS`
//! overrides both): the array queue spins while a peer is half-way through a push or pop, so
//! the unbounded search does not finish. Bounded search is loom's documented practice; most
//! concurrency bugs need two or three preemptions.

use loom::future::block_on;
use loom::model::Builder;
use loom::thread;

use super::{Receiver, RecvError, SendError, channel};

/// Explores `f` under the bound described in the module docs.
fn model(f: impl Fn() + Sync + Send + 'static) {
    model_bounded(3, f);
}

/// Explores `f` with at most `preemptions` preemptions (unless `LOOM_MAX_PREEMPTIONS` says
/// otherwise).
fn model_bounded(preemptions: usize, f: impl Fn() + Sync + Send + 'static) {
    let mut builder = Builder::new();
    builder.preemption_bound.get_or_insert(preemptions);
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
