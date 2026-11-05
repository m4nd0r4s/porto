//! Channel tests: ordering, capacity, close and drain, wake-ups, and a multi-thread stress run.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use super::{
    Receiver, RecvError, SendError, Sender, TryRecvError, TrySendError, channel, unbounded_channel,
};
use crate::Runtime;
use crate::sync::blocking_wait;

/// Counts its wakes.
#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl CountingWaker {
    fn new() -> (Arc<Self>, Waker) {
        let counter = Arc::new(Self::default());
        let waker = Waker::from(counter.clone());
        (counter, waker)
    }

    fn wakes(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

/// Counts drops, to check that buffered values are released.
struct Tracked(Arc<AtomicUsize>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn drain<T>(rx: &Receiver<T>) -> Vec<T> {
    let mut values = Vec::new();
    while let Ok(value) = rx.try_recv() {
        values.push(value);
    }
    values
}

#[test]
fn values_come_out_in_send_order() {
    let (tx, rx) = channel(4);
    for i in 0..4 {
        tx.try_send(i).unwrap();
    }
    assert_eq!(drain(&rx), [0, 1, 2, 3]);
    // Wrap around the ring a few times.
    for round in 0..10 {
        for i in 0..3 {
            tx.try_send(round * 3 + i).unwrap();
        }
        assert_eq!(drain(&rx), [round * 3, round * 3 + 1, round * 3 + 2]);
    }
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn a_full_channel_refuses_and_returns_the_value() {
    let (tx, rx) = channel(2);
    assert_eq!(tx.capacity(), Some(2));
    tx.try_send("a").unwrap();
    tx.try_send("b").unwrap();
    assert_eq!(tx.len(), 2);
    let error = tx.try_send("c").unwrap_err();
    assert!(error.is_full());
    assert_eq!(error.into_inner(), "c");
    assert_eq!(rx.try_recv(), Ok("a"));
    tx.try_send("c").unwrap();
    assert_eq!(drain(&rx), ["b", "c"]);
    assert!(rx.is_empty());
}

#[test]
fn a_capacity_of_one_works() {
    let (tx, rx) = channel(1);
    for i in 0..5 {
        tx.try_send(i).unwrap();
        assert!(tx.try_send(99).unwrap_err().is_full());
        assert_eq!(rx.try_recv(), Ok(i));
    }
}

#[test]
#[should_panic(expected = "channel capacity must be at least 1 (got 0)")]
fn a_zero_capacity_is_refused() {
    let _ = channel::<u8>(0);
}

#[test]
#[should_panic(expected = "is too large")]
fn an_absurd_capacity_is_refused() {
    let _ = channel::<u8>(usize::MAX);
}

#[test]
fn receivers_drain_before_seeing_the_end() {
    let (tx, rx) = channel(4);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    drop(tx);
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    let mut recv = pin!(rx.recv());
    assert_eq!(
        poll(recv.as_mut(), Waker::noop()),
        Poll::Ready(Err(RecvError))
    );
}

#[test]
fn the_channel_ends_only_when_every_sender_is_closed() {
    let (tx, rx) = channel(4);
    let tx2 = tx.clone();
    tx.close();
    tx.close(); // idempotent
    assert!(tx.is_closed());
    assert_eq!(tx.try_send(1), Err(TrySendError::Closed(1)));
    assert!(
        tx.clone().is_closed(),
        "a clone of a closed sender is closed"
    );
    assert!(!tx2.is_closed());
    tx2.try_send(2).unwrap();
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    drop(tx2);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn sends_fail_with_the_value_once_every_receiver_is_gone() {
    let (tx, rx) = channel(4);
    let rx2 = rx.clone();
    rx.close();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    tx.try_send(1).unwrap();
    assert!(!tx.is_closed());
    drop(rx2);
    assert!(tx.is_closed());
    assert_eq!(tx.try_send(2), Err(TrySendError::Closed(2)));
    let mut send = pin!(tx.send(3));
    assert_eq!(
        poll(send.as_mut(), Waker::noop()),
        Poll::Ready(Err(SendError(3)))
    );
}

#[test]
fn the_last_receiver_drops_the_buffered_values() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = channel(4);
    for _ in 0..3 {
        tx.try_send(Tracked(drops.clone())).unwrap();
    }
    drop(rx);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    drop(tx);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

#[test]
fn dropping_the_channel_drops_the_buffered_values() {
    for (tx, rx) in [channel(8), unbounded_channel()] {
        let drops = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            tx.try_send(Tracked(drops.clone())).unwrap();
        }
        drop(rx.try_recv().unwrap());
        // The receiver outlives the senders; the values go with the last handle.
        drop(tx);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(rx);
        assert_eq!(drops.load(Ordering::SeqCst), 5);
    }
}

#[test]
fn a_send_wakes_exactly_one_waiting_receiver_oldest_first() {
    let (tx, rx) = channel::<u32>(4);
    let (first_wakes, first_waker) = CountingWaker::new();
    let (second_wakes, second_waker) = CountingWaker::new();
    let mut first = pin!(rx.recv());
    let mut second = pin!(rx.recv());
    assert!(poll(first.as_mut(), &first_waker).is_pending());
    assert!(poll(second.as_mut(), &second_waker).is_pending());

    tx.try_send(7).unwrap();
    assert_eq!((first_wakes.wakes(), second_wakes.wakes()), (1, 0));
    assert_eq!(poll(first.as_mut(), &first_waker), Poll::Ready(Ok(7)));

    tx.try_send(8).unwrap();
    assert_eq!((first_wakes.wakes(), second_wakes.wakes()), (1, 1));
    assert_eq!(poll(second.as_mut(), &second_waker), Poll::Ready(Ok(8)));
}

#[test]
fn a_receive_wakes_exactly_one_waiting_sender_oldest_first() {
    let (tx, rx) = channel(1);
    tx.try_send(0).unwrap();
    let (first_wakes, first_waker) = CountingWaker::new();
    let (second_wakes, second_waker) = CountingWaker::new();
    let mut first = pin!(tx.send(1));
    let mut second = pin!(tx.send(2));
    assert!(poll(first.as_mut(), &first_waker).is_pending());
    assert!(poll(second.as_mut(), &second_waker).is_pending());

    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!((first_wakes.wakes(), second_wakes.wakes()), (1, 0));
    assert_eq!(poll(first.as_mut(), &first_waker), Poll::Ready(Ok(())));
    assert!(poll(second.as_mut(), &second_waker).is_pending());

    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(second_wakes.wakes(), 1);
    assert_eq!(poll(second.as_mut(), &second_waker), Poll::Ready(Ok(())));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn a_woken_receiver_that_is_cancelled_passes_the_wake_on() {
    let (tx, rx) = channel(4);
    let (second_wakes, second_waker) = CountingWaker::new();
    let mut first = Box::pin(rx.recv());
    let mut second = pin!(rx.recv());
    assert!(poll(first.as_mut(), Waker::noop()).is_pending());
    assert!(poll(second.as_mut(), &second_waker).is_pending());

    tx.try_send(5).unwrap(); // wakes `first`...
    drop(first); // ...which is cancelled before it ran
    assert_eq!(second_wakes.wakes(), 1);
    assert_eq!(poll(second.as_mut(), &second_waker), Poll::Ready(Ok(5)));
}

#[test]
fn a_woken_receiver_that_lost_the_value_waits_again() {
    let (tx, rx) = channel(4);
    let (wakes, waker) = CountingWaker::new();
    let mut waiting = pin!(rx.recv());
    assert!(poll(waiting.as_mut(), &waker).is_pending());
    tx.try_send(1).unwrap();
    assert_eq!(rx.try_recv(), Ok(1), "another receiver takes it first");
    assert!(poll(waiting.as_mut(), &waker).is_pending());
    tx.try_send(2).unwrap();
    assert_eq!(wakes.wakes(), 2);
    assert_eq!(poll(waiting.as_mut(), &waker), Poll::Ready(Ok(2)));
}

#[test]
fn the_last_sender_wakes_every_waiting_receiver() {
    let (tx, rx) = channel::<u8>(4);
    let wakers: Vec<_> = (0..3).map(|_| CountingWaker::new()).collect();
    let mut receives: Vec<_> = (0..3).map(|_| Box::pin(rx.recv())).collect();
    for (recv, (_, waker)) in receives.iter_mut().zip(&wakers) {
        assert!(poll(recv.as_mut(), waker).is_pending());
    }
    drop(tx);
    for (recv, (wakes, waker)) in receives.iter_mut().zip(&wakers) {
        assert_eq!(wakes.wakes(), 1);
        assert_eq!(poll(recv.as_mut(), waker), Poll::Ready(Err(RecvError)));
    }
}

#[test]
fn the_last_receiver_fails_every_waiting_sender() {
    let (tx, rx) = channel(1);
    tx.try_send(0).unwrap();
    let (wakes, waker) = CountingWaker::new();
    let mut send = pin!(tx.send(1));
    assert!(poll(send.as_mut(), &waker).is_pending());
    drop(rx);
    assert_eq!(wakes.wakes(), 1);
    assert_eq!(poll(send.as_mut(), &waker), Poll::Ready(Err(SendError(1))));
}

#[test]
fn a_cancelled_send_drops_its_value_and_leaves_the_queue() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = channel(1);
    tx.try_send(Tracked(drops.clone())).unwrap();
    {
        let mut send = pin!(tx.send(Tracked(drops.clone())));
        assert!(poll(send.as_mut(), Waker::noop()).is_pending());
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(rx.try_recv().unwrap());
    tx.try_send(Tracked(drops.clone())).unwrap();
    assert_eq!(rx.len(), 1);
}

#[test]
#[should_panic(expected = "polled after it completed")]
fn polling_a_completed_receive_again_panics() {
    let (tx, rx) = channel(1);
    tx.try_send(1).unwrap();
    let mut recv = pin!(rx.recv());
    assert!(poll(recv.as_mut(), Waker::noop()).is_ready());
    let _ = poll(recv.as_mut(), Waker::noop());
}

#[test]
#[should_panic(expected = "polled after it completed")]
fn polling_a_completed_send_again_panics() {
    let (tx, _rx) = channel(1);
    let mut send = pin!(tx.send(1));
    assert!(poll(send.as_mut(), Waker::noop()).is_ready());
    let _ = poll(send.as_mut(), Waker::noop());
}

#[test]
fn an_unbounded_channel_never_fills() {
    let (tx, rx) = unbounded_channel();
    assert_eq!(tx.capacity(), None);
    let count = if cfg!(miri) { 100 } else { 10_000 };
    for i in 0..count {
        tx.try_send(i).unwrap();
    }
    assert_eq!(rx.len(), count);
    assert!(drain(&rx).into_iter().eq(0..count));
    drop(tx);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
}

#[test]
fn recv_many_takes_a_batch_after_waiting_for_the_first() {
    let rt = Runtime::new();
    let (tx, rx) = channel(8);
    for i in 0..5 {
        tx.try_send(i).unwrap();
    }
    let mut batch = Vec::new();
    assert_eq!(rt.block_on(rx.recv_many(&mut batch, 3)), 3);
    assert_eq!(rt.block_on(rx.recv_many(&mut batch, 10)), 2);
    assert_eq!(batch, [0, 1, 2, 3, 4]);
    assert_eq!(rt.block_on(rx.recv_many(&mut batch, 0)), 0);
    drop(tx);
    assert_eq!(rt.block_on(rx.recv_many(&mut batch, 10)), 0, "ended");
}

#[test]
fn tasks_exchange_values_through_a_small_channel() {
    let rt = Runtime::new();
    let (tx, rx) = channel(2);
    let received = Arc::new(Mutex::new(Vec::new()));
    for producer in 0..3 {
        let tx = tx.clone();
        rt.spawn(async move {
            for i in 0..20 {
                tx.send((producer, i)).await.unwrap();
            }
        });
    }
    drop(tx);
    for _ in 0..2 {
        let rx = rx.clone();
        let received = received.clone();
        rt.spawn(async move {
            while let Ok(value) = rx.recv().await {
                received.lock().unwrap().push(value);
            }
        });
    }
    drop(rx);
    rt.run_until_idle();
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 60);
    for producer in 0..3 {
        let sent = received
            .iter()
            .filter(|(p, _)| *p == producer)
            .map(|(_, i)| *i);
        assert!(sent.eq(0..20), "per-producer order");
    }
}

#[test]
fn errors_say_what_to_do() {
    let full = TrySendError::Full(1).to_string();
    assert!(full.contains("await send()"), "{full}");
    let closed = SendError(1).to_string();
    assert!(closed.contains("keep a receiver open"), "{closed}");
    let empty = TryRecvError::Empty.to_string();
    assert!(empty.contains("await recv()"), "{empty}");
    let ended = RecvError.to_string();
    assert!(ended.contains("stop receiving"), "{ended}");
}

/// Producers and consumers on plain threads, through the async futures driven by
/// `blocking_wait`. Every value arrives exactly once, and each consumer sees each producer's
/// values in order. A lost wake-up hangs the test.
fn stress(messages_per_producer: usize, capacity: usize) {
    const PRODUCERS: usize = 4;
    const CONSUMERS: usize = 4;
    let (tx, rx) = channel::<(usize, usize)>(capacity);
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..messages_per_producer {
                    // Odd values try the non-waiting path first.
                    let value = if i % 2 == 1 {
                        match tx.try_send((producer, i)) {
                            Ok(()) => continue,
                            Err(error) => error.into_inner(),
                        }
                    } else {
                        (producer, i)
                    };
                    blocking_wait("Sender.blocking_send", tx.send(value))
                        .unwrap()
                        .unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    let consumers: Vec<_> = (0..CONSUMERS)
        .map(|consumer| {
            let rx = rx.clone();
            thread::spawn(move || {
                let mut last = [None::<usize>; PRODUCERS];
                let mut received = 0usize;
                let mut batch = Vec::new();
                loop {
                    let got = if consumer % 2 == 0 {
                        match blocking_wait("Receiver.blocking_recv", rx.recv()).unwrap() {
                            Ok(value) => {
                                batch.push(value);
                                1
                            }
                            Err(RecvError) => 0,
                        }
                    } else {
                        blocking_wait("Receiver.blocking_recv", rx.recv_many(&mut batch, 16))
                            .unwrap()
                    };
                    if got == 0 {
                        return received;
                    }
                    for (producer, i) in batch.drain(..) {
                        assert!(last[producer].is_none_or(|prev| prev < i), "out of order");
                        last[producer] = Some(i);
                        received += 1;
                    }
                }
            })
        })
        .collect();
    drop(rx);
    for producer in producers {
        producer.join().unwrap();
    }
    let total: usize = consumers.into_iter().map(|c| c.join().unwrap()).sum();
    assert_eq!(total, PRODUCERS * messages_per_producer);
}

#[test]
fn stress_small() {
    if cfg!(miri) {
        stress(50, 2);
    } else {
        stress(5_000, 8);
    }
}

/// One million messages (4 producers x 250k). Run in release:
/// `cargo test -p porto-core --release stress_one_million -- --ignored`.
#[test]
#[ignore = "slow in debug builds; run in release with --ignored"]
fn stress_one_million() {
    stress(250_000, 64);
    stress(250_000, 1);
}

#[test]
fn handles_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<Sender<String>>();
    assert_send_sync::<Receiver<String>>();
    assert_send::<super::SendFuture<'static, String>>();
    assert_send::<super::RecvFuture<'static, String>>();
}

#[test]
fn a_ready_wait_with_an_external_node_is_woken_and_passes_an_unobserved_wake_on() {
    use crate::sync::WaitNode;

    let (tx, rx) = channel::<u32>(1);
    let (first_wakes, first_waker) = CountingWaker::new();
    let (second_wakes, second_waker) = CountingWaker::new();
    let first = pin!(WaitNode::new(0));
    let second = pin!(WaitNode::new(0));
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);
    // SAFETY: both nodes outlive their waits on this channel and end idle (cancelled below).
    unsafe {
        assert!(rx.poll_ready(first.as_ref(), &mut first_cx).is_pending());
        assert!(rx.poll_ready(second.as_ref(), &mut second_cx).is_pending());
        tx.try_send(1).unwrap();
        assert_eq!(first_wakes.wakes(), 1, "one send wakes the oldest waiter");
        assert_eq!(second_wakes.wakes(), 0);
        // The first waiter gives up before it saw its wake: the second one gets it.
        rx.cancel_ready(first.as_ref());
        assert_eq!(second_wakes.wakes(), 1);
        assert!(rx.poll_ready(second.as_ref(), &mut second_cx).is_ready());
        assert_eq!(rx.try_recv(), Ok(1));
        // A value is buffered: no wait.
        tx.try_send(2).unwrap();
        assert!(rx.poll_ready(first.as_ref(), &mut first_cx).is_ready());
        // Full: a sender waits for room, and a receive wakes it.
        let (sender_wakes, sender_waker) = CountingWaker::new();
        let mut sender_cx = Context::from_waker(&sender_waker);
        assert!(tx.poll_ready(first.as_ref(), &mut sender_cx).is_pending());
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(sender_wakes.wakes(), 1);
        assert!(tx.poll_ready(first.as_ref(), &mut sender_cx).is_ready());
        // A closed handle never waits.
        rx.close();
        assert!(rx.poll_ready(second.as_ref(), &mut second_cx).is_ready());
        rx.cancel_ready(second.as_ref());
    }
}

#[test]
fn closing_through_a_shared_reference_is_per_handle_and_idempotent() {
    let (tx, rx) = channel::<u32>(2);
    let tx2 = tx.clone();
    tx.close();
    tx.close();
    assert!(tx.is_handle_closed());
    assert!(!tx2.is_handle_closed());
    tx2.try_send(3).unwrap();
    drop(tx2);
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    assert!(
        !rx.is_handle_closed(),
        "the channel ended, the handle is open"
    );
}
