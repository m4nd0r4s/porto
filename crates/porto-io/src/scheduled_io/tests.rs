use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::*;
use crate::IoSlab;
use crate::test_util::CountingWaker;

fn live(slab: &IoSlab) -> (RegistrationId, &ScheduledIo) {
    let id = slab.allocate().unwrap();
    (id, slab.get(id).unwrap())
}

#[test]
fn packing_round_trips_every_field() {
    let ticks = [0, 1, 2, 0x1234, TICK_MASK - 1, TICK_MASK];
    for bits in 0..=Ready::ALL.bits() {
        let ready = Ready::from_bits_truncate(bits);
        for &tick in &ticks {
            for generation in 0..=GENERATION_MASK {
                let word = pack(ready, tick, generation);
                assert_eq!(unpack_ready(word), ready);
                assert_eq!(unpack_tick(word), tick);
                assert_eq!(unpack_generation(word), generation);
            }
        }
    }
}

#[test]
fn packed_fields_do_not_overlap_and_wrap() {
    assert_eq!(
        pack(Ready::EMPTY, TICK_MASK + 1, 0),
        0,
        "the tick wraps at 15 bits"
    );
    assert_eq!(pack(Ready::EMPTY, 0, GENERATION_MASK + 1), 0);
    let all = pack(Ready::ALL, TICK_MASK, GENERATION_MASK);
    assert_eq!(all.count_ones(), 6 + TICK_BITS + GENERATION_BITS);
    assert!(u32::try_from(all).is_ok(), "the word fits a 32-bit usize");
}

#[test]
fn dispatch_bumps_the_tick() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    let before = io.ready_event(id, Interest::READABLE);
    slab.dispatch(id, Ready::READABLE);
    let after = io.ready_event(id, Interest::READABLE);
    assert_eq!(after.ready(), Ready::READABLE);
    assert_eq!(after.tick, (before.tick + 1) & TICK_MASK);
}

#[test]
fn clearing_observed_readiness_clears_it() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    slab.dispatch(id, Ready::READABLE | Ready::WRITABLE);
    let event = io.ready_event(id, Interest::READABLE);
    io.clear_readiness(event);
    assert!(io.ready_event(id, Interest::READABLE).ready().is_empty());
    assert_eq!(
        io.ready_event(id, Interest::WRITABLE).ready(),
        Ready::WRITABLE,
        "only the observed bits are cleared"
    );
}

#[test]
fn stale_tick_clear_is_a_no_op() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    slab.dispatch(id, Ready::READABLE);
    let stale = io.ready_event(id, Interest::READABLE);
    // A newer edge arrives between the observation and the `WouldBlock`.
    slab.dispatch(id, Ready::READABLE);
    io.clear_readiness(stale);
    assert_eq!(
        io.ready_event(id, Interest::READABLE).ready(),
        Ready::READABLE,
        "the newer event must survive"
    );
}

#[test]
fn restricted_clear_leaves_the_other_direction() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    slab.dispatch(id, Ready::READABLE | Ready::WRITABLE);
    let both = io.ready_event(id, Interest::READABLE | Interest::WRITABLE);
    io.clear_readiness(both.restrict(Ready::WRITABLE));
    assert_eq!(
        io.ready_event(id, Interest::READABLE | Interest::WRITABLE)
            .ready(),
        Ready::READABLE
    );
}

#[test]
fn closed_bits_are_never_cleared() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    slab.dispatch(id, Ready::READABLE | Ready::READ_CLOSED);
    let event = io.ready_event(id, Interest::READABLE);
    io.clear_readiness(event);
    assert_eq!(
        io.ready_event(id, Interest::READABLE).ready(),
        Ready::READ_CLOSED
    );
}

#[test]
fn poll_readiness_wakes_on_matching_dispatch_only() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    let (counter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);

    assert!(io.poll_readiness(&mut cx, id, Direction::Read).is_pending());
    slab.dispatch(id, Ready::WRITABLE);
    assert_eq!(counter.wakes(), 0, "a write edge does not wake a reader");
    slab.dispatch(id, Ready::READABLE);
    assert_eq!(counter.wakes(), 1);
    let Poll::Ready(event) = io.poll_readiness(&mut cx, id, Direction::Read) else {
        panic!("readable after the dispatch");
    };
    assert_eq!(event.ready(), Ready::READABLE);
}

#[test]
fn error_wakes_both_directions() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    let (reader, reader_waker) = CountingWaker::new();
    let (writer, writer_waker) = CountingWaker::new();
    assert!(
        io.poll_readiness(&mut Context::from_waker(&reader_waker), id, Direction::Read)
            .is_pending()
    );
    assert!(
        io.poll_readiness(
            &mut Context::from_waker(&writer_waker),
            id,
            Direction::Write
        )
        .is_pending()
    );
    slab.dispatch(id, Ready::ERROR);
    assert_eq!((reader.wakes(), writer.wakes()), (1, 1));
}

#[test]
fn generation_mismatch_ignores_stale_wakers() {
    let slab = IoSlab::new();
    let (old, io) = live(&slab);
    let (old_waiter, old_waker) = CountingWaker::new();
    assert!(
        io.poll_readiness(&mut Context::from_waker(&old_waker), old, Direction::Read)
            .is_pending()
    );
    // Releasing wakes the old waiter, which then observes the closed event.
    assert!(slab.release(old));
    assert_eq!(old_waiter.wakes(), 1);
    let Poll::Ready(closed) =
        io.poll_readiness(&mut Context::from_waker(&old_waker), old, Direction::Read)
    else {
        panic!("a stale wait resolves at once");
    };
    assert_eq!(closed.ready(), Ready::READ_CLOSED | Ready::ERROR);

    // The slot is reused; a new task waits on it.
    let new = slab.allocate().unwrap();
    assert_eq!(new.index(), old.index());
    let (new_waiter, new_waker) = CountingWaker::new();
    assert!(
        io.poll_readiness(&mut Context::from_waker(&new_waker), new, Direction::Read)
            .is_pending()
    );

    // A late event for the old registration changes nothing and wakes nobody.
    assert!(!slab.dispatch(old, Ready::READABLE));
    assert_eq!(new_waiter.wakes(), 0);
    assert!(io.ready_event(new, Interest::READABLE).ready().is_empty());
    // Nor can a stale event clear the new owner's readiness.
    slab.dispatch(new, Ready::READABLE);
    io.clear_readiness(closed);
    assert_eq!(
        io.ready_event(new, Interest::READABLE).ready(),
        Ready::READABLE
    );
    assert_eq!(new_waiter.wakes(), 1);
}

#[test]
fn readiness_future_wakes_every_matching_waiter() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    let mut readers: Vec<_> = (0..3)
        .map(|_| Box::pin(io.readiness(id, Interest::READABLE)))
        .collect();
    let mut writer = Box::pin(io.readiness(id, Interest::WRITABLE));
    let (counter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);
    for reader in &mut readers {
        assert!(reader.as_mut().poll(&mut cx).is_pending());
    }
    assert!(writer.as_mut().poll(&mut cx).is_pending());

    slab.dispatch(id, Ready::READABLE);
    assert_eq!(counter.wakes(), 3, "the writer is not woken");
    for reader in &mut readers {
        let Poll::Ready(event) = reader.as_mut().poll(&mut cx) else {
            panic!("every reader is ready");
        };
        assert_eq!(event.ready(), Ready::READABLE);
    }
    assert!(writer.as_mut().poll(&mut cx).is_pending());
    drop(writer);
    assert!(
        !io.has_list_waiters(),
        "dropping a pending future unlinks it"
    );
}

#[test]
fn readiness_future_relinks_when_readiness_was_consumed() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    let (counter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(io.readiness(id, Interest::READABLE));
    assert!(future.as_mut().poll(&mut cx).is_pending());
    slab.dispatch(id, Ready::READABLE);
    // Someone else consumed the edge before this waiter ran.
    io.clear_readiness(io.ready_event(id, Interest::READABLE));
    assert!(future.as_mut().poll(&mut cx).is_pending());
    assert!(io.has_list_waiters());
    slab.dispatch(id, Ready::READABLE);
    assert_eq!(counter.wakes(), 2);
    assert!(future.as_mut().poll(&mut cx).is_ready());
}

#[test]
fn more_waiters_than_one_wake_batch_are_all_woken() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    let (counter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);
    let mut futures: Vec<_> = (0..100)
        .map(|_| Box::pin(io.readiness(id, Interest::WRITABLE)))
        .collect();
    for future in &mut futures {
        assert!(future.as_mut().poll(&mut cx).is_pending());
    }
    slab.dispatch(id, Ready::WRITABLE);
    assert_eq!(counter.wakes(), 100);
    assert!(!io.has_list_waiters());
}

#[test]
fn release_wakes_list_waiters_with_closed() {
    let slab = IoSlab::new();
    let (id, io) = live(&slab);
    let (counter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(io.readiness(id, Interest::WRITABLE));
    assert!(future.as_mut().poll(&mut cx).is_pending());
    slab.release(id);
    assert_eq!(counter.wakes(), 1);
    assert_eq!(
        future.as_mut().poll(&mut cx),
        Poll::Ready(ReadyEvent::closed(
            Ready::from_interest(Interest::WRITABLE),
            id.generation()
        ))
    );
}

#[test]
fn concurrent_dispatch_is_never_missed() {
    // A waiter that parks while another thread dispatches must be woken or see the readiness.
    for _ in 0..200 {
        let slab = Arc::new(IoSlab::new());
        let id = slab.allocate().unwrap();
        let dispatcher = {
            let slab = Arc::clone(&slab);
            std::thread::spawn(move || assert!(slab.dispatch(id, Ready::READABLE)))
        };
        let io = slab.get(id).unwrap();
        let (counter, waker) = CountingWaker::new();
        let mut cx = Context::from_waker(&waker);
        let mut future = pin!(io.readiness(id, Interest::READABLE));
        let pending = future.as_mut().poll(&mut cx).is_pending();
        dispatcher.join().unwrap();
        if pending {
            assert_eq!(counter.wakes(), 1, "a pending waiter is woken");
        }
        assert!(future.as_mut().poll(&mut cx).is_ready());
    }
}
