//! RwLock tests: shared readers, writer preference, FIFO phases, cancellation, node API,
//! starvation-freedom under a mixed load.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use super::{RwLock, RwLockReadGuard, WaitNode};
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

fn wakes(counter: &CountingWaker) -> usize {
    counter.0.load(Ordering::SeqCst)
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn the_lock_and_its_futures_are_thread_safe() {
    assert_send_sync::<RwLock<Vec<u8>>>();
    assert_send_sync::<super::RwLockRead<'static, Vec<u8>>>();
    assert_send_sync::<super::RwLockWrite<'static, Vec<u8>>>();
}

#[test]
fn readers_share_the_lock_and_a_writer_excludes_them() {
    let lock = RwLock::new(5);
    let Poll::Ready(a) = poll_once(pin!(lock.read()), Waker::noop()) else {
        panic!("a free lock reads at once");
    };
    let b = lock.try_read().expect("readers share");
    assert_eq!(*a + *b, 10);
    assert_eq!(lock.readers(), 2);
    assert!(lock.try_write().is_none());
    drop((a, b));
    let mut writer = lock.try_write().expect("free");
    *writer += 1;
    assert!(lock.is_write_locked());
    assert!(lock.try_read().is_none());
    drop(writer);
    assert_eq!(lock.into_inner(), 6);
}

#[test]
fn a_waiting_writer_blocks_new_readers() {
    let lock = RwLock::new(());
    let reader = lock.try_read().expect("free");
    let (writer_wakes, writer_waker) = counting_waker();
    let mut writer = Box::pin(lock.write());
    assert!(poll_once(writer.as_mut(), &writer_waker).is_pending());
    // Readable (a reader holds it), but a writer waits: new readers queue behind it.
    assert!(lock.try_read().is_none());
    let (late_wakes, late_waker) = counting_waker();
    let mut late = Box::pin(lock.read());
    assert!(poll_once(late.as_mut(), &late_waker).is_pending());

    drop(reader);
    assert_eq!(wakes(&writer_wakes), 1);
    assert_eq!(wakes(&late_wakes), 0);
    let Poll::Ready(guard) = poll_once(writer.as_mut(), &writer_waker) else {
        panic!("the writer was handed the lock");
    };
    drop(guard);
    assert_eq!(wakes(&late_wakes), 1);
    assert!(poll_once(late.as_mut(), &late_waker).is_ready());
}

#[test]
fn a_cancelled_writer_unblocks_the_readers_queued_behind_it() {
    let lock = RwLock::new(());
    let reader = lock.try_read().expect("free");
    let mut writer = Box::pin(lock.write());
    assert!(poll_once(writer.as_mut(), Waker::noop()).is_pending());
    let readers: Vec<_> = (0..3).map(|_| counting_waker()).collect();
    let mut waits: Vec<_> = (0..3).map(|_| Box::pin(lock.read())).collect();
    for (wait, (_, waker)) in waits.iter_mut().zip(&readers) {
        assert!(poll_once(wait.as_mut(), waker).is_pending());
    }
    drop(writer);
    for (wakes_of, _) in &readers {
        assert_eq!(wakes(wakes_of), 1, "the readers behind the writer go in");
    }
    let guards: Vec<_> = waits
        .iter_mut()
        .map(|wait| match poll_once(wait.as_mut(), Waker::noop()) {
            Poll::Ready(guard) => guard,
            Poll::Pending => panic!("granted"),
        })
        .collect();
    assert_eq!(lock.readers(), 4);
    drop((guards, reader));
    assert_eq!(lock.readers(), 0);
}

#[test]
fn waiters_are_served_in_arrival_order_consecutive_readers_together() {
    let lock = RwLock::new(());
    let writer = lock.try_write().expect("free");
    let order = ["r", "r", "w", "r"];
    let wakers: Vec<_> = order.iter().map(|_| counting_waker()).collect();
    let mut reads = Vec::new();
    let mut writes = Vec::new();
    for (kind, (_, waker)) in order.iter().zip(&wakers) {
        if *kind == "r" {
            let mut wait = Box::pin(lock.read());
            assert!(poll_once(wait.as_mut(), waker).is_pending());
            reads.push(wait);
        } else {
            let mut wait = Box::pin(lock.write());
            assert!(poll_once(wait.as_mut(), waker).is_pending());
            writes.push(wait);
        }
    }
    let woken = || wakers.iter().map(|(w, _)| wakes(w)).collect::<Vec<_>>();
    drop(writer);
    assert_eq!(woken(), [1, 1, 0, 0]);
    let first: Vec<_> = reads[..2]
        .iter_mut()
        .map(|wait| match poll_once(wait.as_mut(), Waker::noop()) {
            Poll::Ready(guard) => guard,
            Poll::Pending => panic!("granted"),
        })
        .collect();
    drop(first);
    assert_eq!(woken(), [1, 1, 1, 0]);
    let Poll::Ready(guard) = poll_once(writes[0].as_mut(), Waker::noop()) else {
        panic!("granted");
    };
    drop(guard);
    assert_eq!(woken(), [1, 1, 1, 1]);
    assert!(poll_once(reads[2].as_mut(), Waker::noop()).is_ready());
}

#[test]
fn a_cancelled_granted_writer_passes_the_lock_on() {
    let lock = RwLock::new(());
    let reader = lock.try_read().expect("free");
    let mut writer = Box::pin(lock.write());
    let (wakes_of, waker) = counting_waker();
    let mut late = Box::pin(lock.read());
    assert!(poll_once(writer.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(late.as_mut(), &waker).is_pending());
    drop(reader); // grants the writer
    drop(writer); // which gave up before it saw it
    assert_eq!(wakes(&wakes_of), 1);
    assert!(poll_once(late.as_mut(), &waker).is_ready());
    assert!(!lock.is_write_locked());
}

#[test]
fn releasing_what_is_not_held_is_refused() {
    let lock = RwLock::new(());
    // SAFETY: no guard reference is used.
    unsafe {
        assert!(!lock.force_read_unlock());
        assert!(!lock.force_write_unlock());
    }
    RwLockReadGuard::leak(lock.try_read().expect("free"));
    // SAFETY: as above.
    unsafe {
        assert!(
            !lock.force_write_unlock(),
            "a reader holds it, not a writer"
        );
        assert!(lock.force_read_unlock());
    }
}

#[test]
fn the_node_api_reads_and_writes() {
    let lock = RwLock::new(());
    let writer = lock.try_write().expect("free");
    let node = Box::pin(WaitNode::new(0));
    let (wakes_of, waker) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    // SAFETY: the node is pinned, used with this lock only, and resolved below.
    assert!(unsafe { lock.poll_read_node(node.as_ref(), &mut cx) }.is_pending());
    drop(writer);
    assert_eq!(wakes(&wakes_of), 1);
    // SAFETY: as above; then a write wait on the same node, as a task's would be.
    unsafe {
        assert!(lock.poll_read_node(node.as_ref(), &mut cx).is_ready());
        assert!(lock.poll_write_node(node.as_ref(), &mut cx).is_pending());
        assert!(lock.force_read_unlock());
        assert!(lock.poll_write_node(node.as_ref(), &mut cx).is_ready());
        assert!(lock.force_write_unlock());
    }
}

#[test]
fn reads_and_writes_run_on_the_runtime() {
    let rt = Runtime::new();
    let lock = RwLock::new(0);
    rt.block_on(async {
        *lock.write().await += 1;
        assert_eq!(*lock.read().await, 1);
    });
}

#[test]
fn a_mixed_load_starves_nobody() {
    let lock = Arc::new(RwLock::new(0_usize));
    let threads: Vec<_> = (0..6)
        .map(|n| {
            let lock = lock.clone();
            thread::spawn(move || {
                for _ in 0..1000 {
                    if n % 3 == 0 {
                        *lock.blocking_write().expect("off lane") += 1;
                    } else {
                        let value = *lock.blocking_read().expect("off lane");
                        assert!(value <= 2000);
                    }
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(*lock.try_read().expect("free"), 2000);
}
