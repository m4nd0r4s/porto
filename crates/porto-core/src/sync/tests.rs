//! Wait queue tests: FIFO order, cancel removal, grant handoff, blocking waits.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::{Locked, WaitNode, WaitQueue, WaitState, blocking_wait};
use crate::Runtime;

/// A minimal exclusive lock: what `Lock` will build on the queue.
#[derive(Debug, Default)]
struct TestLock {
    held: bool,
    /// Tokens passed to `pass_grant`, in order.
    passed: Vec<usize>,
}

impl WaitState for TestLock {
    fn pass_grant(waiters: &mut Locked<'_, Self>, token: usize) {
        waiters.passed.push(token);
        release_locked(waiters);
    }
}

/// Hands the lock to the first waiter, or unlocks it if nobody waits.
fn release_locked(locked: &mut Locked<'_, TestLock>) {
    if !locked.grant_front() {
        locked.held = false;
    }
}

fn release(queue: &WaitQueue<TestLock>) {
    release_locked(&mut queue.lock());
}

/// Takes the lock if it is free and nobody is queued ahead.
fn try_take(locked: &mut Locked<'_, TestLock>) -> bool {
    if locked.held || !locked.is_empty() {
        return false;
    }
    locked.held = true;
    true
}

fn held_queue() -> WaitQueue<TestLock> {
    WaitQueue::new(TestLock {
        held: true,
        passed: Vec::new(),
    })
}

/// Counts its wakes.
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

#[test]
fn acquires_at_once_when_free() {
    let queue = WaitQueue::new(TestLock::default());
    let mut wait = pin!(queue.wait(0, try_take));
    assert!(poll_once(wait.as_mut(), Waker::noop()).is_ready());
    let locked = queue.lock();
    assert!(locked.held);
    assert!(locked.is_empty());
}

#[test]
fn grants_in_fifo_order() {
    let queue = held_queue();
    let mut waits: Vec<_> = (0..4).map(|i| Box::pin(queue.wait(i, try_take))).collect();
    let wakers: Vec<_> = (0..4).map(|_| counting_waker()).collect();
    for (wait, (_, waker)) in waits.iter_mut().zip(&wakers) {
        assert!(poll_once(wait.as_mut(), waker).is_pending());
    }
    assert_eq!(queue.lock().len(), 4);
    for granted in 0..4 {
        assert_eq!(queue.lock().front_token(), Some(granted));
        release(&queue);
        for (i, (counter, _)) in wakers.iter().enumerate() {
            let expected = usize::from(i == granted);
            assert_eq!(counter.0.load(Ordering::SeqCst), expected, "waiter {i}");
        }
        // Only the granted waiter completes; the later ones still wait.
        for (i, wait) in waits.iter_mut().enumerate().skip(granted) {
            let ready = poll_once(wait.as_mut(), &wakers[i].1).is_ready();
            assert_eq!(ready, i == granted, "waiter {i} after grant {granted}");
        }
        wakers[granted].0.0.store(0, Ordering::SeqCst);
    }
    let locked = queue.lock();
    assert!(locked.held, "the last waiter holds the lock");
    assert!(locked.is_empty());
}

#[test]
fn newcomer_does_not_jump_the_queue() {
    let queue = held_queue();
    let mut first = pin!(queue.wait(1, try_take));
    assert!(poll_once(first.as_mut(), Waker::noop()).is_pending());
    release(&queue);
    // The lock was handed to `first`, so a newcomer finds it held.
    let mut newcomer = pin!(queue.wait(2, try_take));
    assert!(poll_once(newcomer.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(first.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn cancelled_waiter_is_removed() {
    let queue = held_queue();
    let mut a = Box::pin(queue.wait(0, try_take));
    let mut b = Box::pin(queue.wait(1, try_take));
    let mut c = Box::pin(queue.wait(2, try_take));
    for wait in [&mut a, &mut b, &mut c] {
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());
    }
    drop(b);
    assert_eq!(queue.lock().len(), 2);
    release(&queue);
    assert!(poll_once(a.as_mut(), Waker::noop()).is_ready());
    assert_eq!(queue.lock().front_token(), Some(2));
    release(&queue);
    assert!(poll_once(c.as_mut(), Waker::noop()).is_ready());
    assert!(queue.lock().passed.is_empty(), "nothing was granted to b");
}

#[test]
fn cancelling_head_and_tail_keeps_the_list_linked() {
    let queue = held_queue();
    let mut waits: Vec<_> = (0..3)
        .map(|i| Some(Box::pin(queue.wait(i, try_take))))
        .collect();
    for wait in waits.iter_mut().flatten() {
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());
    }
    waits[0] = None;
    waits[2] = None;
    assert_eq!(queue.lock().front_token(), Some(1));
    // A newcomer goes after the only remaining waiter.
    let mut late = pin!(queue.wait(3, try_take));
    assert!(poll_once(late.as_mut(), Waker::noop()).is_pending());
    release(&queue);
    assert_eq!(queue.lock().front_token(), Some(3));
    release(&queue);
    assert!(poll_once(late.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn cancelled_after_grant_passes_the_grant_on() {
    let queue = held_queue();
    let mut a = Box::pin(queue.wait(10, try_take));
    let mut b = Box::pin(queue.wait(20, try_take));
    let (b_wakes, b_waker) = counting_waker();
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(b.as_mut(), &b_waker).is_pending());
    release(&queue); // grants a
    drop(a); // ... which is cancelled before it observes the grant
    assert_eq!(b_wakes.0.load(Ordering::SeqCst), 1, "the grant went to b");
    assert_eq!(queue.lock().passed, [10]);
    assert!(poll_once(b.as_mut(), &b_waker).is_ready());
    assert!(queue.lock().held);
}

#[test]
fn cancelled_after_grant_with_no_one_left_unlocks() {
    let queue = held_queue();
    let mut a = Box::pin(queue.wait(7, try_take));
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    release(&queue);
    drop(a);
    let locked = queue.lock();
    assert!(!locked.held);
    assert_eq!(locked.passed, [7]);
}

#[test]
fn completed_wait_passes_nothing_on() {
    let queue = held_queue();
    let mut a = Box::pin(queue.wait(0, try_take));
    assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
    release(&queue);
    assert!(poll_once(a.as_mut(), Waker::noop()).is_ready());
    drop(a);
    let locked = queue.lock();
    assert!(locked.held, "the completed waiter owns the lock now");
    assert!(locked.passed.is_empty());
}

#[test]
fn repoll_with_a_new_waker_replaces_the_old_one() {
    let queue = held_queue();
    let mut a = pin!(queue.wait(0, try_take));
    let (old, old_waker) = counting_waker();
    let (new, new_waker) = counting_waker();
    assert!(poll_once(a.as_mut(), &old_waker).is_pending());
    assert!(poll_once(a.as_mut(), &new_waker).is_pending());
    release(&queue);
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(new.0.load(Ordering::SeqCst), 1);
    assert!(poll_once(a.as_mut(), &new_waker).is_ready());
}

#[test]
#[should_panic(expected = "polled after it completed")]
fn polling_after_completion_panics() {
    let queue = WaitQueue::new(TestLock::default());
    let mut wait = pin!(queue.wait(0, try_take));
    assert!(poll_once(wait.as_mut(), Waker::noop()).is_ready());
    let _ = poll_once(wait.as_mut(), Waker::noop());
}

/// An event: `grant_all` wakes everyone and a cancelled grant is simply dropped.
#[derive(Debug, Default)]
struct TestEvent {
    set: bool,
}

impl WaitState for TestEvent {
    fn pass_grant(_: &mut Locked<'_, Self>, _: usize) {}
}

#[test]
fn grant_all_wakes_every_waiter() {
    let queue = WaitQueue::new(TestEvent::default());
    let is_set = |locked: &mut Locked<'_, TestEvent>| locked.set;
    let mut waits: Vec<_> = (0..3).map(|i| Box::pin(queue.wait(i, is_set))).collect();
    for wait in &mut waits {
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());
    }
    {
        let mut locked = queue.lock();
        locked.set = true;
        assert_eq!(locked.grant_all(), 3);
    }
    for wait in &mut waits {
        assert!(poll_once(wait.as_mut(), Waker::noop()).is_ready());
    }
    let mut late = pin!(queue.wait(9, is_set));
    assert!(poll_once(late.as_mut(), Waker::noop()).is_ready());
}

#[test]
fn raw_node_is_reusable_after_completion_and_cancel() {
    // The low-level API PyTask's embedded `wait_node` will use.
    let queue = held_queue();
    let node = pin!(WaitNode::new(5));
    let mut cx = Context::from_waker(Waker::noop());
    // SAFETY: one thread, one queue that outlives every wait, cancelled before the drop.
    unsafe {
        assert!(
            queue
                .poll_wait(node.as_ref(), &mut cx, try_take)
                .is_pending()
        );
        assert!(node.is_pending());
        queue.cancel(node.as_ref());
        assert!(!node.is_pending());
        assert!(queue.lock().is_empty());

        assert!(
            queue
                .poll_wait(node.as_ref(), &mut cx, try_take)
                .is_pending()
        );
        release(&queue);
        assert!(queue.poll_wait(node.as_ref(), &mut cx, try_take).is_ready());
        assert!(!node.is_pending());
        queue.cancel(node.as_ref()); // idle: a no-op
    }
    assert!(queue.lock().passed.is_empty());
}

#[test]
fn tasks_take_the_lock_in_arrival_order() {
    let rt = Runtime::new();
    let queue = Arc::new(held_queue());
    let order = Arc::new(Mutex::new(Vec::new()));
    for i in 0..5 {
        let queue = queue.clone();
        let order = order.clone();
        rt.spawn(async move {
            queue.wait(i, try_take).await;
            order.lock().unwrap().push(i);
            release(&queue);
        });
    }
    rt.run_until_idle();
    assert_eq!(queue.lock().len(), 5);
    release(&queue);
    rt.run_until_idle();
    assert_eq!(*order.lock().unwrap(), [0, 1, 2, 3, 4]);
    assert!(!queue.lock().held);
}

#[test]
fn blocking_wait_completes_on_a_plain_thread() {
    let queue = Arc::new(held_queue());
    let (queued_tx, queued_rx) = mpsc::channel();
    let waiter = {
        let queue = queue.clone();
        thread::spawn(move || {
            let wait = queue.wait(0, |locked| {
                let taken = try_take(locked);
                queued_tx.send(()).unwrap();
                taken
            });
            blocking_wait("TestLock.blocking_acquire", wait)
        })
    };
    queued_rx.recv().unwrap();
    // Give the waiter time to park, so the wake really unparks it.
    thread::sleep(Duration::from_millis(20));
    release(&queue);
    assert_eq!(waiter.join().unwrap(), Ok(()));
    assert!(queue.lock().held);
}

#[test]
fn blocking_wait_on_a_lane_is_an_error() {
    let rt = Runtime::new();
    let queue = held_queue();
    let error = rt
        .block_on(async { blocking_wait("Lock.blocking_acquire", queue.wait(0, try_take)) })
        .unwrap_err();
    assert_eq!(error.operation(), "Lock.blocking_acquire");
    let message = error.to_string();
    assert!(message.starts_with("Lock.blocking_acquire cannot be called on a porto lane"));
    assert!(
        message.contains("Await the async form instead"),
        "{message}"
    );
    // The refused wait never queued.
    assert!(queue.lock().is_empty());
}

#[test]
fn blocking_wait_in_a_task_is_an_error() {
    let rt = Runtime::new();
    let result = Arc::new(Mutex::new(None));
    let slot = result.clone();
    rt.spawn(async move {
        *slot.lock().unwrap() = Some(blocking_wait("Event.blocking_wait", async {}));
    });
    rt.run_until_idle();
    let result = result.lock().unwrap().take().unwrap();
    assert!(result.is_err());
}
