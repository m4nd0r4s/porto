//! Loom models of the wait queue. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sync`.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use loom::sync::Arc;

use loom::thread;

use super::{Locked, WaitNode, WaitQueue, WaitState};

#[derive(Debug)]
struct TestLock {
    held: bool,
}

impl WaitState for TestLock {
    fn pass_grant(waiters: &mut Locked<'_, Self>, _: usize) {
        release_locked(waiters);
    }
}

fn release_locked(locked: &mut Locked<'_, TestLock>) {
    if !locked.grant_front() {
        locked.held = false;
    }
}

fn never(_: &mut Locked<'_, TestLock>) -> bool {
    false
}

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

/// A waiter driven through the low-level API, as `PyTask.wait_node` will be.
struct Waiter {
    queue: Arc<WaitQueue<TestLock>>,
    node: Pin<Box<WaitNode>>,
}

impl Waiter {
    fn new(queue: &Arc<WaitQueue<TestLock>>) -> Self {
        Self {
            queue: queue.clone(),
            node: Box::pin(WaitNode::new(0)),
        }
    }

    fn poll(&self, waker: &Waker) -> Poll<()> {
        // SAFETY: one owner per waiter, one queue, cancelled in `Drop`.
        unsafe {
            self.queue
                .poll_wait(self.node.as_ref(), &mut Context::from_waker(waker), never)
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        // SAFETY: as in `poll`.
        unsafe { self.queue.cancel(self.node.as_ref()) };
    }
}

fn held_queue() -> Arc<WaitQueue<TestLock>> {
    Arc::new(WaitQueue::new(TestLock { held: true }))
}

/// A release grants `a` while `a` is cancelled: whichever wins, `b` ends up with the lock and
/// is woken exactly once.
#[test]
fn sync_grant_vs_cancel_passes_the_grant_on() {
    loom::model(|| {
        let queue = held_queue();
        let a = Waiter::new(&queue);
        let b = Waiter::new(&queue);
        let b_wakes = std::sync::Arc::new(CountingWaker::default());
        let b_waker = Waker::from(b_wakes.clone());
        assert!(a.poll(Waker::noop()).is_pending());
        assert!(b.poll(&b_waker).is_pending());

        let releaser = {
            let queue = queue.clone();
            thread::spawn(move || release_locked(&mut queue.lock()))
        };
        let canceller = thread::spawn(move || drop(a));
        releaser.join().unwrap();
        canceller.join().unwrap();

        assert_eq!(b_wakes.0.load(Ordering::SeqCst), 1);
        assert!(b.poll(&b_waker).is_ready());
        let locked = queue.lock();
        assert!(locked.held, "b holds the lock");
        assert!(locked.is_empty());
    });
}

/// The only waiter is cancelled while a release grants it: the lock ends up free.
#[test]
fn sync_grant_vs_cancel_of_the_last_waiter_unlocks() {
    loom::model(|| {
        let queue = held_queue();
        let a = Waiter::new(&queue);
        assert!(a.poll(Waker::noop()).is_pending());

        let releaser = {
            let queue = queue.clone();
            thread::spawn(move || release_locked(&mut queue.lock()))
        };
        drop(a);
        releaser.join().unwrap();

        let locked = queue.lock();
        assert!(!locked.held, "the grant came back to the lock");
        assert!(locked.is_empty());
    });
}

/// A release grants `a` while `a` polls (lock-free when it sees the grant) and then drops:
/// `a` either completes, keeping the lock, or is still waiting and cancels, freeing it.
#[test]
fn sync_grant_vs_poll_then_drop() {
    loom::model(|| {
        let queue = held_queue();
        let a = Waiter::new(&queue);
        assert!(a.poll(Waker::noop()).is_pending());

        let releaser = {
            let queue = queue.clone();
            thread::spawn(move || release_locked(&mut queue.lock()))
        };
        let completed = a.poll(Waker::noop()).is_ready();
        drop(a);
        releaser.join().unwrap();

        let locked = queue.lock();
        assert_eq!(locked.held, completed);
        assert!(locked.is_empty());
    });
}
