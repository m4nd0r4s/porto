//! Loom models of Semaphore and RwLock grants. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sync`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};

use loom::sync::Arc;
use loom::thread;

use super::{RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore, WaitNode};

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

fn counting_waker() -> (std::sync::Arc<CountingWaker>, Waker) {
    let counter = std::sync::Arc::new(CountingWaker::default());
    (counter.clone(), Waker::from(counter))
}

/// A release (lock-free while nobody waits) races a waiter linking its node: the waiter takes
/// the permit at once or is woken exactly once, never stranded.
#[test]
fn semaphore_release_vs_enqueue_never_loses_the_wakeup() {
    loom::model(|| {
        let semaphore = Arc::new(Semaphore::new(1));
        semaphore.try_acquire(1).expect("free").forget();
        let releaser = {
            let semaphore = semaphore.clone();
            thread::spawn(move || assert!(semaphore.release(1)))
        };
        let node = Box::pin(WaitNode::new(0));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: the node is pinned, used with this semaphore only, and resolved below.
        let first = unsafe { semaphore.poll_acquire_node(node.as_ref(), 1, &mut cx) };
        releaser.join().unwrap();

        if first.is_pending() {
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1, "lost wakeup");
            // SAFETY: as above.
            assert!(unsafe { semaphore.poll_acquire_node(node.as_ref(), 1, &mut cx) }.is_ready());
        }
        assert_eq!(semaphore.acquired_permits(), 1);
        assert!(semaphore.release(1));
        assert_eq!(semaphore.available_permits(), 1);
        assert!(!node.is_pending());
    });
}

/// A granted waiter `a` is cancelled while the holder releases and `b` waits behind it:
/// whichever wins, `b` ends up with the permits, woken once, and none are lost.
#[test]
fn semaphore_cancel_of_a_granted_waiter_passes_the_permits_on() {
    loom::model(|| {
        let semaphore = Arc::new(Semaphore::new(2));
        semaphore.try_acquire(2).expect("free").forget();
        let a = Arc::new(Box::pin(WaitNode::new(0)));
        let mut noop = Context::from_waker(Waker::noop());
        // SAFETY: `a` is pinned, used with this semaphore only, and cancelled below.
        let first = unsafe { semaphore.poll_acquire_node(a.as_ref().as_ref(), 2, &mut noop) };
        assert!(first.is_pending());
        let b = Box::pin(WaitNode::new(0));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: `b` is pinned, used with this semaphore only, and resolved below.
        assert!(unsafe { semaphore.poll_acquire_node(b.as_ref(), 1, &mut cx) }.is_pending());

        let releaser = {
            let semaphore = semaphore.clone();
            thread::spawn(move || assert!(semaphore.release(2)))
        };
        // SAFETY: as above; nothing else uses `a` concurrently.
        unsafe { semaphore.cancel_acquire_node(a.as_ref().as_ref()) };
        releaser.join().unwrap();

        assert_eq!(wakes.0.load(Ordering::SeqCst), 1, "b stranded");
        // SAFETY: as above.
        assert!(unsafe { semaphore.poll_acquire_node(b.as_ref(), 1, &mut cx) }.is_ready());
        assert!(semaphore.release(1));
        assert_eq!(semaphore.available_permits(), 2);
        assert_eq!(semaphore.acquired_permits(), 0);
        assert!(!a.is_pending());
    });
}

/// Two threads acquire and release through the fast paths while a third adds a permit: the
/// counts stay consistent and the permits never go above the total.
#[test]
fn semaphore_fast_paths_vs_add_permits_keep_the_counts() {
    loom::model(|| {
        let semaphore = Arc::new(Semaphore::new(1));
        let user = {
            let semaphore = semaphore.clone();
            thread::spawn(move || {
                if let Some(permit) = semaphore.try_acquire(1) {
                    drop(permit);
                }
            })
        };
        let adder = {
            let semaphore = semaphore.clone();
            thread::spawn(move || semaphore.add_permits(1))
        };
        let mine = semaphore.try_acquire(2);
        user.join().unwrap();
        adder.join().unwrap();
        drop(mine);
        assert_eq!(semaphore.total_permits(), 2);
        assert_eq!(semaphore.available_permits(), 2);
        assert!(!semaphore.release(1));
    });
}

/// A reader releases while a writer links its node: the writer gets the lock at once or is
/// woken exactly once.
#[test]
fn rwlock_read_release_vs_writer_enqueue_never_loses_the_wakeup() {
    loom::model(|| {
        let lock = Arc::new(RwLock::new(()));
        RwLockReadGuard::leak(lock.try_read().expect("free"));
        let releaser = {
            let lock = lock.clone();
            // SAFETY: the leaked guard gave access to `()` only.
            thread::spawn(move || assert!(unsafe { lock.force_read_unlock() }))
        };
        let node = Box::pin(WaitNode::new(0));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: the node is pinned, used with this lock only, and resolved below.
        let first = unsafe { lock.poll_write_node(node.as_ref(), &mut cx) };
        releaser.join().unwrap();

        if first.is_pending() {
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1, "lost wakeup");
            // SAFETY: as above.
            assert!(unsafe { lock.poll_write_node(node.as_ref(), &mut cx) }.is_ready());
        }
        assert!(lock.is_write_locked());
        // SAFETY: the node's grant gave access to `()` only.
        assert!(unsafe { lock.force_write_unlock() });
        assert!(lock.try_write().is_some());
    });
}

/// While a writer waits behind a reader, a new reader on another thread never gets in ahead
/// of it, whatever the interleaving with the first reader's release.
#[test]
fn rwlock_a_waiting_writer_keeps_new_readers_out() {
    loom::model(|| {
        let lock = Arc::new(RwLock::new(()));
        RwLockReadGuard::leak(lock.try_read().expect("free"));
        let writer = Box::pin(WaitNode::new(0));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: the node is pinned, used with this lock only, and resolved below.
        assert!(unsafe { lock.poll_write_node(writer.as_ref(), &mut cx) }.is_pending());

        let reader = {
            let lock = lock.clone();
            thread::spawn(move || lock.try_read().map(RwLockReadGuard::leak).is_some())
        };
        let releaser = {
            let lock = lock.clone();
            // SAFETY: the leaked guard gave access to `()` only.
            thread::spawn(move || assert!(unsafe { lock.force_read_unlock() }))
        };
        assert!(
            !reader.join().unwrap(),
            "a reader got in ahead of a waiting writer"
        );
        releaser.join().unwrap();

        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
        // SAFETY: as above.
        assert!(unsafe { lock.poll_write_node(writer.as_ref(), &mut cx) }.is_ready());
        // SAFETY: the grant gave access to `()` only.
        assert!(unsafe { lock.force_write_unlock() });
        assert_eq!(lock.readers(), 0);
    });
}

/// A writer granted on release is cancelled before it observed it, while a reader waits
/// behind it: the reader ends up holding the lock.
#[test]
fn rwlock_cancel_of_a_granted_writer_lets_the_reader_in() {
    loom::model(|| {
        let lock = Arc::new(RwLock::new(()));
        RwLockWriteGuard::leak(lock.try_write().expect("free"));
        let writer = Arc::new(Box::pin(WaitNode::new(0)));
        let mut noop = Context::from_waker(Waker::noop());
        // SAFETY: the node is pinned, used with this lock only, and cancelled below.
        let first = unsafe { lock.poll_write_node(writer.as_ref().as_ref(), &mut noop) };
        assert!(first.is_pending());
        let reader = Box::pin(WaitNode::new(0));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: as above, resolved below.
        assert!(unsafe { lock.poll_read_node(reader.as_ref(), &mut cx) }.is_pending());

        let releaser = {
            let lock = lock.clone();
            // SAFETY: the leaked guard gave access to `()` only.
            thread::spawn(move || assert!(unsafe { lock.force_write_unlock() }))
        };
        // SAFETY: nothing else uses `writer` concurrently.
        unsafe { lock.cancel_node(writer.as_ref().as_ref()) };
        releaser.join().unwrap();

        assert_eq!(wakes.0.load(Ordering::SeqCst), 1, "reader stranded");
        // SAFETY: as above.
        assert!(unsafe { lock.poll_read_node(reader.as_ref(), &mut cx) }.is_ready());
        assert!(!lock.is_write_locked());
        // SAFETY: the grant gave access to `()` only.
        assert!(unsafe { lock.force_read_unlock() });
        assert!(!writer.is_pending());
    });
}
