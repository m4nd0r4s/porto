//! Loom models of Mutex handoff. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sync`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use loom::sync::Arc;
use loom::thread;

use super::{Mutex, MutexGuard, WaitNode};

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

fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

/// The holder unlocks (handing the lock to `a`) while `a` is cancelled: whichever wins, `b`
/// ends up holding the lock, woken once.
#[test]
fn unlock_vs_cancel_hands_the_lock_to_the_next_waiter() {
    loom::model(|| {
        let mutex = Arc::new(Mutex::new(()));
        crate::sync::MutexGuard::leak(mutex.try_lock().expect("free"));
        let mut a = Box::pin(mutex.lock());
        let mut b = Box::pin(mutex.lock());
        let b_wakes = std::sync::Arc::new(CountingWaker::default());
        let b_waker = Waker::from(b_wakes.clone());
        assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());
        assert!(poll_once(b.as_mut(), &b_waker).is_pending());

        let unlocker = {
            let mutex = mutex.clone();
            // SAFETY: the leaked guard gave access to `()` only.
            thread::spawn(move || assert!(unsafe { mutex.force_unlock() }))
        };
        drop(a);
        unlocker.join().unwrap();

        assert_eq!(b_wakes.0.load(Ordering::SeqCst), 1);
        let Poll::Ready(guard) = poll_once(b.as_mut(), &b_waker) else {
            panic!("b holds the lock");
        };
        drop(guard);
        drop(b);
        assert!(!mutex.is_locked());
    });
}

/// The only waiter is cancelled while the holder unlocks: the mutex ends up unlocked.
#[test]
fn unlock_vs_cancel_of_the_only_waiter_unlocks() {
    loom::model(|| {
        let mutex = Arc::new(Mutex::new(()));
        crate::sync::MutexGuard::leak(mutex.try_lock().expect("free"));
        let mut a = Box::pin(mutex.lock());
        assert!(poll_once(a.as_mut(), Waker::noop()).is_pending());

        let unlocker = {
            let mutex = mutex.clone();
            // SAFETY: as above.
            thread::spawn(move || assert!(unsafe { mutex.force_unlock() }))
        };
        drop(a);
        unlocker.join().unwrap();

        assert!(!mutex.is_locked());
    });
}

/// The holder unlocks with an empty queue while a waiter links its node (the Python task's
/// path): the waiter takes the lock at once or is woken exactly once, never stranded.
#[test]
fn unlock_vs_link_never_loses_the_wakeup() {
    loom::model(|| {
        let mutex = Arc::new(Mutex::new(()));
        MutexGuard::leak(mutex.try_lock().expect("free"));
        let unlocker = {
            let mutex = mutex.clone();
            // SAFETY: the leaked guard gave access to `()` only.
            thread::spawn(move || assert!(unsafe { mutex.force_unlock() }))
        };
        let node = Box::pin(WaitNode::new(0));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: the node is pinned, used with this mutex only, and resolved below.
        let first = unsafe { mutex.poll_lock_node(node.as_ref(), &mut cx) };
        unlocker.join().unwrap();

        if first.is_pending() {
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1, "lost wakeup");
            // SAFETY: as above.
            assert!(unsafe { mutex.poll_lock_node(node.as_ref(), &mut cx) }.is_ready());
        }
        assert!(mutex.is_locked());
        // SAFETY: the node's grant gave access to `()` only.
        assert!(unsafe { mutex.force_unlock() });
        assert!(!mutex.is_locked());
        assert!(!node.is_pending());
    });
}

/// The holder unlocks (without the queue lock when nobody waits) while a queued waiter waits
/// and another thread tries to take the lock: the waiter is handed the lock, and the
/// `try_lock` never barges ahead of it; nobody else holds it meanwhile.
#[test]
fn try_lock_never_barges_ahead_of_a_queued_waiter() {
    loom::model(|| {
        let mutex = Arc::new(Mutex::new(()));
        MutexGuard::leak(mutex.try_lock().expect("free"));
        let node = Box::pin(WaitNode::new(0));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: the node is pinned, used with this mutex only, and resolved below.
        assert!(unsafe { mutex.poll_lock_node(node.as_ref(), &mut cx) }.is_pending());
        let unlocker = {
            let mutex = mutex.clone();
            // SAFETY: the leaked guard gave access to `()` only.
            thread::spawn(move || assert!(unsafe { mutex.force_unlock() }))
        };
        let barger = {
            let mutex = mutex.clone();
            thread::spawn(move || mutex.try_lock().map(MutexGuard::leak).is_some())
        };
        unlocker.join().unwrap();
        assert!(
            !barger.join().unwrap(),
            "try_lock took the lock from a queued waiter"
        );
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1, "lost wakeup");
        // SAFETY: as above.
        assert!(unsafe { mutex.poll_lock_node(node.as_ref(), &mut cx) }.is_ready());
        // The handoff left nobody queued: this release takes the fast path.
        // SAFETY: the node's grant gave access to `()` only.
        assert!(unsafe { mutex.force_unlock() });
        assert!(!mutex.is_locked());
        assert!(mutex.try_lock().is_some());
    });
}

/// A granted waiter `a` is cancelled on one thread while the holder unlocks on another and a
/// late waiter `b` links on a third: `b` ends up holding the lock, nobody is stranded.
#[test]
fn cancel_of_a_granted_waiter_vs_unlock_vs_a_late_link() {
    loom::model(|| {
        let mutex = Arc::new(Mutex::new(()));
        MutexGuard::leak(mutex.try_lock().expect("free"));
        let a = Arc::new(Box::pin(WaitNode::new(0)));
        let mut noop = Context::from_waker(Waker::noop());
        // SAFETY: `a` is pinned, used with this mutex only, and cancelled below.
        assert!(unsafe { mutex.poll_lock_node(a.as_ref().as_ref(), &mut noop) }.is_pending());
        let unlocker = {
            let mutex = mutex.clone();
            // SAFETY: the leaked guard gave access to `()` only.
            thread::spawn(move || assert!(unsafe { mutex.force_unlock() }))
        };
        let canceller = {
            let (mutex, a) = (mutex.clone(), a.clone());
            // SAFETY: as above; nothing else uses `a` concurrently.
            thread::spawn(move || unsafe { mutex.cancel_lock_node(a.as_ref().as_ref()) })
        };
        let b = Box::pin(WaitNode::new(1));
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        // SAFETY: `b` is pinned, used with this mutex only, and resolved below.
        let first = unsafe { mutex.poll_lock_node(b.as_ref(), &mut cx) };
        unlocker.join().unwrap();
        canceller.join().unwrap();

        if first.is_pending() {
            assert_eq!(wakes.0.load(Ordering::SeqCst), 1, "b stranded");
            // SAFETY: as above.
            assert!(unsafe { mutex.poll_lock_node(b.as_ref(), &mut cx) }.is_ready());
        }
        // SAFETY: `b`'s grant gave access to `()` only.
        assert!(unsafe { mutex.force_unlock() });
        assert!(!mutex.is_locked());
        assert!(!a.is_pending());
    });
}

/// Two threads update the value through `try_lock`: loom checks that the guards never give
/// overlapping access to it.
#[test]
fn guards_serialize_value_accesses() {
    loom::model(|| {
        let mutex = Arc::new(Mutex::new(0_usize));
        let other = {
            let mutex = mutex.clone();
            thread::spawn(move || {
                if let Some(mut guard) = mutex.try_lock() {
                    *guard += 1;
                }
            })
        };
        let mine = mutex.try_lock().map(|mut guard| *guard += 1).is_some();
        other.join().unwrap();
        let total = *mutex.try_lock().expect("free");
        assert!(total >= usize::from(mine) && total <= 2);
    });
}
