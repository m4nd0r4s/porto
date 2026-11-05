//! Loom models of Mutex handoff. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sync`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use loom::sync::Arc;
use loom::thread;

use super::Mutex;

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
