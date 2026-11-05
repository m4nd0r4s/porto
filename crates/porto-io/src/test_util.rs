//! Test helpers: a waker that counts its wakes.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

/// Counts how many times its waker was woken.
#[derive(Default)]
pub(crate) struct CountingWaker {
    wakes: AtomicUsize,
}

impl CountingWaker {
    pub(crate) fn new() -> (Arc<Self>, Waker) {
        let counter = Arc::new(Self::default());
        let waker = Waker::from(Arc::clone(&counter));
        (counter, waker)
    }

    pub(crate) fn wakes(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
