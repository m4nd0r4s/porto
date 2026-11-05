//! The concurrency primitives the wait queue, the channels and the scheduler's injector and
//! parking are built from: std's, or loom's under `cfg(loom)` (in loom test builds) so the model
//! checker sees every lock, atomic, cell access and thread unpark.

#[cfg(all(loom, test))]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(all(loom, test))]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, fence};
#[cfg(all(loom, test))]
pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};
#[cfg(all(loom, test))]
pub(crate) use loom_thread::{Thread, current as current_thread, park};

#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, fence};
#[cfg(not(all(loom, test)))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard};
#[cfg(not(all(loom, test)))]
pub(crate) use std::thread::{Thread, current as current_thread, park};

/// Lets other threads run while spinning on a peer's half-finished push or pop. Under loom it
/// yields to the model checker, which would otherwise explore the spin forever.
#[cfg(all(loom, test))]
pub(crate) fn spin(_step: u32) {
    loom::thread::yield_now();
}

/// Lets other threads run while spinning on a peer's half-finished push or pop: a few CPU spin
/// hints first, then the OS scheduler (the peer may have been preempted mid-operation).
#[cfg(not(all(loom, test)))]
pub(crate) fn spin(step: u32) {
    if step < 6 {
        for _ in 0..1u32 << step {
            std::hint::spin_loop();
        }
    } else {
        std::thread::yield_now();
    }
}

/// `std::cell::UnsafeCell` with loom's closure-based API.
#[cfg(not(all(loom, test)))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(all(loom, test)))]
impl<T> UnsafeCell<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

/// Threads with std's park token under loom. loom's own `unpark` of a thread that is blocked on
/// something else (a lock) makes it runnable and drops the token, where std keeps it for the
/// next `park`; that would report lost wakeups that cannot happen.
#[cfg(all(loom, test))]
mod loom_thread {
    use loom::sync::{Arc, Condvar, Mutex};

    /// A handle to unpark a thread: its token.
    #[derive(Clone)]
    pub(crate) struct Thread(Arc<(Mutex<bool>, Condvar)>);

    impl Thread {
        /// Makes the token available, waking the thread if it is parked.
        pub(crate) fn unpark(&self) {
            let (token, parked) = &*self.0;
            *token.lock().unwrap() = true;
            parked.notify_one();
        }
    }

    loom::thread_local! {
        static CURRENT: Thread = Thread(Arc::new((Mutex::new(false), Condvar::new())));
    }

    /// The current thread's handle.
    pub(crate) fn current() -> Thread {
        CURRENT.with(Clone::clone)
    }

    /// Blocks until the token is available, and takes it.
    pub(crate) fn park() {
        CURRENT.with(|thread| {
            let (token, parked) = &*thread.0;
            let mut available = token.lock().unwrap();
            while !*available {
                available = parked.wait(available).unwrap();
            }
            *available = false;
        });
    }
}
