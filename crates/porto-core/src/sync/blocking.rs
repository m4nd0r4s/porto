//! Waiting from threads that do not run a lane: the backing of every `blocking_*` method.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use crate::sched::current_handle;

/// A `blocking_*` call was made on a lane thread.
///
/// Blocking there would stall every task on the lane, possibly including the one that would
/// complete the wait. The Python layer raises it as `RuntimeError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockingOnLane {
    operation: &'static str,
}

impl BlockingOnLane {
    /// The `blocking_*` operation that was refused, as passed to [`blocking_wait`].
    pub fn operation(&self) -> &'static str {
        self.operation
    }
}

impl fmt::Display for BlockingOnLane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} cannot be called on a porto lane thread: it would block every task on the \
             lane. Await the async form instead, or call it from a thread that does not run \
             porto (for example one started with porto.to_thread)",
            self.operation
        )
    }
}

impl Error for BlockingOnLane {}

/// Runs `future` to completion on the calling thread, parking it while the future is pending.
///
/// `operation` names the public method being served (`"Lock.blocking_acquire"`, say) for the
/// error message.
///
/// # Errors
/// [`BlockingOnLane`] if this thread runs a porto lane (a task, or inside `block_on`). The
/// future is dropped unpolled.
pub fn blocking_wait<F: Future>(
    operation: &'static str,
    future: F,
) -> Result<F::Output, BlockingOnLane> {
    if current_handle().is_some() {
        return Err(BlockingOnLane { operation });
    }
    let unpark = Arc::new(Unpark {
        woken: AtomicBool::new(false),
        thread: thread::current(),
    });
    let waker = Waker::from(unpark.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return Ok(output);
        }
        // `park` may return spuriously; the flag says whether a wake really came.
        while !unpark.woken.swap(false, Ordering::Acquire) {
            thread::park();
        }
    }
}

/// Wakes a thread parked in [`blocking_wait`].
struct Unpark {
    woken: AtomicBool,
    thread: Thread,
}

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}
