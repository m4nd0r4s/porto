//! The blocking pool and `spawn_blocking` (`19-threads-and-blocking.md` §1, §4).
//!
//! Work that blocks a thread (a file read, a DNS lookup, a Python callable that does not
//! yield) must not run on a lane: it would stall every task on it. [`Handle::spawn_blocking`]
//! runs a closure on a pool thread instead and returns a [`JoinHandle`] for its result. The
//! pool is elastic: it starts no thread until the first job, adds one for each queued job
//! that no idle worker can take, up to `blocking_threads` (512 by default), and a worker that
//! was idle for `blocking_keepalive` (10 s) exits. There is no `block_in_place`: it would move
//! a lane's work to another thread under a task's feet (spec 19 §4).
//!
//! # Where a job's result goes
//!
//! A job is a task like any other: [`spawn_blocking`](Handle::spawn_blocking) makes a
//! `RustTask` whose future runs the closure when polled, and the [`JoinHandle`] is its join
//! handle. The pool queues the task's run-queue entry and a worker polls it once; completing
//! it wakes whoever awaits the handle through the waker that await registered, which is the
//! same wake path as any other task's: an injector push and a notify of the lane. That path is
//! what the loom models of `sched` and `task` cover; the pool's own queue and thread accounting
//! has its model in `loom_tests`.
//!
//! A job cannot be interrupted: dropping the `JoinHandle` detaches it (the closure still runs,
//! and its result is dropped by the worker), and [`JoinHandle::abort`] only cancels one that
//! has not started. Shutdown ([`Handle`]'s runtime dropped) cancels the queued jobs and gives
//! the running ones one second, then leaves their threads to finish alone.
//!
//! # Python
//!
//! The pool knows nothing about Python. A [`WorkerHooks`] value, installed by the runtime's
//! builder, runs on each new thread and keeps what it returns until the thread exits: the
//! Python bindings create the worker's `PyThreadState` there (spec 25 P0).

mod pool;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::sync::primitives::Arc as PoolArc;
use std::task::{Context, Poll};
use std::time::Duration;

pub use pool::{BlockingStats, WorkerHooks};
use pool::{Pool, PoolConfig, SubmitError};

use crate::task::{JoinHandle, Notified, Schedule, new_joinable_task};

/// The longest a runtime waits for its blocking workers when it is dropped (spec 26 §2).
pub(crate) const SHUTDOWN_WAIT: Duration = Duration::from_secs(1);

impl pool::Job for Notified {
    fn run(self) {
        Notified::run(self);
    }
}

/// How a runtime's blocking pool is sized.
#[derive(Clone)]
pub(crate) struct BlockingConfig {
    pub(crate) max_threads: usize,
    pub(crate) keepalive: Duration,
    pub(crate) hooks: Option<Arc<dyn WorkerHooks>>,
}

impl Default for BlockingConfig {
    fn default() -> Self {
        Self {
            max_threads: 512,
            keepalive: Duration::from_secs(10),
            hooks: None,
        }
    }
}

/// A runtime's blocking pool: where `spawn_blocking` jobs run.
pub(crate) struct BlockingPool {
    pool: PoolArc<Pool<Notified>>,
}

impl BlockingPool {
    pub(crate) fn new(config: BlockingConfig) -> Self {
        Self {
            pool: Pool::new(PoolConfig {
                max_threads: config.max_threads,
                keepalive: config.keepalive,
                hooks: config.hooks,
            }),
        }
    }

    pub(crate) fn stats(&self) -> BlockingStats {
        self.pool.stats()
    }

    /// Runs `job` on a worker. Returns its join handle.
    ///
    /// # Errors
    /// [`BlockingError::NoThread`] when the OS refused to start the first thread.
    pub(crate) fn spawn<F, R>(&self, job: F) -> Result<JoinHandle<R>, BlockingError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (task, join) = new_joinable_task(
            Blocking { job: Some(job) },
            BlockingSchedule {
                pool: self.pool.clone(),
            },
        );
        match Pool::submit(&self.pool, task) {
            Ok(()) => Ok(join),
            // A pool that is shut down drops the entry, which cancels the job: awaiting the
            // handle then reports a cancellation, as for a job still queued at shutdown.
            Err(SubmitError::Closed(task)) => {
                drop(task);
                Ok(join)
            }
            Err(SubmitError::NoThread(task, error)) => {
                drop(task);
                drop(join);
                Err(BlockingError::NoThread(error))
            }
        }
    }

    /// Stops the pool, see [`Pool::shutdown`]: the runtime calls it as it is dropped.
    pub(crate) fn close(&self) {
        drop(self.pool.close());
    }

    /// Waits up to [`SHUTDOWN_WAIT`] for the workers to leave; the rest are detached.
    pub(crate) fn shutdown(&self) -> bool {
        self.pool.shutdown(SHUTDOWN_WAIT)
    }
}

/// `spawn_blocking` could not start a worker.
#[derive(Debug)]
pub enum BlockingError {
    /// The OS refused to start a thread and none is running to take the job.
    NoThread(std::io::Error),
}

impl std::fmt::Display for BlockingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoThread(error) => write!(
                f,
                "porto: could not start a blocking-pool thread ({error}); the process is out \
                 of threads or memory: lower the number of blocking jobs that run at once (a \
                 CapacityLimiter), or raise the limit (`ulimit -u`, container pids limit)"
            ),
        }
    }
}

impl std::error::Error for BlockingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoThread(error) => Some(error),
        }
    }
}

/// The future of a blocking job: runs the closure the one time it is polled.
struct Blocking<F> {
    job: Option<F>,
}

// The closure is only moved out, never pinned in place, so the wrapper is `Unpin` whatever it
// captures.
impl<F> Unpin for Blocking<F> {}

impl<F, R> Future for Blocking<F>
where
    F: FnOnce() -> R,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<R> {
        let job = self
            .job
            .take()
            .expect("a blocking job is polled once: it completes in its first poll");
        Poll::Ready(job())
    }
}

/// Where a blocking job goes when it is woken or aborted: the pool's queue, never a lane.
#[derive(Clone)]
struct BlockingSchedule {
    pool: PoolArc<Pool<Notified>>,
}

// SAFETY: `schedule_local` always returns the task, so nothing is queued through a borrow of
// the scheduler inside the task's own allocation.
unsafe impl Schedule for BlockingSchedule {
    fn schedule_local(&self, task: Notified) -> Result<(), Notified> {
        Err(task)
    }

    fn schedule_remote(self, task: Notified) {
        // The pool hands the entry back if it is shut down (or out of threads); dropping it
        // cancels the job, which drops its closure here, outside the pool's lock.
        let _ = Pool::submit(&self.pool, task);
    }
}
