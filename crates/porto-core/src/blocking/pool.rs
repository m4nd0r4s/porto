//! The elastic thread pool behind `spawn_blocking` (`19-threads-and-blocking.md` §1).
//!
//! One mutex guards the job queue and the thread accounting, and one condition variable parks
//! the idle workers. Every decision is made under that mutex, which is what keeps a job from
//! being stranded:
//!
//! - **submit** pushes the job, then, with `queued` jobs and `idle` waiting workers, spawns a
//!   thread if `queued > idle` and the pool is below its limit, and notifies one idle worker
//!   otherwise. A worker that was notified still counts as idle until it takes the lock, so a
//!   second job pushed before it runs sees `queued > idle` and gets a thread of its own.
//! - **a worker** takes a job if there is one, else waits (counted idle) for a notify or for
//!   `keepalive` to pass. On waking it stops counting itself idle and looks at the queue
//!   again; it retires, leaving the thread count in the same critical section, only if the wait
//!   timed out and the queue is empty (`threads` drops there, and `alive`, which shutdown
//!   waits for, only once the worker has torn its thread state down). So a push after the
//!   retire sees the thread gone, and a push before it is seen by the worker before it decides.
//! - **shutdown** closes the pool and takes the queue under the lock; workers see `closed` with
//!   an empty queue and leave; the caller waits for the count to reach zero, up to its limit.
//!
//! The jobs are generic so the loom models (`loom_tests`) can run the same state machine with
//! trivial jobs; the runtime uses [`Notified`](crate::task::Notified) entries.
//!
//! Threads are detached: a worker holds the pool's `Arc`, so a pool that was shut down while a
//! job still ran stays alive until that worker leaves.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::time::Duration;

#[cfg(all(loom, test))]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(all(loom, test))]
use loom::thread;
#[cfg(not(all(loom, test)))]
use std::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(all(loom, test)))]
use std::thread;

use crate::sync::primitives::{Arc, AtomicBool};

/// Something a worker runs once. Dropping it unrun cancels it.
pub(crate) trait Job: Send + 'static {
    /// Runs the job on the calling worker thread.
    fn run(self);
}

/// Per-thread setup of the pool's workers: what lets a worker run Python jobs (the porto-py
/// crate creates a `PyThreadState` that lives as long as the thread, `25` P0).
pub trait WorkerHooks: Send + Sync + 'static {
    /// Called on a new worker thread before its first job. The value it returns is kept until
    /// the thread exits, and dropped on that thread.
    fn thread_start(&self) -> Box<dyn Any>;
}

/// How the pool is sized.
#[derive(Clone)]
pub(crate) struct PoolConfig {
    /// The most worker threads at a time (at least 1).
    pub(crate) max_threads: usize,
    /// How long an idle worker waits for a job before it exits.
    pub(crate) keepalive: Duration,
    pub(crate) hooks: Option<std::sync::Arc<dyn WorkerHooks>>,
}

/// What the pool's gauges read (`23-observability.md`: `blocking_threads`,
/// `blocking_queue_depth`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockingStats {
    /// Worker threads alive, busy or idle.
    pub threads: usize,
    /// Workers waiting for a job.
    pub idle: usize,
    /// Jobs waiting for a worker.
    pub queued: usize,
}

/// A job could not be queued.
pub(crate) enum SubmitError<J> {
    /// The pool was shut down; the job comes back so the caller drops it (which cancels it)
    /// outside any lock.
    Closed(J),
    /// The OS refused a thread and no worker exists to run the job; it comes back too.
    NoThread(J, io::Error),
}

impl<J> fmt::Debug for SubmitError<J> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed(_) => f.write_str("Closed"),
            Self::NoThread(_, error) => write!(f, "NoThread({error})"),
        }
    }
}

struct State<J> {
    /// Queued jobs, each with the number `submit` gave it: a submit whose worker could not be
    /// started takes back its own job by that number.
    queue: VecDeque<(u64, J)>,
    /// Numbers the jobs.
    submitted: u64,
    /// Workers that can take a job: counted when the thread is made, uncounted in the same
    /// critical section in which it decides to leave (so a submit never counts on a worker that
    /// is on its way out).
    threads: usize,
    /// Worker threads that exist: `threads`, and those that decided to leave and are still
    /// tearing down their per-thread state. What shutdown waits for.
    alive: usize,
    /// Workers inside the condition wait.
    idle: usize,
    closed: bool,
    /// Numbers the workers' threads.
    spawned: u64,
}

pub(crate) struct Pool<J: Job> {
    state: Mutex<State<J>>,
    /// Idle workers wait here for a job, or for the pool to close.
    work: Condvar,
    /// `shutdown` waits here for the workers to leave.
    exited: Condvar,
    config: PoolConfig,
    /// Set when a shutdown gave up waiting for the workers: they are detached, and must leave
    /// Python alone (the interpreter may be finalizing) instead of tearing their state down.
    abandoned: AtomicBool,
    /// Makes `start_worker` fail or stall, for tests of the thread-exhaustion path.
    #[cfg(test)]
    pub(crate) spawn_override: std::sync::Mutex<Option<SpawnOverride>>,
}

/// What a test makes of a thread start: its result.
#[cfg(test)]
pub(crate) type SpawnOverride = std::sync::Arc<dyn Fn() -> io::Result<()> + Send + Sync>;

impl<J: Job> Pool<J> {
    pub(crate) fn new(config: PoolConfig) -> Arc<Self> {
        assert!(config.max_threads >= 1, "a pool needs at least one thread");
        Arc::new(Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                submitted: 0,
                threads: 0,
                alive: 0,
                idle: 0,
                closed: false,
                spawned: 0,
            }),
            work: Condvar::new(),
            exited: Condvar::new(),
            config,
            abandoned: AtomicBool::new(false),
            #[cfg(test)]
            spawn_override: std::sync::Mutex::new(None),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State<J>> {
        // Only the accounting runs under the lock, and a job's panic is caught outside it.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The gauges, as of this instant.
    pub(crate) fn stats(&self) -> BlockingStats {
        let state = self.lock();
        BlockingStats {
            threads: state.threads,
            idle: state.idle,
            queued: state.queue.len(),
        }
    }

    /// Queues `job`, waking an idle worker or starting a thread for it.
    pub(crate) fn submit(this: &Arc<Self>, job: J) -> Result<(), SubmitError<J>> {
        let mut state = this.lock();
        if state.closed {
            return Err(SubmitError::Closed(job));
        }
        state.submitted += 1;
        let id = state.submitted;
        state.queue.push_back((id, job));
        if state.queue.len() <= state.idle || state.threads >= this.config.max_threads {
            // The idle workers can take everything queued, or no thread may be added: the
            // workers pick the queue up as they finish. Waking one is harmless when none waits.
            drop(state);
            this.work.notify_one();
            return Ok(());
        }
        state.threads += 1;
        state.alive += 1;
        state.spawned += 1;
        let number = state.spawned;
        drop(state);
        let Err(error) = Self::start_worker(this, number) else {
            return Ok(());
        };
        let mut state = this.lock();
        state.threads -= 1;
        state.alive -= 1;
        if state.threads > 0 {
            // A worker is alive (or starting) and looks at the queue again before it leaves:
            // the job runs when one is free.
            return Ok(());
        }
        // No worker, and none can be started now: nothing would ever run a queued job. This
        // submitter's own job goes back to it (unless it was run or cancelled meanwhile), and
        // the jobs queued behind it, whose submitters already returned, are cancelled: they
        // are dropped (outside the lock) and report a cancellation.
        let own = state
            .queue
            .iter()
            .position(|(queued, _)| *queued == id)
            .and_then(|at| state.queue.remove(at))
            .map(|(_, job)| job);
        let stranded = std::mem::take(&mut state.queue);
        drop(state);
        drop(stranded);
        match own {
            Some(job) => Err(SubmitError::NoThread(job, error)),
            None => Ok(()),
        }
    }

    fn start_worker(this: &Arc<Self>, number: u64) -> io::Result<()> {
        #[cfg(test)]
        {
            // Cloned out: the override may stall (a test holds two starts at a barrier).
            let spawn = this
                .spawn_override
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(spawn) = spawn {
                return spawn();
            }
        }
        let pool = this.clone();
        let builder = thread::Builder::new();
        #[cfg(not(all(loom, test)))]
        let builder = builder.name(format!("porto-blocking-{number}"));
        #[cfg(all(loom, test))]
        let _ = number;
        // Detached: the thread holds the pool until it leaves.
        builder.spawn(move || Self::work_loop(&pool)).map(drop)
    }

    fn work_loop(this: &Arc<Self>) {
        // A hook that panics leaves the thread without its per-thread state, which a job that
        // needs one makes for itself (a Python job attaches through `PyGILState_Ensure`): the
        // worker must still count itself out when it leaves, or its jobs wait forever.
        let thread_state =
            this.config.hooks.as_ref().and_then(|hooks| {
                panic::catch_unwind(AssertUnwindSafe(|| hooks.thread_start())).ok()
            });
        let mut state = this.lock();
        loop {
            if let Some((_, job)) = state.queue.pop_front() {
                drop(state);
                // A panic in a job must not take the worker with it: a spawn_blocking job
                // reports it through its join handle, and a detached one has no reader.
                drop(panic::catch_unwind(AssertUnwindSafe(|| job.run())));
                state = this.lock();
                continue;
            }
            if state.closed {
                break;
            }
            state.idle += 1;
            let (woken, timed_out) = this.wait_for_work(state);
            state = woken;
            state.idle -= 1;
            if timed_out && state.queue.is_empty() {
                break;
            }
        }
        // Out of the count in the critical section that decided it: a job submitted from here
        // on sees no worker here and gets a thread of its own.
        state.threads -= 1;
        drop(state);
        // The per-thread state goes before the worker counts as gone for shutdown (`alive`): a
        // shutdown that sees no worker left must not have one still tearing its thread state down (the Python
        // bindings take the interpreter back for that, and may be finalizing it next). A
        // shutdown that gave up waiting (the thread was detached) leaves the state alone and
        // leaks it: finalization deletes it, and touching it from a detached thread would race
        // that. What stays possible is a worker that began the teardown before the shutdown gave
        // up and is then blocked on the interpreter; the hook's own drop skips Python once
        // the interpreter is finalizing.
        if let Some(thread_state) = thread_state {
            if this.abandoned.load(Ordering::Acquire) {
                std::mem::forget(thread_state);
            } else {
                drop(panic::catch_unwind(AssertUnwindSafe(|| drop(thread_state))));
            }
        }
        let mut state = this.lock();
        state.alive -= 1;
        let last = state.alive == 0;
        drop(state);
        if last {
            this.exited.notify_all();
        }
    }

    /// Waits on `work` for a notify or the keepalive; returns whether the keepalive passed.
    #[cfg(not(all(loom, test)))]
    fn wait_for_work<'a>(
        &'a self,
        state: MutexGuard<'a, State<J>>,
    ) -> (MutexGuard<'a, State<J>>, bool) {
        let (state, result) = self
            .work
            .wait_timeout(state, self.config.keepalive)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state, result.timed_out())
    }

    /// loom has no clock: a zero keepalive stands for a timeout that fires at any point (the
    /// guard is released for a moment, so other threads run first), anything else waits for a
    /// notify.
    #[cfg(all(loom, test))]
    fn wait_for_work<'a>(
        &'a self,
        state: MutexGuard<'a, State<J>>,
    ) -> (MutexGuard<'a, State<J>>, bool) {
        if self.config.keepalive.is_zero() {
            drop(state);
            thread::yield_now();
            return (self.lock(), true);
        }
        (
            self.work
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            false,
        )
    }

    /// Stops the pool: queued jobs that no worker took are dropped (cancelled), idle workers
    /// leave, busy ones when their job ends. Waits until every worker is gone, but for at most
    /// `wait`; returns whether they all left (the others are detached and exit on their own).
    #[cfg(not(all(loom, test)))]
    pub(crate) fn shutdown(&self, wait: Duration) -> bool {
        let cancelled = self.close();
        // Dropped outside the lock: dropping a job cancels its task, which may run code.
        drop(cancelled);
        let state = self.lock();
        let (state, _) = self
            .exited
            .wait_timeout_while(state, wait, |state| state.alive > 0)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let all_left = state.alive == 0;
        if !all_left {
            // Set under the lock a leaving worker takes after its check: see `work_loop`.
            self.abandoned.store(true, Ordering::Release);
        }
        all_left
    }

    /// loom has no clock: waits for the workers without a limit.
    #[cfg(all(loom, test))]
    pub(crate) fn shutdown(&self, _wait: Duration) -> bool {
        drop(self.close());
        let mut state = self.lock();
        while state.alive > 0 {
            state = self
                .exited
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        true
    }

    /// Closes the pool to new jobs and takes the queued ones; wakes every idle worker.
    pub(crate) fn close(&self) -> VecDeque<J> {
        let mut state = self.lock();
        state.closed = true;
        let queued = std::mem::take(&mut state.queue);
        drop(state);
        self.work.notify_all();
        queued.into_iter().map(|(_, job)| job).collect()
    }
}

impl<J: Job> fmt::Debug for Pool<J> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        f.debug_struct("Pool")
            .field("threads", &state.threads)
            .field("idle", &state.idle)
            .field("queued", &state.queue.len())
            .field("closed", &state.closed)
            .finish_non_exhaustive()
    }
}
