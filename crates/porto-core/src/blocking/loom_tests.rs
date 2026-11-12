//! loom models of the blocking pool's queue and thread accounting (`pool.rs`): a submitted job
//! is run exactly once whichever way submits, idle exits (keepalive) and shutdown interleave
//! with it, and the pool ends with no thread and no idle worker counted.
//!
//! loom has no clock: a keepalive of zero stands for a timeout that fires at any point
//! (`Pool::wait_for_work`), anything else waits for a notify. The models with more than two
//! threads run under a preemption bound of 3 (`LOOM_MAX_PREEMPTIONS` overrides it).
//!
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release blocking`

use std::time::Duration;

use loom::model::Builder;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Condvar, Mutex};
use loom::thread;

use super::pool::{Job, Pool, PoolConfig, SubmitError};

/// Counts the jobs that ran, and lets the test wait for them.
struct Done {
    ran: Mutex<usize>,
    changed: Condvar,
    /// Jobs handed back unrun (closed pool).
    refused: AtomicUsize,
}

impl Done {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            ran: Mutex::new(0),
            changed: Condvar::new(),
            refused: AtomicUsize::new(0),
        })
    }

    fn wait_for(&self, jobs: usize) {
        let mut ran = self.ran.lock().unwrap();
        while *ran < jobs {
            ran = self.changed.wait(ran).unwrap();
        }
    }

    fn ran(&self) -> usize {
        *self.ran.lock().unwrap()
    }
}

struct Counted(Arc<Done>);

impl Job for Counted {
    fn run(self) {
        *self.0.ran.lock().unwrap() += 1;
        self.0.changed.notify_all();
    }
}

fn pool(max_threads: usize, keepalive: Duration) -> Arc<Pool<Counted>> {
    Pool::new(PoolConfig {
        max_threads,
        keepalive,
        hooks: None,
    })
}

/// `Builder` with the bound the three-thread models need.
fn bounded(preemptions: usize) -> Builder {
    let mut builder = Builder::new();
    if builder.preemption_bound.is_none() {
        builder.preemption_bound = Some(preemptions);
    }
    builder
}

fn submit(pool: &Arc<Pool<Counted>>, done: &Arc<Done>) {
    match Pool::submit(pool, Counted(done.clone())) {
        Ok(()) => {}
        Err(SubmitError::Closed(_)) => {
            done.refused.fetch_add(1, Ordering::Relaxed);
        }
        Err(SubmitError::NoThread(..)) => unreachable!("loom threads start"),
    }
}

fn finish(pool: &Arc<Pool<Counted>>) {
    assert!(pool.shutdown(Duration::from_secs(1)), "every worker left");
    let stats = pool.stats();
    assert_eq!(stats.threads, 0, "a worker is still counted");
    assert_eq!(stats.idle, 0, "an idle worker is still counted");
    assert_eq!(stats.queued, 0, "a job is still queued");
}

#[test]
fn a_job_submitted_while_the_worker_idles_runs_once() {
    // A non-zero keepalive: the worker leaves only on shutdown, so the second job finds it
    // idle, or still finishing the first.
    bounded(3).check(|| {
        let pool = pool(1, Duration::from_secs(1));
        let done = Done::new();
        submit(&pool, &done);
        submit(&pool, &done);
        done.wait_for(2);
        assert_eq!(done.ran(), 2);
        finish(&pool);
        assert_eq!(done.ran(), 2, "a job ran twice");
    });
}

#[test]
fn a_worker_that_times_out_never_strands_a_job() {
    // keepalive 0: the idle worker's wait times out at any point, including just as the
    // second job is pushed. The job must be seen by the worker before it retires, or by a
    // thread started for it.
    bounded(3).check(|| {
        let pool = pool(1, Duration::ZERO);
        let done = Done::new();
        submit(&pool, &done);
        done.wait_for(1);
        submit(&pool, &done);
        done.wait_for(2);
        finish(&pool);
        assert_eq!(done.ran(), 2);
    });
}

#[test]
fn two_submitters_get_every_job_run_with_room_for_both() {
    bounded(2).check(|| {
        let pool = pool(2, Duration::from_secs(1));
        let done = Done::new();
        let other = {
            let (pool, done) = (pool.clone(), done.clone());
            thread::spawn(move || submit(&pool, &done))
        };
        submit(&pool, &done);
        other.join().unwrap();
        done.wait_for(2);
        finish(&pool);
        assert_eq!(done.ran(), 2);
    });
}

#[test]
fn a_full_pool_queues_the_job_for_the_worker_it_has() {
    // One thread allowed, two jobs, submitted back to back: the second waits in the queue and
    // the one worker takes it.
    bounded(2).check(|| {
        let pool = pool(1, Duration::ZERO);
        let done = Done::new();
        submit(&pool, &done);
        submit(&pool, &done);
        done.wait_for(2);
        finish(&pool);
        assert_eq!(done.ran(), 2);
    });
}

#[test]
fn shutdown_either_runs_a_job_or_hands_it_back_never_loses_it() {
    bounded(3).check(|| {
        let pool = pool(1, Duration::from_secs(1));
        let done = Done::new();
        let submitter = {
            let (pool, done) = (pool.clone(), done.clone());
            thread::spawn(move || submit(&pool, &done))
        };
        // Cancelled jobs are dropped by the closing side, run ones by a worker.
        let cancelled = pool.close();
        let cancelled = cancelled.len();
        submitter.join().unwrap();
        finish(&pool);
        let ran = done.ran();
        let refused = done.refused.load(Ordering::Relaxed);
        assert_eq!(
            ran + refused + cancelled,
            1,
            "ran {ran}, refused {refused}, cancelled {cancelled}"
        );
    });
}
