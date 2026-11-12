//! The blocking pool and `spawn_blocking` through a real runtime: results come back through the
//! join handle and the lane's wake path, the pool grows with the load and shrinks when idle,
//! and shutdown cancels what has not started.
//!
//! Timing assertions have generous limits: CI machines are slow and loaded.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::blocking::WorkerHooks;
use crate::sched::{Handle, Runtime, spawn_blocking};
use crate::task::JoinHandle;

fn runtime(threads: usize, keepalive: Duration) -> Runtime {
    Runtime::builder()
        .blocking_threads(threads)
        .blocking_keepalive(keepalive)
        .build()
}

/// The message of a panic payload (`&str` or `String`).
fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
        })
        .unwrap_or_default()
}

/// Polls `condition` for up to 20 s.
fn eventually(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn a_job_runs_on_a_worker_thread_and_its_result_reaches_the_awaiting_task() {
    let rt = runtime(4, Duration::from_secs(10));
    let lane = thread::current().id();
    let result = rt.block_on(async {
        spawn_blocking(move || {
            let worker = thread::current();
            (
                worker.id() != lane,
                worker.name().unwrap_or_default().to_owned(),
                crate::sched::Handle::try_current().is_none(),
            )
        })
        .await
        .unwrap()
    });
    let (off_the_lane, name, no_runtime_here) = result;
    assert!(off_the_lane, "the job ran on the lane");
    assert!(name.starts_with("porto-blocking-"), "thread name {name:?}");
    assert!(no_runtime_here, "a worker is not a lane");
}

#[test]
fn a_task_on_the_lane_keeps_running_while_a_job_blocks() {
    let rt = runtime(4, Duration::from_secs(10));
    let (release, wait) = mpsc::channel::<()>();
    let started = Arc::new(AtomicBool::new(false));
    let seen = started.clone();
    let handle = rt.handle().clone();
    let outcome = rt.block_on(async move {
        let job = handle.spawn_blocking(move || {
            seen.store(true, Ordering::Release);
            wait.recv().unwrap();
            7
        });
        // The lane is free: it runs this task while the job waits for the release.
        let ticker = handle.spawn(async move {
            let mut ticks = 0;
            while !started.load(Ordering::Acquire) {
                crate::time::sleep_until(crate::time::Deadline::after(Duration::from_millis(1)))
                    .await;
                ticks += 1;
            }
            release.send(()).unwrap();
            ticks
        });
        (job.await.unwrap(), ticker.await.unwrap())
    });
    assert_eq!(outcome.0, 7);
}

#[test]
fn a_panic_in_a_job_is_the_join_errors_and_the_worker_survives() {
    let rt = runtime(1, Duration::from_secs(10));
    let handle = rt.handle().clone();
    rt.block_on(async move {
        let error = handle
            .spawn_blocking(|| -> u32 { panic!("boom in the job") })
            .await
            .expect_err("a panic is an error");
        assert!(error.is_panic());
        let payload = error.into_panic();
        assert_eq!(
            payload.downcast_ref::<&str>().copied(),
            Some("boom in the job")
        );
        // The one worker is still there to take the next job.
        assert_eq!(handle.spawn_blocking(|| 5).await.unwrap(), 5);
        assert_eq!(handle.blocking_stats().threads, 1);
    });
}

#[test]
fn the_pool_grows_under_100_concurrent_jobs_and_shrinks_after_the_keepalive() {
    const JOBS: usize = 100;
    let keepalive = Duration::from_millis(150);
    let rt = runtime(512, keepalive);
    let handle = rt.handle().clone();
    // Every job waits for the other 99 at the barrier: they are all running at once only if
    // the pool started 100 threads.
    let barrier = Arc::new(Barrier::new(JOBS));
    let peak = Arc::new(AtomicUsize::new(0));
    rt.block_on({
        let handle = handle.clone();
        async move {
            let jobs: Vec<JoinHandle<usize>> = (0..JOBS)
                .map(|number| {
                    let barrier = barrier.clone();
                    let peak = peak.clone();
                    let handle = handle.clone();
                    handle.clone().spawn_blocking(move || {
                        barrier.wait();
                        peak.fetch_max(handle.blocking_stats().threads, Ordering::Relaxed);
                        number
                    })
                })
                .collect();
            let mut total = 0;
            for job in jobs {
                total += job.await.unwrap();
            }
            assert_eq!(total, (0..JOBS).sum::<usize>());
            assert!(
                peak.load(Ordering::Relaxed) >= JOBS,
                "the pool peaked at {} threads, not {JOBS}",
                peak.load(Ordering::Relaxed)
            );
        }
    });
    // Idle for the keepalive: every worker leaves.
    eventually("the idle workers to exit", || {
        handle.blocking_stats().threads == 0
    });
    assert_eq!(handle.blocking_stats().idle, 0);
}

#[test]
fn the_pool_never_runs_more_threads_than_its_limit_and_queues_the_rest() {
    const LIMIT: usize = 3;
    const JOBS: usize = 30;
    let rt = runtime(LIMIT, Duration::from_secs(10));
    let handle = rt.handle().clone();
    let running = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));
    let total = rt.block_on({
        let handle = handle.clone();
        async move {
            let jobs: Vec<_> = (0..JOBS)
                .map(|number| {
                    let running = running.clone();
                    let most = most.clone();
                    handle.spawn_blocking(move || {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        most.fetch_max(now, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(5));
                        running.fetch_sub(1, Ordering::SeqCst);
                        number
                    })
                })
                .collect();
            let mut total = 0;
            for job in jobs {
                total += job.await.unwrap();
            }
            assert!(most.load(Ordering::SeqCst) <= LIMIT);
            assert!(handle.blocking_stats().threads <= LIMIT);
            total
        }
    });
    assert_eq!(total, (0..JOBS).sum::<usize>(), "a queued job was lost");
}

#[test]
fn an_idle_worker_is_reused_instead_of_starting_another_thread() {
    let rt = runtime(8, Duration::from_secs(10));
    let handle = rt.handle().clone();
    rt.block_on(async move {
        let first = handle
            .spawn_blocking(|| thread::current().id())
            .await
            .unwrap();
        // The worker has to be idle again before the next job, or it would not be reusable.
        eventually("the worker to idle", || handle.blocking_stats().idle == 1);
        let second = handle
            .spawn_blocking(|| thread::current().id())
            .await
            .unwrap();
        assert_eq!(first, second, "a second thread was started");
        assert_eq!(handle.blocking_stats().threads, 1);
    });
}

#[test]
fn a_zero_keepalive_lets_a_worker_leave_as_soon_as_it_is_idle() {
    let rt = runtime(8, Duration::ZERO);
    let handle = rt.handle().clone();
    rt.block_on({
        let handle = handle.clone();
        async move {
            assert_eq!(handle.spawn_blocking(|| 1).await.unwrap(), 1);
        }
    });
    eventually("the worker to leave", || {
        handle.blocking_stats().threads == 0
    });
}

#[test]
fn dropping_the_join_handle_detaches_the_job_which_still_runs() {
    let rt = runtime(2, Duration::from_secs(10));
    let ran = Arc::new(AtomicBool::new(false));
    let seen = ran.clone();
    drop(
        rt.handle()
            .spawn_blocking(move || seen.store(true, Ordering::Release)),
    );
    eventually("the detached job to run", || ran.load(Ordering::Acquire));
}

#[test]
fn aborting_a_job_that_has_not_started_cancels_it() {
    let rt = runtime(1, Duration::from_secs(10));
    let handle = rt.handle().clone();
    let (release, wait) = mpsc::channel::<()>();
    let (started, started_rx) = mpsc::channel::<()>();
    let blocker = handle.spawn_blocking(move || {
        started.send(()).unwrap();
        wait.recv().unwrap();
    });
    started_rx.recv().unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let seen = ran.clone();
    let queued = handle.spawn_blocking(move || seen.store(true, Ordering::Release));
    assert_eq!(handle.blocking_stats().queued, 1);
    queued.abort();
    release.send(()).unwrap();
    rt.block_on(async {
        blocker.await.unwrap();
        let error = queued.await.expect_err("it was aborted");
        assert!(error.is_cancelled());
    });
    assert!(!ran.load(Ordering::Acquire), "an aborted job ran");
}

#[test]
fn dropping_the_runtime_cancels_queued_jobs_and_leaves_a_running_one_to_finish() {
    let rt = runtime(1, Duration::from_secs(10));
    let handle = rt.handle().clone();
    let (release, wait) = mpsc::channel::<()>();
    let (started, started_rx) = mpsc::channel::<()>();
    let running = handle.spawn_blocking(move || {
        started.send(()).unwrap();
        wait.recv().unwrap();
        "finished"
    });
    started_rx.recv().unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let seen = ran.clone();
    let queued = handle.spawn_blocking(move || seen.store(true, Ordering::Release));
    // The runtime waits up to a second for the running job, then drops on: release it from
    // another thread only after the wait began, to see the drop return without it too.
    let begun = Instant::now();
    drop(rt);
    assert!(
        begun.elapsed() < Duration::from_secs(10),
        "shutdown waited for a job that cannot be interrupted"
    );
    // A new job after shutdown is cancelled at once.
    let late = handle.spawn_blocking(|| 1);
    release.send(()).unwrap();
    let other = Runtime::new();
    other.block_on(async {
        assert!(queued.await.expect_err("queued at shutdown").is_cancelled());
        assert!(
            late.await
                .expect_err("spawned after shutdown")
                .is_cancelled()
        );
        // The running job finished and kept its result.
        assert_eq!(running.await.unwrap(), "finished");
    });
    assert!(
        !ran.load(Ordering::Acquire),
        "a queued job ran after shutdown"
    );
}

#[test]
fn shutdown_waits_for_a_job_that_finishes_within_the_second() {
    let rt = runtime(1, Duration::from_secs(10));
    let finished = Arc::new(AtomicBool::new(false));
    let seen = finished.clone();
    let (started, started_rx) = mpsc::channel::<()>();
    let job = rt.handle().spawn_blocking(move || {
        started.send(()).unwrap();
        thread::sleep(Duration::from_millis(150));
        seen.store(true, Ordering::Release);
    });
    started_rx.recv().unwrap();
    drop(rt);
    assert!(
        finished.load(Ordering::Acquire),
        "the runtime returned before a 150 ms job finished"
    );
    drop(job);
}

#[test]
fn spawn_blocking_outside_a_runtime_says_what_to_do() {
    let error = std::panic::catch_unwind(|| spawn_blocking(|| 1)).expect_err("no runtime");
    let message = panic_message(&*error);
    assert!(
        message.contains("outside a runtime") && message.contains("Handle::spawn_blocking"),
        "{message}"
    );
}

/// Counts what the hooks saw.
struct Hooks {
    started: Arc<AtomicUsize>,
    ended: Arc<AtomicUsize>,
}

struct OnExit(Arc<AtomicUsize>);

impl Drop for OnExit {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl WorkerHooks for Hooks {
    fn thread_start(&self) -> Box<dyn Any> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Box::new(OnExit(self.ended.clone()))
    }
}

#[test]
fn the_hooks_run_once_per_thread_and_their_value_lives_until_the_thread_exits() {
    let started = Arc::new(AtomicUsize::new(0));
    let ended = Arc::new(AtomicUsize::new(0));
    let rt = Runtime::builder()
        .blocking_keepalive(Duration::ZERO)
        .blocking_hooks(Hooks {
            started: started.clone(),
            ended: ended.clone(),
        })
        .build();
    let handle: Handle = rt.handle().clone();
    rt.block_on(async move {
        for _ in 0..3 {
            handle.spawn_blocking(|| ()).await.unwrap();
        }
    });
    drop(rt);
    let threads = started.load(Ordering::SeqCst);
    assert!(threads >= 1, "the hooks never ran");
    assert_eq!(
        ended.load(Ordering::SeqCst),
        threads,
        "a thread's value outlived its thread"
    );
}

#[test]
fn blocking_threads_zero_is_a_panic_that_says_what_to_pass() {
    let error = std::panic::catch_unwind(|| Runtime::builder().blocking_threads(0))
        .expect_err("zero threads");
    let message = panic_message(&*error);
    assert!(message.contains("at least 1"), "{message}");
}

#[test]
fn jobs_can_use_the_blocking_methods_of_the_sync_primitives() {
    // ST-17: a pool thread, unlike a lane, may block on the runtime's primitives.
    let rt = runtime(2, Duration::from_secs(10));
    let handle = rt.handle().clone();
    let event = Arc::new(crate::sync::Event::new());
    let waiter = event.clone();
    let job = handle.spawn_blocking(move || waiter.blocking_wait().is_ok());
    let seen = Arc::new(Mutex::new(false));
    rt.block_on(async {
        event.set();
        *seen.lock().unwrap() = job.await.unwrap();
    });
    assert!(*seen.lock().unwrap());
}

mod pool_accounting {
    //! The pool's thread-exhaustion path and its hooks, on a `Pool` of its own with a thread
    //! start that fails on demand.

    use std::io;
    use std::sync::Barrier;

    use super::*;
    use crate::blocking::pool::{Job, Pool, PoolConfig, SubmitError};

    /// Records that it ran or was dropped unrun.
    struct Probe {
        ran: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
        did_run: bool,
    }

    impl Probe {
        fn new(ran: &Arc<AtomicUsize>, dropped: &Arc<AtomicUsize>) -> Self {
            Self {
                ran: ran.clone(),
                dropped: dropped.clone(),
                did_run: false,
            }
        }
    }

    impl Job for Probe {
        fn run(mut self) {
            self.did_run = true;
            self.ran.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            if !self.did_run {
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    fn pool(max_threads: usize, hooks: Option<Arc<dyn WorkerHooks>>) -> Arc<Pool<Probe>> {
        Pool::new(PoolConfig {
            max_threads,
            keepalive: Duration::from_secs(10),
            hooks,
        })
    }

    fn refuse_threads(pool: &Pool<Probe>) {
        *pool.spawn_override.lock().unwrap() =
            Some(Arc::new(|| Err(io::Error::other("no threads left"))));
    }

    #[test]
    fn a_job_whose_thread_cannot_start_goes_back_to_its_submitter_and_nothing_stays_queued() {
        let (ran, dropped) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let pool = pool(4, None);
        refuse_threads(&pool);
        let error = Pool::submit(&pool, Probe::new(&ran, &dropped)).expect_err("no thread");
        assert!(matches!(error, SubmitError::NoThread(..)));
        drop(error);
        let stats = pool.stats();
        assert_eq!((stats.threads, stats.queued), (0, 0));
        assert_eq!(dropped.load(Ordering::SeqCst), 1, "returned, then dropped");
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_failed_start_leaves_the_job_queued_for_the_worker_that_is_alive() {
        let (ran, dropped) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let pool = pool(4, None);
        Pool::submit(&pool, Probe::new(&ran, &dropped)).unwrap();
        eventually("the first job to run", || ran.load(Ordering::SeqCst) == 1);
        eventually("the worker to idle", || pool.stats().idle == 1);
        // The worker is idle and alive: a job that needs a second thread, which cannot start,
        // stays queued and the idle worker takes it.
        refuse_threads(&pool);
        for _ in 0..3 {
            Pool::submit(&pool, Probe::new(&ran, &dropped)).expect("a worker is alive");
        }
        eventually("every job to run", || ran.load(Ordering::SeqCst) == 4);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert!(pool.shutdown(Duration::from_secs(5)));
    }

    #[test]
    fn two_submitters_that_both_fail_leave_no_job_stranded() {
        let (ran, dropped) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let pool = pool(4, None);
        // Both submitters queue their job and count a thread before either one's start
        // fails: the second failure sees no worker and must clear what the first left.
        let barrier = Arc::new(Barrier::new(2));
        let at = barrier.clone();
        *pool.spawn_override.lock().unwrap() = Some(Arc::new(move || {
            at.wait();
            Err(io::Error::other("no threads left"))
        }));
        let outcomes: Vec<_> = (0..2)
            .map(|_| {
                let (pool, ran, dropped) = (pool.clone(), ran.clone(), dropped.clone());
                thread::spawn(move || Pool::submit(&pool, Probe::new(&ran, &dropped)).is_err())
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|submitter| submitter.join().unwrap())
            .collect();
        assert!(
            outcomes.iter().any(|&failed| failed),
            "one hears of the failure"
        );
        let stats = pool.stats();
        assert_eq!((stats.threads, stats.queued), (0, 0), "a job is stranded");
        // Each job was returned (and dropped by the test) or cancelled: none is waiting.
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }

    struct PanickingHooks;

    impl WorkerHooks for PanickingHooks {
        fn thread_start(&self) -> Box<dyn Any> {
            panic!("the hook failed");
        }
    }

    #[test]
    fn a_hook_that_panics_costs_the_worker_its_state_not_its_place_in_the_count() {
        let (ran, dropped) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let pool = pool(1, Some(Arc::new(PanickingHooks)));
        for _ in 0..3 {
            Pool::submit(&pool, Probe::new(&ran, &dropped)).unwrap();
        }
        eventually("the jobs to run", || ran.load(Ordering::SeqCst) == 3);
        assert!(pool.shutdown(Duration::from_secs(5)), "the worker left");
        let stats = pool.stats();
        assert_eq!((stats.threads, stats.idle, stats.queued), (0, 0, 0));
    }

    /// A hook whose value must be gone before the worker counts as left.
    struct SlowTeardown(Arc<AtomicBool>);

    struct TearDown(Arc<AtomicBool>);

    impl Drop for TearDown {
        fn drop(&mut self) {
            thread::sleep(Duration::from_millis(100));
            self.0.store(true, Ordering::SeqCst);
        }
    }

    impl WorkerHooks for SlowTeardown {
        fn thread_start(&self) -> Box<dyn Any> {
            Box::new(TearDown(self.0.clone()))
        }
    }

    #[test]
    fn shutdown_returns_only_after_the_workers_tore_down_their_thread_state() {
        let (ran, dropped) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let torn_down = Arc::new(AtomicBool::new(false));
        let pool = pool(1, Some(Arc::new(SlowTeardown(torn_down.clone()))));
        Pool::submit(&pool, Probe::new(&ran, &dropped)).unwrap();
        eventually("the job to run", || ran.load(Ordering::SeqCst) == 1);
        assert!(pool.shutdown(Duration::from_secs(5)));
        assert!(
            torn_down.load(Ordering::SeqCst),
            "shutdown reported the worker gone while it was still tearing its state down"
        );
    }
}
