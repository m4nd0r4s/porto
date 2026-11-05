//! Tests of the lane loop (`12-scheduler-and-lanes.md` §3, §12): injector fairness, the
//! maintenance tick (driver poll, timers, deferred work) and parking in the driver.

use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use porto_io::{Driver, DriverKind, DriverWaker, Interest, RawSource, RegistrationId};

use super::context::current_tick;
use super::lane::GLOBAL_QUEUE_INTERVAL;
use super::{Runtime, spawn};
use crate::time::{Deadline, sleep_until};

/// Pending once, waking itself while running: one round trip through the run queue.
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn yield_now() -> YieldNow {
    YieldNow(false)
}

/// A one-way flag a task (or thread) sets and a future awaits.
#[derive(Clone, Default)]
struct Signal(Arc<Mutex<(bool, Option<Waker>)>>);

impl Signal {
    fn set(&self) {
        let waker = {
            let mut inner = self.0.lock().unwrap();
            inner.0 = true;
            inner.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let signal = self.clone();
        poll_fn(move |cx| {
            let mut inner = signal.0.lock().unwrap();
            if inner.0 {
                Poll::Ready(())
            } else {
                inner.1 = Some(cx.waker().clone());
                Poll::Pending
            }
        })
    }
}

fn iterations(normal: usize) -> usize {
    if cfg!(miri) { 3 } else { normal }
}

fn lane_tick() -> u32 {
    current_tick().expect("called from a task on the lane").1
}

/// Spawns `busy` tasks that wake themselves until `stop` is set: the local queue never runs dry.
/// Each gives up after 100k yields, so a starvation bug fails a test instead of hanging it.
fn spawn_busy_tasks(busy: usize, stop: &Arc<AtomicBool>) {
    for _ in 0..busy {
        let stop = stop.clone();
        spawn(async move {
            for _ in 0..100_000 {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                yield_now().await;
            }
        });
    }
}

#[test]
fn self_waking_tasks_do_not_starve_the_injector() {
    let rt = Runtime::new();
    let handle = rt.handle().clone();
    let rounds = iterations(50);
    let stop = Arc::new(AtomicBool::new(false));
    thread_local! {
        /// The lane tick at which the probe's last injector push landed.
        static PUSHED_AT: Cell<u32> = const { Cell::new(0) };
    }
    // Ticks each injected task waited, from its push to its first poll.
    let waits = Arc::new(Mutex::new(Vec::new()));
    // The helper thread pushes a task onto the injector whenever the probe asks, then says so.
    let (ask, asked) = mpsc::channel::<mpsc::Sender<()>>();
    let helper = {
        let waits = waits.clone();
        thread::spawn(move || {
            for pushed in asked {
                let waits = waits.clone();
                handle.spawn(async move {
                    let wait = lane_tick().wrapping_sub(PUSHED_AT.get());
                    waits.lock().unwrap().push(wait);
                });
                pushed.send(()).unwrap();
            }
        })
    };

    let measured = Signal::default();
    {
        let (stop, measured, waits) = (stop.clone(), measured.clone(), waits.clone());
        rt.spawn(async move {
            spawn_busy_tasks(8, &stop);
            for _ in 0..rounds {
                let before = waits.lock().unwrap().len();
                let (done, pushed) = mpsc::channel();
                ask.send(done).unwrap();
                // Blocks the lane until the task is on the injector, so its wait starts now.
                pushed.recv().unwrap();
                PUSHED_AT.set(lane_tick());
                // Bounded, so a starved injector fails the test instead of hanging it.
                for _ in 0..10_000 {
                    if waits.lock().unwrap().len() > before {
                        break;
                    }
                    yield_now().await;
                }
            }
            stop.store(true, Ordering::Relaxed);
            measured.set();
        });
    }
    rt.block_on(measured.wait());
    // Lets a starved task run, so its wait is recorded.
    rt.run_until_idle();
    helper.join().unwrap();
    let waits = waits.lock().unwrap();
    assert_eq!(waits.len(), rounds);
    let worst = *waits.iter().max().unwrap();
    // The spec's bound is 2 x GLOBAL_QUEUE_INTERVAL; with one injected task at a time and the
    // local queue never empty, the loop does better: the next injector-first tick.
    assert!(
        worst <= GLOBAL_QUEUE_INTERVAL,
        "an injected task waited {worst} ticks behind self-waking tasks (limit {GLOBAL_QUEUE_INTERVAL}, \
         spec bound {}): {waits:?}",
        2 * GLOBAL_QUEUE_INTERVAL
    );
}

#[test]
fn a_rust_only_runtime_runs_a_million_trivial_tasks() {
    let tasks = if cfg!(miri) { 100 } else { 1_000_000 };
    let rt = Runtime::new();
    let done = Signal::default();
    let left = Arc::new(AtomicUsize::new(tasks));
    let started = Instant::now();
    {
        let (done, left) = (done.clone(), left.clone());
        rt.spawn(async move {
            for _ in 0..tasks {
                let (done, left) = (done.clone(), left.clone());
                spawn(async move {
                    if left.fetch_sub(1, Ordering::Relaxed) == 1 {
                        done.set();
                    }
                });
            }
        });
    }
    rt.block_on(done.wait());
    let elapsed = started.elapsed();
    eprintln!(
        "{tasks} trivial tasks spawned and run in {elapsed:?} ({:.0} ns per task)",
        elapsed.as_nanos() as f64 / tasks as f64
    );
    assert_eq!(left.load(Ordering::Relaxed), 0);
}

#[test]
fn timers_fire_at_maintenance_while_the_lane_never_idles() {
    let rt = Runtime::new();
    let stop = Arc::new(AtomicBool::new(false));
    let slept = {
        let stop = stop.clone();
        rt.spawn(async move {
            spawn_busy_tasks(4, &stop);
            sleep_until(Deadline::after(Duration::from_millis(2))).await;
            stop.store(true, Ordering::Relaxed);
        })
    };
    rt.block_on(slept).unwrap();
}

#[test]
fn deferred_work_runs_on_the_lane_and_unparks_it() {
    let rt = Runtime::new();
    let handle = rt.handle().clone();
    let ran = Signal::default();
    let lane_thread = thread::current().id();
    let ran_on = Arc::new(Mutex::new(None));
    let deferrer = {
        let (ran, ran_on) = (ran.clone(), ran_on.clone());
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(if cfg!(miri) { 0 } else { 5 }));
            handle
                .defer(move || {
                    *ran_on.lock().unwrap() = Some(thread::current().id());
                    ran.set();
                })
                .unwrap_or_else(|_| panic!("the runtime is alive"));
        })
    };
    rt.block_on(ran.wait());
    deferrer.join().unwrap();
    assert_eq!(*ran_on.lock().unwrap(), Some(lane_thread));
}

#[test]
fn deferred_work_runs_when_the_runtime_drops_and_is_handed_back_after() {
    let rt = Runtime::new();
    let handle = rt.handle().clone();
    let ran = Arc::new(AtomicUsize::new(0));
    {
        let ran = ran.clone();
        handle
            .defer(move || {
                ran.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap_or_else(|_| panic!("the runtime is alive"));
    }
    drop(rt);
    assert_eq!(
        ran.load(Ordering::SeqCst),
        1,
        "drained at drop, on the lane"
    );
    let late = handle.defer(|| unreachable!("the runtime is gone"));
    assert!(late.is_err(), "handed back unrun");
}

#[test]
fn run_until_idle_runs_deferred_work() {
    let rt = Runtime::new();
    let ran = Arc::new(AtomicBool::new(false));
    {
        let ran = ran.clone();
        let handle = rt.handle().clone();
        thread::spawn(move || {
            handle
                .defer(move || ran.store(true, Ordering::SeqCst))
                .unwrap_or_else(|_| panic!("the runtime is alive"));
        })
        .join()
        .unwrap();
    }
    rt.run_until_idle();
    assert!(ran.load(Ordering::SeqCst));
}

/// A driver with no OS behind it: counts its turns, and blocks a turn with a timeout until it
/// is woken or the timeout passes.
#[derive(Default)]
struct CountingDriver {
    state: Arc<(Mutex<DriverState>, Condvar)>,
    waker: Option<DriverWaker>,
}

#[derive(Default)]
struct DriverState {
    zero_timeout_turns: usize,
    blocking_turns: usize,
    woken: bool,
    wakes: usize,
}

impl CountingDriver {
    fn new() -> Arc<Self> {
        let state: Arc<(Mutex<DriverState>, Condvar)> = Arc::default();
        let waker_state = state.clone();
        Arc::new(Self {
            state,
            waker: Some(DriverWaker::new(move || {
                let (lock, condvar) = &*waker_state;
                let mut state = lock.lock().unwrap();
                state.woken = true;
                state.wakes += 1;
                condvar.notify_all();
                Ok(())
            })),
        })
    }

    fn snapshot(&self) -> (usize, usize, usize) {
        let state = self.state.0.lock().unwrap();
        (state.zero_timeout_turns, state.blocking_turns, state.wakes)
    }
}

impl Driver for CountingDriver {
    fn register(&self, _src: RawSource, _interest: Interest) -> io::Result<RegistrationId> {
        Err(io::Error::other("not a real driver"))
    }
    fn reregister(&self, _id: RegistrationId, _interest: Interest) -> io::Result<()> {
        Err(io::Error::other("not a real driver"))
    }
    fn deregister(&self, _id: RegistrationId) -> io::Result<()> {
        Err(io::Error::other("not a real driver"))
    }
    fn shutdown(&self) {}
    fn turn(&self, timeout: Option<Duration>) -> io::Result<usize> {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        if timeout == Some(Duration::ZERO) {
            state.zero_timeout_turns += 1;
            return Ok(0);
        }
        state.blocking_turns += 1;
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        while !state.woken {
            state = match deadline {
                None => condvar.wait(state).unwrap(),
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    condvar.wait_timeout(state, left).unwrap().0
                }
            };
        }
        state.woken = false;
        Ok(0)
    }
    fn waker(&self) -> &DriverWaker {
        self.waker.as_ref().unwrap()
    }
    fn kind(&self) -> DriverKind {
        DriverKind::Sim
    }
}

#[test]
fn maintenance_polls_the_driver_without_blocking_every_event_interval() {
    let driver = CountingDriver::new();
    let rt = Runtime::builder().driver(driver.clone()).build();
    let yields = 61 * 20;
    let busy = rt.spawn(async move {
        for _ in 0..yields {
            yield_now().await;
        }
    });
    rt.block_on(busy).unwrap();
    let (zero, blocking, _) = driver.snapshot();
    assert!(
        zero >= 20,
        "{zero} non-blocking polls in {yields} ticks of a busy lane"
    );
    assert_eq!(blocking, 0, "a busy lane never blocks in the driver");
}

#[test]
fn an_idle_lane_parks_in_the_driver_and_a_remote_spawn_wakes_it() {
    let driver = CountingDriver::new();
    let rt = Runtime::builder().driver(driver.clone()).build();
    let handle = rt.handle().clone();
    let done = Signal::default();
    let spawner = {
        let done = done.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            handle.spawn(async move { done.set() });
        })
    };
    rt.block_on(done.wait());
    spawner.join().unwrap();
    let (_, blocking, wakes) = driver.snapshot();
    assert!(blocking >= 1, "the idle lane blocked in the driver");
    assert!(
        wakes >= 1,
        "the remote spawn woke the driver, not the thread"
    );
}

#[test]
fn a_lane_parked_in_the_driver_sleeps_until_its_timer() {
    let driver = CountingDriver::new();
    let rt = Runtime::builder().driver(driver.clone()).build();
    let started = Instant::now();
    let nap = Duration::from_millis(30);
    rt.block_on(async move { sleep_until(Deadline::after(nap)).await });
    assert!(started.elapsed() >= nap);
    let (_, blocking, _) = driver.snapshot();
    // Under miri the sleep is over before the lane first looks for work.
    assert!(
        blocking >= 1 || cfg!(miri),
        "it waited in the driver's turn"
    );
}

// miri cannot run kqueue or epoll.
#[cfg(all(unix, not(miri)))]
#[test]
fn readiness_from_the_os_driver_wakes_a_task() {
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    use porto_io::{Direction, MioDriver};

    let driver = Arc::new(MioDriver::new().unwrap());
    let rt = Runtime::builder().driver(driver.clone()).build();
    let (reader, mut writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let id = driver
        .register(RawSource::new(reader.as_raw_fd()), Interest::READABLE)
        .unwrap();
    let writer_thread = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        writer.write_all(b"x").unwrap();
        writer
    });
    let slab_driver = driver.clone();
    rt.block_on(poll_fn(move |cx| {
        let io = slab_driver.slab().get(id).unwrap();
        io.poll_readiness(cx, id, Direction::Read).map(|_| ())
    }));
    let _writer = writer_thread.join().unwrap();
    driver.deregister(id).unwrap();
}

#[test]
fn a_panicking_deferred_work_does_not_lose_the_rest_of_its_batch() {
    let rt = Runtime::new();
    let ran = Arc::new(AtomicUsize::new(0));
    let handle = rt.handle().clone();
    handle
        .defer(|| panic!("deferred work panicked"))
        .unwrap_or_else(|_| panic!("the runtime is alive"));
    {
        let ran = ran.clone();
        handle
            .defer(move || {
                ran.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap_or_else(|_| panic!("the runtime is alive"));
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rt.run_until_idle()));
    assert!(outcome.is_err(), "the panic goes on");
    assert_eq!(ran.load(Ordering::SeqCst), 1, "the rest of the batch ran");
}

#[test]
fn a_panic_in_deferred_work_at_drop_still_runs_the_rest_and_closes_the_queue() {
    let rt = Runtime::new();
    let handle = rt.handle().clone();
    let ran = Arc::new(AtomicUsize::new(0));
    handle
        .defer(|| panic!("deferred work panicked"))
        .unwrap_or_else(|_| panic!("the runtime is alive"));
    {
        let ran = ran.clone();
        handle
            .defer(move || {
                ran.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap_or_else(|_| panic!("the runtime is alive"));
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(rt)));
    assert!(outcome.is_err(), "the panic goes on after the shutdown");
    assert_eq!(ran.load(Ordering::SeqCst), 1, "the rest of the batch ran");
    let late = handle.defer(|| unreachable!("the runtime is gone"));
    assert!(late.is_err(), "the queue is closed");
}

// miri cannot run kqueue or epoll.
#[cfg(all(unix, not(miri)))]
#[test]
fn a_task_waiting_on_io_is_freed_when_the_runtime_drops() {
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    use porto_io::{Direction, MioDriver};

    struct Flag(Arc<AtomicBool>);
    impl Drop for Flag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let driver = Arc::new(MioDriver::new().unwrap());
    let weak = Arc::downgrade(&driver);
    let rt = Runtime::builder().driver(driver.clone()).build();
    let (reader, _writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let id = driver
        .register(RawSource::new(reader.as_raw_fd()), Interest::READABLE)
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Flag(dropped.clone());
    let task_driver = Arc::downgrade(&driver);
    drop(driver);
    let join = rt.handle().spawn(poll_fn(move |cx| {
        let _flag = &flag;
        let driver = task_driver.upgrade().expect("the runtime owns the driver");
        let io = driver.slab().get(id).unwrap();
        io.poll_readiness(cx, id, Direction::Read).map(|_| ())
    }));
    drop(join);
    rt.run_until_idle();
    assert!(
        !dropped.load(Ordering::SeqCst),
        "the task waits on the socket"
    );
    drop(rt);
    assert!(
        dropped.load(Ordering::SeqCst),
        "the task was freed at shutdown"
    );
    assert!(
        weak.upgrade().is_none(),
        "the driver was freed with the runtime"
    );
}

/// A resource that is always ready, behind `coop::poll_proceed`: what a channel with messages
/// queued does.
fn always_ready() -> impl Future<Output = ()> {
    poll_fn(|cx| {
        let proceed = std::task::ready!(crate::coop::poll_proceed(cx));
        proceed.made_progress();
        Poll::Ready(())
    })
}

#[test]
fn a_loop_of_ready_operations_yields_every_coop_budget_and_lets_a_peer_run() {
    const BUDGET: u32 = 16;
    let rt = Runtime::builder().coop_budget(BUDGET).build();
    let done = Arc::new(AtomicUsize::new(0));
    let peer_saw = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let peer = {
        let (done, peer_saw, stop) = (done.clone(), peer_saw.clone(), stop.clone());
        rt.spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                peer_saw.lock().unwrap().push(done.load(Ordering::Relaxed));
                yield_now().await;
            }
        })
    };
    let busy = rt.spawn(async move {
        for _ in 0..1000 {
            always_ready().await;
            done.fetch_add(1, Ordering::Relaxed);
        }
        stop.store(true, Ordering::Relaxed);
    });
    rt.block_on(async move {
        busy.await.unwrap();
        peer.await.unwrap();
    });
    let seen = peer_saw.lock().unwrap();
    let steps: Vec<usize> = seen.windows(2).map(|pair| pair[1] - pair[0]).collect();
    // The busy task got through exactly a budget's worth of operations between two turns of
    // the peer (the last stretch may be shorter).
    let full = &steps[..steps.len() - 1];
    assert!(
        full.len() >= 1000 / BUDGET as usize - 2,
        "the peer starved: {steps:?}"
    );
    assert!(
        full.iter().all(|&step| step == BUDGET as usize),
        "{steps:?}"
    );
}

#[test]
#[should_panic(expected = "coop_budget must be at least 1")]
fn a_zero_coop_budget_is_refused() {
    let _ = Runtime::builder().coop_budget(0);
}
