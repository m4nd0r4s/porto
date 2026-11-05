//! The runtime: lane 0 driven by the thread that calls `block_on`, and its builder.

use std::fmt;
use std::future::Future;
use std::mem;
use std::panic;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use porto_io::Driver;

use super::context::Enter;
use super::lane::{Lane, run_each};
use super::park::ParkHook;
use super::shared::{Handle, Shared};
use super::timers::Timers;
use crate::sync::primitives::current_thread;
use crate::task::{JoinHandle, new_joinable_task};
use crate::time::{Clock, Deadline, MonotonicClock, RuntimeClock};

/// A single-lane runtime.
///
/// Tasks run on whichever thread calls [`Runtime::block_on`] or [`Runtime::run_until_idle`]:
/// that thread is lane 0 (`10-architecture-overview.md` §3). Dropping the runtime cancels every
/// queued task (its future is dropped and its join handle yields a cancelled
/// [`JoinError`](crate::task::JoinError)), and so does any later wake of a task parked on a
/// waker; such a task is freed when its last waker is dropped.
pub struct Runtime {
    lane: Lane,
}

/// Configures a [`Runtime`]: `Runtime::builder().park_hook(hook).clock(clock).build()`.
#[derive(Default)]
#[must_use = "a builder does nothing until `build` is called"]
pub struct Builder {
    park_hook: Option<Box<dyn ParkHook>>,
    clock: Option<Box<dyn Clock>>,
    timer_resolution: Option<Duration>,
    driver: Option<Arc<dyn Driver>>,
}

impl Builder {
    /// Calls `hook` around every park of the lane thread.
    pub fn park_hook(mut self, hook: impl ParkHook) -> Self {
        self.park_hook = Some(Box::new(hook));
        self
    }

    /// Uses `clock` as the runtime clock instead of a [`MonotonicClock`] started by `build`:
    /// the hook for a virtual clock (deterministic mode).
    pub fn clock(mut self, clock: impl Clock) -> Self {
        self.clock = Some(Box::new(clock));
        self
    }

    /// Sets how finely timers are kept: a sleep wakes on the first tick of this length at or
    /// after its deadline (`14-time-and-timers.md` §2). The default, 1 ms, spans about 2.2
    /// years before a timer is re-armed; finer ticks cost more wakes for long timers.
    ///
    /// # Panics
    /// If `resolution` is outside 100 µs..=10 ms.
    pub fn timer_resolution(mut self, resolution: Duration) -> Self {
        assert!(
            (Timers::MIN_RESOLUTION..=Timers::MAX_RESOLUTION).contains(&resolution),
            "porto: timer_resolution must be between 100µs and 10ms, got {resolution:?}; pass \
             a resolution in that range, or leave it unset for the 1ms default"
        );
        self.timer_resolution = Some(resolution);
        self
    }

    /// Gives the runtime an I/O driver (`13-io-driver.md`). The lane polls it without
    /// blocking every 61 tasks and before parking, and parks in it (unparks wake it). A
    /// custom [`clock`](Builder::clock) is then only read, not parked on: the driver waits in
    /// wall-clock time.
    ///
    /// One runtime per driver: a driver shared by two runtimes serializes their polls and
    /// delivers one runtime's readiness from the other's thread. The runtime releases the
    /// driver's registrations ([`Driver::shutdown`]) when it is dropped.
    pub fn driver(mut self, driver: Arc<dyn Driver>) -> Self {
        self.driver = Some(driver);
        self
    }

    /// Creates the runtime. The runtime clock's epoch is now, unless a clock was given.
    pub fn build(self) -> Runtime {
        let clock = match self.clock {
            Some(clock) => RuntimeClock::Custom(clock),
            None => RuntimeClock::Monotonic(MonotonicClock::new()),
        };
        let handle = Handle::new(
            clock,
            self.timer_resolution.unwrap_or(Timers::DEFAULT_RESOLUTION),
            self.driver,
            1,
        );
        Runtime {
            lane: Lane::new(0, handle, self.park_hook),
        }
    }
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("park_hook", &self.park_hook.is_some())
            .field("clock", &self.clock.is_some())
            .field("timer_resolution", &self.timer_resolution)
            .field("driver", &self.driver.as_ref().map(|driver| driver.kind()))
            .finish()
    }
}

impl Runtime {
    /// Creates a runtime with an empty run queue, whose clock starts now.
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Creates a runtime that calls `hook` around every park of the lane thread.
    pub fn with_park_hook(hook: impl ParkHook) -> Self {
        Self::builder().park_hook(hook).build()
    }

    /// Configures a runtime: a park hook, a clock, a driver.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// A handle for spawning onto this runtime from any thread.
    pub fn handle(&self) -> &Handle {
        &self.lane.handle
    }

    /// The current time on this runtime's clock.
    pub fn now(&self) -> Deadline {
        self.shared().clock.now()
    }

    fn shared(&self) -> &Shared {
        self.lane.shared()
    }

    /// Queues `future` as a new task. It first runs at the next `block_on`/`run_until_idle`.
    /// Await the [`JoinHandle`] for the output, or drop it to detach the task.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (task, join) = new_joinable_task(future, self.lane.handle.clone());
        self.lane.local.borrow_mut().push_back(task);
        join
    }

    /// Runs `future` to completion on this thread, which becomes lane 0, running spawned tasks
    /// while it waits. Parks the thread when there is nothing to do.
    ///
    /// Returns as soon as `future` is ready, even if other tasks are still queued.
    ///
    /// # Panics
    /// If called from inside a runtime (a task, or another `block_on`), or if `future` or a
    /// task panics (the panic is resumed here; the runtime stays usable).
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _enter = Enter::new(&self.lane);
        self.lane.start(current_thread());
        let root = Arc::new(RootWaker {
            woken: AtomicBool::new(true),
            shared: self.lane.handle.shared.clone(),
            lane: self.lane.index(),
        });
        let waker = Waker::from(root.clone());
        let mut cx = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            if root.woken.load(Ordering::Relaxed)
                && root.woken.swap(false, Ordering::AcqRel)
                && let Poll::Ready(output) = future.as_mut().poll(&mut cx)
            {
                return output;
            }
            if self.lane.tick() || self.lane.maintenance() {
                continue;
            }
            // Nothing ran and nothing fired: park. A remote push or a root wake since the last
            // park notified the lane, so this one returns at once and the loop looks again.
            self.lane.park();
        }
    }

    /// Runs queued tasks until the local queue and the injector are empty and maintenance
    /// finds nothing to do (no I/O event, no timer due, no deferred work), without parking.
    /// Returns the number of tasks run.
    ///
    /// # Panics
    /// Like [`Runtime::block_on`].
    pub fn run_until_idle(&self) -> usize {
        let _enter = Enter::new(&self.lane);
        let mut ran = 0;
        loop {
            while self.lane.tick() {
                ran += 1;
            }
            if !self.lane.maintenance() {
                return ran;
            }
        }
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let shared = self.lane.handle.shared.clone();
        // Dropping a queue entry cancels its task and drops its future, whose destructor may
        // wake other tasks; with the injector closed (and no lane entered), those wakes cancel
        // their task instead of queueing it. Deferred work still runs here, on the thread
        // dropping the runtime (which is the lane's thread unless the runtime was sent), until
        // the queue is closed too. A panic in deferred work does not stop the shutdown: it goes
        // on once everything is released (unless this drop is already unwinding).
        let mut remote = shared.inject.close();
        // Tasks waiting on I/O are only reachable through the driver's slots, and the driver is
        // kept alive by them (driver -> waker -> task -> handle -> shared -> driver): releasing
        // the registrations wakes them, which cancels them.
        if let Some(driver) = &shared.driver {
            driver.shutdown();
        }
        let mut panicked = None;
        loop {
            let local = mem::take(&mut *self.lane.local.borrow_mut());
            let had_timers = shared.timers.drop_all();
            let deferred = shared.lanes[self.lane.index()].deferred.take_all();
            if local.is_empty() && remote.is_empty() && !had_timers && deferred.is_empty() {
                break;
            }
            drop(local);
            drop(remote);
            let failed = run_each(deferred);
            panicked = panicked.or(failed);
            remote = shared.inject.close();
        }
        let failed = run_each(shared.lanes[self.lane.index()].deferred.close());
        panicked = panicked.or(failed);
        if let Some(payload) = panicked
            && !std::thread::panicking()
        {
            panic::resume_unwind(payload);
        }
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("queued", &self.lane.local.borrow().len())
            .field("injected", &self.shared().inject.len())
            .field("next_timer", &self.shared().timers.next_deadline())
            .finish_non_exhaustive()
    }
}

/// The waker of the `block_on` future: a flag plus a notify of its lane.
struct RootWaker {
    woken: AtomicBool,
    shared: Arc<Shared>,
    lane: usize,
}

impl Wake for RootWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        // A swap rather than a store, as in the loom model (`sched::loom_tests`). A plain store
        // is correct, but loom then lets a later read-modify-write observe an older value and
        // reports a lost wakeup even for a SeqCst store: a loom artifact. The swap costs the
        // same here.
        self.woken.swap(true, Ordering::AcqRel);
        self.shared.notify_lane(self.lane);
    }
}
