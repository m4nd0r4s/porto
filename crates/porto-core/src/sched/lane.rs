//! A single lane: a local FIFO run queue, driven by `block_on` on the calling thread.
//!
//! This is the M0 spike of `12-scheduler-and-lanes.md` §3 with one lane and no LIFO slot,
//! stealing, injector or reactor. Wakes on the lane thread push to the local queue without
//! synchronization; wakes from other threads go through the shared inbox and unpark the lane.
//! Timers live in the lane's wheel shard (see `timers`): they fire every `EVENT_INTERVAL` tasks
//! and before parking, and the lane parks no longer than until the earliest one. The runtime's
//! clock (`time::clock`) decides what "now" is and how a park waits for a deadline.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::pin::pin;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Duration;

use super::park::ParkHook;
use super::shared::Handle;
use super::timers::Timers;
use crate::task::{JoinHandle, Notified, new_joinable_task};
use crate::time::{Clock, Deadline, MonotonicClock, RuntimeClock};

/// Tasks run between two checks of the `block_on` future (Tokio's `event_interval`).
const EVENT_INTERVAL: usize = 61;

thread_local! {
    /// The core whose `block_on` (or `run_until_idle`) is running on this thread.
    static CURRENT: Cell<*const Core> = const { Cell::new(ptr::null()) };
}

/// The lane-owned half of a runtime. Only the lane thread touches `local`.
struct Core {
    handle: Handle,
    local: RefCell<VecDeque<Notified>>,
    /// Tasks taken so far; every `EVENT_INTERVAL`th one comes from the inbox first.
    tick: Cell<usize>,
    park_hook: Option<Box<dyn ParkHook>>,
}

/// A single-lane runtime.
///
/// Tasks run on whichever thread calls [`Runtime::block_on`] or [`Runtime::run_until_idle`].
/// Dropping the runtime cancels every queued task (its future is dropped and its join handle
/// yields a cancelled [`JoinError`](crate::task::JoinError)), and so does any later wake of a
/// task parked on a waker; such a task is freed when its last waker is dropped.
pub struct Runtime {
    core: Core,
}

/// Configures a [`Runtime`]: `Runtime::builder().park_hook(hook).clock(clock).build()`.
#[derive(Default)]
#[must_use = "a builder does nothing until `build` is called"]
pub struct Builder {
    park_hook: Option<Box<dyn ParkHook>>,
    clock: Option<Box<dyn Clock>>,
    timer_resolution: Option<Duration>,
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

    /// Creates the runtime. The runtime clock's epoch is now, unless a clock was given.
    pub fn build(self) -> Runtime {
        let clock = match self.clock {
            Some(clock) => RuntimeClock::Custom(clock),
            None => RuntimeClock::Monotonic(MonotonicClock::new()),
        };
        Runtime {
            core: Core {
                handle: Handle::new(
                    clock,
                    self.timer_resolution.unwrap_or(Timers::DEFAULT_RESOLUTION),
                ),
                local: RefCell::new(VecDeque::with_capacity(256)),
                tick: Cell::new(0),
                park_hook: self.park_hook,
            },
        }
    }
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("park_hook", &self.park_hook.is_some())
            .field("clock", &self.clock.is_some())
            .field("timer_resolution", &self.timer_resolution)
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

    /// Configures a runtime: a park hook, a clock.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// A handle for spawning onto this runtime from any thread.
    pub fn handle(&self) -> &Handle {
        &self.core.handle
    }

    /// The current time on this runtime's clock.
    pub fn now(&self) -> Deadline {
        self.core.clock().now()
    }

    /// Queues `future` as a new task. It first runs at the next `block_on`/`run_until_idle`.
    /// Await the [`JoinHandle`] for the output, or drop it to detach the task.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (task, join) = new_joinable_task(future, self.core.handle.clone());
        self.core.local.borrow_mut().push_back(task);
        join
    }

    /// Runs `future` to completion on this thread, running spawned tasks while it waits.
    /// Parks the thread when there is nothing to do.
    ///
    /// Returns as soon as `future` is ready, even if other tasks are still queued.
    ///
    /// # Panics
    /// If called from inside a runtime (a task, or another `block_on`), or if `future` or a
    /// task panics (the panic is resumed here; the runtime stays usable).
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _enter = Enter::new(&self.core);
        let thread = thread::current();
        self.core.handle.shared.inbox().lane_thread = Some(thread.clone());
        let root = Arc::new(RootWaker {
            woken: AtomicBool::new(true),
            thread,
        });
        let waker = Waker::from(root.clone());
        let mut cx = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            if root.woken.swap(false, Ordering::AcqRel)
                && let Poll::Ready(output) = future.as_mut().poll(&mut cx)
            {
                return output;
            }
            let ran = self.core.run_batch(EVENT_INTERVAL);
            let fired = self.core.fire_expired_timers();
            if ran == 0 && fired == 0 && !root.woken.load(Ordering::Acquire) {
                // Every queue was empty after the last check and no timer is due; a later
                // remote push or root wake unparks us (or has already left the token).
                self.core.park();
            }
        }
    }

    /// Runs queued tasks until both the local queue and the inbox are empty and no timer is
    /// due, without parking. Returns the number of polls.
    ///
    /// # Panics
    /// Like [`Runtime::block_on`].
    pub fn run_until_idle(&self) -> usize {
        let _enter = Enter::new(&self.core);
        let mut ran = 0;
        loop {
            ran += self.core.run_batch(usize::MAX);
            if self.core.fire_expired_timers() == 0 {
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
        self.core.handle.shared.inbox().closed = true;
        // Dropping a queue entry cancels its task and drops its future, whose destructor may
        // wake other tasks; with the inbox closed, those wakes cancel their task instead of
        // queueing it.
        loop {
            let local = mem::take(&mut *self.core.local.borrow_mut());
            let remote = mem::take(&mut self.core.handle.shared.inbox().tasks);
            let had_timers = self.core.timers().drop_all();
            if local.is_empty() && remote.is_empty() && !had_timers {
                break;
            }
            drop(local);
            drop(remote);
        }
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("queued", &self.core.local.borrow().len())
            .field("next_timer", &self.core.timers().next_deadline())
            .finish_non_exhaustive()
    }
}

impl Core {
    fn clock(&self) -> &RuntimeClock {
        &self.handle.shared.clock
    }

    fn timers(&self) -> &Timers {
        &self.handle.shared.timers
    }

    fn fire_expired_timers(&self) -> usize {
        self.timers().fire_expired(|| self.clock().now())
    }

    fn next_task(&self) -> Option<Notified> {
        let tick = self.tick.get().wrapping_add(1);
        self.tick.set(tick);
        // Tasks that keep waking themselves keep the local queue non-empty; checking the inbox
        // first every `EVENT_INTERVAL` tasks keeps remote wakes and spawns from starving.
        if tick.is_multiple_of(EVENT_INTERVAL)
            && let Some(task) = self.handle.shared.inbox().tasks.pop_front()
        {
            return Some(task);
        }
        if let Some(task) = self.local.borrow_mut().pop_front() {
            return Some(task);
        }
        let mut inbox = self.handle.shared.inbox();
        if inbox.tasks.is_empty() {
            return None;
        }
        // The local queue is empty: swap the whole inbox in, keeping both allocations.
        let mut local = self.local.borrow_mut();
        mem::swap(&mut *local, &mut inbox.tasks);
        drop(inbox);
        local.pop_front()
    }

    /// Parks the lane thread until an unpark, or until the earliest timer is due, through the
    /// park hook if there is one.
    fn park(&self) {
        let next_deadline = self.timers().next_deadline();
        let mut park = || match next_deadline {
            None => thread::park(),
            // A park may return early; the caller then finds no timer due and parks again for
            // the rest.
            Some(deadline) => self.clock().park_until(deadline),
        };
        match &self.park_hook {
            Some(hook) => hook.around_park(&mut park),
            None => park(),
        }
    }

    /// Runs up to `max` tasks. No borrow of `local` is held while a task runs, so tasks can
    /// wake and spawn freely.
    fn run_batch(&self, max: usize) -> usize {
        let mut ran = 0;
        while ran < max {
            let Some(task) = self.next_task() else { break };
            task.run();
            ran += 1;
        }
        ran
    }
}

/// Pushes `task` onto the local queue if this thread is running `handle`'s lane.
pub(crate) fn push_local(handle: &Handle, task: Notified) -> Result<(), Notified> {
    // `try_with` fails only while thread locals are being destroyed: not on a lane then.
    let core = CURRENT.try_with(Cell::get).unwrap_or(ptr::null());
    if core.is_null() {
        return Err(task);
    }
    // SAFETY: `CURRENT` is non-null only while an `Enter` guard borrows the core, on this
    // thread, so the core is alive.
    let core = unsafe { &*core };
    if !core.handle.same_runtime(handle) {
        return Err(task);
    }
    core.local.borrow_mut().push_back(task);
    Ok(())
}

/// Calls `f` with the handle of the runtime running on this thread, if any (without cloning
/// it).
pub(crate) fn with_current_handle<R>(f: impl FnOnce(&Handle) -> R) -> Option<R> {
    let core = CURRENT.try_with(Cell::get).ok()?;
    // SAFETY: as in `push_local`.
    (!core.is_null()).then(|| f(&unsafe { &*core }.handle))
}

/// The current time on the clock of the runtime running on this thread, if any.
pub(crate) fn current_time() -> Option<Deadline> {
    let core = CURRENT.try_with(Cell::get).ok()?;
    // SAFETY: as in `push_local`.
    (!core.is_null()).then(|| unsafe { &*core }.clock().now())
}

/// A handle to the runtime running on this thread, if any.
pub(crate) fn current_handle() -> Option<Handle> {
    let core = CURRENT.try_with(Cell::get).ok()?;
    // SAFETY: as in `push_local`.
    (!core.is_null()).then(|| unsafe { &*core }.handle.clone())
}

/// Marks this thread as running a core until dropped.
struct Enter<'a> {
    _core: PhantomData<&'a Core>,
}

impl<'a> Enter<'a> {
    fn new(core: &'a Core) -> Self {
        CURRENT.with(|current| {
            assert!(
                current.get().is_null(),
                "porto: cannot run a runtime from inside a runtime (block_on or \
                 run_until_idle called from a task or from another block_on); spawn the work \
                 onto the current runtime instead, or run the other runtime on its own thread"
            );
            current.set(core);
        });
        Self { _core: PhantomData }
    }
}

impl Drop for Enter<'_> {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(ptr::null()));
    }
}

/// The waker of the `block_on` future: a flag plus an unpark.
struct RootWaker {
    woken: AtomicBool,
    thread: Thread,
}

impl Wake for RootWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}
