//! A lane: a local FIFO run queue and the loop that drives it (`12-scheduler-and-lanes.md` §3).
//!
//! The lane-owned half lives here and is touched only by the thread running the lane; what
//! other threads reach is in `shared`. Each turn of the loop is one *tick*:
//!
//! - every [`EVENT_INTERVAL`] ticks, *maintenance*: poll the I/O driver without blocking, fire
//!   the expired timers, and run the work deferred to this lane;
//! - every [`GLOBAL_QUEUE_INTERVAL`] ticks the injector is tried before the local queue, so
//!   tasks that keep waking themselves on the lane cannot starve wakes from other threads;
//! - otherwise the local queue, then a batch from the injector.
//!
//! Each task poll starts with a fresh cooperative budget (`crate::coop`): a task that spends it
//! wakes itself, which puts it at the back of the local queue.
//!
//! With nothing to run the lane does maintenance once more, then parks: in the driver if the
//! runtime has one, else on its thread, never past the earliest timer. There is no LIFO slot
//! and no stealing yet (M2).

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::park::ParkHook;
use super::shared::{DeferredWork, Handle, Remote, Shared};
use crate::sync::primitives::{Thread, park};
use crate::task::Notified;
use crate::time::Deadline;

/// Ticks between two maintenance passes (`event_interval`, as in Tokio).
pub(crate) const EVENT_INTERVAL: u32 = 61;
/// Ticks between two injector-first picks (`global_queue_interval`).
pub(crate) const GLOBAL_QUEUE_INTERVAL: u32 = 31;
/// The cooperative budget of a task poll unless the builder sets one (`coop_budget`).
pub(crate) const DEFAULT_COOP_BUDGET: u32 = 128;
/// The most tasks one injector pop moves to the local queue (half the local capacity of §2).
const INJECT_BATCH: usize = 128;

/// The lane-owned half of a lane. Only its thread touches it.
pub(crate) struct Lane {
    /// The lane's number: its slot in `Shared::lanes`. Lane 0 is the thread calling `block_on`.
    index: usize,
    pub(crate) handle: Handle,
    pub(crate) local: RefCell<VecDeque<Notified>>,
    tick: Cell<u32>,
    park_hook: Option<Box<dyn ParkHook>>,
    /// The budget each task poll starts with (`crate::coop`).
    coop_budget: u32,
}

impl Lane {
    pub(crate) fn new(
        index: usize,
        handle: Handle,
        park_hook: Option<Box<dyn ParkHook>>,
        coop_budget: u32,
    ) -> Self {
        Self {
            index,
            handle,
            local: RefCell::new(VecDeque::with_capacity(256)),
            tick: Cell::new(0),
            park_hook,
            coop_budget,
        }
    }

    /// Runs `poll` (one task poll, or the `block_on` future's) with a fresh cooperative budget.
    #[inline]
    pub(crate) fn with_budget<R>(&self, poll: impl FnOnce() -> R) -> R {
        crate::coop::budget(self.coop_budget, poll)
    }

    pub(crate) fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn shared(&self) -> &Shared {
        &self.handle.shared
    }

    fn remote(&self) -> &Remote {
        &self.shared().lanes[self.index]
    }

    /// The ticks so far, wrapping.
    #[cfg(all(test, not(loom)))]
    pub(crate) fn ticks(&self) -> u32 {
        self.tick.get()
    }

    /// Makes `thread` the one other threads unpark for this lane. Called before it first parks.
    pub(crate) fn start(&self, thread: Thread) {
        let remote = self.remote();
        remote.parking.set_thread(thread);
        remote
            .parks_in_driver
            .store(self.shared().driver.is_some(), Ordering::Relaxed);
    }

    /// One tick: maintenance if it is due, then runs the next task. Returns whether a task ran.
    pub(crate) fn tick(&self) -> bool {
        let tick = self.tick.get().wrapping_add(1);
        self.tick.set(tick);
        if tick.is_multiple_of(EVENT_INTERVAL) {
            self.maintenance();
        }
        // The borrow of `local` ends before the task runs, so tasks can wake and spawn freely.
        let Some(task) = self.next_task(tick) else {
            return false;
        };
        self.with_budget(|| task.run());
        true
    }

    fn next_task(&self, tick: u32) -> Option<Notified> {
        let shared = self.shared();
        if tick.is_multiple_of(GLOBAL_QUEUE_INTERVAL)
            && let Some(task) = shared.inject.pop()
        {
            return Some(task);
        }
        let mut local = self.local.borrow_mut();
        if let Some(task) = local.pop_front() {
            return Some(task);
        }
        // A fair share of the injector, so other lanes find some too.
        let batch = (shared.inject.len() / shared.lanes.len() + 1).min(INJECT_BATCH);
        shared.inject.pop_batch(&mut local, batch)
    }

    /// Polls the driver without blocking, fires the expired timers and runs the deferred work.
    /// Returns whether any of it did something: woken tasks are on the local queue by then.
    pub(crate) fn maintenance(&self) -> bool {
        let events = self.turn_driver(Some(Duration::ZERO));
        let fired = self.fire_expired_timers();
        let deferred = self.run_deferred();
        events + fired + deferred > 0
    }

    fn fire_expired_timers(&self) -> usize {
        let shared = self.shared();
        shared.timers.fire_expired(|| shared.clock.now())
    }

    /// Runs the work other threads deferred to this lane. Returns how many ran. A panicking
    /// item does not lose the rest of the batch: every item runs, then the first panic goes on.
    fn run_deferred(&self) -> usize {
        let deferred = self.remote().deferred.take_all();
        let ran = deferred.len();
        if let Some(payload) = run_each(deferred) {
            panic::resume_unwind(payload);
        }
        ran
    }

    /// Turns the driver for at most `timeout`, dispatching readiness (the woken tasks land on
    /// the local queue). Returns the number of events; 0 without a driver.
    fn turn_driver(&self, timeout: Option<Duration>) -> usize {
        let Some(driver) = &self.shared().driver else {
            return 0;
        };
        match driver.turn(timeout) {
            Ok(events) => events,
            Err(error) => panic!(
                "porto: polling the {} I/O driver failed ({error}); the runtime cannot wait for \
                 I/O. This is a resource problem (file descriptors, memory) or a porto bug: \
                 report it with the OS and the backend",
                driver.kind()
            ),
        }
    }

    /// Whether anything is queued for this lane: tasks, or deferred work. A cheap early out
    /// before parking; `Parking` is what makes the park safe against concurrent pushes.
    fn has_work(&self) -> bool {
        let shared = self.shared();
        !self.local.borrow().is_empty()
            || !shared.inject.is_empty()
            || !self.remote().deferred.is_empty()
    }

    /// Parks the lane until it is notified, or until the earliest timer is due, unless work
    /// is queued or a notification arrived since it last parked. Goes through the park hook if
    /// there is one. Returns whether it blocked.
    pub(crate) fn park(&self) -> bool {
        if self.has_work() {
            return false;
        }
        let next_deadline = self.shared().timers.next_deadline();
        self.remote().parking.park(|| {
            let mut block = || self.block(next_deadline);
            match &self.park_hook {
                Some(hook) => hook.around_park(&mut block),
                None => block(),
            }
        })
    }

    /// Blocks the lane thread: in the driver if there is one, else on the thread. A block may
    /// return early; the loop then finds nothing due and parks again for the rest.
    fn block(&self, deadline: Option<Deadline>) {
        let shared = self.shared();
        if shared.driver.is_some() {
            let timeout =
                deadline.map(|deadline| deadline.saturating_duration_since(shared.clock.now()));
            self.turn_driver(timeout);
            return;
        }
        match deadline {
            None => park(),
            Some(deadline) => shared.clock.park_until(deadline),
        }
    }
}

/// Runs every item of `batch`, each under its own panic guard, and returns the first panic.
pub(crate) fn run_each(batch: VecDeque<DeferredWork>) -> Option<Box<dyn Any + Send>> {
    let mut first = None;
    for work in batch {
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(work)) {
            first.get_or_insert(payload);
        }
    }
    first
}
