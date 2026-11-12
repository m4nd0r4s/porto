//! State shared between the lanes of a runtime and the threads that wake or spawn onto them
//! (`12-scheduler-and-lanes.md` §2 `Shared`).
//!
//! Nothing here assumes one lane: the injector is drained by whichever lane pops it, and each
//! lane has a [`Remote`] slot (its parking state and deferred work) indexed by lane number.
//! M1 builds one slot, lane 0; M2 builds one per lane and has injector pushes notify an idle
//! lane from the idle set rather than lane 0.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use porto_io::Driver;

use super::context;
use super::inject::Inject;
use super::park::Parking;
use super::timers::Timers;
use crate::scope::{ScopeClosed, ScopeRef, TaskScope};
use crate::task::{
    JoinHandle, Notified, Schedule, TaskBody, TaskBodyRef, TaskFlags, new_body_task,
    new_body_task_in, new_joinable_task, new_joinable_task_in,
};
use crate::time::{Deadline, RuntimeClock};

/// Work a lane runs at its next maintenance tick ([`Handle::defer`]).
pub type DeferredWork = Box<dyn FnOnce() + Send>;

/// A cloneable, `Send + Sync` handle to a runtime: spawns tasks onto it from any thread.
#[derive(Clone)]
pub struct Handle {
    pub(crate) shared: Arc<Shared>,
}

/// The part of a runtime every lane and every waking thread can reach.
pub(crate) struct Shared {
    /// Wakes and spawns from threads that are not running one of this runtime's lanes.
    pub(crate) inject: Inject<Notified>,
    /// One slot per lane, by lane index.
    pub(crate) lanes: Box<[Remote]>,
    /// The runtime clock: every deadline of this runtime is on it.
    pub(crate) clock: RuntimeClock,
    /// The timer wheel (`14-time-and-timers.md` §2). Here rather than lane-local because a
    /// `Sleep` dropped on any thread unlinks its entry from it; sharding it per lane is internal
    /// to `Timers`.
    pub(crate) timers: Timers,
    /// The I/O driver, if the runtime has one (`13-io-driver.md`): polled at maintenance, and
    /// the lane parks in it.
    pub(crate) driver: Option<Arc<dyn Driver>>,
}

/// The part of one lane other threads can reach.
pub(crate) struct Remote {
    pub(crate) parking: Parking,
    /// Whether the lane parks in the driver's `turn` rather than on its thread: an unpark then
    /// wakes the driver.
    pub(crate) parks_in_driver: AtomicBool,
    /// Work handed to this lane from other threads, run at its next maintenance tick
    /// (`12-scheduler-and-lanes.md` §2 `deferred_decref`).
    pub(crate) deferred: Inject<DeferredWork>,
}

impl Remote {
    fn new() -> Self {
        Self {
            parking: Parking::new(),
            parks_in_driver: AtomicBool::new(false),
            deferred: Inject::new(),
        }
    }
}

impl Shared {
    /// Notifies a lane after work was pushed onto the injector, which any lane can take.
    /// With one lane that is lane 0; M2 picks an idle lane from the idle set instead.
    fn notify_any_lane(&self) {
        self.notify_lane(0);
    }

    /// Notifies lane `index` after work was published for it: it looks at its queues before it
    /// next parks, and is unparked if it is parked.
    pub(crate) fn notify_lane(&self, index: usize) {
        if self.lanes[index].parking.notify() {
            self.unpark(index);
        }
    }

    /// Notifies every lane, so each looks at its queues and timers again (a timer moved
    /// earlier from another thread).
    pub(crate) fn notify_all_lanes(&self) {
        for index in 0..self.lanes.len() {
            self.notify_lane(index);
        }
    }

    fn unpark(&self, index: usize) {
        let lane = &self.lanes[index];
        if lane.parks_in_driver.load(Ordering::Relaxed)
            && let Some(driver) = &self.driver
        {
            if let Err(error) = driver.waker().wake() {
                // Nothing to hand the error to: the waker is on the waking thread. The lane is
                // left parked until its next timer, which is a hang if there is none.
                panic!(
                    "porto: could not wake the {} I/O driver ({error}); the lane would miss \
                     this wake. This is a resource problem (file descriptors?) or a porto bug: \
                     report it with the OS and the backend",
                    driver.kind()
                );
            }
        } else {
            lane.parking.unpark_thread();
        }
    }
}

/// A handle that does not keep its runtime alive: what an object that outlives the run (a
/// Python `Scope`, a scope's deadline link) keeps, so it neither holds the runtime's state
/// and I/O driver after `porto.run` returned nor forms a cycle with a timer it armed.
#[derive(Clone)]
pub struct WeakHandle {
    shared: Weak<Shared>,
}

impl WeakHandle {
    /// The runtime's handle, or `None` once the runtime was dropped.
    pub fn upgrade(&self) -> Option<Handle> {
        self.shared.upgrade().map(|shared| Handle { shared })
    }

    /// Whether this is a handle to the runtime `handle` is a handle to.
    pub(crate) fn is(&self, handle: &Handle) -> bool {
        std::ptr::eq(self.shared.as_ptr(), Arc::as_ptr(&handle.shared))
    }
}

impl fmt::Debug for WeakHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WeakHandle")
    }
}

impl Handle {
    /// A handle that does not keep the runtime alive.
    pub fn downgrade(&self) -> WeakHandle {
        WeakHandle {
            shared: Arc::downgrade(&self.shared),
        }
    }

    pub(crate) fn new(
        clock: RuntimeClock,
        timer_resolution: Duration,
        driver: Option<Arc<dyn Driver>>,
        lanes: usize,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                inject: Inject::new(),
                lanes: (0..lanes).map(|_| Remote::new()).collect(),
                clock,
                timers: Timers::new(timer_resolution),
                driver,
            }),
        }
    }

    /// The current time on this runtime's clock, from any thread.
    pub fn now(&self) -> Deadline {
        self.shared.clock.now()
    }

    /// How many timers are armed on this runtime's wheel: sleeps that have not fired or been
    /// cancelled yet. For observability and tests; a cancelled sleep is unlinked at once, so it
    /// no longer counts.
    pub fn armed_timers(&self) -> usize {
        self.shared.timers.armed()
    }

    /// Spawns `future` as a new task. From one of this runtime's lanes it goes on that lane's
    /// local queue; from anywhere else, onto the injector (unparking a lane).
    ///
    /// Await the [`JoinHandle`] for the output, or drop it to detach the task. If the runtime
    /// is already dropped, the task is cancelled at once.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (task, join) = new_joinable_task(future, self.clone());
        if let Err(task) = self.schedule_local(task) {
            self.clone().schedule_remote(task);
        }
        join
    }

    /// Spawns a task running `body` (a task that is not a Rust future, such as a PyTask),
    /// with `flags`, queued like [`Handle::spawn`]. The returned reference reaches the body,
    /// which keeps the outcome and the joiners itself.
    pub fn spawn_body<B: TaskBody>(&self, body: B, flags: TaskFlags) -> TaskBodyRef<B> {
        let (task, body) = new_body_task(body, self.clone(), flags);
        if let Err(task) = self.schedule_local(task) {
            self.clone().schedule_remote(task);
        }
        body
    }

    /// Spawns `future` into `scope`, as [`Handle::spawn`]: the scope counts it in
    /// `live_children` until it ends, and hears how it ended (a panic cancels the scope, C4).
    ///
    /// # Errors
    /// Hands `future` back, unrun, if the scope is closed (C7,
    /// [`crate::scope::ScopeNode::close`]).
    pub fn spawn_in<F, E>(
        &self,
        scope: &ScopeRef<E>,
        future: F,
    ) -> Result<JoinHandle<F::Output>, ScopeClosed<F>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        E: Send + 'static,
    {
        let Some(scope) = TaskScope::try_attach(scope) else {
            return Err(ScopeClosed(future));
        };
        let (task, join) = new_joinable_task_in(future, self.clone(), Some(scope));
        if let Err(task) = self.schedule_local(task) {
            self.clone().schedule_remote(task);
        }
        Ok(join)
    }

    /// Spawns a task running `body` into `scope`, as [`Handle::spawn_body`] and
    /// [`Handle::spawn_in`]: the scope records what [`TaskBody::take_failure`] returns.
    ///
    /// # Errors
    /// Hands `body` back, unrun, if the scope is closed (C7,
    /// [`crate::scope::ScopeNode::close`]).
    pub fn spawn_body_in<B, E>(
        &self,
        scope: &ScopeRef<E>,
        body: B,
        flags: TaskFlags,
    ) -> Result<TaskBodyRef<B>, ScopeClosed<B>>
    where
        B: TaskBody,
        E: Send + 'static,
    {
        let Some(scope) = TaskScope::try_attach(scope) else {
            return Err(ScopeClosed(body));
        };
        let (task, body) = new_body_task_in(body, self.clone(), flags, Some(scope));
        if let Err(task) = self.schedule_local(task) {
            self.clone().schedule_remote(task);
        }
        Ok(body)
    }

    /// Runs `work` on lane 0 at its next maintenance tick (every 61 tasks, and before it
    /// parks), from any thread; a parked lane is unparked for it. This is how an object that
    /// must be released on the lane (a Python reference, `25-cpython-integration-and-memory.md`
    /// P1) is released from elsewhere.
    ///
    /// # Errors
    /// Hands `work` back, unrun, if the runtime is already dropped: the caller decides what to
    /// do with what it owns.
    pub fn defer(&self, work: impl FnOnce() + Send + 'static) -> Result<(), DeferredWork> {
        self.shared.lanes[0].deferred.push(Box::new(work))?;
        self.shared.notify_lane(0);
        Ok(())
    }

    /// A handle to the runtime running on this thread (inside its `block_on` or one of its
    /// tasks), if any.
    pub fn try_current() -> Option<Self> {
        context::current_handle()
    }

    /// The runtime's I/O driver, if it has one: what sources are registered with
    /// ([`porto_io::Registration::new`]) so the lane waits for their readiness
    /// (`13-io-driver.md` §4).
    pub fn driver(&self) -> Option<&Arc<dyn Driver>> {
        self.shared.driver.as_ref()
    }

    pub(crate) fn same_runtime(&self, other: &Handle) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

// SAFETY: `schedule_local` only pushes the task onto the current thread's local queue, or
// returns it.
unsafe impl Schedule for Handle {
    fn schedule_local(&self, task: Notified) -> Result<(), Notified> {
        context::push_local(self, task)
    }

    fn schedule_remote(self, task: Notified) {
        match self.shared.inject.push(task) {
            Ok(()) => self.shared.notify_any_lane(),
            // The runtime is dropped: dropping the entry cancels the task (`Notified`), which
            // runs its future's destructor, outside the injector's lock.
            Err(task) => drop(task),
        }
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
            .field("lanes", &self.shared.lanes.len())
            .field("injected", &self.shared.inject.len())
            .finish_non_exhaustive()
    }
}
