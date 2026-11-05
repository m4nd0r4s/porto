//! State shared between a lane and the threads that wake or spawn onto it.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::Thread;

use super::lane;
use crate::task::{Notified, Schedule, new_task};
use crate::time::{Deadline, RuntimeClock};

/// A cloneable, `Send + Sync` handle to a runtime: spawns tasks onto it from any thread.
#[derive(Clone)]
pub struct Handle {
    pub(crate) shared: Arc<Shared>,
}

/// The part of a lane other threads can reach.
pub(crate) struct Shared {
    inbox: Mutex<Inbox>,
    /// The runtime clock: every deadline of this runtime is on it.
    pub(crate) clock: RuntimeClock,
}

/// Tasks woken or spawned off the lane thread (`12-scheduler-and-lanes.md` §4 "remote
/// inbox"), plus what is needed to unpark the lane.
#[derive(Default)]
pub(crate) struct Inbox {
    pub(crate) tasks: VecDeque<Notified>,
    /// The thread inside `block_on`, unparked on every remote push.
    pub(crate) lane_thread: Option<Thread>,
    /// Set when the runtime is dropped: later wakes drop the task instead of queueing it.
    pub(crate) closed: bool,
}

impl Shared {
    pub(crate) fn inbox(&self) -> MutexGuard<'_, Inbox> {
        // Only queue operations run under the lock, so poisoning can only come from an
        // allocation failure; the queue is still consistent, so keep going.
        self.inbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Handle {
    pub(crate) fn new(clock: RuntimeClock) -> Self {
        Self {
            shared: Arc::new(Shared {
                inbox: Mutex::new(Inbox::default()),
                clock,
            }),
        }
    }

    /// The current time on this runtime's clock, from any thread.
    pub fn now(&self) -> Deadline {
        self.shared.clock.now()
    }

    /// Spawns `future` as a new task. From inside this runtime's `block_on` it goes on the
    /// local queue; from anywhere else, into the lane's inbox (unparking the lane).
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = new_task(future, self.clone());
        if let Err(task) = self.schedule_local(task) {
            self.clone().schedule_remote(task);
        }
    }

    /// A handle to the runtime running on this thread (inside its `block_on` or one of its
    /// tasks), if any.
    pub fn try_current() -> Option<Self> {
        lane::current_handle()
    }

    pub(crate) fn same_runtime(&self, other: &Handle) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

// SAFETY: `schedule_local` only pushes the task onto this thread's local queue, or returns it.
unsafe impl Schedule for Handle {
    fn schedule_local(&self, task: Notified) -> Result<(), Notified> {
        lane::push_local(self, task)
    }

    fn schedule_remote(self, task: Notified) {
        let mut inbox = self.shared.inbox();
        if inbox.closed {
            drop(inbox);
            // Dropping may free the task and run its future's destructor: not under the lock.
            drop(task);
            return;
        }
        inbox.tasks.push_back(task);
        let thread = inbox.lane_thread.clone();
        drop(inbox);
        // An `unpark` that lands before the lane's `park` is not lost: the parker keeps it.
        if let Some(thread) = thread {
            thread.unpark();
        }
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle").finish_non_exhaustive()
    }
}
