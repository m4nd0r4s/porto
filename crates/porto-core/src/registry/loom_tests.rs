//! loom models of the task registry's cross-thread state (`crate::registry`): a task freed off
//! its lane while linked, a completion against a waker dropped elsewhere, the shutdown's
//! cancel-all against a task that is being woken, and a snapshot against a spawn from another
//! thread.
//!
//! The lane is the thread that runs the model; other threads are "off the lane". The scheduler
//! stands in for a runtime's: its queue closes when the model "drops the runtime", like the
//! injector's.
//!
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release registry`

use std::cell::Cell;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::task::{Poll, Waker};

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

use super::TaskRegistry;
use crate::sched::Inject;
use crate::task::{Notified, Registration, Schedule, free_zombie, new_joinable_task};

loom::thread_local! {
    static ON_LANE: Cell<bool> = Cell::new(false);
}

fn on_lane() -> bool {
    ON_LANE.with(Cell::get)
}

/// A scheduler with a registry and a queue that closes.
#[derive(Clone)]
struct Lane {
    registry: Arc<TaskRegistry>,
    queue: Arc<Mutex<(bool, VecDeque<Notified>)>>,
}

// SAFETY: `schedule_local` always returns the task untouched.
unsafe impl Schedule for Lane {
    fn schedule_local(&self, task: Notified) -> Result<(), Notified> {
        Err(task)
    }

    fn schedule_remote(self, task: Notified) {
        let mut queue = self.queue.lock().unwrap();
        if queue.0 {
            drop(queue);
            // Closed: dropping the entry cancels the task, here, off the lane.
            drop(task);
        } else {
            queue.1.push_back(task);
        }
    }

    fn registration(&self) -> Option<Registration<'_>> {
        Some(Registration {
            registry: &self.registry,
            on_lane: on_lane(),
        })
    }
}

impl Lane {
    fn new() -> Self {
        Self {
            registry: Arc::new(TaskRegistry::new(false)),
            queue: Arc::new(Mutex::new((false, VecDeque::new()))),
        }
    }

    /// Links a freshly spawned task, as a spawn on the lane does.
    fn link(&self, task: &Notified) {
        // SAFETY: the calling thread is the lane in the model, and the task is live.
        unsafe { self.registry.link_if_new(task.ptr()) };
    }

    /// Frees what other threads handed to the lane.
    fn free_zombies(&self) {
        // SAFETY: the lane thread of an open registry.
        for task in unsafe { self.registry.take_zombies() } {
            // SAFETY: from `take_zombies`.
            unsafe { free_zombie(task) };
        }
    }

    fn live(&self) -> usize {
        let mut live = 0;
        // SAFETY: the lane thread of an open registry.
        unsafe { self.registry.for_each_live(|_| live += 1) };
        live
    }
}

/// Counts drops of whatever owns it.
struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A parked task: its waker is the only thing that keeps it alive once its join handle is gone.
/// Returns the waker and the drop counter of what the future owns.
fn parked_task(lane: &Lane) -> (Notified, Arc<Mutex<Option<Waker>>>, Arc<AtomicUsize>) {
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    let dropped = Arc::new(AtomicUsize::new(0));
    let (owned, counter) = (stash.clone(), DropCounter(dropped.clone()));
    let (task, join) = new_joinable_task(
        poll_fn(move |cx| {
            let _counter = &counter;
            *owned.lock().unwrap() = Some(cx.waker().clone());
            Poll::<()>::Pending
        }),
        lane.clone(),
    );
    drop(join);
    (task, stash, dropped)
}

/// The last reference to a linked task goes off the lane while the lane walks, takes zombies
/// and frees them: the task is freed exactly once, by the lane, and never stays linked.
#[test]
fn a_task_freed_off_the_lane_is_freed_once_by_the_lane() {
    loom::model(|| {
        ON_LANE.with(|on| on.set(true));
        let lane = Lane::new();
        let (task, stash, dropped) = parked_task(&lane);
        lane.link(&task);
        task.run();
        let waker = stash.lock().unwrap().take().expect("the task parked");
        let other = thread::spawn(move || drop(waker));
        assert!(lane.live() <= 1);
        lane.free_zombies();
        other.join().unwrap();
        lane.free_zombies();
        assert_eq!(dropped.load(Ordering::SeqCst), 1, "freed exactly once");
        assert_eq!(lane.live(), 0, "never left linked");
        assert!(!lane.registry.has_zombies());
    });
}

/// The lane completes a linked task (unlinking it) while another thread drops a waker to it:
/// whoever drops the last reference sees it unlinked and frees it, once.
#[test]
fn a_completion_races_a_waker_dropped_off_the_lane() {
    loom::model(|| {
        ON_LANE.with(|on| on.set(true));
        let lane = Lane::new();
        let dropped = Arc::new(AtomicUsize::new(0));
        let counter = DropCounter(dropped.clone());
        let (task, join) = new_joinable_task(
            poll_fn(move |_| {
                let _counter = &counter;
                Poll::Ready(())
            }),
            lane.clone(),
        );
        drop(join);
        let waker = task.task_ref().into_waker();
        lane.link(&task);
        let other = thread::spawn(move || drop(waker));
        task.run();
        other.join().unwrap();
        lane.free_zombies();
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(lane.live(), 0);
        assert!(!lane.registry.has_zombies());
    });
}

/// The shutdown (close, cancel-all until nothing is left) races a wake of a parked task from
/// another thread: the task is cancelled once, however the two interleave, and nothing stays
/// linked.
#[test]
fn shutdown_cancel_all_races_a_waking_task() {
    loom::model(|| {
        ON_LANE.with(|on| on.set(true));
        let lane = Lane::new();
        let (task, stash, dropped) = parked_task(&lane);
        lane.link(&task);
        task.run();
        let waker = stash.lock().unwrap().take().expect("the task parked");
        let other = thread::spawn(move || waker.wake());

        // The runtime is dropped: its queue closes, then its registry.
        let queued = {
            let mut queue = lane.queue.lock().unwrap();
            queue.0 = true;
            std::mem::take(&mut queue.1)
        };
        // SAFETY: the model owns the runtime, and runs the lane on this thread.
        for task in unsafe { lane.registry.close() } {
            // SAFETY: from `close`.
            unsafe { free_zombie(task) };
        }
        drop(queued);
        while lane.registry.cancel_all() {}
        other.join().unwrap();
        // The wake may have queued the task before the close took the queue (dropped above) or
        // cancelled it itself; a final sweep catches what raced the last pass.
        while lane.registry.cancel_all() {}

        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "cancelled and freed once"
        );
        assert!(lane.registry.is_empty_closed(), "nothing stays linked");
    });
}

/// A spawn from another thread (queued on the injector, unlinked) against the lane's snapshot
/// and its pop: the task shows exactly once while it is queued, and not at all once it
/// completed.
#[test]
fn a_snapshot_counts_a_task_from_another_thread_once() {
    loom::model(|| {
        ON_LANE.with(|on| on.set(true));
        let lane = Lane::new();
        let injector: Arc<Inject<Notified>> = Arc::new(Inject::new());
        let spawner = {
            let (lane, injector) = (lane.clone(), injector.clone());
            thread::spawn(move || {
                let (task, join) = new_joinable_task(poll_fn(|_| Poll::Ready(())), lane);
                drop(join);
                injector.push(task).ok().expect("open");
            })
        };

        // The snapshot, as `Shared::collect_records` takes it.
        let mut counted = lane.live();
        injector.for_each(|queued| {
            if !queued.header().registry.is_linked_relaxed() {
                counted += 1;
            }
        });
        assert!(counted <= 1);

        spawner.join().unwrap();
        // The lane takes it, links it, runs it to its end.
        while let Some(task) = injector.pop() {
            lane.link(&task);
            assert_eq!(lane.live(), 1, "linked once taken");
            task.run();
        }
        assert_eq!(lane.live(), 0, "unlinked at completion");
    });
}
