//! loom models of the lane's parking protocol (`park::Parking`) against the injector: a push
//! or a root wake from another thread never leaves the lane parked with work queued.
//!
//! A lost wakeup shows up as the lane parked forever, which loom reports as a deadlock. The
//! models with three threads run under a preemption bound of 3 (`LOOM_MAX_PREEMPTIONS`
//! overrides it), as the channel's do: unbounded, they do not finish.
//!
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sched`

use std::sync::atomic::Ordering;

use loom::model::Builder;
use loom::sync::Arc;
use loom::sync::atomic::AtomicBool;
use loom::thread;

use super::inject::Inject;
use super::park::Parking;
use crate::sync::primitives::{current_thread, park};

/// The lane and the queue it drains.
struct Lane {
    inject: Inject<u32>,
    parking: Parking,
    /// Stands in for the `block_on` future's waker flag.
    root_woken: AtomicBool,
}

impl Lane {
    fn new() -> Arc<Self> {
        let lane = Arc::new(Self {
            inject: Inject::new(),
            parking: Parking::new(),
            root_woken: AtomicBool::new(false),
        });
        lane.parking.set_thread(current_thread());
        lane
    }

    /// What `Schedule::schedule_remote` does: push, then unpark the lane if it may be parked.
    fn push_remote(&self, item: u32) {
        self.inject.push(item).unwrap();
        if self.parking.notify() {
            self.parking.unpark_thread();
        }
    }

    /// What the root waker does (`RootWaker::wake_by_ref`).
    fn wake_root(&self) {
        self.root_woken.swap(true, Ordering::AcqRel);
        if self.parking.notify() {
            self.parking.unpark_thread();
        }
    }

    /// The lane loop: takes work until `items` were popped (and the root was woken, if
    /// `wait_root`), parking whenever it finds none.
    fn run(&self, items: usize, wait_root: bool) {
        let mut popped = 0;
        let mut root_seen = !wait_root;
        loop {
            while self.inject.pop().is_some() {
                popped += 1;
            }
            root_seen |= self.root_woken.swap(false, Ordering::AcqRel);
            if popped == items && root_seen {
                return;
            }
            // The lane's early out (`Lane::has_work`): a relaxed look, which may miss a push
            // in flight; the parking word is what catches it.
            if self.inject.is_empty() && !self.root_woken.load(Ordering::Relaxed) {
                self.parking.park(park);
            }
        }
    }
}

/// Explores `f` with at most 3 preemptions, unless `LOOM_MAX_PREEMPTIONS` says otherwise.
fn model_bounded(f: impl Fn() + Sync + Send + 'static) {
    let mut builder = Builder::new();
    builder.preemption_bound.get_or_insert(3);
    builder.check(f);
}

#[test]
fn a_remote_push_wakes_a_parking_lane() {
    loom::model(|| {
        let lane = Lane::new();
        let pusher = {
            let lane = lane.clone();
            thread::spawn(move || lane.push_remote(1))
        };
        lane.run(1, false);
        pusher.join().unwrap();
    });
}

#[test]
fn two_remote_pushers_both_reach_the_lane() {
    model_bounded(|| {
        let lane = Lane::new();
        let pushers: Vec<_> = (0..2)
            .map(|item| {
                let lane = lane.clone();
                thread::spawn(move || lane.push_remote(item))
            })
            .collect();
        lane.run(2, false);
        for pusher in pushers {
            pusher.join().unwrap();
        }
    });
}

#[test]
fn a_push_and_a_root_wake_race_the_park() {
    model_bounded(|| {
        let lane = Lane::new();
        let pusher = {
            let lane = lane.clone();
            thread::spawn(move || lane.push_remote(1))
        };
        let waker = {
            let lane = lane.clone();
            thread::spawn(move || lane.wake_root())
        };
        lane.run(1, true);
        pusher.join().unwrap();
        waker.join().unwrap();
    });
}

#[test]
fn a_push_after_the_lane_parked_once_still_wakes_it() {
    loom::model(|| {
        let lane = Lane::new();
        let pusher = {
            let lane = lane.clone();
            thread::spawn(move || {
                lane.push_remote(1);
                lane.push_remote(2);
            })
        };
        lane.run(2, false);
        pusher.join().unwrap();
    });
}
