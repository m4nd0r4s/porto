//! The task registry (`23-observability.md` §1) on real runtimes: links and unlinks, the
//! snapshot handshake, tasks freed off the lane, and the shutdown that cancels what wakers hold.

use std::future::{Future, poll_fn};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};
use std::thread;
use std::time::Duration;

use crate::registry::{SnapshotError, TaskKind, TaskRecord, TaskState};
use crate::scope::ScopeNode;
use crate::{Handle, Runtime};

/// Tasks wait here until `open`: each parks its waker.
#[derive(Default)]
struct Gate {
    open: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl Gate {
    fn wait(self: &Arc<Self>) -> impl Future<Output = ()> + Send + use<> {
        let gate = Arc::clone(self);
        poll_fn(move |cx| {
            if gate.open.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            gate.wakers.lock().unwrap().push(cx.waker().clone());
            Poll::Pending
        })
    }

    fn open(&self) {
        self.open.store(true, Ordering::Release);
        for waker in std::mem::take(&mut *self.wakers.lock().unwrap()) {
            waker.wake();
        }
    }
}

/// Returns `Pending` once, waking itself: lets the other tasks run.
async fn yield_now() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

/// Counts drops.
struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Yields until no task is queued any more: every task ran to its wait (or its end).
async fn settle(handle: &Handle) {
    for _ in 0..1_000 {
        let records = handle.task_snapshot().unwrap();
        if records
            .iter()
            .all(|record| record.state != TaskState::Scheduled)
        {
            return;
        }
        for _ in 0..1_000 {
            yield_now().await;
        }
    }
    panic!("the tasks never settled");
}

fn ids(records: &[TaskRecord]) -> Vec<u64> {
    records.iter().map(|record| record.id).collect()
}

#[test]
fn a_snapshot_of_ten_thousand_live_tasks_is_exactly_the_live_set() {
    let runtime = Runtime::new();
    let gate = Arc::new(Gate::default());
    runtime.block_on(async {
        let handle = Handle::try_current().unwrap();
        let joins: Vec<_> = (0..10_000).map(|_| crate::spawn(gate.wait())).collect();
        settle(&handle).await;
        let mut expected: Vec<u64> = joins.iter().map(crate::task::JoinHandle::id).collect();
        expected.sort_unstable();

        let records = handle.task_snapshot().unwrap();
        assert_eq!(ids(&records), expected, "exactly the live tasks, by id");
        assert!(
            records
                .iter()
                .all(|record| record.kind == TaskKind::Rust && record.state == TaskState::Waiting),
            "every task parked on the gate"
        );

        // Half complete: the snapshot shrinks to the rest.
        gate.open();
        for join in joins {
            join.await.unwrap();
        }
        assert!(handle.task_snapshot().unwrap().is_empty());
    });
}

#[test]
fn a_task_leaves_the_registry_when_it_completes_or_is_cancelled() {
    let runtime = Runtime::new();
    let gate = Arc::new(Gate::default());
    runtime.block_on(async {
        let handle = Handle::try_current().unwrap();
        let finished = crate::spawn(async { 1 });
        let parked = crate::spawn(gate.wait());
        let cancelled = crate::spawn(gate.wait());
        settle(&handle).await;
        assert_eq!(finished.await.unwrap(), 1);
        assert_eq!(handle.task_snapshot().unwrap().len(), 2);
        cancelled.abort();
        yield_now().await;
        assert!(cancelled.await.unwrap_err().is_cancelled());
        let left = handle.task_snapshot().unwrap();
        assert_eq!(ids(&left), vec![parked.id()]);
        gate.open();
        parked.await.unwrap();
        assert!(handle.task_snapshot().unwrap().is_empty());
    });
}

#[test]
fn a_task_freed_while_idle_leaves_the_registry_on_its_lane() {
    let runtime = Runtime::new();
    runtime.block_on(async {
        let handle = Handle::try_current().unwrap();
        // Nothing can ever wake it: its waker is dropped, so the poll frees it.
        crate::spawn(std::future::pending::<()>());
        settle(&handle).await;
        assert!(handle.task_snapshot().unwrap().is_empty());
    });
}

#[test]
fn a_snapshot_names_the_scope_and_the_state() {
    let runtime = Runtime::new();
    let gate = Arc::new(Gate::default());
    let scope = ScopeNode::<()>::new_root(1);
    runtime.block_on(async {
        let handle = Handle::try_current().unwrap();
        let _in_scope = handle.spawn_in(&scope, gate.wait()).unwrap();
        let _outside = crate::spawn(gate.wait());
        let ran = crate::spawn(async {
            // A task sees itself running.
            Handle::try_current().unwrap().task_snapshot().unwrap()
        });
        settle(&handle).await;
        let records = handle.task_snapshot().unwrap();
        let by_scope: Vec<_> = records.iter().map(|record| record.scope_id).collect();
        assert_eq!(
            by_scope
                .iter()
                .filter(|id| **id == Some(scope.id()))
                .count(),
            1
        );
        assert_eq!(by_scope.iter().filter(|id| id.is_none()).count(), 1);
        let seen_by_itself = ran.await.unwrap();
        let running: Vec<_> = seen_by_itself
            .iter()
            .filter(|record| record.state == TaskState::Running)
            .collect();
        assert_eq!(running.len(), 1, "a task sees itself running, once");
        gate.open();
    });
}

#[test]
fn a_snapshot_requested_from_another_thread_is_taken_by_the_running_lane() {
    let runtime = Runtime::new();
    let gate = Arc::new(Gate::default());
    let handle = runtime.handle().clone();
    let requester = runtime.block_on(async {
        // Requested while the lane runs: another thread waits for it.
        let requester = thread::spawn(move || handle.task_snapshot());
        let joins: Vec<_> = (0..100).map(|_| crate::spawn(gate.wait())).collect();
        settle(&Handle::try_current().unwrap()).await;
        // The lane takes the request at a maintenance tick, or when it parks: let it run on.
        let mut waits = 0;
        while !requester.is_finished() {
            crate::time::sleep_until(crate::time::Deadline::after(Duration::from_millis(1))).await;
            waits += 1;
            assert!(waits < 5_000, "the lane never took the snapshot");
        }
        gate.open();
        for join in joins {
            join.await.unwrap();
        }
        requester
    });
    let records = requester.join().unwrap().unwrap();
    assert_eq!(records.len(), 100);
}

#[test]
fn a_snapshot_of_a_lane_that_is_not_running_is_an_error_not_a_hang() {
    let runtime = Runtime::new();
    // The owner thread outside `block_on`: nothing runs the lane.
    assert_eq!(
        runtime.handle().task_snapshot().unwrap_err(),
        SnapshotError::LaneNotRunning
    );
    let handle = runtime.handle().clone();
    let other = thread::spawn(move || handle.task_snapshot());
    assert_eq!(
        other.join().unwrap().unwrap_err(),
        SnapshotError::LaneNotRunning
    );
    // The same request is fine while the lane runs it (from the lane's own thread).
    runtime.block_on(async {
        assert!(
            Handle::try_current()
                .unwrap()
                .task_snapshot()
                .unwrap()
                .is_empty()
        );
    });
    // And a request made without waiting is taken when it next runs.
    let request = runtime.handle().request_task_snapshot();
    assert!(request.try_take().is_none());
    runtime.run_until_idle();
    assert!(request.try_take().unwrap().unwrap().is_empty());
}

#[test]
fn a_walk_at_shutdown_sees_the_tasks_a_cancelled_task_leaves_behind() {
    /// Counts the live tasks as it is dropped, which is while the runtime cancels it.
    struct CountsOnDrop(Handle, Arc<AtomicUsize>);
    impl Drop for CountsOnDrop {
        fn drop(&mut self) {
            let mut live = 0;
            let walked = self.0.with_live_tasks_at_shutdown(None, |_| {
                live += 1;
                true
            });
            assert!(walked, "the registry is closed by then");
            self.1.store(live, Ordering::SeqCst);
        }
    }
    let runtime = Runtime::new();
    let gate = Arc::new(Gate::default());
    let seen = Arc::new(AtomicUsize::new(usize::MAX));
    // The newest task is cancelled first (the registry is walked newest first): the older ones
    // are still parked when it looks.
    drop(runtime.spawn(gate.wait()));
    drop(runtime.spawn(gate.wait()));
    let counter = CountsOnDrop(runtime.handle().clone(), seen.clone());
    drop(runtime.spawn(async move {
        let _counter = counter;
        gate.wait().await;
    }));
    runtime.run_until_idle();
    assert!(
        !runtime.handle().with_live_tasks_at_shutdown(None, |_| true),
        "not closed while the runtime lives"
    );
    drop(runtime);
    assert_eq!(
        seen.load(Ordering::SeqCst),
        2,
        "the two parked tasks that were left"
    );
}

#[test]
fn with_live_tasks_while_stops_where_the_reader_says() {
    let runtime = Runtime::new();
    let gate = Arc::new(Gate::default());
    runtime.block_on(async {
        let handle = Handle::try_current().unwrap();
        let joins: Vec<_> = (0..50).map(|_| crate::spawn(gate.wait())).collect();
        settle(&handle).await;
        let mut visited = 0;
        assert!(handle.with_live_tasks_while(|_| {
            visited += 1;
            visited < 3
        }));
        assert_eq!(visited, 3);
        gate.open();
        for join in joins {
            join.await.unwrap();
        }
    });
}

#[test]
fn a_task_spawned_from_another_thread_counts_while_queued_and_after() {
    let runtime = Runtime::new();
    let gate = Arc::new(Gate::default());
    let handle = runtime.handle().clone();
    let join = thread::spawn({
        let gate = Arc::clone(&gate);
        move || handle.spawn(gate.wait())
    })
    .join()
    .unwrap();
    // Still on the injector: no lane linked it, and the snapshot counts it anyway.
    runtime.block_on(async {
        let handle = Handle::try_current().unwrap();
        let queued = handle.task_snapshot().unwrap();
        assert_eq!(ids(&queued), vec![join.id()]);
        assert_eq!(queued[0].state, TaskState::Scheduled);
        settle(&handle).await;
        // The lane took it off the injector, linked it, and ran it to its wait.
        let parked = handle.task_snapshot().unwrap();
        assert_eq!(ids(&parked), vec![join.id()]);
        assert_eq!(parked[0].state, TaskState::Waiting);
        gate.open();
        join.await.unwrap();
        assert!(handle.task_snapshot().unwrap().is_empty());
    });
}

#[test]
fn with_live_tasks_walks_the_lane_registry_on_the_lane_only() {
    let runtime = Runtime::new();
    let gate = Arc::new(Gate::default());
    let handle = runtime.handle().clone();
    assert!(
        !handle.with_live_tasks(|_| unreachable!()),
        "not on the lane"
    );
    runtime.block_on(async {
        let joins: Vec<_> = (0..5).map(|_| crate::spawn(gate.wait())).collect();
        settle(&handle).await;
        let mut seen = Vec::new();
        assert!(handle.with_live_tasks(|task| seen.push(task.id())));
        seen.sort_unstable();
        let expected: Vec<u64> = joins.iter().map(crate::task::JoinHandle::id).collect();
        assert_eq!(seen, expected);
        gate.open();
    });
}

#[test]
fn a_task_freed_by_a_waker_dropped_off_the_lane_is_freed_by_the_lane() {
    let runtime = Runtime::new();
    let dropped = Arc::new(AtomicUsize::new(0));
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    {
        let (counter, stash) = (DropCount(Arc::clone(&dropped)), Arc::clone(&stash));
        // Parks with its waker in `stash` and holds nothing else alive: the stash is its only
        // reference, since the join handle goes at once.
        drop(runtime.spawn(async move {
            poll_fn(|cx| {
                *stash.lock().unwrap() = Some(cx.waker().clone());
                Poll::<()>::Pending
            })
            .await;
            drop(counter);
        }));
    }
    runtime.run_until_idle();
    assert_eq!(dropped.load(Ordering::SeqCst), 0, "parked, not freed");
    // The last reference goes on another thread: not the lane's, which owns the list.
    let waker = stash.lock().unwrap().take().unwrap();
    thread::spawn(move || drop(waker)).join().unwrap();
    // Handed to the lane, which frees it at its next maintenance tick.
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        0,
        "the lane frees it, not the thread"
    );
    runtime.run_until_idle();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    runtime.block_on(async {
        assert!(
            Handle::try_current()
                .unwrap()
                .task_snapshot()
                .unwrap()
                .is_empty()
        );
    });
}

/// Tasks that wait on something that holds their own waker: nothing but the runtime's end can
/// free them, and the end frees them for good (no `mem::forget` here: LeakSanitizer, in the
/// nightly sanitizers job, reports a task, its handle or its runtime that outlives it).
#[test]
fn dropping_the_runtime_cancels_tasks_kept_alive_by_a_waker_cycle() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new();
    // Each task stores its waker in a cell the future owns: task -> future -> cell -> waker ->
    // task. Only a `Weak` stays outside, to see whether the cycle was freed.
    let cells: Vec<Weak<Mutex<Option<Waker>>>> = (0..50)
        .map(|_| {
            let cell: Arc<Mutex<Option<Waker>>> = Arc::default();
            let (counter, owned) = (DropCount(Arc::clone(&dropped)), Arc::clone(&cell));
            drop(runtime.spawn(async move {
                poll_fn(|cx| {
                    *owned.lock().unwrap() = Some(cx.waker().clone());
                    Poll::<()>::Pending
                })
                .await;
                drop(counter);
            }));
            Arc::downgrade(&cell)
        })
        .collect();
    runtime.run_until_idle();
    assert_eq!(dropped.load(Ordering::SeqCst), 0, "all parked");
    assert!(
        cells.iter().all(|cell| cell.strong_count() == 1),
        "every cycle is alive: the future owns its cell"
    );
    drop(runtime);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        50,
        "the runtime's end cancelled every task its wakers kept alive"
    );
    assert!(
        cells.iter().all(|cell| cell.strong_count() == 0),
        "the cancelled futures freed their cells, and with them the wakers and the tasks"
    );
}

#[test]
fn a_wake_after_the_runtime_is_dropped_finds_the_task_already_cancelled() {
    let runtime = Runtime::new();
    let stash: Arc<Mutex<Option<Waker>>> = Arc::default();
    let owned = Arc::clone(&stash);
    let join = runtime.spawn(async move {
        poll_fn(|cx| {
            *owned.lock().unwrap() = Some(cx.waker().clone());
            Poll::<()>::Pending
        })
        .await;
    });
    runtime.run_until_idle();
    drop(runtime);
    assert!(join.is_finished(), "cancelled by the runtime's end");
    // Waking what is complete does nothing, from any thread.
    let waker = stash.lock().unwrap().take().unwrap();
    thread::spawn(move || waker.wake()).join().unwrap();
}

#[test]
fn shutdown_drops_tasks_spawned_from_other_threads_that_no_lane_took() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new();
    let handle = runtime.handle().clone();
    let counter = DropCount(Arc::clone(&dropped));
    let join = thread::spawn(move || {
        handle.spawn(async move {
            drop(counter);
        })
    })
    .join()
    .unwrap();
    drop(runtime);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(join.is_finished());
}

#[test]
fn the_introspection_switch_is_off_by_default() {
    assert!(!Runtime::new().handle().introspection());
    assert!(
        Runtime::builder()
            .introspection(true)
            .build()
            .handle()
            .introspection()
    );
}
