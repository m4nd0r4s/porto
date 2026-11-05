//! Tasks spawned into a scope: every end (completed, cancelled, panicked, freed while idle,
//! freed by the last join handle) reaches the scope exactly once.
//!
//! Each scope gets one extra child the test holds (`Fixture::_sentinel`), so a task end counted
//! twice shows as `live_children == 0` instead of hiding behind the saturating decrement.

use std::any::Any;
use std::future::{Future, poll_fn};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::tests::{MockScheduler, counter};
use super::{
    Header, JoinError, JoinHandle, Notified, TaskBody, TaskFlags, new_body_task_in,
    new_joinable_task_in,
};
use crate::scope::{CancelReason, ScopeNode, TaskScope};

type Scope = ScopeNode<String>;

/// A scope plus the extra child the test holds.
struct Fixture {
    scope: Arc<Scope>,
    _sentinel: TaskScope,
}

impl Fixture {
    fn new() -> Self {
        let scope = Scope::new_root(0);
        let sentinel = TaskScope::attach(&scope);
        Self {
            scope,
            _sentinel: sentinel,
        }
    }

    /// The scope's live children, the sentinel included.
    fn live(&self) -> u32 {
        self.scope.lock_state().live_children
    }

    fn attach(&self) -> Option<TaskScope> {
        Some(TaskScope::attach(&self.scope))
    }
}

type Spawned = (MockScheduler, Notified, JoinHandle<u32>, Arc<AtomicUsize>);

/// Spawns `poll` into the fixture's scope with a join handle; the future owns a drop counter.
fn spawn_in(
    fixture: &Fixture,
    mut poll: impl FnMut(&Waker) -> Poll<u32> + Send + 'static,
) -> Spawned {
    let sched = MockScheduler::default();
    let (drops, guard) = counter();
    let future = poll_fn(move |cx| {
        let _owned = &guard;
        poll(cx.waker())
    });
    let (task, join) = new_joinable_task_in(future, sched.clone(), fixture.attach());
    (sched, task, join, drops)
}

/// Polls a join handle once with a no-op waker.
fn poll_join(join: &mut JoinHandle<u32>) -> Poll<Result<u32, JoinError>> {
    Pin::new(join).poll(&mut Context::from_waker(Waker::noop()))
}

/// A waker that runs a closure.
struct OnWake<F>(F);

impl<F: Fn() + Send + Sync + 'static> Wake for OnWake<F> {
    fn wake(self: Arc<Self>) {
        (self.0)();
    }
}

fn on_wake(f: impl Fn() + Send + Sync + 'static) -> Waker {
    Waker::from(Arc::new(OnWake(f)))
}

#[test]
fn a_completed_task_ends_in_its_scope_once() {
    let fixture = Fixture::new();
    let (sched, task, mut join, drops) = spawn_in(&fixture, |_| Poll::Ready(7));
    assert_eq!(fixture.live(), 2);
    task.run();
    assert_eq!(fixture.live(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(sched.completed(), 1);
    assert!(matches!(poll_join(&mut join), Poll::Ready(Ok(7))));
    drop(join);
    assert_eq!(fixture.live(), 1, "freeing a completed task ends nothing");
    assert_eq!(fixture.scope.cancel_called(), None);
}

#[test]
fn an_aborted_task_ends_as_cancelled_once() {
    let fixture = Fixture::new();
    let (sched, task, mut join, drops) = spawn_in(&fixture, |_| Poll::Ready(7));
    join.abort();
    task.run();
    assert_eq!(fixture.live(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(sched.completed(), 1);
    assert!(matches!(poll_join(&mut join), Poll::Ready(Err(e)) if e.is_cancelled()));
    drop(join);
    assert_eq!(fixture.live(), 1);
    assert_eq!(
        fixture.scope.cancel_called(),
        None,
        "a cancelled child is no failure"
    );
}

#[test]
fn a_dropped_queue_entry_cancels_the_task_and_ends_it_once() {
    let fixture = Fixture::new();
    let (_sched, task, join, _drops) = spawn_in(&fixture, |_| Poll::Ready(7));
    drop(task);
    assert_eq!(fixture.live(), 1);
    drop(join);
    assert_eq!(fixture.live(), 1);
}

#[test]
fn a_panicking_task_fails_its_scope_once() {
    let fixture = Fixture::new();
    let (_sched, task, mut join, _drops) = spawn_in(&fixture, |_| panic!("child failed"));
    task.run();
    assert_eq!(fixture.live(), 1);
    assert_eq!(
        fixture.scope.cancel_called(),
        Some(CancelReason::ChildFailed)
    );
    assert!(
        fixture.scope.lock_state().errors.is_empty(),
        "the panic payload went to the join handle"
    );
    assert!(matches!(poll_join(&mut join), Poll::Ready(Err(e)) if e.is_panic()));
    drop(join);
    assert_eq!(fixture.live(), 1);
}

#[test]
fn an_unjoined_panic_fails_the_scope_and_is_still_resumed() {
    let fixture = Fixture::new();
    let (_sched, task, join, _drops) = spawn_in(&fixture, |_| panic!("child failed"));
    drop(join);
    let resumed = panic::catch_unwind(AssertUnwindSafe(|| task.run()));
    assert!(resumed.is_err());
    assert_eq!(fixture.live(), 1);
    assert_eq!(
        fixture.scope.cancel_called(),
        Some(CancelReason::ChildFailed)
    );
}

#[test]
fn a_task_freed_by_its_last_join_handle_ends_once_after_its_future_is_dropped() {
    let scope = Scope::new_root(0);
    let (drops, guard) = counter();
    let checked = Arc::new(AtomicUsize::new(0));
    {
        // The owner waits in `ScopeExit`: its waker checks the future is gone by then.
        let drops = Arc::clone(&drops);
        let checked = Arc::clone(&checked);
        scope.lock_state().exit_waiter = Some(on_wake(move || {
            assert_eq!(drops.load(Ordering::SeqCst), 1, "future dropped first");
            checked.fetch_add(1, Ordering::SeqCst);
        }));
    }
    let sched = MockScheduler::default();
    // Never keeps its waker: after the first poll only the join handle references it.
    let future = poll_fn(move |_| {
        let _owned = &guard;
        Poll::<u32>::Pending
    });
    let (task, join) = new_joinable_task_in(future, sched.clone(), Some(TaskScope::attach(&scope)));
    task.run();
    assert_eq!(scope.lock_state().live_children, 1);
    assert_eq!(checked.load(Ordering::SeqCst), 0);
    // The join handle is the last owner: dropping it frees the idle task.
    drop(join);
    assert_eq!(scope.lock_state().live_children, 0);
    assert_eq!(checked.load(Ordering::SeqCst), 1);
    assert_eq!(sched.completed(), 0, "a freed task never completed");
    assert_eq!(scope.cancel_called(), None);
    assert_eq!(Arc::strong_count(&scope), 1, "the task released the scope");
}

#[test]
fn a_detached_task_freed_at_poll_end_ends_once() {
    let fixture = Fixture::new();
    let (sched, task, join, drops) = spawn_in(&fixture, |_| Poll::Pending);
    drop(join);
    // The poll returns `Pending` with no waker kept: `IdleAction::Dealloc`.
    task.run();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.live(), 1);
    assert_eq!(sched.completed(), 0);
}

#[test]
fn a_task_kept_by_its_waker_ends_when_the_waker_goes() {
    let fixture = Fixture::new();
    let parked: Arc<Mutex<Option<Waker>>> = Arc::default();
    let slot = Arc::clone(&parked);
    let (_sched, task, join, _drops) = spawn_in(&fixture, move |waker| {
        *slot.lock().unwrap() = Some(waker.clone());
        Poll::Pending
    });
    drop(join);
    task.run();
    assert_eq!(fixture.live(), 2, "idle, not ended");
    let waker = parked.lock().unwrap().take();
    drop(waker);
    assert_eq!(fixture.live(), 1);
}

#[test]
fn the_exit_waiter_wakes_when_the_last_child_ends() {
    let scope = Scope::new_root(0);
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&wakes);
    scope.lock_state().exit_waiter = Some(on_wake(move || {
        counted.fetch_add(1, Ordering::SeqCst);
    }));
    let sched = MockScheduler::default();
    let (first, first_join) = new_joinable_task_in(
        poll_fn(|_| Poll::Ready(1)),
        sched.clone(),
        Some(TaskScope::attach(&scope)),
    );
    let (second, second_join) = new_joinable_task_in(
        poll_fn(|_| Poll::Ready(2)),
        sched.clone(),
        Some(TaskScope::attach(&scope)),
    );
    first.run();
    assert_eq!(wakes.load(Ordering::SeqCst), 0);
    second.run();
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert!(scope.lock_state().exit_waiter.is_none());
    drop((first_join, second_join));
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
}

#[test]
fn a_panicking_exit_waiter_still_releases_the_task() {
    let scope = Scope::new_root(0);
    scope.lock_state().exit_waiter = Some(on_wake(|| panic!("the exit waiter panicked")));
    let sched = MockScheduler::default();
    let (drops, guard) = counter();
    let (task, join) = new_joinable_task_in(
        poll_fn(move |_| {
            let _owned = &guard;
            Poll::Ready(1)
        }),
        sched.clone(),
        Some(TaskScope::attach(&scope)),
    );
    let resumed = panic::catch_unwind(AssertUnwindSafe(|| task.run()));
    assert!(resumed.is_err(), "the waker's panic goes on from the lane");
    assert_eq!(scope.lock_state().live_children, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(sched.completed(), 1);
    drop(join);
    assert_eq!(Arc::strong_count(&scope), 1, "the task released the scope");
}

#[test]
fn a_task_releases_its_scope_reference_when_it_completes() {
    let fixture = Fixture::new();
    // The fixture's `Arc` plus the sentinel's reference.
    assert_eq!(Arc::strong_count(&fixture.scope), 2);
    let (_sched, task, join, _drops) = spawn_in(&fixture, |_| Poll::Ready(7));
    assert_eq!(Arc::strong_count(&fixture.scope), 3);
    task.run();
    assert_eq!(
        Arc::strong_count(&fixture.scope),
        2,
        "released at completion, not when the task is freed"
    );
    drop(join);
}

/// What a test body does when polled.
#[derive(Clone, Copy)]
enum BodyScript {
    /// Finish with no failure.
    Succeed,
    /// Finish, reporting a failure the scope keeps (a `String`).
    Fail,
    /// Finish, reporting a failure of another type.
    FailWithOther,
    /// Panic.
    Panic,
    /// Stay pending without keeping the waker.
    Idle,
}

struct ScriptedBody {
    script: BodyScript,
    take_failures: Arc<AtomicUsize>,
}

impl TaskBody for ScriptedBody {
    fn poll(self: Pin<&mut Self>, _: &Header, _: &mut Context<'_>) -> Poll<()> {
        match self.script {
            BodyScript::Panic => panic!("the body panicked"),
            BodyScript::Idle => Poll::Pending,
            _ => Poll::Ready(()),
        }
    }

    fn cancel(self: Pin<&mut Self>, _: &Header) {}

    fn take_failure(self: Pin<&mut Self>) -> Option<Box<dyn Any + Send>> {
        self.take_failures.fetch_add(1, Ordering::SeqCst);
        match self.script {
            BodyScript::Fail => Some(Box::new(String::from("body failed"))),
            BodyScript::FailWithOther => Some(Box::new(42_u8)),
            _ => None,
        }
    }
}

fn scripted(script: BodyScript) -> (ScriptedBody, Arc<AtomicUsize>) {
    let take_failures = Arc::new(AtomicUsize::new(0));
    let body = ScriptedBody {
        script,
        take_failures: Arc::clone(&take_failures),
    };
    (body, take_failures)
}

fn spawn_body_in(fixture: &Fixture, script: BodyScript) -> (Notified, Arc<AtomicUsize>) {
    let (body, take_failures) = scripted(script);
    let (task, _body) = new_body_task_in(
        body,
        MockScheduler::default(),
        TaskFlags::PYTHON,
        fixture.attach(),
    );
    (task, take_failures)
}

#[test]
fn a_body_that_succeeds_ends_once() {
    let fixture = Fixture::new();
    let (task, take_failures) = spawn_body_in(&fixture, BodyScript::Succeed);
    task.run();
    assert_eq!(fixture.live(), 1);
    assert_eq!(take_failures.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.scope.cancel_called(), None);
}

#[test]
fn a_body_failure_is_recorded_and_cancels_the_scope() {
    let fixture = Fixture::new();
    let (task, _) = spawn_body_in(&fixture, BodyScript::Fail);
    task.run();
    assert_eq!(fixture.live(), 1);
    assert_eq!(fixture.scope.lock_state().errors, ["body failed"]);
    assert_eq!(
        fixture.scope.cancel_called(),
        Some(CancelReason::ChildFailed)
    );
}

#[test]
fn a_body_failure_of_another_type_cancels_but_records_nothing() {
    let fixture = Fixture::new();
    let (task, _) = spawn_body_in(&fixture, BodyScript::FailWithOther);
    task.run();
    assert_eq!(fixture.live(), 1);
    assert!(fixture.scope.lock_state().errors.is_empty());
    assert_eq!(
        fixture.scope.cancel_called(),
        Some(CancelReason::ChildFailed)
    );
}

#[test]
fn a_panicking_body_fails_its_scope_once() {
    let fixture = Fixture::new();
    let (task, take_failures) = spawn_body_in(&fixture, BodyScript::Panic);
    assert!(panic::catch_unwind(AssertUnwindSafe(|| task.run())).is_err());
    assert_eq!(fixture.live(), 1);
    assert_eq!(take_failures.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture.scope.cancel_called(),
        Some(CancelReason::ChildFailed)
    );
}

#[test]
fn a_cancelled_body_ends_once_without_asking_for_a_failure() {
    let fixture = Fixture::new();
    let (task, take_failures) = spawn_body_in(&fixture, BodyScript::Fail);
    drop(task);
    assert_eq!(fixture.live(), 1);
    assert_eq!(take_failures.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.scope.cancel_called(), None);
}

#[test]
fn an_idle_body_freed_at_poll_end_ends_once() {
    let fixture = Fixture::new();
    let (task, take_failures) = spawn_body_in(&fixture, BodyScript::Idle);
    task.run();
    assert_eq!(fixture.live(), 1);
    assert_eq!(take_failures.load(Ordering::SeqCst), 0);
}

#[test]
fn a_body_outside_a_scope_is_never_asked_for_a_failure() {
    let (body, take_failures) = scripted(BodyScript::Fail);
    let (task, body) = new_body_task_in(body, MockScheduler::default(), TaskFlags::PYTHON, None);
    task.run();
    assert!(body.is_complete());
    assert_eq!(take_failures.load(Ordering::SeqCst), 0);
}
