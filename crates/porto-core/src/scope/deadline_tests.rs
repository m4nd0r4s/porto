//! Scope deadline tests (C9), on a hand-driven clock: the wheel is fired by the test, so
//! nothing here depends on real time.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::{CancelReason, ScopeNode};
use crate::sched::Handle;
use crate::time::{Clock, Deadline, RuntimeClock};

type Scope = ScopeNode<&'static str>;

/// A clock that moves only when the test says so.
#[derive(Clone, Default)]
struct HandClock(Arc<AtomicU64>);

impl HandClock {
    fn set(&self, millis: u64) {
        self.0.store(millis * 1_000_000, Ordering::SeqCst);
    }
}

impl Clock for HandClock {
    fn now(&self) -> Deadline {
        Deadline::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

/// A one-lane runtime handle on `clock`, 1 ms ticks, no driver: its wheel is fired by `fire`.
fn handle(clock: &HandClock) -> Handle {
    Handle::new(
        RuntimeClock::Custom(Box::new(clock.clone())),
        Duration::from_millis(1),
        None,
        1,
        crate::blocking::BlockingConfig::default(),
    )
}

/// Fires every timer due on `handle`'s clock, as the lane does before it parks.
fn fire(handle: &Handle) -> usize {
    handle.shared.timers.fire_expired(|| handle.now())
}

fn at(millis: u64) -> Deadline {
    Deadline::from_nanos(millis * 1_000_000)
}

#[test]
fn a_deadline_cancels_the_scope_once_it_passed_and_never_before() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    Scope::set_deadline(&scope, &handle, at(10));
    assert_eq!(scope.deadline(), at(10));
    assert_eq!(handle.armed_timers(), 1);
    clock.set(9);
    assert_eq!(fire(&handle), 0);
    assert_eq!(scope.cancel_called(), None, "fired early");
    clock.set(10);
    assert_eq!(fire(&handle), 1);
    assert_eq!(scope.cancel_called(), Some(CancelReason::Deadline));
    assert_eq!(handle.armed_timers(), 0);
    assert_eq!(
        Arc::strong_count(&scope),
        1,
        "the fired waker kept a reference"
    );
}

#[test]
fn a_deadline_already_passed_cancels_at_once() {
    let clock = HandClock::default();
    clock.set(5);
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    Scope::set_deadline(&scope, &handle, at(5));
    assert_eq!(scope.cancel_called(), Some(CancelReason::Deadline));
    assert_eq!(handle.armed_timers(), 0);
}

#[test]
fn setting_the_deadline_again_moves_the_timer() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    Scope::set_deadline(&scope, &handle, at(10));
    Scope::set_deadline(&scope, &handle, at(20));
    assert_eq!(handle.armed_timers(), 1);
    clock.set(15);
    fire(&handle);
    assert_eq!(scope.cancel_called(), None, "the old deadline fired");
    Scope::set_deadline(&scope, &handle, at(12));
    assert_eq!(
        scope.cancel_called(),
        Some(CancelReason::Deadline),
        "moved into the past"
    );
    assert_eq!(handle.armed_timers(), 0);
}

#[test]
fn removing_the_deadline_disarms_it() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    Scope::set_deadline(&scope, &handle, at(10));
    Scope::set_deadline(&scope, &handle, Deadline::NEVER);
    assert_eq!(handle.armed_timers(), 0);
    assert_eq!(Arc::strong_count(&scope), 1);
    clock.set(100);
    fire(&handle);
    assert_eq!(scope.cancel_called(), None);
}

#[test]
fn closing_the_scope_disarms_its_deadline() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    Scope::set_deadline(&scope, &handle, at(10));
    drop(scope.close());
    assert_eq!(handle.armed_timers(), 0);
    assert_eq!(Arc::strong_count(&scope), 1);
    Scope::set_deadline(&scope, &handle, at(20));
    assert_eq!(handle.armed_timers(), 0, "a closed scope re-armed");
    assert_eq!(scope.deadline(), at(20));
}

#[test]
fn a_cancelled_scope_arms_no_deadline() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    Scope::set_deadline(&scope, &handle, at(10));
    assert!(scope.cancel(CancelReason::Explicit));
    clock.set(10);
    fire(&handle);
    assert_eq!(
        scope.cancel_called(),
        Some(CancelReason::Explicit),
        "the first reason wins"
    );
    Scope::set_deadline(&scope, &handle, at(30));
    assert_eq!(handle.armed_timers(), 0);
}

#[test]
fn an_outer_deadline_reaches_nested_scopes_but_not_a_shielded_one() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let root = Scope::new_root(0);
    let outer = ScopeNode::new_child(&root, 0, false);
    let inner = ScopeNode::new_child(&outer, 0, false);
    let shield = ScopeNode::new_child(&outer, 0, true);
    Scope::set_deadline(&outer, &handle, at(10));
    Scope::set_deadline(&inner, &handle, at(50));
    clock.set(10);
    fire(&handle);
    let found = inner.effective_cancellation().unwrap();
    assert_eq!(found.scope.id(), outer.id());
    assert_eq!(found.reason, CancelReason::Deadline);
    assert!(shield.effective_cancellation().is_none());
    assert_eq!(
        inner.cancel_called(),
        None,
        "the inner deadline has not passed"
    );
    drop(inner.close());
    assert_eq!(handle.armed_timers(), 0);
}

#[test]
fn a_scope_with_a_linked_deadline_is_freed_with_the_wheel() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    Scope::set_deadline(&scope, &handle, at(10));
    let weak = Arc::downgrade(&scope);
    drop(scope);
    assert!(
        weak.upgrade().is_some(),
        "the wheel holds the scope while linked"
    );
    assert!(handle.shared.timers.drop_all());
    assert!(weak.upgrade().is_none());
}

#[test]
fn a_deadline_set_on_a_dropped_runtimes_wheel_links_nothing_and_leaks_nothing() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    // What `Runtime::drop` does.
    handle.shared.timers.drop_all();
    Scope::set_deadline(&scope, &handle, at(10));
    assert_eq!(handle.armed_timers(), 0);
    assert_eq!(scope.deadline(), at(10), "the deadline is still recorded");
    assert_eq!(Arc::strong_count(&scope), 1, "the waker kept a reference");
    // And once the runtime is gone, the scope holds no state of it.
    let weak = handle.downgrade();
    drop(handle);
    assert!(weak.upgrade().is_none(), "the scope kept the runtime alive");
    drop(scope.close());
}

#[test]
fn an_armed_scope_does_not_keep_its_runtime_alive_once_the_runtime_dropped_its_timers() {
    let clock = HandClock::default();
    let handle = handle(&clock);
    let scope = Scope::new_root(0);
    Scope::set_deadline(&scope, &handle, at(10));
    let weak = handle.downgrade();
    // What `Runtime::drop` does first, then its last handle goes.
    assert!(handle.shared.timers.drop_all());
    drop(handle);
    assert!(weak.upgrade().is_none(), "the scope kept the runtime alive");
    assert_eq!(Arc::strong_count(&scope), 1);
    drop(scope.close());
}
