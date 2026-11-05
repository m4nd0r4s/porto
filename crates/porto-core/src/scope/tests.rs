//! Scope tree and effective-cancellation tests.

use std::sync::{Arc, Barrier};
use std::thread;

use super::{CancelCache, CancelReason, ScopeNode};

type Scope = ScopeNode<&'static str>;

fn child(parent: &Arc<Scope>) -> Arc<Scope> {
    ScopeNode::new_child(parent, 0, false)
}

fn shielded(parent: &Arc<Scope>) -> Arc<Scope> {
    ScopeNode::new_child(parent, 0, true)
}

fn cancelled(scope: &Scope) -> bool {
    scope.effective_cancellation().is_some()
}

#[test]
fn a_new_tree_is_not_cancelled() {
    let root = Scope::new_root(1);
    let inner = child(&child(&root));
    assert!(!cancelled(&root));
    assert!(!cancelled(&inner));
    assert_eq!(root.owner_task(), 1);
    assert_eq!(inner.parent().unwrap().parent().unwrap().id(), root.id());
}

#[test]
fn cancelling_a_scope_cancels_everything_nested_in_it() {
    let root = Scope::new_root(0);
    let outer = child(&root);
    let middle = child(&outer);
    let inner = child(&middle);
    assert!(middle.cancel(CancelReason::Explicit));
    assert!(!cancelled(&root));
    assert!(!cancelled(&outer));
    let found = inner.effective_cancellation().unwrap();
    assert_eq!(found.scope.id(), middle.id());
    assert_eq!(found.reason, CancelReason::Explicit);
    assert_eq!(found.reason_for(&inner), CancelReason::Parent);
    assert_eq!(found.reason_for(&middle), CancelReason::Explicit);
    assert_eq!(inner.cancel_called(), None);
}

#[test]
fn the_first_cancel_reason_wins() {
    let root = Scope::new_root(0);
    assert!(root.cancel(CancelReason::Deadline));
    assert!(!root.cancel(CancelReason::Explicit));
    assert_eq!(root.cancel_called(), Some(CancelReason::Deadline));
}

#[test]
fn a_shield_stops_an_outer_cancel() {
    let root = Scope::new_root(0);
    let shield = shielded(&root);
    let inner = child(&shield);
    root.cancel(CancelReason::Shutdown);
    assert!(!cancelled(&shield));
    assert!(!cancelled(&inner));
}

#[test]
fn a_shielded_scopes_own_cancel_counts() {
    let root = Scope::new_root(0);
    let shield = shielded(&root);
    let inner = child(&shield);
    shield.cancel(CancelReason::Deadline);
    let found = inner.effective_cancellation().unwrap();
    assert_eq!(found.scope.id(), shield.id());
    assert_eq!(found.reason, CancelReason::Deadline);
    assert!(cancelled(&shield));
}

#[test]
fn a_cancel_inside_a_shield_still_reaches_inner_scopes() {
    let root = Scope::new_root(0);
    let shield = shielded(&root);
    let middle = child(&shield);
    let inner = child(&middle);
    root.cancel(CancelReason::Explicit);
    assert!(!cancelled(&inner));
    middle.cancel(CancelReason::ChildFailed);
    assert_eq!(
        inner.effective_cancellation().unwrap().reason,
        CancelReason::ChildFailed
    );
}

#[test]
fn nested_shields_each_stop_the_walk() {
    let root = Scope::new_root(0);
    let outer_shield = shielded(&root);
    let middle = child(&outer_shield);
    let inner_shield = shielded(&middle);
    let inner = child(&inner_shield);
    middle.cancel(CancelReason::Explicit);
    assert!(cancelled(&middle));
    assert!(!cancelled(&inner));
    inner_shield.set_shield(false);
    assert!(cancelled(&inner));
    inner_shield.set_shield(true);
    assert!(!cancelled(&inner));
}

#[test]
fn removing_a_shield_exposes_an_earlier_outer_cancel() {
    let root = Scope::new_root(0);
    let shield = shielded(&root);
    let inner = child(&shield);
    root.cancel(CancelReason::Explicit);
    let mut cache = CancelCache::new();
    assert!(!cache.is_cancelled(&inner));
    shield.set_shield(false);
    assert!(cache.is_cancelled(&inner));
}

#[test]
fn a_scope_created_under_a_cancelled_one_starts_cancelled() {
    let root = Scope::new_root(0);
    root.cancel(CancelReason::Explicit);
    let late = child(&root);
    let mut cache = CancelCache::new();
    assert!(cache.is_cancelled(&late));
    assert!(!cache.is_cancelled(&shielded(&root)));
}

#[test]
fn the_cache_sees_a_cancel_after_a_clean_check() {
    let root = Scope::new_root(0);
    let outer = child(&root);
    let inner = child(&outer);
    let mut cache = CancelCache::new();
    assert!(!cache.is_cancelled(&inner));
    assert!(!cache.is_cancelled(&inner));
    outer.cancel(CancelReason::Explicit);
    assert!(cache.is_cancelled(&inner));
    // Level-triggered: it stays cancelled.
    assert!(cache.is_cancelled(&inner));
}

#[test]
fn the_cache_ignores_cancels_off_the_tasks_path() {
    let root = Scope::new_root(0);
    let mine = child(&root);
    let sibling = child(&root);
    let mut cache = CancelCache::new();
    assert!(!cache.is_cancelled(&mine));
    let epoch = mine.cancel_epoch();
    sibling.cancel(CancelReason::Explicit);
    assert_eq!(mine.cancel_epoch(), epoch);
    assert!(!cache.is_cancelled(&mine));
}

#[test]
fn the_cache_follows_the_task_into_another_scope() {
    let root = Scope::new_root(0);
    let shield = shielded(&root);
    root.cancel(CancelReason::Explicit);
    let mut cache = CancelCache::new();
    assert!(!cache.is_cancelled(&shield));
    assert!(cache.is_cancelled(&root));
    assert!(!cache.is_cancelled(&shield));
}

#[test]
fn a_cancel_skips_the_epochs_of_shielded_subtrees() {
    let root = Scope::new_root(0);
    let shield = shielded(&root);
    let inner = child(&shield);
    let exposed = child(&root);
    let (shield_epoch, inner_epoch, exposed_epoch) = (
        shield.cancel_epoch(),
        inner.cancel_epoch(),
        exposed.cancel_epoch(),
    );
    root.cancel(CancelReason::Explicit);
    assert_eq!(shield.cancel_epoch(), shield_epoch);
    assert_eq!(inner.cancel_epoch(), inner_epoch);
    assert_ne!(exposed.cancel_epoch(), exposed_epoch);
}

#[test]
fn children_unlink_in_any_order() {
    let root = Scope::new_root(0);
    let children: Vec<_> = (0..5).map(|_| child(&root)).collect();
    let mut iter = children.into_iter();
    let (first, second, third) = (iter.next(), iter.next(), iter.next());
    let rest: Vec<_> = iter.collect();
    drop(second);
    drop(rest);
    drop(first);
    root.cancel(CancelReason::Explicit);
    assert!(cancelled(third.as_ref().unwrap()));
    drop(third);
    assert!(root.lock_state().child_scopes_is_empty());
}

#[test]
fn state_counters_are_plain_data_under_the_lock() {
    let root = Scope::new_root(0);
    {
        let mut state = root.lock_state();
        state.live_children += 2;
        state.errors.push("boom");
    }
    let state = root.lock_state();
    assert_eq!(state.live_children, 2);
    assert_eq!(state.errors, ["boom"]);
    assert!(state.exit_waiter.is_none());
}

#[test]
fn concurrent_cancels_and_scope_churn() {
    const THREADS: usize = 4;
    let root = Scope::new_root(0);
    let outer = child(&root);
    let barrier = Arc::new(Barrier::new(THREADS + 1));
    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            let (outer, barrier) = (Arc::clone(&outer), Arc::clone(&barrier));
            thread::spawn(move || {
                barrier.wait();
                let mut cache = CancelCache::new();
                let mut saw_cancel = false;
                for _ in 0..if cfg!(miri) { 20 } else { 2_000 } {
                    let scope = child(&child(&outer));
                    let now = cache.is_cancelled(&scope);
                    assert!(!saw_cancel || now, "a cancel was observed, then lost");
                    saw_cancel |= now;
                }
            })
        })
        .collect();
    barrier.wait();
    outer.cancel(CancelReason::Explicit);
    for worker in workers {
        worker.join().unwrap();
    }
    let mut cache = CancelCache::new();
    assert!(cache.is_cancelled(&child(&outer)));
}

#[test]
fn a_scope_tree_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Scope>();
    assert_send_sync::<Arc<Scope>>();
}
