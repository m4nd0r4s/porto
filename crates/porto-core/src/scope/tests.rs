//! Scope tree and effective-cancellation tests.

use std::sync::{Arc, Barrier};
use std::thread;

use super::{CancelCache, CancelReason, ScopeNode, TaskEnd, TaskScope};

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
fn the_scope_cache_follows_cancels_and_shields_on_the_path() {
    let root = Scope::new_root(0);
    let shield = shielded(&root);
    let inner = child(&shield);
    let other = child(&root);
    assert!(!inner.is_cancelled());
    other.cancel(CancelReason::Explicit);
    assert!(!inner.is_cancelled(), "a cancel off the path");
    root.cancel(CancelReason::Explicit);
    assert!(!inner.is_cancelled(), "behind a shield");
    shield.set_shield(false);
    assert!(inner.is_cancelled());
    assert!(inner.is_cancelled(), "cached");
    shield.set_shield(true);
    assert!(!inner.is_cancelled());
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

#[test]
fn a_task_scope_counts_a_child_until_it_ends() {
    let scope = Scope::new_root(0);
    let first = TaskScope::attach(&scope);
    let second = TaskScope::attach(&scope);
    assert_eq!(scope.lock_state().live_children, 2);
    assert_eq!(Arc::strong_count(&scope), 3);
    first.end(TaskEnd::Completed);
    assert_eq!(scope.lock_state().live_children, 1);
    drop(second);
    assert_eq!(
        scope.lock_state().live_children,
        0,
        "a dropped link ends as freed"
    );
    assert_eq!(Arc::strong_count(&scope), 1);
    assert_eq!(scope.cancel_called(), None);
}

#[test]
fn a_failed_child_records_its_error_and_cancels_the_scope_and_its_nested_scopes() {
    let scope = Scope::new_root(0);
    let nested = child(&scope);
    TaskScope::attach(&scope).end(TaskEnd::Failed(Some(Box::new("boom"))));
    assert_eq!(scope.lock_state().errors, ["boom"]);
    assert_eq!(scope.cancel_called(), Some(CancelReason::ChildFailed));
    assert!(cancelled(&nested));
}

#[test]
fn a_failure_that_is_not_the_scope_error_type_still_cancels() {
    let scope = Scope::new_root(0);
    TaskScope::attach(&scope).end(TaskEnd::Failed(Some(Box::new(42_u32))));
    TaskScope::attach(&scope).end(TaskEnd::Failed(None));
    assert!(scope.lock_state().errors.is_empty());
    assert_eq!(scope.cancel_called(), Some(CancelReason::ChildFailed));
}

#[test]
fn the_first_cancel_reason_wins_over_a_later_child_failure() {
    let scope = Scope::new_root(0);
    assert!(scope.cancel(CancelReason::Explicit));
    TaskScope::attach(&scope).end(TaskEnd::Failed(Some(Box::new("late"))));
    assert_eq!(scope.cancel_called(), Some(CancelReason::Explicit));
    assert_eq!(scope.lock_state().errors, ["late"], "still recorded");
}

#[test]
fn ends_that_are_not_failures_leave_the_scope_alone() {
    let scope = Scope::new_root(0);
    for end in [TaskEnd::Completed, TaskEnd::Cancelled, TaskEnd::Freed] {
        TaskScope::attach(&scope).end(end);
    }
    assert_eq!(scope.lock_state().live_children, 0);
    assert_eq!(scope.cancel_called(), None);
    assert!(scope.lock_state().errors.is_empty());
}

#[test]
fn task_end_debug_names_the_end() {
    assert_eq!(format!("{:?}", TaskEnd::Freed), "Freed");
    assert_eq!(
        format!("{:?}", TaskEnd::Failed(Some(Box::new(1_u8)))),
        "Failed(Some(..))"
    );
}

#[test]
fn tasks_spawned_into_a_scope_on_a_runtime_end_in_it() {
    let rt = crate::Runtime::new();
    let scope = Scope::new_root(0);
    let handle = rt.handle();
    let ok = handle.spawn_in(&scope, async { 1 }).unwrap();
    let failed = handle
        .spawn_in(&scope, async { panic!("child failed") })
        .unwrap();
    assert_eq!(scope.lock_state().live_children, 2);
    assert_eq!(rt.block_on(ok).unwrap(), 1);
    assert!(rt.block_on(failed).unwrap_err().is_panic());
    assert_eq!(scope.lock_state().live_children, 0);
    assert_eq!(scope.cancel_called(), Some(CancelReason::ChildFailed));
    assert_eq!(Arc::strong_count(&scope), 1);
}

#[test]
fn a_closed_scope_refuses_new_tasks_and_hands_them_back() {
    let rt = crate::Runtime::new();
    let scope = Scope::new_root(0);
    TaskScope::attach(&scope).end(TaskEnd::Failed(Some(Box::new("boom"))));
    let errors = scope.close();
    assert_eq!(errors.len(), 1, "close hands the recorded failures over");
    assert!(scope.lock_state().closed);
    assert!(
        scope.close().is_empty(),
        "closing again returns nothing more"
    );
    assert!(TaskScope::try_attach(&scope).is_none());
    let refused = rt.handle().spawn_in(&scope, async { 1 }).unwrap_err();
    assert!(refused.to_string().contains("scope is closed"), "{refused}");
    assert_eq!(rt.block_on(refused.0), 1, "the future comes back unrun");
    assert_eq!(scope.lock_state().live_children, 0);
    assert_eq!(Arc::strong_count(&scope), 1);
}

#[test]
#[should_panic(expected = "closed scope")]
fn attaching_to_a_closed_scope_panics() {
    let scope = Scope::new_root(0);
    drop(scope.close());
    let _child = TaskScope::attach(&scope);
}

mod waiters {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use super::*;

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn counting_waker() -> (Arc<CountWakes>, Waker) {
        let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
        (wakes.clone(), Waker::from(wakes))
    }

    fn wakes(counter: &CountWakes) -> usize {
        counter.0.load(Ordering::Relaxed)
    }

    #[test]
    fn a_cancel_wakes_the_waiters_of_its_subtree_but_not_of_a_shielded_one() {
        let root = Scope::new_root(0);
        let inner = child(&root);
        let shield = shielded(&root);
        let (in_root, root_waker) = counting_waker();
        let (in_inner, inner_waker) = counting_waker();
        let (in_shield, shield_waker) = counting_waker();
        assert!(!root.register_waiter(1, &root_waker));
        assert!(!inner.register_waiter(2, &inner_waker));
        assert!(!shield.register_waiter(3, &shield_waker));

        assert!(root.cancel(CancelReason::Explicit));
        assert_eq!(wakes(&in_root), 1);
        assert_eq!(wakes(&in_inner), 1);
        assert_eq!(wakes(&in_shield), 0, "the shield stops the cancel");
        // The registrations stay until the waits end: the scope holds the wakers till then.
        assert_eq!(inner.lock_state().waiter_count(), 1);
        root.unregister_waiter(1);
        inner.unregister_waiter(2);
        shield.unregister_waiter(3);
        drop(root_waker);
        assert_eq!(
            Arc::strong_count(&in_root),
            1,
            "the scope dropped its clone"
        );

        assert!(!root.cancel(CancelReason::Explicit), "cancelled already");
        assert_eq!(wakes(&in_root), 1);
    }

    #[test]
    fn registering_in_a_cancelled_scope_says_so() {
        let root = Scope::new_root(0);
        let inner = child(&root);
        root.cancel(CancelReason::Shutdown);
        let (_, waker) = counting_waker();
        assert!(inner.register_waiter(1, &waker));
        inner.unregister_waiter(1);
        // Unregistering a key that is not filed does nothing.
        inner.unregister_waiter(1);
    }

    #[test]
    fn unshielding_wakes_the_waiters_an_outer_cancel_now_reaches() {
        let root = Scope::new_root(0);
        let shield = shielded(&root);
        let (counter, waker) = counting_waker();
        assert!(!shield.register_waiter(1, &waker));
        root.cancel(CancelReason::Explicit);
        assert_eq!(wakes(&counter), 0);
        shield.set_shield(false);
        assert_eq!(wakes(&counter), 1);
        shield.set_shield(true);
        assert_eq!(wakes(&counter), 1, "shielding cancels nobody");
        shield.unregister_waiter(1);
    }

    #[test]
    fn a_waiter_woken_normally_leaves_the_scope_free_to_drop() {
        // porto-szb.114: the wait ends without a cancel (what it waits on wakes it), and the
        // task takes its entry out, so the scope drops with no waiter left (a debug assert).
        let root = Scope::new_root(0);
        let inner = child(&root);
        let (counter, waker) = counting_waker();
        assert!(!inner.register_waiter(1, &waker));
        waker.wake_by_ref();
        inner.unregister_waiter(1);
        assert_eq!(inner.lock_state().waiter_count(), 0);
        assert_eq!(
            Arc::strong_count(&counter),
            2,
            "the scope dropped its clone"
        );
        drop(inner);
        root.cancel(CancelReason::Explicit);
        assert_eq!(
            wakes(&counter),
            1,
            "only the normal wake: the cancel finds nobody"
        );
    }

    #[test]
    fn sequential_waits_in_a_long_lived_scope_do_not_grow_it() {
        let root = Scope::new_root(0);
        let (_, waker) = counting_waker();
        for key in 0..10_000 {
            assert!(!root.register_waiter(key * 64, &waker));
            assert_eq!(root.lock_state().waiter_count(), 1);
            root.unregister_waiter(key * 64);
        }
        assert_eq!(root.lock_state().waiter_count(), 0);
    }

    #[test]
    fn a_cancel_from_another_thread_wakes_the_waiter() {
        let root = Scope::new_root(0);
        let (counter, waker) = counting_waker();
        assert!(!root.register_waiter(1, &waker));
        let canceller = {
            let root = Arc::clone(&root);
            thread::spawn(move || root.cancel(CancelReason::Explicit))
        };
        assert!(canceller.join().unwrap());
        assert_eq!(wakes(&counter), 1);
        root.unregister_waiter(1);
    }
}

#[test]
fn a_task_cancel_is_a_reason_of_its_own() {
    let parent = ScopeNode::<()>::new_root(1);
    let implicit = ScopeNode::new_child(&parent, 2, false);
    let nested = ScopeNode::new_child(&implicit, 2, false);
    assert!(implicit.cancel(CancelReason::TaskCancelled));
    assert_eq!(implicit.cancel_called(), Some(CancelReason::TaskCancelled));
    assert!(
        nested.is_cancelled(),
        "what the task entered is inside the cancel"
    );
    assert!(!parent.is_cancelled(), "its siblings' scope is not");
    assert_eq!(
        nested.effective_cancellation().map(|found| found.reason),
        Some(CancelReason::TaskCancelled)
    );
}

mod adoption {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use super::*;

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn counting_waker() -> (Arc<CountWakes>, Waker) {
        let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
        (wakes.clone(), Waker::from(wakes))
    }

    #[test]
    fn an_adopted_scope_follows_its_new_parent_and_leaves_the_old_one() {
        let root = Scope::new_root(0);
        let old = child(&root);
        let new = child(&root);
        let hidden = child(&old);
        let nested = child(&hidden);
        assert!(hidden.adopt(&new));
        assert_eq!(hidden.parent().unwrap().id(), new.id());
        assert!(old.lock_state().child_scopes_is_empty());
        assert!(!new.lock_state().child_scopes_is_empty());

        assert!(old.cancel(CancelReason::Explicit));
        assert!(
            !cancelled(&hidden),
            "the old parent's cancel no longer reaches it"
        );
        assert!(!cancelled(&nested));
        assert!(new.cancel(CancelReason::Explicit));
        assert!(cancelled(&hidden));
        assert!(
            cancelled(&nested),
            "what is nested in the adopted scope follows it"
        );
    }

    #[test]
    fn a_scope_is_adopted_once_and_a_root_never() {
        let root = Scope::new_root(0);
        let first = child(&root);
        let second = child(&root);
        let hidden = child(&root);
        assert!(hidden.adopt(&first));
        assert!(!hidden.adopt(&second), "adopted already");
        assert_eq!(hidden.parent().unwrap().id(), first.id());
        assert!(!root.adopt(&first), "a root has no parent to replace");
    }

    #[test]
    fn an_adopted_scope_is_woken_by_its_new_parents_cancel_not_the_old_ones() {
        let root = Scope::new_root(0);
        let old = child(&root);
        let new = child(&root);
        let hidden = child(&old);
        let (woken, waker) = counting_waker();
        assert!(!hidden.register_waiter(1, &waker));
        assert!(hidden.adopt(&new));
        assert!(old.cancel(CancelReason::Explicit));
        assert_eq!(woken.0.load(Ordering::Relaxed), 0);
        assert!(new.cancel(CancelReason::Explicit));
        assert_eq!(woken.0.load(Ordering::Relaxed), 1);
        hidden.unregister_waiter(1);
    }

    #[test]
    fn adopting_into_a_cancelled_scope_cancels_the_adopted_one_and_wakes_its_waiters() {
        let root = Scope::new_root(0);
        let old = child(&root);
        let new = child(&root);
        let hidden = child(&old);
        let (woken, waker) = counting_waker();
        assert!(!hidden.register_waiter(1, &waker));
        assert!(new.cancel(CancelReason::Explicit));
        assert!(hidden.adopt(&new));
        assert!(cancelled(&hidden));
        assert_eq!(woken.0.load(Ordering::Relaxed), 1, "the adoption woke it");
        hidden.unregister_waiter(1);
    }

    #[test]
    fn dropping_adopted_scopes_unlinks_them_from_the_new_parent() {
        let root = Scope::new_root(0);
        let old = child(&root);
        let new = child(&root);
        let first = child(&old);
        let second = child(&old);
        assert!(first.adopt(&new));
        assert!(second.adopt(&new));
        drop(first);
        assert!(!new.lock_state().child_scopes_is_empty());
        drop(second);
        assert!(new.lock_state().child_scopes_is_empty());
        assert!(old.lock_state().child_scopes_is_empty());
        // The old parent is held until the adopted scope is gone, and no longer.
        assert!(Arc::strong_count(&old) == 1);
    }

    #[test]
    fn a_task_counts_in_the_scope_it_was_moved_to() {
        let root = Scope::new_root(0);
        let hidden = child(&root);
        let target = child(&root);
        let task_scope = TaskScope::attach(&hidden);
        assert!(task_scope.is_for(&hidden));
        assert!(!task_scope.is_for(&target));
        task_scope.end(TaskEnd::Completed);
        assert_eq!(hidden.lock_state().live_children, 0);
    }
}
