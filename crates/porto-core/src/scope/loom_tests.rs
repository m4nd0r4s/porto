//! loom models of the scope tree (`52-testing-strategy.md` §2: scope cancel vs spawn), and of
//! children ending while the owner waits for them.
//!
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release scope`

use std::future::poll_fn;
use std::task::Poll;

use loom::sync::Arc;
use loom::thread;

use super::{CancelCache, CancelReason, ScopeNode, TaskEnd, TaskScope};

type Scope = ScopeNode<()>;

/// A task spawned into a scope while another thread cancels it: whatever it saw before, it is
/// cancelled once the cancel has returned, and so is a scope it nests in that scope. A shielded
/// scope it nests never is.
#[test]
fn scope_cancel_vs_spawn() {
    loom::model(|| {
        let root = Scope::new_root(0);
        let scope = ScopeNode::new_child(&root, 0, false);
        let canceller = {
            let scope = Arc::clone(&scope);
            thread::spawn(move || assert!(scope.cancel(CancelReason::Explicit)))
        };

        // The spawn: the child joins the scope and runs its first checkpoint.
        scope.lock_state().live_children += 1;
        let mut task = CancelCache::new();
        task.is_cancelled(&scope);
        let nested = ScopeNode::new_child(&scope, 1, false);
        let shield = ScopeNode::new_child(&scope, 1, true);
        let mut in_nested = CancelCache::new();
        in_nested.is_cancelled(&nested);

        canceller.join().unwrap();
        assert!(task.is_cancelled(&scope));
        assert!(in_nested.is_cancelled(&nested));
        assert!(!CancelCache::new().is_cancelled(&shield));
        assert_eq!(scope.lock_state().live_children, 1);
    });
}

/// Nested scopes created and dropped on one thread while another cancels their parent: the
/// cancel's walk down the child list and the children's unlinking never race, and a scope that
/// outlives the cancel sees it.
#[test]
fn scope_cancel_vs_nested_scope_churn() {
    loom::model(|| {
        let root = Scope::new_root(0);
        let canceller = {
            let root = Arc::clone(&root);
            thread::spawn(move || root.cancel(CancelReason::Shutdown))
        };

        let short_lived = ScopeNode::new_child(&root, 1, false);
        let kept = ScopeNode::new_child(&root, 1, false);
        let mut cache = CancelCache::new();
        cache.is_cancelled(&kept);
        drop(short_lived);

        canceller.join().unwrap();
        assert!(cache.is_cancelled(&kept));
        drop(kept);
        assert!(root.lock_state().child_scopes_is_empty());
    });
}

/// Removing a shield while an outer scope is cancelled: once both have returned, a task under
/// the former shield is cancelled, whatever order they ran in.
#[test]
fn scope_cancel_vs_unshield() {
    loom::model(|| {
        let root = Scope::new_root(0);
        let shield = ScopeNode::new_child(&root, 0, true);
        let inner = ScopeNode::new_child(&shield, 0, false);
        let mut task = CancelCache::new();
        task.is_cancelled(&inner);

        let canceller = {
            let root = Arc::clone(&root);
            thread::spawn(move || root.cancel(CancelReason::Explicit))
        };
        shield.set_shield(false);
        task.is_cancelled(&inner);

        canceller.join().unwrap();
        assert!(task.is_cancelled(&inner));
    });
}

/// Two children end on two threads, one failing (C4), while the owner waits in `ScopeExit`
/// (C5): the owner wakes after both ended, and sees the failure recorded and the scope
/// cancelled.
#[test]
fn child_ends_vs_exit_wait() {
    loom::model(|| {
        let scope = ScopeNode::<u8>::new_root(0);
        let failing = TaskScope::attach(&scope);
        let completing = TaskScope::attach(&scope);
        let first = thread::spawn(move || failing.end(TaskEnd::Failed(Some(Box::new(7_u8)))));
        let second = thread::spawn(move || completing.end(TaskEnd::Completed));

        loom::future::block_on(poll_fn(|cx| {
            let mut state = scope.lock_state();
            if state.live_children == 0 {
                return Poll::Ready(());
            }
            state.exit_waiter = Some(cx.waker().clone());
            Poll::Pending
        }));
        // The cancel comes before the count, the failure under the same lock as the count: an
        // owner that sees no child left sees both, without waiting for the ending threads.
        assert_eq!(scope.cancel_called(), Some(CancelReason::ChildFailed));
        assert_eq!(scope.lock_state().errors, [7]);
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(Arc::strong_count(&scope), 1);
    });
}

/// A single failing child (C4): the owner that sees it gone already sees the scope cancelled.
#[test]
fn a_failing_child_cancels_before_the_owner_can_exit() {
    loom::model(|| {
        let scope = ScopeNode::<u8>::new_root(0);
        let failing = TaskScope::attach(&scope);
        let first = thread::spawn(move || failing.end(TaskEnd::Failed(None)));
        loom::future::block_on(poll_fn(|cx| {
            let mut state = scope.lock_state();
            if state.live_children == 0 {
                return Poll::Ready(());
            }
            state.exit_waiter = Some(cx.waker().clone());
            Poll::Pending
        }));
        let seen = scope.cancel_called();
        first.join().unwrap();
        assert_eq!(seen, Some(CancelReason::ChildFailed));
    });
}

/// A task registers as a waiter of a nested scope while another thread cancels the root
/// (C10): either the cancel wakes it, or its registration sees the cancel. Never neither, which
/// would leave it waiting forever.
#[test]
fn register_waiter_vs_cancel() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    loom::model(|| {
        let root = Scope::new_root(0);
        let scope = ScopeNode::new_child(&root, 1, false);
        let canceller = {
            let root = Arc::clone(&root);
            thread::spawn(move || assert!(root.cancel(CancelReason::Explicit)))
        };

        let wakes = std::sync::Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(std::sync::Arc::clone(&wakes));
        let saw_cancel = scope.register_waiter(1, &waker);

        canceller.join().unwrap();
        assert!(saw_cancel || wakes.0.load(Ordering::SeqCst) == 1);
        scope.unregister_waiter(1);
    });
}

/// A waiter whose wait ends normally (woken by what it waited on, porto-szb.114) and
/// unregisters while another thread cancels the scope: the entry is gone once both are done,
/// whichever went first, and the cancel woke it at most once. The scope then drops with no
/// waiter left (a debug assert).
#[test]
fn normal_wake_vs_cancel_unregisters_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    loom::model(|| {
        let root = Scope::new_root(0);
        let scope = ScopeNode::new_child(&root, 1, false);
        let wakes = std::sync::Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(std::sync::Arc::clone(&wakes));
        assert!(!scope.register_waiter(1, &waker));
        let canceller = {
            let root = Arc::clone(&root);
            thread::spawn(move || assert!(root.cancel(CancelReason::Explicit)))
        };

        // The normal wake: the wait is over, the task takes its entry out.
        scope.unregister_waiter(1);

        canceller.join().unwrap();
        assert_eq!(scope.lock_state().waiter_count(), 0);
        assert!(wakes.0.load(Ordering::SeqCst) <= 1);
        // A cancel that missed the entry is still seen by the next check (level-triggered).
        assert!(scope.is_cancelled());
        drop(waker);
        assert_eq!(
            std::sync::Arc::strong_count(&wakes),
            1,
            "no clone left anywhere"
        );
    });
}

/// The scope's own walk cache against a cancel of its parent from another thread: whatever a
/// check saw before, one after the cancel returned sees it, and a check from a third thread
/// that refills the cache meanwhile never leaves a stale answer behind.
#[test]
fn scope_walk_cache_vs_cancel() {
    loom::model(|| {
        let root = Scope::new_root(0);
        let scope = ScopeNode::new_child(&root, 1, false);
        let canceller = {
            let root = Arc::clone(&root);
            thread::spawn(move || assert!(root.cancel(CancelReason::Explicit)))
        };
        let checker = {
            let scope = Arc::clone(&scope);
            thread::spawn(move || scope.is_cancelled())
        };
        scope.is_cancelled();
        canceller.join().unwrap();
        checker.join().unwrap();
        assert!(scope.is_cancelled());
    });
}

/// From the PR #39 review: a leaf two levels down fills its walk cache while the root is
/// cancelled and an unrelated scope is cancelled too (two generation bumps in either order).
#[test]
fn scope_walk_cache_two_levels_vs_root_and_unrelated_cancel() {
    loom::model(|| {
        let root = Scope::new_root(0);
        let mid = ScopeNode::new_child(&root, 1, false);
        let leaf = ScopeNode::new_child(&mid, 1, false);
        let other = Scope::new_root(0);
        let canceller = {
            let root = Arc::clone(&root);
            thread::spawn(move || assert!(root.cancel(CancelReason::Explicit)))
        };
        let unrelated = {
            let other = Arc::clone(&other);
            thread::spawn(move || {
                other.cancel(CancelReason::Explicit);
            })
        };
        leaf.is_cancelled();
        canceller.join().unwrap();
        // After the root cancel returned, a check on this thread must see it, whatever the
        // unrelated cancel does meanwhile.
        assert!(leaf.is_cancelled());
        unrelated.join().unwrap();
        assert!(leaf.is_cancelled());
    });
}

/// ADVERSARIAL: register in a grandchild while the root is cancelled.
#[test]
fn register_waiter_grandchild_vs_cancel() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};
    struct CountWakes(AtomicUsize);
    impl Wake for CountWakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    loom::model(|| {
        let root = Scope::new_root(0);
        let mid = ScopeNode::new_child(&root, 1, false);
        let leaf = ScopeNode::new_child(&mid, 1, false);
        let canceller = {
            let mid = Arc::clone(&mid);
            thread::spawn(move || assert!(mid.cancel(CancelReason::Explicit)))
        };
        let wakes = std::sync::Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(std::sync::Arc::clone(&wakes));
        // A check (cache fill) before registering, like the step loop's `cancellation()`.
        let early = leaf.is_cancelled();
        let saw_cancel = leaf.register_waiter(1, &waker);
        canceller.join().unwrap();
        assert!(early || saw_cancel || wakes.0.load(Ordering::SeqCst) == 1);
        // After a wake, the woken task's cached check sees it.
        assert!(leaf.is_cancelled());
        leaf.unregister_waiter(1);
    });
}

/// The scope deadline models (C9): the deadline timer fired on the lane while another thread
/// closes the scope, moves its deadline, or cancels it. The wheel's shard lock is std's (no
/// loom operation runs under it), the scope's lock is loom's.
mod deadline {
    use std::time::Duration;

    use loom::sync::Arc;
    use loom::thread;

    use super::Scope;
    use crate::sched::Handle;
    use crate::scope::CancelReason;
    use crate::time::{Clock, Deadline, RuntimeClock};

    /// A clock stopped at 10 ms.
    struct Stopped;

    impl Clock for Stopped {
        fn now(&self) -> Deadline {
            Deadline::from_nanos(10_000_000)
        }
    }

    fn handle() -> Handle {
        Handle::new(
            RuntimeClock::Custom(Box::new(Stopped)),
            Duration::from_millis(1),
            None,
            1,
            crate::blocking::BlockingConfig::default(),
            false,
        )
    }

    fn at(millis: u64) -> Deadline {
        Deadline::from_nanos(millis * 1_000_000)
    }

    /// Links `scope`'s deadline timer at 10 ms although the clock already reads 10 ms, as if it
    /// was set a moment earlier (`set_deadline` would cancel at once): armed at 11 ms, then
    /// moved to 10 ms, due, which is what the lane finds when it next fires the wheel.
    fn arm_due(scope: &Arc<Scope>, handle: &Handle) {
        Scope::set_deadline(scope, handle, at(11));
        // Fires nothing (11 ms is not due), then moves the linked entry to 10 ms, due.
        scope.lock_state().deadline = at(10);
        let entry = std::ptr::NonNull::from(&scope.deadline_timer);
        // SAFETY: the entry is linked in `handle`'s wheel; the scope's lock orders arms, and
        // no other thread runs yet.
        let armed = unsafe { handle.shared.timers.arm(entry, at(10), None) };
        assert!(matches!(armed, crate::sched::Armed::Linked(None)));
    }

    /// The lane fires the due timer while the owner closes the scope: either the scope was
    /// cancelled by its deadline before it closed, or the fire found it closed. A closed scope
    /// is never cancelled afterwards (the check and the cancel are one step under its lock), and
    /// the wheel holds no reference to it either way.
    #[test]
    fn deadline_fire_vs_close() {
        loom::model(|| {
            let handle = handle();
            let scope = Scope::new_root(0);
            arm_due(&scope, &handle);
            let lane = {
                let handle = handle.clone();
                thread::spawn(move || handle.shared.timers.fire_expired(|| handle.now()))
            };
            drop(scope.close());
            let at_close = scope.cancel_called();
            let fired = lane.join().unwrap();
            assert!(fired <= 1);
            assert_eq!(scope.cancel_called(), at_close, "cancelled after it closed");
            assert!(matches!(at_close, None | Some(CancelReason::Deadline)));
            assert_eq!(handle.armed_timers(), 0);
            assert_eq!(Arc::strong_count(&scope), 1, "the wheel kept a reference");
        });
    }

    /// The lane fires the due timer while another thread removes the deadline: once
    /// `set_deadline(NEVER)` returned, the scope is never cancelled by it any more.
    #[test]
    fn deadline_fire_vs_removed_deadline() {
        loom::model(|| {
            let handle = handle();
            let scope = Scope::new_root(0);
            arm_due(&scope, &handle);
            let lane = {
                let handle = handle.clone();
                thread::spawn(move || handle.shared.timers.fire_expired(|| handle.now()))
            };
            Scope::set_deadline(&scope, &handle, Deadline::NEVER);
            let after_removal = scope.cancel_called();
            lane.join().unwrap();
            assert_eq!(
                scope.cancel_called(),
                after_removal,
                "cancelled after removal"
            );
            assert_eq!(handle.armed_timers(), 0);
            drop(scope.close());
            assert_eq!(Arc::strong_count(&scope), 1);
        });
    }

    /// The lane fires the due timer while another thread sets an earlier deadline that is due
    /// too (the scope is cancelled by whichever comes first, exactly once, and nothing stays
    /// linked).
    #[test]
    fn deadline_fire_vs_earlier_rearm() {
        loom::model(|| {
            let handle = handle();
            let scope = Scope::new_root(0);
            arm_due(&scope, &handle);
            let lane = {
                let handle = handle.clone();
                thread::spawn(move || handle.shared.timers.fire_expired(|| handle.now()))
            };
            Scope::set_deadline(&scope, &handle, at(5));
            lane.join().unwrap();
            assert_eq!(scope.cancel_called(), Some(CancelReason::Deadline));
            assert_eq!(handle.armed_timers(), 0);
            drop(scope.close());
            assert_eq!(Arc::strong_count(&scope), 1);
        });
    }

    /// The lane fires the due timer while another thread moves the deadline later: the scope
    /// is either cancelled by the old deadline (the fire won) or left alone with its timer
    /// linked at the new one, never both and never neither.
    #[test]
    fn deadline_fire_vs_later_deadline() {
        loom::model(|| {
            let handle = handle();
            let scope = Scope::new_root(0);
            arm_due(&scope, &handle);
            let lane = {
                let handle = handle.clone();
                thread::spawn(move || handle.shared.timers.fire_expired(|| handle.now()))
            };
            Scope::set_deadline(&scope, &handle, at(20));
            lane.join().unwrap();
            let cancelled = scope.cancel_called() == Some(CancelReason::Deadline);
            let linked = handle.armed_timers() == 1;
            assert!(
                cancelled != linked,
                "cancelled: {cancelled}, linked: {linked}"
            );
            drop(scope.close());
            assert_eq!(handle.armed_timers(), 0);
            assert_eq!(Arc::strong_count(&scope), 1);
        });
    }

    /// The deadline fires while another thread cancels the scope: exactly one reason is
    /// recorded, the first, and the other cancel changes nothing.
    #[test]
    fn deadline_fire_vs_explicit_cancel() {
        loom::model(|| {
            let handle = handle();
            let scope = Scope::new_root(0);
            let nested = crate::scope::ScopeNode::new_child(&scope, 0, false);
            arm_due(&scope, &handle);
            let lane = {
                let handle = handle.clone();
                thread::spawn(move || handle.shared.timers.fire_expired(|| handle.now()))
            };
            let won = scope.cancel(CancelReason::Explicit);
            lane.join().unwrap();
            let reason = scope.cancel_called();
            assert_eq!(
                reason,
                Some(if won {
                    CancelReason::Explicit
                } else {
                    CancelReason::Deadline
                })
            );
            assert!(nested.effective_cancellation().is_some());
            drop(nested);
            drop(scope.close());
            assert_eq!(Arc::strong_count(&scope), 1);
        });
    }
}

/// A scope is adopted by a new parent while another thread cancels that parent (`Scope.start`
/// re-parenting its hidden scope): whichever came first, the adopted scope ends up cancelled,
/// and its waiter, registered before, was woken by the cancel or by the adoption: never
/// neither, which would leave a started task asleep in a cancelled scope.
#[test]
fn adopt_vs_the_new_parents_cancel() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    loom::model(|| {
        let root = Scope::new_root(0);
        let old = ScopeNode::new_child(&root, 1, false);
        let new = ScopeNode::new_child(&root, 1, false);
        let hidden = ScopeNode::new_child(&old, 1, false);
        let wakes = std::sync::Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(std::sync::Arc::clone(&wakes));
        assert!(!hidden.register_waiter(1, &waker));
        let canceller = {
            let new = Arc::clone(&new);
            thread::spawn(move || assert!(new.cancel(CancelReason::Explicit)))
        };

        assert!(hidden.adopt(&new));

        canceller.join().unwrap();
        assert!(hidden.effective_cancellation().is_some());
        assert!(
            wakes.0.load(Ordering::SeqCst) >= 1,
            "the waiter was never woken"
        );
        hidden.unregister_waiter(1);
    });
}

/// A scope is adopted while its last reference is dropped on another thread: the child lists
/// of both parents stay consistent (the debug asserts of the parents' drops check they are
/// empty).
#[test]
fn adopt_vs_the_scopes_drop() {
    loom::model(|| {
        let root = Scope::new_root(0);
        let old = ScopeNode::new_child(&root, 1, false);
        let new = ScopeNode::new_child(&root, 1, false);
        let sibling = ScopeNode::new_child(&old, 1, false);
        let hidden = ScopeNode::new_child(&old, 1, false);
        let adopter = {
            let new = Arc::clone(&new);
            let hidden = Arc::clone(&hidden);
            thread::spawn(move || {
                assert!(hidden.adopt(&new));
            })
        };
        drop(sibling);
        adopter.join().unwrap();
        drop(hidden);
        assert!(new.lock_state().child_scopes_is_empty());
    });
}

/// What `Task::install_implicit` and `Task::cancel` (porto-py) do about a task's implicit
/// scope, with the same primitives: the task makes its scope and publishes it under the lock,
/// then reads the cancel flag; `cancel` stores the flag under the lock, then reads the scope.
/// Whichever is second sees the other, so the scope is cancelled either way and the cancel is
/// never lost.
#[test]
fn install_implicit_scope_vs_task_cancel() {
    use loom::sync::Mutex;
    use loom::sync::atomic::{AtomicBool, Ordering};

    loom::model(|| {
        let base = Scope::new_root(0);
        let shared = Arc::new((Mutex::new(None::<Arc<Scope>>), AtomicBool::new(false)));
        let canceller = {
            let shared = Arc::clone(&shared);
            thread::spawn(move || {
                let implicit = {
                    let published = shared.0.lock().unwrap();
                    shared.1.store(true, Ordering::SeqCst);
                    published.clone()
                };
                if let Some(scope) = implicit {
                    scope.cancel(CancelReason::TaskCancelled);
                }
            })
        };

        let scope = ScopeNode::new_child(&base, 1, false);
        let requested = {
            let mut published = shared.0.lock().unwrap();
            *published = Some(Arc::clone(&scope));
            shared.1.load(Ordering::SeqCst)
        };
        if requested {
            scope.cancel(CancelReason::TaskCancelled);
        }

        canceller.join().unwrap();
        assert_eq!(scope.cancel_called(), Some(CancelReason::TaskCancelled));
        assert!(!base.is_cancelled(), "only the task's own scope");
    });
}

/// A scope is adopted while a shield on its old parent (or its new one) is removed: whatever
/// the interleaving, it ends up cancelled through the new parent (the root is cancelled), and
/// its waiter was woken, by the unshield or by the adoption: never neither.
fn adopt_vs_unshield(shield_old: bool) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    loom::model(move || {
        let root = Scope::new_root(0);
        assert!(root.cancel(CancelReason::Explicit));
        let old = ScopeNode::new_child(&root, 1, shield_old);
        let new = ScopeNode::new_child(&root, 1, !shield_old);
        let hidden = ScopeNode::new_child(&old, 1, false);
        let wakes = std::sync::Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(std::sync::Arc::clone(&wakes));
        // The old parent shields it: not cancelled yet.
        let before = hidden.register_waiter(1, &waker);
        assert_eq!(before, !shield_old);
        let unshielder = {
            let (old, new) = (Arc::clone(&old), Arc::clone(&new));
            thread::spawn(move || {
                if shield_old {
                    old.set_shield(false);
                } else {
                    new.set_shield(false);
                }
            })
        };

        assert!(hidden.adopt(&new));

        unshielder.join().unwrap();
        assert!(hidden.effective_cancellation().is_some());
        assert!(
            before || wakes.0.load(Ordering::SeqCst) >= 1,
            "the waiter was never woken"
        );
        hidden.unregister_waiter(1);
    });
}

#[test]
fn adopt_vs_the_old_parents_unshield() {
    adopt_vs_unshield(true);
}

#[test]
fn adopt_vs_the_new_parents_unshield() {
    adopt_vs_unshield(false);
}

/// A task's check fills the scope's cached walk result while the scope is adopted by a parent
/// that is cancelled: the cache, filled from the old parent's walk, is never believed once the
/// adoption bumped the generation.
#[test]
fn adopt_vs_the_walk_cache_refill() {
    loom::model(|| {
        let root = Scope::new_root(0);
        let old = ScopeNode::new_child(&root, 1, false);
        let new = ScopeNode::new_child(&root, 1, false);
        assert!(new.cancel(CancelReason::Explicit));
        let hidden = ScopeNode::new_child(&old, 1, false);
        let checker = {
            let hidden = Arc::clone(&hidden);
            thread::spawn(move || hidden.is_cancelled())
        };

        assert!(hidden.adopt(&new));

        checker.join().unwrap();
        assert!(
            hidden.is_cancelled(),
            "a stale cached walk survived the adoption"
        );
    });
}
