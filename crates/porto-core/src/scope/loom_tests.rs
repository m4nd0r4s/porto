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
