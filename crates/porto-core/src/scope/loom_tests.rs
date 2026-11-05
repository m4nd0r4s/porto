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
        // The failure is recorded under the same lock as the count, the cancel just after:
        // both are in once the ending threads are done.
        assert_eq!(scope.lock_state().errors, [7]);
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(scope.cancel_called(), Some(CancelReason::ChildFailed));
        assert_eq!(Arc::strong_count(&scope), 1);
    });
}
