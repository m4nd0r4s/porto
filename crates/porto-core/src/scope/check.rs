//! The per-task cancel check: a cached walk result, revalidated by one atomic load.

use std::sync::atomic::Ordering;

use super::node::ScopeNode;
use super::sync::CANCEL_GENERATION;

/// A task's cached effective-cancellation result (§1.2).
///
/// The check loads the global cancel generation; if it has not moved since the last check and
/// the task is still in the same scope, the cached answer stands. Otherwise the scope's cancel
/// epoch tells whether anything on its path changed, and only then does the walk run.
///
/// Scopes are compared by id, not address: a scope freed and replaced by another at the same
/// address must not inherit its cached answer.
#[derive(Clone, Copy, Debug)]
pub struct CancelCache {
    scope_id: u64,
    epoch: u64,
    generation: u64,
    cancelled: bool,
}

impl CancelCache {
    /// An empty cache: the first check walks.
    pub const fn new() -> Self {
        Self {
            // Scope ids start at 1.
            scope_id: 0,
            epoch: 0,
            generation: u64::MAX,
            cancelled: false,
        }
    }

    /// Whether a task whose innermost scope is `scope` is effectively cancelled.
    #[inline]
    pub fn is_cancelled<E>(&mut self, scope: &ScopeNode<E>) -> bool {
        let generation = CANCEL_GENERATION.load(Ordering::Acquire);
        if generation == self.generation && scope.id() == self.scope_id {
            return self.cancelled;
        }
        self.refresh(scope, generation)
    }

    /// The slow path: the generation moved or the task changed scope.
    #[cold]
    fn refresh<E>(&mut self, scope: &ScopeNode<E>, generation: u64) -> bool {
        // The epoch is read before the walk: a cancel that races with the walk bumps it
        // afterwards, so the next check walks again.
        let epoch = scope.cancel_epoch();
        if scope.id() != self.scope_id || epoch != self.epoch {
            self.cancelled = scope.effective_cancellation().is_some();
            self.scope_id = scope.id();
            self.epoch = epoch;
        }
        self.generation = generation;
        self.cancelled
    }
}

impl Default for CancelCache {
    fn default() -> Self {
        Self::new()
    }
}
