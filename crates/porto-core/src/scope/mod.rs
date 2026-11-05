//! Scopes: the cancel tree and the effective-cancellation check.
//!
//! See `15-structured-concurrency-and-cancellation.md` §1.1 and §1.2. A [`ScopeNode`] is one
//! scope in the tree: it links to its parent, keeps an intrusive list of its nested scopes, and
//! carries the cancel and shield bits the walk reads without locking. A task is cancelled when
//! the walk from its innermost scope outward meets a cancelled scope before a shielded one; a
//! shielded scope's own cancellation still counts. A [`CancelCache`] per task makes the common
//! check one atomic load of the global cancel generation. A task in a cancellable wait files its
//! waker with its innermost scope ([`ScopeNode::register_waiter`]), and a cancel wakes the ones
//! in the subtree it reaches (C10). A task spawned into a scope holds a
//! [`TaskScope`], through which the scope hears how the task ended ([`TaskEnd`]), exactly once.
//!
//! The loom models run with `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release scope`.

mod check;
mod child;
mod node;
mod sync;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

pub use check::CancelCache;
pub use child::{TaskEnd, TaskScope};
pub use node::{CancelReason, Cancellation, ScopeNode, ScopeState};

/// A counted reference to a scope: what a task holds for the scopes it entered
/// (`11-task-model-and-trap-protocol.md` §2, `cancel_stack`).
pub type ScopeRef<E> = sync::Arc<ScopeNode<E>>;
