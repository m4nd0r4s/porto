//! Scopes: the cancel tree and the effective-cancellation check.
//!
//! See `15-structured-concurrency-and-cancellation.md` §1.1 and §1.2. A [`ScopeNode`] is one
//! scope in the tree: it links to its parent, keeps an intrusive list of its nested scopes, and
//! carries the cancel and shield bits the walk reads without locking. A task is cancelled when
//! the walk from its innermost scope outward meets a cancelled scope before a shielded one; a
//! shielded scope's own cancellation still counts. A [`CancelCache`] per task makes the common
//! check one atomic load of the global cancel generation.
//!
//! The loom models run with `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release scope`.

mod check;
mod node;
mod sync;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

pub use check::CancelCache;
pub use node::{CancelReason, Cancellation, ScopeNode, ScopeState};
