//! What the loom models share: how a model is bounded (`cfg(loom)` test builds only).
//!
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release` runs every model
//! (51-build-ci-release.md §2). Most explore every interleaving (`loom::model`); the models with
//! three threads, or a queue that spins on a half-finished peer, cannot finish unbounded, so
//! they declare the largest preemption bound they can explore, which is also what the
//! nightly run uses. `LOOM_MAX_PREEMPTIONS` (the pull-request run sets it to 2) lowers every
//! model to that bound, and never raises a capped one above its own: at a bound its model
//! cannot explore, loom stops with "Model exceeded maximum number of branches".

use loom::model::Builder;

/// Sets `builder`'s preemption bound to `cap`, or to `LOOM_MAX_PREEMPTIONS` when that is lower.
pub(crate) fn cap_preemptions(builder: &mut Builder, cap: usize) {
    builder.preemption_bound = Some(builder.preemption_bound.map_or(cap, |set| set.min(cap)));
}
