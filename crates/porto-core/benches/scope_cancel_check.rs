//! The per-task cancel check (`15-structured-concurrency-and-cancellation.md` §1.2).
//!
//! With the cancel generation unchanged, the check is one atomic load and an id compare,
//! whatever the depth of the scope tree. The walk is benched beside it for scale.
//!
//! `cargo bench -p porto-core --bench scope_cancel_check`

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use porto_core::scope::{CancelCache, ScopeNode};

/// A chain of `depth` scopes under a root, returning the innermost.
fn chain(depth: usize) -> (Arc<ScopeNode<()>>, Vec<Arc<ScopeNode<()>>>) {
    let root = ScopeNode::new_root(0);
    let mut scopes = vec![ScopeNode::new_child(&root, 0, false)];
    for _ in 1..depth {
        let next = ScopeNode::new_child(scopes.last().unwrap(), 0, false);
        scopes.push(next);
    }
    (root, scopes)
}

fn cancel_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("scope_cancel_check");
    for depth in [1, 16] {
        let (_root, scopes) = chain(depth);
        let inner = scopes.last().unwrap();
        let mut cache = CancelCache::new();
        cache.is_cancelled(inner);
        group.bench_function(format!("unchanged_generation/depth_{depth}"), |b| {
            b.iter(|| cache.is_cancelled(black_box(inner)));
        });
        group.bench_function(format!("full_walk/depth_{depth}"), |b| {
            b.iter(|| black_box(inner).effective_cancellation().is_some());
        });
    }
    group.finish();
}

criterion_group!(benches, cancel_check);
criterion_main!(benches);
