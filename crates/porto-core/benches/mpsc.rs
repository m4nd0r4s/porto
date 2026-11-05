//! Uncontended send + receive on one thread (`16-sync-primitives-and-channels.md` §2.4).
//!
//! The fast path is one lock-free push, one lock-free pop and a `SeqCst` fence plus a load on
//! each side to check for waiters. `std::sync::mpsc::sync_channel` is benched beside it for
//! scale.
//!
//! `cargo bench -p porto-core --bench mpsc`

use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::task::{Context, Waker};

use criterion::{Criterion, criterion_group, criterion_main};
use porto_core::sync::mpsc;

fn uncontended(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpsc_uncontended");

    let (tx, rx) = mpsc::channel(1024);
    group.bench_function("bounded/try_send+try_recv", |b| {
        b.iter(|| {
            tx.try_send(black_box(1u64)).unwrap();
            rx.try_recv().unwrap()
        });
    });

    let mut cx = Context::from_waker(Waker::noop());
    group.bench_function("bounded/send+recv_futures", |b| {
        b.iter(|| {
            assert!(pin!(tx.send(black_box(1u64))).poll(&mut cx).is_ready());
            pin!(rx.recv()).poll(&mut cx)
        });
    });

    let (tx, rx) = mpsc::unbounded_channel();
    group.bench_function("unbounded/try_send+try_recv", |b| {
        b.iter(|| {
            tx.try_send(black_box(1u64)).unwrap();
            rx.try_recv().unwrap()
        });
    });

    let (tx, rx) = std::sync::mpsc::sync_channel(1024);
    group.bench_function("std_sync_channel/try_send+try_recv", |b| {
        b.iter(|| {
            tx.try_send(black_box(1u64)).unwrap();
            rx.try_recv().unwrap()
        });
    });

    group.finish();
}

criterion_group!(benches, uncontended);
criterion_main!(benches);
