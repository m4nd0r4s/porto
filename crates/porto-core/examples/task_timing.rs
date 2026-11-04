//! Rough per-operation timings of the M0 task core (Rust futures, no Python).
//!
//! `cargo run --release -p porto-core --example task_timing`
//!
//! Not a benchmark harness: the M0 gate is measured from Python by pyperf (porto-itr.6).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use porto_core::Runtime;

const N: u32 = 1_000_000;

/// Pending once, waking itself: one round trip through the local run queue.
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn per_op(label: &str, start: Instant, ops: u32) {
    let ns = start.elapsed().as_secs_f64() * 1e9 / f64::from(ops);
    println!("{label:<34} {ns:>7.1} ns/op");
}

fn main() {
    let rt = Runtime::new();

    let start = Instant::now();
    for _ in 0..N {
        rt.spawn(async {});
    }
    rt.run_until_idle();
    per_op("spawn + run to completion", start, N);

    rt.spawn(async {
        for _ in 0..N {
            YieldNow(false).await;
        }
    });
    let start = Instant::now();
    rt.run_until_idle();
    per_op("yield round trip (one task)", start, N);

    for _ in 0..2 {
        rt.spawn(async {
            for _ in 0..N / 2 {
                YieldNow(false).await;
            }
        });
    }
    let start = Instant::now();
    rt.run_until_idle();
    per_op("task switch (two tasks yielding)", start, N);
}
