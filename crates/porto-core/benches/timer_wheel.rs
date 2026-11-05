//! The timer wheel (`14-time-and-timers.md` §2), through `Sleep`: arming and cancelling an
//! entry (O(1), whatever else is armed), and firing due entries in batches.
//!
//! `cargo bench -p porto-core --bench timer_wheel`

use std::future::{Future, poll_fn};
use std::hint::black_box;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use porto_core::Runtime;
use porto_core::time::{Clock, Deadline, Sleep, sleep_until};

/// A clock the bench moves by hand, so timers fire when it says.
#[derive(Clone, Default)]
struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    fn advance(&self, by: Duration) {
        let nanos = u64::try_from(by.as_nanos()).unwrap();
        self.0.fetch_add(nanos, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Deadline {
        Deadline::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

fn runtime() -> (Runtime, ManualClock) {
    let clock = ManualClock::default();
    (Runtime::builder().clock(clock.clone()).build(), clock)
}

/// Arms `count` sleeps due within `spread_micros` after `from`, polled with a no-op waker.
fn armed(rt: &Runtime, from: Deadline, spread_micros: u64, count: u64) -> Vec<Pin<Box<Sleep>>> {
    let mut cx = Context::from_waker(Waker::noop());
    rt.block_on(async {
        (0..count)
            .map(|index| {
                let offset = Duration::from_micros(index * 7919 % spread_micros + 1);
                let mut sleep = Box::pin(sleep_until(from.saturating_add(offset)));
                assert!(sleep.as_mut().poll(&mut cx).is_pending());
                sleep
            })
            .collect()
    })
}

fn insert_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer_wheel/insert_cancel");
    for others in [0, 10_000] {
        let (rt, _clock) = runtime();
        // Other timers on every level: insert and cancel stay O(1).
        let _others = armed(&rt, rt.now(), 3_600_000_000, others);
        let far = Deadline::from_duration(Duration::from_secs(60));
        rt.block_on(poll_fn(|cx| {
            group.bench_function(format!("{others}_armed"), |b| {
                b.iter(|| {
                    let mut sleep = pin!(sleep_until(black_box(far)));
                    let _ = black_box(sleep.as_mut().poll(cx));
                    // Dropped here: the entry is unlinked.
                });
            });
            Poll::Ready(())
        }));
    }
    group.finish();
}

fn fire(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer_wheel/fire");
    for count in [1_000u64, 10_000] {
        group.throughput(Throughput::Elements(count));
        let (rt, clock) = runtime();
        group.bench_function(format!("{count}_over_1s"), |b| {
            b.iter_batched(
                || armed(&rt, rt.now(), 1_000_000, count),
                |sleeps| {
                    clock.advance(Duration::from_secs(2));
                    // Fires every due timer (cascading on the way); no task runs.
                    rt.run_until_idle();
                    sleeps
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, insert_cancel, fire);
criterion_main!(benches);
