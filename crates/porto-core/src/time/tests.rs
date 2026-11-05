//! Runtime clock, `Sleep` and lane timer tests.

use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use super::{Clock, Deadline, MonotonicClock, sleep_until};
use crate::{ParkHook, Runtime, spawn};

/// `n` time units: milliseconds, or seconds under Miri, whose virtual clock runs much faster
/// than the interpreter (parking advances it for free, so long sleeps cost nothing there).
fn ticks(n: u64) -> Duration {
    if cfg!(miri) {
        Duration::from_secs(n)
    } else {
        Duration::from_millis(n)
    }
}

#[test]
fn a_deadline_is_eight_bytes_even_in_an_option() {
    assert_eq!(size_of::<Deadline>(), 8);
    assert_eq!(size_of::<Option<Deadline>>(), 8);
}

#[test]
fn deadline_arithmetic_saturates() {
    assert_eq!(Deadline::from_nanos(0), Deadline::from_nanos(1));
    assert_eq!(
        Deadline::from_nanos(5).saturating_add(Duration::MAX),
        Deadline::NEVER
    );
    assert_eq!(
        Deadline::from_nanos(5).saturating_duration_since(Deadline::from_nanos(7)),
        Duration::ZERO
    );
    assert_eq!(
        Deadline::from_nanos(1_500_000_000).saturating_duration_since(Deadline::from_nanos(1)),
        Duration::from_nanos(1_499_999_999)
    );
    assert!((Deadline::from_nanos(2_500_000_000).as_secs_f64() - 2.5).abs() < 1e-12);
    assert_eq!(format!("{:?}", Deadline::NEVER), "Deadline(never)");
    assert_eq!(
        format!("{:?}", Deadline::from_nanos(2_000)),
        "Deadline(2µs)"
    );
}

#[test]
fn the_clock_is_monotonic() {
    let rt = Runtime::new();
    let mut previous = rt.now();
    for _ in 0..100 {
        let now = rt.now();
        assert!(now >= previous);
        previous = now;
    }
    rt.block_on(async {
        let first = Deadline::now();
        let second = Deadline::now();
        assert!(second >= first);
        assert!(Deadline::after(ticks(10)) > second);
    });
    assert!(rt.now() >= previous);
}

#[test]
fn the_clock_starts_at_zero_when_the_runtime_is_built() {
    let first = Runtime::new();
    // Under a second (generous for slow CI), not the process's age.
    assert!(
        first
            .now()
            .saturating_duration_since(Deadline::from_nanos(0))
            < Duration::from_secs(1)
    );
    thread::sleep(ticks(5));
    let second = Runtime::new();
    // Each runtime counts from its own start.
    assert!(second.now() < first.now());
    assert!(first.now().saturating_duration_since(second.now()) >= ticks(5));
}

#[test]
fn a_monotonic_clock_reads_the_time_since_its_epoch() {
    let clock = MonotonicClock::new();
    let before = clock.now();
    thread::sleep(ticks(2));
    let after = clock.now();
    assert!(after.saturating_duration_since(before) >= ticks(2));
    assert!(after.saturating_duration_since(Deadline::from_nanos(0)) <= clock.epoch().elapsed());
}

#[test]
fn every_reader_sees_the_runtime_clock() {
    let rt = Runtime::new();
    let handle = rt.handle().clone();
    let before = rt.now();
    let inside = rt.block_on(async { Deadline::now() });
    let from_thread = thread::spawn(move || handle.now()).join().unwrap();
    assert!(before <= inside && inside <= from_thread && from_thread <= rt.now());
}

#[test]
fn the_clock_is_only_readable_inside_a_runtime() {
    assert_eq!(Deadline::try_now(), None);
    let panic = panic::catch_unwind(Deadline::now).unwrap_err();
    let message = panic.downcast_ref::<String>().cloned().unwrap_or_default();
    assert!(message.contains("outside a runtime"), "{message}");
    assert!(message.contains("Handle::now"), "{message}");
    let rt = Runtime::new();
    assert!(rt.block_on(async { Deadline::try_now() }).is_some());
}

/// A virtual clock that autojumps: parking until a deadline moves the clock there at once.
#[derive(Clone, Default)]
struct JumpingClock {
    nanos: Arc<AtomicU64>,
    jumps: Arc<AtomicUsize>,
}

impl Clock for JumpingClock {
    fn now(&self) -> Deadline {
        Deadline::from_nanos(self.nanos.load(Ordering::SeqCst))
    }

    fn park_until(&self, deadline: Deadline) {
        self.jumps.fetch_add(1, Ordering::SeqCst);
        self.nanos.fetch_max(deadline.as_nanos(), Ordering::SeqCst);
    }
}

#[test]
fn a_custom_clock_drives_deadlines_timers_and_parks() {
    let clock = JumpingClock::default();
    let rt = Runtime::builder().clock(clock.clone()).build();
    assert_eq!(rt.now(), Deadline::from_nanos(0));
    let hour = Duration::from_secs(3600);
    let started = Instant::now();
    let woke_at = rt.block_on(async move {
        sleep_until(Deadline::from_duration(hour)).await;
        Deadline::now()
    });
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "an hour of virtual time took {:?}",
        started.elapsed()
    );
    assert_eq!(woke_at, Deadline::from_duration(hour));
    assert_eq!(rt.now(), Deadline::from_duration(hour));
    assert_eq!(rt.handle().now(), Deadline::from_duration(hour));
    assert_eq!(clock.jumps.load(Ordering::SeqCst), 1);
}

#[test]
fn a_custom_clock_works_with_a_park_hook() {
    let clock = JumpingClock::default();
    let hook = CountingHook::default();
    let rt = Runtime::builder()
        .park_hook(hook.clone())
        .clock(clock.clone())
        .build();
    rt.block_on(sleep_until(Deadline::from_duration(Duration::from_secs(
        60,
    ))));
    assert_eq!(rt.now(), Deadline::from_duration(Duration::from_secs(60)));
    assert_eq!(hook.around.load(Ordering::SeqCst), 1);
    assert_eq!(clock.jumps.load(Ordering::SeqCst), 1);
    assert!(format!("{:?}", Runtime::builder().clock(clock)).contains("clock: true"));
}

#[test]
fn a_sleep_never_completes_before_its_deadline() {
    let rt = Runtime::new();
    for delay in [0, 1, 3] {
        let deadline = rt.now().saturating_add(ticks(delay));
        rt.block_on(sleep_until(deadline));
        assert!(rt.now() >= deadline);
    }
}

#[test]
fn a_past_deadline_is_ready_without_a_timer() {
    let rt = Runtime::new();
    let ran = Arc::new(AtomicUsize::new(0));
    let in_task = ran.clone();
    rt.spawn(async move {
        sleep_until(Deadline::from_nanos(1)).await;
        in_task.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(rt.run_until_idle(), 1);
    assert_eq!(ran.load(Ordering::SeqCst), 1);
}

#[test]
fn sleeps_wake_in_deadline_order_and_ties_in_registration_order() {
    let rt = Runtime::new();
    let woke = Arc::new(Mutex::new(Vec::new()));
    let start = rt.now();
    // (label, delay): registered in this order, so equal delays keep it.
    let sleepers = [("c", 6), ("a", 2), ("d", 6), ("b", 4), ("e", 8)];
    let main_woke = woke.clone();
    rt.block_on(async move {
        for (label, delay) in sleepers {
            let woke = main_woke.clone();
            spawn(async move {
                let deadline = start.saturating_add(ticks(delay));
                sleep_until(deadline).await;
                assert!(Deadline::now() >= deadline, "{label} woke early");
                woke.lock().unwrap().push(label);
            });
        }
        while main_woke.lock().unwrap().len() < sleepers.len() {
            sleep_until(Deadline::after(ticks(1))).await;
        }
    });
    assert_eq!(*woke.lock().unwrap(), ["a", "b", "c", "d", "e"]);
}

#[test]
fn run_until_idle_fires_due_timers_but_does_not_wait() {
    let rt = Runtime::new();
    let done = Arc::new(AtomicUsize::new(0));
    let in_task = done.clone();
    let deadline = rt.now().saturating_add(ticks(2));
    rt.spawn(async move {
        sleep_until(deadline).await;
        in_task.fetch_add(1, Ordering::SeqCst);
    });
    rt.run_until_idle();
    if rt.now() < deadline {
        assert_eq!(done.load(Ordering::SeqCst), 0);
    }
    while rt.now() < deadline {
        thread::sleep(ticks(1));
    }
    rt.run_until_idle();
    assert_eq!(done.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_the_runtime_frees_tasks_waiting_on_timers() {
    struct DropCounter(Arc<AtomicUsize>);
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let rt = Runtime::new();
    let counter = DropCounter(dropped.clone());
    rt.spawn(async move {
        let _counter = counter;
        sleep_until(Deadline::NEVER).await;
    });
    rt.run_until_idle();
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(rt);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn a_sleep_polled_outside_a_runtime_panics_with_a_hint() {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut sleep = sleep_until(Deadline::NEVER);
    let panic = panic::catch_unwind(AssertUnwindSafe(|| {
        std::future::Future::poll(std::pin::Pin::new(&mut sleep), &mut cx)
    }))
    .unwrap_err();
    let message = panic.downcast_ref::<&str>().copied().unwrap_or_default();
    assert!(message.contains("outside a runtime"), "{message}");
}

/// Counts parks and checks they nest: `park` runs inside the hook, once per call.
#[derive(Clone, Default)]
struct CountingHook {
    around: Arc<AtomicUsize>,
    parks: Arc<AtomicUsize>,
}

impl ParkHook for CountingHook {
    fn around_park(&self, park: &mut dyn FnMut()) {
        self.around.fetch_add(1, Ordering::SeqCst);
        let before = self.parks.load(Ordering::SeqCst);
        park();
        self.parks.store(before + 1, Ordering::SeqCst);
    }
}

#[test]
fn the_park_hook_wraps_every_park() {
    let hook = CountingHook::default();
    let rt = Runtime::with_park_hook(hook.clone());
    // Nothing to park for: the hook is not called.
    rt.block_on(async {});
    assert_eq!(hook.around.load(Ordering::SeqCst), 0);
    // A sleep with nothing else to run parks the lane through the hook.
    let deadline = rt.now().saturating_add(ticks(3));
    rt.block_on(sleep_until(deadline));
    assert!(rt.now() >= deadline);
    let around = hook.around.load(Ordering::SeqCst);
    assert!(around >= 1);
    assert_eq!(hook.parks.load(Ordering::SeqCst), around);
}

/// A hook that does not call `park`: the lane keeps spinning but stays correct.
struct SpinningHook;

impl ParkHook for SpinningHook {
    fn around_park(&self, _park: &mut dyn FnMut()) {}
}

#[test]
fn a_hook_that_skips_the_park_only_spins() {
    let rt = Runtime::with_park_hook(SpinningHook);
    let deadline = rt.now().saturating_add(ticks(1));
    rt.block_on(sleep_until(deadline));
    assert!(rt.now() >= deadline);
}
