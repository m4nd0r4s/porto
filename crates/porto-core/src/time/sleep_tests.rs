//! `Sleep` on the timer wheel: cancel on drop, far deadlines, resolution, reset.

use std::future::{Future, poll_fn};
use std::panic;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::{Clock, Deadline, Sleep, sleep_until};
use crate::{Runtime, spawn};

/// A virtual clock that autojumps to the deadline a lane parks for.
#[derive(Clone, Default)]
struct JumpingClock {
    nanos: Arc<AtomicU64>,
    jumps: Arc<AtomicUsize>,
}

impl JumpingClock {
    fn set(&self, at: Duration) {
        self.nanos
            .store(Deadline::from_duration(at).as_nanos(), Ordering::SeqCst);
    }
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

/// A waker that counts its wakes; `Arc::strong_count` says whether a timer still holds it.
#[derive(Default)]
struct CountingWaker {
    wakes: AtomicUsize,
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

/// Polls `sleep` once with `waker`, expecting it to wait.
fn poll_pending(sleep: Pin<&mut Sleep>, waker: &Arc<CountingWaker>) {
    let waker = Waker::from(waker.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(sleep.poll(&mut cx).is_pending());
}

fn next_timer(rt: &Runtime) -> String {
    let debug = format!("{rt:?}");
    let start = debug
        .find("next_timer: ")
        .expect("Runtime's Debug shows the next timer");
    debug[start..]
        .split([',', ' '])
        .nth(1)
        .unwrap_or_default()
        .to_owned()
}

fn virtual_runtime() -> (Runtime, JumpingClock) {
    let clock = JumpingClock::default();
    (Runtime::builder().clock(clock.clone()).build(), clock)
}

#[test]
fn dropping_a_pending_sleep_cancels_its_timer() {
    let (rt, clock) = virtual_runtime();
    let waker = Arc::new(CountingWaker::default());
    rt.block_on(async {
        let mut sleep = Box::pin(sleep_until(Deadline::from_nanos(5_000_000)));
        poll_pending(sleep.as_mut(), &waker);
        // The timer holds a clone of the waker...
        assert_eq!(Arc::strong_count(&waker), 2);
        drop(sleep);
        // ...and lets go of it with the sleep: the task is not kept alive until the deadline.
        assert_eq!(Arc::strong_count(&waker), 1);
    });
    assert_eq!(next_timer(&rt), "None");
    clock.set(Duration::from_secs(1));
    assert_eq!(rt.run_until_idle(), 0);
    // No spurious wake at the old deadline.
    assert_eq!(waker.wakes.load(Ordering::SeqCst), 0);
}

#[test]
fn a_sleep_dropped_on_another_thread_cancels_its_timer() {
    let (rt, clock) = virtual_runtime();
    let waker = Arc::new(CountingWaker::default());
    let mut sleep = Box::pin(sleep_until(Deadline::from_nanos(5_000_000)));
    rt.block_on(async { poll_pending(sleep.as_mut(), &waker) });
    assert_ne!(next_timer(&rt), "None");
    thread::spawn(move || drop(sleep)).join().unwrap();
    assert_eq!(next_timer(&rt), "None");
    assert_eq!(Arc::strong_count(&waker), 1);
    clock.set(Duration::from_secs(1));
    rt.run_until_idle();
    assert_eq!(waker.wakes.load(Ordering::SeqCst), 0);
}

#[test]
fn a_sleep_outliving_its_runtime_drops_cleanly() {
    let (rt, _clock) = virtual_runtime();
    let waker = Arc::new(CountingWaker::default());
    let mut sleep = Box::pin(sleep_until(Deadline::from_nanos(5_000_000)));
    rt.block_on(async { poll_pending(sleep.as_mut(), &waker) });
    // The runtime unlinks every entry and drops its wakers...
    drop(rt);
    assert_eq!(Arc::strong_count(&waker), 1);
    // ...and the sleep, which keeps the shard alive, finds its entry unlinked.
    drop(sleep);
}

#[test]
fn a_select_style_drop_does_not_wake_the_task_later() {
    let (rt, clock) = virtual_runtime();
    let polls = Arc::new(AtomicUsize::new(0));
    let in_task = polls.clone();
    rt.spawn(async move {
        // A timeout that loses: polled once, then dropped while pending.
        let mut timeout = Box::pin(sleep_until(Deadline::from_duration(Duration::from_secs(9))));
        poll_fn(|cx| {
            in_task.fetch_add(1, Ordering::SeqCst);
            assert!(timeout.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(timeout);
        // Then the task waits on something that never comes.
        poll_fn(|_| {
            in_task.fetch_add(1, Ordering::SeqCst);
            Poll::<()>::Pending
        })
        .await;
    });
    rt.run_until_idle();
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    clock.set(Duration::from_secs(10));
    rt.run_until_idle();
    // The dropped timeout would have repolled the task at 9 s.
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[test]
fn a_sleep_beyond_the_wheel_span_wakes_at_its_deadline() {
    let (rt, clock) = virtual_runtime();
    // 64^6 ms is about 2.2 years: five years is clamped and re-armed on the way.
    let five_years = Duration::from_secs(5 * 365 * 24 * 3600);
    let woke_at = rt.block_on(async move {
        sleep_until(Deadline::from_duration(five_years)).await;
        Deadline::now()
    });
    assert_eq!(woke_at, Deadline::from_duration(five_years));
    let jumps = clock.jumps.load(Ordering::SeqCst);
    assert!((2..=4).contains(&jumps), "{jumps} parks for five years");
}

#[test]
fn a_sleep_wakes_on_the_first_tick_at_or_after_its_deadline() {
    let clock = JumpingClock::default();
    let rt = Runtime::builder()
        .clock(clock.clone())
        .timer_resolution(Duration::from_millis(10))
        .build();
    let woke_at = rt.block_on(async {
        sleep_until(Deadline::from_duration(Duration::from_millis(15))).await;
        Deadline::now()
    });
    assert_eq!(woke_at, Deadline::from_duration(Duration::from_millis(20)));
    assert_eq!(clock.jumps.load(Ordering::SeqCst), 1);
}

#[test]
fn a_timer_resolution_out_of_range_panics_with_the_range() {
    for resolution in [Duration::from_micros(99), Duration::from_millis(11)] {
        let panic =
            panic::catch_unwind(|| Runtime::builder().timer_resolution(resolution)).unwrap_err();
        let message = panic.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(message.contains("between 100µs and 10ms"), "{message}");
    }
    let rt = Runtime::builder()
        .timer_resolution(Duration::from_micros(100))
        .build();
    rt.block_on(sleep_until(rt.now()));
    let debug = format!(
        "{:?}",
        Runtime::builder().timer_resolution(Duration::from_millis(10))
    );
    assert!(debug.contains("timer_resolution: Some(10ms)"), "{debug}");
}

#[test]
fn reset_moves_an_armed_timer_and_keeps_its_waker() {
    let (rt, _clock) = virtual_runtime();
    let woke_at = rt.block_on(async {
        let mut sleep = pin!(sleep_until(Deadline::from_duration(Duration::from_secs(
            3600
        ))));
        poll_fn(|cx| {
            assert!(sleep.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        sleep
            .as_mut()
            .reset(Deadline::from_duration(Duration::from_secs(2)));
        assert_eq!(
            sleep.deadline(),
            Deadline::from_duration(Duration::from_secs(2))
        );
        sleep.as_mut().await;
        let first = Deadline::now();
        // A completed sleep reset to a later deadline waits again.
        sleep
            .as_mut()
            .reset(Deadline::from_duration(Duration::from_secs(5)));
        sleep.await;
        (first, Deadline::now())
    });
    assert_eq!(
        woke_at,
        (
            Deadline::from_duration(Duration::from_secs(2)),
            Deadline::from_duration(Duration::from_secs(5))
        )
    );
}

#[test]
fn reset_from_another_thread_wakes_the_lane_for_the_new_deadline() {
    let rt = Runtime::new();
    let start = rt.now();
    let sleep = Arc::new(Mutex::new(Box::pin(sleep_until(Deadline::NEVER))));
    let in_task = sleep.clone();
    let woken = Arc::new(Mutex::new(None::<Waker>));
    let done = Arc::new(AtomicUsize::new(0));
    let (in_woken, in_done) = (woken.clone(), done.clone());
    rt.spawn(async move {
        poll_fn(|cx| in_task.lock().unwrap().as_mut().poll(cx)).await;
        in_done.fetch_add(1, Ordering::SeqCst);
        if let Some(waker) = in_woken.lock().unwrap().take() {
            waker.wake();
        }
    });
    let handle = rt.handle().clone();
    let resetter = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let soon = handle.now().saturating_add(Duration::from_millis(2));
        sleep.lock().unwrap().as_mut().reset(soon);
    });
    // The lane parks until NEVER unless the reset unparks it.
    rt.block_on(poll_fn(|cx| {
        if done.load(Ordering::SeqCst) == 1 {
            return Poll::Ready(());
        }
        *woken.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }));
    resetter.join().unwrap();
    assert!(rt.now().saturating_duration_since(start) < Duration::from_secs(5));
}

#[test]
fn a_sleep_polled_by_another_runtime_moves_its_timer() {
    let (first, _) = virtual_runtime();
    let (second, _) = virtual_runtime();
    let waker = Arc::new(CountingWaker::default());
    let mut sleep = Box::pin(sleep_until(Deadline::from_nanos(5_000_000)));
    first.block_on(async { poll_pending(sleep.as_mut(), &waker) });
    assert_ne!(next_timer(&first), "None");
    second.block_on(async { poll_pending(sleep.as_mut(), &waker) });
    assert_eq!(next_timer(&first), "None");
    assert_ne!(next_timer(&second), "None");
    drop(sleep);
    assert_eq!(next_timer(&second), "None");
    assert_eq!(Arc::strong_count(&waker), 1);
}

#[test]
fn many_timers_due_together_all_fire() {
    let (rt, _clock) = virtual_runtime();
    let done = Arc::new(AtomicUsize::new(0));
    // More than one firing batch, on one tick and spread over a few.
    for index in 0..100u64 {
        let done = done.clone();
        rt.spawn(async move {
            let at = Duration::from_millis(10 + index % 3);
            sleep_until(Deadline::from_duration(at)).await;
            assert!(Deadline::now() >= Deadline::from_duration(at));
            done.fetch_add(1, Ordering::SeqCst);
        });
    }
    rt.block_on(sleep_until(Deadline::from_duration(Duration::from_millis(
        13,
    ))));
    rt.run_until_idle();
    assert_eq!(done.load(Ordering::SeqCst), 100);
}

#[test]
fn spawned_sleepers_wake_in_deadline_order() {
    let (rt, _clock) = virtual_runtime();
    let woke = Arc::new(Mutex::new(Vec::new()));
    for (label, millis) in [("c", 3000), ("a", 5), ("d", 3000), ("b", 70)] {
        let woke = woke.clone();
        rt.spawn(async move {
            sleep_until(Deadline::from_duration(Duration::from_millis(millis))).await;
            woke.lock().unwrap().push(label);
        });
    }
    rt.block_on(async {
        spawn(async {});
        sleep_until(Deadline::from_duration(Duration::from_secs(4))).await;
    });
    assert_eq!(*woke.lock().unwrap(), ["a", "b", "c", "d"]);
}

#[test]
fn a_sleep_is_64_bytes() {
    assert_eq!(size_of::<Sleep>(), 64);
}
