//! [`TaskTimer`]: a timer-wheel entry embedded in a task body, for the task's one sleep at a
//! time (`11-task-model-and-trap-protocol.md` §6, `14-time-and-timers.md` §2).
//!
//! Unlike a [`Sleep`](super::Sleep), it records no runtime: it is only ever polled by the task
//! that embeds it, on that task's runtime, and the waker it links is that task's, which keeps
//! the allocation (and so the entry) alive for as long as the entry is linked.

use std::fmt;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

use super::Deadline;
use super::wheel::TimerEntry;
use crate::sched::{Armed, Handle, with_current_handle};

/// A deadline and the wheel entry that wakes its task there. 56 bytes, no allocation.
pub struct TaskTimer {
    entry: TimerEntry,
    deadline: Deadline,
}

impl TaskTimer {
    /// An unarmed timer whose deadline is [`Deadline::NEVER`].
    pub const fn new() -> Self {
        Self {
            entry: TimerEntry::new(),
            deadline: Deadline::NEVER,
        }
    }

    /// The deadline the timer waits for.
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Sets the deadline for the next [`TaskTimer::poll`]. A linked entry stays where it is
    /// until then: at worst it wakes the task early, and the poll re-arms it.
    pub fn reset(self: Pin<&mut Self>, deadline: Deadline) {
        // SAFETY: only the deadline is written; the entry is not moved.
        unsafe { self.get_unchecked_mut() }.deadline = deadline;
    }

    /// `Ready` once the runtime clock reached the deadline (the entry is unlinked then);
    /// otherwise links the entry to wake `cx`'s waker at the deadline and returns `Pending`.
    ///
    /// # Panics
    /// When called outside a runtime.
    ///
    /// # Safety
    /// - Every `poll` and [`TaskTimer::cancel`] of this timer runs on the same runtime, on its
    ///   lane: the entry is linked in that runtime's wheel or in none.
    /// - `cx`'s waker owns (through its clones) a reference to whatever embeds the timer, so
    ///   the timer outlives its link: the wheel unlinks an entry before it drops its waker.
    pub unsafe fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.into_ref();
        let deadline = this.deadline;
        let entry = NonNull::from(&this.entry);
        let polled = with_current_handle(|handle| {
            if handle.now() >= deadline {
                // Fired, or due but not fired yet: either way the entry has nothing left to do.
                // SAFETY: the caller's contract: linked on this runtime or nowhere.
                drop(unsafe { disarm(handle, entry) });
                return Poll::Ready(());
            }
            // Cloned before the shard lock: a clone may run arbitrary code.
            let waker = cx.waker().clone();
            // SAFETY: the entry is pinned and outlives its link (the caller's contract).
            let armed = unsafe { handle.shared.timers.arm(entry, deadline, Some(waker)) };
            if let Armed::Linked(replaced) = armed {
                // Dropped here, outside the lock.
                drop(replaced);
            }
            Poll::Pending
        });
        polled.unwrap_or_else(|| {
            panic!(
                "porto_core::time::TaskTimer polled outside a runtime: poll it from the task \
                 that embeds it"
            )
        })
    }

    /// Unlinks the entry if it is linked: the task stops waiting for the deadline.
    ///
    /// # Panics
    /// When called outside a runtime.
    ///
    /// # Safety
    /// As for [`TaskTimer::poll`].
    pub unsafe fn cancel(self: Pin<&mut Self>) {
        let entry = NonNull::from(&self.into_ref().entry);
        let waker = with_current_handle(|handle| {
            // SAFETY: the caller's contract: linked on this runtime or nowhere.
            unsafe { disarm(handle, entry) }
        });
        let Some(waker) = waker else {
            panic!(
                "porto_core::time::TaskTimer cancelled outside a runtime: cancel it from the \
                 task that embeds it"
            );
        };
        // Dropped outside the shard lock.
        drop(waker);
    }
}

/// # Safety
/// `entry` is live, linked on `handle`'s runtime or nowhere.
unsafe fn disarm(handle: &Handle, entry: NonNull<TimerEntry>) -> Option<Waker> {
    // SAFETY: forwarded to the caller.
    unsafe { handle.shared.timers.disarm(entry) }
}

impl Default for TaskTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TaskTimer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskTimer")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::future::poll_fn;
    use std::marker::PhantomPinned;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use super::TaskTimer;
    use crate::Runtime;
    use crate::task::{Header, TaskBody, TaskFlags};
    use crate::time::Deadline;

    /// A task body that sleeps `naps` times for `nap` each, then finishes.
    struct Napper {
        timer: TaskTimer,
        nap: Duration,
        naps: usize,
        polls: Arc<AtomicUsize>,
        done: Arc<AtomicUsize>,
        _pinned: PhantomPinned,
    }

    impl TaskBody for Napper {
        fn poll(self: Pin<&mut Self>, _: &Header, cx: &mut Context<'_>) -> Poll<()> {
            // SAFETY: only `timer` is used pinned, and it is not moved out.
            let this = unsafe { self.get_unchecked_mut() };
            this.polls.fetch_add(1, Ordering::Relaxed);
            loop {
                if this.naps == 0 {
                    this.done.fetch_add(1, Ordering::Relaxed);
                    return Poll::Ready(());
                }
                // SAFETY: the timer is embedded in this task and never moves.
                let mut timer = unsafe { Pin::new_unchecked(&mut this.timer) };
                if timer.deadline() == Deadline::NEVER {
                    timer.as_mut().reset(Deadline::after(this.nap));
                }
                // SAFETY: polled by the task that embeds it, on its runtime, with its waker.
                if unsafe { timer.as_mut().poll(cx) }.is_pending() {
                    return Poll::Pending;
                }
                timer.reset(Deadline::NEVER);
                this.naps -= 1;
            }
        }

        fn cancel(self: Pin<&mut Self>, _: &Header) {}
    }

    fn napper(nap: Duration, naps: usize) -> (Napper, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let polls = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicUsize::new(0));
        let body = Napper {
            timer: TaskTimer::new(),
            nap,
            naps,
            polls: polls.clone(),
            done: done.clone(),
            _pinned: PhantomPinned,
        };
        (body, polls, done)
    }

    #[test]
    fn a_task_timer_wakes_its_task_once_per_sleep() {
        let rt = Runtime::new();
        let (body, polls, done) = napper(Duration::from_millis(2), 3);
        let task = rt.handle().spawn_body(body, TaskFlags::NONE);
        let started = std::time::Instant::now();
        rt.block_on(poll_fn(|cx| {
            if task.is_complete() {
                return Poll::Ready(());
            }
            // Busy-polled: the task does not wake the root.
            cx.waker().wake_by_ref();
            Poll::Pending
        }));
        assert!(
            started.elapsed() >= Duration::from_millis(6),
            "slept too little"
        );
        assert_eq!(done.load(Ordering::Relaxed), 1);
        assert_eq!(
            polls.load(Ordering::Relaxed),
            4,
            "the first poll, then one per sleep"
        );
    }

    #[test]
    fn dropping_the_runtime_frees_a_task_whose_timer_is_armed() {
        let rt = Runtime::new();
        let (body, polls, done) = napper(Duration::from_secs(3600), 1);
        let task = rt.handle().spawn_body(body, TaskFlags::NONE);
        rt.run_until_idle();
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert!(!task.is_complete());
        let freed = Arc::downgrade(&polls);
        drop(polls);
        drop(task);
        // The wheel holds the last reference, through the entry's waker.
        assert!(freed.upgrade().is_some());
        drop(rt);
        assert!(freed.upgrade().is_none(), "the task was not freed");
        assert_eq!(done.load(Ordering::Relaxed), 0);
    }
}
