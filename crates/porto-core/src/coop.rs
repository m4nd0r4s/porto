//! The cooperative budget (`12-scheduler-and-lanes.md` §10), as in Tokio.
//!
//! A lane runs each task under [`budget`]: the task may do `coop_budget` operations in one poll.
//! A Rust resource that can complete without suspending (a channel with a message queued, a
//! lock that is free) calls [`poll_proceed`] first: it spends one unit of the budget, or, once
//! the budget is spent, wakes the task and returns `Pending`, so the task yields to the lane
//! (to the back of its queue) and a loop over always-ready operations cannot starve the others.
//!
//! ```
//! use std::future::poll_fn;
//! use std::task::Poll;
//!
//! let rt = porto_core::Runtime::builder().coop_budget(4).build();
//! let polls = rt.block_on(rt.spawn(async {
//!     let (mut polls, mut done) = (0, 0);
//!     // A resource that always has an item, behind `poll_proceed`.
//!     poll_fn(|cx| {
//!         polls += 1;
//!         loop {
//!             let proceed = std::task::ready!(porto_core::coop::poll_proceed(cx));
//!             proceed.made_progress();
//!             done += 1;
//!             if done == 10 {
//!                 return Poll::Ready(());
//!             }
//!         }
//!     })
//!     .await;
//!     polls
//! }));
//! assert_eq!(polls.unwrap(), 3, "4 + 4 + 2 operations");
//! ```
//!
//! Python tasks keep their budget in the frame of their poll rather than here (`porto-py`'s
//! step loop), starting from [`remaining`]: a step does not touch a thread local.

use std::cell::Cell;
use std::task::{Context, Poll};

thread_local! {
    /// The budget of the task polled on this thread; `None` when unconstrained (outside a
    /// lane's poll).
    static BUDGET: Cell<Option<u32>> = const { Cell::new(None) };
}

/// Runs `f` with a budget of `units` operations, and restores the previous budget afterwards
/// (also if `f` unwinds). Lanes call it around each task poll.
#[inline]
pub fn budget<R>(units: u32, f: impl FnOnce() -> R) -> R {
    with_budget(Some(units), f)
}

/// Runs `f` with no budget: [`poll_proceed`] always proceeds.
#[inline]
pub fn unconstrained<R>(f: impl FnOnce() -> R) -> R {
    with_budget(None, f)
}

#[inline]
fn with_budget<R>(units: Option<u32>, f: impl FnOnce() -> R) -> R {
    /// Puts the outer budget back.
    struct Restore(Option<u32>);

    impl Drop for Restore {
        #[inline]
        fn drop(&mut self) {
            BUDGET.set(self.0);
        }
    }

    let _restore = Restore(BUDGET.replace(units));
    f()
}

/// The units left in this poll's budget; `None` when unconstrained.
#[inline]
pub fn remaining() -> Option<u32> {
    BUDGET.get()
}

/// Whether the current task may still run an operation without yielding.
#[inline]
pub fn has_budget_remaining() -> bool {
    BUDGET.get() != Some(0)
}

/// Spends one unit of the budget: `Ready` if there was one; otherwise the task is woken (so it
/// runs again after the tasks queued before it) and this returns `Pending`.
///
/// The unit comes back when the returned guard is dropped, unless the caller marked it with
/// [`RestoreOnPending::made_progress`]: an operation that turned out not to be ready (and
/// returns `Pending` for its own reason) costs nothing.
#[inline]
pub fn poll_proceed(cx: &mut Context<'_>) -> Poll<RestoreOnPending> {
    let units = BUDGET.get();
    match units {
        Some(0) => {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
        Some(left) => {
            BUDGET.set(Some(left - 1));
            Poll::Ready(RestoreOnPending(Cell::new(units)))
        }
        None => Poll::Ready(RestoreOnPending(Cell::new(None))),
    }
}

/// The unit [`poll_proceed`] spent: given back on drop unless the operation made progress.
#[derive(Debug)]
#[must_use = "dropping the guard at once gives the unit back; call made_progress() once the \
              operation completed"]
pub struct RestoreOnPending(Cell<Option<u32>>);

impl RestoreOnPending {
    /// The operation completed: the unit stays spent.
    #[inline]
    pub fn made_progress(&self) {
        self.0.set(None);
    }
}

impl Drop for RestoreOnPending {
    #[inline]
    fn drop(&mut self) {
        if let Some(before) = self.0.get() {
            BUDGET.set(Some(before));
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use super::*;

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn counting_waker() -> (Arc<CountWakes>, Waker) {
        let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
        (wakes.clone(), Waker::from(wakes))
    }

    #[test]
    fn poll_proceed_returns_pending_at_exhaustion_and_wakes_the_task() {
        let (wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        budget(3, || {
            for left in (0..3).rev() {
                let Poll::Ready(proceed) = poll_proceed(&mut cx) else {
                    panic!("the budget had {} units left", left + 1);
                };
                proceed.made_progress();
                assert_eq!(remaining(), Some(left));
            }
            assert!(!has_budget_remaining());
            assert!(poll_proceed(&mut cx).is_pending());
            assert_eq!(
                wakes.0.load(Ordering::Relaxed),
                1,
                "woken to run again later"
            );
        });
    }

    #[test]
    fn a_unit_without_progress_is_given_back() {
        let (_wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        budget(1, || {
            let Poll::Ready(proceed) = poll_proceed(&mut cx) else {
                panic!("one unit left");
            };
            assert_eq!(remaining(), Some(0));
            drop(proceed);
            assert_eq!(remaining(), Some(1));
        });
    }

    #[test]
    fn budgets_nest_and_unconstrained_always_proceeds() {
        let (_wakes, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(remaining(), None, "outside a poll");
        budget(5, || {
            unconstrained(|| {
                for _ in 0..1000 {
                    assert!(poll_proceed(&mut cx).map(|p| p.made_progress()).is_ready());
                }
            });
            budget(0, || assert!(poll_proceed(&mut cx).is_pending()));
            assert_eq!(remaining(), Some(5));
        });
        assert_eq!(remaining(), None);
    }

    #[test]
    fn the_budget_is_restored_when_the_poll_unwinds() {
        let unwound = std::panic::catch_unwind(|| budget(2, || panic!("a task panicked")));
        assert!(unwound.is_err());
        assert_eq!(remaining(), None);
    }
}
