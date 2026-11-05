//! The `Sleep` future: ready once the clock reaches its deadline.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use super::Deadline;
use crate::sched::register_timer;

/// A future that completes at `deadline`, never earlier. See [`sleep_until`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Sleep {
    deadline: Deadline,
    /// The identity of the waker the lane's timer holds, if one is registered. Not a `Waker`:
    /// a task's future holding its own task's waker is a reference cycle that would keep the
    /// task alive after its runtime is dropped.
    registered: Option<WakerIdentity>,
}

/// What `Waker::will_wake` compares: the data and vtable addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WakerIdentity(usize, usize);

impl WakerIdentity {
    fn of(waker: &Waker) -> Self {
        Self(
            waker.data().addr(),
            std::ptr::from_ref(waker.vtable()).addr(),
        )
    }
}

/// Completes at `deadline`, never earlier.
///
/// The timer lives on the lane that first polls the future. Dropping a pending `Sleep` does not
/// remove its timer: the entry fires (a spurious wake) at its deadline, and only then releases
/// the task (intrusive, removable entries arrive with the M1 timer wheel).
///
/// # Panics
/// When polled outside a porto runtime.
pub fn sleep_until(deadline: Deadline) -> Sleep {
    Sleep {
        deadline,
        registered: None,
    }
}

impl Sleep {
    /// When the sleep completes.
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Some(now) = Deadline::try_now() else {
            panic!(
                "porto_core::time::Sleep polled outside a runtime: await it from a task or \
                 inside Runtime::block_on"
            );
        };
        if now >= self.deadline {
            return Poll::Ready(());
        }
        // Polled again before the deadline: keep the registered entry unless the future moved
        // to another task, whose waker must be the one that fires.
        let identity = WakerIdentity::of(cx.waker());
        if self.registered != Some(identity) {
            register_timer(self.deadline, cx.waker().clone());
            self.registered = Some(identity);
        }
        Poll::Pending
    }
}
