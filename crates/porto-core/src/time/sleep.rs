//! The `Sleep` future: ready once the clock reaches its deadline.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

use super::Deadline;
use super::wheel::TimerEntry;
use crate::sched::{Armed, Handle, with_current_handle};

/// A future that completes at `deadline`, never earlier. See [`sleep_until`].
///
/// It embeds its timer-wheel entry, so it allocates nothing, and it is `!Unpin`: pin it
/// (`std::pin::pin!`, `Box::pin`) to poll it. The first poll that has to wait links the entry
/// into the wheel of the polling lane; dropping the `Sleep`, on any thread, unlinks it, so a
/// dropped sleep never wakes its task and does not keep it alive.
#[must_use = "futures do nothing unless polled"]
pub struct Sleep {
    entry: TimerEntry,
    deadline: Deadline,
    /// The runtime whose wheel the entry was last armed in; the entry is linked there or
    /// nowhere.
    armed_on: Option<Handle>,
}

/// Completes at `deadline`, never earlier.
///
/// The timer lives on the lane that polls the future. A pending `Sleep` that is dropped
/// cancels its timer.
///
/// # Panics
/// When polled outside a porto runtime.
pub fn sleep_until(deadline: Deadline) -> Sleep {
    Sleep {
        entry: TimerEntry::new(),
        deadline,
        armed_on: None,
    }
}

impl Sleep {
    /// When the sleep completes.
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// Moves the deadline to `deadline`, whether the sleep has completed or not: it completes
    /// again once the clock reaches the new one. An armed timer moves with it and keeps its
    /// waker, so a task waiting on the sleep wakes at the new deadline without being polled
    /// first.
    pub fn reset(self: Pin<&mut Self>, deadline: Deadline) {
        // SAFETY: nothing is moved out of the pinned sleep.
        let this = unsafe { self.get_unchecked_mut() };
        this.deadline = deadline;
        let Some(handle) = &this.armed_on else {
            return;
        };
        let entry = NonNull::from(&this.entry);
        // SAFETY: the entry is live and pinned, linked on `handle`'s lane or nowhere, and
        // `Drop` disarms it there.
        let armed = unsafe { handle.shared.timers.arm(entry, deadline, None) };
        if matches!(armed, Armed::Linked(_))
            && with_current_handle(|current| current.same_runtime(handle)) != Some(true)
        {
            // The lane may be parked until the old deadline: let it look again.
            handle.shared.unpark_lane();
        }
    }

    /// Unlinks the entry if it is armed; returns its waker, for the caller to drop.
    fn disarm(&mut self) -> Option<Waker> {
        let handle = self.armed_on.as_ref()?;
        // SAFETY: the entry is live, linked on `handle`'s lane or nowhere.
        unsafe { handle.shared.timers.disarm(NonNull::from(&self.entry)) }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: nothing is moved out of the pinned sleep.
        let this = unsafe { self.get_unchecked_mut() };
        let Some(poll) = with_current_handle(|current| {
            if current.now() >= this.deadline {
                // Fired, or due but not fired yet: either way the entry has nothing left to do.
                drop(this.disarm());
                return Poll::Ready(());
            }
            if let Some(armed_on) = &this.armed_on
                && !armed_on.same_runtime(current)
            {
                // Polled by another runtime: the entry moves to its wheel.
                drop(this.disarm());
                this.armed_on = None;
            }
            let handle = this.armed_on.get_or_insert_with(|| current.clone());
            // Cloned before the shard lock: a clone may run arbitrary code.
            let waker = cx.waker().clone();
            let entry = NonNull::from(&this.entry);
            // SAFETY: the entry is live and pinned, linked on `handle`'s lane or nowhere, and
            // `Drop` disarms it there.
            let armed = unsafe { handle.shared.timers.arm(entry, this.deadline, Some(waker)) };
            if let Armed::Linked(replaced) = armed {
                // Dropped here, outside the lock.
                drop(replaced);
            }
            Poll::Pending
        }) else {
            panic!(
                "porto_core::time::Sleep polled outside a runtime: await it from a task or \
                 inside Runtime::block_on"
            );
        };
        poll
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        // Unlinked before the memory goes; the waker is dropped outside the shard lock.
        drop(self.disarm());
    }
}

impl fmt::Debug for Sleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sleep")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}
