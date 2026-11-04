//! The task state word: lifecycle bits plus a reference count, in one `AtomicU32`.
//!
//! Layout and transitions follow `11-task-model-and-trap-protocol.md` §3, which follows
//! Tokio's `runtime/task/state.rs`. Every transition is a single atomic RMW (a CAS loop where
//! the next value depends on the current one), so wakers on any thread race safely with the
//! lane that polls the task.

use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};

/// A lane is currently polling the task.
pub const RUNNING: u32 = 1 << 0;
/// A wake arrived while the task was running: reschedule it when the poll returns.
pub const NOTIFIED: u32 = 1 << 1;
/// The future finished (or panicked) and has been dropped.
pub const COMPLETE: u32 = 1 << 2;
/// An enclosing scope requested cancellation (reserved: scopes arrive in M1).
pub const CANCEL_REQ: u32 = 1 << 3;
/// Someone holds a join handle (reserved: join handles arrive in M1).
pub const JOIN_INTEREST: u32 = 1 << 4;
/// The task sits in a run queue, which owns one reference to it.
pub const SCHEDULED: u32 = 1 << 5;
/// One reference in the high bits of the state word.
pub const REF_ONE: u32 = 1 << 6;
/// All lifecycle bits (everything below the reference count).
pub const LIFECYCLE_MASK: u32 = REF_ONE - 1;
/// The reference-count bits.
pub const REF_COUNT_MASK: u32 = !LIFECYCLE_MASK;

/// Refcounts at or above this abort the process (like `Arc`'s overflow guard): tens of
/// millions of live wakers for one task means a leak, and wrapping would be a use-after-free.
const MAX_REFS: u32 = (REF_COUNT_MASK >> REF_ONE.trailing_zeros()) / 2;

/// A snapshot of the state word.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Snapshot(u32);

impl Snapshot {
    /// The raw bits.
    pub fn bits(self) -> u32 {
        self.0
    }
    /// Whether [`RUNNING`] is set.
    pub fn is_running(self) -> bool {
        self.0 & RUNNING != 0
    }
    /// Whether [`NOTIFIED`] is set.
    pub fn is_notified(self) -> bool {
        self.0 & NOTIFIED != 0
    }
    /// Whether [`COMPLETE`] is set.
    pub fn is_complete(self) -> bool {
        self.0 & COMPLETE != 0
    }
    /// Whether [`SCHEDULED`] is set.
    pub fn is_scheduled(self) -> bool {
        self.0 & SCHEDULED != 0
    }
    /// The number of live references.
    pub fn ref_count(self) -> u32 {
        self.0 >> REF_ONE.trailing_zeros()
    }
    fn ref_inc(self) -> Self {
        if self.ref_count() >= MAX_REFS {
            std::process::abort();
        }
        Self(self.0 + REF_ONE)
    }
    fn ref_dec(self) -> Self {
        debug_assert!(self.ref_count() > 0, "task refcount underflow");
        Self(self.0 - REF_ONE)
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("running", &self.is_running())
            .field("notified", &self.is_notified())
            .field("complete", &self.is_complete())
            .field("scheduled", &self.is_scheduled())
            .field("refs", &self.ref_count())
            .finish()
    }
}

/// What a waker must do after a wake transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WakeAction {
    /// Nothing: the task is running (it will reschedule itself), queued or complete.
    DoNothing,
    /// Push the task to its scheduler; the queue owns the reference the wake carried or added.
    Submit,
    /// The wake dropped the last reference: free the task.
    Dealloc,
}

/// What the lane must do after a poll returned `Pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdleAction {
    /// The task is idle and other references (wakers) keep it alive.
    Idle,
    /// A wake arrived during the poll: push the task back, reusing the run reference.
    Reschedule,
    /// No waker survived the poll: nothing can ever wake the task, so free it.
    Dealloc,
}

/// The atomic state word of a task.
pub struct State(AtomicU32);

impl State {
    /// A freshly spawned task: queued, with the one reference its run queue owns.
    pub fn new_scheduled() -> Self {
        Self(AtomicU32::new(SCHEDULED | REF_ONE))
    }

    /// Reads the current state.
    pub fn load(&self) -> Snapshot {
        Snapshot(self.0.load(Ordering::Acquire))
    }

    /// Adds a reference (waker clone). Aborts on overflow.
    pub(crate) fn ref_inc(&self) {
        // Relaxed is enough: the caller already holds a reference, so the task cannot be
        // freed concurrently, and no data is published through the increment.
        let prev = Snapshot(self.0.fetch_add(REF_ONE, Ordering::Relaxed));
        if prev.ref_count() >= MAX_REFS {
            std::process::abort();
        }
    }

    /// Drops a reference. Returns `true` if it was the last one and the task must be freed.
    pub(crate) fn ref_dec(&self) -> bool {
        // AcqRel: the last decrement must see every write made through other references.
        let prev = Snapshot(self.0.fetch_sub(REF_ONE, Ordering::AcqRel));
        debug_assert!(prev.ref_count() > 0, "task refcount underflow");
        prev.ref_count() == 1
    }

    /// Poll start: SCHEDULED → RUNNING. The run queue's reference becomes the run reference.
    pub(crate) fn transition_to_running(&self) {
        // A queued task is never running, complete or notified (NOTIFIED only lives while
        // RUNNING), and wakes leave a queued task's lifecycle bits alone, so flipping the two
        // bits is a single RMW instead of a CAS loop.
        let prev = Snapshot(self.0.fetch_xor(RUNNING | SCHEDULED, Ordering::AcqRel));
        debug_assert!(
            prev.is_scheduled() && !prev.is_running() && !prev.is_complete() && !prev.is_notified(),
            "poll of a task that was not queued: {prev:?}"
        );
    }

    /// Poll end with `Pending`: clear RUNNING, or requeue if a wake arrived during the poll.
    pub(crate) fn transition_to_idle(&self) -> IdleAction {
        let mut action = IdleAction::Idle;
        self.update(|cur| {
            debug_assert!(cur.is_running() && !cur.is_complete() && !cur.is_scheduled());
            if cur.is_notified() {
                action = IdleAction::Reschedule;
                Snapshot((cur.0 & !(RUNNING | NOTIFIED)) | SCHEDULED)
            } else {
                let next = Snapshot(cur.0 & !RUNNING).ref_dec();
                action = if next.ref_count() == 0 {
                    IdleAction::Dealloc
                } else {
                    IdleAction::Idle
                };
                next
            }
        });
        action
    }

    /// Poll end with `Ready` (the future is already dropped): set COMPLETE, clear RUNNING and
    /// NOTIFIED, and drop the run reference. Returns `true` if the task must be freed.
    pub(crate) fn transition_to_complete_and_release(&self) -> bool {
        let mut last = false;
        self.update(|cur| {
            debug_assert!(cur.is_running() && !cur.is_complete());
            let next = Snapshot((cur.0 & !(RUNNING | NOTIFIED)) | COMPLETE).ref_dec();
            last = next.ref_count() == 0;
            next
        });
        last
    }

    /// `Waker::wake`: consumes the waker's reference.
    pub(crate) fn transition_to_notified_by_val(&self) -> WakeAction {
        let mut action = WakeAction::DoNothing;
        self.update(|cur| {
            if cur.is_running() {
                // The lane holds the run reference, so this decrement never reaches zero.
                action = WakeAction::DoNothing;
                Snapshot(cur.0 | NOTIFIED).ref_dec()
            } else if cur.is_complete() || cur.is_scheduled() {
                let next = cur.ref_dec();
                action = if next.ref_count() == 0 {
                    WakeAction::Dealloc
                } else {
                    WakeAction::DoNothing
                };
                next
            } else {
                // Idle: the waker's reference moves into the run queue.
                action = WakeAction::Submit;
                Snapshot(cur.0 | SCHEDULED)
            }
        });
        action
    }

    /// `Waker::wake_by_ref`: adds a reference only when the task must be queued.
    pub(crate) fn transition_to_notified_by_ref(&self) -> WakeAction {
        let mut action = WakeAction::DoNothing;
        self.update(|cur| {
            if cur.is_running() {
                action = WakeAction::DoNothing;
                Snapshot(cur.0 | NOTIFIED)
            } else if cur.is_complete() || cur.is_scheduled() {
                action = WakeAction::DoNothing;
                cur
            } else {
                action = WakeAction::Submit;
                Snapshot(cur.0 | SCHEDULED).ref_inc()
            }
        });
        action
    }

    /// A CAS loop. `f` may run several times; only the result of the last call is kept,
    /// so it must recompute every output from `cur`.
    fn update(&self, mut f: impl FnMut(Snapshot) -> Snapshot) {
        let mut cur = Snapshot(self.0.load(Ordering::Acquire));
        loop {
            let next = f(cur);
            if next == cur {
                return;
            }
            match self
                .0
                .compare_exchange_weak(cur.0, next.0, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(actual) => cur = Snapshot(actual),
            }
        }
    }
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.load().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running(refs: u32) -> State {
        let state = State::new_scheduled();
        for _ in 1..refs {
            state.ref_inc();
        }
        state.transition_to_running();
        state
    }

    #[test]
    fn spawned_task_is_scheduled_with_one_ref() {
        let s = State::new_scheduled().load();
        assert!(s.is_scheduled() && !s.is_running());
        assert_eq!(s.ref_count(), 1);
    }

    #[test]
    fn wake_while_running_sets_notified_once() {
        let state = running(3);
        assert_eq!(state.transition_to_notified_by_ref(), WakeAction::DoNothing);
        assert_eq!(state.transition_to_notified_by_val(), WakeAction::DoNothing);
        let s = state.load();
        assert!(s.is_running() && s.is_notified());
        assert_eq!(s.ref_count(), 2);
        assert_eq!(state.transition_to_idle(), IdleAction::Reschedule);
        let s = state.load();
        assert!(s.is_scheduled() && !s.is_running() && !s.is_notified());
        assert_eq!(
            s.ref_count(),
            2,
            "the run reference moves back into the queue"
        );
    }

    #[test]
    fn wake_of_idle_task_submits_once() {
        let state = running(2);
        assert_eq!(state.transition_to_idle(), IdleAction::Idle);
        assert_eq!(state.transition_to_notified_by_ref(), WakeAction::Submit);
        assert_eq!(state.transition_to_notified_by_ref(), WakeAction::DoNothing);
        let s = state.load();
        assert!(s.is_scheduled());
        assert_eq!(s.ref_count(), 2, "waker + queue");
    }

    #[test]
    fn pending_without_wakers_deallocates() {
        assert_eq!(running(1).transition_to_idle(), IdleAction::Dealloc);
    }

    #[test]
    fn wake_after_complete_only_drops_the_ref() {
        let state = running(2);
        assert!(!state.transition_to_complete_and_release());
        assert_eq!(state.transition_to_notified_by_ref(), WakeAction::DoNothing);
        assert_eq!(state.transition_to_notified_by_val(), WakeAction::Dealloc);
    }
}
