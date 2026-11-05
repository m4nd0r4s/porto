//! The task state word: lifecycle bits plus a reference count, in one `AtomicU32`.
//!
//! Layout and transitions follow `11-task-model-and-trap-protocol.md` §3, which follows
//! Tokio's `runtime/task/state.rs`. Every transition is a single atomic RMW (a CAS loop where
//! the next value depends on the current one), so wakers on any thread race safely with the
//! lane that polls the task.

use std::fmt;
use std::sync::atomic::Ordering;

use super::sync::AtomicU32;

/// A lane is currently polling the task.
pub const RUNNING: u32 = 1 << 0;
/// A wake arrived while the task was running: reschedule it when the poll returns.
pub const NOTIFIED: u32 = 1 << 1;
/// The future finished (or panicked, or was cancelled) and has been dropped; the output, if
/// any, is stored.
pub const COMPLETE: u32 = 1 << 2;
/// Cancellation was requested. A Rust task seeing it at poll start drops its future instead of
/// polling it ([`JoinHandle::abort`](super::JoinHandle::abort), shutdown).
pub const CANCEL_REQ: u32 = 1 << 3;
/// A [`JoinHandle`](super::JoinHandle) is alive: completion leaves the output for it.
pub const JOIN_INTEREST: u32 = 1 << 4;
/// The task sits in a run queue, which owns one reference to it.
pub const SCHEDULED: u32 = 1 << 5;
/// The join handle stored a waker in the task. Hands the waker slot between the handle and the
/// lane that completes the task (see [`State::set_join_waker`]).
pub const JOIN_WAKER: u32 = 1 << 6;
/// One reference in the high bits of the state word.
pub const REF_ONE: u32 = 1 << 7;
/// All lifecycle bits (everything below the reference count).
pub const LIFECYCLE_MASK: u32 = REF_ONE - 1;
/// The reference-count bits.
pub const REF_COUNT_MASK: u32 = !LIFECYCLE_MASK;

/// Refcounts at or above this abort the process (like `Arc`'s overflow guard): millions of
/// live wakers for one task means a leak, and wrapping would be a use-after-free.
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
    /// Whether [`CANCEL_REQ`] is set.
    pub fn is_cancel_requested(self) -> bool {
        self.0 & CANCEL_REQ != 0
    }
    /// Whether [`JOIN_INTEREST`] is set.
    pub fn is_join_interested(self) -> bool {
        self.0 & JOIN_INTEREST != 0
    }
    /// Whether [`JOIN_WAKER`] is set.
    pub fn is_join_waker_set(self) -> bool {
        self.0 & JOIN_WAKER != 0
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
    fn set(self, bits: u32) -> Self {
        Self(self.0 | bits)
    }
    fn unset(self, bits: u32) -> Self {
        Self(self.0 & !bits)
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("running", &self.is_running())
            .field("notified", &self.is_notified())
            .field("complete", &self.is_complete())
            .field("cancel_req", &self.is_cancel_requested())
            .field("join_interest", &self.is_join_interested())
            .field("join_waker", &self.is_join_waker_set())
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

/// What a dropped [`JoinHandle`](super::JoinHandle) must clean up itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinDropAction {
    /// The task completed while the handle was alive, so the output is the handle's to drop.
    pub(crate) drop_output: bool,
    /// The waker slot is the handle's (no waker set, or the lane is done with it).
    pub(crate) drop_waker: bool,
}

/// The atomic state word of a task.
///
/// Memory ordering: every transition that changes the word is an `AcqRel` RMW, so a transition
/// that writes synchronizes with every later transition. A wake that finds the task queued,
/// running-and-notified or complete writes nothing (`wake_by_ref`), so it publishes nothing:
/// see `11-task-model-and-trap-protocol.md` §3.1.
pub struct State(AtomicU32);

impl State {
    /// A freshly spawned task: queued, with the reference its run queue owns, plus a second
    /// reference and JOIN_INTEREST if a join handle is created along with it.
    pub(crate) fn new_scheduled(join_handle: bool) -> Self {
        let bits = if join_handle {
            SCHEDULED | JOIN_INTEREST | (2 * REF_ONE)
        } else {
            SCHEDULED | REF_ONE
        };
        Self(AtomicU32::new(bits))
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
    /// Returns the state before, so the poll can see CANCEL_REQ.
    pub(crate) fn transition_to_running(&self) -> Snapshot {
        // A queued task is never running, complete or notified (NOTIFIED only lives while
        // RUNNING), and wakes leave a queued task's lifecycle bits alone, so flipping the two
        // bits is a single RMW instead of a CAS loop.
        let prev = Snapshot(self.0.fetch_xor(RUNNING | SCHEDULED, Ordering::AcqRel));
        debug_assert!(
            prev.is_scheduled() && !prev.is_running() && !prev.is_complete() && !prev.is_notified(),
            "poll of a task that was not queued: {prev:?}"
        );
        prev
    }

    /// Poll end with `Pending`: clear RUNNING, or requeue if a wake arrived during the poll.
    pub(crate) fn transition_to_idle(&self) -> IdleAction {
        let mut action = IdleAction::Idle;
        self.update(|cur| {
            debug_assert!(cur.is_running() && !cur.is_complete() && !cur.is_scheduled());
            if cur.is_notified() {
                action = IdleAction::Reschedule;
                cur.unset(RUNNING | NOTIFIED).set(SCHEDULED)
            } else {
                let next = cur.unset(RUNNING).ref_dec();
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

    /// Poll end with `Ready` (the future is dropped and the output stored): set COMPLETE and
    /// clear RUNNING and NOTIFIED. The run reference stays: the lane still wakes the joiner and
    /// drops what the joiner will not. Returns the new state.
    pub(crate) fn transition_to_complete(&self) -> Snapshot {
        let mut next = Snapshot(0);
        self.update(|cur| {
            debug_assert!(cur.is_running() && !cur.is_complete() && !cur.is_scheduled());
            next = cur.unset(RUNNING | NOTIFIED).set(COMPLETE);
            next
        });
        next
    }

    /// `Waker::wake`: consumes the waker's reference.
    pub(crate) fn transition_to_notified_by_val(&self) -> WakeAction {
        let mut action = WakeAction::DoNothing;
        self.update(|cur| {
            if cur.is_running() {
                // The lane holds the run reference, so this decrement never reaches zero.
                action = WakeAction::DoNothing;
                cur.set(NOTIFIED).ref_dec()
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
                cur.set(SCHEDULED)
            }
        });
        action
    }

    /// `Waker::wake_by_ref`: adds a reference only when the task must be queued.
    pub(crate) fn transition_to_notified_by_ref(&self) -> WakeAction {
        self.notify_by_ref(0)
    }

    /// `JoinHandle::abort`: set CANCEL_REQ and make sure the task gets polled once more, which
    /// cancels it. Adds a reference only when the task must be queued.
    pub(crate) fn transition_to_cancel_requested(&self) -> WakeAction {
        self.notify_by_ref(CANCEL_REQ)
    }

    /// A by-reference wake that also sets `extra`.
    fn notify_by_ref(&self, extra: u32) -> WakeAction {
        let mut action = WakeAction::DoNothing;
        self.update(|cur| {
            if cur.is_complete() {
                action = WakeAction::DoNothing;
                cur
            } else if cur.is_running() {
                action = WakeAction::DoNothing;
                cur.set(NOTIFIED | extra)
            } else if cur.is_scheduled() {
                action = WakeAction::DoNothing;
                cur.set(extra)
            } else {
                action = WakeAction::Submit;
                cur.set(SCHEDULED | extra).ref_inc()
            }
        });
        action
    }

    /// The join handle stored its waker: publish it to the lane. Fails (returning the state)
    /// if the task completed first; the handle then still owns the waker slot.
    ///
    /// Ownership of the waker slot: while JOIN_WAKER is clear only the join handle touches
    /// it. While it is set the handle may only read it, and the lane may read it once the
    /// task is COMPLETE. After completion only the lane clears JOIN_WAKER
    /// ([`Self::unset_join_waker_after_complete`]); whoever clears the last of JOIN_INTEREST
    /// and JOIN_WAKER drops the waker.
    pub(crate) fn set_join_waker(&self) -> Result<Snapshot, Snapshot> {
        self.try_update(|cur| {
            debug_assert!(cur.is_join_interested() && !cur.is_join_waker_set());
            if cur.is_complete() {
                return None;
            }
            Some(cur.set(JOIN_WAKER))
        })
    }

    /// The join handle takes its waker slot back to store a different waker. Fails if the
    /// task completed: the lane may be reading the waker.
    pub(crate) fn unset_join_waker(&self) -> Result<Snapshot, Snapshot> {
        self.try_update(|cur| {
            debug_assert!(cur.is_join_interested() && cur.is_join_waker_set());
            if cur.is_complete() {
                return None;
            }
            Some(cur.unset(JOIN_WAKER))
        })
    }

    /// The lane woke the joiner after completion and gives up the waker slot. Returns the new
    /// state: without JOIN_INTEREST the handle is gone, and the lane drops the waker.
    pub(crate) fn unset_join_waker_after_complete(&self) -> Snapshot {
        let prev = Snapshot(self.0.fetch_and(!JOIN_WAKER, Ordering::AcqRel));
        debug_assert!(prev.is_complete() && prev.is_join_waker_set());
        prev.unset(JOIN_WAKER)
    }

    /// The join handle is dropped: clear JOIN_INTEREST, and JOIN_WAKER too if the task has not
    /// completed (the lane never touches the waker then). The handle's reference is dropped
    /// separately, after the cleanup.
    pub(crate) fn transition_to_join_handle_dropped(&self) -> JoinDropAction {
        let mut action = JoinDropAction {
            drop_output: false,
            drop_waker: false,
        };
        self.update(|cur| {
            debug_assert!(cur.is_join_interested());
            let mut next = cur.unset(JOIN_INTEREST);
            if !cur.is_complete() {
                next = next.unset(JOIN_WAKER);
            }
            action = JoinDropAction {
                drop_output: cur.is_complete(),
                drop_waker: !next.is_join_waker_set(),
            };
            next
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

    /// A CAS loop that gives up (returning the current state) when `f` returns `None`.
    fn try_update(
        &self,
        mut f: impl FnMut(Snapshot) -> Option<Snapshot>,
    ) -> Result<Snapshot, Snapshot> {
        let mut cur = Snapshot(self.0.load(Ordering::Acquire));
        loop {
            let Some(next) = f(cur) else {
                return Err(cur);
            };
            match self
                .0
                .compare_exchange_weak(cur.0, next.0, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(next),
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

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    /// A RUNNING task with `refs` references (the run reference included).
    fn running(refs: u32, join_handle: bool) -> State {
        let state = State::new_scheduled(join_handle);
        let have = if join_handle { 2 } else { 1 };
        for _ in have..refs {
            state.ref_inc();
        }
        state.transition_to_running();
        state
    }

    #[test]
    fn spawned_task_is_scheduled_with_one_ref() {
        let s = State::new_scheduled(false).load();
        assert!(s.is_scheduled() && !s.is_running() && !s.is_join_interested());
        assert_eq!(s.ref_count(), 1);
    }

    #[test]
    fn spawn_with_a_join_handle_adds_its_reference_and_interest() {
        let s = State::new_scheduled(true).load();
        assert!(s.is_scheduled() && s.is_join_interested() && !s.is_join_waker_set());
        assert_eq!(s.ref_count(), 2);
    }

    #[test]
    fn poll_start_moves_scheduled_to_running_and_reports_cancel() {
        let state = State::new_scheduled(false);
        let prev = state.transition_to_running();
        assert!(prev.is_scheduled() && !prev.is_cancel_requested());
        let s = state.load();
        assert!(s.is_running() && !s.is_scheduled());
        assert_eq!(
            s.ref_count(),
            1,
            "the queue reference became the run reference"
        );

        let state = State::new_scheduled(false);
        assert_eq!(
            state.transition_to_cancel_requested(),
            WakeAction::DoNothing
        );
        assert!(state.transition_to_running().is_cancel_requested());
    }

    #[test]
    fn wake_while_running_sets_notified_once() {
        let state = running(3, false);
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
        let state = running(2, false);
        assert_eq!(state.transition_to_idle(), IdleAction::Idle);
        assert_eq!(state.transition_to_notified_by_ref(), WakeAction::Submit);
        assert_eq!(state.transition_to_notified_by_ref(), WakeAction::DoNothing);
        let s = state.load();
        assert!(s.is_scheduled());
        assert_eq!(s.ref_count(), 2, "waker + queue");
    }

    #[test]
    fn by_value_wake_of_idle_task_moves_its_reference_into_the_queue() {
        let state = running(2, false);
        assert_eq!(state.transition_to_idle(), IdleAction::Idle);
        assert_eq!(state.transition_to_notified_by_val(), WakeAction::Submit);
        let s = state.load();
        assert!(s.is_scheduled());
        assert_eq!(s.ref_count(), 1);
    }

    #[test]
    fn by_value_wake_of_queued_task_drops_its_reference() {
        let state = State::new_scheduled(false);
        state.ref_inc();
        assert_eq!(state.transition_to_notified_by_val(), WakeAction::DoNothing);
        assert_eq!(state.load().ref_count(), 1);
    }

    #[test]
    fn pending_without_wakers_deallocates() {
        assert_eq!(running(1, false).transition_to_idle(), IdleAction::Dealloc);
    }

    #[test]
    fn completion_keeps_the_run_reference_and_clears_notified() {
        let state = running(2, false);
        state.transition_to_notified_by_ref();
        let s = state.transition_to_complete();
        assert!(s.is_complete() && !s.is_running() && !s.is_notified());
        assert_eq!(s.ref_count(), 2);
        assert_eq!(state.load(), s);
    }

    #[test]
    fn wake_after_complete_only_drops_the_ref() {
        let state = running(2, false);
        state.transition_to_complete();
        assert!(!state.ref_dec());
        assert_eq!(state.transition_to_notified_by_ref(), WakeAction::DoNothing);
        assert_eq!(
            state.transition_to_cancel_requested(),
            WakeAction::DoNothing
        );
        assert_eq!(state.transition_to_notified_by_val(), WakeAction::Dealloc);
    }

    #[test]
    fn cancel_of_idle_task_queues_it() {
        let state = running(2, true);
        assert_eq!(state.transition_to_idle(), IdleAction::Idle);
        assert_eq!(state.transition_to_cancel_requested(), WakeAction::Submit);
        let s = state.load();
        assert!(s.is_scheduled() && s.is_cancel_requested());
        assert_eq!(s.ref_count(), 2, "join handle + queue");
        assert_eq!(
            state.transition_to_cancel_requested(),
            WakeAction::DoNothing
        );
    }

    #[test]
    fn cancel_of_running_task_makes_it_requeue() {
        let state = running(2, true);
        assert_eq!(
            state.transition_to_cancel_requested(),
            WakeAction::DoNothing
        );
        assert!(state.load().is_notified());
        assert_eq!(state.transition_to_idle(), IdleAction::Reschedule);
        assert!(state.transition_to_running().is_cancel_requested());
    }

    #[test]
    fn join_waker_is_set_and_unset_before_completion() {
        let state = running(2, true);
        let s = state.set_join_waker().unwrap();
        assert!(s.is_join_waker_set());
        let s = state.unset_join_waker().unwrap();
        assert!(!s.is_join_waker_set());
    }

    #[test]
    fn join_waker_cannot_be_set_or_taken_back_after_completion() {
        let state = running(2, true);
        state.transition_to_complete();
        assert!(state.set_join_waker().unwrap_err().is_complete());

        let state = running(2, true);
        state.set_join_waker().unwrap();
        state.transition_to_complete();
        assert!(state.unset_join_waker().unwrap_err().is_join_waker_set());
        let s = state.unset_join_waker_after_complete();
        assert!(!s.is_join_waker_set() && s.is_join_interested());
    }

    #[test]
    fn join_handle_drop_before_completion_owns_the_waker_but_not_the_output() {
        let state = running(2, true);
        state.set_join_waker().unwrap();
        let action = state.transition_to_join_handle_dropped();
        assert_eq!(
            action,
            JoinDropAction {
                drop_output: false,
                drop_waker: true
            }
        );
        let s = state.load();
        assert!(!s.is_join_interested() && !s.is_join_waker_set());
    }

    #[test]
    fn join_handle_drop_after_completion_owns_the_output() {
        let state = running(2, true);
        state.set_join_waker().unwrap();
        state.transition_to_complete();
        let action = state.transition_to_join_handle_dropped();
        assert_eq!(
            action,
            JoinDropAction {
                drop_output: true,
                drop_waker: false
            },
            "the lane still holds JOIN_WAKER, so it drops the waker"
        );
        let s = state.unset_join_waker_after_complete();
        assert!(!s.is_join_interested(), "the lane sees the handle is gone");
    }

    #[test]
    fn join_handle_drop_after_the_lane_released_the_waker_drops_it() {
        let state = running(2, true);
        state.set_join_waker().unwrap();
        state.transition_to_complete();
        state.unset_join_waker_after_complete();
        let action = state.transition_to_join_handle_dropped();
        assert_eq!(
            action,
            JoinDropAction {
                drop_output: true,
                drop_waker: true
            }
        );
    }
}
