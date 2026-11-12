//! Scope deadlines (`15-structured-concurrency-and-cancellation.md` C3, C9; `14-time-and-
//! timers.md` §2): the wheel entry a [`ScopeNode`] embeds, which cancels the scope with
//! [`CancelReason::Deadline`] once its deadline passed.
//!
//! The entry's waker is the node itself (a counted reference, through [`DeadlineWaker`]): the
//! wheel keeps the node alive while the entry is linked, and firing the entry is waking that
//! waker, which cancels the scope. Every arm and disarm runs under the scope's lock, so a
//! deadline set from one thread, the scope closed on another and the timer fired on the lane
//! are ordered: a closed scope never re-arms, and a fire that lost a race with a later deadline
//! finds the deadline not reached and leaves the scope alone (the later deadline linked the
//! entry again).
//!
//! Each scope arms only its own deadline. The effective deadline of a task, the earliest over
//! the scopes it is in up to the first shielded one (C9), follows from the walk (§1.2): an
//! enclosing scope's deadline cancels that scope, which reaches the task unless a shield is in
//! between.
//!
//! Lock order: the scope's lock, then the wheel shard's. The wheel wakes fired entries after
//! releasing its lock, so a fire takes the scope's lock with no shard lock held.

use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::task::{RawWaker, RawWakerVTable, Waker};

use super::node::{CancelReason, ScopeNode, ScopeState};
use super::sync::Arc;
use crate::sched::{Armed, Handle, WeakHandle, with_current_handle};
use crate::time::Deadline;

impl<E> ScopeNode<E> {
    /// When the scope cancels itself, [`Deadline::NEVER`] if it has no deadline.
    pub fn deadline(&self) -> Deadline {
        self.lock_state().deadline
    }

    /// Unlinks the deadline timer if it is linked, and returns its waker (a reference to this
    /// node), for the caller to drop after the lock.
    pub(super) fn disarm_deadline(&self, state: &mut ScopeState<E>) -> Option<Waker> {
        let handle = state.deadline_on.take()?;
        // A runtime that is gone unlinked every entry when it shut its wheel down.
        let handle = handle.upgrade()?;
        let entry = NonNull::from(&self.deadline_timer);
        // SAFETY: the entry is live (it is in `self`), and `deadline_on` names the only
        // runtime whose wheel can link it; arms and disarms are serialized by the scope's lock,
        // which `state` proves is held.
        unsafe { handle.shared.timers.disarm(entry) }
    }
}

impl<E: Send + 'static> ScopeNode<E> {
    /// Sets the scope's deadline on `handle`'s clock (C9): once it passed, the scope is
    /// cancelled with [`CancelReason::Deadline`]. A deadline already passed cancels it now;
    /// [`Deadline::NEVER`] removes the deadline. Setting it again moves the timer (it re-arms).
    ///
    /// A closed or already cancelled scope records the deadline and arms nothing: a cancelled
    /// scope stays cancelled (the first reason wins), and a closed one has nothing left to
    /// cancel. Safe from any thread; from a thread that is not one of `handle`'s lanes, the
    /// lanes are told to look at their timers again.
    ///
    /// An associated function (`ScopeNode::set_deadline(&scope, ..)`): the timer's waker takes
    /// a reference to the node.
    pub fn set_deadline(this: &Arc<Self>, handle: &Handle, deadline: Deadline) {
        let mut state = this.lock_state();
        state.deadline = deadline;
        let idle = state.closed || this.cancel_called().is_some() || deadline == Deadline::NEVER;
        let due = !idle && handle.now() >= deadline;
        if idle || due {
            let timer = this.disarm_deadline(&mut state);
            drop(state);
            // A reference to this node, which `this` keeps alive: never the last one.
            drop(timer);
            if due {
                // Re-checked under the cancel's own lock: a close, or another deadline set in
                // the gap since the lock above, wins over this one.
                this.cancel_if(CancelReason::Deadline, |state| {
                    !state.closed && state.deadline == deadline
                });
            }
            return;
        }
        let stale = match &state.deadline_on {
            // Linked on another runtime's wheel: moved to this one.
            Some(armed_on) if !armed_on.is(handle) => this.disarm_deadline(&mut state),
            _ => None,
        };
        let waker = DeadlineWaker::waker(Arc::clone(this));
        let entry = NonNull::from(&this.deadline_timer);
        // SAFETY: the entry lives in this node, which never moves (it is in an `Arc`) and
        // outlives the link: the waker linked with it holds a reference to the node until the
        // wheel fires or disarms it. It is linked in `handle`'s wheel or in none (another
        // runtime's was disarmed above), and every arm and disarm runs under the scope's lock,
        // held here.
        let armed = unsafe { handle.shared.timers.arm(entry, deadline, Some(waker)) };
        // A shut-down wheel (the runtime is dropped) linked nothing: no timer, no deadline_on.
        let (linked, replaced) = match armed {
            Armed::Linked(replaced) => (true, replaced),
            Armed::Closed(waker) => (false, waker),
            Armed::Unlinked => (false, None),
        };
        state.deadline_on = linked.then(|| handle.downgrade());
        drop(state);
        drop(stale);
        drop(replaced);
        if !linked {
            return;
        }
        if with_current_handle(|current| current.same_runtime(handle)) != Some(true) {
            // The lane may be parked past the new deadline: let it look again.
            handle.shared.notify_all_lanes();
        }
    }

    /// The deadline timer fired: cancels the scope if its deadline really passed. A deadline
    /// moved later while the fired entry was on its way here was linked again by that move,
    /// and fires later; one removed, or a scope closed meanwhile, ends here. The check and the
    /// cancel are one step under the scope's lock, so none of those can slip in between.
    fn deadline_fired(&self) {
        self.cancel_if(CancelReason::Deadline, |state| {
            !state.closed
                && state
                    .deadline_on
                    .as_ref()
                    .and_then(WeakHandle::upgrade)
                    .is_some_and(|handle| handle.now() >= state.deadline)
        });
    }
}

/// The waker of a scope's deadline timer: a counted reference to the node, whose wake
/// cancels the scope ([`ScopeNode::deadline_fired`]). No allocation: the waker's data is the
/// node's `Arc`.
struct DeadlineWaker<E>(PhantomData<E>);

impl<E: Send + 'static> DeadlineWaker<E> {
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::clone, Self::wake, Self::wake_by_ref, Self::drop);

    /// A waker that owns `node`'s reference.
    fn waker(node: Arc<ScopeNode<E>>) -> Waker {
        let data = Arc::into_raw(node).cast::<()>();
        // SAFETY: the vtable's functions treat `data` as the `Arc` it came from: cloning
        // counts a new reference, waking by value and dropping release this one, and waking by
        // reference releases nothing. A `ScopeNode<E>` is `Send + Sync`, so the waker may go to
        // any thread.
        unsafe { Waker::from_raw(RawWaker::new(data, &Self::VTABLE)) }
    }

    /// # Safety
    /// `data` came from [`DeadlineWaker::waker`] and its reference is still counted.
    unsafe fn clone(data: *const ()) -> RawWaker {
        // SAFETY: the caller's contract; the new reference is the clone's.
        unsafe { Arc::increment_strong_count(data.cast::<ScopeNode<E>>()) };
        RawWaker::new(data, &Self::VTABLE)
    }

    /// # Safety
    /// As for `clone`; the reference is given up.
    unsafe fn wake(data: *const ()) {
        // SAFETY: the caller's contract. The reference goes when `node` drops, after the wake.
        let node = unsafe { Arc::from_raw(data.cast::<ScopeNode<E>>()) };
        node.deadline_fired();
    }

    /// # Safety
    /// As for `clone`; the reference is kept.
    unsafe fn wake_by_ref(data: *const ()) {
        // SAFETY: the caller's contract; `ManuallyDrop` keeps the reference counted.
        let node = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<ScopeNode<E>>()) });
        node.deadline_fired();
    }

    /// # Safety
    /// As for `clone`; the reference is given up.
    unsafe fn drop(data: *const ()) {
        // SAFETY: the caller's contract.
        drop(unsafe { Arc::from_raw(data.cast::<ScopeNode<E>>()) });
    }
}
