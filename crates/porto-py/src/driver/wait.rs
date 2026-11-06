//! A PyTask's one wait at a time (11-task-model-and-trap-protocol.md §4 step 1, §5.3 T3): set
//! it, resolve it in place, or give it up.
//!
//! The wait is stored packed in the body (`pending.rs`) and resolved where it lies, so the wait
//! node or timer it linked and the payload that tells where are never apart: whatever stops
//! the wait (it resolves, it is abandoned for a cancellation, the task is cancelled or freed)
//! unlinks and releases them together.
//!
//! T3, per kind, when a wait is given up before it resolved:
//!
//! - `Sleep`: the timer entry is unlinked (on the lane only, see [`PyTask::release_wait`]);
//! - `Notified`: the node leaves the primitive's queue, and an exclusive grant it has not
//!   observed yet is passed on;
//! - `Join`: the node leaves the target's join list;
//! - `ScopeExit`: the scope forgets the task's exit waker;
//! - `Future`, `Io`: the future is dropped, which unregisters its wakers (an operation
//!   consumes data only in the attempt that completes it);
//! - `Yield`, `YieldThen`: nothing is linked; the value is dropped.
//!
//! A `Notified`, `Join` or `ScopeExit` wait is a reference cycle while it is pending: the task
//! holds what it waits on (the payload), which holds the task's waker. If nothing ends the wait,
//! nothing ever frees either, so the lane keeps the waker too ([`lane::keep_linked_wait`]) and
//! wakes it when the run ends, which cancels the task (spec 11 §3.2).
//!
//! A cancellable wait (every kind but the yields and `ScopeExit`, which never abandons its
//! children, C5) of a task in a scope is also filed with its innermost scope
//! ([`PyTask::watch_cancel`]), so a cancel that reaches the scope wakes it, from any thread
//! (C10). That is the same cycle (the task holds its scope), so the lane keeps those wakers
//! too.

use std::ptr::{self, NonNull};
use std::task::{Context, Poll, Waker};

use porto_core::scope::ScopeNode;
use pyo3::prelude::*;

use super::lane;
use super::pending::{Kind, Payload, Pending};
use super::pytask::{PyTask, Resume, TaskOutcome, joined_cancelled, shut_down_error};

/// Who keeps a task's waker for its current wait, besides what it waits on: bits of
/// `PyTask::wait_links`, cleared when the wait ends.
pub(super) struct WaitLinks;

impl WaitLinks {
    /// The lane ([`lane::keep_linked_wait`]).
    const LANE: u8 = 1;
    /// The task's innermost scope ([`porto_core::scope::ScopeNode::register_waiter`]).
    const SCOPE: u8 = 2;
}

impl PyTask {
    /// Whether the task waits for something.
    pub(super) fn is_waiting(&self) -> bool {
        self.pending_kind != Kind::None
    }

    /// Starts waiting for `pending`. The task waits for nothing yet.
    pub(super) fn set_pending(&mut self, pending: Pending) {
        debug_assert!(!self.is_waiting(), "a task waits for one thing at a time");
        debug_assert!(self.wait_links == 0, "the last wait left its waker filed");
        if let Pending::Sleep(deadline) = pending {
            self.timer().reset(deadline);
        }
        let (kind, payload) = pending.pack();
        self.pending_kind = kind;
        self.pending_payload = payload;
    }

    /// Resolves the wait: `Ready` with what to resume the coroutine with (the wait is over,
    /// nothing is linked any more), or `Pending` with `cx`'s waker registered. Waiting for
    /// nothing is ready with `Resume::None`.
    ///
    /// MUST be called from the task's poll, on its lane, with `cx` its own context.
    #[inline]
    pub(super) fn poll_pending(&mut self, py: Python<'_>, cx: &mut Context<'_>) -> Poll<Resume> {
        match self.pending_kind {
            Kind::None => Poll::Ready(Resume::None),
            // A yielded task was requeued, so the wait is over. Its payload owns nothing: the
            // hot path of a switch, kept inline.
            Kind::Yield => {
                self.pending_kind = Kind::None;
                Poll::Ready(Resume::None)
            }
            _ => self.poll_linked_wait(py, cx),
        }
    }

    /// [`PyTask::poll_pending`] for the kinds that register something.
    fn poll_linked_wait(&mut self, py: Python<'_>, cx: &mut Context<'_>) -> Poll<Resume> {
        let resume = match self.pending_kind {
            Kind::None | Kind::Yield => unreachable!("resolved inline"),
            Kind::YieldThen => match self.take_pending() {
                Pending::YieldThen(value) => return Poll::Ready(Resume::Value(value)),
                _ => unreachable!("the payload of a YieldThen is its value"),
            },
            Kind::Sleep => {
                // SAFETY: the timer is embedded in this task, which its lane polls with the
                // task's own waker (the caller's contract).
                if unsafe { self.timer().poll(cx) }.is_pending() {
                    return Poll::Pending;
                }
                // The deadline is `Copy`: nothing to drop, but a sleep in a scope was filed with
                // it (`watch_cancel`), which the end of the wait undoes (porto-szb.114).
                self.unlink_wait();
                self.pending_kind = Kind::None;
                return Poll::Ready(Resume::None);
            }
            Kind::Notified => {
                // SAFETY: `wait` is the live field of a Notified payload.
                let target = unsafe { &*self.pending_payload.wait };
                // SAFETY: the node is embedded in this pinned body and waits on this target
                // only, until the wait is resolved or released (here and `release_wait`), which
                // only the task's own poll, cancel and drop do, one at a time.
                if unsafe { target.poll_wait(self.wait_node(), cx) }.is_pending() {
                    return self.linked_pending(cx.waker());
                }
                Resume::None
            }
            Kind::Join => {
                // SAFETY: `join` is the live field of a Join payload.
                let target = unsafe { &*self.pending_payload.join }.body();
                // SAFETY: as for Notified, with the target's join list, which the payload's
                // reference keeps alive.
                let joined =
                    unsafe { PyTask::join_waiters(target).poll_wait(self.wait_node(), cx) };
                if joined.is_pending() {
                    return self.linked_pending(cx.waker());
                }
                // SAFETY: the wait returned ready, so the join list is closed and the outcome
                // published; the reference keeps the body alive.
                joined_resume(py, unsafe { PyTask::outcome(target, py) })
            }
            Kind::ScopeExit => {
                // SAFETY: `scope` is the live field of a ScopeExit payload.
                let scope = unsafe { &*self.pending_payload.scope };
                if poll_scope_exit(scope, cx.waker()).is_pending() {
                    return self.linked_pending(cx.waker());
                }
                Resume::None
            }
            Kind::Future => {
                // SAFETY: `future` is the live field of a Future payload.
                match unsafe { &mut *self.pending_payload.future }.poll(cx) {
                    Poll::Ready(resume) => resume,
                    Poll::Pending => return Poll::Pending,
                }
            }
            Kind::Io => {
                // SAFETY: `io` is the live field of an Io payload.
                match unsafe { &mut *self.pending_payload.io }.0.poll(cx) {
                    Poll::Ready(resume) => resume,
                    Poll::Pending => return Poll::Pending,
                }
            }
            Kind::AsyncioFuture => {
                unreachable!("asyncio futures are not awaited yet (the compatibility layer)")
            }
        };
        // Resolved: nothing is linked any more, only the payload is left to drop.
        drop(self.take_pending());
        Poll::Ready(resume)
    }

    /// Gives the wait up before it resolved (T3), from the task's poll on its lane: a
    /// cancellation is thrown in instead. Unlinks everything the wait registered.
    pub(super) fn abandon_wait(&mut self) {
        if self.pending_kind == Kind::Sleep {
            // SAFETY: on the task's lane, inside its runtime (the caller's contract).
            unsafe { self.timer().cancel() };
        }
        self.release_wait();
    }

    /// Gives the wait up (T3) from where the task is cancelled or freed, which may be off its
    /// lane or after its runtime is gone: everything but a sleep's timer entry is unlinked. A
    /// linked entry holds the task's waker, so the wheel unlinks it before the task can be
    /// freed, and a stale firing finds the task cancelled.
    pub(super) fn release_wait(&mut self) {
        match self.pending_kind {
            Kind::Notified => {
                // SAFETY: as in `poll_pending`.
                unsafe { (*self.pending_payload.wait).cancel(self.wait_node()) };
            }
            Kind::Join => {
                // SAFETY: as in `poll_pending`.
                let target = unsafe { &*self.pending_payload.join }.body();
                // SAFETY: as in `poll_pending`.
                unsafe { PyTask::join_waiters(target).cancel(self.wait_node()) };
            }
            Kind::ScopeExit => {
                // SAFETY: `scope` is the live field of a ScopeExit payload.
                cancel_scope_exit(unsafe { &*self.pending_payload.scope });
            }
            // Dropping the payload below undoes the rest (a future's drop unregisters it).
            _ => {}
        }
        drop(self.take_pending());
    }

    /// `Pending` for a wait whose target now holds `waker` (the task's), which the lane then
    /// keeps too, once per wait: see the module docs.
    fn linked_pending(&mut self, waker: &Waker) -> Poll<Resume> {
        self.keep_on_lane(waker);
        Poll::Pending
    }

    fn keep_on_lane(&mut self, waker: &Waker) {
        if self.wait_links & WaitLinks::LANE == 0 {
            self.wait_links |= WaitLinks::LANE;
            lane::keep_linked_wait(self.key(), waker);
        }
    }

    /// Files `waker` (the task's) with its innermost scope for the pending wait, if the wait
    /// is cancellable and the task is in a scope, so a cancel that reaches the scope wakes it
    /// (C10); once per wait. Returns whether the scope is effectively cancelled, checked after
    /// filing (the caller then gives the wait up): see `ScopeNode::register_waiter`.
    pub(super) fn watch_cancel(&mut self, waker: &Waker) -> bool {
        if self.wait_links & WaitLinks::SCOPE != 0 || !self.pending_kind.is_cancellable() {
            return false;
        }
        let key = self.key();
        let Some(scope) = self.innermost_scope() else {
            return false;
        };
        let cancelled = scope.register_waiter(key, waker);
        self.wait_links |= WaitLinks::SCOPE;
        self.keep_on_lane(waker);
        cancelled
    }

    /// The key the lane keeps the task's linked wait under: the body's address, which is the
    /// task's while it lives (the lane's waker keeps it alive while the key is in use).
    fn key(&self) -> usize {
        ptr::from_ref(self).addr()
    }

    /// Moves the payload out; the task waits for nothing afterwards, and neither its scope nor
    /// the lane keeps its waker.
    fn take_pending(&mut self) -> Pending {
        self.unlink_wait();
        let kind = std::mem::replace(&mut self.pending_kind, Kind::None);
        let payload = std::mem::replace(&mut self.pending_payload, Payload::EMPTY);
        // SAFETY: the payload was packed with `kind` (`set_pending`), and is replaced above.
        unsafe { Pending::unpack(kind, &payload) }
    }

    /// Takes the task's waker back from its scope and the lane, if the wait filed it there.
    /// Every end of a wait comes through here once, whatever ended it (resolved, abandoned,
    /// released): the links are cleared on the way, so a second call does nothing. A stale
    /// link would keep the entry filed with a scope the task may since have left, and make the
    /// next wait skip filing with its own (porto-szb.114).
    #[inline]
    fn unlink_wait(&mut self) {
        let links = std::mem::take(&mut self.wait_links);
        if links != 0 {
            self.forget_links(links);
        }
    }

    #[cold]
    fn forget_links(&self, links: u8) {
        if links & WaitLinks::SCOPE != 0
            && let Some(scope) = self.innermost_scope()
        {
            // The scope filed with is still the innermost one: scopes are entered and exited
            // only while the coroutine runs, never during a wait.
            scope.unregister_waiter(self.key());
        }
        if links & WaitLinks::LANE != 0 {
            lane::forget_linked_wait(self.key());
        }
    }

    /// Whether the body behind `target` is this task's.
    pub(super) fn is_same_task(&self, target: NonNull<PyTask>) -> bool {
        std::ptr::eq(self, target.as_ptr())
    }
}

/// What a joiner resumes with: the target's result, or its exception thrown in.
fn joined_resume(py: Python<'_>, outcome: Option<TaskOutcome>) -> Resume {
    match outcome {
        Some(TaskOutcome::Ok(value)) => Resume::Value(value),
        Some(TaskOutcome::Err(exception)) => Resume::Throw(exception),
        Some(TaskOutcome::Cancelled(Some(exception))) => {
            Resume::Throw(joined_cancelled(py, exception))
        }
        Some(TaskOutcome::Cancelled(None)) | None => {
            Resume::Throw(shut_down_error().into_value(py))
        }
    }
}

/// `Ready` once `scope` has no live children; otherwise files `waker` as its exit waiter
/// (`ScopeState::exit_waiter`), which the last child to finish wakes (porto-szb.93).
fn poll_scope_exit<E>(scope: &ScopeNode<E>, waker: &Waker) -> Poll<()> {
    let mut state = scope.lock_state();
    if state.live_children == 0 {
        let stale = state.exit_waiter.take();
        drop(state);
        // Dropped after the unlock: dropping a waker can run arbitrary code.
        drop(stale);
        return Poll::Ready(());
    }
    let stale = match &state.exit_waiter {
        Some(filed) if filed.will_wake(waker) => None,
        _ => state.exit_waiter.replace(waker.clone()),
    };
    drop(state);
    drop(stale);
    Poll::Pending
}

/// Forgets the exit waiter of `scope`: its owner stops waiting (T3).
fn cancel_scope_exit<E>(scope: &ScopeNode<E>) {
    let stale = scope.lock_state().exit_waiter.take();
    drop(stale);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;

    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_scope_exit_waits_for_the_last_child_and_forgets_its_waker_when_cancelled() {
        let scope = ScopeNode::<()>::new_root(1);
        let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        assert!(poll_scope_exit(&scope, &waker).is_ready(), "no children");

        scope.lock_state().live_children = 1;
        assert!(poll_scope_exit(&scope, &waker).is_pending());
        assert!(scope.lock_state().exit_waiter.is_some());
        // T3: a cancelled wait leaves no waker behind.
        cancel_scope_exit(&scope);
        assert!(scope.lock_state().exit_waiter.is_none());
        assert_eq!(Arc::strong_count(&wakes), 2, "the scope dropped its clone");

        assert!(poll_scope_exit(&scope, &waker).is_pending());
        let filed = {
            let mut state = scope.lock_state();
            state.live_children = 0;
            state.exit_waiter.take()
        };
        filed.expect("the waiter is filed").wake();
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert!(poll_scope_exit(&scope, &waker).is_ready());
    }
}
