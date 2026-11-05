//! The link from a task to the scope it was spawned into: [`TaskScope`] and [`TaskEnd`].
//!
//! The task header is type-erased while a scope is a `ScopeNode<E>`, so the header holds a
//! thin pointer to the node and the node starts (`repr(C)`) with a `&'static` vtable for its
//! `E`: the header stays one word, and a task without a scope pays one null check.

use std::any::Any;
use std::fmt;
use std::mem;
use std::ptr::NonNull;

use super::node::{CancelReason, ScopeNode};
use super::sync::Arc;

/// How a task ended, as the scope it was spawned into hears it.
pub enum TaskEnd {
    /// It ran to completion: its output, or its body's outcome, is stored.
    Completed,
    /// It completed as cancelled: [`crate::task::JoinHandle::abort`], or its run-queue entry was
    /// dropped (shutdown).
    Cancelled,
    /// It failed (C4). The payload is the failure when the scope can own it: what
    /// [`crate::task::TaskBody::take_failure`] returned. A panic carries none: its payload goes
    /// to the join handle, or is resumed on the lane.
    Failed(Option<Box<dyn Any + Send>>),
    /// It was freed without completing: it was idle when its last reference (waker, join handle
    /// or task reference) went, so it can never run again.
    Freed,
}

impl fmt::Debug for TaskEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed => f.write_str("Completed"),
            Self::Cancelled => f.write_str("Cancelled"),
            Self::Failed(None) => f.write_str("Failed(None)"),
            Self::Failed(Some(_)) => f.write_str("Failed(Some(..))"),
            Self::Freed => f.write_str("Freed"),
        }
    }
}

/// The scope-type-specific operation a task reaches through its erased [`TaskScope`].
pub(super) struct ChildVTable {
    /// Tells the scope one of its children ended and releases the child's reference to it.
    child_ended: unsafe fn(NonNull<ChildVTableRef>, TaskEnd),
}

/// The first field of every `ScopeNode<E>`: what a [`TaskScope`] points at.
pub(super) type ChildVTableRef = &'static ChildVTable;

/// The vtable of `ScopeNode<E>`. A constant expression of fn pointers: promoted to a
/// `'static`, one per `E`.
pub(super) fn child_vtable<E: Send + 'static>() -> ChildVTableRef {
    &ChildVTable {
        child_ended: child_ended::<E>,
    }
}

/// # Safety
/// `node` came from `Arc::into_raw` of an `Arc<ScopeNode<E>>` (in [`TaskScope::attach`]), and
/// the caller gives up that reference.
unsafe fn child_ended<E: Send + 'static>(node: NonNull<ChildVTableRef>, end: TaskEnd) {
    // SAFETY: the caller's contract. The reference is released when `scope` drops, unwinding
    // included.
    let scope = unsafe { Arc::from_raw(node.cast::<ScopeNode<E>>().as_ptr().cast_const()) };
    scope.child_ended(end);
}

/// An owned, type-erased reference to the scope a task was spawned into, counted in the scope's
/// `live_children` (`11-task-model-and-trap-protocol.md` §1, the header's `scope`).
///
/// Ending it ([`TaskScope::end`]) tells the scope how the task ended and releases the
/// reference; dropping it ends it as [`TaskEnd::Freed`]. Either happens exactly once, which is
/// how every task end (completed, cancelled, panicked or freed while idle) reaches the scope
/// once.
pub struct TaskScope {
    node: NonNull<ChildVTableRef>,
}

// SAFETY: the pointer is a counted reference to a `ScopeNode<E>` with `E: Send` (see
// `attach`), which is `Send + Sync`; the scope's lock orders ends from different threads.
unsafe impl Send for TaskScope {}
// SAFETY: `&TaskScope` exposes nothing.
unsafe impl Sync for TaskScope {}

impl TaskScope {
    /// Counts a new child task in `scope` (`live_children`) and returns the reference the task
    /// holds until it ends.
    ///
    /// # Panics
    /// If the scope already has `u32::MAX` live children.
    pub fn attach<E: Send + 'static>(scope: &Arc<ScopeNode<E>>) -> Self {
        {
            let mut state = scope.lock_state();
            state.live_children = state.live_children.checked_add(1).expect(
                "porto: a scope has 4 billion live tasks; wait for some to finish before \
                 spawning more into it",
            );
        }
        let ptr = Arc::into_raw(Arc::clone(scope));
        Self {
            // SAFETY: `Arc::into_raw` never returns null. A `ScopeNode` is `repr(C)` with its
            // vtable reference first, so the node's address is the field's.
            node: unsafe { NonNull::new_unchecked(ptr.cast_mut().cast::<ChildVTableRef>()) },
        }
    }

    /// Tells the scope how its child ended and releases the child's reference to it.
    pub fn end(self, end: TaskEnd) {
        let node = self.node;
        mem::forget(self);
        // SAFETY: we own the reference `attach` took, and give it up here, once.
        unsafe { Self::notify(node, end) }
    }

    /// # Safety
    /// `node` is a reference taken by `attach`, given up by the caller.
    unsafe fn notify(node: NonNull<ChildVTableRef>, end: TaskEnd) {
        // SAFETY: the node is alive (we hold a reference) and starts with its vtable reference,
        // which is never written after construction.
        let vtable = unsafe { *node.as_ptr() };
        // SAFETY: the vtable is the node's own, and the caller gives up the reference.
        unsafe { (vtable.child_ended)(node, end) }
    }
}

impl Drop for TaskScope {
    fn drop(&mut self) {
        // SAFETY: `end` forgets `self`, so this runs only for a reference nobody ended.
        unsafe { Self::notify(self.node, TaskEnd::Freed) }
    }
}

impl fmt::Debug for TaskScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskScope").finish_non_exhaustive()
    }
}

impl<E: Send + 'static> ScopeNode<E> {
    /// One child ended: count it out, record a failure (C4, which also cancels the scope), and
    /// wake the owner waiting in `ScopeExit` once the last child is gone.
    fn child_ended(&self, end: TaskEnd) {
        let failed = matches!(end, TaskEnd::Failed(_));
        // A payload that is not an `E` is dropped here, outside the lock.
        let error = match end {
            TaskEnd::Failed(Some(payload)) => payload.downcast::<E>().ok().map(|error| *error),
            _ => None,
        };
        // Cancelled before the child is counted out: `cancel` takes the same lock, so an owner
        // that sees the last child gone also sees the cancellation (C4).
        if failed {
            self.cancel(CancelReason::ChildFailed);
        }
        let mut state = self.lock_state();
        debug_assert!(
            state.live_children > 0,
            "a child ended that the scope never counted"
        );
        state.live_children = state.live_children.saturating_sub(1);
        if let Some(error) = error {
            state.errors.push(error);
        }
        let waiter = if state.live_children == 0 {
            state.exit_waiter.take()
        } else {
            None
        };
        drop(state);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}
