//! Type-erased task pointers and the per-type vtable.

use std::ptr::NonNull;
use std::task::Waker;

use super::header::Header;

/// Operations that depend on the concrete task type (`11-task-model-and-trap-protocol.md` §1).
///
/// Every function takes a pointer to the task's [`Header`], which is the first field of the
/// `repr(C)` allocation, so the implementation casts it back to the concrete cell. One static
/// vtable exists per task type; the header points at it.
pub struct TaskVTable {
    /// Polls the task once. Consumes the reference its run queue owned.
    pub(crate) poll: unsafe fn(NonNull<Header>),
    /// Drops the task's future (or coroutine) in place, keeping the allocation. The caller
    /// holds RUNNING, which grants exclusive access to the body.
    #[allow(
        dead_code,
        reason = "type-erased callers (PyTask cancellation, porto-szb.4) arrive later"
    )]
    pub(crate) drop_future: unsafe fn(NonNull<Header>),
    /// Frees the allocation, dropping whatever the body still owns (future, scheduler handle).
    pub(crate) dealloc: unsafe fn(NonNull<Header>),
    /// Hands the task to its scheduler. Consumes one reference, which the queue now owns.
    pub(crate) schedule: unsafe fn(NonNull<Header>),
    /// For the join handle: writes `Poll::Ready(output)` through the `*mut ()` (a
    /// `*mut Poll<Result<T, JoinError>>`) if the task is complete, else registers the waker.
    pub(crate) try_read_output: unsafe fn(NonNull<Header>, *mut (), &Waker),
    /// For the join handle: drops what it owns (output, waker) and its reference.
    pub(crate) drop_join_handle: unsafe fn(NonNull<Header>),
    /// The concrete future's type name, for introspection alongside the task id.
    pub(crate) type_name: fn() -> &'static str,
}

/// A non-owning pointer to a task. Copying it does not touch the refcount.
#[derive(Clone, Copy)]
pub(crate) struct RawTask {
    ptr: NonNull<Header>,
}

impl RawTask {
    /// # Safety
    /// `ptr` must point to the header of a live task allocation.
    pub(crate) unsafe fn from_raw(ptr: NonNull<Header>) -> Self {
        Self { ptr }
    }

    pub(crate) fn as_ptr(self) -> NonNull<Header> {
        self.ptr
    }

    pub(crate) fn header(&self) -> &Header {
        // SAFETY: `from_raw`'s contract: the allocation is live while a reference is held,
        // and every caller of `header` holds one.
        unsafe { self.ptr.as_ref() }
    }

    /// # Safety
    /// The caller transfers one reference (the run queue's) to the poll.
    pub(crate) unsafe fn poll(self) {
        // SAFETY: the vtable belongs to this allocation and the caller hands over a reference.
        unsafe { (self.header().vtable.poll)(self.ptr) }
    }

    /// # Safety
    /// The caller holds RUNNING (exclusive access to the body) and a reference.
    #[allow(
        dead_code,
        reason = "type-erased callers (PyTask cancellation, porto-szb.4) arrive later"
    )]
    pub(crate) unsafe fn drop_future(self) {
        // SAFETY: the vtable belongs to this allocation; RUNNING excludes other body access.
        unsafe { (self.header().vtable.drop_future)(self.ptr) }
    }

    /// # Safety
    /// The caller transfers one reference, and the task must be in the SCHEDULED state.
    pub(crate) unsafe fn schedule(self) {
        // SAFETY: the vtable belongs to this allocation and the caller hands over a reference.
        unsafe { (self.header().vtable.schedule)(self.ptr) }
    }

    /// # Safety
    /// The refcount must have just dropped to zero: nothing else can reach the task.
    pub(crate) unsafe fn dealloc(self) {
        // SAFETY: the vtable belongs to this allocation and no reference remains.
        unsafe { (self.header().vtable.dealloc)(self.ptr) }
    }

    /// Drops one reference and frees the task if it was the last.
    ///
    /// # Safety
    /// The caller must own the reference it drops.
    pub(crate) unsafe fn drop_reference(self) {
        if self.header().state.ref_dec() {
            // SAFETY: the decrement released the last reference.
            unsafe { self.dealloc() }
        }
    }
}
