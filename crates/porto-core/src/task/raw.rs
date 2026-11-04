//! Type-erased task pointers and the per-type vtable.

use std::ptr::NonNull;

use super::header::Header;

/// Operations that depend on the concrete future and scheduler types.
///
/// Every function takes a pointer to the task's [`Header`], which is the first field of the
/// `repr(C)` allocation, so the implementation casts it back to the concrete cell.
pub struct Vtable {
    /// Polls the task once. Consumes the reference its run queue owned.
    pub(crate) poll: unsafe fn(NonNull<Header>),
    /// Hands the task to its scheduler. Consumes one reference, which the queue now owns.
    pub(crate) schedule: unsafe fn(NonNull<Header>),
    /// Drops the future (if still present) and the scheduler handle, and frees the memory.
    pub(crate) dealloc: unsafe fn(NonNull<Header>),
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
