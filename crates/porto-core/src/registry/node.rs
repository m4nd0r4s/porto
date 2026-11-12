//! The registry's intrusive node, embedded in every task header.

use std::ptr;
use std::sync::atomic::Ordering;

use crate::sync::primitives::AtomicPtr;
use crate::task::Header;

/// Marks a node that is on no list. `prev` is null for the first node of a list, so "linked"
/// needs a value no header pointer has.
const UNLINKED: *mut Header = ptr::dangling_mut::<Header>();

/// A node in the intrusive, doubly linked list of live tasks (`crate::registry`).
///
/// 16 bytes, part of the header's 64-byte budget. The links are atomics so the one question
/// another thread asks of a node (is it linked: the last reference is dropped off the lane)
/// is well defined; the lane that owns the list reads and writes them with relaxed loads and
/// stores, which cost what plain ones do.
#[repr(C)]
pub struct RegistryNode {
    prev: AtomicPtr<Header>,
    next: AtomicPtr<Header>,
}

impl RegistryNode {
    pub(crate) fn unlinked() -> Self {
        Self {
            prev: AtomicPtr::new(UNLINKED),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Whether the node is on a list, as seen by a thread that does not own the list: the
    /// acquire pairs with the release of [`RegistryNode::set_unlinked`], so a task seen
    /// unlinked is not touched by its lane any more.
    pub(crate) fn is_linked(&self) -> bool {
        self.prev.load(Ordering::Acquire) != UNLINKED
    }

    /// [`RegistryNode::is_linked`] for the thread that owns the list (or holds its lock),
    /// which wrote the links itself.
    #[inline]
    pub(crate) fn is_linked_relaxed(&self) -> bool {
        self.prev.load(Ordering::Relaxed) != UNLINKED
    }

    /// The neighbours: `(prev, next)`, null at the ends.
    #[inline]
    pub(crate) fn neighbours(&self) -> (*mut Header, *mut Header) {
        (
            self.prev.load(Ordering::Relaxed),
            self.next.load(Ordering::Relaxed),
        )
    }

    /// Marks the node linked at the head of a list, in front of `next`.
    #[inline]
    pub(crate) fn set_linked(&self, next: *mut Header) {
        self.next.store(next, Ordering::Relaxed);
        self.prev.store(ptr::null_mut(), Ordering::Release);
    }

    #[inline]
    pub(crate) fn set_prev(&self, prev: *mut Header) {
        self.prev.store(prev, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn set_next(&self, next: *mut Header) {
        self.next.store(next, Ordering::Relaxed);
    }

    /// Marks the node as on no list: the last write the list makes to it.
    #[inline]
    pub(crate) fn set_unlinked(&self) {
        self.next.store(ptr::null_mut(), Ordering::Relaxed);
        self.prev.store(UNLINKED, Ordering::Release);
    }
}
