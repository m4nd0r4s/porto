//! The waiters of one `ScheduledIo`: a slot per direction plus an intrusive list for the rare
//! extra waiters (several tasks waiting on the same socket).

use std::ptr::NonNull;
use std::task::Waker;

use crate::{Interest, Ready};

/// Guarded by the `ScheduledIo`'s mutex.
#[derive(Default)]
pub(crate) struct Waiters {
    /// The task waiting to read.
    pub(crate) reader: Option<Waker>,
    /// The task waiting to write.
    pub(crate) writer: Option<Waker>,
    /// Every other waiter, oldest first.
    pub(crate) list: WaiterList,
}

/// A doubly linked list of [`WaiterNode`]s that live inside pinned `Readiness` futures.
#[derive(Default)]
pub(crate) struct WaiterList {
    head: Option<NonNull<WaiterNode>>,
    tail: Option<NonNull<WaiterNode>>,
}

// SAFETY: the nodes are only read or written with the owning `ScheduledIo`'s mutex held, and a
// node is unlinked (under that mutex) before the future holding it is dropped, so moving the
// list to another thread never races with the nodes' owners.
unsafe impl Send for WaiterList {}

/// One extra waiter, embedded in a pinned `Readiness` future.
pub(crate) struct WaiterNode {
    prev: Option<NonNull<WaiterNode>>,
    next: Option<NonNull<WaiterNode>>,
    /// The waker to call; taken when the node is notified.
    pub(crate) waker: Option<Waker>,
    /// What the waiter waits for.
    pub(crate) interest: Interest,
    /// Set (and the node unlinked) when a wake matched the interest.
    pub(crate) notified: bool,
}

impl WaiterNode {
    pub(crate) const fn new(interest: Interest) -> Self {
        Self {
            prev: None,
            next: None,
            waker: None,
            interest,
            notified: false,
        }
    }
}

impl WaiterList {
    /// Appends `node`.
    ///
    /// # Safety
    ///
    /// The caller holds the owning mutex, `node` is valid and not linked into any list, and it
    /// stays at the same address until it is unlinked.
    pub(crate) unsafe fn push_back(&mut self, mut node: NonNull<WaiterNode>) {
        // SAFETY: the caller guarantees `node` is valid and that we hold the mutex.
        unsafe {
            node.as_mut().prev = self.tail;
            node.as_mut().next = None;
        }
        match self.tail {
            // SAFETY: linked nodes are valid while the mutex is held.
            Some(mut tail) => unsafe { tail.as_mut().next = Some(node) },
            None => self.head = Some(node),
        }
        self.tail = Some(node);
    }

    /// Unlinks `node`.
    ///
    /// # Safety
    ///
    /// The caller holds the owning mutex and `node` is linked into this list.
    pub(crate) unsafe fn remove(&mut self, mut node: NonNull<WaiterNode>) {
        // SAFETY: the caller guarantees `node` is linked here, so it and its neighbours are
        // valid while the mutex is held.
        unsafe {
            let (prev, next) = (node.as_ref().prev, node.as_ref().next);
            match prev {
                Some(mut prev) => prev.as_mut().next = next,
                None => self.head = next,
            }
            match next {
                Some(mut next) => next.as_mut().prev = prev,
                None => self.tail = prev,
            }
            node.as_mut().prev = None;
            node.as_mut().next = None;
        }
    }

    /// Unlinks and notifies every node whose interest matches `ready`, moving its waker into
    /// `wakers`. Returns `false` when `wakers` filled up before the list was exhausted.
    pub(crate) fn notify_matching(&mut self, ready: Ready, wakers: &mut WakeList) -> bool {
        let mut cursor = self.head;
        while let Some(mut node) = cursor {
            if wakers.is_full() {
                return false;
            }
            // SAFETY: linked nodes are valid, and `&mut self` means we hold the mutex.
            let node_ref = unsafe { node.as_mut() };
            cursor = node_ref.next;
            if Ready::from_interest(node_ref.interest).intersects(ready) {
                // SAFETY: `node` is linked into this list.
                unsafe { self.remove(node) };
                // SAFETY: as above; `remove` only touched the link fields.
                let node_ref = unsafe { node.as_mut() };
                node_ref.notified = true;
                if let Some(waker) = node_ref.waker.take() {
                    wakers.push(waker);
                }
            }
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.head.is_none()
    }
}

const WAKE_LIST_CAPACITY: usize = 32;

/// Wakers collected under the mutex and called after it is released, in batches.
pub(crate) struct WakeList {
    wakers: [Option<Waker>; WAKE_LIST_CAPACITY],
    len: usize,
}

impl WakeList {
    pub(crate) fn new() -> Self {
        Self {
            wakers: std::array::from_fn(|_| None),
            len: 0,
        }
    }

    pub(crate) const fn is_full(&self) -> bool {
        self.len == WAKE_LIST_CAPACITY
    }

    pub(crate) fn push(&mut self, waker: Waker) {
        debug_assert!(!self.is_full());
        self.wakers[self.len] = Some(waker);
        self.len += 1;
    }

    /// Calls and drops every collected waker.
    pub(crate) fn wake_all(&mut self) {
        for waker in &mut self.wakers[..self.len] {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }
        self.len = 0;
    }
}
