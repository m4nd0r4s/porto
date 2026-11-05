//! [`JoinWaiters`]: the intrusive list of whoever waits for a task to finish, in one word
//! (`11-task-model-and-trap-protocol.md` §2, `join`).
//!
//! The word is the head of a doubly linked list of [`WaitNode`]s plus two bits: LOCKED, a spin
//! lock held only for a few pointer writes, and CLOSED, set once by [`JoinWaiters::close`]
//! when the task has finished. A node follows the wait queue's protocol: `IDLE -> WAITING`
//! (the owner links it, under the lock), `WAITING -> WOKEN` (`close` unlinks it and takes its
//! waker, under the lock; that store is its last access to the node), `WOKEN -> IDLE` (the
//! owner observes it). Whoever finishes the task stores its outcome before `close`, and a
//! waiter reads it only after its wait returned `Ready`, which acquires the close.
//!
//! A task waits on one thing at a time, so a task waiting for another one can use the same
//! embedded node it uses for every other wait.

use std::fmt;
use std::hint;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

use super::primitives::{AtomicPtr, spin};
use super::wait_queue::{IDLE, WAITING, WOKEN, WaitNode};

/// The spin lock bit.
const LOCKED: usize = 1;
/// Set by [`JoinWaiters::close`]: every later wait is ready at once.
const CLOSED: usize = 2;
/// The bits that are not the head pointer (nodes are at least 8-byte aligned).
const FLAGS: usize = LOCKED | CLOSED;

const _: () = assert!(align_of::<WaitNode>() > FLAGS);

/// The waiters of one task: an intrusive list of [`WaitNode`]s, closed when the task finishes.
pub struct JoinWaiters {
    /// The first node, tagged with the flag bits in its low (alignment) bits.
    word: AtomicPtr<WaitNode>,
}

impl JoinWaiters {
    /// An open list with no waiters.
    #[cfg(not(all(loom, test)))]
    pub const fn new() -> Self {
        Self {
            word: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// An open list with no waiters.
    #[cfg(all(loom, test))]
    pub fn new() -> Self {
        Self {
            word: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Whether [`JoinWaiters::close`] ran: what the waiters wait for has happened.
    pub fn is_closed(&self) -> bool {
        self.word.load(Ordering::Acquire).addr() & CLOSED != 0
    }

    /// `Ready` once the list is closed; otherwise links `node` (or updates its waker) to be
    /// woken by [`JoinWaiters::close`], and returns `Pending`. A `Ready` node is idle again.
    ///
    /// # Safety
    /// The same as [`WaitQueue::poll_wait`](super::WaitQueue::poll_wait), with this list in
    /// place of the queue: calls with one node are not concurrent; from `Pending` until `Ready`
    /// or [`JoinWaiters::cancel`] the node stays in place, is used with this list only, and
    /// this list outlives that period.
    pub unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        let node = node.get_ref();
        match node.state.load(Ordering::Acquire) {
            WOKEN => {
                // `close`'s store was its last access; the node is ours again.
                node.state.store(IDLE, Ordering::Relaxed);
                Poll::Ready(())
            }
            IDLE => {
                if self.is_closed() {
                    return Poll::Ready(());
                }
                let waker = cx.waker().clone();
                let locked = self.lock();
                if locked.head.addr() & CLOSED != 0 {
                    drop(locked);
                    return Poll::Ready(());
                }
                // SAFETY: the node is idle, so in no list; it stays in place and with this
                // list until it leaves it (the caller's contract), and we hold the lock.
                unsafe { locked.push_front(node, waker) };
                Poll::Pending
            }
            _ => {
                let mut stale = None;
                let locked = self.lock();
                if node.state.load(Ordering::Acquire) == WOKEN {
                    drop(locked);
                    node.state.store(IDLE, Ordering::Relaxed);
                    return Poll::Ready(());
                }
                node.links.with_mut(|links| {
                    // SAFETY: the node is in this list and we hold its lock.
                    let links = unsafe { &mut *links };
                    if !links
                        .waker
                        .as_ref()
                        .is_some_and(|w| w.will_wake(cx.waker()))
                    {
                        stale = links.waker.replace(cx.waker().clone());
                    }
                });
                drop(locked);
                // Dropped after the unlock: dropping a waker can run arbitrary code.
                drop(stale);
                Poll::Pending
            }
        }
    }

    /// Stops `node`'s wait: unlinks it if it is linked, forgets an unobserved wake. The node is
    /// idle afterwards.
    ///
    /// # Safety
    /// As for [`JoinWaiters::poll_wait`]; if the node is pending, it is pending on this list.
    pub unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        let node = node.get_ref();
        // Only the owner (us) moves a node out of IDLE.
        if node.state.load(Ordering::Acquire) == IDLE {
            return;
        }
        let mut locked = self.lock();
        let stale = if node.state.load(Ordering::Acquire) == WAITING {
            // SAFETY: the node is in this list and we hold the lock.
            unsafe { locked.unlink(node) }
        } else {
            None
        };
        node.state.store(IDLE, Ordering::Relaxed);
        drop(locked);
        drop(stale);
    }

    /// Closes the list and wakes every waiter: the task finished, and whatever it leaves for
    /// its joiners was stored before this call. Later waits are ready at once. Returns how many
    /// waiters were woken.
    pub fn close(&self) -> usize {
        let mut locked = self.lock();
        let mut node = untagged(locked.head).cast_const();
        locked.head = ptr::without_provenance_mut(CLOSED);
        let mut wakers: Vec<Waker> = Vec::new();
        while !node.is_null() {
            // SAFETY: a linked node stays alive and in place until it leaves the list, which
            // needs the lock we hold.
            let current = unsafe { &*node };
            current.links.with_mut(|links| {
                // SAFETY: as above.
                let links = unsafe { &mut *links };
                node = links.next;
                links.prev = ptr::null();
                links.next = ptr::null();
                wakers.extend(links.waker.take());
            });
            // The last access: the owner may free the node once it sees this.
            current.state.store(WOKEN, Ordering::Release);
        }
        drop(locked);
        let woken = wakers.len();
        for waker in wakers {
            waker.wake();
        }
        woken
    }

    fn lock(&self) -> Locked<'_> {
        let mut spins = 0_u32;
        let mut current = self.word.load(Ordering::Relaxed);
        loop {
            if current.addr() & LOCKED == 0 {
                match self.word.compare_exchange_weak(
                    current,
                    current.map_addr(|addr| addr | LOCKED),
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        return Locked {
                            list: self,
                            head: current,
                        };
                    }
                    Err(actual) => {
                        current = actual;
                        continue;
                    }
                }
            }
            // Held for a few pointer writes: spin briefly, then give the holder the CPU.
            if spins < 16 && !cfg!(all(loom, test)) {
                hint::spin_loop();
                spins += 1;
            } else {
                // A step past the spin-hint range: yield to the OS scheduler.
                spin(u32::MAX);
            }
            current = self.word.load(Ordering::Relaxed);
        }
    }
}

impl Default for JoinWaiters {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for JoinWaiters {
    fn drop(&mut self) {
        // Waiters keep whatever owns the list alive, so none can be left.
        let word = self.word.load(Ordering::Relaxed);
        debug_assert!(
            untagged(word).is_null(),
            "JoinWaiters dropped with linked waiters"
        );
    }
}

impl fmt::Debug for JoinWaiters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinWaiters")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// The head pointer of a tagged word.
fn untagged(word: *mut WaitNode) -> *mut WaitNode {
    word.map_addr(|addr| addr & !FLAGS)
}

/// The list, locked: `head` is the word stored back on unlock (without LOCKED).
struct Locked<'a> {
    list: &'a JoinWaiters,
    head: *mut WaitNode,
}

impl Locked<'_> {
    /// Links `node` at the front, with `waker`, and unlocks.
    ///
    /// # Safety
    /// `node` is idle (in no list), and stays in place until it leaves this one.
    unsafe fn push_front(mut self, node: &WaitNode, waker: Waker) {
        let first = untagged(self.head).cast_const();
        node.links.with_mut(|links| {
            // SAFETY: the node is in no list, so nothing else touches its links.
            let links = unsafe { &mut *links };
            links.prev = ptr::null();
            links.next = first;
            links.waker = Some(waker);
        });
        let node_ptr = ptr::from_ref(node);
        if !first.is_null() {
            // SAFETY: `first` is in the list and we hold the lock.
            unsafe { &*first }
                .links
                .with_mut(|links| unsafe { (*links).prev = node_ptr });
        }
        node.state.store(WAITING, Ordering::Relaxed);
        let closed = self.head.addr() & CLOSED;
        self.head = node_ptr.cast_mut().map_addr(|addr| addr | closed);
    }

    /// Removes `node` from the list and returns its waker. Leaves its `state` to the caller.
    ///
    /// # Safety
    /// `node` is in this list.
    unsafe fn unlink(&mut self, node: &WaitNode) -> Option<Waker> {
        let (prev, next, waker) = node.links.with_mut(|links| {
            // SAFETY: the node is in this list and we hold the lock.
            let links = unsafe { &mut *links };
            let taken = (links.prev, links.next, links.waker.take());
            links.prev = ptr::null();
            links.next = ptr::null();
            taken
        });
        if prev.is_null() {
            let closed = self.head.addr() & CLOSED;
            self.head = next.cast_mut().map_addr(|addr| addr | closed);
        } else {
            // SAFETY: neighbours of a listed node are listed, and we hold the lock.
            unsafe { &*prev }
                .links
                .with_mut(|links| unsafe { (*links).next = next });
        }
        if !next.is_null() {
            // SAFETY: as above.
            unsafe { &*next }
                .links
                .with_mut(|links| unsafe { (*links).prev = prev });
        }
        waker
    }
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        // Release: publishes the list edits (and, for `close`, what the task stored before).
        self.list.word.store(self.head, Ordering::Release);
    }
}
