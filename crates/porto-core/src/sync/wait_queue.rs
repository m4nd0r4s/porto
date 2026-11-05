//! The FIFO wait queue shared by every primitive: intrusive nodes, cancel-safe grants.
//!
//! A [`WaitQueue<S>`] is a mutex around the primitive's state `S` and a doubly linked list of
//! [`WaitNode`]s. Nodes are intrusive: they live in the waiting future (Rust) or in the task
//! (`PyTask.wait_node`), so waiting allocates nothing.
//!
//! A node's `state` moves `IDLE -> WAITING` (the owner enqueues it, under the lock), then
//! `WAITING -> GRANTED` or `WAITING -> WOKEN` (a granter unlinks it and takes its waker, under
//! the lock), then back to `IDLE` (the owner observes the grant, or cancels). The granter's
//! store is its last access to the node, so the owner may observe it without the lock and free
//! the node at once. `GRANTED` is an exclusive grant ([`Locked::grant_front`]), which a
//! cancelled owner passes on; `WOKEN` is a shared one ([`Locked::grant_all`]), which it drops.
//! Everything else in a node (links, waker) is only touched under the queue lock.

use std::fmt;
use std::future::Future;
use std::marker::PhantomPinned;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

use super::primitives::{AtomicU8, Mutex, MutexGuard, UnsafeCell};

/// Not in a queue, no grant pending.
const IDLE: u8 = 0;
/// Linked into a queue, waiting for a grant.
const WAITING: u8 = 1;
/// Unlinked by [`Locked::grant_front`]; the owner has not observed the grant yet.
const GRANTED: u8 = 2;
/// Unlinked by [`Locked::grant_all`]; the owner has not observed the wake yet.
const WOKEN: u8 = 3;

/// Whether `state` is a grant the owner has not observed yet.
fn is_granted(state: u8) -> bool {
    state == GRANTED || state == WOKEN
}

/// What a primitive does with a grant its waiter can no longer take.
pub trait WaitState: Sized {
    /// Called, with the queue locked, when a waiter that was granted is cancelled before it
    /// observed the grant (the grant and the cancel raced). `token` is the waiter's
    /// [`WaitNode::token`].
    ///
    /// The implementation MUST pass the grant on, typically to the next waiter
    /// ([`Locked::grant_front`]), or return it to the state (unlock, give the permits back).
    /// Grants made by [`Locked::grant_all`] are shared (an event or `notify_all` wakes
    /// everyone) and are never passed on: this is not called for them.
    fn pass_grant(waiters: &mut Locked<'_, Self>, token: usize);
}

/// A waiter: one entry of a [`WaitQueue`], embedded in whatever waits.
///
/// The node must not move while it is queued, hence `!Unpin`. Drop it only when it is not
/// queued: dropping a queued node aborts the process (its queue would keep a dangling pointer).
/// [`Wait`] upholds this on its own; code using the low-level [`WaitQueue::poll_wait`] calls
/// [`WaitQueue::cancel`] first.
pub struct WaitNode {
    state: AtomicU8,
    /// Set by the owner at creation; the primitive's meaning (permits wanted, reader/writer).
    token: usize,
    /// Guarded by the lock of the queue the node is in.
    links: UnsafeCell<Links>,
    _pinned: PhantomPinned,
}

struct Links {
    prev: *const WaitNode,
    next: *const WaitNode,
    waker: Option<Waker>,
}

// SAFETY: `links` is only accessed under the queue lock and `state` is atomic. The owner-side
// calls (`poll_wait`, `cancel`) are serialized by their safety contract.
unsafe impl Send for WaitNode {}
// SAFETY: as above; a shared reference gives no access to anything but the atomic `state`.
unsafe impl Sync for WaitNode {}

impl WaitNode {
    /// A node that is not in any queue. `token` is what [`Locked::front_token`] and
    /// [`WaitState::pass_grant`] report for it.
    pub fn new(token: usize) -> Self {
        Self {
            state: AtomicU8::new(IDLE),
            token,
            links: UnsafeCell::new(Links {
                prev: ptr::null(),
                next: ptr::null(),
                waker: None,
            }),
            _pinned: PhantomPinned,
        }
    }

    /// The token this node was created with.
    pub fn token(&self) -> usize {
        self.token
    }

    /// Whether the node is in a queue or holds a grant it has not observed yet.
    pub fn is_pending(&self) -> bool {
        self.state.load(Ordering::Acquire) != IDLE
    }
}

impl Drop for WaitNode {
    fn drop(&mut self) {
        if self.state.load(Ordering::Acquire) == WAITING {
            // The queue still links this node; carrying on would be a use after free.
            eprintln!(
                "porto: a WaitNode was dropped while still queued; call WaitQueue::cancel \
                 before dropping it (or use the Wait future, which does). Aborting."
            );
            std::process::abort();
        }
    }
}

impl fmt::Debug for WaitNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.state.load(Ordering::Acquire) {
            IDLE => "idle",
            WAITING => "waiting",
            GRANTED => "granted",
            _ => "woken",
        };
        f.debug_struct("WaitNode")
            .field("state", &state)
            .field("token", &self.token)
            .finish()
    }
}

/// The list and the primitive's state, behind the queue lock.
struct Inner<S> {
    state: S,
    head: *const WaitNode,
    tail: *const WaitNode,
    len: usize,
}

// SAFETY: the raw pointers are to nodes that are `Send`, and are only followed under the lock.
unsafe impl<S: Send> Send for Inner<S> {}

/// A FIFO queue of waiters plus the state `S` of the primitive they wait on.
///
/// The state and the queue share one lock ([`WaitQueue::lock`]), so a primitive decides "free
/// or wait" and "grant to whom" atomically. Wakers are woken after the lock is released.
pub struct WaitQueue<S> {
    inner: Mutex<Inner<S>>,
}

impl<S: WaitState> WaitQueue<S> {
    /// An empty queue around `state`.
    pub fn new(state: S) -> Self {
        Self {
            inner: Mutex::new(Inner {
                state,
                head: ptr::null(),
                tail: ptr::null(),
                len: 0,
            }),
        }
    }

    /// Locks the state and the queue. Wakes queued by [`Locked::grant_front`] and
    /// [`Locked::grant_all`] run when the guard is dropped, after the unlock.
    pub fn lock(&self) -> Locked<'_, S> {
        // A panic under the lock leaves the list consistent (every list edit is panic-free),
        // so poisoning carries no information here.
        let guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        Locked {
            guard: ManuallyDrop::new(guard),
            wakes: Vec::new(),
        }
    }

    /// A future that completes once the waiter is granted, or at once if `acquire` succeeds
    /// on the first poll. Dropping it before it completes cancels the wait, passing on a grant
    /// that raced with the drop.
    ///
    /// `acquire` runs under the lock on the first poll only; it returns `true` when it took
    /// what the waiter wants without waiting. For fairness it should fail while
    /// [`Locked::is_empty`] is false.
    pub fn wait<A>(&self, token: usize, acquire: A) -> Wait<'_, S, A>
    where
        A: FnOnce(&mut Locked<'_, S>) -> bool,
    {
        Wait {
            queue: self,
            node: WaitNode::new(token),
            acquire: Some(acquire),
            done: false,
        }
    }

    /// Polls `node`'s wait on this queue: `Ready` once it is granted (or `acquire` succeeds on
    /// a node that is not queued yet), otherwise queues the node (or updates its waker) and
    /// returns `Pending`. A `Ready` node is idle again and may be reused.
    ///
    /// # Safety
    /// - Calls with the same node (`poll_wait`, `cancel`) are not concurrent.
    /// - From a `Pending` return until a `Ready` return or [`WaitQueue::cancel`], the node is
    ///   used with this queue only, and this queue outlives that period.
    /// - In that period the node stays at its address and is not freed without running
    ///   [`WaitNode`]'s `Drop` (an allocation that embeds it, such as a task body, must drop it
    ///   in place rather than free the memory raw).
    /// - The granter reads the node through shared references while the owner may hold the
    ///   embedding struct, so the owner reaches the node only through `&`/`Pin<&>`; a struct
    ///   that embeds it is `!Unpin` through it, which keeps `&mut` to that struct from
    ///   asserting exclusive access.
    /// - Cancellation requested from another thread is forwarded to the owner, which calls
    ///   [`WaitQueue::cancel`]; calling it from that other thread breaks the first rule.
    pub unsafe fn poll_wait<A>(
        &self,
        node: Pin<&WaitNode>,
        cx: &mut Context<'_>,
        acquire: A,
    ) -> Poll<()>
    where
        A: FnOnce(&mut Locked<'_, S>) -> bool,
    {
        let node = node.get_ref();
        match node.state.load(Ordering::Acquire) {
            GRANTED | WOKEN => {
                // The granter's last access was its grant store; the node is ours again.
                node.state.store(IDLE, Ordering::Relaxed);
                Poll::Ready(())
            }
            IDLE => {
                let mut locked = self.lock();
                if acquire(&mut locked) {
                    return Poll::Ready(());
                }
                // SAFETY: the node is idle, so in no list; it is pinned, and the contract keeps
                // it alive and with this queue until it leaves the list.
                unsafe { locked.push_back(node, cx.waker().clone()) };
                Poll::Pending
            }
            _ => {
                // Declared before the guard so the old waker drops after the unlock: dropping
                // a waker can run arbitrary code, which may use this queue.
                let mut stale = None;
                let _locked = self.lock();
                if is_granted(node.state.load(Ordering::Acquire)) {
                    node.state.store(IDLE, Ordering::Relaxed);
                    return Poll::Ready(());
                }
                node.links.with_mut(|links| {
                    // SAFETY: the node is in this queue's list and we hold its lock.
                    let links = unsafe { &mut *links };
                    if !links
                        .waker
                        .as_ref()
                        .is_some_and(|w| w.will_wake(cx.waker()))
                    {
                        stale = links.waker.replace(cx.waker().clone());
                    }
                });
                Poll::Pending
            }
        }
    }

    /// Cancels `node`'s wait: unlinks it if it is still queued, or passes its grant on
    /// ([`WaitState::pass_grant`]) if it was granted by [`Locked::grant_front`] but has not
    /// observed it. Drops an unobserved [`Locked::grant_all`] wake. Does nothing for an idle
    /// node. The node is idle afterwards.
    ///
    /// # Safety
    /// As for [`WaitQueue::poll_wait`]; if the node is pending, it is pending on this queue.
    pub unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        let node = node.get_ref();
        // Only the owner (us) moves a node out of `IDLE`.
        if node.state.load(Ordering::Acquire) == IDLE {
            return;
        }
        let mut stale = None;
        let mut locked = self.lock();
        match node.state.load(Ordering::Acquire) {
            WAITING => {
                // SAFETY: the node is in this queue's list and we hold its lock.
                stale = unsafe { locked.unlink(node) };
                node.state.store(IDLE, Ordering::Relaxed);
            }
            GRANTED => {
                node.state.store(IDLE, Ordering::Relaxed);
                S::pass_grant(&mut locked, node.token);
            }
            WOKEN => node.state.store(IDLE, Ordering::Relaxed),
            _ => {}
        }
        drop(locked);
        drop(stale);
    }
}

impl<S> Drop for WaitQueue<S> {
    fn drop(&mut self) {
        // `Wait` borrows the queue, so only a broken `poll_wait` contract leaves nodes here.
        let len = self
            .inner
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .len;
        debug_assert_eq!(len, 0, "WaitQueue dropped with {len} queued waiters");
    }
}

impl<S: fmt::Debug> fmt::Debug for WaitQueue<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("WaitQueue")
            .field("state", &inner.state)
            .field("waiters", &inner.len)
            .finish()
    }
}

/// A locked [`WaitQueue`]: the primitive's state (through `Deref`) plus the waiter list.
///
/// Wakers of granted waiters are collected and woken when the guard drops, after the unlock.
pub struct Locked<'a, S> {
    guard: ManuallyDrop<MutexGuard<'a, Inner<S>>>,
    wakes: Vec<Waker>,
}

impl<S> Locked<'_, S> {
    /// Whether no waiter is queued.
    pub fn is_empty(&self) -> bool {
        self.guard.len == 0
    }

    /// The number of queued waiters.
    pub fn len(&self) -> usize {
        self.guard.len
    }

    /// The token of the first queued waiter, the next one [`Locked::grant_front`] grants.
    pub fn front_token(&self) -> Option<usize> {
        // SAFETY: queued nodes stay alive while they are in the list, and we hold the lock.
        (!self.guard.head.is_null()).then(|| unsafe { (*self.guard.head).token })
    }

    /// Grants the first waiter: unlinks it and wakes it once the lock is released. Returns
    /// `false` if no waiter is queued.
    ///
    /// The grant is exclusive (a lock, a permit): if the waiter is cancelled before it observes
    /// the grant, [`WaitState::pass_grant`] passes it on.
    pub fn grant_front(&mut self) -> bool {
        self.grant_front_as(GRANTED)
    }

    /// Wakes every queued waiter, in order. Returns how many there were.
    ///
    /// The wakes are shared (an event, `notify_all`): a waiter cancelled before it observes its
    /// wake drops it, and [`WaitState::pass_grant`] is not called.
    pub fn grant_all(&mut self) -> usize {
        let mut granted = 0;
        while self.grant_front_as(WOKEN) {
            granted += 1;
        }
        granted
    }

    /// Unlinks the first waiter, queues its wake and leaves it in `grant` (`GRANTED`, `WOKEN`).
    fn grant_front_as(&mut self, grant: u8) -> bool {
        let head = self.guard.head;
        if head.is_null() {
            return false;
        }
        // SAFETY: `head` is in the list and we hold the lock.
        let node = unsafe { &*head };
        // SAFETY: as above.
        if let Some(waker) = unsafe { self.unlink(node) } {
            self.wakes.push(waker);
        }
        // The last access: from here the owner may observe the grant and free the node.
        node.state.store(grant, Ordering::Release);
        true
    }

    /// Appends `node` with `waker` and marks it waiting.
    ///
    /// # Safety
    /// `node` is in no list, is pinned, and stays alive until it is unlinked.
    unsafe fn push_back(&mut self, node: &WaitNode, waker: Waker) {
        let inner = &mut **self.guard;
        let tail = inner.tail;
        node.links.with_mut(|links| {
            // SAFETY: the node is not in any list, so nothing else touches its links.
            let links = unsafe { &mut *links };
            links.prev = tail;
            links.next = ptr::null();
            links.waker = Some(waker);
        });
        let node_ptr = ptr::from_ref(node);
        if tail.is_null() {
            inner.head = node_ptr;
        } else {
            // SAFETY: `tail` is in the list and we hold the lock.
            unsafe { &*tail }
                .links
                .with_mut(|links| unsafe { (*links).next = node_ptr });
        }
        inner.tail = node_ptr;
        inner.len += 1;
        node.state.store(WAITING, Ordering::Relaxed);
    }

    /// Removes `node` from the list and returns its waker. Leaves its `state` to the caller.
    ///
    /// # Safety
    /// `node` is in this list.
    unsafe fn unlink(&mut self, node: &WaitNode) -> Option<Waker> {
        let inner = &mut **self.guard;
        let (prev, next, waker) = node.links.with_mut(|links| {
            // SAFETY: the node is in this list and we hold the lock.
            let links = unsafe { &mut *links };
            let taken = (links.prev, links.next, links.waker.take());
            links.prev = ptr::null();
            links.next = ptr::null();
            taken
        });
        if prev.is_null() {
            inner.head = next;
        } else {
            // SAFETY: neighbours of a listed node are listed, and we hold the lock.
            unsafe { &*prev }
                .links
                .with_mut(|links| unsafe { (*links).next = next });
        }
        if next.is_null() {
            inner.tail = prev;
        } else {
            // SAFETY: as above.
            unsafe { &*next }
                .links
                .with_mut(|links| unsafe { (*links).prev = prev });
        }
        inner.len -= 1;
        waker
    }
}

impl<S> Deref for Locked<'_, S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.guard.state
    }
}

impl<S> DerefMut for Locked<'_, S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.guard.state
    }
}

impl<S> Drop for Locked<'_, S> {
    fn drop(&mut self) {
        // SAFETY: dropped once, here, and never used again.
        unsafe { ManuallyDrop::drop(&mut self.guard) };
        for waker in self.wakes.drain(..) {
            waker.wake();
        }
    }
}

impl<S: fmt::Debug> fmt::Debug for Locked<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Locked")
            .field("state", &self.guard.state)
            .field("waiters", &self.guard.len)
            .finish()
    }
}

/// The future returned by [`WaitQueue::wait`].
#[must_use = "futures do nothing unless polled"]
pub struct Wait<'a, S: WaitState, A> {
    queue: &'a WaitQueue<S>,
    node: WaitNode,
    /// `None` once the first poll has run it.
    acquire: Option<A>,
    done: bool,
}

impl<S, A> Future for Wait<'_, S, A>
where
    S: WaitState,
    A: FnOnce(&mut Locked<'_, S>) -> bool,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `node` is never moved out of the pinned future; `acquire` is not pinned.
        let this = unsafe { self.get_unchecked_mut() };
        assert!(
            !this.done,
            "porto: a sync Wait future was polled after it completed; await it only once"
        );
        // SAFETY: `node` is structurally pinned (see above).
        let node = unsafe { Pin::new_unchecked(&this.node) };
        // The first poll may acquire at once; later polls only wait for the grant.
        let acquire = this.acquire.take();
        // SAFETY: `&mut self` serializes calls on the node; it is only ever used with
        // `this.queue`, which outlives the future, and `Drop` cancels a pending wait.
        let poll = unsafe {
            this.queue
                .poll_wait(node, cx, |locked| acquire.is_some_and(|a| a(locked)))
        };
        this.done = poll.is_ready();
        poll
    }
}

impl<S: WaitState, A> Drop for Wait<'_, S, A> {
    fn drop(&mut self) {
        // SAFETY: the node was pinned since its first poll and is dropped right after this;
        // `&mut self` serializes; it was only used with `self.queue`.
        unsafe { self.queue.cancel(Pin::new_unchecked(&self.node)) };
    }
}

impl<S: WaitState, A> fmt::Debug for Wait<'_, S, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wait")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}
