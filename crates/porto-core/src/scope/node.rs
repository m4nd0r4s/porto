//! The scope tree: [`ScopeNode`], its lock-protected [`ScopeState`], and cancellation.

use std::collections::HashMap;
use std::fmt;
use std::hash::{BuildHasherDefault, Hasher};
use std::marker::PhantomPinned;
use std::ptr;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::task::Waker;

use super::child::{ChildVTableRef, child_vtable};
use super::sync::{
    Arc, AtomicPtr, AtomicU8, AtomicU64, CANCEL_GENERATION, Mutex, MutexGuard, UnsafeCell,
};
use crate::sched::WeakHandle;
use crate::time::Deadline;
use crate::time::wheel::TimerEntry;

/// Why a scope was cancelled. The first reason wins: cancelling a cancelled scope does nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CancelReason {
    /// `scope.cancel()` was called.
    Explicit = 1,
    /// The scope's deadline passed.
    Deadline = 2,
    /// An enclosing scope was cancelled. The walk reaches that scope itself, so this is only
    /// the reason [`Cancellation::reason_for`] reports to an inner scope.
    Parent = 3,
    /// A task spawned into the scope failed (C4).
    ChildFailed = 4,
    /// The runtime is shutting down.
    Shutdown = 5,
    /// `Task.cancel()` was called: the implicit scope of one task (15 §4) was cancelled.
    TaskCancelled = 6,
}

impl CancelReason {
    fn from_bits(bits: u8) -> Option<Self> {
        match bits & REASON_MASK {
            0 => None,
            1 => Some(Self::Explicit),
            2 => Some(Self::Deadline),
            3 => Some(Self::Parent),
            4 => Some(Self::ChildFailed),
            6 => Some(Self::TaskCancelled),
            _ => Some(Self::Shutdown),
        }
    }
}

/// A `walk_cache` that matches no generation: the first check walks.
const NO_WALK: u64 = u64::MAX;

/// [`ScopeNode::status`] bits 0-2: the [`CancelReason`], 0 while not cancelled.
const REASON_MASK: u8 = 0b0111;
/// [`ScopeNode::status`] bit 3: the scope is shielded.
const SHIELD: u8 = 0b1000;

/// Never reused, so a [`super::CancelCache`] can tell scopes apart even when one is freed and
/// another is allocated at its address. Not part of the loom models, so always std's.
static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// One scope of the cancel tree (`15-structured-concurrency-and-cancellation.md` §1.1).
///
/// A child holds its parent alive, and the parent lists its children intrusively without
/// owning them: a child unlinks itself when it is dropped. `E` is the failure type the scope
/// collects (Python exceptions for porto-py); this crate never looks inside it.
///
/// `repr(C)` keeps `child_vtable` first: a task's type-erased [`super::TaskScope`] points at
/// the node and reads it there.
#[repr(C)]
pub struct ScopeNode<E> {
    /// How a child task, which does not know `E`, reports its end to this scope.
    child_vtable: ChildVTableRef,
    id: u64,
    parent: Option<Arc<ScopeNode<E>>>,
    /// Null, or the scope that adopted this one in place of `parent` ([`ScopeNode::adopt`]):
    /// a leaked `Box` of the `Arc`, published once and freed when this node is. From then on
    /// the walk and the child lists see it as the parent; `parent` stays held, only to keep
    /// what it points at alive.
    adopted: AtomicPtr<Arc<ScopeNode<E>>>,
    /// The task whose `async with` created the scope.
    owner_task: u64,
    /// The cancel reason and the shield bit, so the walk reads them without locking. Written
    /// only while `state` is locked.
    status: AtomicU8,
    /// Bumped whenever the walk's result for this scope may have changed: this scope or an
    /// ancestor was cancelled, or a shield on the way changed. Tasks compare it to their
    /// snapshot after the generation moved.
    cancel_epoch: AtomicU64,
    /// The walk's result for this scope, cached with the cancel generation it was computed at:
    /// `generation << 1 | cancelled`. See [`ScopeNode::is_cancelled`].
    walk_cache: AtomicU64,
    /// This scope's links in the parent's `child_scopes`, guarded by the parent's `state` lock.
    siblings: UnsafeCell<Links<E>>,
    state: Mutex<ScopeState<E>>,
    /// The wheel entry that cancels the scope at its deadline (`super::deadline`): linked in
    /// the wheel of [`ScopeState::deadline_on`]'s runtime or in none, and only armed or
    /// disarmed under `state`'s lock. While linked, its waker holds a reference to this node,
    /// so the node outlives the link.
    pub(super) deadline_timer: TimerEntry,
    /// Neighbours write `siblings` through shared references while this node's `Drop` holds
    /// `&mut self` (it waits for the parent's lock before unlinking). `!Unpin` keeps that
    /// `&mut` from asserting unique access, as for tokio's intrusive waiters.
    _not_unique: PhantomPinned,
}

/// The parts of a scope that change under its lock (§1.1).
///
/// `cancel_called` and `shield` live in the node's status bits instead, written under this
/// lock, so the effective-cancellation walk needs no lock.
pub struct ScopeState<E> {
    /// Tasks spawned into this scope that have not completed.
    pub live_children: u32,
    /// Failures collected from the body and the children, raised as a group at exit.
    pub errors: Vec<E>,
    /// A task ended because this scope's own cancellation reached it (not an enclosing
    /// scope's): the scope's exit counts that as a cancel it absorbed (C3, `cancelled_caught`).
    /// Set by the binding, which alone tells how a task's body ended.
    pub cancel_absorbed: bool,
    /// The owner task, waiting in `ScopeExit` for `live_children` to reach zero.
    pub exit_waiter: Option<Waker>,
    /// The scope's block exited ([`ScopeNode::close`]): no task can be spawned into it any more
    /// (C7, [`super::TaskScope::try_attach`]).
    pub closed: bool,
    /// When the scope cancels itself with [`CancelReason::Deadline`] (C9);
    /// [`Deadline::NEVER`] for none. See [`ScopeNode::set_deadline`].
    pub(super) deadline: Deadline,
    /// The runtime whose wheel links `deadline_timer`, `None` while it is linked nowhere. Weak:
    /// the linked entry's waker holds this node, so a strong handle would make a cycle through
    /// the runtime's wheel, and would keep the runtime's state alive after the run.
    pub(super) deadline_on: Option<WeakHandle>,
    /// Nested scopes, for propagating a cancel down the tree.
    child_scopes: ChildList<E>,
    /// The tasks whose innermost scope this is and that wait in a cancellable wait, by task
    /// key: a cancel that reaches this scope wakes them (C10). See
    /// [`ScopeNode::register_waiter`].
    /// Boxed on the first registration: most scopes never have a waiting task in them.
    waiters: Option<Box<Waiters>>,
}

type Waiters = HashMap<usize, Waker, BuildHasherDefault<KeyHasher>>;

/// Hashes the task keys of [`ScopeState::waiters`]: addresses, unique and spread already, so a
/// multiply is enough (and a scope made per request pays for no random hasher state).
#[derive(Default)]
struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        // Keys are cache-line-aligned addresses: the multiply keeps their zero low bits, and
        // the table picks buckets from the low bits, so bring the mixed high bits down.
        self.0.rotate_left(26)
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = (self.0.rotate_left(8) ^ u64::from(byte)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }

    fn write_usize(&mut self, key: usize) {
        self.0 = (key as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// An intrusive list of child scopes: the head here, the links in each child.
struct ChildList<E> {
    head: *const ScopeNode<E>,
}

struct Links<E> {
    prev: *const ScopeNode<E>,
    next: *const ScopeNode<E>,
}

// SAFETY: the raw pointers in `ChildList` and `Links` point to child scopes that are alive while
// they are linked (a child unlinks itself before it is freed), and they are only read or written
// while the parent's `state` lock is held, which orders every access across threads. The rest
// of the node is atomics, an `Arc` and a `Mutex`, which are `Send + Sync` when `E: Send`.
unsafe impl<E: Send> Send for ScopeNode<E> {}
// SAFETY: as for `Send`; shared access never touches the pointers without the parent's lock.
unsafe impl<E: Send> Sync for ScopeNode<E> {}
// SAFETY: the head is only used under the owning scope's lock, see `ScopeNode`'s `Send`.
unsafe impl<E: Send> Send for ChildList<E> {}

/// The result of an effective-cancellation walk that found a cancelled scope.
pub struct Cancellation<'a, E> {
    /// The cancelled scope the walk reached first.
    pub scope: &'a ScopeNode<E>,
    /// Why that scope was cancelled.
    pub reason: CancelReason,
}

impl<E> Cancellation<'_, E> {
    /// The reason as seen from `inner`, the scope the walk started at: the scope's own reason
    /// if `inner` is the cancelled scope, else [`CancelReason::Parent`].
    pub fn reason_for(&self, inner: &ScopeNode<E>) -> CancelReason {
        if ptr::eq(self.scope, inner) {
            self.reason
        } else {
            CancelReason::Parent
        }
    }
}

impl<E> ScopeState<E> {
    #[cfg(test)]
    pub(super) fn child_scopes_is_empty(&self) -> bool {
        self.child_scopes.head.is_null()
    }

    /// How many tasks are registered as waiting in this scope.
    pub fn waiter_count(&self) -> usize {
        self.waiters.as_ref().map_or(0, |waiters| waiters.len())
    }
}

impl<E: Send + 'static> ScopeNode<E> {
    /// A scope with no parent: the root scope of a runtime, owned by its main task.
    pub fn new_root(owner_task: u64) -> Arc<Self> {
        Arc::new(Self::new(None, owner_task, false))
    }

    /// A scope nested in `parent`, created by `owner_task`'s `async with`.
    pub fn new_child(parent: &Arc<Self>, owner_task: u64, shield: bool) -> Arc<Self> {
        let child = Arc::new(Self::new(Some(Arc::clone(parent)), owner_task, shield));
        let child_ptr: *const Self = &raw const *child;
        let mut state = parent.lock_state();
        let old_head = state.child_scopes.head;
        // SAFETY: the parent's lock is held, which guards the links of its children, and
        // `old_head` (if any) is a linked child, so it is alive.
        unsafe {
            child.siblings.with_mut(|links| (*links).next = old_head);
            if let Some(old_head) = old_head.as_ref() {
                old_head
                    .siblings
                    .with_mut(|links| (*links).prev = child_ptr);
            }
        }
        state.child_scopes.head = child_ptr;
        drop(state);
        child
    }

    fn new(parent: Option<Arc<Self>>, owner_task: u64, shield: bool) -> Self {
        Self {
            child_vtable: child_vtable::<E>(),
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent,
            adopted: AtomicPtr::new(ptr::null_mut()),
            owner_task,
            status: AtomicU8::new(if shield { SHIELD } else { 0 }),
            cancel_epoch: AtomicU64::new(0),
            walk_cache: AtomicU64::new(NO_WALK),
            deadline_timer: TimerEntry::new(),
            siblings: UnsafeCell::new(Links {
                prev: ptr::null(),
                next: ptr::null(),
            }),
            state: Mutex::new(ScopeState {
                live_children: 0,
                errors: Vec::new(),
                cancel_absorbed: false,
                exit_waiter: None,
                closed: false,
                deadline: Deadline::NEVER,
                deadline_on: None,
                child_scopes: ChildList { head: ptr::null() },
                waiters: None,
            }),
            _not_unique: PhantomPinned,
        }
    }
}

impl<E> ScopeNode<E> {
    /// Unique for the process lifetime, never reused.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The enclosing scope, `None` for a root: the one that adopted this scope, if any.
    pub fn parent(&self) -> Option<&Arc<Self>> {
        let adopted = self.adopted.load(Ordering::Acquire);
        // SAFETY: non-null is a `Box<Arc<_>>` published by `adopt` (Acquire pairs with its
        // Release) and freed only when this node is dropped, which the borrow of `self` rules
        // out.
        match unsafe { adopted.as_ref() } {
            Some(adopted) => Some(adopted),
            None => self.parent.as_ref(),
        }
    }

    /// The task whose `async with` created the scope.
    pub fn owner_task(&self) -> u64 {
        self.owner_task
    }

    /// Why this scope itself was cancelled, `None` if it was not (its ancestors may be).
    pub fn cancel_called(&self) -> Option<CancelReason> {
        CancelReason::from_bits(self.status.load(Ordering::Acquire))
    }

    /// Whether the scope is shielded from its ancestors' cancellation.
    pub fn is_shielded(&self) -> bool {
        self.status.load(Ordering::Acquire) & SHIELD != 0
    }

    /// The current cancel epoch, see [`super::CancelCache`].
    pub(super) fn cancel_epoch(&self) -> u64 {
        self.cancel_epoch.load(Ordering::Acquire)
    }

    /// Locks the scope's state. A panic while it was locked leaves plain counters behind, so a
    /// poisoned lock is used as is.
    ///
    /// Never drop a task handle, a task waker or a body reference while holding the guard: the
    /// last one can free a task of this scope, which locks it again to count the task out. Take
    /// the old value out (`mem::replace`), drop the guard, then drop the value.
    pub fn lock_state(&self) -> MutexGuard<'_, ScopeState<E>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Closes the scope (C7): from now on no task can be spawned into it, and its deadline no
    /// longer cancels it (its timer is disarmed). Returns the failures it recorded, which the
    /// caller drops (or raises) after the lock is released. Closing again returns nothing more.
    pub fn close(&self) -> Vec<E> {
        let mut state = self.lock_state();
        state.closed = true;
        let timer = self.disarm_deadline(&mut state);
        let errors = std::mem::take(&mut state.errors);
        drop(state);
        // The timer's waker holds a reference to this node: dropped after the lock.
        drop(timer);
        errors
    }

    /// Cancels the scope. Returns `false`, and changes nothing, if it was already cancelled.
    ///
    /// Safe from any thread (C10). Every task whose walk now reaches this scope sees itself
    /// cancelled at its next check, and the waiters registered in the subtree the cancel
    /// reaches ([`ScopeNode::register_waiter`]) are woken, after every lock is released.
    pub fn cancel(&self, reason: CancelReason) -> bool {
        self.cancel_if(reason, |_| true)
    }

    /// [`ScopeNode::cancel`], if `allowed` says so about the state, checked under the same lock
    /// that the cancel takes: what a closing scope or a moved deadline cannot slip between.
    /// Returns whether it cancelled.
    pub(super) fn cancel_if(
        &self,
        reason: CancelReason,
        allowed: impl FnOnce(&ScopeState<E>) -> bool,
    ) -> bool {
        let mut wake = Vec::new();
        let state = self.lock_state();
        let status = self.status.load(Ordering::Relaxed);
        if status & REASON_MASK != 0 || !allowed(&state) {
            return false;
        }
        self.status.store(status | reason as u8, Ordering::Release);
        self.bump_subtree(&state, Some(&mut wake));
        drop(state);
        CANCEL_GENERATION.fetch_add(1, Ordering::Release);
        wake_all(wake);
        true
    }

    /// Shields the scope from its ancestors' cancellation, or stops shielding it. Unshielding
    /// wakes the waiters of the subtree, which may be cancelled now.
    pub fn set_shield(&self, shield: bool) {
        let mut wake = Vec::new();
        let state = self.lock_state();
        let status = self.status.load(Ordering::Relaxed);
        if (status & SHIELD != 0) == shield {
            return;
        }
        self.status.store(status ^ SHIELD, Ordering::Release);
        self.bump_subtree(&state, (!shield).then_some(&mut wake));
        drop(state);
        CANCEL_GENERATION.fetch_add(1, Ordering::Release);
        wake_all(wake);
    }

    /// Bumps the epoch of this scope and of every descendant its cancellation reaches: the walk
    /// stops at a shielded child, so its subtree is left alone. Collects the wakers of their
    /// waiters into `wake`, if given. `state` is this scope's state, locked; the children's
    /// locks are taken under it (parent before child, the tree's order).
    fn bump_subtree(&self, state: &ScopeState<E>, mut wake: Option<&mut Vec<Waker>>) {
        self.cancel_epoch.fetch_add(1, Ordering::Release);
        if let (Some(wake), Some(waiters)) = (wake.as_deref_mut(), &state.waiters) {
            wake.extend(waiters.values().cloned());
        }
        let mut child = state.child_scopes.head;
        // SAFETY: this scope's lock is held, so no child can unlink itself (it takes that lock
        // first) and every linked child stays alive while we use it.
        while let Some(node) = unsafe { child.as_ref() } {
            let child_state = node.lock_state();
            if !node.is_shielded() {
                node.bump_subtree(&child_state, wake.as_deref_mut());
            }
            drop(child_state);
            // SAFETY: still under this scope's lock, which guards the child's links.
            child = unsafe { node.siblings.with(|links| (*links).next) };
        }
    }

    /// Files `waker` as the waker of the task keyed `key` (any value unique among live tasks,
    /// such as its header's address), which waits in a cancellable wait and whose innermost
    /// scope this is: a cancel that reaches this scope wakes it (C10). A later registration of
    /// the same key replaces it. Unregister it when the wait ends
    /// ([`ScopeNode::unregister_waiter`]): the scope holds the waker until then.
    ///
    /// Returns whether the scope is effectively cancelled, walked after the waker is filed, so
    /// a cancel that raced with the registration is never missed: either it found the waker,
    /// or the walk sees it. The check MUST NOT use a [`super::CancelCache`], whose generation
    /// the cancel bumps only after it woke nobody here.
    pub fn register_waiter(&self, key: usize, waker: &Waker) -> bool {
        let mut state = self.lock_state();
        let waiters = state.waiters.get_or_insert_default();
        let stale = match waiters.get(&key) {
            Some(filed) if filed.will_wake(waker) => None,
            _ => waiters.insert(key, waker.clone()),
        };
        drop(state);
        // Dropped after the unlock: dropping a waker can free its task, which locks the scope.
        drop(stale);
        self.effective_cancellation().is_some()
    }

    /// Forgets the waker [`ScopeNode::register_waiter`] filed under `key`, if any.
    pub fn unregister_waiter(&self, key: usize) {
        let mut state = self.lock_state();
        let filed = state
            .waiters
            .as_mut()
            .and_then(|waiters| waiters.remove(&key));
        drop(state);
        drop(filed);
    }

    /// Whether a task whose innermost scope is this one is effectively cancelled: the walk's
    /// result, cached in the scope for every task in it (§1.2).
    ///
    /// The check loads the global cancel generation and the scope's cached `(generation,
    /// result)`: while no cancel or shield change happened anywhere since the cache was filled,
    /// that is the answer, so the common check is two loads and a compare and touches nothing
    /// shared for writing. Otherwise it walks and refills the cache. A refill by a thread that
    /// read an older generation can overwrite a newer one; it only makes the next check walk
    /// again, since the generation it records is the one read before its walk.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        let generation = CANCEL_GENERATION.load(Ordering::Acquire);
        let cached = self.walk_cache.load(Ordering::Relaxed);
        if cached >> 1 == generation & (u64::MAX >> 1) {
            return cached & 1 != 0;
        }
        self.refill_walk_cache(generation)
    }

    #[cold]
    fn refill_walk_cache(&self, generation: u64) -> bool {
        let cancelled = self.effective_cancellation().is_some();
        self.walk_cache
            .store(generation << 1 | u64::from(cancelled), Ordering::Relaxed);
        cancelled
    }

    /// The effective-cancellation walk (§1.2): from this scope outward, the first scope that is
    /// cancelled before one that is shielded, counting the shielded scope's own cancellation.
    pub fn effective_cancellation(&self) -> Option<Cancellation<'_, E>> {
        let mut node = self;
        loop {
            let status = node.status.load(Ordering::Acquire);
            if let Some(reason) = CancelReason::from_bits(status) {
                return Some(Cancellation {
                    scope: node,
                    reason,
                });
            }
            if status & SHIELD != 0 {
                return None;
            }
            node = node.parent()?;
        }
    }

    /// Makes `new_parent` this scope's parent in place of the one it was nested in: the walk
    /// from here, the cancels `new_parent` propagates and its subtree waiters (C10) reach it
    /// from now on, and the old parent's do not. For `Scope.start` (15 §3): the hidden scope a
    /// started task runs in until it reports is adopted by the scope it is started in.
    ///
    /// Safe from any thread. At most once: returns `false`, changing nothing, if this scope
    /// was adopted already. A root (a scope entered by a task in no scope) can be adopted
    /// too: it then has a parent for the first time.
    pub fn adopt(&self, new_parent: &Arc<Self>) -> bool {
        let published = Box::into_raw(Box::new(Arc::clone(new_parent)));
        if self
            .adopted
            .compare_exchange(
                ptr::null_mut(),
                published,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            // SAFETY: `published` came from `Box::into_raw` above and was not stored.
            drop(unsafe { Box::from_raw(published) });
            return false;
        }
        // Out of the old parent's child list, then into the new one's: never in both, so
        // each list is only touched under its own lock. In between, a cancel of either parent
        // misses this scope's subtree, which the wake-up below makes up for.
        if let Some(old_parent) = &self.parent {
            old_parent.unlink_child(self);
        }
        new_parent.link_child(self);
        // Everything the walk from here (or below) saw is stale: the parent changed.
        let mut wake = Vec::new();
        let state = self.lock_state();
        self.bump_subtree(
            &state,
            self.effective_cancellation().is_some().then_some(&mut wake),
        );
        drop(state);
        CANCEL_GENERATION.fetch_add(1, Ordering::Release);
        wake_all(wake);
        true
    }

    /// Links `child` at the head of this scope's child list.
    fn link_child(&self, child: &ScopeNode<E>) {
        let child_ptr: *const ScopeNode<E> = child;
        let mut state = self.lock_state();
        let old_head = state.child_scopes.head;
        // SAFETY: this scope's lock is held, which guards the links of its children, and
        // `old_head` (if any) is a linked child, so it is alive; `child` is linked nowhere.
        unsafe {
            child.siblings.with_mut(|links| {
                (*links).prev = ptr::null();
                (*links).next = old_head;
            });
            if let Some(old_head) = old_head.as_ref() {
                old_head
                    .siblings
                    .with_mut(|links| (*links).prev = child_ptr);
            }
        }
        state.child_scopes.head = child_ptr;
    }

    /// Unlinks `child` from this scope's child list.
    fn unlink_child(&self, child: &ScopeNode<E>) {
        let mut state = self.lock_state();
        // SAFETY: this scope's lock is held, which guards the links of its children; the
        // linked neighbours are alive, and `child` is linked here (the caller's contract).
        unsafe {
            let (prev, next) = child.siblings.with(|links| ((*links).prev, (*links).next));
            match prev.as_ref() {
                Some(prev) => prev.siblings.with_mut(|links| (*links).next = next),
                None => state.child_scopes.head = next,
            }
            if let Some(next) = next.as_ref() {
                next.siblings.with_mut(|links| (*links).prev = prev);
            }
            child.siblings.with_mut(|links| {
                (*links).prev = ptr::null();
                (*links).next = ptr::null();
            });
        }
    }
}

/// Wakes `wakers` by value: a by-value wake always writes the task's state word, so the poll
/// it causes sees what the waker wrote before it (the cancel bits, spec 11 §3.1).
fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

impl<E> Drop for ScopeNode<E> {
    fn drop(&mut self) {
        debug_assert!(
            self.lock_state().child_scopes.head.is_null(),
            "a child scope outlived its parent, but children hold their parent alive"
        );
        debug_assert!(
            self.lock_state().waiter_count() == 0,
            "a waiter outlived its scope, but waiting tasks hold their scope alive"
        );
        // The scope that adopted this one holds its links now; the original parent only keeps
        // what it points at alive.
        let adopted = self.adopted.load(Ordering::Acquire);
        let adopted = (!adopted.is_null())
            // SAFETY: `adopt` leaked this box and nothing else frees it.
            .then(|| unsafe { Box::from_raw(adopted) });
        let Some(parent) = adopted.as_deref().or(self.parent.as_ref()) else {
            return;
        };
        parent.unlink_child(self);
        // Dropped after the unlink, which took the parent's lock: a parent freed by this drop
        // would take its own.
        drop(adopted);
    }
}

impl<E> fmt::Debug for ScopeNode<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopeNode")
            .field("id", &self.id)
            .field("parent", &self.parent.as_ref().map(|parent| parent.id))
            .field("owner_task", &self.owner_task)
            .field("cancel_called", &self.cancel_called())
            .field("shield", &self.is_shielded())
            .finish_non_exhaustive()
    }
}
