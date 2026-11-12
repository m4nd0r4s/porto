//! The per-lane task registry (`23-observability.md` §1, `11-task-model-and-trap-protocol.md`
//! §1): every task of a lane (Rust and Python) is linked into the lane's intrusive list when it
//! is spawned and unlinked when it completes.
//!
//! # Who touches the list
//!
//! The lane owns it, so the hot path takes no lock and does no atomic read-modify-write: a
//! spawn on the lane links the task at the head, and the completion of a task on the lane
//! unlinks it, with plain loads and stores. "The lane" is the thread running the lane's loop (or
//! the one that owns a runtime that is not running, for [`Runtime::spawn`](crate::Runtime));
//! the callers decide, see [`crate::task::Schedule::registry`]. Everything another thread does
//! about a task goes around the list:
//!
//! - **A spawn from another thread** does not link: the task is queued on the injector, and the
//!   lane links it when it takes it from there ([`TaskRegistry::link_if_new`]). A snapshot
//!   counts what the injector still holds, so it is exact all the same.
//! - **A task freed off its lane** while still linked (the last reference was a waker dropped
//!   on another thread, or the task completed off its lane) is handed to the lane: it goes on
//!   a lock-free stack of "zombies" (refcount 0, memory still valid because it is still
//!   linked), and the lane unlinks and frees it at its next maintenance tick
//!   ([`TaskRegistry::take_zombies`]). Nothing reaches into the list from outside.
//!
//! # Shutdown
//!
//! A runtime that is dropped has no lane any more, yet wakers keep tasks alive (and may be
//! dropped on any thread) for as long as they like. [`TaskRegistry::close`] ends the lane's
//! ownership: it drains the zombies, and from then on every list operation, from any thread,
//! takes the registry's lock (a path only a shutdown walks). [`TaskRegistry::cancel_all`] then
//! cancels every task that is still linked, so no task outlives its runtime however its
//! references are held: a waker cycle (a task waiting on a primitive that holds its waker, which
//! the task holds) is broken, which is what [`Runtime`](crate::Runtime)'s drop relies on.
//!
//! # Snapshots
//!
//! A snapshot is requested of each lane through its deferred-work inbox and taken at the lane's
//! next maintenance tick, or at once if it is parked (the request wakes it): see
//! [`TaskSnapshotRequest`]. The lane's own code walks the list with
//! [`Handle::with_live_tasks`](crate::Handle::with_live_tasks).
//!
//! The loom models run with `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release registry`.

mod node;
mod snapshot;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

use std::marker::PhantomData;
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering;

use crate::sync::primitives::{AtomicBool, AtomicPtr, Mutex, MutexGuard, UnsafeCell, spin};
use crate::task::state::Snapshot;
use crate::task::{Header, TaskBody, TaskFlags, TaskRef};

pub use node::RegistryNode;
pub(crate) use snapshot::{SnapshotCell, record_of};
pub use snapshot::{SnapshotError, TaskKind, TaskRecord, TaskSnapshotRequest, TaskState};

/// Closes the zombie stack: pushes to it fail from then on.
const CLOSED: *mut Header = ptr::without_provenance_mut(usize::MAX);

/// What the owner of a task about to be freed must do first, see
/// [`TaskRegistry::before_free`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeforeFree {
    /// The task is unlinked: free it now.
    Proceed,
    /// The registry took the task, to free it on its lane: do not touch it again.
    Deferred,
}

/// One lane's registry of live tasks.
pub struct TaskRegistry {
    /// The first task of the list, null if it is empty. Touched by the lane thread while the
    /// registry is open, and by whoever holds `lock` once it is [closed](TaskRegistry::close).
    head: UnsafeCell<*mut Header>,
    /// Tasks freed off the lane while linked (a stack through `Header::queue_next`), or
    /// [`CLOSED`].
    zombies: AtomicPtr<Header>,
    /// Set by [`TaskRegistry::close`]: every list operation takes `lock` from then on.
    closed: AtomicBool,
    lock: Mutex<()>,
    /// The introspection switch: whether tasks record timing (`11` §2 `TaskStats`).
    introspection: AtomicBool,
}

// SAFETY: `head` is only touched under the discipline in the module docs (the lane thread while
// open, the lock once closed), every other field is atomic or a lock, and the headers the list
// points at are alive while they are linked.
unsafe impl Send for TaskRegistry {}
// SAFETY: as above.
unsafe impl Sync for TaskRegistry {}

impl TaskRegistry {
    /// An empty, open registry.
    pub fn new(introspection: bool) -> Self {
        Self {
            head: UnsafeCell::new(ptr::null_mut()),
            zombies: AtomicPtr::new(ptr::null_mut()),
            closed: AtomicBool::new(false),
            lock: Mutex::new(()),
            introspection: AtomicBool::new(introspection),
        }
    }

    /// Whether tasks of this lane record their timing (first poll, time in polls, step
    /// start): off by default, since it costs clock reads on every step.
    pub fn introspection(&self) -> bool {
        self.introspection.load(Ordering::Relaxed)
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        // The list is only mutated by code that does not panic midway.
        self.lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether [`TaskRegistry::close`] ran.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    // ----- the list ---------------------------------------------------------------------

    /// Links `task` at the head.
    ///
    /// # Safety
    /// The caller is the lane thread of an open registry or holds `lock`, and `task` is live
    /// and unlinked.
    unsafe fn push_front(&self, task: NonNull<Header>) {
        let old = self.head.with_mut(|head| {
            // SAFETY: the caller's contract: the head is ours.
            unsafe { ptr::replace(head, task.as_ptr()) }
        });
        // SAFETY: `task` is live, and so are the tasks on the list.
        unsafe {
            task.as_ref().registry.set_linked(old);
            if !old.is_null() {
                (*old).registry.set_prev(task.as_ptr());
            }
        }
    }

    /// Unlinks `header`, which is linked in this list.
    ///
    /// # Safety
    /// As for [`TaskRegistry::push_front`], and `header` is linked here.
    unsafe fn unlink(&self, header: &Header) {
        let (prev, next) = header.registry.neighbours();
        // SAFETY: the caller's contract: the neighbours are alive while linked.
        unsafe {
            if prev.is_null() {
                self.head.with_mut(|head| *head = next);
            } else {
                (*prev).registry.set_next(next);
            }
            if !next.is_null() {
                (*next).registry.set_prev(prev);
            }
        }
        header.registry.set_unlinked();
    }

    /// Runs `f` on every linked task, newest first. `f` may unlink the task it is given, no
    /// other.
    ///
    /// # Safety
    /// As for [`TaskRegistry::push_front`].
    unsafe fn walk(&self, mut f: impl FnMut(NonNull<Header>)) {
        // SAFETY: the caller's contract: the head is ours.
        let mut at = self.head.with(|head| unsafe { *head });
        while let Some(task) = NonNull::new(at) {
            // SAFETY: a linked task is alive; its link is read before `f` can unlink it.
            at = unsafe { task.as_ref() }.registry.neighbours().1;
            f(task);
        }
    }

    // ----- hot path: the lane -----------------------------------------------------------

    /// Links a task the lane spawned or took from the injector, unless it is linked already.
    /// A closed registry links nothing: its tasks are cancelled as they come.
    ///
    /// # Safety
    /// The caller is the lane thread (see the module docs), and `task` is live, with the
    /// provenance of its allocation.
    #[inline]
    pub unsafe fn link_if_new(&self, task: NonNull<Header>) {
        // SAFETY: `task` is live (the caller's contract).
        if unsafe { task.as_ref() }.registry.is_linked_relaxed() {
            return;
        }
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        // SAFETY: the lane thread of an open registry, and the task is unlinked.
        unsafe { self.push_front(task) };
    }

    /// A task completed: unlinks it if it is linked and this thread may. `on_lane` says whether
    /// it is the lane thread. A completion off the lane of an open registry leaves the task
    /// linked (it is complete, so walks skip it): the lane unlinks it when its last reference
    /// goes (see [`TaskRegistry::before_free`]).
    #[inline]
    pub fn completed(&self, header: &Header, on_lane: bool) {
        if !header.registry.is_linked_relaxed() {
            return;
        }
        self.unlink_task(header, on_lane);
    }

    #[cold]
    fn unlink_task_closed(&self, header: &Header) {
        let _locked = self.lock();
        if header.registry.is_linked_relaxed() {
            // SAFETY: the lock is held on a closed registry, and the task is linked.
            unsafe { self.unlink(header) };
        }
    }

    /// Unlinks `header` if this thread may: the lane thread of an open registry, or anyone
    /// once it is closed. Returns whether the task is unlinked now.
    #[inline]
    fn unlink_task(&self, header: &Header, on_lane: bool) -> bool {
        if on_lane && !self.closed.load(Ordering::Relaxed) {
            // SAFETY: the lane thread of an open registry, and the task is linked.
            unsafe { self.unlink(header) };
            return true;
        }
        if self.is_closed() {
            self.unlink_task_closed(header);
            return true;
        }
        false
    }

    // ----- freeing ----------------------------------------------------------------------

    /// The last reference to `task` is gone and it is about to be freed: makes sure it is not
    /// on the list. A task that is still linked (it never completed, or it completed off the
    /// lane) is unlinked here if this thread may, else handed to the lane, which frees it.
    ///
    /// # Safety
    /// The caller holds the last reference to `task`, with the provenance of its allocation.
    #[inline]
    pub unsafe fn before_free(&self, task: NonNull<Header>, on_lane: bool) -> BeforeFree {
        // SAFETY: the caller's contract.
        let header = unsafe { task.as_ref() };
        if !header.registry.is_linked() {
            return BeforeFree::Proceed;
        }
        self.before_free_linked(task, header, on_lane)
    }

    #[cold]
    fn before_free_linked(
        &self,
        task: NonNull<Header>,
        header: &Header,
        on_lane: bool,
    ) -> BeforeFree {
        if self.unlink_task(header, on_lane) {
            return BeforeFree::Proceed;
        }
        // Off the lane of an open registry: the lane unlinks it.
        let mut top = self.zombies.load(Ordering::Relaxed);
        loop {
            if top == CLOSED {
                // Closed meanwhile: the closing thread drained the stack, take the lock.
                self.unlink_task_closed(header);
                return BeforeFree::Proceed;
            }
            // SAFETY: the last reference is ours and the task is on no queue, so the link is
            // ours until the push below publishes it.
            unsafe { header.set_queue_next(top) };
            match self.zombies.compare_exchange_weak(
                top,
                task.as_ptr(),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return BeforeFree::Deferred,
                Err(actual) => top = actual,
            }
        }
    }

    /// Whether tasks freed off the lane wait for it.
    pub fn has_zombies(&self) -> bool {
        let top = self.zombies.load(Ordering::Relaxed);
        !top.is_null() && top != CLOSED
    }

    /// Unlinks the tasks freed off the lane and returns them, for the caller to free outside
    /// any lock (freeing one runs the body's destructor).
    ///
    /// # Safety
    /// The caller is the lane thread of an open registry.
    pub unsafe fn take_zombies(&self) -> Vec<NonNull<Header>> {
        let top = self.zombies.swap(ptr::null_mut(), Ordering::AcqRel);
        // SAFETY: the lane thread of an open registry; the stack is ours once swapped out.
        unsafe { self.unlink_stack(top) }
    }

    /// # Safety
    /// As for `unlink`, for every task of the stack starting at `top`, which the caller took.
    unsafe fn unlink_stack(&self, top: *mut Header) -> Vec<NonNull<Header>> {
        let mut freed = Vec::new();
        let mut at = top;
        while let Some(task) = NonNull::new(at) {
            // SAFETY: a zombie is alive (linked) until the lane frees it, and its link was
            // published by the push.
            let header = unsafe { task.as_ref() };
            // SAFETY: as above.
            at = unsafe { header.queue_next() };
            if header.registry.is_linked_relaxed() {
                // SAFETY: the caller's contract; the task is linked.
                unsafe { self.unlink(header) };
            }
            freed.push(task);
        }
        freed
    }

    // ----- shutdown ---------------------------------------------------------------------

    /// Ends the lane's ownership of the list (the runtime is being dropped): drains the zombies
    /// and makes every later list operation, from any thread, take the lock. Returns the
    /// zombies, unlinked, for the caller to free.
    ///
    /// # Safety
    /// The caller owns the runtime: no thread is running its lane, and none will.
    pub unsafe fn close(&self) -> Vec<NonNull<Header>> {
        let _locked = self.lock();
        self.closed.store(true, Ordering::Release);
        let top = self.zombies.swap(CLOSED, Ordering::AcqRel);
        let top = if top == CLOSED { ptr::null_mut() } else { top };
        // SAFETY: the lock is held, and no other thread touches the list without it.
        unsafe { self.unlink_stack(top) }
    }

    /// Cancels every task that is still linked, once the registry is closed: a task that is
    /// idle is woken with a cancellation request, which completes it where it is (nothing can
    /// queue it any more); a complete one is only unlinked. Returns whether it found any task
    /// to cancel; a caller loops until it does not (a cancelled task may wake others).
    ///
    /// # Panics
    /// If the registry is not closed.
    pub fn cancel_all(&self) -> bool {
        assert!(
            self.is_closed(),
            "porto: a registry is cancelled once closed"
        );
        let mut any = false;
        let mut rounds = 0_u32;
        loop {
            let mut victims = Vec::new();
            {
                let _locked = self.lock();
                // SAFETY: the lock is held on a closed registry.
                unsafe {
                    self.walk(|task| {
                        let header = task.as_ref();
                        if header.state().load().is_complete() {
                            self.unlink(header);
                        } else if let Some(task) = TaskRef::try_new(task) {
                            victims.push(task);
                        }
                        // else: its last reference is going; it unlinks itself under the lock
                    });
                }
            }
            if victims.is_empty() {
                return any;
            }
            any = true;
            for task in victims {
                task.cancel();
            }
            // A task another thread is polling finishes by itself; give it the time.
            rounds += 1;
            if rounds > 1 {
                spin(rounds);
            }
        }
    }

    /// Whether nothing is linked, for the loom models of a closed registry.
    #[cfg(all(test, loom))]
    pub(crate) fn is_empty_closed(&self) -> bool {
        assert!(self.is_closed());
        let _locked = self.lock();
        let mut empty = true;
        // SAFETY: the lock is held on a closed registry.
        unsafe { self.walk(|_| empty = false) };
        empty
    }

    // ----- walks ------------------------------------------------------------------------

    /// Runs `f` on every live task (linked, not complete, with a reference), newest first.
    ///
    /// # Safety
    /// The caller is the lane thread of an open registry, and `f` neither spawns, completes
    /// nor frees a task.
    pub unsafe fn for_each_live(&self, mut f: impl FnMut(LiveTask<'_>)) {
        // SAFETY: the caller's contract.
        unsafe {
            self.walk(|task| {
                let state = task.as_ref().state().load();
                if is_live(state) {
                    f(LiveTask {
                        task,
                        state,
                        _list: PhantomData,
                    });
                }
            });
        }
    }
}

/// Whether a task in the list counts as live: it did not complete, and a reference to it still
/// exists (a task whose last reference went off the lane waits there to be freed).
fn is_live(state: Snapshot) -> bool {
    !state.is_complete() && state.ref_count() > 0
}

/// A task on the registry's list, as the lane's walk shows it ([`Handle::with_live_tasks`]).
///
/// The view is valid for the duration of the callback: the lane thread is the only one that
/// links, unlinks and frees tasks, and the walk is on it.
///
/// [`Handle::with_live_tasks`]: crate::Handle::with_live_tasks
pub struct LiveTask<'a> {
    task: NonNull<Header>,
    state: Snapshot,
    _list: PhantomData<&'a Header>,
}

impl LiveTask<'_> {
    /// The task's header.
    pub fn header(&self) -> &Header {
        // SAFETY: linked, so alive, and the walk is on the lane that frees it.
        unsafe { self.task.as_ref() }
    }

    /// The task's id.
    pub fn id(&self) -> u64 {
        self.header().id()
    }

    /// The task's state when the walk reached it.
    pub fn state(&self) -> Snapshot {
        self.state
    }

    /// The task's spawn flags.
    pub fn flags(&self) -> TaskFlags {
        self.header().flags()
    }

    /// Whether the task counts in `scope`.
    ///
    /// Reads the header's scope slot: right for a task that is not being polled by another
    /// thread, which holds for one lane (M1). With several lanes (M2), a stolen task could move
    /// scope while this reads (`Scope.start`): porto-j62.27.
    pub fn is_in_scope<E>(&self, scope: &crate::scope::ScopeRef<E>) -> bool {
        // SAFETY: see above: the lane thread reads the slot of a live task nobody else polls.
        unsafe { self.header().counts_in(scope) }
    }

    /// The id of the scope the task counts in, if any. See [`LiveTask::is_in_scope`].
    pub fn scope_id(&self) -> Option<u64> {
        // SAFETY: as for `is_in_scope`.
        unsafe { self.header().scope_id() }
    }

    /// A pointer to the task's body, for a task spawned with [`Handle::spawn_body`] with body
    /// type `B`: valid while the walk runs, with the provenance of the allocation.
    ///
    /// Dereferencing it is only sound for what `B` synchronizes: the lane polls bodies through
    /// `&mut`, so read single fields through the raw pointer, and only for a task that is not
    /// running (see [`crate::task::TaskBodyRef::body`]).
    ///
    /// # Safety
    /// The task's body is a `B` (a [`TaskFlags::PYTHON`] task of porto-py is a `PyTask`): a
    /// task of another kind has no body at that offset.
    ///
    /// [`Handle::spawn_body`]: crate::Handle::spawn_body
    pub unsafe fn body<B: TaskBody>(&self) -> NonNull<B> {
        let offset = self.header().vtable.body_offset;
        // SAFETY: `body_offset` is the body's offset in this allocation (the caller's contract:
        // it is a body task), and the pointer has the allocation's provenance.
        unsafe { self.task.cast::<u8>().add(offset).cast::<B>() }
    }
}

impl std::fmt::Debug for TaskRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRegistry")
            .field("closed", &self.is_closed())
            .field("zombies", &self.has_zombies())
            .finish_non_exhaustive()
    }
}
