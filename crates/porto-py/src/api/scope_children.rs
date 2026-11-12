//! The tasks a scope lists for `Scope.tasks` (20-python-api-reference.md "Structured
//! concurrency").
//!
//! A [`ChildList`] belongs to a `Scope`. It is an intrusive doubly linked list through the
//! `Task` handles of the tasks spawned into it: a spawn links the handle in (one lock, a few
//! stores, no allocation), and a task takes itself out when it ends ([`unlink`], called by
//! `Task::finish`), touching only its two neighbours. It holds no reference, so a finished task
//! leaves nothing behind (porto-szb.43 measured a list of owning handles that was compacted
//! lazily: it kept ended handles until the exit and made 10k-task fan-outs 20% slower).
//!
//! **Single lane only.** The links are sound because of the order of things on the one lane
//! that runs the tasks of a runtime (M1), not because anything is counted:
//!
//! - a task's body holds its handle strongly until it ends, and `Task::finish` unlinks the task
//!   before the body lets go (`PyTask::complete`, `cancel` and `Drop` all finish the handle
//!   first), so a handle in a list is alive;
//! - a task's `home` points at the list of the `Scope` it is listed in, and the `Scope` takes all
//!   its tasks off ([`ChildList::detach_all`]) when it closes and when it is freed, so the
//!   pointer never outlives the list. An ordinary scope cannot even be freed with listed tasks:
//!   its exit waits for them (C5), and a task unlinks before it counts out of the scope.
//!
//! What does not hold once tasks run on several lanes (M2), and what `porto-szb.24` (the
//! per-lane registry) and `porto-szb.142` (derive `Scope.tasks` from it) must resolve before
//! `lanes > 1`: `unlink` reads `home` and then locks the list it points at, so a scope freed in
//! that window is a use after free (the list needs a counted owner, or the registry replaces
//! it); and `PyTask::drop`, which finishes the handle, can run on a thread that is not the
//! task's lane (a waker dropped on another thread) — already possible at one lane, where it is
//! safe only because a scope is always closed (its tasks detached) before it can be freed: the
//! real precondition of the window above is a scope freed unclosed. Routing a freed PyTask's
//! Python references to its lane (`TaskBody::release`, porto-szb.15) does *not* make
//! `finish` run there: the handle must forget the pointer to the body before the allocation
//! goes, so `finish` (and with it the unlink) stays on the thread that dropped the last
//! reference; only the coroutine, the context and the outcome are released on the lane. The orderings below are `SeqCst` so that the
//! one store-load pattern (`register` publishes `home` then reads `ended`, `finish` publishes
//! `ended` then reads `home`) cannot miss on both sides.

use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use pyo3::Borrowed;
use pyo3::prelude::*;

use super::task::Task;

/// The first task of the list: the others hang off it through `Task::next`.
struct Head(*const Task);

// SAFETY: the pointer is only dereferenced under the list's lock, while the invariants in the
// module docs make it point at a live task.
unsafe impl Send for Head {}

impl Head {
    /// Links `task` in at the head.
    fn insert(&mut self, task: &Task) {
        task.set_links(ptr::null(), self.0);
        if !self.0.is_null() {
            // SAFETY: a listed task is alive (module docs).
            unsafe { &*self.0 }.set_prev(ptr::from_ref(task));
        }
        self.0 = ptr::from_ref(task);
    }

    /// Unlinks `task`, which is listed here: its neighbours point at each other.
    fn remove(&mut self, task: &Task) {
        let (prev, next) = (task.prev(), task.next());
        if prev.is_null() {
            self.0 = next;
        } else {
            // SAFETY: a listed task is alive (module docs).
            unsafe { &*prev }.set_next(next);
        }
        if !next.is_null() {
            // SAFETY: as above.
            unsafe { &*next }.set_prev(prev);
        }
        task.set_links(ptr::null(), ptr::null());
    }
}

/// A scope's list of its tasks.
pub(super) struct ChildList {
    head: Mutex<Head>,
    /// Whether anything was ever listed: a scope that spawned nothing skips the lock at its exit.
    used: AtomicBool,
}

impl ChildList {
    pub(super) const fn new() -> Self {
        Self {
            head: Mutex::new(Head(ptr::null())),
            used: AtomicBool::new(false),
        }
    }

    /// Lists `task` here, taking it off the list it was in (a task `Scope.start` moves).
    pub(super) fn register(&self, task: &Bound<'_, Task>) {
        let state = task.get();
        // A task is listed nowhere when it is spawned: skip the swap-and-lock of `unlink`.
        if state.is_listed() {
            unlink(state);
        }
        state.set_object(task.as_ptr());
        let mut head = self.head.lock();
        head.insert(state);
        state.set_home(ptr::from_ref(self).cast_mut());
        // A task that ended meanwhile (another lane ran it to the end before this): it already
        // looked for its list and found none.
        let ended = state.has_ended();
        drop(head);
        if !self.used.load(Ordering::Relaxed) {
            self.used.store(true, Ordering::Relaxed);
        }
        if ended {
            unlink(state);
        }
    }

    /// The listed tasks as their handles, in spawn order.
    pub(super) fn snapshot<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyAny>> {
        if !self.used.load(Ordering::Relaxed) {
            return Vec::new();
        }
        let mut listed: Vec<(u64, Bound<'py, PyAny>)> = Vec::new();
        {
            let head = self.head.lock();
            let mut at = head.0;
            while !at.is_null() {
                // SAFETY: a listed task is alive, and the lock keeps it listed (module docs).
                let task = unsafe { &*at };
                // SAFETY: `object` is the task's Python object, set when it was listed; the new
                // reference outlives the link.
                let object = unsafe { Borrowed::from_ptr(py, task.object()) }.to_owned();
                listed.push((task.task_id(), object));
                at = task.next();
            }
        }
        listed.sort_unstable_by_key(|(id, _)| *id);
        listed.into_iter().map(|(_, object)| object).collect()
    }

    /// Takes every task off: the scope closed or is being freed, so nothing may point at this
    /// list any more.
    pub(super) fn detach_all(&self) {
        if !self.used.load(Ordering::Relaxed) {
            return;
        }
        let mut head = self.head.lock();
        let mut at = std::mem::replace(&mut head.0, ptr::null());
        while !at.is_null() {
            // SAFETY: a listed task is alive (module docs).
            let task = unsafe { &*at };
            at = task.next();
            task.set_links(ptr::null(), ptr::null());
            task.set_home(ptr::null_mut());
        }
    }
}

impl Drop for ChildList {
    fn drop(&mut self) {
        self.detach_all();
    }
}

/// Takes `task` off the list it is in, if any: it ended, or `Scope.start` moves it.
///
/// Locks the list first and checks under the lock that the task is still listed there, so a
/// `detach_all` or a second unlink in between is seen.
pub(super) fn unlink(task: &Task) {
    loop {
        let home = task.home();
        if home.is_null() {
            return;
        }
        // SAFETY: a task's `home` is cleared before its list dies (module docs, single lane).
        let list = unsafe { &*home };
        let mut head = list.head.lock();
        if task.home() == home {
            head.remove(task);
            task.set_home(ptr::null_mut());
            return;
        }
    }
}
