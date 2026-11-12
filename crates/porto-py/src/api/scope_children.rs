//! The tasks a scope lists for `Scope.tasks` (20-python-api-reference.md "Structured
//! concurrency").
//!
//! A [`ChildList`] belongs to a `Scope`. It holds *non-owning* links to the `Task` handles of the
//! tasks spawned into it, and a task takes itself out when it ends ([`unlink`], called by
//! `Task::finish`), so a finished task leaves nothing behind: no handle is kept alive, and a
//! spawn costs one lock and a push, an end one lock and a swap-remove (porto-szb.43 measured a
//! list of owning handles that was compacted lazily: it kept the ended handles until the exit
//! and made 10k-task fan-outs 20% slower).
//!
//! The links are sound because of the order of things, not because anything is counted:
//!
//! - a task's body holds its handle strongly until it ends, and `Task::finish` unlinks the task
//!   before the body lets go (`PyTask::complete`, `cancel` and `Drop` all finish the handle
//!   first), so a handle in a list is alive;
//! - a task's `home` points at the list of the `Scope` it is listed in, and the `Scope` takes all
//!   its tasks off ([`ChildList::detach_all`]) when it closes and when it is freed, so the
//!   pointer never outlives the list. An ordinary scope cannot even be freed with listed tasks:
//!   its exit waits for them (C5), and a task unlinks before it counts out of the scope.
//!
//! All of it runs on the lane that runs the tasks, except a snapshot (`Scope.tasks`), which
//! takes the list's lock like the rest.

use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::{Borrowed, ffi};

use super::task::Task;

/// One listed task: its `Task` struct (for the id and the slot) and the Python object around it
/// (to hand out in a snapshot).
struct Entry {
    task: *const Task,
    object: *mut ffi::PyObject,
}

/// The entries. Slot 0 is the first task of a scope, inline, so a scope that spawns one task
/// (the common case of a scope per request) allocates nothing; slot `n + 1` is `rest[n]`.
#[derive(Default)]
struct Children {
    first: Option<Entry>,
    rest: Vec<Entry>,
}

// SAFETY: the pointers are only dereferenced under the list's lock, while the invariants in the
// module docs make them point at live tasks.
unsafe impl Send for Children {}

impl Children {
    /// Lists `entry` and returns its slot.
    fn insert(&mut self, entry: Entry) -> u32 {
        if self.first.is_none() {
            self.first = Some(entry);
            return 0;
        }
        self.rest.push(entry);
        u32::try_from(self.rest.len()).expect("a scope lists fewer than 4 billion tasks")
    }

    /// Takes `task` off, if it is listed where it says: the last entry moves into its place.
    fn remove(&mut self, task: &Task) {
        let slot = task.slot() as usize;
        let this = ptr::from_ref(task);
        if slot == 0 {
            if self.first.as_ref().is_some_and(|entry| entry.task == this) {
                self.first = None;
            }
            return;
        }
        let index = slot - 1;
        if self.rest.get(index).is_some_and(|entry| entry.task == this) {
            self.rest.swap_remove(index);
            if let Some(moved) = self.rest.get(index) {
                // SAFETY: a listed task is alive (module docs).
                unsafe { &*moved.task }.set_slot(u32::try_from(index + 1).unwrap_or(u32::MAX));
            }
        }
    }
}

/// A scope's list of its tasks.
pub(super) struct ChildList {
    children: Mutex<Children>,
    /// Whether anything was ever listed: a scope that spawned nothing skips the lock at its exit.
    used: AtomicBool,
}

impl ChildList {
    pub(super) const fn new() -> Self {
        Self {
            children: Mutex::new(Children {
                first: None,
                rest: Vec::new(),
            }),
            used: AtomicBool::new(false),
        }
    }

    /// Lists `task` here, taking it off the list it was in (a task `Scope.start` moves).
    pub(super) fn register(&self, task: &Bound<'_, Task>) {
        let state = task.get();
        unlink(state);
        let entry = Entry {
            task: ptr::from_ref(state),
            object: task.as_ptr(),
        };
        let mut children = self.children.lock();
        let slot = children.insert(entry);
        state.set_slot(slot);
        state.set_home(ptr::from_ref(self).cast_mut());
        // A task that ended meanwhile (another lane ran it to the end before this): it already
        // looked for its list and found none.
        let ended = state.has_ended();
        drop(children);
        self.used.store(true, Ordering::Relaxed);
        if ended {
            unlink(state);
        }
    }

    /// The listed tasks as their handles, in spawn order.
    pub(super) fn snapshot<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyAny>> {
        if !self.used.load(Ordering::Relaxed) {
            return Vec::new();
        }
        let mut listed: Vec<(u64, Bound<'py, PyAny>)> = {
            let children = self.children.lock();
            children
                .first
                .iter()
                .chain(children.rest.iter())
                .map(|entry| {
                    // SAFETY: a listed task is alive, and the lock keeps it listed (module
                    // docs); the new reference outlives the entry.
                    unsafe {
                        (
                            (*entry.task).task_id(),
                            Borrowed::from_ptr(py, entry.object).to_owned(),
                        )
                    }
                })
                .collect()
        };
        listed.sort_unstable_by_key(|(id, _)| *id);
        listed.into_iter().map(|(_, object)| object).collect()
    }

    /// Takes every task off: the scope closed or is being freed, so nothing may point at this
    /// list any more.
    pub(super) fn detach_all(&self) {
        if !self.used.load(Ordering::Relaxed) {
            return;
        }
        let mut children = self.children.lock();
        let first = children.first.take();
        for entry in first.into_iter().chain(children.rest.drain(..)) {
            // SAFETY: a listed task is alive (module docs).
            unsafe { &*entry.task }.set_home(ptr::null_mut());
        }
    }
}

impl Drop for ChildList {
    fn drop(&mut self) {
        self.detach_all();
    }
}

/// Takes `task` off the list it is in, if any: it ended, or `Scope.start` moves it.
pub(super) fn unlink(task: &Task) {
    let home = task.take_home();
    if home.is_null() {
        return;
    }
    // SAFETY: a task's `home` is cleared before its list dies (module docs).
    unsafe { &*home }.children.lock().remove(task);
}
