//! The task header: the type-erased prefix every task allocation starts with.

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use super::flags::TaskFlags;
use super::raw::TaskVTable;
use super::state::State;
use crate::registry::RegistryNode;
use crate::scope::TaskScope;

/// The fixed prefix of every task (`11-task-model-and-trap-protocol.md` §1).
///
/// `repr(C)` keeps the field order, and the order packs the two 16-bit fields into the padding
/// after the 32-bit state word: 56 bytes on 64-bit targets, inside the ADR-034 budget of one
/// cache line (64 bytes, registry node included). The fields M1 does not use yet are present so
/// the size assertion measures the real layout.
#[repr(C)]
pub struct Header {
    /// Lifecycle bits and refcount, see [`super::state`].
    pub(crate) state: State,
    /// The last lane that ran the task (soft affinity).
    pub(crate) owner_lane: AtomicU16,
    /// Fixed at spawn: PIN, PYTHON, DETACHED_DROP.
    pub(crate) flags: TaskFlags,
    /// Type-specific operations: poll, drop, dealloc, schedule, type name.
    pub(crate) vtable: &'static TaskVTable,
    /// Monotonically increasing, never reused.
    pub(crate) id: u64,
    /// The spawning scope, `None` for a task spawned outside one. The slot owns the task's
    /// counted reference to the scope, set at spawn and taken exactly once when the task ends:
    /// by the lane that completes it (holding RUNNING), or by `dealloc` if it never completed.
    /// Nothing else touches it.
    scope: UnsafeCell<Option<TaskScope>>,
    /// Intrusive link of a queue the task is in: today the registry's list of tasks freed off
    /// their lane (`crate::registry`), which only holds a task no run queue owns; the intrusive
    /// injector (M2) shares it for the same reason.
    queue_next: UnsafeCell<*mut Header>,
    /// Intrusive node in the per-lane task registry (introspection), see `crate::registry`.
    pub(crate) registry: RegistryNode,
}

const _: () = assert!(
    size_of::<Option<TaskScope>>() == size_of::<usize>(),
    "the header's scope slot is one thin pointer"
);

const _: () = assert!(
    size_of::<Header>() <= 64,
    "ADR-034: the task header must fit in 64 bytes (one cache line)"
);

/// The next task id. Starts at 1 so 0 can mean "no task" in diagnostics.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Hands out a fresh task id: unique for the process lifetime and increasing in call order.
fn next_id() -> u64 {
    // Relaxed: uniqueness only needs the RMW's atomicity, not ordering. A u64 cannot wrap in
    // practice (centuries at a billion spawns per second).
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

impl Header {
    /// A header for a task that is about to be queued for the first time, with the queue's
    /// reference, plus a join handle's if `join_handle`.
    /// `scope` is the scope the task is spawned into, already counting it as a child.
    pub(crate) fn new(
        vtable: &'static TaskVTable,
        flags: TaskFlags,
        join_handle: bool,
        scope: Option<TaskScope>,
    ) -> Self {
        Self {
            state: State::new_scheduled(join_handle),
            owner_lane: AtomicU16::new(0),
            flags,
            vtable,
            id: next_id(),
            scope: UnsafeCell::new(scope),
            queue_next: UnsafeCell::new(ptr::null_mut()),
            registry: RegistryNode::unlinked(),
        }
    }

    /// The registry's link for a task freed off its lane.
    ///
    /// # Safety
    /// The caller holds the last reference (no queue owns the task), and writes before
    /// publishing the task to the registry's list.
    pub(crate) unsafe fn set_queue_next(&self, next: *mut Header) {
        // SAFETY: the caller's contract.
        unsafe { *self.queue_next.get() = next };
    }

    /// The registry's link, see [`Header::set_queue_next`].
    ///
    /// # Safety
    /// The task was published to the registry's list by `set_queue_next`'s caller.
    pub(crate) unsafe fn queue_next(&self) -> *mut Header {
        // SAFETY: the caller's contract.
        unsafe { *self.queue_next.get() }
    }

    /// The task's id: unique for the process lifetime and increasing in spawn order.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The lane that last ran the task (soft affinity): 0 until lanes record themselves here
    /// (M2).
    pub fn owner_lane(&self) -> u16 {
        self.owner_lane.load(Ordering::Relaxed)
    }

    /// The task's state word.
    pub fn state(&self) -> &State {
        &self.state
    }

    /// The flags the task was spawned with.
    pub fn flags(&self) -> TaskFlags {
        self.flags
    }

    /// The type name of the task's future, for introspection and debugging.
    pub fn type_name(&self) -> &'static str {
        (self.vtable.type_name)()
    }

    /// Whether the task still holds its scope: spawned into one and not ended yet.
    ///
    /// # Safety
    /// As for [`Header::take_scope`].
    #[inline]
    pub(crate) unsafe fn has_scope(&self) -> bool {
        // SAFETY: the caller's contract: no one writes the slot concurrently.
        unsafe { (*self.scope.get()).is_some() }
    }

    /// Whether the task counts in `scope` now (see [`TaskScope::is_for`]).
    ///
    /// # Safety
    /// The caller holds RUNNING (it is polling the task), so no other thread writes the slot.
    pub unsafe fn counts_in<E>(&self, scope: &crate::scope::ScopeRef<E>) -> bool {
        // SAFETY: the caller's contract.
        unsafe { (*self.scope.get()).as_ref() }.is_some_and(|task_scope| task_scope.is_for(scope))
    }

    /// Makes the task count in `scope` instead of the one it counts in: for a task moved to
    /// another scope while it runs (`Scope.start`, 15 §3). Returns the old reference, which the
    /// caller ends or drops outside any scope lock.
    ///
    /// # Safety
    /// The caller holds RUNNING (it is polling the task): the slot is otherwise only touched by
    /// whoever completes the task, which is the same thread afterwards.
    pub unsafe fn replace_scope(&self, scope: TaskScope) -> Option<TaskScope> {
        // SAFETY: the caller's contract.
        unsafe { (*self.scope.get()).replace(scope) }
    }

    /// The id of the scope the task counts in, if any.
    ///
    /// # Safety
    /// No other thread writes the slot: the caller holds RUNNING, is the lane thread of a
    /// single-lane runtime reading a task that is not running, or otherwise knows the task does
    /// not end or move meanwhile (see [`crate::registry::LiveTask`]).
    pub(crate) unsafe fn scope_id(&self) -> Option<u64> {
        // SAFETY: the caller's contract.
        unsafe { (*self.scope.get()).as_ref() }.map(TaskScope::scope_id)
    }

    /// Takes the task's scope reference out, leaving `None`: the task is ending. A task outside
    /// any scope pays this one null check.
    ///
    /// # Safety
    /// The caller holds RUNNING and is completing the task, or holds the last reference (in
    /// `dealloc`): either way, no other thread can reach the slot.
    #[inline]
    pub(crate) unsafe fn take_scope(&self) -> Option<TaskScope> {
        let slot = self.scope.get();
        // SAFETY: the caller's contract. Checked first so a task without a scope writes
        // nothing to the header.
        unsafe {
            if (*slot).is_some() {
                (*slot).take()
            } else {
                None
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::collections::HashSet;
    use std::thread;

    use super::*;

    #[test]
    fn header_fits_the_adr_034_budget() {
        println!("size_of::<Header>() = {}", size_of::<Header>());
        assert!(size_of::<Header>() <= 64);
        assert_eq!(align_of::<Header>(), 8);
        #[cfg(target_pointer_width = "64")]
        assert_eq!(
            size_of::<Header>(),
            56,
            "a field order change wasted padding"
        );
    }

    #[test]
    fn ids_are_strictly_increasing_on_one_thread() {
        let ids: Vec<u64> = (0..1000).map(|_| next_id()).collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(ids[0] >= 1, "0 is never a task id");
    }

    #[test]
    fn ids_are_never_reused_across_threads() {
        let per_thread = if cfg!(miri) { 50 } else { 10_000 };
        let threads: Vec<_> = (0..4)
            .map(|_| thread::spawn(move || (0..per_thread).map(|_| next_id()).collect::<Vec<_>>()))
            .collect();
        let mut seen = HashSet::new();
        for handle in threads {
            let ids = handle.join().unwrap();
            assert!(
                ids.windows(2).all(|pair| pair[0] < pair[1]),
                "per-thread order"
            );
            for id in ids {
                assert!(seen.insert(id), "id {id} handed out twice");
            }
        }
    }
}
