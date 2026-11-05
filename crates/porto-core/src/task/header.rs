//! The task header: the type-erased prefix every task allocation starts with.

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use super::flags::TaskFlags;
use super::raw::TaskVTable;
use super::state::State;

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
    #[allow(
        dead_code,
        reason = "lanes record themselves here once there are several (M2)"
    )]
    pub(crate) owner_lane: AtomicU16,
    /// Fixed at spawn: PIN, PYTHON, DETACHED_DROP.
    pub(crate) flags: TaskFlags,
    /// Type-specific operations: poll, drop, dealloc, schedule, type name.
    pub(crate) vtable: &'static TaskVTable,
    /// Monotonically increasing, never reused.
    pub(crate) id: u64,
    /// The spawning scope. Null until scopes exist; the task body holds the strong reference.
    #[allow(dead_code, reason = "set once the scope tree lands (porto-szb.35)")]
    pub(crate) scope: *const ScopeNode,
    /// Intrusive run-queue link for the injector.
    #[allow(dead_code, reason = "used by the intrusive injector queue (M2)")]
    pub(crate) queue_next: UnsafeCell<*const Header>,
    /// Intrusive node in the per-lane task registry (introspection).
    #[allow(dead_code, reason = "linked by the task registry (porto-szb.24)")]
    pub(crate) registry: RegistryNode,
}

/// The scope a task was spawned in, as seen from the header: opaque here.
///
/// A placeholder so the header has its final type and size; the scope tree (porto-szb.35)
/// replaces it with the real scope node.
#[repr(C)]
pub struct ScopeNode {
    _opaque: [u8; 0],
}

/// A node in the intrusive, doubly linked list of live tasks.
///
/// A placeholder with the final size; the task registry (porto-szb.24) owns the linking logic.
#[repr(C)]
pub struct RegistryNode {
    #[allow(dead_code, reason = "linked by the task registry (porto-szb.24)")]
    prev: UnsafeCell<*const Header>,
    #[allow(dead_code, reason = "linked by the task registry (porto-szb.24)")]
    next: UnsafeCell<*const Header>,
}

impl RegistryNode {
    const fn unlinked() -> Self {
        Self {
            prev: UnsafeCell::new(ptr::null()),
            next: UnsafeCell::new(ptr::null()),
        }
    }
}

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
    /// A header for a task that is about to be queued for the first time.
    pub(crate) fn new(vtable: &'static TaskVTable, flags: TaskFlags) -> Self {
        Self {
            state: State::new_scheduled(),
            owner_lane: AtomicU16::new(0),
            flags,
            vtable,
            id: next_id(),
            scope: ptr::null(),
            queue_next: UnsafeCell::new(ptr::null()),
            registry: RegistryNode::unlinked(),
        }
    }

    /// The task's id: unique for the process lifetime and increasing in spawn order.
    pub fn id(&self) -> u64 {
        self.id
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
}

#[cfg(test)]
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
