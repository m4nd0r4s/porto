//! The task header: the type-erased prefix every task allocation starts with.

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use super::raw::Vtable;
use super::state::State;

/// The fixed prefix of every task (`11-task-model-and-trap-protocol.md` §1).
///
/// The layout is the spec's, including the fields M0 does not use yet, so the size assertion
/// below measures the real budget (ADR-034: at most 64 bytes, registry node included).
#[repr(C)]
#[allow(
    dead_code,
    reason = "owner_lane, flags, scope, queue_next and registry are reserved for M1 (lanes, \
              Python task flags, scopes, the injector, the registry)"
)]
pub struct Header {
    /// Lifecycle bits and refcount, see [`super::state`].
    pub(crate) state: State,
    /// The last lane that ran the task (soft affinity).
    pub(crate) owner_lane: AtomicU16,
    /// PIN, PYTHON, DETACHED_DROP, ...
    pub(crate) flags: u16,
    /// Type-specific operations: poll, schedule, dealloc.
    pub(crate) vtable: &'static Vtable,
    /// Monotonically increasing, never reused.
    pub(crate) id: u64,
    /// The spawning scope.
    pub(crate) scope: *const (),
    /// Intrusive run-queue link for the injector.
    pub(crate) queue_next: UnsafeCell<*const Header>,
    /// Intrusive node in the task registry (introspection).
    pub(crate) registry: RegistryNode,
}

/// A node in the intrusive, doubly linked list of live tasks.
#[repr(C)]
#[allow(dead_code, reason = "the task registry arrives in M1")]
pub struct RegistryNode {
    prev: UnsafeCell<*const Header>,
    next: UnsafeCell<*const Header>,
}

const _: () = assert!(
    size_of::<Header>() <= 64,
    "ADR-034: the task header must fit in 64 bytes"
);

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

impl Header {
    /// A header for a task that is about to be queued for the first time.
    pub(crate) fn new(vtable: &'static Vtable) -> Self {
        Self {
            state: State::new_scheduled(),
            owner_lane: AtomicU16::new(0),
            flags: 0,
            vtable,
            // Relaxed: uniqueness only needs the RMW's atomicity, not ordering. A u64 cannot
            // wrap in practice (centuries at a billion spawns per second).
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            scope: ptr::null(),
            queue_next: UnsafeCell::new(ptr::null()),
            registry: RegistryNode {
                prev: UnsafeCell::new(ptr::null()),
                next: UnsafeCell::new(ptr::null()),
            },
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_fits_the_adr_034_budget() {
        println!("size_of::<Header>() = {}", size_of::<Header>());
        assert!(size_of::<Header>() <= 64);
        assert_eq!(align_of::<Header>(), 8);
    }
}
