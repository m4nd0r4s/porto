//! [`IoSlab`]: the generation-tagged slots holding every registration's [`ScheduledIo`].

use std::io;
use std::sync::{Mutex, OnceLock, PoisonError};

use crate::driver::RESERVED_INDEX;
use crate::{Ready, RegistrationId, ScheduledIo};

/// Slots in the first page; each next page doubles.
const FIRST_PAGE_LEN: usize = 32;
const FIRST_PAGE_SHIFT: u32 = FIRST_PAGE_LEN.trailing_zeros();
/// Enough pages to address just under 2^24 slots, the index width of a `RegistrationId`.
const PAGES: usize = 19;
/// Slots across every page: 32 * (2^19 - 1).
const CAPACITY: usize = FIRST_PAGE_LEN * ((1 << PAGES) - 1);
const _: () = assert!(CAPACITY <= RESERVED_INDEX);

/// The slots of every live registration. Pages are allocated on demand and never freed, so a
/// `&ScheduledIo` stays valid as long as the slab and lookups take no lock. A released slot
/// moves to its next generation before it is reused, so stale IDs are ignored.
pub struct IoSlab {
    pages: [OnceLock<Box<[ScheduledIo]>>; PAGES],
    allocator: Mutex<Allocator>,
}

struct Allocator {
    /// Released slots, reused first.
    free: Vec<usize>,
    /// Slots below this index have been handed out at least once.
    next_unused: usize,
    /// At most this many slots (`CAPACITY`, or less in tests).
    limit: usize,
}

impl Default for IoSlab {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for IoSlab {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoSlab").finish_non_exhaustive()
    }
}

impl IoSlab {
    /// An empty slab. No page is allocated until the first registration.
    pub fn new() -> Self {
        Self::with_limit(CAPACITY)
    }

    fn with_limit(limit: usize) -> Self {
        Self {
            pages: std::array::from_fn(|_| OnceLock::new()),
            allocator: Mutex::new(Allocator {
                free: Vec::new(),
                next_unused: 0,
                limit,
            }),
        }
    }

    /// Takes a free slot and returns its ID, tagged with the slot's current generation.
    ///
    /// # Errors
    ///
    /// When every slot is taken.
    pub fn allocate(&self) -> io::Result<RegistrationId> {
        let index = {
            let mut allocator = self
                .allocator
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(index) = allocator.free.pop() {
                index
            } else if allocator.next_unused < allocator.limit {
                allocator.next_unused += 1;
                allocator.next_unused - 1
            } else {
                return Err(io::Error::other(format!(
                    "too many I/O registrations: the driver holds at most {} at once; close \
                     the sockets and files you no longer use",
                    allocator.limit
                )));
            }
        };
        let (page, offset) = locate(index);
        let entry = &self.pages[page].get_or_init(|| new_page(page))[offset];
        Ok(RegistrationId::new(index, entry.generation()))
    }

    /// The slot of a live registration, or `None` when `id` is stale or was never allocated.
    pub fn get(&self, id: RegistrationId) -> Option<&ScheduledIo> {
        self.slot(id)
            .filter(|io| io.generation() == id.generation())
    }

    /// Records readiness for `id` and wakes its waiters. Returns `false`, waking nobody, when
    /// `id` is stale: an event for a registration that has since been released.
    pub fn dispatch(&self, id: RegistrationId, ready: Ready) -> bool {
        self.slot(id).is_some_and(|io| io.dispatch(id, ready))
    }

    /// Releases `id`'s slot: its generation moves on, its waiters are woken and observe
    /// [`Ready::CLOSED`], and it becomes free for reuse. Returns `false` when `id` was stale.
    pub fn release(&self, id: RegistrationId) -> bool {
        if !self.slot(id).is_some_and(|io| io.release(id)) {
            return false;
        }
        self.allocator
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .free
            .push(id.index());
        true
    }

    /// The slot at `id`'s index, whatever its generation.
    fn slot(&self, id: RegistrationId) -> Option<&ScheduledIo> {
        if id.index() >= CAPACITY {
            return None;
        }
        let (page, offset) = locate(id.index());
        self.pages[page].get().map(|page| &page[offset])
    }
}

/// The page and the offset in it of slot `index`. Page `p` holds `32 << p` slots.
const fn locate(index: usize) -> (usize, usize) {
    let shifted = index + FIRST_PAGE_LEN;
    let page = (usize::BITS - 1 - shifted.leading_zeros() - FIRST_PAGE_SHIFT) as usize;
    (page, shifted - (FIRST_PAGE_LEN << page))
}

fn new_page(page: usize) -> Box<[ScheduledIo]> {
    std::iter::repeat_with(ScheduledIo::default)
        .take(FIRST_PAGE_LEN << page)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locate_maps_indices_onto_doubling_pages() {
        assert_eq!(locate(0), (0, 0));
        assert_eq!(locate(31), (0, 31));
        assert_eq!(locate(32), (1, 0));
        assert_eq!(locate(95), (1, 63));
        assert_eq!(locate(96), (2, 0));
        assert_eq!(
            locate(CAPACITY - 1),
            (PAGES - 1, (FIRST_PAGE_LEN << (PAGES - 1)) - 1)
        );
    }

    #[test]
    fn released_slot_is_reused_with_the_next_generation() {
        let slab = IoSlab::new();
        let first = slab.allocate().unwrap();
        assert!(slab.get(first).is_some());
        assert!(slab.release(first));
        assert!(slab.get(first).is_none(), "a released ID is stale");
        assert!(!slab.release(first), "releasing twice is refused");

        let second = slab.allocate().unwrap();
        assert_eq!(second.index(), first.index());
        assert_eq!(second.generation(), first.generation() + 1);
        assert!(slab.get(first).is_none());
        assert!(slab.get(second).is_some());
    }

    #[test]
    fn stale_dispatch_is_ignored() {
        let slab = IoSlab::new();
        let old = slab.allocate().unwrap();
        slab.release(old);
        let new = slab.allocate().unwrap();
        assert!(!slab.dispatch(old, Ready::READABLE));
        let io = slab.get(new).unwrap();
        assert!(
            io.ready_event(new, crate::Interest::READABLE)
                .ready()
                .is_empty()
        );
    }

    #[test]
    fn generation_wraps_around() {
        let slab = IoSlab::new();
        let mut id = slab.allocate().unwrap();
        for _ in 0..300 {
            slab.release(id);
            id = slab.allocate().unwrap();
        }
        assert_eq!(id.index(), 0);
        assert_eq!(id.generation(), 300 % 256);
        assert!(slab.dispatch(id, Ready::WRITABLE));
    }

    #[test]
    fn ids_beyond_the_slab_are_never_live() {
        let slab = IoSlab::new();
        let unallocated = RegistrationId::new(5_000, 0);
        assert!(slab.get(unallocated).is_none());
        assert!(!slab.dispatch(unallocated, Ready::READABLE));
        assert!(!slab.release(unallocated));
    }

    #[test]
    fn a_full_slab_says_what_to_do() {
        let slab = IoSlab::with_limit(2);
        slab.allocate().unwrap();
        let second = slab.allocate().unwrap();
        let error = slab.allocate().unwrap_err();
        assert!(error.to_string().contains("too many I/O registrations"));
        assert!(error.to_string().contains("close the sockets and files"));
        slab.release(second);
        slab.allocate().expect("a released slot is free again");
    }
}
