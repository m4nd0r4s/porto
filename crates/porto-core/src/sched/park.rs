//! The park hook: lets an embedder run code around every park of a lane thread.
//!
//! A lane parks when it has nothing to run (until a wake, or its earliest timer). An embedder
//! that holds a per-thread resource while tasks run can release it for the duration of the park
//! and take it back afterwards. The Python bindings detach the thread state, so other Python
//! threads and the free-threaded stop-the-world GC can proceed (`25-cpython-integration-and-
//! memory.md` P2); the core itself knows nothing about what the hook does.

/// Wraps every park of a lane thread. Install it with [`Runtime::with_park_hook`].
///
/// [`Runtime::with_park_hook`]: super::Runtime::with_park_hook
pub trait ParkHook: Send + 'static {
    /// Called on the lane thread instead of parking it. MUST call `park` once: it blocks until
    /// the lane is unparked or its earliest timer is due. If it returns without calling `park`,
    /// the lane does not block and checks its queues again.
    fn around_park(&self, park: &mut dyn FnMut());
}
