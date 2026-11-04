//! The scheduler. M0 has a single lane (`12-scheduler-and-lanes.md`, single-lane parts).

mod lane;
mod shared;

#[cfg(test)]
mod tests;

use std::future::Future;

pub use lane::Runtime;
pub use shared::Handle;

/// Spawns `future` onto the runtime running on this thread.
///
/// # Panics
/// If no runtime is running on this thread. Call it from a task or from inside
/// [`Runtime::block_on`], or use [`Handle::spawn`] from other threads.
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let Some(handle) = lane::current_handle() else {
        panic!(
            "porto_core::spawn called outside a runtime: call it from a task or inside \
             Runtime::block_on, or use Handle::spawn from other threads"
        );
    };
    handle.spawn(future);
}
