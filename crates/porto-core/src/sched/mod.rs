//! The scheduler. M0 has a single lane (`12-scheduler-and-lanes.md`, single-lane parts).

mod lane;
mod park;
mod shared;
mod timers;

#[cfg(all(test, not(loom)))]
mod tests;

use std::future::Future;

use crate::task::JoinHandle;

pub use lane::{Builder, Runtime};
pub(crate) use lane::{current_handle, current_time, with_current_handle};
pub use park::ParkHook;
pub use shared::Handle;
pub(crate) use timers::Armed;

/// Spawns `future` onto the runtime running on this thread. Await the [`JoinHandle`] for the
/// output, or drop it to detach the task.
///
/// # Panics
/// If no runtime is running on this thread. Call it from a task or from inside
/// [`Runtime::block_on`], or use [`Handle::spawn`] from other threads.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let Some(handle) = lane::current_handle() else {
        panic!(
            "porto_core::spawn called outside a runtime: call it from a task or inside \
             Runtime::block_on, or use Handle::spawn from other threads"
        );
    };
    handle.spawn(future)
}
