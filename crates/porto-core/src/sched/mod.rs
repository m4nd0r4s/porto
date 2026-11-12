//! The scheduler (`12-scheduler-and-lanes.md`). M1 runs a single lane: lane 0, the thread that
//! calls [`Runtime::block_on`]. The loop (`lane`) and the shared state (`shared`) are already
//! split the way M2's lanes need: a lane owns its local queue and tick, everything another
//! thread reaches (the injector, each lane's parking slot and deferred work, the timers and
//! the driver) is in `Shared`.
//!
//! The loom model of the parking protocol runs with
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-core --release sched`.

mod context;
mod inject;
mod lane;
mod park;
mod runtime;
mod shared;
mod timers;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, not(loom)))]
mod loop_tests;

#[cfg(all(test, loom))]
mod loom_tests;

use std::future::Future;

use crate::task::JoinHandle;

pub(crate) use context::{current_handle, current_time, with_current_handle};
#[cfg(all(test, loom))]
pub(crate) use inject::Inject;
pub use park::ParkHook;
pub use runtime::{Builder, Runtime};
pub use shared::{DeferredWork, Handle, WeakHandle};
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
    let Some(handle) = context::current_handle() else {
        panic!(
            "porto_core::spawn called outside a runtime: call it from a task or inside \
             Runtime::block_on, or use Handle::spawn from other threads"
        );
    };
    handle.spawn(future)
}

/// Runs `job` on the blocking pool of the runtime running on this thread, and returns a handle
/// for its result: see [`Handle::spawn_blocking`].
///
/// # Panics
/// If no runtime is running on this thread, or if the OS cannot start a pool thread. Call it
/// from a task or from inside [`Runtime::block_on`], or use [`Handle::spawn_blocking`] from
/// other threads.
pub fn spawn_blocking<F, R>(job: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let Some(handle) = context::current_handle() else {
        panic!(
            "porto_core::spawn_blocking called outside a runtime: call it from a task or inside \
             Runtime::block_on, or use Handle::spawn_blocking from other threads"
        );
    };
    handle.spawn_blocking(job)
}
