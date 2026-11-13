//! Porto's core: task header, scheduler and lanes, scopes and the cancel tree, timers, sync
//! primitives, the cooperative budget and the task registry.
//!
//! This crate MUST build and pass its tests without Python. See the
//! [architecture overview](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/10-architecture-overview.md).
//!
//! M0 spike: the task core ([`task`]), a single-lane executor ([`sched`]) and its timers
//! ([`time`]). M1 adds the wait queue the sync primitives build on ([`sync`]) and the runtime
//! configuration ([`config`]) and the cooperative budget ([`coop`]).
//!
//! ```
//! let rt = porto_core::Runtime::new();
//! let task = rt.spawn(async { porto_core::spawn(async { 1 + 1 }).await.unwrap() });
//! assert_eq!(rt.block_on(task).unwrap(), 2);
//! ```

// ADR-027 / spec 25 §8: a panic in a task must unwind to the task boundary, so the task fails
// with `porto.RustPanic` and the lane keeps running. `panic = "abort"` would turn every panic
// into a process abort: set `panic = "unwind"` in the profile (Cargo.toml) and drop `-C panic=abort`.
#[cfg(panic = "abort")]
compile_error!(
    "porto requires panic = \"unwind\" (ADR-027): a panic in a task is caught and becomes \
     porto.RustPanic; set `panic = \"unwind\"` in the Cargo profile and remove `-C panic=abort`"
);

pub mod blocking;
pub mod config;
pub mod coop;
#[cfg(all(test, loom))]
mod loom_support;
pub mod registry;
pub mod sched;
pub mod scope;
pub mod sync;
pub mod task;
pub mod time;

pub use blocking::{BlockingError, BlockingStats, WorkerHooks};
pub use registry::{LiveTask, SnapshotError, TaskKind, TaskRecord, TaskSnapshotRequest, TaskState};
pub use sched::{
    Builder, Handle, ParkHook, Runtime, WeakHandle, forget_lane_in_forked_child, spawn,
    spawn_blocking,
};
