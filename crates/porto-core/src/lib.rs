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

pub mod config;
pub mod coop;
pub mod sched;
pub mod scope;
pub mod sync;
pub mod task;
pub mod time;

pub use sched::{Builder, Handle, ParkHook, Runtime, WeakHandle, spawn};
