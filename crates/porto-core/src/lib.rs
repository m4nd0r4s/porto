//! Porto's core: task header, scheduler and lanes, scopes and the cancel tree, timers, sync
//! primitives, the cooperative budget and the task registry.
//!
//! This crate MUST build and pass its tests without Python. See the
//! [architecture overview](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/10-architecture-overview.md).
//!
//! M0 spike: the task core ([`task`]) and a single-lane executor ([`sched`]).
//!
//! ```
//! let rt = porto_core::Runtime::new();
//! rt.spawn(async { porto_core::spawn(async {}) });
//! assert_eq!(rt.block_on(async { 1 + 1 }), 2);
//! ```

pub mod sched;
pub mod task;

pub use sched::{Handle, Runtime, spawn};
