//! Porto, an async runtime: the facade crate for pure Rust users.
//!
//! It re-exports the runtime crates behind one API. See
//! [Rust API & the C ABI](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/21-rust-api-and-abi.md),
//! Part A.

/// The core runtime. Its items move to the top level of this crate as they land.
pub use porto_core;

/// Spawning and joining tasks (`21-rust-api-and-abi.md` Part A, `porto::task`).
pub mod task {
    pub use porto_core::task::{JoinError, JoinHandle};
    pub use porto_core::{spawn, spawn_blocking};
}
