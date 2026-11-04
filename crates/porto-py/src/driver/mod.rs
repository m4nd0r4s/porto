//! The step driver: Python coroutines as porto tasks (11-task-model-and-trap-protocol.md).
//!
//! - `trap` and `ready`: the awaitables a coroutine hands to the driver (§5-§6).

pub mod ready;
pub mod trap;
