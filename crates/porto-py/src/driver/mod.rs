//! The step driver: Python coroutines as porto tasks (11-task-model-and-trap-protocol.md).
//!
//! - `trap` and `ready`: the awaitables a coroutine hands to the driver (§5-§6);
//! - `pytask`: the task body and how a coroutine is spawned (§2);
//! - `step`: `PyTask::poll`, the step loop (§4);
//! - `lane`: the Python lane that runs them, attached except while parked.

pub(crate) mod lane;
pub(crate) mod pytask;
pub mod ready;
mod step;
pub mod trap;
