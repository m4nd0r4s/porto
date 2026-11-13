//! The step driver: Python coroutines as porto tasks (11-task-model-and-trap-protocol.md).
//!
//! - `trap` and `ready`: the awaitables a coroutine hands to the driver (§5-§6);
//! - `pending`: what a task waits for, packed in one word (§5.2); `thin`: the one-word handles
//!   it carries;
//! - `pytask`: the task body and how a coroutine is spawned (§2); `wait`: its one wait at a
//!   time, resolved or given up (§5.3 T3);
//! - `step`: `PyTask::poll`, the step loop (§4), and `checkpoint_if_needed` (T2);
//! - `lane`: the Python lane that runs them, attached except while parked;
//! - `freelist`: the lock-free freelist of the hot awaitables on GIL builds; `fast_new`: making
//!   them over it without pyo3's generic path.

pub(crate) mod fast_new;
pub(crate) mod freelist;
pub(crate) mod lane;
pub(crate) mod pending;
pub(crate) mod pytask;
pub mod ready;
pub(crate) mod startup;
pub(crate) mod step;
pub(crate) mod thin;
pub mod trap;
mod wait;

#[cfg(test)]
mod future_trap_tests;
