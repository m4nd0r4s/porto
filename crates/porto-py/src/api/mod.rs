//! The functions and classes `porto._core` exposes to the `porto` package: the public `run`,
//! `scope`, `sleep`, `checkpoint`, `current_time` and the sync primitives, the configuration's resolution, plus private (underscore) entry points for tests.

pub(crate) mod channel;
pub(crate) mod channel_op;
pub(crate) mod config;
pub(crate) mod run;
pub(crate) mod scope;
mod scope_deadline;
mod scope_exit;
mod started;
pub(crate) mod sync;
pub(crate) mod task;
pub(crate) mod time;
