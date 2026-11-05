//! The functions and classes `porto._core` exposes to the `porto` package: the public `run`,
//! `scope`, `sleep`, `checkpoint` and `current_time`, plus private (underscore) entry points for tests.

pub(crate) mod run;
pub(crate) mod scope;
pub(crate) mod time;
