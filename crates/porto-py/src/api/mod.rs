//! The functions `porto._core` exposes to the `porto` package. M0 has only private (underscore)
//! entry points; the public `porto.run`, `porto.sleep` and `porto.checkpoint` build on them.

pub(crate) mod run;
pub(crate) mod time;
