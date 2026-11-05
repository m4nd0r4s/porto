//! The runtime's configuration (`20-python-api-reference.md` RuntimeConfig, `26` §1): the same
//! fields as Python's `porto.RuntimeConfig`, built and validated by a [`ConfigBuilder`].
//!
//! ```
//! use std::time::Duration;
//! use porto_core::config::{Lanes, RuntimeConfig, Steal};
//!
//! let config = RuntimeConfig::builder()
//!     .lanes(Lanes::Auto)
//!     .steal(Steal::Eager)
//!     .graceful_timeout(Duration::from_secs(30))
//!     .build()
//!     .unwrap();
//! assert_eq!(config.coop_budget(), 128);
//! assert!(config.lanes().resolve_with_cores(false, 8) == 8);
//! ```
//!
//! The Python package reads the `PORTO_*` environment variables and gives them their
//! precedence (`26` §1); this module only holds and checks the values.

mod builder;
mod error;
mod lanes;
mod runtime_config;
mod values;

#[cfg(all(test, not(loom)))]
mod tests;

pub use builder::{ConfigBuilder, MAX_TIMER_RESOLUTION, MIN_TIMER_RESOLUTION};
pub use error::ConfigError;
pub use lanes::{Lanes, MAX_AUTO_LANES, physical_cores};
pub use runtime_config::{DeterministicConfig, RuntimeConfig};
pub use values::{AnyioBackend, DeterministicClock, IoBackend, PythonSteal, Steal, UnknownName};
