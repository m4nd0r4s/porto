//! [`ConfigError`]: a configuration value the runtime cannot use, and how to fix it.

use std::fmt;

/// A configuration value out of range. The message names the field, the value and what to set
/// instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    field: &'static str,
    message: String,
}

impl ConfigError {
    pub(super) fn new(field: &'static str, message: String) -> Self {
        Self { field, message }
    }

    /// The name of the field, as in Python's `RuntimeConfig` (`"lanes"`, `"coop_budget"`, ...).
    pub fn field(&self) -> &'static str {
        self.field
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "porto: invalid {}: {}", self.field, self.message)
    }
}

impl std::error::Error for ConfigError {}
