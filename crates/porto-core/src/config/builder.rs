//! [`ConfigBuilder`]: sets [`RuntimeConfig`]'s fields and checks them in `build`.

use std::time::Duration;

use super::error::ConfigError;
use super::lanes::Lanes;
use super::runtime_config::{DeterministicConfig, RuntimeConfig};
use super::values::{AnyioBackend, IoBackend, PythonSteal, Steal};

/// The finest `timer_resolution` (`14-time-and-timers.md` §2), as the scheduler's timers
/// accept it.
pub const MIN_TIMER_RESOLUTION: Duration = Duration::from_micros(100);
/// The coarsest `timer_resolution`.
pub const MAX_TIMER_RESOLUTION: Duration = Duration::from_millis(10);

/// Builds a [`RuntimeConfig`]: `RuntimeConfig::builder().lanes(Lanes::Auto).build()?`.
/// Unset fields keep their defaults; [`ConfigBuilder::build`] checks the values.
#[derive(Debug, Clone, Default)]
#[must_use = "a builder does nothing until `build` is called"]
pub struct ConfigBuilder {
    config: RuntimeConfig,
}

/// A setter for a field that needs no conversion.
macro_rules! setter {
    ($(#[$meta:meta])* $field:ident: $ty:ty) => {
        $(#[$meta])*
        pub fn $field(mut self, $field: $ty) -> Self {
            self.config.$field = $field;
            self
        }
    };
}

impl ConfigBuilder {
    setter!(
        /// How many lanes run tasks: at least 1, or [`Lanes::Auto`].
        lanes: Lanes
    );
    setter!(
        /// The I/O backend.
        io_backend: IoBackend
    );
    setter!(
        /// Whether an unavailable `io_backend` is an error.
        io_backend_strict: bool
    );
    setter!(
        /// How idle lanes steal work.
        steal: Steal
    );
    setter!(
        /// Whether Python tasks move between lanes.
        python_steal: PythonSteal
    );
    setter!(
        /// The queue length past which an adaptive lane gives Python tasks away: at least 1.
        python_steal_threshold: u32
    );
    setter!(
        /// Operations per task step before it must yield: at least 1.
        coop_budget: u32
    );
    setter!(
        /// The most blocking-pool threads: at least 1.
        blocking_threads: usize
    );
    setter!(
        /// How long an idle blocking-pool thread lives.
        blocking_keepalive: Duration
    );
    setter!(
        /// The timer wheel's tick: between 100 µs and 10 ms.
        timer_resolution: Duration
    );
    setter!(
        /// Whether the asyncio facade is installed.
        asyncio_compat: bool
    );
    setter!(
        /// How AnyIO runs on porto.
        anyio_backend: AnyioBackend
    );
    setter!(
        /// Whether the runtime handles SIGINT and SIGTERM.
        handle_signals: bool
    );
    setter!(
        /// Debug mode.
        debug: bool
    );
    setter!(
        /// The slow-step report's threshold, above zero; `None` turns the report off.
        slow_step_threshold: Option<Duration>
    );
    setter!(
        /// Deterministic mode's settings; `None` turns it off.
        deterministic: Option<DeterministicConfig>
    );
    setter!(
        /// Whether the runtime keeps metrics.
        metrics: bool
    );
    setter!(
        /// How long a shutdown waits for main to finish on its own.
        graceful_timeout: Duration
    );
    setter!(
        /// How long a shutdown waits for cancelled tasks.
        cancel_timeout: Duration
    );

    /// The `host:port` the console listens on (`"127.0.0.1:6669"`); `None` for no console.
    pub fn console(mut self, console: Option<impl Into<String>>) -> Self {
        self.config.console = console.map(Into::into);
        self
    }

    /// Checks the values and returns the configuration.
    ///
    /// # Errors
    /// A [`ConfigError`] naming the first field out of range and what to set instead.
    pub fn build(self) -> Result<RuntimeConfig, ConfigError> {
        let config = self.config;
        if config.lanes == Lanes::Fixed(0) {
            return Err(ConfigError::new(
                "lanes",
                "0 lanes cannot run anything; use Lanes::Fixed(1) or more, or Lanes::Auto"
                    .to_owned(),
            ));
        }
        at_least_one("python_steal_threshold", config.python_steal_threshold == 0)?;
        at_least_one("coop_budget", config.coop_budget == 0)?;
        at_least_one("blocking_threads", config.blocking_threads == 0)?;
        let resolution = config.timer_resolution;
        if !(MIN_TIMER_RESOLUTION..=MAX_TIMER_RESOLUTION).contains(&resolution) {
            return Err(ConfigError::new(
                "timer_resolution",
                format!(
                    "{resolution:?} is outside 100µs..=10ms; use a resolution in that range, \
                     or leave it unset for the 1ms default"
                ),
            ));
        }
        if config.slow_step_threshold == Some(Duration::ZERO) {
            return Err(ConfigError::new(
                "slow_step_threshold",
                "0 would report every step; use a positive duration, or None to turn the \
                 report off"
                    .to_owned(),
            ));
        }
        if let Some(console) = &config.console {
            check_console(console)?;
        }
        if let Some(deterministic) = &config.deterministic
            && deterministic.max_steps == Some(0)
        {
            return Err(ConfigError::new(
                "deterministic",
                "max_steps = Some(0) stops before the first step; use a positive limit, or \
                 None for no limit"
                    .to_owned(),
            ));
        }
        Ok(config)
    }
}

/// Refuses a count that `is_zero`.
fn at_least_one(field: &'static str, is_zero: bool) -> Result<(), ConfigError> {
    if is_zero {
        return Err(ConfigError::new(
            field,
            format!("0 is too small; use 1 or more, or leave {field} unset for its default"),
        ));
    }
    Ok(())
}

/// Checks that `console` is `host:port`, with a non-empty host and a port number.
fn check_console(console: &str) -> Result<(), ConfigError> {
    let valid = console
        .rsplit_once(':')
        .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
    if valid {
        return Ok(());
    }
    Err(ConfigError::new(
        "console",
        format!("{console:?} is not host:port; use an address such as \"127.0.0.1:6669\""),
    ))
}
