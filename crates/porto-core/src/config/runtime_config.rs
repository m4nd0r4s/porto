//! [`RuntimeConfig`] and [`DeterministicConfig`]: the validated values, read through getters.

use std::time::Duration;

use super::builder::ConfigBuilder;
use super::lanes::Lanes;
use super::values::{AnyioBackend, DeterministicClock, IoBackend, PythonSteal, Steal};

/// The runtime's configuration: every field of Python's `porto.RuntimeConfig`, with the same
/// defaults (`20-python-api-reference.md`). Build one with [`RuntimeConfig::builder`]; the
/// builder checks the values, so a `RuntimeConfig` is always valid.
///
/// Durations are Python's float seconds. The fields about Python (`python_steal`,
/// `asyncio_compat`, `anyio_backend`, ...) are carried for porto-py and ignored by a pure-Rust
/// runtime.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeConfig {
    pub(super) lanes: Lanes,
    pub(super) io_backend: IoBackend,
    pub(super) io_backend_strict: bool,
    pub(super) steal: Steal,
    pub(super) python_steal: PythonSteal,
    pub(super) python_steal_threshold: u32,
    pub(super) coop_budget: u32,
    pub(super) blocking_threads: usize,
    pub(super) blocking_keepalive: Duration,
    pub(super) timer_resolution: Duration,
    pub(super) asyncio_compat: bool,
    pub(super) anyio_backend: AnyioBackend,
    pub(super) handle_signals: bool,
    pub(super) debug: bool,
    pub(super) slow_step_threshold: Option<Duration>,
    pub(super) console: Option<String>,
    pub(super) metrics: bool,
    pub(super) deterministic: Option<DeterministicConfig>,
    pub(super) graceful_timeout: Duration,
    pub(super) cancel_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            lanes: Lanes::default(),
            io_backend: IoBackend::Auto,
            io_backend_strict: false,
            steal: Steal::Adaptive,
            python_steal: PythonSteal::Adaptive,
            python_steal_threshold: 8,
            coop_budget: 128,
            blocking_threads: 512,
            blocking_keepalive: Duration::from_secs(10),
            timer_resolution: Duration::from_millis(1),
            asyncio_compat: true,
            anyio_backend: AnyioBackend::Native,
            handle_signals: true,
            debug: false,
            slow_step_threshold: Some(Duration::from_millis(100)),
            console: None,
            metrics: true,
            deterministic: None,
            graceful_timeout: Duration::from_secs(10),
            cancel_timeout: Duration::from_secs(5),
        }
    }
}

impl RuntimeConfig {
    /// A builder starting from the defaults.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// How many lanes run tasks (`12-scheduler-and-lanes.md` §7).
    pub fn lanes(&self) -> Lanes {
        self.lanes
    }

    /// The I/O backend to use, or [`IoBackend::Auto`] to probe.
    pub fn io_backend(&self) -> IoBackend {
        self.io_backend
    }

    /// Whether an unavailable `io_backend` is an error instead of falling back to the probe.
    pub fn io_backend_strict(&self) -> bool {
        self.io_backend_strict
    }

    /// `io_backend` and `io_backend_strict` as the I/O driver takes them
    /// ([`MioDriver::open`](porto_io::MioDriver::open)).
    pub fn io_backend_config(&self) -> porto_io::IoBackendConfig {
        let backend = match self.io_backend {
            IoBackend::Auto => porto_io::IoBackend::Auto,
            IoBackend::Epoll => porto_io::IoBackend::Epoll,
            IoBackend::Kqueue => porto_io::IoBackend::Kqueue,
            IoBackend::Iocp => porto_io::IoBackend::Iocp,
            IoBackend::IoUring => porto_io::IoBackend::IoUring,
        };
        porto_io::IoBackendConfig {
            backend,
            strict: self.io_backend_strict,
        }
    }

    /// How idle lanes steal work.
    pub fn steal(&self) -> Steal {
        self.steal
    }

    /// Whether Python tasks move between lanes.
    pub fn python_steal(&self) -> PythonSteal {
        self.python_steal
    }

    /// The queue length past which an adaptive lane gives Python tasks away.
    pub fn python_steal_threshold(&self) -> u32 {
        self.python_steal_threshold
    }

    /// How many operations a task runs before it must yield (`12-scheduler-and-lanes.md` §10).
    pub fn coop_budget(&self) -> u32 {
        self.coop_budget
    }

    /// The most threads the blocking pool runs (`19-threads-and-blocking.md`).
    pub fn blocking_threads(&self) -> usize {
        self.blocking_threads
    }

    /// How long an idle blocking-pool thread waits for work before it exits.
    pub fn blocking_keepalive(&self) -> Duration {
        self.blocking_keepalive
    }

    /// The timer wheel's tick (`14-time-and-timers.md` §2).
    pub fn timer_resolution(&self) -> Duration {
        self.timer_resolution
    }

    /// Whether the asyncio facade is installed (`22-asyncio-anyio-compat.md`).
    pub fn asyncio_compat(&self) -> bool {
        self.asyncio_compat
    }

    /// How AnyIO runs on porto.
    pub fn anyio_backend(&self) -> AnyioBackend {
        self.anyio_backend
    }

    /// Whether the runtime handles SIGINT and SIGTERM (only on the main thread).
    pub fn handle_signals(&self) -> bool {
        self.handle_signals
    }

    /// Debug mode: origins of coroutines and spawns, never-awaited traps.
    pub fn debug(&self) -> bool {
        self.debug
    }

    /// A task step longer than this is reported as slow; `None` turns the report off.
    pub fn slow_step_threshold(&self) -> Option<Duration> {
        self.slow_step_threshold
    }

    /// Whether the runtime's tasks record their timing (`23-observability.md` §1): on in debug
    /// mode and with a console, the two consumers of it until `porto.inspect` (M5).
    pub fn introspection(&self) -> bool {
        self.debug || self.console.is_some()
    }

    /// The `host:port` the console listens on, if any (`23-observability.md`).
    pub fn console(&self) -> Option<&str> {
        self.console.as_deref()
    }

    /// Whether the runtime keeps metrics.
    pub fn metrics(&self) -> bool {
        self.metrics
    }

    /// Deterministic mode's settings, if it is on (`24-deterministic-testing.md`).
    pub fn deterministic(&self) -> Option<&DeterministicConfig> {
        self.deterministic.as_ref()
    }

    /// How long a shutdown waits for main to finish on its own (`26` §2, DRAINING).
    pub fn graceful_timeout(&self) -> Duration {
        self.graceful_timeout
    }

    /// How long a shutdown waits for cancelled tasks before abandoning them (`26` §2).
    pub fn cancel_timeout(&self) -> Duration {
        self.cancel_timeout
    }
}

/// Deterministic mode (`24-deterministic-testing.md` §2): one lane, a seeded scheduler and a
/// virtual clock. Python's `porto.DeterministicConfig`, without the simulated network, which
/// does not exist yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicConfig {
    /// The scheduler's seed: the same seed replays the same interleaving.
    pub seed: u64,
    /// The clock the runtime reads.
    pub clock: DeterministicClock,
    /// Whether the virtual clock jumps to the next timer when no task can run.
    pub autojump: bool,
    /// The most scheduling steps before the run fails; `None` for no limit. Must not be 0.
    pub max_steps: Option<u64>,
}

impl Default for DeterministicConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            clock: DeterministicClock::Virtual,
            autojump: true,
            max_steps: Some(10_000_000),
        }
    }
}
