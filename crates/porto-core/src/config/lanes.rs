//! [`Lanes`]: how many lanes the runtime runs, and the `"auto"` rule
//! (`12-scheduler-and-lanes.md` §7).

use std::fmt;
use std::num::NonZeroUsize;

/// The most lanes `Lanes::Auto` resolves to.
pub const MAX_AUTO_LANES: usize = 64;

/// The lane count: a number, or `Auto` (one per physical core, see [`Lanes::resolve`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lanes {
    /// One lane per physical core (logical cores if unknown), at most [`MAX_AUTO_LANES`]; on a
    /// GIL build, at most 2.
    Auto,
    /// Exactly this many lanes. The builder refuses 0.
    Fixed(usize),
}

impl Default for Lanes {
    fn default() -> Self {
        Self::Fixed(1)
    }
}

impl Lanes {
    /// The lane count on this machine. `gil_build` is whether Python runs with a GIL: Python
    /// code then uses one lane, so `Auto` stops at 2 (lane 0 plus one Rust-only lane).
    pub fn resolve(self, gil_build: bool) -> usize {
        self.resolve_with_cores(gil_build, physical_cores())
    }

    /// [`Lanes::resolve`] on a machine with `cores` physical cores.
    pub fn resolve_with_cores(self, gil_build: bool, cores: usize) -> usize {
        match self {
            Self::Fixed(lanes) => lanes,
            Self::Auto => {
                let cores = cores.clamp(1, MAX_AUTO_LANES);
                if gil_build { cores.min(2) } else { cores }
            }
        }
    }
}

impl fmt::Display for Lanes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Fixed(lanes) => write!(f, "{lanes}"),
        }
    }
}

/// The number of physical cores, or of logical ones when the platform does not say (at least
/// 1).
pub fn physical_cores() -> usize {
    match num_cpus::get_physical() {
        0 => std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
        cores => cores,
    }
}
