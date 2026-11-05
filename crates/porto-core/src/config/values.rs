//! The configuration's enumerations. Each one parses from, and prints as, its lowercase name:
//! the spelling of the Python `Literal`s and of the `PORTO_*` environment variables (`26` §1).

use std::fmt;
use std::str::FromStr;

/// A name that is not one of an enumeration's values. The message lists the valid names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownName {
    kind: &'static str,
    name: String,
    valid: &'static [&'static str],
}

impl fmt::Display for UnknownName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "porto: {:?} is not a valid {}; use one of: {}",
            self.name,
            self.kind,
            self.valid.join(", ")
        )
    }
}

impl std::error::Error for UnknownName {}

/// Declares an enumeration with lowercase names, `name()`, `Display` and `FromStr`.
macro_rules! named_enum {
    (
        $(#[$meta:meta])*
        $kind:literal, $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident => $text:literal, )+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
        pub enum $name {
            $( $(#[$variant_meta])* $variant, )+
        }

        impl $name {
            /// Every value's name, in declaration order.
            pub const NAMES: &'static [&'static str] = &[$($text),+];

            /// The lowercase name, as Python and the environment spell it.
            pub fn name(self) -> &'static str {
                match self {
                    $( Self::$variant => $text, )+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.name())
            }
        }

        impl FromStr for $name {
            type Err = UnknownName;

            fn from_str(name: &str) -> Result<Self, UnknownName> {
                match name {
                    $( $text => Ok(Self::$variant), )+
                    _ => Err(UnknownName { kind: $kind, name: name.to_owned(), valid: Self::NAMES }),
                }
            }
        }
    };
}

named_enum! {
    /// The I/O backend (`13-io-driver.md`). `Auto` probes for the best one on this platform.
    "io_backend", IoBackend {
        /// Probe: io_uring, then epoll on Linux; kqueue on macOS and BSD; IOCP on Windows.
        #[default]
        Auto => "auto",
        /// Linux epoll.
        Epoll => "epoll",
        /// macOS and BSD kqueue.
        Kqueue => "kqueue",
        /// Windows I/O completion ports.
        Iocp => "iocp",
        /// Linux io_uring.
        IoUring => "io_uring",
    }
}

named_enum! {
    /// How idle lanes steal work from busy ones (`12-scheduler-and-lanes.md` §5).
    "steal", Steal {
        /// Steal when idle, with a bounded number of searching lanes.
        #[default]
        Adaptive => "adaptive",
        /// Never steal: each task stays on the lane that spawned it.
        Off => "off",
        /// Steal as soon as a lane runs out of work, with no bound on searchers.
        Eager => "eager",
    }
}

named_enum! {
    /// Whether Python tasks move between lanes (`12-scheduler-and-lanes.md` §5, free-threaded
    /// builds only).
    "python_steal", PythonSteal {
        /// Steal Python tasks once a lane's queue exceeds `python_steal_threshold`.
        #[default]
        Adaptive => "adaptive",
        /// Python tasks never move.
        Off => "off",
    }
}

named_enum! {
    /// How AnyIO runs on porto (`22-asyncio-anyio-compat.md`).
    "anyio_backend", AnyioBackend {
        /// porto's own AnyIO backend.
        #[default]
        Native => "native",
        /// AnyIO's asyncio backend over porto's asyncio facade.
        Facade => "facade",
        /// AnyIO is not installed as a backend.
        Off => "off",
    }
}

named_enum! {
    /// The clock of deterministic mode (`24-deterministic-testing.md` §2).
    "clock", DeterministicClock {
        /// A virtual clock that only moves when the runtime advances it.
        #[default]
        Virtual => "virtual",
        /// The wall clock.
        Real => "real",
    }
}
