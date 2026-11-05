//! Choosing the I/O backend from the `io_backend` and `io_backend_strict` settings
//! (13-io-driver.md §3, 20-python-api-reference.md).
//!
//! Only the mio backends exist so far: `epoll` on Linux, `kqueue` on macOS and the BSDs, and
//! mio's IOCP poller on Windows. A request for another backend falls back to the native one
//! with a warning, or fails when `io_backend_strict` is set.

use std::fmt;
use std::str::FromStr;

use crate::DriverKind;

/// The `io_backend` setting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum IoBackend {
    /// The native backend of this OS.
    #[default]
    Auto,
    /// Linux `epoll`.
    Epoll,
    /// macOS and BSD `kqueue`.
    Kqueue,
    /// Windows IOCP.
    Iocp,
    /// Linux `io_uring`.
    IoUring,
}

impl IoBackend {
    /// Every setting, in the order the error messages list them.
    pub const ALL: [Self; 5] = [
        Self::Auto,
        Self::Epoll,
        Self::Kqueue,
        Self::Iocp,
        Self::IoUring,
    ];

    /// The setting's value, as written in `RuntimeConfig(io_backend=...)`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Epoll => "epoll",
            Self::Kqueue => "kqueue",
            Self::Iocp => "iocp",
            Self::IoUring => "io_uring",
        }
    }
}

impl fmt::Display for IoBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for IoBackend {
    type Err = UnknownIoBackend;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|backend| backend.name() == value)
            .ok_or_else(|| UnknownIoBackend(value.to_owned()))
    }
}

/// An `io_backend` value that names no backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownIoBackend(String);

impl fmt::Display for UnknownIoBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown io_backend {:?}; use one of ", self.0)?;
        for (i, backend) in IoBackend::ALL.into_iter().enumerate() {
            let separator = if i == 0 { "" } else { ", " };
            write!(f, "{separator}'{backend}'")?;
        }
        Ok(())
    }
}

impl std::error::Error for UnknownIoBackend {}

/// The I/O settings of a runtime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoBackendConfig {
    /// Which backend to use.
    pub backend: IoBackend,
    /// Fail instead of falling back when `backend` is unavailable.
    pub strict: bool,
}

/// The outcome of [`select_backend`]: the backend to run, and the fallback warning to emit
/// when it is not the one requested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendSelection {
    /// The backend to run.
    pub kind: DriverKind,
    /// Why the requested backend was replaced by `kind`. The runtime reports it as a warning.
    pub fallback: Option<UnavailableBackend>,
}

/// A requested backend that cannot run here. Its message is the fallback warning, or the
/// error when `io_backend_strict` forbids the fallback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnavailableBackend {
    requested: IoBackend,
    fallback: DriverKind,
    reason: &'static str,
    strict: bool,
}

impl UnavailableBackend {
    /// The backend that was asked for.
    pub const fn requested(&self) -> IoBackend {
        self.requested
    }

    /// The backend used instead (when not strict).
    pub const fn fallback(&self) -> DriverKind {
        self.fallback
    }

    /// Whether `io_backend_strict` turned the fallback into this error.
    pub const fn is_strict(&self) -> bool {
        self.strict
    }
}

impl fmt::Display for UnavailableBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            requested,
            fallback,
            reason,
            strict,
        } = self;
        if *strict {
            write!(
                f,
                "io_backend='{requested}' is unavailable: {reason}, and io_backend_strict=True \
                 forbids falling back to {fallback}; use io_backend='auto', or set \
                 io_backend_strict=False to fall back to {fallback} with a warning"
            )
        } else {
            write!(
                f,
                "io_backend='{requested}' is unavailable: {reason}; using {fallback} instead. \
                 Use io_backend='auto' to silence this warning, or io_backend_strict=True to \
                 make it an error"
            )
        }
    }
}

impl std::error::Error for UnavailableBackend {}

/// The backend this OS runs natively, through mio.
pub const NATIVE_DRIVER_KIND: DriverKind = if cfg!(windows) {
    DriverKind::Iocp
} else if cfg!(any(
    target_os = "linux",
    target_os = "android",
    target_os = "illumos",
    target_os = "redox"
)) {
    DriverKind::Epoll
} else {
    DriverKind::Kqueue
};

/// Resolves `config` to the backend to run.
///
/// # Errors
///
/// When the requested backend is unavailable and `config.strict` is set.
pub fn select_backend(config: IoBackendConfig) -> Result<BackendSelection, UnavailableBackend> {
    let native = NATIVE_DRIVER_KIND;
    let requested = match config.backend {
        IoBackend::Auto => native,
        IoBackend::Epoll => DriverKind::Epoll,
        IoBackend::Kqueue => DriverKind::Kqueue,
        IoBackend::Iocp => DriverKind::Iocp,
        IoBackend::IoUring => DriverKind::IoUring,
    };
    if requested == native {
        return Ok(BackendSelection {
            kind: native,
            fallback: None,
        });
    }
    let unavailable = UnavailableBackend {
        requested: config.backend,
        fallback: native,
        reason: unavailable_reason(requested),
        strict: config.strict,
    };
    if config.strict {
        Err(unavailable)
    } else {
        Ok(BackendSelection {
            kind: native,
            fallback: Some(unavailable),
        })
    }
}

/// Why `kind`, which is not this OS's native backend, cannot run.
const fn unavailable_reason(kind: DriverKind) -> &'static str {
    match kind {
        DriverKind::IoUring if cfg!(any(target_os = "linux", target_os = "android")) => {
            "this version of porto has no io_uring backend yet"
        }
        DriverKind::IoUring => "io_uring exists only on Linux",
        DriverKind::Epoll => "epoll exists only on Linux",
        DriverKind::Kqueue => "kqueue exists only on macOS and the BSDs",
        DriverKind::Iocp => "IOCP exists only on Windows",
        DriverKind::Sim => "the simulation backend is chosen by deterministic mode, not io_backend",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(backend: IoBackend, strict: bool) -> IoBackendConfig {
        IoBackendConfig { backend, strict }
    }

    #[test]
    fn names_round_trip_and_unknown_names_list_the_choices() {
        for backend in IoBackend::ALL {
            assert_eq!(backend.name().parse(), Ok(backend));
        }
        let error = "uring".parse::<IoBackend>().unwrap_err().to_string();
        assert_eq!(
            error,
            "unknown io_backend \"uring\"; use one of 'auto', 'epoll', 'kqueue', 'iocp', \
             'io_uring'"
        );
    }

    #[test]
    fn auto_and_the_native_backend_select_it_without_a_warning() {
        let native_setting: IoBackend = NATIVE_DRIVER_KIND.name().parse().unwrap();
        for backend in [IoBackend::Auto, native_setting] {
            for strict in [false, true] {
                let selection = select_backend(config(backend, strict)).unwrap();
                assert_eq!(selection.kind, NATIVE_DRIVER_KIND);
                assert_eq!(selection.fallback, None);
            }
        }
    }

    #[test]
    fn io_uring_falls_back_to_the_native_backend_with_a_warning() {
        let selection = select_backend(config(IoBackend::IoUring, false)).unwrap();
        assert_eq!(selection.kind, NATIVE_DRIVER_KIND);
        let warning = selection.fallback.unwrap();
        assert_eq!(warning.requested(), IoBackend::IoUring);
        assert_eq!(warning.fallback(), NATIVE_DRIVER_KIND);
        assert!(!warning.is_strict());
        let message = warning.to_string();
        assert!(message.starts_with("io_backend='io_uring' is unavailable: "));
        assert!(message.contains(&format!("using {NATIVE_DRIVER_KIND} instead")));
        assert!(message.contains("io_backend='auto' to silence this warning"));
    }

    #[test]
    fn strict_io_uring_is_an_error_that_says_what_to_do() {
        let error = select_backend(config(IoBackend::IoUring, true)).unwrap_err();
        assert!(error.is_strict());
        let message = error.to_string();
        assert!(message.starts_with("io_backend='io_uring' is unavailable: "));
        assert!(message.contains("io_backend_strict=True forbids falling back"));
        assert!(message.contains("use io_backend='auto'"));
        assert!(message.contains("io_backend_strict=False"));
    }

    #[test]
    fn a_foreign_backend_falls_back_or_fails_when_strict() {
        for backend in [IoBackend::Epoll, IoBackend::Kqueue, IoBackend::Iocp] {
            if backend.name() == NATIVE_DRIVER_KIND.name() {
                continue;
            }
            let selection = select_backend(config(backend, false)).unwrap();
            assert_eq!(selection.kind, NATIVE_DRIVER_KIND);
            let warning = selection.fallback.unwrap().to_string();
            assert!(warning.contains("exists only on"), "{warning}");
            let error = select_backend(config(backend, true)).unwrap_err();
            assert!(error.to_string().contains("io_backend_strict=True forbids"));
        }
    }
}
