//! The [`Driver`] trait and the handles it deals in.

use std::fmt;
use std::io;
use std::time::Duration;

use crate::Interest;

/// A backend that waits for readiness of registered OS resources and dispatches it to their
/// [`ScheduledIo`](crate::ScheduledIo) entries.
pub trait Driver: Send + Sync {
    /// Registers `src` for `interest` (edge-triggered) and returns its slab ID.
    ///
    /// # Errors
    ///
    /// When the OS refuses the registration or the slab is full.
    fn register(&self, src: RawSource, interest: Interest) -> io::Result<RegistrationId>;

    /// Changes the interest of a live registration.
    ///
    /// # Errors
    ///
    /// When the OS refuses the change, or `id` is stale.
    fn reregister(&self, id: RegistrationId, interest: Interest) -> io::Result<()>;

    /// Removes a registration. Its slab entry is kept until the generation is bumped, so stale
    /// wakers are ignored.
    ///
    /// # Errors
    ///
    /// When the OS refuses the removal, or `id` is stale.
    fn deregister(&self, id: RegistrationId) -> io::Result<()>;

    /// Blocks for at most `timeout` (forever with `None`), dispatching readiness to the
    /// `ScheduledIo` entries. Returns the number of events dispatched.
    ///
    /// # Errors
    ///
    /// When waiting fails for a reason other than an interruption.
    fn turn(&self, timeout: Option<Duration>) -> io::Result<usize>;

    /// Unblocks [`turn`](Driver::turn) from another thread.
    fn waker(&self) -> &DriverWaker;

    /// Which backend this is.
    fn kind(&self) -> DriverKind;
}

/// The raw OS handle of a resource: a file descriptor on Unix.
#[cfg(unix)]
pub type RawOsHandle = std::os::fd::RawFd;

/// The raw OS handle of a resource: a socket on Windows.
#[cfg(windows)]
pub type RawOsHandle = std::os::windows::io::RawSocket;

/// An OS resource to register with a [`Driver`]. It does not own the handle: whoever
/// registers it keeps it open until it is deregistered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RawSource(RawOsHandle);

impl RawSource {
    /// Wraps a raw handle.
    pub const fn new(raw: RawOsHandle) -> Self {
        Self(raw)
    }

    /// The raw handle.
    pub const fn raw(self) -> RawOsHandle {
        self.0
    }
}

/// Bits of a [`RegistrationId`] that hold the slab index.
pub(crate) const INDEX_BITS: u32 = 24;
const INDEX_MASK: usize = (1 << INDEX_BITS) - 1;
/// Bits of a generation, in a [`RegistrationId`] and in a `ScheduledIo` readiness word.
pub(crate) const GENERATION_BITS: u32 = 8;
pub(crate) const GENERATION_MASK: usize = (1 << GENERATION_BITS) - 1;
/// An index the slab never hands out, so a token with every index bit set (`usize::MAX`) is
/// free for the driver's own waker.
pub(crate) const RESERVED_INDEX: usize = INDEX_MASK;

/// A generation-tagged slab ID: the slot index plus the generation the slot had when it was
/// allocated. Once the slot is released its generation moves on, so the old ID goes stale and
/// events or wakers that carry it are ignored. It fits in a `usize` (a mio `Token`) on 32-bit
/// targets too.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegistrationId(usize);

impl RegistrationId {
    pub(crate) const fn new(index: usize, generation: usize) -> Self {
        debug_assert!(index < RESERVED_INDEX);
        Self((index & INDEX_MASK) | ((generation & GENERATION_MASK) << INDEX_BITS))
    }

    /// The slot index in the slab.
    pub(crate) const fn index(self) -> usize {
        self.0 & INDEX_MASK
    }

    /// The generation of the slot this ID was allocated from.
    pub(crate) const fn generation(self) -> usize {
        (self.0 >> INDEX_BITS) & GENERATION_MASK
    }

    /// The ID as a `usize`, for a backend's event token.
    pub const fn to_usize(self) -> usize {
        self.0
    }

    /// The ID from a backend's event token. `None` for the token reserved to the driver waker
    /// (`usize::MAX`) or any other value no slab ever hands out.
    pub const fn from_usize(token: usize) -> Option<Self> {
        let unused = !(INDEX_MASK | (GENERATION_MASK << INDEX_BITS));
        if token & INDEX_MASK == RESERVED_INDEX || token & unused != 0 {
            None
        } else {
            Some(Self(token))
        }
    }
}

impl fmt::Debug for RegistrationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistrationId")
            .field("index", &self.index())
            .field("generation", &self.generation())
            .finish()
    }
}

/// The backend a [`Driver`] runs on (13-io-driver.md §3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DriverKind {
    /// Linux `epoll`, through mio.
    Epoll,
    /// macOS and BSD `kqueue`, through mio.
    Kqueue,
    /// Windows IOCP with AFD readiness emulation, through mio.
    Iocp,
    /// Linux `io_uring` (5.19 or later).
    IoUring,
    /// The deterministic simulation backend.
    Sim,
}

impl DriverKind {
    /// The name `porto.lowlevel.current_driver_kind()` reports.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Epoll => "epoll",
            Self::Kqueue => "kqueue",
            Self::Iocp => "iocp",
            Self::IoUring => "io_uring",
            Self::Sim => "sim",
        }
    }
}

impl fmt::Display for DriverKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

type WakeFn = dyn Fn() -> io::Result<()> + Send + Sync;

/// Unblocks a [`Driver::turn`] in progress, or makes the next one return at once.
pub struct DriverWaker {
    wake: Box<WakeFn>,
}

impl DriverWaker {
    /// A waker that calls `wake`, which the backend makes interrupt its wait (a mio `Waker`,
    /// an eventfd write, a condvar notify).
    pub fn new(wake: impl Fn() -> io::Result<()> + Send + Sync + 'static) -> Self {
        Self {
            wake: Box::new(wake),
        }
    }

    /// Unblocks the driver.
    ///
    /// # Errors
    ///
    /// When the backend fails to signal its wait.
    pub fn wake(&self) -> io::Result<()> {
        (self.wake)()
    }
}

impl fmt::Debug for DriverWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DriverWaker").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::{IoSlab, Ready};

    /// A driver that turns by dispatching whatever was queued, to prove the trait is usable
    /// as `dyn Driver` without any OS backend.
    struct QueueDriver {
        slab: IoSlab,
        queued: std::sync::Mutex<Vec<(RegistrationId, Ready)>>,
        waker: DriverWaker,
    }

    impl Driver for QueueDriver {
        fn register(&self, _src: RawSource, _interest: Interest) -> io::Result<RegistrationId> {
            self.slab.allocate()
        }

        fn reregister(&self, id: RegistrationId, _interest: Interest) -> io::Result<()> {
            self.slab.get(id).map(|_| ()).ok_or_else(stale)
        }

        fn deregister(&self, id: RegistrationId) -> io::Result<()> {
            self.slab.release(id).then_some(()).ok_or_else(stale)
        }

        fn turn(&self, _timeout: Option<Duration>) -> io::Result<usize> {
            let queued = std::mem::take(&mut *self.queued.lock().unwrap());
            Ok(queued
                .into_iter()
                .filter(|&(id, ready)| self.slab.dispatch(id, ready))
                .count())
        }

        fn waker(&self) -> &DriverWaker {
            &self.waker
        }

        fn kind(&self) -> DriverKind {
            DriverKind::Sim
        }
    }

    fn stale() -> io::Error {
        io::Error::new(io::ErrorKind::NotFound, "stale registration")
    }

    #[test]
    fn driver_is_object_safe_and_dispatches_through_the_slab() {
        let wakes = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&wakes);
        let driver: Box<dyn Driver> = Box::new(QueueDriver {
            slab: IoSlab::new(),
            queued: std::sync::Mutex::default(),
            waker: DriverWaker::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }),
        });
        let id = driver
            .register(RawSource::new(0), Interest::READABLE)
            .unwrap();
        driver.reregister(id, Interest::WRITABLE).unwrap();
        driver.waker().wake().unwrap();
        assert_eq!(wakes.load(Ordering::Relaxed), 1);
        assert_eq!(driver.kind().to_string(), "sim");
        driver.deregister(id).unwrap();
        assert_eq!(
            driver.deregister(id).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn registration_id_round_trips_through_a_token() {
        let id = RegistrationId::new(12_345, 200);
        assert_eq!(id.index(), 12_345);
        assert_eq!(id.generation(), 200);
        assert_eq!(RegistrationId::from_usize(id.to_usize()), Some(id));
        assert_eq!(RegistrationId::from_usize(usize::MAX), None);
        assert_eq!(RegistrationId::from_usize(RESERVED_INDEX), None);
    }

    #[test]
    fn raw_source_wraps_the_handle() {
        assert_eq!(RawSource::new(7).raw(), 7);
    }
}
