//! [`MioDriver`]: the readiness backend over mio, `epoll` on Linux and `kqueue` on macOS and
//! the BSDs (13-io-driver.md §3).
//!
//! mio registers every source edge-triggered (`EPOLLET`, `EV_CLEAR`): an event fires once per
//! readiness change, and the consumer clears what it consumed when an operation hits
//! `WouldBlock`. Each registration's mio token is its [`RegistrationId`]; the token
//! `usize::MAX`, which no slab ID can take, belongs to the driver's own waker.
//!
//! On Windows mio polls an IOCP port, and only the waker works: registering a raw socket needs
//! the AFD readiness backend, which is not written yet.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use mio::event::Event;
use mio::{Events, Poll, Registry, Token, Waker};

use crate::backend::{IoBackendConfig, NATIVE_DRIVER_KIND, UnavailableBackend, select_backend};
use crate::{
    Driver, DriverKind, DriverWaker, Interest, IoSlab, RawOsHandle, RawSource, Ready,
    RegistrationId,
};

/// The token of the driver's waker. [`RegistrationId::from_usize`] rejects it.
const WAKER_TOKEN: Token = Token(usize::MAX);
/// Events fetched per `epoll_wait`/`kevent` call. More stay queued for the next turn.
const EVENTS_PER_TURN: usize = 1024;

/// The mio backend: `epoll` on Linux, `kqueue` on macOS and the BSDs.
pub struct MioDriver {
    /// The poller and its event buffer; `turn` holds the lock while it waits.
    poll: Mutex<(Poll, Events)>,
    /// A handle on the poller that registers without waiting for `turn`.
    registry: Registry,
    slab: IoSlab,
    /// The raw handle of each live registration, to reregister and deregister it.
    sources: Mutex<HashMap<RegistrationId, RawOsHandle>>,
    waker: DriverWaker,
}

impl std::fmt::Debug for MioDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MioDriver")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl MioDriver {
    /// The native backend of this OS.
    ///
    /// # Errors
    ///
    /// When the OS refuses to create the poller or its waker (out of file descriptors).
    pub fn new() -> io::Result<Self> {
        let poll = Poll::new()?;
        let registry = poll.registry().try_clone()?;
        let waker = Arc::new(Waker::new(&registry, WAKER_TOKEN)?);
        Ok(Self {
            poll: Mutex::new((poll, Events::with_capacity(EVENTS_PER_TURN))),
            registry,
            slab: IoSlab::new(),
            sources: Mutex::default(),
            waker: DriverWaker::new(move || waker.wake()),
        })
    }

    /// The backend `config` selects, and the fallback warning the runtime should emit when
    /// it is not the one requested.
    ///
    /// # Errors
    ///
    /// When the requested backend is unavailable and `config.strict` is set (an
    /// [`io::ErrorKind::Unsupported`] error wrapping the [`UnavailableBackend`]), or when the
    /// poller cannot be created.
    pub fn open(config: IoBackendConfig) -> io::Result<(Self, Option<UnavailableBackend>)> {
        let selection = select_backend(config)
            .map_err(|error| io::Error::new(io::ErrorKind::Unsupported, error))?;
        debug_assert_eq!(selection.kind, NATIVE_DRIVER_KIND);
        Ok((Self::new()?, selection.fallback))
    }

    /// The slots holding each registration's readiness, for the tasks that wait on it.
    pub const fn slab(&self) -> &IoSlab {
        &self.slab
    }

    fn sources(&self) -> std::sync::MutexGuard<'_, HashMap<RegistrationId, RawOsHandle>> {
        self.sources.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Driver for MioDriver {
    fn register(&self, src: RawSource, interest: Interest) -> io::Result<RegistrationId> {
        let id = self.slab.allocate()?;
        if let Err(error) = os::register(&self.registry, src.raw(), id, interest) {
            self.slab.release(id);
            return Err(error);
        }
        self.sources().insert(id, src.raw());
        Ok(id)
    }

    fn reregister(&self, id: RegistrationId, interest: Interest) -> io::Result<()> {
        let raw = *self.sources().get(&id).ok_or_else(|| stale(id))?;
        os::reregister(&self.registry, raw, id, interest)
    }

    /// Removes the registration from the poller and releases its slot, waking its waiters
    /// with [`Ready::CLOSED`]. The slot is released even when the OS refuses the removal (the
    /// handle was already closed, which removed it), and that error is returned.
    fn deregister(&self, id: RegistrationId) -> io::Result<()> {
        let raw = self.sources().remove(&id).ok_or_else(|| stale(id))?;
        let removed = os::deregister(&self.registry, raw);
        self.slab.release(id);
        removed
    }

    fn shutdown(&self) {
        // Taken out first: releasing wakes tasks, whose cancellation may deregister sources.
        let sources = std::mem::take(&mut *self.sources());
        for (id, raw) in sources {
            // The poller goes away with the driver; a refused removal changes nothing here.
            drop(os::deregister(&self.registry, raw));
            self.slab.release(id);
        }
    }

    fn turn(&self, timeout: Option<Duration>) -> io::Result<usize> {
        let mut guard = self.poll.lock().unwrap_or_else(PoisonError::into_inner);
        let (poll, events) = &mut *guard;
        match poll.poll(events, timeout) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(0),
            Err(error) => return Err(error),
        }
        Ok(events
            .iter()
            .filter_map(|event| {
                RegistrationId::from_usize(event.token().0).map(|id| (id, ready_of(event)))
            })
            .filter(|&(id, ready)| self.slab.dispatch(id, ready))
            .count())
    }

    fn waker(&self) -> &DriverWaker {
        &self.waker
    }

    fn kind(&self) -> DriverKind {
        NATIVE_DRIVER_KIND
    }
}

fn stale(id: RegistrationId) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "{id:?} is not registered with the I/O driver: it was deregistered already, or \
             belongs to another driver; register the resource again"
        ),
    )
}

/// The readiness an event reports.
fn ready_of(event: &Event) -> Ready {
    let flags = [
        (event.is_readable(), Ready::READABLE),
        (event.is_writable(), Ready::WRITABLE),
        (event.is_read_closed(), Ready::READ_CLOSED),
        (event.is_write_closed(), Ready::WRITE_CLOSED),
        (event.is_error(), Ready::ERROR),
        (event.is_priority(), Ready::PRIORITY),
    ];
    flags
        .into_iter()
        .filter(|&(set, _)| set)
        .fold(Ready::EMPTY, |ready, (_, bit)| ready | bit)
}

#[cfg(unix)]
mod os {
    use std::io;

    use mio::unix::SourceFd;
    use mio::{Registry, Token};

    use crate::{Interest, RawOsHandle, RegistrationId};

    pub(super) fn register(
        registry: &Registry,
        fd: RawOsHandle,
        id: RegistrationId,
        interest: Interest,
    ) -> io::Result<()> {
        registry.register(
            &mut SourceFd(&fd),
            Token(id.to_usize()),
            mio_interest(interest),
        )
    }

    pub(super) fn reregister(
        registry: &Registry,
        fd: RawOsHandle,
        id: RegistrationId,
        interest: Interest,
    ) -> io::Result<()> {
        registry.reregister(
            &mut SourceFd(&fd),
            Token(id.to_usize()),
            mio_interest(interest),
        )
    }

    pub(super) fn deregister(registry: &Registry, fd: RawOsHandle) -> io::Result<()> {
        registry.deregister(&mut SourceFd(&fd))
    }

    /// mio's interest for `interest`. Priority data has its own interest on Linux only; kqueue
    /// reports out-of-band data as readable, so there it waits for readable.
    pub(super) fn mio_interest(interest: Interest) -> mio::Interest {
        let readable = interest.is_readable()
            || (interest.is_priority() && !cfg!(any(target_os = "linux", target_os = "android")));
        let mut wanted: Option<mio::Interest> = None;
        let mut add = |bit: mio::Interest| {
            wanted = Some(wanted.map_or(bit, |wanted| wanted | bit));
        };
        if readable {
            add(mio::Interest::READABLE);
        }
        if interest.is_writable() {
            add(mio::Interest::WRITABLE);
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if interest.is_priority() {
            add(mio::Interest::PRIORITY);
        }
        wanted.expect("an Interest is never empty")
    }
}

#[cfg(windows)]
mod os {
    use std::io;

    use mio::Registry;

    use crate::{Interest, RawOsHandle, RegistrationId};

    pub(super) fn register(
        _registry: &Registry,
        _socket: RawOsHandle,
        _id: RegistrationId,
        _interest: Interest,
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "porto cannot register sockets on Windows yet: the IOCP readiness backend is not \
             written; run this on Linux or macOS for now",
        ))
    }

    pub(super) fn reregister(
        _registry: &Registry,
        _socket: RawOsHandle,
        _id: RegistrationId,
        _interest: Interest,
    ) -> io::Result<()> {
        unreachable!("register never succeeds on Windows, so no ID reaches reregister")
    }

    pub(super) fn deregister(_registry: &Registry, _socket: RawOsHandle) -> io::Result<()> {
        unreachable!("register never succeeds on Windows, so no ID reaches deregister")
    }
}

#[cfg(test)]
mod tests;
