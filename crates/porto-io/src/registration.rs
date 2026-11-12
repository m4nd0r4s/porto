//! [`Registration`]: an OS source registered with a [`Driver`] for its whole life, and closed
//! safely while other tasks wait on it or use it (13-io-driver.md §4).
//!
//! - [`Registration::new`] registers the source.
//! - [`Registration::close`] marks it closed, so no new operation starts; deregisters it,
//!   which releases its slab slot and wakes every waiter with [`Ready::CLOSED`](crate::Ready::CLOSED); then closes
//!   the handle, once the last operation already using it is done (see `close_gate`). The
//!   waiters, and every later call, fail with a [`ClosedResource`] error (`EBADF`).
//! - Dropping it does the same; nobody can be waiting on it by then.
//!
//! A released slot moves to its next generation before it is reused, and a closed
//! registration never looks at its slot again, so a handle number or a slot reused by a new
//! resource never wakes or feeds the old one's waiters.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use crate::close_gate::CloseGate;
pub use crate::close_gate::SourceRef;
use crate::{
    Direction, Driver, Interest, RawSource, Readiness, ReadyEvent, RegistrationId, ScheduledIo,
};

/// An OS resource [`Registration::new`] can register: anything that lends a file descriptor
/// (Unix) or a socket (Windows). The registration owns it and closes it by dropping it.
pub trait IoSource {
    /// The raw handle to register.
    fn raw_source(&self) -> RawSource;
}

#[cfg(unix)]
impl<T: std::os::fd::AsFd> IoSource for T {
    fn raw_source(&self) -> RawSource {
        use std::os::fd::AsRawFd;
        RawSource::new(self.as_fd().as_raw_fd())
    }
}

#[cfg(windows)]
impl<T: std::os::windows::io::AsSocket> IoSource for T {
    fn raw_source(&self) -> RawSource {
        use std::os::windows::io::AsRawSocket;
        RawSource::new(self.as_socket().as_raw_socket())
    }
}

/// A nonblocking OS source registered with a driver, edge-triggered, for the interest it was
/// registered with. Operations attempt the syscall, and on `WouldBlock` clear the readiness
/// they consumed and wait for more ([`try_io`](Self::try_io), [`async_io`](Self::async_io)).
pub struct Registration<S> {
    driver: Arc<dyn Driver>,
    id: RegistrationId,
    gate: CloseGate<S>,
}

impl<S: IoSource> Registration<S> {
    /// Registers `source`, which MUST be in nonblocking mode, with `driver` for `interest`.
    ///
    /// # Errors
    ///
    /// When the driver refuses the registration (the OS refused it, or the slab is full);
    /// `source` is dropped, which closes it.
    pub fn new(driver: Arc<dyn Driver>, source: S, interest: Interest) -> io::Result<Self> {
        let id = driver.register(source.raw_source(), interest)?;
        Ok(Self {
            driver,
            id,
            gate: CloseGate::new(source),
        })
    }
}

impl<S> Registration<S> {
    /// The slab ID of this registration.
    pub const fn id(&self) -> RegistrationId {
        self.id
    }

    /// Whether `driver` is the driver this registration was made with: the only one that ever
    /// polls it, so a task of any other run that waits on it would wait forever.
    pub fn is_driven_by(&self, driver: &Arc<dyn Driver>) -> bool {
        // Compared by address only: two vtables for one type may differ between codegen units.
        std::ptr::addr_eq(Arc::as_ptr(&self.driver), Arc::as_ptr(driver))
    }

    /// Whether [`close`](Self::close) was called.
    pub fn is_closed(&self) -> bool {
        self.gate.is_closed()
    }

    /// The source, kept open while the returned reference lives, even if another task closes
    /// the registration meanwhile (the handle is closed once the reference is dropped).
    ///
    /// # Errors
    ///
    /// A [`ClosedResource`] error once the registration is closed.
    pub fn source(&self) -> io::Result<SourceRef<'_, S>> {
        self.gate.acquire().ok_or_else(ClosedResource::closed)
    }

    /// Changes the interest the source is registered for.
    ///
    /// # Errors
    ///
    /// A [`ClosedResource`] error once the registration is closed, or the driver's error.
    pub fn reregister(&self, interest: Interest) -> io::Result<()> {
        let _open = self.source()?;
        self.driver.reregister(self.id, interest)
    }

    /// Polls for readiness in one `direction`, keeping one waker per direction: the cheap wait
    /// of the one task reading (or writing) the source.
    ///
    /// # Errors
    ///
    /// A [`ClosedResource`] error once the registration is closed (also when it is closed
    /// while this waits) or its driver shut down.
    pub fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        direction: Direction,
    ) -> Poll<io::Result<ReadyEvent>> {
        if self.gate.is_closed() {
            return Poll::Ready(Err(ClosedResource::closed()));
        }
        let event = ready!(self.scheduled_io().poll_readiness(cx, self.id, direction));
        Poll::Ready(self.checked(event))
    }

    /// Waits for readiness matching `interest`, for any number of concurrent waiters.
    pub fn readiness(&self, interest: Interest) -> RegistrationReadiness<'_, S> {
        RegistrationReadiness {
            registration: self,
            inner: self.scheduled_io().readiness(self.id, interest),
        }
    }

    /// Completes once the registration is closed (or released by its driver), at once if it
    /// already is: what a waiting task that is not waiting for readiness selects against to
    /// end promptly when the resource is closed under it.
    ///
    /// Only the release of the registration ends it, never readiness: a hang-up (which sets
    /// closed bits that are never cleared) leaves it pending.
    pub async fn closed(&self) {
        let released = RegistrationReadiness {
            registration: self,
            inner: self.scheduled_io().released(self.id),
        };
        // Resolves with the closed error, the only thing it can resolve with.
        drop(released.await);
    }

    /// Forgets the readiness in `event`, which an operation consumed (it hit `WouldBlock`),
    /// unless newer readiness arrived since it was observed.
    pub fn clear_readiness(&self, event: ReadyEvent) {
        if !self.gate.is_closed() {
            self.scheduled_io().clear_readiness(event);
        }
    }

    /// Runs `f`, a nonblocking operation on the source, if the source is ready for
    /// `interest`; if `f` hits `WouldBlock`, clears the readiness it consumed.
    ///
    /// # Errors
    ///
    /// `WouldBlock` when the source is not ready (wait with [`readiness`](Self::readiness)
    /// or [`poll_ready`](Self::poll_ready) first), a [`ClosedResource`] error once the
    /// registration is closed, or `f`'s error.
    pub fn try_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce(&S) -> io::Result<R>,
    ) -> io::Result<R> {
        let source = self.source()?;
        let event = self.checked(self.scheduled_io().ready_event(self.id, interest))?;
        if event.ready().is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let result = source.with(f);
        if matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
            self.scheduled_io().clear_readiness(event);
        }
        result
    }

    /// Runs `f`, a nonblocking operation on the source, at once, whatever readiness was
    /// observed: the fast path of an operation that may succeed before the driver reported
    /// anything (a fresh registration, data that arrived since the last turn). If `f` hits
    /// `WouldBlock`, clears the readiness observed before the attempt, unless newer readiness
    /// arrived meanwhile, so the next wait does not return at once for nothing.
    ///
    /// # Errors
    ///
    /// A [`ClosedResource`] error once the registration is closed or released, or `f`'s error
    /// (`WouldBlock` when the source is not ready: wait with [`readiness`](Self::readiness),
    /// then try again).
    pub fn attempt_io<R>(
        &self,
        interest: Interest,
        f: impl FnOnce(&S) -> io::Result<R>,
    ) -> io::Result<R> {
        let source = self.source()?;
        let event = self.checked(self.scheduled_io().ready_event(self.id, interest))?;
        let result = source.with(f);
        if !event.ready().is_empty()
            && matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        {
            self.scheduled_io().clear_readiness(event);
        }
        result
    }

    /// Runs `f`, a nonblocking operation on the source, until it does not hit `WouldBlock`,
    /// waiting for readiness matching `interest` before each attempt.
    ///
    /// # Errors
    ///
    /// A [`ClosedResource`] error once the registration is closed (also while this waits),
    /// or `f`'s error.
    pub async fn async_io<R>(
        &self,
        interest: Interest,
        mut f: impl FnMut(&S) -> io::Result<R>,
    ) -> io::Result<R> {
        loop {
            let event = self.readiness(interest).await?;
            let result = self.source()?.with(&mut f);
            match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.clear_readiness(event);
                }
                result => return result,
            }
        }
    }

    /// Closes the registration: no new operation starts, the source is deregistered, every
    /// waiter wakes with a [`ClosedResource`] error, and the handle is closed once the
    /// operations already using it are done. Closing again does nothing.
    ///
    /// # Errors
    ///
    /// When the driver refused the deregistration. The registration is closed anyway.
    pub fn close(&self) -> io::Result<()> {
        let Some(closer) = self.gate.close() else {
            return Ok(());
        };
        // Released by the driver already (it shut down): nothing to deregister.
        if self.driver.slab().get(self.id).is_none() {
            return Ok(());
        }
        // The closer's reference keeps the handle open while the driver removes it, so the
        // driver never names a handle number the close freed for someone else.
        let deregistered = self.driver.deregister(self.id);
        drop(closer);
        deregistered
    }

    /// The registration's slot, whatever its generation: the `ScheduledIo` compares the
    /// generation itself and reports a stale ID as released.
    fn scheduled_io(&self) -> &ScheduledIo {
        self.driver
            .slab()
            .slot(self.id)
            .expect("a registration's slot was allocated by its driver's slab")
    }

    /// `event`, or the error a closed or released registration reports.
    fn checked(&self, event: ReadyEvent) -> io::Result<ReadyEvent> {
        if self.gate.is_closed() {
            Err(ClosedResource::closed())
        } else if event.is_released() {
            Err(ClosedResource::released())
        } else {
            Ok(event)
        }
    }
}

impl<S> Drop for Registration<S> {
    fn drop(&mut self) {
        if let Some(closer) = self.gate.close() {
            // Nobody can wait on it any more; the slot is released even if the OS refuses.
            drop(self.driver.deregister(self.id));
            drop(closer);
        }
    }
}

impl<S> fmt::Debug for Registration<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registration")
            .field("id", &self.id)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// The future returned by [`Registration::readiness`].
#[must_use = "futures do nothing unless awaited"]
pub struct RegistrationReadiness<'a, S> {
    registration: &'a Registration<S>,
    inner: Readiness<'a>,
}

impl<S> Future for RegistrationReadiness<'_, S> {
    type Output = io::Result<ReadyEvent>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `inner` is never moved out of `self`; it is pinned along with it.
        let this = unsafe { self.get_unchecked_mut() };
        if this.registration.gate.is_closed() {
            return Poll::Ready(Err(ClosedResource::closed()));
        }
        // SAFETY: as above.
        let inner = unsafe { Pin::new_unchecked(&mut this.inner) };
        let event = ready!(inner.poll(cx));
        Poll::Ready(this.registration.checked(event))
    }
}

/// Why an operation on a [`Registration`] failed: it was closed, or released by its driver.
/// Carried by an [`io::Error`]; test for it with [`ClosedResource::is`]. porto raises it in
/// Python as `porto.ClosedResourceError` (an `OSError` with errno `EBADF`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosedResource {
    by_driver: bool,
}

impl ClosedResource {
    /// `EBADF`, the errno the Python error carries (9 on every platform porto supports).
    pub const ERRNO: i32 = 9;

    fn closed() -> io::Error {
        io::Error::other(Self { by_driver: false })
    }

    fn released() -> io::Error {
        io::Error::other(Self { by_driver: true })
    }

    /// The `ClosedResource` in `error`, if it carries one.
    pub fn of(error: &io::Error) -> Option<Self> {
        error.get_ref()?.downcast_ref::<Self>().copied()
    }

    /// Whether `error` reports a closed resource.
    pub fn is(error: &io::Error) -> bool {
        Self::of(error).is_some()
    }

    /// Whether the driver released the registration (the runtime shut down) rather than the
    /// program closing it.
    pub const fn by_driver(self) -> bool {
        self.by_driver
    }
}

impl fmt::Display for ClosedResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.by_driver {
            f.write_str(
                "the I/O resource is no longer registered: its runtime shut down. Open the \
                 resource again inside a running runtime",
            )
        } else {
            f.write_str(
                "the I/O resource was closed (by this task or another) while this operation \
                 used or waited on it; stop using a resource once it is closed, or open a new \
                 one",
            )
        }
    }
}

impl std::error::Error for ClosedResource {}

#[cfg(test)]
mod tests;
