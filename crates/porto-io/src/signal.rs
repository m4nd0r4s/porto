//! Signals delivered through a self-pipe (18-fs-process-signals.md §3). Unix only.
//!
//! A signal handler may do almost nothing (async-signal-safety): the handler installed here
//! only `write`s the signal number, one byte, to a pipe, and the read end is registered with the
//! I/O driver like any other source. A task then reads the numbers ([`Signals::next`]) and
//! delivers them wherever they are wanted. The handler runs on whichever thread the OS chose,
//! so delivery does not depend on the thread that receives.
//!
//! Python-level handlers are not involved: [`Signals::subscribe`] replaces the signal's
//! disposition (whatever it is, Python's handler included) until the [`Subscription`] drops,
//! which puts the previous one back. Several subscriptions to one signal share the handler, the
//! last one dropped restores.
//!
//! A pipe that is full drops the signal number, as the kernel coalesces a signal that is
//! already pending: a burst of one signal may be delivered once.

use std::io::{self, Read};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use mio::unix::pipe;

use crate::{Driver, Interest, Registration};

/// One more than the largest signal number the handler table covers (Linux has 64 real-time
/// signals).
const SIGNAL_SLOTS: usize = 65;

/// The write end of the pipe that receives each signal, -1 for a signal nobody subscribed to.
/// Read by the signal handler, so atomics and nothing else.
static WRITE_FDS: [AtomicI32; SIGNAL_SLOTS] = [const { AtomicI32::new(-1) }; SIGNAL_SLOTS];

/// What is installed for one signal.
struct Slot {
    /// How many subscriptions use the handler.
    subscribers: usize,
    /// The disposition before the first subscription, put back by the last.
    previous: Option<libc::sigaction>,
}

static SLOTS: Mutex<[Slot; SIGNAL_SLOTS]> = Mutex::new(
    [const {
        Slot {
            subscribers: 0,
            previous: None,
        }
    }; SIGNAL_SLOTS],
);

fn lock_slots() -> std::sync::MutexGuard<'static, [Slot; SIGNAL_SLOTS]> {
    SLOTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The handler: writes the signal number to its pipe. Nothing else is async-signal-safe to do
/// here; `errno` is saved and restored, as the interrupted code may be about to read it.
extern "C" fn on_signal(signum: libc::c_int) {
    // SAFETY: `errno_location` is the calling thread's `errno`, valid for the thread's life.
    let errno = unsafe { errno_location().read() };
    if let Ok(index) = usize::try_from(signum)
        && let Some(slot) = WRITE_FDS.get(index)
    {
        let fd = slot.load(Ordering::Relaxed);
        if fd >= 0 {
            // Signal numbers are below 256.
            let byte = u8::try_from(signum).unwrap_or(u8::MAX);
            // SAFETY: `write` is async-signal-safe; `fd` is the write end of a pipe that stays
            // open while a subscription to this signal exists, and the buffer is one live byte.
            // A failed write (full pipe) drops the notification, see the module docs.
            let _ = unsafe { libc::write(fd, (&raw const byte).cast(), 1) };
        }
    }
    // SAFETY: as above.
    unsafe { errno_location().write(errno) };
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "emscripten"))]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: the libc function takes no arguments and returns the thread's `errno` address.
    unsafe { libc::__errno_location() }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly"
))]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: the libc function takes no arguments and returns the thread's `errno` address.
    unsafe { libc::__error() }
}

#[cfg(any(target_os = "netbsd", target_os = "openbsd"))]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: the libc function takes no arguments and returns the thread's `errno` address.
    unsafe { libc::__errno() }
}

struct Inner {
    receiver: Registration<pipe::Receiver>,
    // Kept open for the handlers: `WRITE_FDS` holds its descriptor.
    sender: pipe::Sender,
}

/// The self-pipe of one runtime, registered with its driver. Cheap to clone.
#[derive(Clone)]
pub struct Signals {
    inner: Arc<Inner>,
}

impl Signals {
    /// Opens the pipe and registers its read end with `driver`.
    ///
    /// # Errors
    ///
    /// When the OS refuses the pipe or the driver the registration.
    pub fn new(driver: Arc<dyn Driver>) -> io::Result<Self> {
        let (sender, receiver) = pipe::new()?;
        let receiver = Registration::new(driver, receiver, Interest::READABLE)?;
        Ok(Self {
            inner: Arc::new(Inner { receiver, sender }),
        })
    }

    /// Starts delivering `signum` to this pipe, replacing its current disposition until the
    /// returned subscription drops.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for a number that is not a signal this can handle (`SIGKILL` and
    /// `SIGSTOP` cannot be caught), `AlreadyExists` if another [`Signals`] has the signal, or
    /// the OS error of `sigaction`.
    pub fn subscribe(&self, signum: i32) -> io::Result<Subscription> {
        let index = usize::try_from(signum)
            .ok()
            .filter(|index| (1..SIGNAL_SLOTS).contains(index))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("signal number {signum} is out of range"),
                )
            })?;
        let fd = std::os::fd::AsRawFd::as_raw_fd(&self.inner.sender);
        let mut slots = lock_slots();
        let slot = &mut slots[index];
        if slot.subscribers == 0 {
            let previous = install(signum)?;
            slot.previous = Some(previous);
            WRITE_FDS[index].store(fd, Ordering::Release);
        } else if WRITE_FDS[index].load(Ordering::Acquire) != fd {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("signal {signum} is already delivered to another runtime"),
            ));
        }
        slot.subscribers += 1;
        drop(slots);
        Ok(Subscription {
            signum,
            index,
            _signals: self.clone(),
        })
    }

    /// The next signal number that arrived, waiting for one.
    ///
    /// # Errors
    ///
    /// A [`ClosedResource`](crate::ClosedResource) error once the driver shut down.
    pub async fn next(&self) -> io::Result<i32> {
        self.inner
            .receiver
            .async_io(Interest::READABLE, |receiver| {
                let mut byte = [0_u8; 1];
                loop {
                    match (&*receiver).read(&mut byte) {
                        Ok(1) => return Ok(i32::from(byte[0])),
                        Ok(_) => return Err(io::ErrorKind::UnexpectedEof.into()),
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error),
                    }
                }
            })
            .await
    }
}

/// Installs [`on_signal`] for `signum`, returning the disposition it replaced.
fn install(signum: i32) -> io::Result<libc::sigaction> {
    // SAFETY: `sigaction` is plain data; zeroed is a valid empty value to fill in.
    let mut action: libc::sigaction = unsafe { MaybeUninit::zeroed().assume_init() };
    action.sa_sigaction = on_signal as *const () as libc::sighandler_t;
    action.sa_flags = libc::SA_RESTART;
    // SAFETY: `action.sa_mask` is a valid sigset to initialize.
    unsafe { libc::sigfillset(&raw mut action.sa_mask) };
    let mut previous = MaybeUninit::<libc::sigaction>::zeroed();
    // SAFETY: both pointers are valid for the call; `previous` is written by it.
    if unsafe { libc::sigaction(signum, &raw const action, previous.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `sigaction` succeeded, so it wrote `previous`.
    Ok(unsafe { previous.assume_init() })
}

/// A signal delivered to a [`Signals`] pipe; dropping it stops that, and the last subscription
/// to a signal puts its previous disposition back.
#[must_use = "dropping the subscription stops the delivery"]
pub struct Subscription {
    signum: i32,
    index: usize,
    // Keeps the pipe (the handler's descriptor) open for as long as the handler is installed.
    _signals: Signals,
}

impl Subscription {
    /// The signal number.
    #[must_use]
    pub fn signum(&self) -> i32 {
        self.signum
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut slots = lock_slots();
        let slot = &mut slots[self.index];
        slot.subscribers -= 1;
        if slot.subscribers == 0
            && let Some(previous) = slot.previous.take()
        {
            // SAFETY: `previous` is what `sigaction` returned for this signal; restoring it is
            // what undoing our own install means. A failure leaves our handler in place, which
            // writes to a pipe that stays open until `WRITE_FDS` is cleared below.
            unsafe { libc::sigaction(self.signum, &raw const previous, std::ptr::null_mut()) };
            WRITE_FDS[self.index].store(-1, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MioDriver;

    /// The handler currently installed for `signum`.
    fn current_handler(signum: i32) -> libc::sighandler_t {
        let mut current = MaybeUninit::<libc::sigaction>::zeroed();
        // SAFETY: a null new action only queries; `current` is written.
        unsafe {
            libc::sigaction(signum, std::ptr::null(), current.as_mut_ptr());
            current.assume_init().sa_sigaction
        }
    }

    #[test]
    fn the_last_subscription_puts_the_previous_disposition_back() {
        let signum = libc::SIGUSR2;
        let before = current_handler(signum);
        let driver = Arc::new(MioDriver::new().unwrap());
        let signals = Signals::new(driver).unwrap();
        let first = signals.subscribe(signum).unwrap();
        let second = signals.subscribe(signum).unwrap();
        assert_eq!(current_handler(signum), on_signal as *const () as usize);
        drop(first);
        assert_eq!(
            current_handler(signum),
            on_signal as *const () as usize,
            "one subscriber is left"
        );
        drop(second);
        assert_eq!(current_handler(signum), before);
    }

    #[test]
    fn a_signal_that_cannot_be_caught_is_refused() {
        let driver = Arc::new(MioDriver::new().unwrap());
        let signals = Signals::new(driver).unwrap();
        let error = signals.subscribe(libc::SIGKILL).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(signals.subscribe(0).is_err());
        assert!(signals.subscribe(1000).is_err());
    }
}
