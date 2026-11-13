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
//! The pipe byte is only a wakeup. What is delivered is a per-signal *pending* count the handler
//! raises and [`Signals::next`] takes, so a full pipe cannot lose the only SIGINT, and signals
//! that arrive close together are delivered once (as the kernel coalesces a pending signal).
//!
//! **Repeated signals chain.** A subscription made with [`Signals::subscribe_chained`] also
//! calls the disposition it replaced (Python's C handler for SIGINT) from the second arrival of
//! the signal on, whether or not the reader took the first: a lane stuck in synchronous code
//! (before or after it acted on the first Ctrl-C) cannot do anything else, and Python's handler
//! trips the interpreter's eval breaker, so the second Ctrl-C raises `KeyboardInterrupt` in that
//! code, as with `asyncio.Runner`.

use std::io::{self, Read};
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicUsize, Ordering};

use parking_lot::Mutex;

use mio::unix::pipe;

use crate::{Driver, Interest, Registration};

/// One more than the largest signal number the handler table covers (Linux has 64 real-time
/// signals).
const SIGNAL_SLOTS: usize = 65;

/// The write end of the pipe that receives each signal, -1 for a signal nobody subscribed to.
/// Read by the signal handler, so atomics and nothing else.
static WRITE_FDS: [AtomicI32; SIGNAL_SLOTS] = [const { AtomicI32::new(-1) }; SIGNAL_SLOTS];

/// How many times each signal arrived since [`Signals::next`] last took it.
static PENDING: [AtomicU32; SIGNAL_SLOTS] = [const { AtomicU32::new(0) }; SIGNAL_SLOTS];

/// How many times each signal arrived since it was subscribed, whatever the reader took: from the
/// second on, a chaining subscription calls the replaced handler every time.
static SEEN: [AtomicU32; SIGNAL_SLOTS] = [const { AtomicU32::new(0) }; SIGNAL_SLOTS];

/// The handler each signal's handler chains to on a repeat, as a function address; 0 for none
/// (the default or ignore disposition, or a handler that takes `siginfo`).
static PREVIOUS: [AtomicUsize; SIGNAL_SLOTS] = [const { AtomicUsize::new(0) }; SIGNAL_SLOTS];

/// Whether a subscription asked for chaining on a repeat.
static CHAIN: [AtomicBool; SIGNAL_SLOTS] = [const { AtomicBool::new(false) }; SIGNAL_SLOTS];

/// What is installed for one signal.
struct Slot {
    /// How many subscriptions use the handler.
    subscribers: usize,
    /// How many of them chain on a repeat.
    chained: usize,
    /// The disposition before the first subscription, put back by the last.
    previous: Option<libc::sigaction>,
}

static SLOTS: Mutex<[Slot; SIGNAL_SLOTS]> = Mutex::new(
    [const {
        Slot {
            subscribers: 0,
            chained: 0,
            previous: None,
        }
    }; SIGNAL_SLOTS],
);

fn lock_slots() -> parking_lot::MutexGuard<'static, [Slot; SIGNAL_SLOTS]> {
    SLOTS.lock()
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
            PENDING[index].fetch_add(1, Ordering::AcqRel);
            let seen = SEEN[index].fetch_add(1, Ordering::AcqRel);
            // Signal numbers are below 256.
            let byte = u8::try_from(signum).unwrap_or(u8::MAX);
            // SAFETY: `write` is async-signal-safe; `fd` is the write end of a pipe that stays
            // open while a subscription to this signal exists, and the buffer is one live byte.
            // A failed write (full pipe) loses only the wakeup: the pending count stays.
            let _ = unsafe { libc::write(fd, (&raw const byte).cast(), 1) };
            if seen > 0 && CHAIN[index].load(Ordering::Relaxed) {
                let previous = PREVIOUS[index].load(Ordering::Relaxed);
                if previous != 0 {
                    // SAFETY: `previous` was stored by `install` from a `sigaction` whose
                    // handler is a plain `void (*)(int)` (no `SA_SIGINFO`), neither default nor
                    // ignore: calling it is what the kernel would have done.
                    let handler: extern "C" fn(libc::c_int) = unsafe {
                        std::mem::transmute::<usize, extern "C" fn(libc::c_int)>(previous)
                    };
                    handler(signum);
                }
            }
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
        self.subscribe_with(signum, false)
    }

    /// Like [`subscribe`](Self::subscribe), and when the signal arrives again before the first
    /// was taken by [`next`](Self::next), the disposition it replaced is called too (see the
    /// module docs). For SIGINT, that is Python's handler.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe).
    pub fn subscribe_chained(&self, signum: i32) -> io::Result<Subscription> {
        self.subscribe_with(signum, true)
    }

    fn subscribe_with(&self, signum: i32, chained: bool) -> io::Result<Subscription> {
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
            // Before the handler is installed, so a signal that arrives at once is not dropped.
            PENDING[index].store(0, Ordering::Release);
            SEEN[index].store(0, Ordering::Release);
            WRITE_FDS[index].store(fd, Ordering::Release);
            let previous = match install(signum) {
                Ok(previous) => previous,
                Err(error) => {
                    WRITE_FDS[index].store(-1, Ordering::Release);
                    return Err(error);
                }
            };
            let callable = previous.sa_flags & libc::SA_SIGINFO == 0
                && previous.sa_sigaction != libc::SIG_DFL
                && previous.sa_sigaction != libc::SIG_IGN
                && previous.sa_sigaction != libc::SIG_ERR;
            PREVIOUS[index].store(
                if callable { previous.sa_sigaction } else { 0 },
                Ordering::Release,
            );
            slot.previous = Some(previous);
        } else if WRITE_FDS[index].load(Ordering::Acquire) != fd {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("signal {signum} is already delivered to another runtime"),
            ));
        }
        slot.subscribers += 1;
        if chained {
            slot.chained += 1;
            CHAIN[index].store(true, Ordering::Release);
        }
        drop(slots);
        Ok(Subscription {
            signum,
            index,
            chained,
            _signals: self.clone(),
        })
    }

    /// The next signal that arrived, waiting for one. A signal that arrived several times
    /// since the last call is returned once.
    ///
    /// # Errors
    ///
    /// A [`ClosedResource`](crate::ClosedResource) error once the driver shut down.
    pub async fn next(&self) -> io::Result<i32> {
        loop {
            if let Some(signum) = self.take_pending() {
                return Ok(signum);
            }
            // Only a wakeup: what arrived is in the pending counts.
            self.inner
                .receiver
                .async_io(Interest::READABLE, |receiver| {
                    let mut bytes = [0_u8; 64];
                    loop {
                        match (&*receiver).read(&mut bytes) {
                            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                            Ok(count) => return Ok(count),
                            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                            Err(error) => return Err(error),
                        }
                    }
                })
                .await?;
        }
    }

    /// Takes one signal of this pipe that is pending, if any.
    fn take_pending(&self) -> Option<i32> {
        let fd = std::os::fd::AsRawFd::as_raw_fd(&self.inner.sender);
        (1..SIGNAL_SLOTS).find_map(|index| {
            (WRITE_FDS[index].load(Ordering::Acquire) == fd
                && PENDING[index].swap(0, Ordering::AcqRel) > 0)
                .then(|| i32::try_from(index).ok())
                .flatten()
        })
    }
}

/// Installs [`on_signal`] for `signum`, returning the disposition it replaced.
fn install(signum: i32) -> io::Result<libc::sigaction> {
    // SAFETY: `sigaction` is plain data; zeroed is a valid empty value to fill in.
    let mut action: libc::sigaction = unsafe { MaybeUninit::zeroed().assume_init() };
    action.sa_sigaction = on_signal as *const () as libc::sighandler_t;
    // No `SA_RESTART`, as Python installs its own handlers: a blocking syscall of the lane
    // (`recv`, `read`, a lock wait) returns `EINTR` so the chained Python handler's
    // `KeyboardInterrupt` is raised by the code that made it, instead of the syscall resuming.
    // The driver's poll treats `EINTR` as a wakeup.
    action.sa_flags = 0;
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
    chained: bool,
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
        // Zero already in a forked child, which let go of every signal ([`forget_in_child`]).
        slot.subscribers = slot.subscribers.saturating_sub(1);
        if self.chained {
            slot.chained = slot.chained.saturating_sub(1);
            CHAIN[self.index].store(slot.chained > 0, Ordering::Release);
        }
        if slot.subscribers == 0
            && let Some(previous) = slot.previous.take()
        {
            // SAFETY: `previous` is what `sigaction` returned for this signal; restoring it is
            // what undoing our own install means. A failure leaves our handler in place, which
            // writes to a pipe that stays open until `WRITE_FDS` is cleared below.
            unsafe { libc::sigaction(self.signum, &raw const previous, std::ptr::null_mut()) };
            WRITE_FDS[self.index].store(-1, Ordering::Release);
            PREVIOUS[self.index].store(0, Ordering::Release);
            PENDING[self.index].store(0, Ordering::Release);
            SEEN[self.index].store(0, Ordering::Release);
        }
    }
}

/// Whether [`prepare_fork`] holds the slots' lock, until the fork's parent or child hook.
static FORK_LOCKED: AtomicBool = AtomicBool::new(false);

/// Before `fork()` (`os.register_at_fork(before=...)`): holds the slots' lock across the fork, so
/// the child never inherits it locked by a thread it does not have, in the middle of a
/// subscription. Pair it with [`after_fork_in_parent`] and [`forget_in_child`].
pub fn prepare_fork() {
    std::mem::forget(SLOTS.lock());
    FORK_LOCKED.store(true, Ordering::Release);
}

/// Like [`prepare_fork`], without waiting: `false` if another thread holds the lock. A caller
/// that holds the GIL retries with the GIL released in between, so that the holder (which may
/// need the GIL to finish) is never waited for with it.
pub fn try_prepare_fork() -> bool {
    match SLOTS.try_lock() {
        Some(guard) => {
            std::mem::forget(guard);
            FORK_LOCKED.store(true, Ordering::Release);
            true
        }
        None => false,
    }
}

/// After `fork()` in the parent: lets the slots go.
pub fn after_fork_in_parent() {
    if FORK_LOCKED.swap(false, Ordering::AcqRel) {
        // SAFETY: `prepare_fork` took the lock on this thread and forgot its guard.
        unsafe { SLOTS.force_unlock() };
    }
}

/// In a forked child: lets go of every signal the parent's runtime received. The child shares
/// the parent's pipe, so its handlers would write to the *parent's* runtime; instead each signal
/// gets its previous disposition back (Python's, usually) and nothing is delivered to any pipe.
/// Call it from `os.register_at_fork(after_in_child=...)`, after [`prepare_fork`] ran.
pub fn forget_in_child() {
    // Normally this thread holds the lock since `prepare_fork`. Without it (a fork that skipped
    // the `before` hook) never wait for a thread the child does not have: only the handlers'
    // targets can be cleared, which silences them.
    let held = FORK_LOCKED.swap(false, Ordering::AcqRel);
    let guard = if held { None } else { SLOTS.try_lock() };
    for (index, target) in WRITE_FDS.iter().enumerate() {
        target.store(-1, Ordering::Release);
        PENDING[index].store(0, Ordering::Release);
        SEEN[index].store(0, Ordering::Release);
        PREVIOUS[index].store(0, Ordering::Release);
        CHAIN[index].store(false, Ordering::Release);
    }
    if held || guard.is_some() {
        // SAFETY: held: `prepare_fork`'s forgotten guard lives on in this thread, the only one
        // in the child; otherwise the guard is alive.
        let slots = unsafe { &mut *SLOTS.data_ptr() };
        for (index, slot) in slots.iter_mut().enumerate() {
            slot.subscribers = 0;
            slot.chained = 0;
            if let Some(previous) = slot.previous.take()
                && let Ok(signum) = i32::try_from(index)
            {
                // SAFETY: `previous` is what `sigaction` returned for this signal.
                unsafe { libc::sigaction(signum, &raw const previous, std::ptr::null_mut()) };
            }
        }
    }
    drop(guard);
    if held {
        // SAFETY: see above: this thread holds the forgotten guard.
        unsafe { SLOTS.force_unlock() };
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
    fn signals_that_arrive_close_together_are_taken_once_and_a_repeat_is_not_lost() {
        let driver = Arc::new(MioDriver::new().unwrap());
        let signals = Signals::new(driver).unwrap();
        let subscription = signals.subscribe_chained(libc::SIGURG).unwrap();
        assert_eq!(signals.take_pending(), None);
        // SAFETY: raising a signal with our handler installed; SIGURG's default is ignore.
        unsafe {
            libc::raise(libc::SIGURG);
            libc::raise(libc::SIGURG);
        }
        assert_eq!(signals.take_pending(), Some(libc::SIGURG));
        assert_eq!(signals.take_pending(), None);
        // SAFETY: as above.
        unsafe { libc::raise(libc::SIGURG) };
        assert_eq!(signals.take_pending(), Some(libc::SIGURG));
        drop(subscription);
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
