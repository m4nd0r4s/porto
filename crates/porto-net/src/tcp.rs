//! TCP sockets (17-networking-tls-dns.md §1): the listening socket `listen_tcp` opens,
//! nonblocking `accept`, the socket options of a stream, and the rules for running out of file
//! descriptors, and for connections that fail before they are accepted.
//!
//! Every socket made here is close-on-exec, and nonblocking (the registration it goes into
//! requires it).

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

use socket2::{Domain, Protocol, SockAddr, Socket, TcpKeepalive, Type};

/// How long `accept` waits after `EMFILE`, `ENFILE`, `ENOBUFS` or `ENOMEM` before it tries again: the pending
/// connection stays queued, and every attempt would fail the same way until a descriptor is
/// freed, so trying at once would spin a core (17 §1.2).
pub const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// The most often the warning about running out of descriptors or memory is logged.
pub const EXHAUSTION_WARNING_INTERVAL: Duration = Duration::from_secs(10);

/// What a listening socket is opened with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListenOptions {
    /// The address to listen on, or `None` for every interface.
    pub host: Option<IpAddr>,
    /// The port, 0 for one the OS picks.
    pub port: u16,
    /// The length of the queue of connections waiting to be accepted.
    pub backlog: i32,
    /// `SO_REUSEPORT`: several sockets may listen on the same port, the kernel spreading the
    /// connections between them.
    pub reuse_port: bool,
    /// For an IPv6 address (and for `host: None`, which then listens on `::`), accept IPv4
    /// connections too: `IPV6_V6ONLY` off. With `host: None` and no dual stack the socket
    /// listens on `0.0.0.0` (IPv4 only).
    pub dual_stack: bool,
}

impl ListenOptions {
    /// The address the socket binds to.
    pub const fn bind_address(&self) -> SocketAddr {
        let ip = match self.host {
            Some(host) => host,
            None if self.dual_stack => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            None => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        };
        SocketAddr::new(ip, self.port)
    }
}

/// Opens a listening TCP socket: nonblocking, close-on-exec, `SO_REUSEADDR` where the OS means
/// by it what POSIX does (not on Windows, where it lets another program take the port), bound
/// and listening.
///
/// With `host: None` and a dual stack, the socket listens on `::`; a host without IPv6 gets an
/// IPv4 socket on `0.0.0.0` instead, so `listen_tcp()` works everywhere.
///
/// # Errors
///
/// The OS's error from `socket`, `bind` or `listen` (`EADDRINUSE`, `EACCES`, ...), or
/// [`io::ErrorKind::Unsupported`] for `reuse_port` on an OS without it.
pub fn listen(options: &ListenOptions) -> io::Result<Socket> {
    match open_listener(options) {
        Err(error) if options.host.is_none() && options.dual_stack && no_ipv6(&error) => {
            open_listener(&ListenOptions {
                dual_stack: false,
                ..*options
            })
        }
        result => result,
    }
}

fn open_listener(options: &ListenOptions) -> io::Result<Socket> {
    let address = options.bind_address();
    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_nonblocking(true)?;
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    if options.reuse_port {
        set_reuse_port(&socket)?;
    }
    if address.is_ipv6() {
        socket.set_only_v6(!options.dual_stack)?;
    }
    socket.bind(&SockAddr::from(address))?;
    socket.listen(options.backlog)?;
    Ok(socket)
}

/// Whether `error` says this host cannot make an IPv6 socket or bind to `::`.
pub(crate) fn no_ipv6(error: &io::Error) -> bool {
    // `EAFNOSUPPORT` (97 on Linux, 47 on the BSDs and macOS, `WSAEAFNOSUPPORT` 10047 on
    // Windows) is what a kernel without IPv6 returns from `socket`; `EADDRNOTAVAIL` (99, 49,
    // 10049) is what one with IPv6 disabled returns from `bind`.
    const CODES: [i32; 6] = [97, 47, 10047, 99, 49, 10049];
    matches!(error.raw_os_error(), Some(code) if CODES.contains(&code))
}

#[cfg(all(
    unix,
    not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
))]
pub(crate) fn set_reuse_port(socket: &Socket) -> io::Result<()> {
    socket.set_reuse_port(true)
}

#[cfg(not(all(
    unix,
    not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
)))]
pub(crate) fn set_reuse_port(_socket: &Socket) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SO_REUSEPORT is not available on this OS",
    ))
}

/// Accepts one connection from the nonblocking `listener`: the new socket is nonblocking and
/// close-on-exec, made so by the kernel in one step where it can (`accept4` with
/// `SOCK_NONBLOCK | SOCK_CLOEXEC` on Linux and the BSDs), so a thread that forks between the
/// accept and a later `fcntl` cannot leak it to a child.
///
/// # Errors
///
/// `WouldBlock` when nothing is waiting; `EMFILE`, `ENFILE`, `ENOBUFS` and `ENOMEM` when the OS
/// has no resources for the connection (see [`is_resource_exhaustion`]); the OS's error
/// otherwise. A connection that failed before `accept` could return it (see
/// [`is_aborted_connection`]) is skipped: the next queued one is accepted.
pub fn accept(listener: &Socket) -> io::Result<(Socket, SockAddr)> {
    accept_skipping(|| accept_once(listener))
}

/// The most connections [`accept`] skips in a row before it gives the caller back its thread.
pub const MAX_SKIPPED_CONNECTIONS: usize = 64;

/// What [`accept`] returns, inside an [`io::Error`] (see [`AcceptSkipLimit::of`]), after it
/// skipped [`MAX_SKIPPED_CONNECTIONS`] connections in a row: more may be waiting, or the same
/// error may persist (a firewall or seccomp policy refusing every accept, a dead network), so
/// the caller yields its thread, tries again, and after repeated rounds backs off and warns.
#[derive(Debug)]
pub struct AcceptSkipLimit {
    last: io::Error,
}

impl AcceptSkipLimit {
    /// The error [`accept`] returns after it skipped too many connections, `last` being the
    /// error of the last one.
    pub fn error(last: io::Error) -> io::Error {
        io::Error::other(Self { last })
    }

    /// The `AcceptSkipLimit` in `error`, if it carries one.
    pub fn of(error: &io::Error) -> Option<&Self> {
        error.get_ref()?.downcast_ref::<Self>()
    }

    /// The error of the last skipped connection.
    pub fn last_error(&self) -> &io::Error {
        &self.last
    }
}

impl std::fmt::Display for AcceptSkipLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "accept skipped {MAX_SKIPPED_CONNECTIONS} connections in a row that failed before \
             they were accepted (the last: {})",
            self.last
        )
    }
}

impl std::error::Error for AcceptSkipLimit {}

/// Runs `next` until it returns something other than an aborted connection, skipping at most
/// [`MAX_SKIPPED_CONNECTIONS`] of them.
fn accept_skipping<T>(mut next: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let mut skipped = 0;
    loop {
        match next() {
            Err(error) if is_aborted_connection(&error) => {
                skipped += 1;
                if skipped == MAX_SKIPPED_CONNECTIONS {
                    return Err(AcceptSkipLimit::error(error));
                }
            }
            result => return result,
        }
    }
}

fn accept_once(listener: &Socket) -> io::Result<(Socket, SockAddr)> {
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "cygwin",
    ))]
    {
        let flags = libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
        listener.accept4(flags)
    }
    #[cfg(not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "cygwin",
    )))]
    {
        // `accept` makes the socket close-on-exec (non-inheritable on Windows) by itself.
        let (connection, address) = listener.accept()?;
        connection.set_nonblocking(true)?;
        Ok((connection, address))
    }
}

/// Whether `error` says the connection `accept` was returning failed before the program ever
/// saw it, so the listener is fine and the next queued connection is a new chance: the client
/// reset it first (`ECONNABORTED`), the protocol failed on it (`EPROTO`), a firewall refused it
/// (`EPERM`), or the network under it went away (`ENETDOWN`, `ENOPROTOOPT`, `EHOSTDOWN`,
/// `ENONET`, `EHOSTUNREACH`, `EOPNOTSUPP`, `ENETUNREACH`: accept(2) lists these to be treated
/// like `EAGAIN`); on Windows, a connection reset in the backlog (`WSAECONNRESET`). `accept`
/// skips them silently, like trio and asyncio do.
pub fn is_aborted_connection(error: &io::Error) -> bool {
    // `ConnectionAborted` is `ECONNABORTED` and `WSAECONNABORTED`; `ConnectionReset` is what
    // Winsock's accept returns (`WSAECONNRESET`) for a connection reset while it was still in
    // the backlog. On Unix a reset connection is accepted fine and fails at its first read.
    match error.kind() {
        io::ErrorKind::ConnectionAborted => return true,
        io::ErrorKind::ConnectionReset if cfg!(windows) => return true,
        _ => {}
    }
    error.raw_os_error().is_some_and(is_unix_aborted_errno)
}

#[cfg(unix)]
fn is_unix_aborted_errno(code: i32) -> bool {
    matches!(code, libc::EPROTO | libc::EPERM | libc::ENETDOWN) || is_network_errno(code)
}

#[cfg(not(unix))]
fn is_unix_aborted_errno(_code: i32) -> bool {
    false
}

/// The network errors accept(2) on Linux says to treat like `EAGAIN`.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn is_network_errno(code: i32) -> bool {
    matches!(
        code,
        libc::ENOPROTOOPT
            | libc::EHOSTDOWN
            | libc::ENONET
            | libc::EHOSTUNREACH
            | libc::EOPNOTSUPP
            | libc::ENETUNREACH
    )
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn is_network_errno(_code: i32) -> bool {
    false
}

/// Whether `error` is the OS running out of what a connection needs: file descriptors
/// (`EMFILE`: this process has no more, `ENFILE`: the system has none; `WSAEMFILE` on Windows)
/// or buffers and memory (`ENOBUFS`, `ENOMEM`; `WSAENOBUFS`). The connection that caused it is
/// still queued, so `accept` backs off ([`ACCEPT_BACKOFF`]) instead of failing.
pub fn is_resource_exhaustion(error: &io::Error) -> bool {
    #[cfg(unix)]
    const CODES: [i32; 4] = [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM];
    #[cfg(windows)]
    const CODES: [i32; 2] = [10024, 10055];
    #[cfg(not(any(unix, windows)))]
    const CODES: [i32; 0] = [];
    matches!(error.raw_os_error(), Some(code) if CODES.contains(&code))
}

/// `TCP_NODELAY`: send small writes at once instead of waiting to coalesce them (Nagle).
///
/// # Errors
///
/// The OS's error (`EBADF`, `ENOTCONN` after a reset, ...).
pub fn set_nodelay(socket: &Socket, nodelay: bool) -> io::Result<()> {
    socket.set_tcp_nodelay(nodelay)
}

/// Turns TCP keepalive on: the first probe after `idle` of silence, then one every `interval`,
/// the connection dropped after `count` unanswered probes. The OS keeps whole seconds, so each
/// duration is rounded up to one.
///
/// # Errors
///
/// The OS's error.
pub fn set_keepalive(
    socket: &Socket,
    idle: Duration,
    interval: Duration,
    count: u32,
) -> io::Result<()> {
    let keepalive = TcpKeepalive::new()
        .with_time(round_up_to_seconds(idle))
        .with_interval(round_up_to_seconds(interval))
        .with_retries(count);
    socket.set_tcp_keepalive(&keepalive)
}

fn round_up_to_seconds(duration: Duration) -> Duration {
    Duration::from_secs(duration.as_secs() + u64::from(duration.subsec_nanos() > 0))
}

/// Lets an event through at most once per interval: the rate limit of the warning about
/// running out of descriptors, which `accept` repeats every [`ACCEPT_BACKOFF`] while it lasts.
#[derive(Debug)]
pub struct RateLimit {
    interval: Duration,
    last: Option<Instant>,
}

impl RateLimit {
    /// A limit that lets one event through per `interval`.
    pub const fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: None,
        }
    }

    /// Whether an event at `now` goes through (the first always does); if so, it counts.
    pub fn allow(&mut self, now: Instant) -> bool {
        match self.last {
            Some(last) if now.saturating_duration_since(last) < self.interval => false,
            _ => {
                self.last = Some(now);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests;
