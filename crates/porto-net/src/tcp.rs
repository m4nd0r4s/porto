//! TCP sockets (17-networking-tls-dns.md §1): the listening socket `listen_tcp` opens,
//! nonblocking `accept`, the socket options of a stream, and the rules for running out of file
//! descriptors.
//!
//! Every socket made here is close-on-exec, and nonblocking (the registration it goes into
//! requires it).

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use socket2::{Domain, Protocol, SockAddr, Socket, TcpKeepalive, Type};

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
fn no_ipv6(error: &io::Error) -> bool {
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
fn set_reuse_port(socket: &Socket) -> io::Result<()> {
    socket.set_reuse_port(true)
}

#[cfg(not(all(
    unix,
    not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
)))]
fn set_reuse_port(_socket: &Socket) -> io::Result<()> {
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
/// `WouldBlock` when nothing is waiting; `EMFILE` and `ENFILE` when no descriptor is left
/// (see [`is_descriptor_exhaustion`]); the OS's error otherwise.
pub fn accept(listener: &Socket) -> io::Result<(Socket, SockAddr)> {
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
        let flags = i32::from(Type::NONBLOCK) | i32::from(Type::CLOEXEC);
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

#[cfg(test)]
mod tests;
