//! UDP sockets (17-networking-tls-dns.md §2): the datagram socket `bind_udp` opens.
//!
//! Like every socket made here it is nonblocking and close-on-exec. Sending and receiving
//! datagrams is the caller's: it runs `recv_from` and `send_to` through `porto_io::Registration`.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use crate::tcp::{no_ipv6, set_reuse_port};

/// The most bytes one UDP datagram carries over IPv4 (65535 less the IP and UDP headers): a
/// receive buffer of 65535 never truncates.
pub const MAX_DATAGRAM: usize = 65535;

/// What a datagram socket is bound with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindOptions {
    /// The address to bind, or `None` for every interface: IPv4 and IPv6 on one `::` socket
    /// (`IPV6_V6ONLY` off), or `0.0.0.0` when `broadcast` is set or the host has no IPv6.
    pub host: Option<IpAddr>,
    /// The port, 0 for one the OS picks.
    pub port: u16,
    /// `SO_REUSEPORT`: several sockets may bind the same port, the kernel spreading the
    /// datagrams between them.
    pub reuse_port: bool,
    /// `SO_BROADCAST`: the socket may send to a broadcast address. IPv4 only.
    pub broadcast: bool,
}

impl BindOptions {
    /// The address the socket binds to first: for `host: None` the dual-stack wildcard (`::`),
    /// or the IPv4 one (`0.0.0.0`) when `broadcast` is set, since IPv6 has no broadcast.
    pub const fn bind_address(&self) -> SocketAddr {
        let ip = match self.host {
            Some(host) => host,
            None if self.broadcast => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            None => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        };
        SocketAddr::new(ip, self.port)
    }
}

/// Opens a datagram socket: nonblocking, close-on-exec and bound. With `host: None` a host
/// without IPv6 gets an IPv4 socket on `0.0.0.0` instead, so `bind_udp(None)` works everywhere.
///
/// # Errors
///
/// The OS's error from `socket`, `bind` or `setsockopt` (`EADDRINUSE`, `EACCES`, ...), or
/// [`io::ErrorKind::Unsupported`] for `reuse_port` on an OS without it. `InvalidInput` for
/// `broadcast` with an IPv6 `host`.
pub fn bind(options: &BindOptions) -> io::Result<Socket> {
    if options.broadcast && matches!(options.host, Some(IpAddr::V6(_))) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "IPv6 has no broadcast",
        ));
    }
    match open_bound(options, options.bind_address()) {
        Err(error) if options.host.is_none() && no_ipv6(&error) => open_bound(
            options,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), options.port),
        ),
        result => result,
    }
}

fn open_bound(options: &BindOptions, address: SocketAddr) -> io::Result<Socket> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::DGRAM,
        Some(Protocol::UDP),
    )?;
    socket.set_nonblocking(true)?;
    if options.reuse_port {
        set_reuse_port(&socket)?;
    }
    if options.broadcast {
        socket.set_broadcast(true)?;
    }
    if address.is_ipv6() {
        // `host: None` means every interface of both families; an IPv6 host means that one
        // family, and the socket is not asked to take the other.
        socket.set_only_v6(options.host.is_some())?;
    }
    socket.bind(&SockAddr::from(address))?;
    Ok(socket)
}

/// How a socket of `local`'s family reaches `destination`: the same address, or for an IPv4
/// destination of a dual-stack IPv6 socket, its IPv4-mapped IPv6 form (what such a socket
/// wants, and what it shows a peer as). `None` when the socket cannot reach that family.
pub fn reachable(local: &SockAddr, dual_stack: bool, destination: &SockAddr) -> Option<SockAddr> {
    let (local, destination) = (local.as_socket()?, destination.as_socket()?);
    match (local, destination) {
        (SocketAddr::V4(_), SocketAddr::V4(_)) | (SocketAddr::V6(_), SocketAddr::V6(_)) => {
            Some(SockAddr::from(destination))
        }
        (SocketAddr::V6(_), SocketAddr::V4(v4)) if dual_stack => Some(SockAddr::from(
            SocketAddr::new(IpAddr::V6(v4.ip().to_ipv6_mapped()), v4.port()),
        )),
        _ => None,
    }
}

/// Whether `socket` is an IPv6 socket that takes IPv4 too (`IPV6_V6ONLY` off).
pub fn is_dual_stack(socket: &Socket) -> bool {
    socket.only_v6().is_ok_and(|only| !only)
}

#[cfg(test)]
mod tests;
