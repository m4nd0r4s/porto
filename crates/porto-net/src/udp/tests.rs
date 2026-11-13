//! The datagram socket over real loopback sockets.

use std::net::UdpSocket;

use super::*;

fn options() -> BindOptions {
    BindOptions {
        host: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port: 0,
        reuse_port: false,
        broadcast: false,
    }
}

fn port_of(socket: &Socket) -> u16 {
    socket
        .local_addr()
        .unwrap()
        .as_socket()
        .expect("an IP address")
        .port()
}

/// Whether this host can bind IPv6 loopback at all (some CI containers cannot).
fn has_ipv6() -> bool {
    UdpSocket::bind((Ipv6Addr::LOCALHOST, 0)).is_ok()
}

#[test]
fn port_zero_binds_a_port_the_os_picks() {
    let socket = bind(&options()).unwrap();
    assert_ne!(port_of(&socket), 0);
}

#[test]
fn the_socket_is_nonblocking() {
    let socket = bind(&options()).unwrap();
    let mut buffer = [std::mem::MaybeUninit::uninit(); 8];
    let error = socket.recv_from(&mut buffer).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
}

#[cfg(unix)]
#[test]
fn the_socket_is_close_on_exec() {
    use std::os::fd::AsRawFd;

    let socket = bind(&options()).unwrap();
    // SAFETY: `fcntl(F_GETFD)` only reads the flags of an open descriptor.
    let flags = unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0 && flags & libc::FD_CLOEXEC != 0);
}

#[test]
fn datagrams_round_trip_over_v4() {
    let socket = bind(&options()).unwrap();
    let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    client
        .send_to(b"ping", (Ipv4Addr::LOCALHOST, port_of(&socket)))
        .unwrap();
    let mut buffer = [std::mem::MaybeUninit::uninit(); 16];
    let (received, from) = loop {
        match socket.recv_from(&mut buffer) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            result => break result.unwrap(),
        }
    };
    assert_eq!(received, 4);
    assert_eq!(from.as_socket(), Some(client.local_addr().unwrap()));
}

#[test]
fn an_ipv6_host_binds_an_ipv6_socket_that_takes_no_ipv4() {
    if !has_ipv6() {
        return;
    }
    let socket = bind(&BindOptions {
        host: Some(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        ..options()
    })
    .unwrap();
    assert!(socket.local_addr().unwrap().is_ipv6());
    assert!(!is_dual_stack(&socket));
}

#[test]
fn no_host_binds_every_interface_on_one_dual_stack_socket() {
    let socket = bind(&BindOptions {
        host: None,
        ..options()
    })
    .unwrap();
    let local = socket.local_addr().unwrap();
    if local.is_ipv6() {
        assert!(
            is_dual_stack(&socket),
            "an IPv6 wildcard must take IPv4 too"
        );
        assert_eq!(local.as_socket().unwrap().ip(), Ipv6Addr::UNSPECIFIED);
    } else {
        // A host without IPv6 falls back to the IPv4 wildcard.
        assert_eq!(local.as_socket().unwrap().ip(), Ipv4Addr::UNSPECIFIED);
    }
}

#[test]
fn broadcast_sets_so_broadcast_and_binds_ipv4() {
    let plain = bind(&options()).unwrap();
    assert!(!plain.broadcast().unwrap());
    let broadcasting = bind(&BindOptions {
        broadcast: true,
        ..options()
    })
    .unwrap();
    assert!(broadcasting.broadcast().unwrap());
    // With no host, broadcast picks the IPv4 wildcard: IPv6 has no broadcast.
    let wildcard = bind(&BindOptions {
        host: None,
        broadcast: true,
        ..options()
    })
    .unwrap();
    assert!(wildcard.broadcast().unwrap());
    assert!(wildcard.local_addr().unwrap().is_ipv4());
}

#[test]
fn broadcast_with_an_ipv6_host_is_refused() {
    let error = bind(&BindOptions {
        host: Some(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        broadcast: true,
        ..options()
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
#[test]
fn reuse_port_lets_two_sockets_share_a_port() {
    let first = bind(&BindOptions {
        reuse_port: true,
        ..options()
    })
    .unwrap();
    let second = bind(&BindOptions {
        port: port_of(&first),
        reuse_port: true,
        ..options()
    })
    .unwrap();
    assert_eq!(port_of(&first), port_of(&second));
    // Without it the port is taken.
    let error = bind(&BindOptions {
        port: port_of(&first),
        ..options()
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
}

#[cfg(not(unix))]
#[test]
fn reuse_port_is_unsupported_where_the_os_has_none() {
    let error = bind(&BindOptions {
        reuse_port: true,
        ..options()
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
}

#[test]
fn a_taken_port_is_addr_in_use() {
    let first = bind(&options()).unwrap();
    let error = bind(&BindOptions {
        port: port_of(&first),
        ..options()
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
}

#[test]
fn an_ipv4_destination_of_a_dual_stack_socket_is_mapped() {
    let local = SockAddr::from(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 9));
    let v4 = SockAddr::from(SocketAddr::new(Ipv4Addr::new(10, 1, 2, 3).into(), 53));
    let mapped = reachable(&local, true, &v4).unwrap().as_socket().unwrap();
    assert_eq!(
        mapped,
        SocketAddr::new(Ipv4Addr::new(10, 1, 2, 3).to_ipv6_mapped().into(), 53)
    );
    // A v6-only socket cannot reach IPv4, and neither can an IPv4 socket reach IPv6.
    assert!(reachable(&local, false, &v4).is_none());
    let v6 = SockAddr::from(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 53));
    let local_v4 = SockAddr::from(SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 9));
    assert!(reachable(&local_v4, false, &v6).is_none());
    // The same family is passed through.
    assert_eq!(
        reachable(&local_v4, false, &v4).unwrap().as_socket(),
        v4.as_socket()
    );
    assert_eq!(
        reachable(&local, true, &v6).unwrap().as_socket(),
        v6.as_socket()
    );
}
