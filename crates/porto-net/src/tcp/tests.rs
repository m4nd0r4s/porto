//! The listening socket, accept and the options over real loopback sockets.

use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use super::*;

fn options() -> ListenOptions {
    ListenOptions {
        host: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port: 0,
        backlog: 16,
        reuse_port: false,
        dual_stack: true,
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

/// Accepts, waiting for the connection the kernel has already queued for a loopback connect.
fn accept_queued(listener: &Socket) -> (Socket, SockAddr) {
    for _ in 0..500 {
        match accept(listener) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => return result.unwrap(),
        }
    }
    panic!("no connection arrived");
}

#[test]
fn port_zero_binds_a_port_the_os_picks() {
    let listener = listen(&options()).unwrap();
    assert_ne!(port_of(&listener), 0);
}

#[test]
fn accept_returns_a_nonblocking_close_on_exec_socket_and_the_peer_address() {
    let listener = listen(&options()).unwrap();
    assert!(matches!(
        accept(&listener),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    ));
    let mut client = TcpStream::connect((Ipv4Addr::LOCALHOST, port_of(&listener))).unwrap();
    let (connection, peer) = accept_queued(&listener);
    assert_eq!(peer.as_socket(), Some(client.local_addr().unwrap()));
    #[cfg(unix)]
    {
        assert!(
            is_close_on_exec(&connection),
            "the accepted socket is close-on-exec"
        );
    }
    // Nonblocking: reading with nothing sent would block, so it reports WouldBlock.
    let mut buffer = [0u8; 8];
    assert!(matches!(
        (&connection).read(&mut buffer),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    ));
    client.write_all(b"hi").unwrap();
    for _ in 0..500 {
        match (&connection).read(&mut buffer) {
            Ok(2) => return,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    panic!("the bytes never arrived");
}

/// Whether `FD_CLOEXEC` is set on `fd`, which the caller keeps open.
#[cfg(unix)]
fn is_close_on_exec(fd: &impl std::os::fd::AsRawFd) -> bool {
    // SAFETY: `fcntl(F_GETFD)` only reads the flags of a descriptor `fd` keeps open.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
    flags >= 0 && flags & libc::FD_CLOEXEC != 0
}

#[test]
fn the_listening_socket_is_close_on_exec() {
    #[cfg(unix)]
    {
        let listener = listen(&options()).unwrap();
        assert!(is_close_on_exec(&listener));
    }
}

#[test]
fn bind_address_follows_the_host_and_the_dual_stack() {
    let any = ListenOptions {
        host: None,
        ..options()
    };
    assert_eq!(any.bind_address(), "[::]:0".parse::<SocketAddr>().unwrap());
    let v4_only = ListenOptions {
        dual_stack: false,
        ..any
    };
    assert_eq!(
        v4_only.bind_address(),
        "0.0.0.0:0".parse::<SocketAddr>().unwrap()
    );
    let named = ListenOptions {
        host: Some("::1".parse().unwrap()),
        port: 80,
        ..any
    };
    assert_eq!(named.bind_address(), "[::1]:80".parse().unwrap());
}

#[test]
fn a_taken_port_is_an_address_in_use_error() {
    let first = listen(&options()).unwrap();
    let again = ListenOptions {
        port: port_of(&first),
        ..options()
    };
    let error = listen(&again).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
}

#[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
#[test]
fn reuse_port_lets_two_sockets_share_a_port() {
    let first = listen(&ListenOptions {
        reuse_port: true,
        ..options()
    })
    .unwrap();
    let second = listen(&ListenOptions {
        reuse_port: true,
        port: port_of(&first),
        ..options()
    })
    .unwrap();
    assert_eq!(port_of(&first), port_of(&second));
}

#[test]
fn nodelay_and_keepalive_are_set() {
    let listener = listen(&options()).unwrap();
    let _client = TcpStream::connect((Ipv4Addr::LOCALHOST, port_of(&listener))).unwrap();
    let (connection, _) = accept_queued(&listener);
    set_nodelay(&connection, true).unwrap();
    assert!(connection.tcp_nodelay().unwrap());
    set_nodelay(&connection, false).unwrap();
    assert!(!connection.tcp_nodelay().unwrap());
    set_keepalive(
        &connection,
        Duration::from_millis(1500),
        Duration::from_secs(3),
        4,
    )
    .unwrap();
    assert!(connection.keepalive().unwrap());
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
    {
        // Rounded up to whole seconds.
        assert_eq!(connection.tcp_keepalive_time().unwrap().as_secs(), 2);
        assert_eq!(connection.tcp_keepalive_interval().unwrap().as_secs(), 3);
        assert_eq!(connection.tcp_keepalive_retries().unwrap(), 4);
    }
}

#[test]
fn resource_exhaustion_is_no_descriptors_no_buffers_or_no_memory() {
    #[cfg(unix)]
    for code in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
        assert!(is_resource_exhaustion(&io::Error::from_raw_os_error(code)));
    }
    assert!(!is_resource_exhaustion(&io::Error::from_raw_os_error(2)));
    assert!(!is_resource_exhaustion(&io::ErrorKind::WouldBlock.into()));
    // A synthetic out-of-memory error (no OS code) is not the OS running out.
    assert!(!is_resource_exhaustion(&io::ErrorKind::OutOfMemory.into()));
}

#[test]
fn a_connection_that_failed_before_accept_is_skipped_not_raised() {
    assert!(is_aborted_connection(
        &io::ErrorKind::ConnectionAborted.into()
    ));
    #[cfg(unix)]
    for code in [
        libc::ECONNABORTED,
        libc::EPROTO,
        libc::EPERM,
        libc::ENETDOWN,
    ] {
        assert!(is_aborted_connection(&io::Error::from_raw_os_error(code)));
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    for code in [libc::EHOSTUNREACH, libc::ENETUNREACH, libc::EOPNOTSUPP] {
        assert!(is_aborted_connection(&io::Error::from_raw_os_error(code)));
    }
    // Resource exhaustion, a closed listener and a would-block are something else.
    #[cfg(unix)]
    for code in [libc::EMFILE, libc::ENFILE, libc::EBADF, libc::EINVAL] {
        assert!(!is_aborted_connection(&io::Error::from_raw_os_error(code)));
    }
    assert!(!is_aborted_connection(&io::ErrorKind::WouldBlock.into()));
}

#[test]
fn windows_reports_a_connection_reset_in_the_backlog_as_a_reset() {
    // `WSAECONNRESET` (10054) from Winsock's accept is skipped on Windows only: elsewhere a
    // reset connection is accepted and fails at its first read.
    let reset = io::Error::from(io::ErrorKind::ConnectionReset);
    assert_eq!(is_aborted_connection(&reset), cfg!(windows));
    #[cfg(windows)]
    assert!(is_aborted_connection(&io::Error::from_raw_os_error(10054)));
}

#[test]
fn skipping_is_capped_so_a_persistent_error_cannot_hold_the_caller() {
    let mut calls = 0;
    let result: io::Result<()> = accept_skipping(|| {
        calls += 1;
        Err(io::Error::from(io::ErrorKind::ConnectionAborted))
    });
    assert_eq!(
        calls, MAX_SKIPPED_CONNECTIONS,
        "no more attempts than the cap"
    );
    let error = result.unwrap_err();
    let limit = AcceptSkipLimit::of(&error).expect("the skip limit, not the raw error");
    assert_eq!(limit.last_error().kind(), io::ErrorKind::ConnectionAborted);
    assert!(error.to_string().contains("64 connections in a row"));
    assert!(!is_aborted_connection(&error), "it is not itself skipped");
    assert!(!is_resource_exhaustion(&error));
}

#[test]
fn skipping_stops_at_the_first_connection_that_works_or_the_first_other_error() {
    let mut calls = 0;
    let accepted = accept_skipping(|| {
        calls += 1;
        if calls < 10 {
            Err(io::Error::from(io::ErrorKind::ConnectionAborted))
        } else {
            Ok(calls)
        }
    });
    assert_eq!(accepted.unwrap(), 10);
    let mut calls = 0;
    let blocked: io::Result<()> = accept_skipping(|| {
        calls += 1;
        if calls < 5 {
            Err(io::Error::from(io::ErrorKind::ConnectionAborted))
        } else {
            Err(io::ErrorKind::WouldBlock.into())
        }
    });
    assert_eq!(blocked.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(calls, 5);
    // The cap counts consecutive skips: a connection that works starts the count over.
    let mut calls = 0;
    let result = accept_skipping(|| {
        calls += 1;
        if calls < MAX_SKIPPED_CONNECTIONS {
            Err(io::Error::from(io::ErrorKind::ConnectionAborted))
        } else {
            Ok(())
        }
    });
    assert!(result.is_ok(), "one short of the cap still gets through");
}

#[cfg(unix)]
#[test]
fn accept_skips_a_connection_the_client_reset_and_returns_the_next() {
    use std::net::Shutdown;
    let listener = listen(&options()).unwrap();
    let port = port_of(&listener);
    for _ in 0..4 {
        let client = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        client.set_linger(Some(Duration::ZERO)).unwrap();
        client
            .connect(&SockAddr::from(SocketAddr::from((
                Ipv4Addr::LOCALHOST,
                port,
            ))))
            .unwrap();
        let _ = client.shutdown(Shutdown::Both);
        drop(client); // RST before the connection was accepted
    }
    let mut last = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).unwrap();
    last.write_all(b"good").unwrap();
    // However the OS reports the reset ones (accepted, then reset; or ECONNABORTED, skipped),
    // accept never raises: it returns connections until the queue is empty.
    let mut accepted = 0;
    for _ in 0..500 {
        match accept(&listener) {
            Ok(_) => accepted += 1,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock && accepted > 0 => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("accept raised {error}"),
        }
    }
    assert!(accepted >= 1, "the good connection was accepted");
}

#[test]
fn the_rate_limit_lets_one_event_through_per_interval() {
    let mut limit = RateLimit::new(Duration::from_secs(10));
    let start = Instant::now();
    assert!(limit.allow(start));
    assert!(!limit.allow(start + Duration::from_secs(9)));
    assert!(limit.allow(start + Duration::from_secs(10)));
    assert!(!limit.allow(start + Duration::from_secs(11)));
    // A clock that reads earlier than the last event never panics.
    assert!(!limit.allow(start));
}
