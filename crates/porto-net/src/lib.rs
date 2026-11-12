//! Porto's networking without Python: the sockets `porto.net` wraps and the rules they follow
//! (17-networking-tls-dns.md).
//!
//! - [`tcp`]: the listening socket, nonblocking `accept` and the TCP socket options.
//!
//! Nothing here registers a socket with the I/O driver or waits: the caller makes the sockets
//! nonblocking-ready here and runs them through `porto_io::Registration`. This crate MUST build
//! and pass its tests without Python (rule A1).

pub mod tcp;
