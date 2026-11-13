//! Unix domain stream sockets (17-networking-tls-dns.md §2): the listening socket `listen_unix`
//! opens, with the file mode it promises, the nonblocking connect, and the socket file.
//!
//! A path names a socket file (`/run/app.sock`); on Linux a name that starts with a NUL byte is
//! in the abstract namespace instead, which has no file and no mode. Every socket made here is
//! nonblocking and close-on-exec.

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use socket2::{Domain, SockAddr, Socket, Type};

/// The file mode `listen_unix` gives a socket file when the caller names none: the owner and the
/// group may connect, nobody else.
pub const DEFAULT_MODE: u32 = 0o660;

/// The most bytes a socket path can have on this OS, including a leading NUL of an abstract
/// name (`sizeof(sockaddr_un.sun_path)`: 108 on Linux, 104 on macOS and the BSDs).
pub fn max_path_len() -> usize {
    // SAFETY: `sockaddr_un` is plain data for which all-zero bytes are a valid value.
    let address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_path.len()
}

/// Whether `name` is in Linux's abstract namespace (it starts with a NUL byte).
pub fn is_abstract(name: &[u8]) -> bool {
    name.first() == Some(&0)
}

/// The address of the socket `name` names: a filesystem path, or on Linux (and Android) an
/// abstract name that starts with a NUL byte.
///
/// # Errors
///
/// [`io::ErrorKind::InvalidInput`] with the reason in the message: empty, a NUL byte inside a
/// path, too long for `sockaddr_un`, or an abstract name on an OS without them.
pub fn address(name: &[u8]) -> io::Result<SockAddr> {
    let invalid = |reason: String| io::Error::new(io::ErrorKind::InvalidInput, reason);
    if name.is_empty() {
        return Err(invalid("it is empty".to_owned()));
    }
    if is_abstract(name) {
        if !cfg!(any(target_os = "linux", target_os = "android")) {
            return Err(invalid(
                "a name starting with a NUL byte is Linux's abstract namespace, which this OS \
                 does not have"
                    .to_owned(),
            ));
        }
    } else if name.contains(&0) {
        // The kernel ends a path at its first NUL: refuse one inside a path rather than
        // use a shorter path than the one asked for.
        return Err(invalid("it has a NUL byte inside the path".to_owned()));
    }
    let limit = max_path_len();
    let too_long = if is_abstract(name) {
        name.len() > limit
    } else {
        name.len() >= limit
    };
    if too_long {
        return Err(invalid(format!(
            "it is {} bytes and this OS takes paths of at most {} (name the socket in a \
             shorter directory, or change to its directory first and use a relative path)",
            name.len(),
            limit - 1
        )));
    }
    SockAddr::unix(OsStr::from_bytes(name))
}

/// The socket file a listener created, remembered to remove it on close, but only if it is still
/// the file the listener made: a path another process replaced is not ours to delete.
#[derive(Debug)]
pub struct SocketFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketFile {
    /// Removes the file if the path still holds the socket this made. `true` if it was removed.
    pub fn remove(&self) -> bool {
        match std::fs::symlink_metadata(&self.path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && metadata.dev() == self.device
                    && metadata.ino() == self.inode =>
            {
                std::fs::remove_file(&self.path).is_ok()
            }
            _ => false,
        }
    }

    /// The path of the file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Serializes the processes' `umask` changes made here: `umask` is process-wide.
static UMASK: Mutex<()> = Mutex::new(());

/// Opens a listening Unix stream socket at `address`: nonblocking, close-on-exec, bound and
/// listening. For a filesystem path the socket file has exactly the permissions `mode` says
/// (`0..=0o777`), and is never visible with more: the bind runs under a `umask` that clears what
/// `mode` clears, on top of the process's own. The abstract namespace has no file, so `mode`
/// and the returned file are nothing there.
///
/// # Errors
///
/// The OS's error from `socket`, `bind` (`EADDRINUSE` if the path exists, which is never
/// removed on the caller's behalf; `ENOENT` if its directory does not; `EACCES`), `chmod` or
/// `listen`.
pub fn listen(
    address: &SockAddr,
    backlog: i32,
    mode: u32,
) -> io::Result<(Socket, Option<SocketFile>)> {
    let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    socket.set_nonblocking(true)?;
    let Some(path) = address.as_pathname() else {
        socket.bind(address)?;
        socket.listen(backlog)?;
        return Ok((socket, None));
    };
    bind_with_mode(&socket, address, mode)?;
    let file = match finish_file(path, mode) {
        Ok(file) => file,
        Err(error) => {
            // The socket file exists, and this listener is the one to remove it.
            drop(std::fs::remove_file(path));
            return Err(error);
        }
    };
    if let Err(error) = socket.listen(backlog) {
        file.remove();
        return Err(error);
    }
    Ok((socket, Some(file)))
}

/// Binds `socket` under a `umask` that leaves the socket file with at most `mode`.
fn bind_with_mode(socket: &Socket, address: &SockAddr, mode: u32) -> io::Result<()> {
    let _serialized = UMASK.lock().unwrap_or_else(PoisonError::into_inner);
    let clear = libc::mode_t::try_from(!mode & 0o777).unwrap_or(0o777);
    // Read the process's umask by setting the most restrictive one (which no file is ever
    // created more openly under), then add to it what `mode` clears.
    // SAFETY: `umask` only swaps the process's file mode creation mask.
    let previous = unsafe { libc::umask(0o777) };
    // SAFETY: as above.
    unsafe { libc::umask(previous | clear) };
    let bound = socket.bind(address);
    // SAFETY: as above.
    unsafe { libc::umask(previous) };
    bound
}

/// Gives the socket file at `path` exactly `mode` (the umask only ever takes bits away) and
/// remembers which file it is.
fn finish_file(path: &Path, mode: u32) -> io::Result<SocketFile> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    let metadata = std::fs::symlink_metadata(path)?;
    Ok(SocketFile {
        path: path.to_owned(),
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

/// Connects a new nonblocking close-on-exec Unix stream socket to `address`. A Unix connect is
/// decided at once: `Ok(Some(socket))` when it is connected, `Ok(None)` when the listener's
/// backlog is full (`EAGAIN` on Linux: poll cannot wait for room, the caller tries again a
/// little later), the OS's error otherwise (`ENOENT`, `ECONNREFUSED`, `EACCES`).
pub fn connect(address: &SockAddr) -> io::Result<Option<Socket>> {
    let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    socket.set_nonblocking(true)?;
    loop {
        match socket.connect(address) {
            Ok(()) => return Ok(Some(socket)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests;
