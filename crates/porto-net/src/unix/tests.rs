//! The Unix listening socket, its file mode and the connect over real sockets.

use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;

use super::*;

fn temp_path(name: &str) -> PathBuf {
    // Short: a socket path must fit `sockaddr_un.sun_path` (104 bytes on macOS).
    let directory = std::env::temp_dir().join(format!("pn{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    directory.join(name)
}

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn address_of(path: &Path) -> SockAddr {
    address(path.as_os_str().as_bytes()).unwrap()
}

#[test]
fn the_socket_file_has_exactly_the_mode_asked_for() {
    for mode in [0o600, 0o660, 0o666, 0o777, 0o000] {
        let path = temp_path(&format!("mode{mode:o}"));
        drop(std::fs::remove_file(&path));
        let (_socket, file) = listen(&address_of(&path), 8, mode).unwrap();
        assert_eq!(mode_of(&path), mode, "mode {mode:o}");
        file.unwrap().remove();
    }
}

#[test]
fn the_mode_does_not_depend_on_the_processs_umask() {
    // A umask that removes bits the mode asks for: the file still gets all of them. (The umask
    // is process-wide and the other tests run beside this one, so this only checks the file.)
    // SAFETY: `umask` swaps the process's mask; restored right after the listen.
    let previous = unsafe { libc::umask(0o077) };
    let path = temp_path("umask");
    drop(std::fs::remove_file(&path));
    let result = listen(&address_of(&path), 8, 0o664);
    // SAFETY: as above.
    unsafe { libc::umask(previous) };
    let (_socket, file) = result.unwrap();
    assert_eq!(mode_of(&path), 0o664);
    file.unwrap().remove();
}

#[test]
fn an_existing_path_is_addr_in_use_and_is_left_alone() {
    let path = temp_path("taken");
    drop(std::fs::remove_file(&path));
    let (_first, file) = listen(&address_of(&path), 8, 0o660).unwrap();
    let error = listen(&address_of(&path), 8, 0o660).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    assert!(path.exists(), "the first listener's file survives");
    file.unwrap().remove();
}

#[test]
fn a_missing_directory_is_not_found() {
    let path = temp_path("no/such/dir/sock");
    let error = listen(&address_of(&path), 8, 0o660).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[test]
fn the_socket_file_is_removed_only_while_it_is_the_one_that_was_made() {
    let path = temp_path("removal");
    drop(std::fs::remove_file(&path));
    let (_socket, file) = listen(&address_of(&path), 8, 0o660).unwrap();
    let file = file.unwrap();
    assert_eq!(file.path(), path);
    assert!(file.remove());
    assert!(!path.exists());
    assert!(!file.remove(), "removing twice does nothing");
    // Another file at the path is not ours to delete.
    std::fs::write(&path, b"someone else's").unwrap();
    assert!(!file.remove());
    assert!(path.exists());
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn a_connect_reaches_the_listener_at_once() {
    let path = temp_path("connect");
    drop(std::fs::remove_file(&path));
    let (listener, file) = listen(&address_of(&path), 8, 0o660).unwrap();
    let client = connect(&address_of(&path)).unwrap().expect("connected");
    let (connection, _peer) = crate::tcp::accept(&listener).unwrap();
    client.send(b"hi").unwrap();
    drop(client);
    let mut stream = UnixStream::from(std::os::fd::OwnedFd::from(connection));
    stream.set_nonblocking(false).unwrap();
    let mut received = Vec::new();
    stream.read_to_end(&mut received).unwrap();
    assert_eq!(received, b"hi");
    file.unwrap().remove();
}

#[test]
fn connecting_to_nothing_is_not_found_and_to_a_stale_file_is_refused() {
    let path = temp_path("nothing");
    drop(std::fs::remove_file(&path));
    let error = connect(&address_of(&path)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    // A socket file nobody listens on any more.
    let (socket, _file) = listen(&address_of(&path), 8, 0o660).unwrap();
    drop(socket);
    let error = connect(&address_of(&path)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn addresses_are_checked_with_a_reason() {
    for (name, wanted) in [
        (b"".as_slice(), "empty"),
        (b"/tmp/a\0b", "NUL byte inside"),
        (&[b'/'; 200], "at most"),
    ] {
        let error = address(name).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(wanted), "{error}");
    }
    assert!(address(b"/tmp/ok.sock").is_ok());
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn an_abstract_name_listens_without_a_file() {
    let name = format!("\0porto-net-test-{}", std::process::id());
    let address = address(name.as_bytes()).unwrap();
    let (listener, file) = listen(&address, 8, 0o600).unwrap();
    assert!(file.is_none(), "an abstract socket has no file to remove");
    let client = connect(&address).unwrap().expect("connected");
    assert!(crate::tcp::accept(&listener).is_ok());
    drop(client);
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[test]
fn an_abstract_name_is_refused_where_the_os_has_none() {
    let error = address(b"\0name").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("abstract"), "{error}");
}

#[test]
fn a_process_other_than_the_creator_never_removes_the_file() {
    // What a forked child holding a copy of the listener is: another process id.
    let path = temp_path("creator");
    drop(std::fs::remove_file(&path));
    let (_socket, file) = listen(&address_of(&path), 8, 0o660).unwrap();
    let mut file = file.unwrap();
    file.creator += 1;
    assert!(!file.remove());
    assert!(
        path.exists(),
        "the creator's file survives the other process"
    );
    file.creator -= 1;
    assert!(file.remove());
}
