//! Socket addresses between Python and [`SockAddr`], in the shapes the `socket` module uses:
//! `(host, port)` for IPv4, `(host, port, flowinfo, scope_id)` for IPv6, a path for a Unix
//! socket (`''` when it is unnamed, `bytes` for a Linux abstract name).

use std::net::{IpAddr, SocketAddr, SocketAddrV6};

use pyo3::exceptions::{PyOSError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyTuple;
#[cfg(unix)]
use pyo3::types::{PyBytes, PyString};
use socket2::SockAddr;

/// `address` as the `socket` module shows it.
pub(crate) fn to_py(py: Python<'_>, address: &SockAddr) -> PyResult<Py<PyAny>> {
    if let Some(address) = address.as_socket() {
        let host = address.ip().to_string();
        return Ok(match address {
            SocketAddr::V4(v4) => (host, v4.port()).into_pyobject(py)?.into_any().unbind(),
            SocketAddr::V6(v6) => (host, v6.port(), v6.flowinfo(), v6.scope_id())
                .into_pyobject(py)?
                .into_any()
                .unbind(),
        });
    }
    #[cfg(unix)]
    {
        // An unbound Unix sender: macOS reports an `AF_UNIX` address with no path, Linux an
        // empty one (length 0, family 0). Both read as '' on every platform.
        if address.is_unnamed() || address.len() == 0 {
            return Ok(PyString::new(py, "").into_any().unbind());
        }
        if let Some(path) = address.as_pathname() {
            return Ok(path.as_os_str().into_pyobject(py)?.into_any().unbind());
        }
        if let Some(name) = address.as_abstract_namespace() {
            let mut abstract_name = Vec::with_capacity(name.len() + 1);
            abstract_name.push(0);
            abstract_name.extend_from_slice(name);
            return Ok(PyBytes::new(py, &abstract_name).into_any().unbind());
        }
    }
    Err(PyOSError::new_err(format!(
        "porto: the peer's address has an address family porto does not convert yet ({}); \
         use an IPv4, IPv6 or Unix socket",
        address.family()
    )))
}

/// The address `object` names: `(host, port)` or `(host, port, flowinfo, scope_id)` with a
/// numeric host, or a path (`str` or `bytes`; `bytes` starting with a NUL byte for a Linux
/// abstract name) for a Unix socket.
pub(crate) fn from_py(object: &Bound<'_, PyAny>) -> PyResult<SockAddr> {
    if let Ok(tuple) = object.cast::<PyTuple>() {
        return from_tuple(tuple);
    }
    #[cfg(unix)]
    if object.is_instance_of::<PyString>() || object.is_instance_of::<PyBytes>() {
        return unix_from_py(object);
    }
    Err(PyTypeError::new_err(format!(
        "porto: a socket address must be a (host, port) tuple, (host, port, flowinfo, \
         scope_id) for IPv6, or a Unix socket path, not {}",
        object.get_type().name()?
    )))
}

/// The bytes of the Unix socket name `object` is: a `str` path (encoded as `os.fsencode` does),
/// a `bytes` one, or an `os.PathLike`; a name starting with a NUL byte is a Linux abstract name.
#[cfg(unix)]
fn unix_name(object: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    use std::os::unix::ffi::OsStrExt;

    if let Ok(bytes) = object.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    let path: std::path::PathBuf = object.extract()?;
    Ok(path.as_os_str().as_bytes().to_vec())
}

/// A Unix socket address from a `str` path (encoded as `os.fsencode` does) or a `bytes` one; a
/// name starting with a NUL byte is a Linux abstract name.
#[cfg(unix)]
fn unix_from_py(object: &Bound<'_, PyAny>) -> PyResult<SockAddr> {
    let encoded = unix_name(object)?;
    porto_net::unix::address(&encoded).map_err(|error| invalid_unix_address(object, &error))
}

#[cfg(unix)]
fn invalid_unix_address(object: &Bound<'_, PyAny>, reason: &std::io::Error) -> PyErr {
    PyValueError::new_err(format!(
        "porto: {} is not a valid Unix socket address ({reason}); use a path of at most about \
         100 bytes, without NUL bytes except a leading one for a Linux abstract name",
        object
            .repr()
            .map_or_else(|_| "the address".to_owned(), |repr| repr.to_string())
    ))
}

/// The address of the socket `path` names, for `call` (`listen_unix()`, `connect_unix()`): a
/// `str`, `bytes` or `os.PathLike`; `TypeError` for anything else, `ValueError` for a name that
/// is empty, has a NUL byte inside, is too long, or is abstract on an OS without them.
#[cfg(unix)]
pub(crate) fn unix_address(path: &Bound<'_, PyAny>, call: &str) -> PyResult<SockAddr> {
    if !(path.is_instance_of::<PyString>()
        || path.is_instance_of::<PyBytes>()
        || path.hasattr("__fspath__")?)
    {
        return Err(PyTypeError::new_err(format!(
            "porto: {call}'s path must be a str, bytes or os.PathLike naming the socket, not {}; \
             pass the path of the socket file, or a name starting with \"\\0\" for a Linux \
             abstract socket",
            path.get_type().name()?
        )));
    }
    let name = unix_name(path)?;
    porto_net::unix::address(&name).map_err(|reason| {
        PyValueError::new_err(format!(
            "porto: {call}'s path {} is not a valid Unix socket address: {reason}",
            path.repr()
                .map_or_else(|_| "the path".to_owned(), |repr| repr.to_string())
        ))
    })
}

/// An OS error from binding or connecting the Unix socket `address`, as the `OSError` Python
/// raises for a path: its errno, its message, and the path as `filename`.
#[cfg(unix)]
pub(crate) fn path_error(error: &std::io::Error, address: &SockAddr) -> PyErr {
    let Some(errno) = error.raw_os_error() else {
        return PyOSError::new_err(error.to_string());
    };
    let filename = address.as_pathname().map_or_else(
        || {
            address
                .as_abstract_namespace()
                .map_or_else(String::new, |name| {
                    format!("\\0{}", String::from_utf8_lossy(name))
                })
        },
        |path| path.to_string_lossy().into_owned(),
    );
    PyOSError::new_err((errno, error.to_string(), filename))
}

fn from_tuple(tuple: &Bound<'_, PyTuple>) -> PyResult<SockAddr> {
    let shape_error = || {
        PyTypeError::new_err(
            "porto: a socket address tuple is (host, port), or (host, port, flowinfo, \
             scope_id) for IPv6, with host a str and the rest ints",
        )
    };
    if !(2..=4).contains(&tuple.len()) {
        return Err(shape_error());
    }
    let host: String = tuple.get_item(0)?.extract().map_err(|_| shape_error())?;
    let port: u16 = tuple.get_item(1)?.extract().map_err(|_| {
        PyValueError::new_err("porto: the port of a socket address must be an int in 0..=65535")
    })?;
    let ip: IpAddr = host.parse().map_err(|_| {
        PyValueError::new_err(format!(
            "porto: {host:?} is not a numeric IP address; resolve the host name first \
             (socket.getaddrinfo) and pass the address it returns"
        ))
    })?;
    let address = match ip {
        IpAddr::V4(_) if tuple.len() > 2 => {
            return Err(PyValueError::new_err(
                "porto: an IPv4 socket address is (host, port); flowinfo and scope_id are for \
                 IPv6 only",
            ));
        }
        IpAddr::V4(_) => SocketAddr::new(ip, port),
        IpAddr::V6(v6) => {
            let flowinfo: u32 = optional_item(tuple, 2)?;
            let scope_id: u32 = optional_item(tuple, 3)?;
            SocketAddr::V6(SocketAddrV6::new(v6, port, flowinfo, scope_id))
        }
    };
    Ok(SockAddr::from(address))
}

/// The int at `index`, or 0 when the tuple is shorter.
fn optional_item(tuple: &Bound<'_, PyTuple>, index: usize) -> PyResult<u32> {
    if index >= tuple.len() {
        return Ok(0);
    }
    tuple.get_item(index)?.extract().map_err(|_| {
        PyValueError::new_err(
            "porto: the flowinfo and scope_id of an IPv6 socket address must be ints in \
             0..=4294967295",
        )
    })
}
