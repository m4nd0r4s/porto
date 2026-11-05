//! Porto's I/O driver: readiness semantics over registered OS resources.
//!
//! A resource is registered once with a [`Driver`]. Each operation is attempted nonblocking;
//! on `WouldBlock` the task waits for readiness of the needed [`Interest`], clears the readiness
//! it consumed with [`ScheduledIo::clear_readiness`] and retries. See
//! [13-io-driver.md](https://github.com/m4nd0r4s/porto/blob/main/docs/spec/10-porto-runtime/13-io-driver.md).
//!
//! This crate MUST build and pass its tests without Python (rule A1).
//!
//! ```
//! use porto_io::{Interest, IoSlab, Ready};
//!
//! let slab = IoSlab::new();
//! let id = slab.allocate()?;
//! assert!(slab.dispatch(id, Ready::READABLE));
//! let io = slab.get(id).expect("the registration is live");
//! let event = io.ready_event(id, Interest::READABLE);
//! assert!(event.ready().is_readable());
//! // The read hit `WouldBlock`: forget the readiness that was consumed.
//! io.clear_readiness(event);
//! assert!(io.ready_event(id, Interest::READABLE).ready().is_empty());
//! # Ok::<(), std::io::Error>(())
//! ```

mod backend;
mod driver;
mod mio_driver;
mod ready;
mod scheduled_io;
mod slab;
mod waiters;

#[cfg(test)]
mod test_util;

pub use backend::{
    BackendSelection, IoBackend, IoBackendConfig, NATIVE_DRIVER_KIND, UnavailableBackend,
    UnknownIoBackend, select_backend,
};
pub use driver::{Driver, DriverKind, DriverWaker, RawOsHandle, RawSource, RegistrationId};
pub use mio_driver::MioDriver;
pub use ready::{Interest, Ready};
pub use scheduled_io::{Direction, Readiness, ReadyEvent, ScheduledIo};
pub use slab::IoSlab;
