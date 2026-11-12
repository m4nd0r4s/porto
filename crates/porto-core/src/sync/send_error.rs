//! The error of a send that can fail two ways, shared by the oneshot, watch and broadcast
//! channels.

use std::error::Error;
use std::fmt;

/// Why a send on a oneshot, watch or broadcast channel failed. The value is returned.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    /// This sender was closed, or (a oneshot sender) already sent: the program closed it. The
    /// Python layer raises `porto.ClosedResourceError`.
    Closed(T),
    /// Every receiver was closed or dropped, so nothing would receive the value. The Python
    /// layer raises `porto.BrokenResourceError`.
    Broken(T),
}

impl<T> SendError<T> {
    /// The value that was not sent.
    pub fn into_inner(self) -> T {
        match self {
            Self::Closed(value) | Self::Broken(value) => value,
        }
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed(_) => f.write_str("Closed(..)"),
            Self::Broken(_) => f.write_str("Broken(..)"),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed(_) => f.write_str(
                "this sender was closed, or it is a oneshot sender that already sent its value \
                 (a oneshot channel carries exactly one value, so create another channel for \
                 the next one); send through a sender that is still open",
            ),
            Self::Broken(_) => f.write_str(
                "every receiver of this channel was closed or dropped, so nothing would receive \
                 the value (it is returned in the error); keep a receiver open while sending",
            ),
        }
    }
}

impl<T> Error for SendError<T> {}
