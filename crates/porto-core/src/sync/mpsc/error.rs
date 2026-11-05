//! The channel's errors. Each one says what happened and what to do about it.

use std::error::Error;
use std::fmt;

/// [`Sender::send`](super::Sender::send) failed: the channel is closed. The value is returned.
///
/// Either every receiver was closed or dropped, or this sender was closed. The Python layer
/// raises it as `porto.BrokenResourceError`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> SendError<T> {
    /// The value that was not sent.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(CLOSED_FOR_SEND)
    }
}

impl<T> Error for SendError<T> {}

/// [`Sender::try_send`](super::Sender::try_send) failed. The value is returned.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is at capacity. The Python layer raises `porto.WouldBlock`.
    Full(T),
    /// As for [`SendError`].
    Closed(T),
}

impl<T> TrySendError<T> {
    /// The value that was not sent.
    pub fn into_inner(self) -> T {
        match self {
            Self::Full(value) | Self::Closed(value) => value,
        }
    }

    /// Whether the channel was full (rather than closed).
    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full(_))
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("Full(..)"),
            Self::Closed(_) => f.write_str("Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str(
                "the channel is full; await send() to wait for room, or give the channel a \
                 larger capacity",
            ),
            Self::Closed(_) => f.write_str(CLOSED_FOR_SEND),
        }
    }
}

impl<T> Error for TrySendError<T> {}

impl<T> From<SendError<T>> for TrySendError<T> {
    fn from(error: SendError<T>) -> Self {
        Self::Closed(error.0)
    }
}

const CLOSED_FOR_SEND: &str = "the channel is closed: every receiver was closed or dropped, or \
     this sender was closed, so nothing would receive the value (it is returned in the error); \
     stop sending, or keep a receiver open for as long as senders may send";

/// [`Receiver::recv`](super::Receiver::recv) found the channel closed and drained.
///
/// Every sender was closed or dropped and every value sent was received. This is the normal
/// end of a stream: `async for` stops here, and the Python layer raises
/// `porto.EndOfChannel` from `recv()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(END_OF_CHANNEL)
    }
}

impl Error for RecvError {}

/// [`Receiver::try_recv`](super::Receiver::try_recv) got no value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// Nothing is buffered yet, but senders remain. The Python layer raises
    /// `porto.WouldBlock`.
    Empty,
    /// As for [`RecvError`].
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str(
                "the channel is empty; await recv() to wait for a value, or try again later",
            ),
            Self::Closed => f.write_str(END_OF_CHANNEL),
        }
    }
}

impl Error for TryRecvError {}

impl From<RecvError> for TryRecvError {
    fn from(_: RecvError) -> Self {
        Self::Closed
    }
}

const END_OF_CHANNEL: &str = "the channel has ended: every sender was closed or dropped and \
     every value was received; stop receiving (this is the normal end of the stream)";
