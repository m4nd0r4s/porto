//! The buffered reads of a stream (17-networking-tls-dns.md §1.1): `read_exact`, `read_until`,
//! `readline` and `read_to_end`, as one [`BufferedRead`] that receives into the stream's
//! [`RecvBuffer`] until it can complete, then takes its result out of it.
//!
//! An attempt checks the buffer first, then receives and checks again, until it completes or the
//! socket has nothing more (`WouldBlock`, the operation waits and attempts again). It takes bytes
//! out only on completing, so a cancelled read leaves everything it received in the buffer (T3),
//! and the next read of any kind sees it. The buffer never holds more than the operation's
//! bound (`n`, `limit`) plus what earlier reads left.

use std::io;
use std::sync::Arc;

use parking_lot::Mutex;
use porto_net::scan;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use socket2::Socket;

use super::recv_buffer::RecvBuffer;
use crate::errors::{IncompleteReadError, LimitOverrunError};

/// A stream's receive buffer, shared by the stream and the operations reading it.
pub(crate) type SharedRecvBuffer = Arc<Mutex<RecvBuffer>>;

/// The most `read_exact` makes room for at a time (it grows by this much, not by `n`, so a huge
/// `n` that the peer never sends does not allocate).
const EXACT_CHUNK: usize = 64 << 10;
/// What a search for a separator makes room for at a time, and what `read_to_end` does.
const SEARCH_CHUNK: usize = 4 << 10;
const TO_END_CHUNK: usize = 16 << 10;

/// How many bytes one attempt receives before it yields the lane: a peer that sends faster than
/// the lane reads would otherwise hold it for up to the `limit` of a `read_to_end`.
const YIELD_AFTER: usize = 1 << 20;

/// The error an attempt returns after [`YIELD_AFTER`] bytes without completing: the operation
/// yields its lane and attempts again, readiness not cleared (more may be waiting).
#[derive(Debug)]
pub(crate) struct YieldLane;

impl YieldLane {
    fn error() -> io::Error {
        io::Error::other(Self)
    }

    /// Whether `error` is the request to yield.
    pub(crate) fn is(error: &io::Error) -> bool {
        error.get_ref().is_some_and(|inner| inner.is::<Self>())
    }
}

impl std::fmt::Display for YieldLane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the read yields the lane before it receives more")
    }
}

impl std::error::Error for YieldLane {}

/// What a buffered read waits for.
pub(crate) enum ReadMode {
    /// Exactly `n` bytes.
    Exact(usize),
    /// Up to and including `separator`, which must end within the first `limit` bytes. `call`
    /// names the method for the error messages (`read_until()`, `readline()`).
    Until {
        call: &'static str,
        separator: Vec<u8>,
        limit: usize,
    },
    /// Everything up to the end of the stream, at most `limit` bytes.
    ToEnd { limit: usize },
}

/// How a buffered read ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The bytes: taken out of the buffer.
    Bytes(Vec<u8>),
    /// The stream ended first: `partial` is what arrived (taken out of the buffer), `expected` the
    /// count `read_exact` asked for (`None` when reading up to a separator).
    Incomplete {
        partial: Vec<u8>,
        expected: Option<usize>,
    },
    /// The bound was reached first; `buffered` bytes stay in the buffer.
    Overrun {
        call: &'static str,
        limit: usize,
        buffered: usize,
        separator: bool,
    },
}

/// One buffered read, between attempts.
pub(crate) struct BufferedRead {
    buffer: SharedRecvBuffer,
    mode: ReadMode,
    /// For `Until`: how many buffered bytes were searched without finding the separator, so an
    /// attempt resumes there and a delimiter that arrives in many small segments is not scanned
    /// for from the start each time.
    searched: usize,
    /// Whether the last attempt ended with a receive that found the socket empty.
    drained: bool,
}

impl BufferedRead {
    pub(crate) fn new(buffer: SharedRecvBuffer, mode: ReadMode) -> Self {
        Self {
            buffer,
            mode,
            searched: 0,
            drained: false,
        }
    }

    /// Whether the last [`attempt`](Self::attempt) ended with a receive that got less than it
    /// had room for: the socket had nothing more then.
    pub(crate) fn drained(&self) -> bool {
        self.drained
    }

    /// Completes from what is buffered already, without the socket: the outcome, or `None`
    /// when more has to be received.
    pub(crate) fn complete_from_buffer(&mut self) -> Option<Outcome> {
        let buffer = Arc::clone(&self.buffer);
        let mut buffer = buffer.lock();
        self.check(&mut buffer)
    }

    fn check(&mut self, buffer: &mut RecvBuffer) -> Option<Outcome> {
        match &self.mode {
            ReadMode::Exact(count) => {
                (buffer.len() >= *count).then(|| Outcome::Bytes(buffer.take(*count)))
            }
            ReadMode::Until {
                call,
                separator,
                limit,
            } => {
                let data = buffer.data();
                let from = self.searched.saturating_sub(separator.len() - 1);
                if let Some(at) = scan::find(data, separator, from) {
                    let end = at + separator.len();
                    return Some(if end <= *limit {
                        Outcome::Bytes(buffer.take(end))
                    } else {
                        Outcome::Overrun {
                            call,
                            limit: *limit,
                            buffered: data.len(),
                            separator: true,
                        }
                    });
                }
                self.searched = data.len();
                (data.len() >= *limit).then_some(Outcome::Overrun {
                    call,
                    limit: *limit,
                    buffered: data.len(),
                    separator: true,
                })
            }
            ReadMode::ToEnd { limit } => (buffer.len() > *limit).then(|| Outcome::Overrun {
                call: "read_to_end()",
                limit: *limit,
                buffered: buffer.len(),
                separator: false,
            }),
        }
    }

    /// How many more bytes the read may take in, and how many it expects to: the arguments of
    /// [`RecvBuffer::fill`].
    fn room_and_want(&self, buffered: usize) -> (usize, usize) {
        match &self.mode {
            ReadMode::Exact(count) => {
                let room = count - buffered;
                (room, room.min(EXACT_CHUNK))
            }
            ReadMode::Until { limit, .. } => {
                let room = limit - buffered;
                (room, room.min(SEARCH_CHUNK))
            }
            ReadMode::ToEnd { limit } => {
                let room = limit.saturating_add(1) - buffered;
                (room, room.min(TO_END_CHUNK))
            }
        }
    }

    /// Attempts the read once: receives while the socket has bytes and the buffer cannot
    /// answer yet.
    ///
    /// # Errors
    ///
    /// `WouldBlock` when the socket has nothing more and the read is not complete (what it
    /// received stays buffered), [`YieldLane`] after [`YIELD_AFTER`] bytes without completing, or
    /// the receive's error.
    pub(crate) fn attempt(&mut self, socket: &Socket) -> io::Result<Outcome> {
        let shared = Arc::clone(&self.buffer);
        let mut buffer = shared.lock();
        let mut received = 0;
        self.drained = false;
        loop {
            if let Some(outcome) = self.check(&mut buffer) {
                return Ok(outcome);
            }
            if received >= YIELD_AFTER {
                return Err(YieldLane::error());
            }
            let (room, want) = self.room_and_want(buffer.len());
            let arrived = buffer.fill(socket, room, want)?;
            if arrived > 0 {
                received += arrived;
                self.drained = buffer.last_receive_short();
                continue;
            }
            // The end of the stream: nothing more will arrive.
            let partial = buffer.take(usize::MAX);
            return Ok(match self.mode {
                ReadMode::Exact(count) => Outcome::Incomplete {
                    partial,
                    expected: Some(count),
                },
                ReadMode::Until { .. } => Outcome::Incomplete {
                    partial,
                    expected: None,
                },
                ReadMode::ToEnd { .. } => Outcome::Bytes(partial),
            });
        }
    }
}

impl Outcome {
    /// The Python value of the outcome, or the error it raises.
    pub(crate) fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Bytes(bytes) => Ok(PyBytes::new(py, &bytes).into_any().unbind()),
            Self::Incomplete { partial, expected } => Err(IncompleteReadError::new_err((
                PyBytes::new(py, &partial).unbind(),
                expected,
            ))),
            Self::Overrun {
                call,
                limit,
                buffered,
                separator,
            } => {
                let found = if separator {
                    format!("found no separator in the first {limit} bytes")
                } else {
                    format!("reached its limit of {limit} bytes before the stream ended")
                };
                Err(LimitOverrunError::new_err((
                    format!(
                        "porto: {call} {found}; the {buffered} bytes received so far are still \
                     buffered (nothing was consumed): read them with read(), call again with a \
                     larger limit, or close the stream if the peer should not send this much"
                    ),
                    buffered,
                )))
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    use super::*;

    fn pair() -> (Socket, UnixStream, SharedRecvBuffer) {
        let (writer, reader) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        (
            Socket::from(reader),
            writer,
            Arc::new(Mutex::new(RecvBuffer::default())),
        )
    }

    fn until(buffer: &SharedRecvBuffer, separator: &[u8], limit: usize) -> BufferedRead {
        BufferedRead::new(
            Arc::clone(buffer),
            ReadMode::Until {
                call: "read_until()",
                separator: separator.to_vec(),
                limit,
            },
        )
    }

    fn would_block(result: io::Result<Outcome>) {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn read_exact_waits_for_all_of_its_bytes_and_receives_no_more() {
        let (socket, mut writer, buffer) = pair();
        let mut read = BufferedRead::new(Arc::clone(&buffer), ReadMode::Exact(6));
        writer.write_all(b"abc").unwrap();
        would_block(read.attempt(&socket));
        assert_eq!(buffer.lock().data(), b"abc", "kept for the next attempt");
        writer.write_all(b"defgh").unwrap();
        assert_eq!(
            read.attempt(&socket).unwrap(),
            Outcome::Bytes(b"abcdef".to_vec())
        );
        assert!(
            buffer.lock().is_empty(),
            "it asked the socket for no more than it needed"
        );
        let mut next = BufferedRead::new(buffer, ReadMode::Exact(2));
        assert_eq!(
            next.attempt(&socket).unwrap(),
            Outcome::Bytes(b"gh".to_vec())
        );
    }

    #[test]
    fn read_exact_of_nothing_completes_at_once() {
        let (_socket, _writer, buffer) = pair();
        let mut read = BufferedRead::new(buffer, ReadMode::Exact(0));
        assert_eq!(
            read.complete_from_buffer(),
            Some(Outcome::Bytes(Vec::new()))
        );
    }

    #[test]
    fn the_end_of_the_stream_before_read_exact_is_done_reports_the_partial_bytes() {
        let (socket, mut writer, buffer) = pair();
        writer.write_all(b"abc").unwrap();
        drop(writer);
        let mut read = BufferedRead::new(Arc::clone(&buffer), ReadMode::Exact(10));
        assert_eq!(
            read.attempt(&socket).unwrap(),
            Outcome::Incomplete {
                partial: b"abc".to_vec(),
                expected: Some(10)
            }
        );
        assert!(buffer.lock().is_empty(), "the partial bytes were consumed");
    }

    #[test]
    fn read_until_includes_the_separator_and_leaves_what_follows() {
        let (socket, mut writer, buffer) = pair();
        writer.write_all(b"one\r\ntwo\r\n").unwrap();
        let mut first = until(&buffer, b"\r\n", 100);
        assert_eq!(
            first.attempt(&socket).unwrap(),
            Outcome::Bytes(b"one\r\n".to_vec())
        );
        let mut second = until(&buffer, b"\r\n", 100);
        assert_eq!(
            second.complete_from_buffer(),
            Some(Outcome::Bytes(b"two\r\n".to_vec()))
        );
    }

    #[test]
    fn read_until_finds_a_separator_split_over_many_segments() {
        let (socket, mut writer, buffer) = pair();
        let mut read = until(&buffer, b"\r\n\r\n", 100);
        for segment in [&b"GET / HT"[..], b"TP/1.1\r", b"\n", b"\r", b"\nrest"] {
            would_block(read.attempt(&socket));
            writer.write_all(segment).unwrap();
        }
        assert_eq!(
            read.attempt(&socket).unwrap(),
            Outcome::Bytes(b"GET / HTTP/1.1\r\n\r\n".to_vec())
        );
        assert_eq!(buffer.lock().data(), b"rest");
    }

    #[test]
    fn read_until_without_the_separator_within_the_limit_is_an_overrun_that_consumes_nothing() {
        let (socket, mut writer, buffer) = pair();
        writer.write_all(&[b'x'; 50]).unwrap();
        let mut read = until(&buffer, b"\n", 10);
        assert_eq!(
            read.attempt(&socket).unwrap(),
            Outcome::Overrun {
                call: "read_until()",
                limit: 10,
                buffered: 10,
                separator: true
            }
        );
        assert_eq!(
            buffer.lock().len(),
            10,
            "the buffer never grew past the limit"
        );
    }

    #[test]
    fn a_separator_that_ends_past_the_limit_is_an_overrun() {
        let (_socket, _writer, buffer) = pair();
        buffer_with(&buffer, b"abcdefgh\n");
        let mut read = until(&buffer, b"\n", 5);
        assert!(matches!(
            read.complete_from_buffer(),
            Some(Outcome::Overrun { .. })
        ));
        let mut read = until(&buffer, b"\n", 9);
        assert_eq!(
            read.complete_from_buffer(),
            Some(Outcome::Bytes(b"abcdefgh\n".to_vec()))
        );
    }

    /// Puts `bytes` into the buffer, as a receive would.
    fn buffer_with(buffer: &SharedRecvBuffer, bytes: &[u8]) {
        let (socket, mut writer) = {
            let (socket, writer, _) = pair();
            (socket, writer)
        };
        writer.write_all(bytes).unwrap();
        assert_eq!(
            buffer
                .lock()
                .fill(&socket, bytes.len(), bytes.len())
                .unwrap(),
            bytes.len()
        );
    }

    #[test]
    fn the_end_of_the_stream_before_the_separator_reports_the_partial_bytes() {
        let (socket, mut writer, buffer) = pair();
        writer.write_all(b"no newline").unwrap();
        drop(writer);
        let mut read = until(&buffer, b"\n", 100);
        assert_eq!(
            read.attempt(&socket).unwrap(),
            Outcome::Incomplete {
                partial: b"no newline".to_vec(),
                expected: None
            }
        );
    }

    #[test]
    fn read_to_end_returns_everything_at_the_end_of_the_stream() {
        let (socket, mut writer, buffer) = pair();
        writer.write_all(&[b'z'; 6_000]).unwrap();
        drop(writer);
        let mut read = BufferedRead::new(buffer, ReadMode::ToEnd { limit: 1 << 20 });
        let Outcome::Bytes(bytes) = read.attempt(&socket).unwrap() else {
            panic!("expected the bytes");
        };
        assert_eq!(bytes.len(), 6_000);
    }

    #[test]
    fn read_to_end_over_its_limit_is_an_overrun_that_keeps_the_bytes() {
        let (socket, mut writer, buffer) = pair();
        writer.write_all(&[b'z'; 100]).unwrap();
        let mut read = BufferedRead::new(Arc::clone(&buffer), ReadMode::ToEnd { limit: 60 });
        assert_eq!(
            read.attempt(&socket).unwrap(),
            Outcome::Overrun {
                call: "read_to_end()",
                limit: 60,
                buffered: 61,
                separator: false
            }
        );
        assert_eq!(buffer.lock().len(), 61);
    }

    #[test]
    fn read_to_end_of_exactly_the_limit_succeeds() {
        let (socket, mut writer, buffer) = pair();
        writer.write_all(&[b'z'; 60]).unwrap();
        drop(writer);
        let mut read = BufferedRead::new(buffer, ReadMode::ToEnd { limit: 60 });
        assert_eq!(
            read.attempt(&socket).unwrap(),
            Outcome::Bytes(vec![b'z'; 60])
        );
    }

    #[test]
    fn an_abandoned_read_loses_nothing() {
        let (socket, mut writer, buffer) = pair();
        writer.write_all(b"par").unwrap();
        let mut abandoned = until(&buffer, b"\n", 100);
        would_block(abandoned.attempt(&socket));
        drop(abandoned); // what a cancellation does
        writer.write_all(b"tial\nnext").unwrap();
        let mut again = until(&buffer, b"\n", 100);
        assert_eq!(
            again.attempt(&socket).unwrap(),
            Outcome::Bytes(b"partial\n".to_vec())
        );
    }
}
