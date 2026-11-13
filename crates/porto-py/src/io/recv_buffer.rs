//! [`RecvBuffer`]: the bytes a stream received and nobody consumed yet (17-networking-tls-dns.md
//! §1.1, "internal read buffer").
//!
//! The buffered reads (`read_exact`, `read_until`, `readline`, `read_to_end`) receive into it
//! and take what they return out of it; a plain `read` takes buffered bytes first. Bytes only
//! leave when an operation completes with them, so an operation that is cancelled has lost
//! nothing: what it received is still here for the next read (T3).
//!
//! The storage is a [`PooledBytes`] of the lane's pool (13-io-driver.md §6), taken when the first
//! bytes are buffered and handed back to the pool as soon as the buffer is empty again, so a
//! stream that is idle between messages holds no buffer. Consuming moves a start offset, never
//! the bytes; the unconsumed bytes move to the front of the storage only when more room is needed.

use std::io;

use socket2::Socket;

use super::pool::{self, PooledBytes};

/// The unconsumed bytes of a stream.
#[derive(Default)]
pub(crate) struct RecvBuffer {
    /// The storage: `bytes[start..]` is what is buffered. `None` while nothing is.
    bytes: Option<PooledBytes>,
    /// Where the unconsumed bytes start in `bytes`.
    start: usize,
    /// Whether the last receive got less than it had room for: the socket was empty then.
    last_receive_short: bool,
}

impl RecvBuffer {
    /// How many bytes are buffered.
    pub(crate) fn len(&self) -> usize {
        self.bytes
            .as_ref()
            .map_or(0, |bytes| bytes.len() - self.start)
    }

    /// Whether the last [`fill`](Self::fill) that received anything got less than it had room
    /// for, which means a stream socket had nothing more at that moment.
    pub(crate) fn last_receive_short(&self) -> bool {
        self.last_receive_short
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The buffered bytes.
    pub(crate) fn data(&self) -> &[u8] {
        self.bytes
            .as_ref()
            .map_or(&[], |bytes| &bytes[self.start..])
    }

    /// Takes the first `count` buffered bytes (all of them if fewer are buffered) out.
    pub(crate) fn take(&mut self, count: usize) -> Vec<u8> {
        let taken = self.data()[..count.min(self.len())].to_vec();
        self.consume(taken.len());
        taken
    }

    /// Takes the first bytes (as many as `destination` has room for, at most all) out into
    /// `destination`; how many.
    pub(crate) fn take_into(&mut self, destination: &mut [std::mem::MaybeUninit<u8>]) -> usize {
        let count = destination.len().min(self.len());
        for (slot, &byte) in destination.iter_mut().zip(self.data()) {
            slot.write(byte);
        }
        self.consume(count);
        count
    }

    /// Takes up to `count` buffered bytes out into storage from the lane's pool.
    ///
    /// # Errors
    ///
    /// `OutOfMemory` when the storage cannot be allocated; nothing is consumed then.
    pub(crate) fn take_pooled(&mut self, count: usize) -> io::Result<PooledBytes> {
        let count = count.min(self.len());
        let mut taken = PooledBytes::with_capacity(count)?;
        taken.vec_mut().extend_from_slice(&self.data()[..count]);
        self.consume(count);
        Ok(taken)
    }

    /// Drops the first `count` buffered bytes; the storage goes back to the pool once the
    /// buffer is empty.
    fn consume(&mut self, count: usize) {
        self.start += count;
        if self.is_empty() {
            self.bytes = None;
            self.start = 0;
        }
    }

    /// Receives from `socket` once, appending at most `room` bytes (`room >= 1`) to the buffer;
    /// how many arrived, 0 at the end of the stream. The storage is made to have room for `want`
    /// more bytes first (`want` is what the caller expects to need: more than `room` is wasted).
    ///
    /// # Errors
    ///
    /// `socket.recv`'s error (`WouldBlock` when nothing arrived yet: the buffer is unchanged),
    /// or `OutOfMemory` when the storage cannot grow.
    pub(crate) fn fill(&mut self, socket: &Socket, room: usize, want: usize) -> io::Result<usize> {
        let outcome = self.try_fill(socket, room, want.clamp(1, room));
        // Nothing arrived and nothing is buffered (a read that waits, then is cancelled, or the
        // end of the stream): the storage taken for the receive goes back to the pool, so a
        // stream that is idle between messages holds none, however many reads gave up on it.
        if !matches!(outcome, Ok(received) if received > 0) && self.is_empty() {
            self.bytes = None;
            self.start = 0;
        }
        outcome
    }

    fn try_fill(&mut self, socket: &Socket, room: usize, want: usize) -> io::Result<usize> {
        let bytes = self.make_room(want)?;
        let used = bytes.len();
        let spare = bytes.vec_mut().spare_capacity_mut();
        let size = room.min(spare.len());
        let spare = &mut spare[..size];
        let received = socket.recv(spare)?;
        // SAFETY: `recv` initialized the first `received` bytes after the `used` ones, which
        // are within the capacity.
        unsafe { bytes.vec_mut().set_len(used + received) };
        self.last_receive_short = received < size;
        Ok(received)
    }

    /// The storage with at least `want` bytes of spare capacity, moving or growing it if needed.
    /// Above the pool's largest class the capacity at least doubles, so receiving `n` bytes
    /// copies `O(n)` in all, not `O(n^2)` (a 64 MiB `read_to_end` grows in 16 KiB steps).
    fn make_room(&mut self, want: usize) -> io::Result<&mut PooledBytes> {
        let bytes = match self.bytes.take() {
            Some(bytes) => bytes,
            None => PooledBytes::with_capacity(want)?,
        };
        let mut bytes = bytes;
        let spare = |bytes: &PooledBytes| bytes.vec().capacity() - bytes.len();
        if spare(&bytes) < want && self.start > 0 {
            // Move what is buffered to the front: cheaper than a larger buffer while the
            // unconsumed part is small, and the buffer never grows past what it holds.
            bytes.vec_mut().drain(..self.start);
            self.start = 0;
        }
        let needed = bytes.len() + want;
        let grown = if needed > bytes.vec().capacity() && needed > pool::LARGEST_CLASS {
            let doubled = needed.max(bytes.vec().capacity().saturating_mul(2));
            // If the doubled storage cannot be had, the exact one may.
            bytes
                .reserve_total(doubled)
                .or_else(|_| bytes.reserve_total(needed))
        } else {
            bytes.reserve_total(needed)
        };
        // The storage stays with the buffer whatever happened: a failed growth keeps the bytes.
        let bytes = self.bytes.insert(bytes);
        grown.map(|()| bytes)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    use super::*;

    /// A receiving socket, and the plain stream that writes to it.
    fn pair() -> (Socket, UnixStream) {
        let (writer, reader) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        (Socket::from(reader), writer)
    }

    fn wait_would_block(buffer: &mut RecvBuffer, socket: &Socket) {
        loop {
            match buffer.fill(socket, 1 << 20, 4096) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                Err(error) => panic!("{error}"),
                Ok(0) => panic!("the end of the stream"),
                Ok(_) => {}
            }
        }
    }

    #[test]
    fn an_empty_buffer_holds_no_storage() {
        let buffer = RecvBuffer::default();
        assert!(buffer.is_empty());
        assert_eq!(buffer.data(), b"");
        assert!(buffer.bytes.is_none());
    }

    #[test]
    fn bytes_are_received_and_taken_in_order() {
        let (socket, mut writer) = pair();
        let mut buffer = RecvBuffer::default();
        writer.write_all(b"hello world").unwrap();
        wait_would_block(&mut buffer, &socket);
        assert_eq!(buffer.data(), b"hello world");
        assert_eq!(buffer.take(5), b"hello");
        assert_eq!(buffer.data(), b" world");
        assert_eq!(buffer.take(100), b" world");
        assert!(buffer.is_empty());
        assert!(buffer.bytes.is_none(), "the storage went back to the pool");
    }

    #[test]
    fn a_receive_that_found_nothing_gives_the_storage_back_while_the_buffer_is_empty() {
        let (socket, mut writer) = pair();
        let mut buffer = RecvBuffer::default();
        let error = buffer.fill(&socket, 100, 100).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(buffer.bytes.is_none(), "an idle buffer holds no storage");
        // With bytes buffered, a receive that finds nothing more keeps them and their storage.
        writer.write_all(b"abc").unwrap();
        assert_eq!(buffer.fill(&socket, 100, 100).unwrap(), 3);
        assert!(buffer.fill(&socket, 100, 100).is_err());
        assert_eq!(buffer.data(), b"abc");
        assert!(buffer.bytes.is_some());
        // The end of the stream with nothing buffered also leaves none.
        let (socket, writer) = pair();
        let mut buffer = RecvBuffer::default();
        drop(writer);
        assert_eq!(buffer.fill(&socket, 100, 100).unwrap(), 0);
        assert!(buffer.bytes.is_none());
    }

    #[test]
    fn storage_above_the_largest_class_grows_geometrically() {
        let mut buffer = RecvBuffer::default();
        let mut reallocations = 0;
        let mut capacity = 0;
        for _ in 0..(64 << 20) / (16 << 10) {
            let bytes = buffer.make_room(16 << 10).unwrap();
            let grown = bytes.vec().capacity();
            if grown != capacity {
                reallocations += 1;
                capacity = grown;
            }
            // Pretend 16 KiB arrived.
            let length = bytes.len();
            bytes.vec_mut().resize(length + (16 << 10), 0);
        }
        assert_eq!(buffer.len(), 64 << 20);
        // 4 KiB to 256 KiB is up to four classes; doubling from there to 64 MiB is eight more.
        assert!(
            reallocations <= 16,
            "{reallocations} reallocations for 64 MiB"
        );
    }

    #[test]
    fn a_receive_nothing_arrived_for_leaves_the_buffer_as_it_was() {
        let (socket, mut writer) = pair();
        let mut buffer = RecvBuffer::default();
        let error = buffer.fill(&socket, 100, 100).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(buffer.is_empty());
        writer.write_all(b"abc").unwrap();
        assert_eq!(buffer.fill(&socket, 100, 100).unwrap(), 3);
        assert_eq!(buffer.data(), b"abc");
    }

    #[test]
    fn a_receive_never_takes_more_than_the_room() {
        let (socket, mut writer) = pair();
        let mut buffer = RecvBuffer::default();
        writer.write_all(b"0123456789").unwrap();
        assert_eq!(buffer.fill(&socket, 4, 4096).unwrap(), 4);
        assert_eq!(buffer.data(), b"0123");
    }

    #[test]
    fn the_end_of_the_stream_receives_zero() {
        let (socket, writer) = pair();
        let mut buffer = RecvBuffer::default();
        drop(writer);
        assert_eq!(buffer.fill(&socket, 100, 100).unwrap(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn consumed_bytes_make_room_by_moving_what_is_left_to_the_front() {
        let (socket, mut writer) = pair();
        let mut buffer = RecvBuffer::default();
        writer.write_all(&[b'a'; 3000]).unwrap();
        wait_would_block(&mut buffer, &socket);
        assert_eq!(buffer.take(2900).len(), 2900);
        let capacity = buffer.bytes.as_ref().unwrap().vec().capacity();
        writer.write_all(&[b'b'; 3000]).unwrap();
        wait_would_block(&mut buffer, &socket);
        assert_eq!(buffer.len(), 3100);
        assert_eq!(&buffer.data()[..100], &[b'a'; 100][..]);
        assert_eq!(&buffer.data()[100..], &[b'b'; 3000][..]);
        assert_eq!(
            buffer.bytes.as_ref().unwrap().vec().capacity(),
            capacity,
            "it fit after moving to the front"
        );
    }

    #[test]
    fn the_storage_grows_to_the_next_pool_class_when_it_holds_more() {
        let (socket, mut writer) = pair();
        let mut buffer = RecvBuffer::default();
        // In pieces the socket's own buffer holds: the plain writer blocks on a full one.
        for _ in 0..2 {
            writer.write_all(&[7_u8; 5_000]).unwrap();
            wait_would_block(&mut buffer, &socket);
        }
        assert_eq!(buffer.len(), 10_000);
        assert!(buffer.data().iter().all(|&byte| byte == 7));
        assert!(buffer.bytes.as_ref().unwrap().vec().capacity() >= 10_000);
    }

    #[test]
    fn take_into_and_take_pooled_copy_out_the_first_bytes() {
        let (socket, mut writer) = pair();
        let mut buffer = RecvBuffer::default();
        writer.write_all(b"0123456789").unwrap();
        wait_would_block(&mut buffer, &socket);
        let mut slots = [std::mem::MaybeUninit::<u8>::uninit(); 4];
        assert_eq!(buffer.take_into(&mut slots), 4);
        // SAFETY: `take_into` initialized the 4 slots.
        let copied: Vec<u8> = slots
            .iter()
            .map(|slot| unsafe { slot.assume_init() })
            .collect();
        assert_eq!(copied, b"0123");
        let pooled = buffer.take_pooled(3).unwrap();
        assert_eq!(&*pooled, b"456");
        assert_eq!(buffer.data(), b"789");
        let rest = buffer.take_pooled(100).unwrap();
        assert_eq!(&*rest, b"789");
        assert!(buffer.is_empty());
    }
}
