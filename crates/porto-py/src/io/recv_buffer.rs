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

use super::pool::PooledBytes;

/// The unconsumed bytes of a stream.
#[derive(Default)]
pub(crate) struct RecvBuffer {
    /// The storage: `bytes[start..]` is what is buffered. `None` while nothing is.
    bytes: Option<PooledBytes>,
    /// Where the unconsumed bytes start in `bytes`.
    start: usize,
}

impl RecvBuffer {
    /// How many bytes are buffered.
    pub(crate) fn len(&self) -> usize {
        self.bytes
            .as_ref()
            .map_or(0, |bytes| bytes.len() - self.start)
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
        let want = want.clamp(1, room);
        let bytes = self.make_room(want)?;
        let used = bytes.len();
        let spare = bytes.vec_mut().spare_capacity_mut();
        let size = room.min(spare.len());
        let spare = &mut spare[..size];
        let received = socket.recv(spare)?;
        // SAFETY: `recv` initialized the first `received` bytes after the `used` ones, which
        // are within the capacity.
        unsafe { bytes.vec_mut().set_len(used + received) };
        Ok(received)
    }

    /// The storage with at least `want` bytes of spare capacity, moving or growing it if needed.
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
        let grown = bytes.reserve_total(bytes.len() + want);
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
