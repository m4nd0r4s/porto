//! The registration lifecycle over real sockets (13-io-driver.md §4): register and
//! deregister, readiness through the driver, close while other tasks wait or operate, and no
//! use after close when the slot or the handle number is reused.

#![cfg(unix)]

use std::future::Future;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use super::*;
use crate::MioDriver;
use crate::Ready;
use crate::test_util::{CountingWaker, SlabDriver};

const LONG: Option<Duration> = Some(Duration::from_secs(5));
const SHORT: Option<Duration> = Some(Duration::from_millis(20));

fn driver() -> Arc<MioDriver> {
    Arc::new(MioDriver::new().unwrap())
}

fn socketpair() -> (UnixStream, UnixStream) {
    let (left, right) = UnixStream::pair().unwrap();
    left.set_nonblocking(true).unwrap();
    right.set_nonblocking(true).unwrap();
    (left, right)
}

fn register(driver: &Arc<MioDriver>, stream: UnixStream) -> Registration<UnixStream> {
    let driver: Arc<dyn Driver> = driver.clone();
    Registration::new(driver, stream, Interest::READABLE | Interest::WRITABLE).unwrap()
}

fn read(registration: &Registration<UnixStream>) -> io::Result<Vec<u8>> {
    registration.try_io(Interest::READABLE, |mut stream| {
        let mut buffer = [0; 64];
        let read = stream.read(&mut buffer)?;
        Ok(buffer[..read].to_vec())
    })
}

fn assert_closed(error: &io::Error) {
    let closed = ClosedResource::of(error).unwrap_or_else(|| panic!("not closed: {error}"));
    assert!(!closed.by_driver());
    assert_eq!(ClosedResource::ERRNO, 9, "EBADF");
}

fn assert_poll_closed(poll: Poll<io::Result<ReadyEvent>>) {
    match poll {
        Poll::Ready(Err(error)) => assert_closed(&error),
        other => panic!("expected a closed error, got {other:?}"),
    }
}

/// No OS calls, so miri runs it: the readiness future's pin projection, the close waking it,
/// and the close gate behind a registration.
#[test]
fn readiness_and_close_without_an_os_driver() {
    let driver: Arc<dyn Driver> = Arc::new(SlabDriver::new());
    let registration =
        Registration::new(Arc::clone(&driver), io::stdin(), Interest::READABLE).unwrap();
    let (waiter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);
    {
        let mut ready = pin!(registration.readiness(Interest::READABLE));
        assert!(ready.as_mut().poll(&mut cx).is_pending());
        assert!(driver.slab().dispatch(registration.id(), Ready::READABLE));
        assert_eq!(waiter.wakes(), 1);
        match ready.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(event)) => assert!(event.ready().is_readable()),
            other => panic!("expected readiness, got {other:?}"),
        }
    }
    let mut waiting = pin!(registration.readiness(Interest::WRITABLE));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    let source = registration.source().unwrap();
    registration.close().unwrap();
    assert_eq!(waiter.wakes(), 2, "the close woke the waiter");
    assert_poll_closed(waiting.as_mut().poll(&mut cx));
    drop(source);
    assert!(!driver.has_registrations());
}

#[test]
fn a_registration_is_driven_by_the_driver_it_was_made_with_only() {
    let (_peer, stream) = socketpair();
    let first = driver();
    let second = driver();
    let registration = register(&first, stream);
    let first: Arc<dyn Driver> = first;
    let second: Arc<dyn Driver> = second;
    assert!(registration.is_driven_by(&first));
    assert!(!registration.is_driven_by(&second));
}

#[test]
fn closed_completes_when_the_registration_is_closed() {
    let driver = driver();
    let (_peer, stream) = socketpair();
    let registration = register(&driver, stream);
    let mut closed = std::pin::pin!(registration.closed());
    let (counter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);
    assert!(closed.as_mut().poll(&mut cx).is_pending());
    registration.close().unwrap();
    assert_eq!(counter.wakes(), 1, "close woke the waiter");
    assert!(closed.as_mut().poll(&mut cx).is_ready());
    // Already closed: ready at once.
    assert!(
        std::pin::pin!(registration.closed())
            .poll(&mut cx)
            .is_ready()
    );
}

#[test]
fn closed_ignores_readiness_including_a_hang_up_whose_bits_never_clear() {
    let driver = driver();
    let (peer, stream) = socketpair();
    let registration = register(&driver, stream);
    let mut closed = std::pin::pin!(registration.closed());
    let (counter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);
    assert!(closed.as_mut().poll(&mut cx).is_pending());
    // The peer shuts down: the driver reports readable and read-closed, and the closed bits
    // stay set for good.
    peer.shutdown(std::net::Shutdown::Both).unwrap();
    driver.turn(LONG).unwrap();
    let event = registration.readiness(Interest::READABLE);
    let mut event = std::pin::pin!(event);
    assert!(
        event.as_mut().poll(&mut cx).is_ready(),
        "the hang-up is readiness"
    );
    // However often it is polled, `closed` stays pending: no loop around it can spin.
    for _ in 0..1000 {
        assert!(closed.as_mut().poll(&mut cx).is_pending());
    }
    assert!(
        counter.wakes() <= 2,
        "at most the wakes the hang-up itself caused"
    );
    registration.close().unwrap();
    assert!(closed.as_mut().poll(&mut cx).is_ready());
}

#[test]
fn new_registers_and_drop_deregisters() {
    let driver = driver();
    let (_left, right) = socketpair();
    let registration = register(&driver, right);
    assert!(driver.has_registrations());
    let id = registration.id();
    assert!(driver.slab().get(id).is_some());
    drop(registration);
    assert!(!driver.has_registrations());
    assert!(
        driver.slab().get(id).is_none(),
        "the slot moved to its next generation"
    );
}

#[test]
fn try_io_reads_once_ready_and_clears_on_would_block() {
    let driver = driver();
    let (mut left, right) = socketpair();
    let registration = register(&driver, right);
    assert_eq!(
        read(&registration).unwrap_err().kind(),
        io::ErrorKind::WouldBlock,
        "not ready before the driver reported it"
    );
    left.write_all(b"ping").unwrap();
    assert!(driver.turn(LONG).unwrap() >= 1);
    assert_eq!(read(&registration).unwrap(), b"ping");
    // The next read hits WouldBlock and clears the readiness it consumed.
    assert_eq!(
        read(&registration).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let event = registration
        .scheduled_io()
        .ready_event(registration.id(), Interest::READABLE);
    assert!(event.ready().is_empty(), "readiness cleared");
}

#[test]
fn attempt_io_reads_before_the_driver_reports_and_clears_on_would_block() {
    let driver = driver();
    let (mut left, right) = socketpair();
    let registration = register(&driver, right);
    left.write_all(b"early").unwrap();
    let read_now = |registration: &Registration<UnixStream>| {
        registration.attempt_io(Interest::READABLE, |mut stream| {
            let mut buffer = [0; 64];
            let read = stream.read(&mut buffer)?;
            Ok(buffer[..read].to_vec())
        })
    };
    assert_eq!(
        read_now(&registration).unwrap(),
        b"early",
        "the fast path reads without waiting for the driver"
    );
    // Once the driver reported readiness, an attempt that hits `WouldBlock` clears it.
    left.write_all(b"late").unwrap();
    assert!(driver.turn(LONG).unwrap() >= 1);
    let reported = registration
        .scheduled_io()
        .ready_event(registration.id(), Interest::READABLE);
    assert!(reported.ready().is_readable());
    assert_eq!(read_now(&registration).unwrap(), b"late");
    assert_eq!(
        read_now(&registration).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let event = registration
        .scheduled_io()
        .ready_event(registration.id(), Interest::READABLE);
    assert!(event.ready().is_empty(), "the stale readiness was cleared");
    registration.close().unwrap();
    assert_closed(&read_now(&registration).unwrap_err());
}

#[test]
fn attempt_io_when_ready_skips_the_syscall_once_readiness_was_cleared() {
    let driver = driver();
    let (mut left, right) = socketpair();
    let registration = register(&driver, right);
    let calls = std::cell::Cell::new(0_usize);
    // Reads up to `room` bytes; `touched` is what `f` was told.
    let read_now = |registration: &Registration<UnixStream>, room: usize| {
        registration.attempt_io_when_ready(Interest::READABLE, |mut stream, ready| {
            calls.set(calls.get() + 1);
            if !ready {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let mut buffer = vec![0; room];
            let read = stream.read(&mut buffer)?;
            Ok((buffer[..read].to_vec(), read < room))
        })
    };
    // Fresh: the driver has reported nothing, so the socket is tried (data may be queued).
    left.write_all(b"early").unwrap();
    assert_eq!(read_now(&registration, 64).unwrap(), b"early");
    // Never observed and nothing there: tried once more, and not an error to retry.
    assert_eq!(
        read_now(&registration, 64).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    // The driver reports data: ready, read, and the short read drains the readiness.
    left.write_all(b"ping").unwrap();
    assert!(driver.turn(LONG).unwrap() >= 1);
    assert_eq!(read_now(&registration, 64).unwrap(), b"ping");
    let readiness = |registration: &Registration<UnixStream>| {
        registration
            .scheduled_io()
            .ready_event(registration.id(), Interest::READABLE)
            .ready()
    };
    assert!(
        readiness(&registration).is_empty(),
        "a short read forgets readiness"
    );
    // Now the registration was observed and has no readiness: `f` is told not to touch the
    // socket, and a byte that arrived meanwhile is not read until the driver reports it.
    left.write_all(b"x").unwrap();
    let before = calls.get();
    let error = read_now(&registration, 64).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(calls.get(), before + 1, "f ran, told ready = false");
    assert!(driver.turn(LONG).unwrap() >= 1);
    assert_eq!(read_now(&registration, 64).unwrap(), b"x");
    // A full read (no room left) says nothing about the socket: readiness stays.
    left.write_all(b"abcd").unwrap();
    assert!(driver.turn(LONG).unwrap() >= 1);
    assert_eq!(read_now(&registration, 4).unwrap(), b"abcd");
    assert!(
        !readiness(&registration).is_empty(),
        "a full read keeps readiness"
    );
    registration.close().unwrap();
    assert_closed(&read_now(&registration, 4).unwrap_err());
}

#[test]
fn async_io_drain_clears_readiness_after_a_short_read() {
    let driver = driver();
    let (mut left, right) = socketpair();
    let registration = register(&driver, right);
    left.write_all(b"ping").unwrap();
    assert!(driver.turn(LONG).unwrap() >= 1);
    let mut future = pin!(
        registration.async_io_drain(Interest::READABLE, |mut stream| {
            let mut buffer = [0; 64];
            let read = stream.read(&mut buffer)?;
            Ok((buffer[..read].to_vec(), read < buffer.len()))
        })
    );
    let waker = CountingWaker::new();
    let mut cx = Context::from_waker(&waker.1);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(bytes)) => assert_eq!(bytes, b"ping"),
        other => panic!("expected the bytes, got {other:?}"),
    }
    let event = registration
        .scheduled_io()
        .ready_event(registration.id(), Interest::READABLE);
    assert!(
        event.ready().is_empty(),
        "the short read cleared the readiness"
    );
}

#[test]
fn close_wakes_every_waiter_with_a_closed_error() {
    let driver = driver();
    let (_left, right) = socketpair();
    let registration = register(&driver, right);

    let (slot_waiter, slot_waker) = CountingWaker::new();
    let mut slot_cx = Context::from_waker(&slot_waker);
    assert!(
        registration
            .poll_ready(&mut slot_cx, Direction::Read)
            .is_pending()
    );
    let (list_waiter, list_waker) = CountingWaker::new();
    let mut list_cx = Context::from_waker(&list_waker);
    let mut listed = pin!(registration.readiness(Interest::READABLE));
    assert!(listed.as_mut().poll(&mut list_cx).is_pending());

    registration.close().unwrap();
    assert_eq!(slot_waiter.wakes(), 1);
    assert_eq!(list_waiter.wakes(), 1);
    assert_poll_closed(registration.poll_ready(&mut slot_cx, Direction::Read));
    assert_poll_closed(listed.as_mut().poll(&mut list_cx));
    assert_closed(&read(&registration).unwrap_err());
    assert_closed(&registration.source().err().unwrap());
    assert_closed(&registration.reregister(Interest::WRITABLE).unwrap_err());
    assert!(registration.close().is_ok(), "closing twice does nothing");
    assert!(!driver.has_registrations());
}

#[test]
fn close_closes_the_handle_after_the_operation_using_it() {
    let driver = driver();
    let (mut left, right) = socketpair();
    let registration = register(&driver, right);
    left.write_all(b"data").unwrap();
    assert!(driver.turn(LONG).unwrap() >= 1);

    let read = registration
        .try_io(Interest::READABLE, |mut stream| {
            registration.close().unwrap();
            // Closed, but the handle stays open until this operation is done.
            let mut buffer = [0; 8];
            stream.read(&mut buffer)
        })
        .unwrap();
    assert_eq!(read, 4);
    // Now it is closed: the peer sees EOF.
    let mut buffer = [0; 8];
    left.set_nonblocking(false).unwrap();
    assert_eq!(left.read(&mut buffer).unwrap(), 0);
}

#[test]
fn a_waiter_on_another_thread_wakes_when_the_registration_is_closed() {
    for _ in 0..50 {
        let driver = driver();
        let (_left, right) = socketpair();
        let registration = Arc::new(register(&driver, right));
        let waiting = {
            let registration = Arc::clone(&registration);
            thread::spawn(move || block_on(registration.readiness(Interest::READABLE)))
        };
        thread::yield_now();
        registration.close().unwrap();
        assert_closed(&waiting.join().unwrap().unwrap_err());
    }
}

#[test]
fn a_reopened_handle_and_slot_never_reach_the_closed_registration() {
    let driver = driver();
    let (_old_left, old_right) = socketpair();
    let old_fd = old_right.as_raw_fd();
    let old = register(&driver, old_right);
    let (old_waiter, old_waker) = CountingWaker::new();
    let mut old_cx = Context::from_waker(&old_waker);
    assert!(old.poll_ready(&mut old_cx, Direction::Read).is_pending());
    old.close().unwrap();
    assert_eq!(old_waiter.wakes(), 1, "woken by the close");

    // The released slot is reused at once, and so is the lowest free handle number: the old
    // one, unless a test on another thread took it first.
    let (mut new_left, new_right) = socketpair();
    let new_fds = [new_left.as_raw_fd(), new_right.as_raw_fd()];
    let new = register(&driver, new_right);
    assert_eq!(new.id().index(), old.id().index(), "the slot was reused");
    if !new_fds.contains(&old_fd) {
        eprintln!("the handle number {old_fd} was not reused (now {new_fds:?})");
    }

    new_left.write_all(b"for the new owner").unwrap();
    assert!(driver.turn(LONG).unwrap() >= 1);
    assert!(read(&new).is_ok());
    assert_eq!(
        old_waiter.wakes(),
        1,
        "the new owner's event did not wake the old waiter"
    );
    assert_poll_closed(old.poll_ready(&mut old_cx, Direction::Read));
    assert_closed(&read(&old).unwrap_err());
}

#[test]
fn a_closed_registration_stays_closed_past_the_generation_wrap() {
    let driver = driver();
    let (_left, right) = socketpair();
    let old = register(&driver, right);
    old.close().unwrap();
    // 300 reuses of the slot: the 8-bit generation wraps, and an ID-only check would see the
    // old ID live again. The registration's own closed state does not wrap.
    let mut current = None;
    for _ in 0..300 {
        drop(current.take());
        let (left, right) = socketpair();
        current = Some((left, register(&driver, right)));
    }
    let (mut left, new) = current.unwrap();
    assert_eq!(new.id().index(), old.id().index());
    left.write_all(b"x").unwrap();
    assert!(driver.turn(LONG).unwrap() >= 1);
    assert_closed(&read(&old).unwrap_err());
    let (_, waker) = CountingWaker::new();
    assert_poll_closed(old.poll_ready(&mut Context::from_waker(&waker), Direction::Read));
}

#[test]
fn a_driver_shutdown_releases_registrations_with_a_driver_error() {
    let driver = driver();
    let (_left, right) = socketpair();
    let registration = register(&driver, right);
    let (waiter, waker) = CountingWaker::new();
    let mut cx = Context::from_waker(&waker);
    assert!(
        registration
            .poll_ready(&mut cx, Direction::Read)
            .is_pending()
    );
    driver.shutdown();
    assert_eq!(waiter.wakes(), 1);
    match registration.poll_ready(&mut cx, Direction::Read) {
        Poll::Ready(Err(error)) => {
            assert!(ClosedResource::of(&error).unwrap().by_driver());
            assert!(error.to_string().contains("runtime shut down"), "{error}");
        }
        other => panic!("expected a released error, got {other:?}"),
    }
    registration
        .close()
        .expect("closing after a shutdown is fine");
}

#[test]
fn readiness_from_the_driver_wakes_async_io() {
    let driver = driver();
    let (mut left, right) = socketpair();
    let registration = Arc::new(register(&driver, right));
    let reading = {
        let registration = Arc::clone(&registration);
        thread::spawn(move || {
            block_on(registration.async_io(Interest::READABLE, |mut stream| {
                let mut buffer = [0; 8];
                let read = stream.read(&mut buffer)?;
                Ok(buffer[..read].to_vec())
            }))
        })
    };
    thread::sleep(Duration::from_millis(20));
    left.write_all(b"hello").unwrap();
    while !reading.is_finished() {
        driver.turn(SHORT).unwrap();
    }
    assert_eq!(reading.join().unwrap().unwrap(), b"hello");
}

#[test]
fn the_closed_error_says_what_to_do() {
    let message = ClosedResource::closed().to_string();
    assert!(message.contains("was closed"), "{message}");
    assert!(message.contains("open a new one"), "{message}");
}

/// Runs `future` to completion on this thread, parking between polls.
fn block_on<F: Future>(future: F) -> F::Output {
    struct Unparker(thread::Thread);
    impl std::task::Wake for Unparker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}
