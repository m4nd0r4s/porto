use std::thread;
use std::time::Instant;

use super::*;
use crate::backend::IoBackend;

#[test]
fn turn_honours_its_timeout() {
    let driver = MioDriver::new().unwrap();
    let start = Instant::now();
    assert_eq!(driver.turn(Some(Duration::from_millis(50))).unwrap(), 0);
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(45),
        "returned after {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "returned after {elapsed:?}"
    );
}

#[test]
fn a_zero_timeout_polls_without_blocking() {
    let driver = MioDriver::new().unwrap();
    let start = Instant::now();
    assert_eq!(driver.turn(Some(Duration::ZERO)).unwrap(), 0);
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[test]
fn the_waker_unblocks_a_turn_from_another_thread() {
    let driver = Arc::new(MioDriver::new().unwrap());
    let remote = Arc::clone(&driver);
    let waking = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        remote.waker().wake().unwrap();
    });
    let start = Instant::now();
    assert_eq!(
        driver.turn(None).unwrap(),
        0,
        "the waker's own event is not a dispatch"
    );
    assert!(start.elapsed() < Duration::from_secs(10));
    waking.join().unwrap();
}

#[test]
fn a_wake_before_the_turn_makes_it_return_at_once() {
    let driver = MioDriver::new().unwrap();
    driver.waker().wake().unwrap();
    let start = Instant::now();
    assert_eq!(driver.turn(Some(Duration::from_secs(10))).unwrap(), 0);
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[test]
fn kind_is_the_native_backend() {
    let kind = MioDriver::new().unwrap().kind();
    assert_eq!(kind, NATIVE_DRIVER_KIND);
    #[cfg(target_os = "linux")]
    assert_eq!(kind, DriverKind::Epoll);
    #[cfg(target_os = "macos")]
    assert_eq!(kind, DriverKind::Kqueue);
    #[cfg(windows)]
    assert_eq!(kind, DriverKind::Iocp);
}

#[test]
fn open_falls_back_with_a_warning_or_fails_when_strict() {
    let lenient = IoBackendConfig {
        backend: IoBackend::IoUring,
        strict: false,
    };
    let (driver, warning) = MioDriver::open(lenient).unwrap();
    assert_eq!(driver.kind(), NATIVE_DRIVER_KIND);
    assert_eq!(warning.unwrap().requested(), IoBackend::IoUring);

    let strict = IoBackendConfig {
        backend: IoBackend::IoUring,
        strict: true,
    };
    let error = MioDriver::open(strict).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    let inner = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<UnavailableBackend>())
        .expect("the error wraps an UnavailableBackend");
    assert!(inner.is_strict());
    assert!(error.to_string().contains("io_backend_strict=False"));

    let (_, none) = MioDriver::open(IoBackendConfig::default()).unwrap();
    assert_eq!(none, None);
}

#[test]
fn ids_never_registered_here_are_refused() {
    let driver = MioDriver::new().unwrap();
    let foreign = RegistrationId::new(3, 0);
    let error = driver.deregister(foreign).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert!(error.to_string().contains("register the resource again"));
    let error = driver.reregister(foreign, Interest::READABLE).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[cfg(windows)]
#[test]
fn registering_a_socket_on_windows_says_it_is_not_supported_yet() {
    let driver = MioDriver::new().unwrap();
    let error = driver
        .register(RawSource::new(0), Interest::READABLE)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert!(error.to_string().contains("run this on Linux or macOS"));
}

#[cfg(unix)]
mod unix {
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::task::Context;

    use super::*;
    use crate::Direction;
    use crate::test_util::CountingWaker;

    const SHORT: Option<Duration> = Some(Duration::from_millis(20));
    const LONG: Option<Duration> = Some(Duration::from_secs(5));

    fn socketpair() -> (UnixStream, UnixStream) {
        let (left, right) = UnixStream::pair().unwrap();
        left.set_nonblocking(true).unwrap();
        right.set_nonblocking(true).unwrap();
        (left, right)
    }

    fn register(driver: &MioDriver, stream: &UnixStream, interest: Interest) -> RegistrationId {
        driver
            .register(RawSource::new(stream.as_raw_fd()), interest)
            .unwrap()
    }

    fn readiness(driver: &MioDriver, id: RegistrationId, interest: Interest) -> Ready {
        driver
            .slab()
            .get(id)
            .unwrap()
            .ready_event(id, interest)
            .ready()
    }

    #[test]
    fn socketpair_readiness_reaches_the_slot_and_wakes_the_reader() {
        let driver = MioDriver::new().unwrap();
        let (mut left, right) = socketpair();
        let id = register(&driver, &right, Interest::READABLE | Interest::WRITABLE);

        assert_eq!(driver.turn(LONG).unwrap(), 1, "a fresh socket is writable");
        assert!(readiness(&driver, id, Interest::WRITABLE).is_writable());
        assert!(readiness(&driver, id, Interest::READABLE).is_empty());

        let (reader, waker) = CountingWaker::new();
        let io = driver.slab().get(id).unwrap();
        let poll = io.poll_readiness(&mut Context::from_waker(&waker), id, Direction::Read);
        assert!(poll.is_pending());

        left.write_all(b"ping").unwrap();
        assert_eq!(driver.turn(LONG).unwrap(), 1);
        assert_eq!(reader.wakes(), 1);
        assert!(readiness(&driver, id, Interest::READABLE).is_readable());

        // kqueue reports the hang-up on both its read and write filters, epoll in one event.
        drop(left);
        assert!(driver.turn(LONG).unwrap() >= 1);
        assert!(readiness(&driver, id, Interest::READABLE).is_read_closed());
    }

    #[test]
    fn readiness_is_edge_triggered() {
        let driver = MioDriver::new().unwrap();
        let (mut left, mut right) = socketpair();
        let id = register(&driver, &right, Interest::READABLE);

        left.write_all(b"one").unwrap();
        assert_eq!(driver.turn(LONG).unwrap(), 1);
        assert_eq!(
            driver.turn(SHORT).unwrap(),
            0,
            "unread data does not fire again without a new edge"
        );

        // The consumer drains the socket, hits WouldBlock and clears what it saw.
        let io = driver.slab().get(id).unwrap();
        let event = io.ready_event(id, Interest::READABLE);
        let mut buffer = [0; 16];
        assert_eq!(right.read(&mut buffer).unwrap(), 3);
        let blocked = right.read(&mut buffer).unwrap_err();
        assert_eq!(blocked.kind(), io::ErrorKind::WouldBlock);
        io.clear_readiness(event);
        assert!(readiness(&driver, id, Interest::READABLE).is_empty());

        left.write_all(b"two").unwrap();
        assert_eq!(driver.turn(LONG).unwrap(), 1, "new data is a new edge");
        assert!(readiness(&driver, id, Interest::READABLE).is_readable());
    }

    #[test]
    fn reregister_changes_the_interest() {
        let driver = MioDriver::new().unwrap();
        let (mut left, right) = socketpair();
        let id = register(&driver, &right, Interest::READABLE);
        assert_eq!(driver.turn(SHORT).unwrap(), 0, "writability is not watched");

        driver.reregister(id, Interest::WRITABLE).unwrap();
        assert_eq!(driver.turn(LONG).unwrap(), 1);
        assert!(readiness(&driver, id, Interest::WRITABLE).is_writable());

        left.write_all(b"ignored").unwrap();
        assert_eq!(driver.turn(SHORT).unwrap(), 0, "readability is not watched");
    }

    #[test]
    fn deregister_wakes_waiters_with_closed_and_stops_events() {
        let driver = MioDriver::new().unwrap();
        let (mut left, right) = socketpair();
        let id = register(&driver, &right, Interest::READABLE);
        let io = driver.slab().get(id).unwrap();
        let (reader, waker) = CountingWaker::new();
        let poll = io.poll_readiness(&mut Context::from_waker(&waker), id, Direction::Read);
        assert!(poll.is_pending());

        driver.deregister(id).unwrap();
        assert_eq!(reader.wakes(), 1);
        assert!(driver.slab().get(id).is_none());
        assert_eq!(
            driver.deregister(id).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );

        left.write_all(b"late").unwrap();
        assert_eq!(driver.turn(SHORT).unwrap(), 0);
    }

    #[test]
    fn events_for_a_stale_generation_are_ignored() {
        let driver = MioDriver::new().unwrap();
        let (mut old_left, old_right) = socketpair();
        let old = register(&driver, &old_right, Interest::READABLE);
        old_left.write_all(b"queued").unwrap();

        // The slot is released while the kernel still holds an event under the old token, as
        // when a deregistration races with a turn, and a new registration takes the slot.
        assert!(driver.slab().release(old));
        let (_new_left, new_right) = socketpair();
        let new = register(&driver, &new_right, Interest::READABLE);
        assert_eq!(new.index(), old.index());
        assert_ne!(new, old);
        let (reader, waker) = CountingWaker::new();
        let io = driver.slab().get(new).unwrap();
        let poll = io.poll_readiness(&mut Context::from_waker(&waker), new, Direction::Read);
        assert!(poll.is_pending());

        assert_eq!(driver.turn(SHORT).unwrap(), 0, "the stale event is dropped");
        assert_eq!(reader.wakes(), 0, "the new owner is not woken");
        assert!(readiness(&driver, new, Interest::READABLE).is_empty());
    }

    #[test]
    fn a_failed_registration_frees_its_slot() {
        let driver = MioDriver::new().unwrap();
        let error = driver
            .register(RawSource::new(-1), Interest::READABLE)
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc_ebadf()));
        let (_left, right) = socketpair();
        let id = register(&driver, &right, Interest::READABLE);
        assert_eq!(
            id.index(),
            0,
            "the slot of the failed registration was reused"
        );
    }

    /// `EBADF` is 9 on every Unix porto targets.
    const fn libc_ebadf() -> i32 {
        9
    }
}
