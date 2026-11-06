//! Test helpers: a waker that counts its wakes.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

/// Counts how many times its waker was woken.
#[derive(Default)]
pub(crate) struct CountingWaker {
    wakes: AtomicUsize,
}

impl CountingWaker {
    pub(crate) fn new() -> (Arc<Self>, Waker) {
        let counter = Arc::new(Self::default());
        let waker = Waker::from(Arc::clone(&counter));
        (counter, waker)
    }

    pub(crate) fn wakes(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

/// A driver with no OS behind it: registrations only take slab slots, and readiness comes
/// from [`IoSlab::dispatch`](crate::IoSlab::dispatch). Lets miri run the registration code.
pub(crate) struct SlabDriver {
    slab: crate::IoSlab,
    waker: crate::DriverWaker,
}

impl SlabDriver {
    pub(crate) fn new() -> Self {
        Self {
            slab: crate::IoSlab::new(),
            waker: crate::DriverWaker::new(|| Ok(())),
        }
    }
}

impl crate::Driver for SlabDriver {
    fn register(
        &self,
        _src: crate::RawSource,
        _interest: crate::Interest,
    ) -> std::io::Result<crate::RegistrationId> {
        self.slab.allocate()
    }

    fn reregister(
        &self,
        _id: crate::RegistrationId,
        _interest: crate::Interest,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn deregister(&self, id: crate::RegistrationId) -> std::io::Result<()> {
        self.slab.release(id);
        Ok(())
    }

    fn turn(&self, _timeout: Option<std::time::Duration>) -> std::io::Result<usize> {
        Ok(0)
    }

    fn waker(&self) -> &crate::DriverWaker {
        &self.waker
    }

    fn slab(&self) -> &crate::IoSlab {
        &self.slab
    }

    fn shutdown(&self) {}

    fn kind(&self) -> crate::DriverKind {
        crate::DriverKind::Sim
    }
}
