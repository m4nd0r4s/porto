//! One-word handles for trap payloads (11-task-model-and-trap-protocol.md §2, §5.2).
//!
//! A pending wait is a kind byte plus one payload word (`pending.rs`), but a `Box<dyn Future>`
//! or an `Arc<dyn Trait>` is two words: a data pointer and a vtable pointer. These handles keep
//! the vtable in the allocation instead, in front of the value (as the unstable
//! `std::boxed::ThinBox` does), so the handle is the data pointer alone:
//!
//! - [`BoxedFuture`]: an owned, pinned `dyn Future + Send` (the `Future` and `Io` traps);
//! - [`WaitTarget`]: a counted reference to a [`Waitable`] primitive that the task's embedded
//!   [`WaitNode`] waits on (the `Notified` trap). Taking one is a refcount increment, never an
//!   allocation, so sync waits stay allocation-free (§6).

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::{Context, Poll};

use porto_core::sync::WaitNode;

/// An owned, pinned `dyn Future<Output = O> + Send` in one word.
pub(crate) struct BoxedFuture<O: 'static> {
    /// The first field of a boxed `FutureCell<F>`.
    cell: NonNull<&'static FutureVTable<O>>,
}

/// The vtable first, so a pointer to the cell is a pointer to its vtable.
#[repr(C)]
struct FutureCell<F: Future<Output: 'static>> {
    vtable: &'static FutureVTable<F::Output>,
    future: F,
}

struct FutureVTable<O: 'static> {
    poll: unsafe fn(NonNull<&'static FutureVTable<O>>, &mut Context<'_>) -> Poll<O>,
    drop: unsafe fn(NonNull<&'static FutureVTable<O>>),
}

// SAFETY: the box owns an `F: Send` (`BoxedFuture::new`); the handle is a unique owner, like a
// `Box`, so moving it to another thread moves the future.
unsafe impl<O: 'static> Send for BoxedFuture<O> {}

impl<O: 'static> BoxedFuture<O> {
    /// Boxes `future`; it is pinned from now on.
    pub(crate) fn new<F>(future: F) -> Self
    where
        F: Future<Output = O> + Send + 'static,
    {
        let cell = Box::new(FutureCell {
            vtable: future_vtable::<F>(),
            future,
        });
        Self {
            cell: NonNull::from(Box::leak(cell)).cast(),
        }
    }

    /// Polls the future.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<O> {
        // SAFETY: `cell` is a live `FutureCell` (owned by `self`), whose first field is the
        // vtable made for its type; `&mut self` makes the poll exclusive.
        unsafe { ((*self.cell.as_ptr()).poll)(self.cell, cx) }
    }
}

impl<O: 'static> Drop for BoxedFuture<O> {
    fn drop(&mut self) {
        // SAFETY: as in `poll`; the cell is freed once, here.
        unsafe { ((*self.cell.as_ptr()).drop)(self.cell) }
    }
}

fn future_vtable<F: Future<Output: 'static> + Send + 'static>() -> &'static FutureVTable<F::Output>
{
    // A constant of fn pointers: promoted to a `'static`, one per `F`.
    &FutureVTable {
        poll: poll_future::<F>,
        drop: drop_future::<F>,
    }
}

/// # Safety
/// `cell` is a live `FutureCell<F>`, not polled or dropped concurrently.
unsafe fn poll_future<F: Future<Output: 'static>>(
    cell: NonNull<&'static FutureVTable<F::Output>>,
    cx: &mut Context<'_>,
) -> Poll<F::Output> {
    let cell = cell.cast::<FutureCell<F>>().as_ptr();
    // SAFETY: the caller's contract; the future never moves out of its box.
    unsafe { Pin::new_unchecked(&mut (*cell).future) }.poll(cx)
}

/// # Safety
/// `cell` came from `Box::leak` of a `FutureCell<F>` and is not used afterwards.
unsafe fn drop_future<F: Future<Output: 'static>>(cell: NonNull<&'static FutureVTable<F::Output>>) {
    // SAFETY: the caller's contract.
    drop(unsafe { Box::from_raw(cell.cast::<FutureCell<F>>().as_ptr()) });
}

/// A primitive a task waits on with its embedded [`WaitNode`] (a sync primitive's wait queue).
///
/// # Safety
/// Implementations follow [`WaitQueue::poll_wait`](porto_core::sync::WaitQueue::poll_wait)'s
/// node protocol, and the callers uphold its contract: calls with one node are not concurrent,
/// and from a `Pending` until a `Ready` or [`WaitSource::cancel`] the node stays in place and
/// is used with this source only.
pub(crate) unsafe trait WaitSource: Send + Sync + 'static {
    /// `Ready` once the wait is granted; otherwise links `node` (or updates its waker).
    ///
    /// # Safety
    /// See the trait.
    unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()>;

    /// Stops `node`'s wait (unlinks it, passes an unobserved exclusive grant on). The node is
    /// idle afterwards.
    ///
    /// # Safety
    /// See the trait.
    unsafe fn cancel(&self, node: Pin<&WaitNode>);
}

/// A primitive's shared state with its vtable in front, so a [`WaitTarget`] is one word. A
/// primitive keeps an `Arc<Waitable<S>>` and hands out [`Waitable::target`]s.
#[repr(C)]
pub(crate) struct Waitable<S> {
    vtable: &'static WaitVTable,
    source: S,
}

struct WaitVTable {
    poll_wait:
        unsafe fn(NonNull<&'static WaitVTable>, Pin<&WaitNode>, &mut Context<'_>) -> Poll<()>,
    cancel: unsafe fn(NonNull<&'static WaitVTable>, Pin<&WaitNode>),
    release: unsafe fn(NonNull<&'static WaitVTable>),
}

impl<S: WaitSource> Waitable<S> {
    /// Shares `source`.
    pub(crate) fn new(source: S) -> Arc<Self> {
        Arc::new(Self {
            vtable: wait_vtable::<S>(),
            source,
        })
    }

    /// A counted reference to this primitive, for a `Notified` trap.
    pub(crate) fn target(self: &Arc<Self>) -> WaitTarget {
        let raw = Arc::into_raw(Arc::clone(self));
        WaitTarget {
            // SAFETY: `Arc::into_raw` never returns null.
            waitable: unsafe { NonNull::new_unchecked(raw.cast_mut()) }.cast(),
        }
    }
}

impl<S> Deref for Waitable<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.source
    }
}

/// A counted, type-erased reference to a [`Waitable`] primitive, in one word.
pub(crate) struct WaitTarget {
    /// The first field of an `Arc<Waitable<S>>`'s value; the handle owns one count.
    waitable: NonNull<&'static WaitVTable>,
}

// SAFETY: the handle is an `Arc<Waitable<S>>` with `S: Send + Sync` (`WaitSource`'s bounds).
unsafe impl Send for WaitTarget {}
// SAFETY: as above; `&WaitTarget` only reaches `&S`.
unsafe impl Sync for WaitTarget {}

impl WaitTarget {
    fn vtable(&self) -> &'static WaitVTable {
        // SAFETY: the count this handle owns keeps the `Waitable` alive; its first field is
        // the vtable, which is never written.
        unsafe { *self.waitable.as_ptr() }
    }

    /// See [`WaitSource::poll_wait`].
    ///
    /// # Safety
    /// See [`WaitSource`].
    pub(crate) unsafe fn poll_wait(&self, node: Pin<&WaitNode>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the vtable matches the `Waitable`; the node contract is the caller's.
        unsafe { (self.vtable().poll_wait)(self.waitable, node, cx) }
    }

    /// See [`WaitSource::cancel`].
    ///
    /// # Safety
    /// See [`WaitSource`].
    pub(crate) unsafe fn cancel(&self, node: Pin<&WaitNode>) {
        // SAFETY: as in `poll_wait`.
        unsafe { (self.vtable().cancel)(self.waitable, node) }
    }
}

impl Drop for WaitTarget {
    fn drop(&mut self) {
        // SAFETY: the handle owns one count, released once, here.
        unsafe { (self.vtable().release)(self.waitable) }
    }
}

fn wait_vtable<S: WaitSource>() -> &'static WaitVTable {
    &WaitVTable {
        poll_wait: poll_wait::<S>,
        cancel: cancel_wait::<S>,
        release: release::<S>,
    }
}

/// # Safety
/// `waitable` is a live `Waitable<S>` (a count is held), and `S::poll_wait`'s contract holds.
unsafe fn poll_wait<S: WaitSource>(
    waitable: NonNull<&'static WaitVTable>,
    node: Pin<&WaitNode>,
    cx: &mut Context<'_>,
) -> Poll<()> {
    // SAFETY: the caller's contract.
    unsafe {
        waitable
            .cast::<Waitable<S>>()
            .as_ref()
            .source
            .poll_wait(node, cx)
    }
}

/// # Safety
/// As for [`poll_wait`], with `S::cancel`'s contract.
unsafe fn cancel_wait<S: WaitSource>(waitable: NonNull<&'static WaitVTable>, node: Pin<&WaitNode>) {
    // SAFETY: the caller's contract.
    unsafe { waitable.cast::<Waitable<S>>().as_ref().source.cancel(node) }
}

/// # Safety
/// `waitable` came from `Arc::into_raw` of an `Arc<Waitable<S>>`, whose count the caller gives
/// up.
unsafe fn release<S: WaitSource>(waitable: NonNull<&'static WaitVTable>) {
    // SAFETY: the caller's contract.
    drop(unsafe { Arc::from_raw(waitable.cast::<Waitable<S>>().as_ptr().cast_const()) });
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Waker;

    use super::*;

    const _: () = assert!(size_of::<BoxedFuture<u64>>() == size_of::<usize>());
    const _: () = assert!(size_of::<WaitTarget>() == size_of::<usize>());

    /// Counts its drops; ready on its second poll.
    struct Twice {
        polls: u32,
        drops: &'static AtomicUsize,
    }

    impl Future for Twice {
        type Output = u32;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
            self.polls += 1;
            if self.polls < 2 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(self.polls)
        }
    }

    impl Drop for Twice {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_boxed_future_polls_and_drops_its_future_once() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut boxed = BoxedFuture::new(Twice {
            polls: 0,
            drops: &DROPS,
        });
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(boxed.poll(&mut cx), Poll::Pending);
        assert_eq!(boxed.poll(&mut cx), Poll::Ready(2));
        assert_eq!(DROPS.load(Ordering::Relaxed), 0);
        drop(boxed);
        assert_eq!(DROPS.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_boxed_future_dropped_before_it_finished_drops_its_future() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        let mut boxed = BoxedFuture::new(Twice {
            polls: 0,
            drops: &DROPS,
        });
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(boxed.poll(&mut cx), Poll::Pending);
        drop(boxed);
        assert_eq!(DROPS.load(Ordering::Relaxed), 1);
    }

    /// Ready on its `ready`-th poll; counts cancels.
    struct Counter {
        polls: AtomicUsize,
        cancels: AtomicUsize,
        ready: usize,
    }

    // SAFETY: never links the node.
    unsafe impl WaitSource for Counter {
        unsafe fn poll_wait(&self, _node: Pin<&WaitNode>, _cx: &mut Context<'_>) -> Poll<()> {
            if self.polls.fetch_add(1, Ordering::Relaxed) + 1 >= self.ready {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }

        unsafe fn cancel(&self, _node: Pin<&WaitNode>) {
            self.cancels.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_wait_target_reaches_its_primitive_and_holds_one_count() {
        let waitable = Waitable::new(Counter {
            polls: AtomicUsize::new(0),
            cancels: AtomicUsize::new(0),
            ready: 2,
        });
        let target = waitable.target();
        assert_eq!(Arc::strong_count(&waitable), 2);
        let node = std::pin::pin!(WaitNode::new(0));
        let mut cx = Context::from_waker(Waker::noop());
        // SAFETY: `Counter` never links the node.
        unsafe {
            assert_eq!(target.poll_wait(node.as_ref(), &mut cx), Poll::Pending);
            assert_eq!(target.poll_wait(node.as_ref(), &mut cx), Poll::Ready(()));
            target.cancel(node.as_ref());
        }
        assert_eq!(waitable.cancels.load(Ordering::Relaxed), 1);
        drop(target);
        assert_eq!(Arc::strong_count(&waitable), 1);
    }
}
