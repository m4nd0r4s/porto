//! Tests of [`JoinWaiters`]; the loom model of close-versus-cancel is at the end.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use super::{JoinWaiters, WaitNode};

/// Counts its wakes.
#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (std::sync::Arc<CountingWaker>, Waker) {
    let counter = std::sync::Arc::new(CountingWaker::default());
    (counter.clone(), Waker::from(counter))
}

fn poll(list: &JoinWaiters, node: Pin<&WaitNode>, waker: &Waker) -> Poll<()> {
    // SAFETY: every test node is pinned in a box that outlives its wait, on one thread, and is
    // only used with `list`.
    unsafe { list.poll_wait(node, &mut Context::from_waker(waker)) }
}

#[cfg(not(loom))]
mod std_tests {
    use super::*;

    #[test]
    fn a_wait_after_close_is_ready_at_once() {
        let list = JoinWaiters::new();
        assert_eq!(list.close(), 0);
        assert!(list.is_closed());
        let node = Box::pin(WaitNode::new(0));
        let (_, waker) = counting_waker();
        assert!(poll(&list, node.as_ref(), &waker).is_ready());
        assert!(!node.is_pending());
    }

    #[test]
    fn close_wakes_every_waiter_once() {
        let list = JoinWaiters::new();
        let nodes: Vec<_> = (0..3).map(|_| Box::pin(WaitNode::new(0))).collect();
        let (counter, waker) = counting_waker();
        for node in &nodes {
            assert!(poll(&list, node.as_ref(), &waker).is_pending());
            assert!(node.is_pending());
        }
        assert_eq!(list.close(), 3);
        assert_eq!(counter.0.load(Ordering::SeqCst), 3);
        for node in &nodes {
            assert!(poll(&list, node.as_ref(), &waker).is_ready());
            assert!(!node.is_pending(), "a ready node is idle again");
        }
    }

    #[test]
    fn a_repoll_replaces_the_waker() {
        let list = JoinWaiters::new();
        let node = Box::pin(WaitNode::new(0));
        let (first, first_waker) = counting_waker();
        let (second, second_waker) = counting_waker();
        assert!(poll(&list, node.as_ref(), &first_waker).is_pending());
        assert!(poll(&list, node.as_ref(), &second_waker).is_pending());
        list.close();
        assert_eq!(first.0.load(Ordering::SeqCst), 0);
        assert_eq!(second.0.load(Ordering::SeqCst), 1);
        assert!(poll(&list, node.as_ref(), &second_waker).is_ready());
    }

    #[test]
    fn cancelling_any_waiter_keeps_the_others_linked() {
        for cancelled in 0..3 {
            let list = JoinWaiters::new();
            let nodes: Vec<_> = (0..3).map(|_| Box::pin(WaitNode::new(0))).collect();
            let (counter, waker) = counting_waker();
            for node in &nodes {
                assert!(poll(&list, node.as_ref(), &waker).is_pending());
            }
            // SAFETY: the node is pending on `list`.
            unsafe { list.cancel(nodes[cancelled].as_ref()) };
            assert!(!nodes[cancelled].is_pending());
            assert_eq!(
                list.close(),
                2,
                "cancelled waiter {cancelled} was still linked"
            );
            assert_eq!(counter.0.load(Ordering::SeqCst), 2);
        }
    }

    #[test]
    fn cancelling_after_close_forgets_the_wake() {
        let list = JoinWaiters::new();
        let node = Box::pin(WaitNode::new(0));
        let (_, waker) = counting_waker();
        assert!(poll(&list, node.as_ref(), &waker).is_pending());
        list.close();
        // SAFETY: the node is pending (woken) on `list`.
        unsafe { list.cancel(node.as_ref()) };
        assert!(!node.is_pending());
        // Dropping the node now is fine: it is idle.
    }

    #[test]
    fn a_close_from_another_thread_publishes_what_was_stored_before() {
        let list = std::sync::Arc::new(JoinWaiters::new());
        let outcome = std::sync::Arc::new(AtomicUsize::new(0));
        let node = Box::pin(WaitNode::new(0));
        let (counter, waker) = counting_waker();
        assert!(poll(&list, node.as_ref(), &waker).is_pending());
        let closer = {
            let (list, outcome) = (list.clone(), outcome.clone());
            std::thread::spawn(move || {
                outcome.store(42, Ordering::Relaxed);
                list.close();
            })
        };
        closer.join().unwrap();
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert!(poll(&list, node.as_ref(), &waker).is_ready());
        assert_eq!(outcome.load(Ordering::Relaxed), 42);
    }
}

#[cfg(loom)]
mod loom_models {
    use loom::sync::Arc;
    use loom::thread;

    use super::*;

    /// A waiter that cancels while the task closes the list: the node ends idle, it is woken
    /// at most once, and nothing is left linked.
    #[test]
    fn close_races_cancel() {
        loom::model(|| {
            let list = Arc::new(JoinWaiters::new());
            let node = Box::pin(WaitNode::new(0));
            let (counter, waker) = counting_waker();
            assert!(poll(&list, node.as_ref(), &waker).is_pending());
            let closer = {
                let list = list.clone();
                thread::spawn(move || {
                    list.close();
                })
            };
            // SAFETY: the node is pending on `list`, and only this thread uses it.
            unsafe { list.cancel(node.as_ref()) };
            assert!(!node.is_pending());
            closer.join().unwrap();
            assert!(counter.0.load(Ordering::SeqCst) <= 1);
            assert!(list.is_closed());
        });
    }

    /// A waiter links on another thread while the list closes: either it saw the close, or
    /// close woke it, and it is ready once the closer is done.
    #[test]
    fn a_wait_races_close() {
        loom::model(|| {
            let list = Arc::new(JoinWaiters::new());
            let waiter = {
                let list = list.clone();
                thread::spawn(move || {
                    let node = Box::pin(WaitNode::new(0));
                    let (counter, waker) = counting_waker();
                    let first = poll(&list, node.as_ref(), &waker);
                    (node, counter, waker, first)
                })
            };
            list.close();
            let (node, counter, waker, first) = waiter.join().unwrap();
            let woken = counter.0.load(Ordering::SeqCst);
            match first {
                Poll::Ready(()) => assert_eq!(woken, 0),
                Poll::Pending => assert_eq!(woken, 1, "a linked waiter was not woken"),
            }
            assert!(poll(&list, node.as_ref(), &waker).is_ready());
            assert!(!node.is_pending());
        });
    }
}
