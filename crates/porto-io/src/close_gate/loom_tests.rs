//! Loom models of the close gate: an operation racing with `close`, and two closers. loom's
//! cell reports any read of the source that is not ordered before its drop, and each model
//! checks the source is dropped exactly once and never while the closer still deregisters it.
//!
//! `RUSTFLAGS="--cfg loom" cargo test -p porto-io --release loom`

use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;

use super::CloseGate;

/// Counts its drops. Each read goes through loom's cell, which checks it against the drop.
struct Source(Arc<AtomicUsize>);

impl Source {
    fn use_it(&self) {
        assert_eq!(
            self.0.load(Ordering::Relaxed),
            0,
            "used after it was closed"
        );
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn gate() -> (Arc<CloseGate<Source>>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (Arc::new(CloseGate::new(Source(Arc::clone(&drops)))), drops)
}

/// The closer deregisters with the source still open: its reference keeps it.
fn close_and_deregister(gate: &CloseGate<Source>) -> bool {
    match gate.close() {
        Some(closer) => {
            closer.with(Source::use_it);
            true
        }
        None => false,
    }
}

#[test]
fn loom_an_operation_racing_with_close_never_uses_a_closed_source() {
    loom::model(|| {
        let (gate, drops) = gate();
        let operating = {
            let gate = Arc::clone(&gate);
            thread::spawn(move || {
                if let Some(operation) = gate.acquire() {
                    operation.with(Source::use_it);
                }
            })
        };
        assert!(close_and_deregister(&gate));
        operating.join().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1, "dropped exactly once");
        assert!(gate.acquire().is_none());
    });
}

#[test]
fn loom_two_closers_and_an_operation_close_once() {
    loom::model(|| {
        let (gate, drops) = gate();
        let closing = {
            let gate = Arc::clone(&gate);
            thread::spawn(move || close_and_deregister(&gate))
        };
        let operating = {
            let gate = Arc::clone(&gate);
            thread::spawn(move || {
                if let Some(operation) = gate.acquire() {
                    operation.with(Source::use_it);
                }
            })
        };
        let closed_here = close_and_deregister(&gate);
        let closed_there = closing.join().unwrap();
        operating.join().unwrap();
        assert!(closed_here ^ closed_there, "exactly one close wins");
        assert_eq!(drops.load(Ordering::Relaxed), 1, "dropped exactly once");
    });
}
