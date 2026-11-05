//! Multi-producer multi-consumer channels (`16-sync-primitives-and-channels.md` §2).
//!
//! The module keeps the spec's name, `mpsc`, but both ends clone: every value goes to exactly
//! one of the receivers.
//!
//! - [`channel`]: bounded. The buffer is a lock-free Vyukov array queue; senders wait while it
//!   is full. This is the default, so producers cannot outrun consumers unnoticed.
//! - [`unbounded_channel`]: an explicit opt-in; sends never wait.
//!
//! Waiting senders and receivers queue on two [`WaitQueue`](super::WaitQueue)s, and each push
//! (or pop) wakes exactly one waiter on the other side. The fast path takes no lock.
//!
//! Closure (§2.3): the channel ends for receivers once every [`Sender`] is closed or dropped,
//! after they drain what is buffered; sends fail, returning the value, once every
//! [`Receiver`] is closed or dropped.
//!
//! ```
//! use porto_core::sync::mpsc;
//!
//! let rt = porto_core::Runtime::new();
//! let (tx, rx) = mpsc::channel(16);
//! let sum = rt.block_on(async move {
//!     porto_core::spawn(async move {
//!         for i in 1..=10 {
//!             tx.send(i).await.unwrap();
//!         }
//!     });
//!     let mut sum = 0;
//!     while let Ok(i) = rx.recv().await {
//!         sum += i;
//!     }
//!     sum
//! });
//! assert_eq!(sum, 55);
//! ```
//!
//! The close and wake-up races are model-checked with loom:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p porto-core --release mpsc
//! ```

mod array_queue;
mod chan;
mod error;
mod receiver;
mod sender;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

pub use error::{RecvError, SendError, TryRecvError, TrySendError};
pub use receiver::{Receiver, RecvFuture};
pub use sender::{SendFuture, Sender};

use super::primitives::Arc;
use array_queue::MAX_CAPACITY;
use chan::Chan;

/// A bounded channel holding up to `capacity` values. Clone either end for more producers or
/// consumers.
///
/// # Panics
/// If `capacity` is zero (a rendezvous channel is not supported) or above `usize::MAX / 4`.
pub fn channel<T: Send>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(
        capacity > 0,
        "porto: channel capacity must be at least 1 (got 0); pass the number of values the \
         channel may buffer, or use unbounded_channel() for no limit"
    );
    assert!(
        capacity <= MAX_CAPACITY,
        "porto: channel capacity {capacity} is too large (the limit is {MAX_CAPACITY}); a \
         bounded channel allocates its capacity up front, so pass a realistic bound, or use \
         unbounded_channel()"
    );
    pair(Chan::new(Some(capacity)))
}

/// A channel without a capacity limit: sends never wait, so a slow consumer lets the buffer
/// grow without bound. Prefer [`channel`] unless the producer is bounded some other way.
pub fn unbounded_channel<T: Send>() -> (Sender<T>, Receiver<T>) {
    pair(Chan::new(None))
}

fn pair<T>(chan: Chan<T>) -> (Sender<T>, Receiver<T>) {
    let chan = Arc::new(chan);
    (Sender::new(chan.clone()), Receiver::new(chan))
}
