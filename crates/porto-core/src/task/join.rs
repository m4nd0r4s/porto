//! [`JoinHandle`]: waits for a task's output; [`JoinError`]: why there is none.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::raw::RawTask;
use super::state::WakeAction;

/// An owned permission to wait for a task's output, like Tokio's.
///
/// Awaiting it yields the output, or a [`JoinError`] if the task panicked or was cancelled.
/// Dropping it detaches the task: it keeps running, and its output is dropped when it
/// completes. [`JoinHandle::abort`] cancels the task.
pub struct JoinHandle<T> {
    pub(super) task: RawTask,
    _output: PhantomData<T>,
}

// SAFETY: the handle owns a reference to a task whose future, output and scheduler are `Send`,
// and only moves `T` out of it.
unsafe impl<T: Send> Send for JoinHandle<T> {}
// SAFETY: `&JoinHandle` only reaches the header (atomics and fields fixed at spawn): `abort`,
// `id` and `is_finished` never touch the output.
unsafe impl<T: Send> Sync for JoinHandle<T> {}

impl<T> Unpin for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    /// # Safety
    /// `task` must be live with output type `T`, and the caller must give up a reference it
    /// owns along with JOIN_INTEREST.
    pub(crate) unsafe fn from_raw(task: RawTask) -> Self {
        Self {
            task,
            _output: PhantomData,
        }
    }

    /// Cancels the task: if it has not completed yet, it is not polled again, its future is
    /// dropped on the lane that would have polled it, and awaiting this handle yields a
    /// [`JoinError`] whose [`JoinError::is_cancelled`] is true. A task that is running
    /// finishes its current poll first; if that poll completes it, the output wins.
    pub fn abort(&self) {
        match self.task.header().state.transition_to_cancel_requested() {
            WakeAction::DoNothing => {}
            // SAFETY: the transition added the reference the queue now owns and set SCHEDULED.
            WakeAction::Submit => unsafe { self.task.schedule() },
            WakeAction::Dealloc => unreachable!("a cancel request never drops a reference"),
        }
    }

    /// Whether the task has completed (returned, panicked or been cancelled).
    pub fn is_finished(&self) -> bool {
        self.task.header().state.load().is_complete()
    }

    /// The task's id.
    pub fn id(&self) -> u64 {
        self.task.header().id()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut output: Poll<Result<T, JoinError>> = Poll::Pending;
        let task = self.task;
        // SAFETY: we are the task's join handle, its output type is `T`, and `output` is a
        // `Poll<Result<T, JoinError>>`.
        unsafe {
            (task.header().vtable.try_read_output)(
                task.as_ptr(),
                (&raw mut output).cast(),
                cx.waker(),
            );
        }
        output
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        let task = self.task;
        // SAFETY: we are the task's join handle and give up our reference.
        unsafe { (task.header().vtable.drop_join_handle)(task.as_ptr()) }
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header = self.task.header();
        f.debug_struct("JoinHandle")
            .field("id", &header.id())
            .field("type", &header.type_name())
            .field("state", header.state())
            .finish()
    }
}

/// Why a task has no output: it panicked, or it was cancelled before it finished.
pub struct JoinError {
    id: u64,
    repr: Repr,
}

enum Repr {
    Cancelled,
    Panic(Box<dyn Any + Send + 'static>),
}

impl JoinError {
    pub(crate) fn cancelled(id: u64) -> Self {
        Self {
            id,
            repr: Repr::Cancelled,
        }
    }

    pub(crate) fn panic(id: u64, payload: Box<dyn Any + Send + 'static>) -> Self {
        Self {
            id,
            repr: Repr::Panic(payload),
        }
    }

    /// The id of the task that failed.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Whether the task was cancelled ([`JoinHandle::abort`], or its runtime shut down).
    pub fn is_cancelled(&self) -> bool {
        matches!(self.repr, Repr::Cancelled)
    }

    /// Whether the task panicked.
    pub fn is_panic(&self) -> bool {
        matches!(self.repr, Repr::Panic(_))
    }

    /// The panic payload, to pass to [`std::panic::resume_unwind`].
    ///
    /// # Panics
    /// If the task was cancelled: check [`JoinError::is_panic`] first, or use
    /// [`JoinError::try_into_panic`].
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        self.try_into_panic().unwrap_or_else(|error| {
            panic!(
                "porto: JoinError::into_panic called on {error}; check JoinError::is_panic \
                 first, or use JoinError::try_into_panic"
            )
        })
    }

    /// The panic payload, or the error back if the task was cancelled.
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, JoinError> {
        match self.repr {
            Repr::Panic(payload) => Ok(payload),
            Repr::Cancelled => Err(self),
        }
    }
}

/// The message of a panic payload, if it is a string (as `panic!` with a message makes it).
fn panic_message(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::Cancelled => write!(
                f,
                "task {} was cancelled before it finished (JoinHandle::abort, or its runtime \
                 shut down); keep the runtime alive until the tasks you await are done",
                self.id
            ),
            Repr::Panic(payload) => match panic_message(payload.as_ref()) {
                Some(message) => write!(
                    f,
                    "task {} panicked: {message}; JoinError::into_panic gives the payload to \
                     resume_unwind",
                    self.id
                ),
                None => write!(
                    f,
                    "task {} panicked; JoinError::into_panic gives the payload to \
                     resume_unwind",
                    self.id
                ),
            },
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::Cancelled => write!(f, "JoinError::Cancelled({})", self.id),
            Repr::Panic(payload) => match panic_message(payload.as_ref()) {
                Some(message) => write!(f, "JoinError::Panic({}, {message:?})", self.id),
                None => write!(f, "JoinError::Panic({}, ..)", self.id),
            },
        }
    }
}

impl std::error::Error for JoinError {}
