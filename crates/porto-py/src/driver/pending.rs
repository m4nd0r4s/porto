//! What a task waits for between two steps (11-task-model-and-trap-protocol.md §2, §5.2), and
//! its packed form: a one-byte [`Kind`] plus a one-word [`Payload`].
//!
//! [`Pending`] mirrors [`TrapKind`](super::trap::TrapKind) plus `None` and `AsyncioFuture`.
//! Both the `_Trap` object and the PyTask body store it packed, so a trap stays two words and
//! the body 64 bytes without its nodes: every kind carries at most one word (a deadline, a
//! Python object, or one pointer from `thin.rs`, porto-core or the scope tree).

use std::future::Future;
use std::mem::ManuallyDrop;
use std::ptr;

use porto_core::task::TaskBodyRef;
use porto_core::time::Deadline;
use pyo3::prelude::*;

use super::pytask::{PyScopeRef, PyTask, Resume};
use super::thin::{BoxedFuture, WaitTarget};

/// What a task waits for (spec 11 §5.2).
pub(crate) enum Pending {
    /// Nothing: step right away.
    None,
    /// A schedule point: the task was requeued, the next poll resumes it.
    Yield,
    /// A schedule point, then resume with this value (T2: a synchronous result when the budget
    /// ran out).
    YieldThen(Py<PyAny>),
    /// Until the runtime clock reaches the deadline, through the task's embedded timer.
    Sleep(Deadline),
    /// Until a sync primitive grants the task's embedded wait node.
    Notified(WaitTarget),
    /// Until an I/O operation succeeds, re-attempted on readiness (spec 13 §5).
    Io(IoWait),
    /// Until another task finishes, through its join list and the embedded wait node; resumes
    /// with its result, or throws its exception.
    Join(TaskBodyRef<PyTask>),
    /// Until a scope's children finished (spec 15 §1.1).
    ScopeExit(PyScopeRef),
    /// Until a Rust future completes with the resume value.
    Future(BoxedFuture<Resume>),
    /// A future from a third-party native extension (porto-abi, M3).
    #[allow(dead_code, reason = "uninhabited until porto-abi (M3)")]
    AbiFuture(AbiFuture),
    /// An asyncio future (the asyncio compatibility layer, §22).
    #[allow(dead_code, reason = "made by the asyncio compatibility layer (§22)")]
    AsyncioFuture(Py<PyAny>),
}

/// The variant of a packed [`Pending`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum Kind {
    None,
    Yield,
    YieldThen,
    Sleep,
    Notified,
    Io,
    Join,
    ScopeExit,
    Future,
    AsyncioFuture,
}

impl Kind {
    /// Whether the payload of this kind owns something (a reference, a box) to release.
    pub(crate) fn owns_payload(self) -> bool {
        !matches!(self, Self::None | Self::Yield | Self::Sleep)
    }

    /// The name `_Trap.kind` shows.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Yield | Self::YieldThen => "yield",
            Self::Sleep => "sleep",
            Self::Notified => "notified",
            Self::Io => "io",
            Self::Join => "join",
            Self::ScopeExit => "scope_exit",
            Self::Future => "future",
            Self::AsyncioFuture => "asyncio_future",
        }
    }
}

/// The payload of a packed [`Pending`]; which field is live depends on its [`Kind`]. It owns
/// what the live field owns, but has no `Drop`: whoever stores one drops it through
/// [`Pending::unpack`].
pub(crate) union Payload {
    empty: (),
    pub(crate) deadline: Deadline,
    object: ManuallyDrop<Py<PyAny>>,
    pub(crate) wait: ManuallyDrop<WaitTarget>,
    pub(crate) io: ManuallyDrop<IoWait>,
    pub(crate) join: ManuallyDrop<TaskBodyRef<PyTask>>,
    pub(crate) scope: ManuallyDrop<PyScopeRef>,
    pub(crate) future: ManuallyDrop<BoxedFuture<Resume>>,
}

// A pending wait is a kind byte plus one word (25-cpython-integration-and-memory.md §5); a
// deadline is 8 bytes on 32-bit targets too.
const _: () = assert!(
    size_of::<Payload>() == size_of::<Deadline>() && size_of::<Deadline>() >= size_of::<usize>()
        || size_of::<Payload>() == size_of::<usize>()
);

impl Payload {
    /// The payload of a kind that carries nothing.
    pub(crate) const EMPTY: Self = Self { empty: () };
}

impl Pending {
    /// The variant.
    pub(crate) fn kind(&self) -> Kind {
        match self {
            Self::None => Kind::None,
            Self::Yield => Kind::Yield,
            Self::YieldThen(_) => Kind::YieldThen,
            Self::Sleep(_) => Kind::Sleep,
            Self::Notified(_) => Kind::Notified,
            Self::Io(_) => Kind::Io,
            Self::Join(_) => Kind::Join,
            Self::ScopeExit(_) => Kind::ScopeExit,
            Self::Future(_) => Kind::Future,
            Self::AbiFuture(never) => match *never {},
            Self::AsyncioFuture(_) => Kind::AsyncioFuture,
        }
    }

    /// The kind byte and the payload word.
    pub(crate) fn pack(self) -> (Kind, Payload) {
        let empty = Payload::EMPTY;
        match self {
            Self::None => (Kind::None, empty),
            Self::Yield => (Kind::Yield, empty),
            Self::YieldThen(value) => (Kind::YieldThen, object(value)),
            Self::Sleep(deadline) => (Kind::Sleep, Payload { deadline }),
            Self::Notified(wait) => (
                Kind::Notified,
                Payload {
                    wait: ManuallyDrop::new(wait),
                },
            ),
            Self::Io(io) => (
                Kind::Io,
                Payload {
                    io: ManuallyDrop::new(io),
                },
            ),
            Self::Join(join) => (
                Kind::Join,
                Payload {
                    join: ManuallyDrop::new(join),
                },
            ),
            Self::ScopeExit(scope) => (
                Kind::ScopeExit,
                Payload {
                    scope: ManuallyDrop::new(scope),
                },
            ),
            Self::Future(future) => (
                Kind::Future,
                Payload {
                    future: ManuallyDrop::new(future),
                },
            ),
            Self::AbiFuture(never) => match never {},
            Self::AsyncioFuture(future) => (Kind::AsyncioFuture, object(future)),
        }
    }

    /// Moves the value out of `payload`, which is left logically empty.
    ///
    /// # Safety
    /// `payload` was packed with `kind` and not unpacked since; afterwards it is treated as
    /// empty (its owner stores `Kind::None`, or never reads it again).
    pub(crate) unsafe fn unpack(kind: Kind, payload: &Payload) -> Self {
        // SAFETY: the caller's contract: the field `kind` names is the live one, and it is
        // moved out once.
        unsafe {
            match kind {
                Kind::None => Self::None,
                Kind::Yield => Self::Yield,
                Kind::YieldThen => Self::YieldThen(read(&payload.object)),
                Kind::Sleep => Self::Sleep(payload.deadline),
                Kind::Notified => Self::Notified(read(&payload.wait)),
                Kind::Io => Self::Io(read(&payload.io)),
                Kind::Join => Self::Join(read(&payload.join)),
                Kind::ScopeExit => Self::ScopeExit(read(&payload.scope)),
                Kind::Future => Self::Future(read(&payload.future)),
                Kind::AsyncioFuture => Self::AsyncioFuture(read(&payload.object)),
            }
        }
    }
}

fn object(object: Py<PyAny>) -> Payload {
    Payload {
        object: ManuallyDrop::new(object),
    }
}

/// # Safety
/// `field` is live and moved out once.
unsafe fn read<T>(field: &ManuallyDrop<T>) -> T {
    // SAFETY: the caller's contract.
    unsafe { ManuallyDrop::into_inner(ptr::read(field)) }
}

/// What an `Io` trap waits for (13-io-driver.md §5): an operation attempted without blocking,
/// re-attempted when its registration becomes ready, resolving to the resume value.
///
/// Spec 13 §5 sketches the trap as `Io { reg, interest, op }`; packed, that is one pointer to
/// the operation, which owns its registration reference, interest and op state. The operation
/// is a future: polling it attempts the op (and registers the task's waker for the readiness
/// it needs on `WouldBlock`); dropping it unregisters that waker. T3: it consumes data only in
/// the attempt that returns `Ready`, so dropping a pending one loses nothing. porto-szb.62
/// brings the registrations and the `IoOp` set.
pub(crate) struct IoWait(pub(crate) BoxedFuture<Resume>);

impl IoWait {
    /// An `Io` wait for `op`.
    #[allow(
        dead_code,
        reason = "made by the socket and lowlevel I/O APIs (porto-szb.62)"
    )]
    pub(crate) fn new(op: impl Future<Output = Resume> + Send + 'static) -> Self {
        Self(BoxedFuture::new(op))
    }
}

/// The payload of an `AbiFuture` trap: an `FfiFuture<AbiResume>` from a third-party extension
/// built against porto-abi (spec 11 §5.2, §21). The ABI arrives in M3; until then no value of
/// this type exists.
pub(crate) enum AbiFuture {}
