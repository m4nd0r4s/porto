//! Snapshots of the registry (`23-observability.md` §1): the request posted to each lane, the
//! records it copies, and what the requester waits on.
//!
//! The lane copies its list when it runs the request, at its next maintenance tick or at once if
//! it is parked; the records are the raw material of `porto.inspect`'s `TaskInfo` (M5).

use std::fmt;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::task::state::Snapshot;
use crate::task::{Header, TaskFlags};

/// Whether a task runs Rust or Python code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskKind {
    /// A Rust future, or a body that is not Python's.
    Rust,
    /// A coroutine ([`TaskFlags::PYTHON`]).
    Python,
}

/// Where a task is in its lifecycle (`TaskInfo.state`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    /// In a run queue.
    Scheduled,
    /// Being polled.
    Running,
    /// Idle: waiting for a wake.
    Waiting,
    /// Finished. A live snapshot never holds one: completion unlinks the task.
    Complete,
}

/// What a snapshot copies of one task: the raw record behind `TaskInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskRecord {
    /// The task's id, increasing in spawn order.
    pub id: u64,
    /// Rust or Python.
    pub kind: TaskKind,
    /// Where it is in its lifecycle.
    pub state: TaskState,
    /// The type name of its future or body.
    pub type_name: &'static str,
    /// The flags it was spawned with.
    pub flags: TaskFlags,
    /// The lane that last ran it.
    pub owner_lane: u16,
    /// The scope it counts in, if any.
    pub scope_id: Option<u64>,
    /// Whether a cancellation was requested and not delivered yet.
    pub cancel_requested: bool,
}

impl TaskRecord {
    /// The record of the task behind `header`, in `state`.
    ///
    /// # Safety
    /// As for [`Header::scope_id`](crate::task::Header): nobody ends or moves the task
    /// meanwhile.
    pub(crate) unsafe fn of(header: &Header, state: Snapshot) -> Self {
        Self {
            id: header.id(),
            kind: if header.flags().contains(TaskFlags::PYTHON) {
                TaskKind::Python
            } else {
                TaskKind::Rust
            },
            state: if state.is_complete() {
                TaskState::Complete
            } else if state.is_running() {
                TaskState::Running
            } else if state.is_scheduled() {
                TaskState::Scheduled
            } else {
                TaskState::Waiting
            },
            type_name: header.type_name(),
            flags: header.flags(),
            owner_lane: header.owner_lane(),
            // SAFETY: the caller's contract.
            scope_id: unsafe { header.scope_id() },
            cancel_requested: state.is_cancel_requested(),
        }
    }
}

/// The record of a task the lane walk shows.
pub(crate) fn record_of(task: &super::LiveTask<'_>) -> TaskRecord {
    // SAFETY: a live task of the lane's own walk (see `LiveTask::is_in_scope`).
    unsafe { TaskRecord::of(task.header(), task.state()) }
}

/// Why a snapshot has no records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// The runtime was dropped before a lane took the snapshot.
    RuntimeClosed,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeClosed => f.write_str(
                "porto: the runtime was dropped before it took the task snapshot; request it \
                 while the runtime runs",
            ),
        }
    }
}

impl std::error::Error for SnapshotError {}

/// What each lane delivers into and the requester waits on.
pub(crate) struct SnapshotCell {
    state: Mutex<Collected>,
    ready: Condvar,
}

struct Collected {
    /// Lanes that have not delivered yet.
    pending: usize,
    records: Vec<TaskRecord>,
    closed: bool,
}

impl SnapshotCell {
    pub(crate) fn new(lanes: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Collected {
                pending: lanes,
                records: Vec::new(),
                closed: false,
            }),
            ready: Condvar::new(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Collected> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// One lane delivers its records, or `None` if it is closed.
    pub(crate) fn deliver(&self, records: Option<Vec<TaskRecord>>) {
        let mut state = self.lock();
        match records {
            Some(records) => state.records.extend(records),
            None => state.closed = true,
        }
        state.pending -= 1;
        if state.pending == 0 {
            self.ready.notify_all();
        }
    }

    fn take(state: &mut Collected) -> Result<Vec<TaskRecord>, SnapshotError> {
        if state.closed {
            return Err(SnapshotError::RuntimeClosed);
        }
        let mut records = std::mem::take(&mut state.records);
        records.sort_unstable_by_key(|record| record.id);
        Ok(records)
    }
}

/// A snapshot of the live tasks, requested of every lane and not yet taken: poll it with
/// [`TaskSnapshotRequest::try_take`] or wait for it.
///
/// A lane copies its tasks at its next maintenance tick (at most `event_interval` tasks from
/// now), or at once if it is parked: the request wakes it. A lane that is not running (a runtime
/// whose `block_on` is not called) takes it when it next runs, so waiting on a request from the
/// only thread that can run the lane never returns: use [`Handle::task_snapshot`], which takes
/// the snapshot directly on the lane's own thread.
///
/// [`Handle::task_snapshot`]: crate::Handle::task_snapshot
pub struct TaskSnapshotRequest {
    cell: Arc<SnapshotCell>,
}

impl TaskSnapshotRequest {
    pub(crate) fn new(cell: Arc<SnapshotCell>) -> Self {
        Self { cell }
    }

    /// The records, sorted by task id, if every lane has delivered them.
    pub fn try_take(&self) -> Option<Result<Vec<TaskRecord>, SnapshotError>> {
        let mut state = self.cell.lock();
        (state.pending == 0).then(|| SnapshotCell::take(&mut state))
    }

    /// Waits for every lane to deliver, then returns the records sorted by task id.
    ///
    /// # Errors
    /// [`SnapshotError::RuntimeClosed`] if the runtime was dropped first.
    pub fn wait(self) -> Result<Vec<TaskRecord>, SnapshotError> {
        let mut state = self.cell.lock();
        while state.pending > 0 {
            state = self
                .cell
                .ready
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        SnapshotCell::take(&mut state)
    }

    /// [`TaskSnapshotRequest::wait`] for at most `timeout`; `None` if it passed first.
    pub fn wait_timeout(
        &self,
        timeout: Duration,
    ) -> Option<Result<Vec<TaskRecord>, SnapshotError>> {
        let deadline = Instant::now() + timeout;
        let mut state = self.cell.lock();
        while state.pending > 0 {
            let left = deadline.checked_duration_since(Instant::now())?;
            let (next, _) = self
                .cell
                .ready
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
        }
        Some(SnapshotCell::take(&mut state))
    }
}

impl fmt::Debug for TaskSnapshotRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskSnapshotRequest")
            .field("pending_lanes", &self.cell.lock().pending)
            .finish()
    }
}
