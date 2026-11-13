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
    /// A lane is not running: no thread is inside its `block_on` or `run_until_idle`, so
    /// nothing would take the snapshot.
    LaneNotRunning,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RuntimeClosed => f.write_str(
                "porto: the runtime was dropped before it took the task snapshot; request it \
                 while the runtime runs",
            ),
            Self::LaneNotRunning => f.write_str(
                "porto: a lane of the runtime is not running, so no one can take the task \
                 snapshot: request it from a task, or from another thread while block_on runs",
            ),
        }
    }
}

impl std::error::Error for SnapshotError {}

/// Whether a lane is running (a thread is inside its loop), or its runtime is gone.
pub(crate) type LaneRunning = Box<dyn Fn(usize) -> bool + Send + Sync>;

/// What each lane delivers into and the requester waits on.
pub(crate) struct SnapshotCell {
    state: Mutex<Collected>,
    ready: Condvar,
    lane_running: LaneRunning,
}

struct Collected {
    /// Lanes that have not delivered yet.
    pending: Vec<usize>,
    records: Vec<TaskRecord>,
    closed: bool,
}

/// How long a waiter sleeps before it looks again whether the lanes it waits for run.
const WAIT_SLICE: Duration = Duration::from_millis(50);
/// How many looks in a row find a lane not running before the wait gives up.
const IDLE_LOOKS: u32 = 5;

impl SnapshotCell {
    pub(crate) fn new(lanes: usize, lane_running: LaneRunning) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Collected {
                pending: (0..lanes).collect(),
                records: Vec::new(),
                closed: false,
            }),
            ready: Condvar::new(),
            lane_running,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Collected> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Lane `lane` delivers its records, or `None` if it is closed.
    pub(crate) fn deliver(&self, lane: usize, records: Option<Vec<TaskRecord>>) {
        let mut state = self.lock();
        match records {
            Some(records) => state.records.extend(records),
            None => state.closed = true,
        }
        state.pending.retain(|pending| *pending != lane);
        if state.pending.is_empty() {
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

    /// Whether every lane that has not delivered is running.
    fn pending_lanes_run(&self, state: &Collected) -> bool {
        state.pending.iter().all(|lane| (self.lane_running)(*lane))
    }
}

/// A snapshot of the live tasks, requested of every lane and not yet taken: poll it with
/// [`TaskSnapshotRequest::try_take`] or wait for it.
///
/// A lane copies its tasks at its next maintenance tick (at most `event_interval` tasks from
/// now), or at once if it is parked: the request wakes it. A lane that is not running (no thread
/// is inside its `block_on` or `run_until_idle`) takes it when it next runs, so
/// [`TaskSnapshotRequest::try_take`] may stay empty until then, and a wait gives up with
/// [`SnapshotError::LaneNotRunning`] when a lane it waits for is not running. On a lane's own
/// thread, [`Handle::task_snapshot`] takes the snapshot directly.
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
        state
            .pending
            .is_empty()
            .then(|| SnapshotCell::take(&mut state))
    }

    /// Waits for every lane to deliver, then returns the records sorted by task id.
    ///
    /// # Errors
    /// [`SnapshotError::RuntimeClosed`] if the runtime was dropped first;
    /// [`SnapshotError::LaneNotRunning`] if a lane that has not delivered stays not running for
    /// about a quarter of a second (the owner thread outside `block_on` is such a lane): the wait
    /// would otherwise never end.
    pub fn wait(self) -> Result<Vec<TaskRecord>, SnapshotError> {
        let mut state = self.cell.lock();
        // When the pending lanes were last seen running; a lane that stays stopped for
        // IDLE_LOOKS slices of elapsed time (not wake-ups: a spurious one must not count) ends it.
        let mut last_running = Instant::now();
        while !state.pending.is_empty() {
            let (next, _) = self
                .cell
                .ready
                .wait_timeout(state, WAIT_SLICE)
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
            if state.pending.is_empty() {
                break;
            }
            if self.cell.pending_lanes_run(&state) {
                last_running = Instant::now();
            } else if last_running.elapsed() >= WAIT_SLICE * IDLE_LOOKS {
                return Err(SnapshotError::LaneNotRunning);
            }
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
        while !state.pending.is_empty() {
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
            .field("pending_lanes", &self.cell.lock().pending.len())
            .finish()
    }
}
