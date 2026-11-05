//! `Waker`s that point straight at a task: each owned waker is one [`TaskRef`], so clone adds a
//! reference, drop removes one, and wake schedules the task at most once per idle period.

use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::task::{RawWaker, RawWakerVTable, Waker};

use super::header::Header;
use super::raw::RawTask;
use super::state::WakeAction;
use super::task_ref::TaskRef;

static WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_waker, wake_by_val, wake_by_ref, drop_waker);

fn raw_waker(task: RawTask) -> RawWaker {
    RawWaker::new(task.as_ptr().as_ptr().cast_const().cast(), &WAKER_VTABLE)
}

impl TaskRef {
    /// Turns this reference into a waker for the task; the waker owns the reference.
    pub fn into_waker(self) -> Waker {
        let raw = raw_waker(self.into_raw());
        // SAFETY: the data pointer is a task header whose reference the waker now owns, and
        // the vtable upholds the `RawWaker` contract (see the functions below).
        unsafe { Waker::from_raw(raw) }
    }
}

/// A waker borrowed for the duration of one poll. It does not own a reference (the run
/// reference keeps the task alive), so it must not be dropped as a `Waker`.
pub(crate) struct WakerRef(ManuallyDrop<Waker>);

impl WakerRef {
    /// # Safety
    /// The task must stay alive for as long as the `WakerRef` (and borrows of it) live.
    pub(crate) unsafe fn new(task: RawTask) -> Self {
        // SAFETY: the data pointer is a task header and the vtable upholds the `RawWaker`
        // contract; `ManuallyDrop` keeps the borrowed waker from dropping a reference.
        let waker = unsafe { Waker::from_raw(raw_waker(task)) };
        Self(ManuallyDrop::new(waker))
    }
}

impl Deref for WakerRef {
    type Target = Waker;
    fn deref(&self) -> &Waker {
        &self.0
    }
}

/// # Safety
/// `data` must be the header pointer of a live task whose reference the caller holds.
unsafe fn task(data: *const ()) -> RawTask {
    // SAFETY: wakers are only built from non-null task header pointers (`raw_waker`).
    let ptr = unsafe { NonNull::new_unchecked(data.cast_mut().cast::<Header>()) };
    // SAFETY: the waker being used holds (or borrows) a reference, so the task is live.
    unsafe { RawTask::from_raw(ptr) }
}

/// The waker's reference as a `TaskRef` that must not be dropped (the waker keeps owning it).
///
/// # Safety
/// As [`task`].
unsafe fn borrowed_ref(data: *const ()) -> ManuallyDrop<TaskRef> {
    // SAFETY: the reference is only lent: `ManuallyDrop` never gives it up.
    ManuallyDrop::new(unsafe { TaskRef::from_owned(task(data)) })
}

unsafe fn clone_waker(data: *const ()) -> RawWaker {
    // SAFETY: called through a live waker, see `task`.
    let clone = unsafe { borrowed_ref(data) }.deref().clone();
    raw_waker(clone.into_raw())
}

unsafe fn drop_waker(data: *const ()) {
    // SAFETY: called through a live, owned waker, which gives up its reference here.
    drop(unsafe { TaskRef::from_owned(task(data)) });
}

unsafe fn wake_by_val(data: *const ()) {
    // SAFETY: called through a live, owned waker, see `task`.
    let task = unsafe { task(data) };
    match task.header().state.transition_to_notified_by_val() {
        WakeAction::DoNothing => {}
        // SAFETY: the waker's reference moved to the queue and the task is now SCHEDULED.
        WakeAction::Submit => unsafe { task.schedule() },
        // SAFETY: the transition dropped the last reference.
        WakeAction::Dealloc => unsafe { task.dealloc() },
    }
}

unsafe fn wake_by_ref(data: *const ()) {
    // SAFETY: called through a live waker, see `task`.
    let task = unsafe { task(data) };
    match task.header().state.transition_to_notified_by_ref() {
        WakeAction::DoNothing => {}
        // SAFETY: the transition added the reference the queue now owns, and set SCHEDULED.
        WakeAction::Submit => unsafe { task.schedule() },
        WakeAction::Dealloc => unreachable!("wake_by_ref never drops a reference"),
    }
}
