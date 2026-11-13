//! Private test hooks of `porto._core` (underscore names, not part of the public API).
//!
//! - `_make_ready(value)` builds a `_Ready`, which has no Python constructor (traps come from
//!   `_yield_trap()` and `_sleep_trap(delay)`);
//! - `_send(iterator, value)` calls `PyIter_Send` directly, which is how the step driver resumes
//!   a coroutine, so tests reach `am_send` even where the `SEND` opcode does not use it;
//! - `_take_unnormalized_exception(exc_type, value, traceback)` raises a raw exception triple
//!   and takes it back through `take_exception`, which normalizes it on 3.11;
//! - `_count_allocations(fn)` counts every allocation CPython makes while `fn()` runs;
//! - the error hooks of [`errors`];
//! - the trap-kind hooks of `traps.rs`;
//! - `_IoHandle`, a socket registered with the running lane's driver, whose methods are the
//!   `Io` trap's operations (`crate::io`);
//! - `_buffer_pool_stats()`: how this lane's buffer pool was used, per size class.

use core::ffi::c_void;
use core::ptr;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyTraceback, PyType};

pub mod errors;

use crate::Ready;
use crate::py_compat::{self, Sent, take_exception};

mod traps;

/// Adds the hooks to `module`.
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(make_ready, module)?)?;
    module.add_function(wrap_pyfunction!(send, module)?)?;
    module.add_function(wrap_pyfunction!(take_unnormalized_exception, module)?)?;
    module.add_function(wrap_pyfunction!(count_allocations, module)?)?;
    module.add_function(wrap_pyfunction!(buffer_pool_stats, module)?)?;
    module.add_function(wrap_pyfunction!(gil_stats, module)?)?;
    module.add_function(wrap_pyfunction!(startup_trace, module)?)?;
    module.add_class::<crate::io::handle::IoHandle>()?;
    errors::register(module)?;
    traps::register(module)
}

/// `_startup_trace()`: the start steps the last run installed and removed, in order
/// (`install:asyncgen_hooks`, ..., `uninstall:asyncgen_hooks`): 26 §2 step 1.
#[pyfunction(name = "_startup_trace")]
fn startup_trace() -> Vec<String> {
    crate::driver::startup::last_trace()
}

/// `(park detaches, periodic releases)`: how often a lane in this process gave the GIL up to
/// park, and how often it gave it up for the other Python threads (12 §6). Both stay 0 between
/// two reads while a lane runs tasks back to back with no other thread around.
#[pyfunction(name = "_gil_stats")]
fn gil_stats() -> (u64, u64) {
    crate::driver::lane::gil_stats()
}

/// This thread's buffer pool, one `(capacity, free, hits, misses, discarded)` tuple per size
/// class, smallest first. Empty counts outside a run (the pool is cleared when a run ends).
#[pyfunction(name = "_buffer_pool_stats")]
fn buffer_pool_stats() -> Vec<(usize, usize, u64, u64, u64)> {
    use crate::io::pool;
    pool::SIZE_CLASSES
        .iter()
        .zip(pool::stats())
        .map(|(&capacity, stats)| {
            (
                capacity,
                stats.free,
                stats.hits,
                stats.misses,
                stats.discarded,
            )
        })
        .collect()
}

/// A `_Ready` holding `value`.
#[pyfunction(name = "_make_ready")]
#[pyo3(signature = (value, /))]
fn make_ready(value: Py<PyAny>) -> Ready {
    Ready::new(value)
}

/// `PyIter_Send(iterator, value)`: `(True, result)` on return, `(False, yielded)` on yield.
#[pyfunction(name = "_send")]
#[pyo3(signature = (iterator, value, /))]
fn send<'py>(
    iterator: &Bound<'py, PyAny>,
    value: Bound<'py, PyAny>,
) -> PyResult<(bool, Bound<'py, PyAny>)> {
    match py_compat::send(iterator, Some(value)) {
        Sent::Returned(result) => Ok((true, result)),
        Sent::Yielded(yielded) => Ok((false, yielded)),
        Sent::Raised(exception) => Err(PyErr::from_value(exception.into_any())),
    }
}

/// Sets the raised exception to the raw `(exc_type, value, traceback)` triple, as a C extension
/// calling `PyErr_Restore` would, then returns what `take_exception` makes of it. On 3.11 the
/// triple stays unnormalized (`value` need not be an `exc_type` instance) until
/// `take_exception` normalizes it; 3.12+ normalizes it in `PyErr_Restore`.
#[pyfunction(name = "_take_unnormalized_exception")]
#[pyo3(signature = (exc_type, value, traceback, /))]
#[allow(
    deprecated,
    reason = "PyErr_Restore is deprecated from 3.12, but it is the raw-triple API C extensions \
              still call, which is what this hook reproduces"
)]
fn take_unnormalized_exception<'py>(
    exc_type: &Bound<'py, PyType>,
    value: &Bound<'py, PyAny>,
    traceback: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBaseException>> {
    let py = exc_type.py();
    if !exc_type.is_subclass_of::<PyBaseException>()? {
        return Err(PyTypeError::new_err(format!(
            "_take_unnormalized_exception: exc_type must be an exception class, not {}",
            exc_type.name()?
        )));
    }
    if !traceback.is_none() && !traceback.is_instance_of::<PyTraceback>() {
        return Err(PyTypeError::new_err(
            "_take_unnormalized_exception: traceback must be a traceback or None",
        ));
    }
    let value = if value.is_none() {
        ptr::null_mut()
    } else {
        value.clone().into_ptr()
    };
    let traceback = if traceback.is_none() {
        ptr::null_mut()
    } else {
        traceback.clone().into_ptr()
    };
    // SAFETY: attached (we hold `py`); `exc_type` is an exception class and `traceback` a
    // traceback or NULL (checked above). PyErr_Restore steals the three new references.
    unsafe { ffi::PyErr_Restore(exc_type.clone().into_ptr(), value, traceback) };
    Ok(take_exception(py))
}

/// Allocations made through the wrapped allocators since they were installed.
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
/// Serialises `_count_allocations` calls: one set of wrappers at a time.
static COUNTING: Mutex<()> = Mutex::new(());

/// The domains we count: `PyMem_*` and `PyObject_*` (every Python object). `PyMem_Raw*` is
/// left alone: it is called without an attached thread and is not on the paths we measure.
const DOMAINS: [ffi::PyMemAllocatorDomain; 2] = [
    ffi::PyMemAllocatorDomain::PYMEM_DOMAIN_MEM,
    ffi::PyMemAllocatorDomain::PYMEM_DOMAIN_OBJ,
];

/// Calls `fn()` and returns how many allocations (malloc, calloc and realloc calls in the MEM
/// and OBJ domains) happened meanwhile, on any thread. Do not start `tracemalloc` inside `fn`:
/// it swaps the same allocators.
#[pyfunction(name = "_count_allocations")]
#[pyo3(signature = (function, /))]
fn count_allocations(function: &Bound<'_, PyAny>) -> PyResult<usize> {
    // The lock is the point: one set of wrappers at a time while `function` runs. Only this
    // test hook takes it, and `function` calling it again is the test's own deadlock.
    let _serialised = COUNTING
        .lock() // lock-across-python: serialises the allocator wrappers; no other path takes it
        .map_err(|_| PyRuntimeError::new_err("porto._core: _count_allocations was poisoned"))?;
    let guard = CountingAllocators::install();
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let outcome = function.call0();
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
    drop(guard);
    drop(outcome?);
    Ok(allocations)
}

fn empty_allocator() -> ffi::PyMemAllocatorEx {
    ffi::PyMemAllocatorEx {
        ctx: ptr::null_mut(),
        malloc: None,
        calloc: None,
        realloc: None,
        free: None,
    }
}

/// The counting wrappers, installed for as long as this guard lives. It owns the original
/// allocators, which are the wrappers' `ctx` and so must stay at a fixed address until they are
/// reinstalled (also when `fn()` raised).
struct CountingAllocators(*mut [ffi::PyMemAllocatorEx; 2]);

impl CountingAllocators {
    fn install() -> Self {
        let originals = Box::into_raw(Box::new([empty_allocator(), empty_allocator()]));
        for (index, domain) in DOMAINS.into_iter().enumerate() {
            // SAFETY: `index` < 2, so the pointer stays inside the live boxed array.
            let original = unsafe { originals.cast::<ffi::PyMemAllocatorEx>().add(index) };
            // SAFETY: attached (only called from `count_allocations`); `original` is a valid
            // out-pointer.
            unsafe { ffi::PyMem_GetAllocator(domain, original) };
            let mut counting = ffi::PyMemAllocatorEx {
                ctx: original.cast(),
                malloc: Some(counting_malloc),
                calloc: Some(counting_calloc),
                realloc: Some(counting_realloc),
                free: Some(forwarding_free),
            };
            // SAFETY: attached. The wrappers forward every call to the original allocator,
            // which CPython allows after initialisation (tracemalloc does the same); `ctx`
            // stays valid until `drop` reinstalls the originals.
            unsafe { ffi::PyMem_SetAllocator(domain, &raw mut counting) };
        }
        Self(originals)
    }
}

impl Drop for CountingAllocators {
    fn drop(&mut self) {
        for (index, domain) in DOMAINS.into_iter().enumerate() {
            // SAFETY: `index` < 2, inside the boxed array, which is live until freed below.
            let original = unsafe { self.0.cast::<ffi::PyMemAllocatorEx>().add(index) };
            // SAFETY: still attached; `original` is the allocator `PyMem_GetAllocator` returned.
            unsafe { ffi::PyMem_SetAllocator(domain, original) };
        }
        // SAFETY: the pointer came from `Box::into_raw` in `install`, and no wrapper can use it
        // any more: the originals are back in place.
        drop(unsafe { Box::from_raw(self.0) });
    }
}

/// The original allocator a wrapper forwards to.
///
/// # Safety
///
/// `ctx` must be the `ctx` installed by `count_allocations`: a live `PyMemAllocatorEx`.
unsafe fn original<'a>(ctx: *mut c_void) -> &'a ffi::PyMemAllocatorEx {
    // SAFETY: guaranteed by the caller.
    unsafe { &*ctx.cast::<ffi::PyMemAllocatorEx>() }
}

extern "C" fn counting_malloc(ctx: *mut c_void, size: usize) -> *mut c_void {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    // SAFETY: CPython passes back the `ctx` we installed.
    let original = unsafe { original(ctx) };
    original
        .malloc
        .map_or(ptr::null_mut(), |malloc| malloc(original.ctx, size))
}

extern "C" fn counting_calloc(ctx: *mut c_void, count: usize, size: usize) -> *mut c_void {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    // SAFETY: CPython passes back the `ctx` we installed.
    let original = unsafe { original(ctx) };
    original
        .calloc
        .map_or(ptr::null_mut(), |calloc| calloc(original.ctx, count, size))
}

extern "C" fn counting_realloc(ctx: *mut c_void, block: *mut c_void, size: usize) -> *mut c_void {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    // SAFETY: CPython passes back the `ctx` we installed.
    let original = unsafe { original(ctx) };
    original.realloc.map_or(ptr::null_mut(), |realloc| {
        realloc(original.ctx, block, size)
    })
}

extern "C" fn forwarding_free(ctx: *mut c_void, block: *mut c_void) {
    // SAFETY: CPython passes back the `ctx` we installed.
    let original = unsafe { original(ctx) };
    if let Some(free) = original.free {
        free(original.ctx, block);
    }
}
