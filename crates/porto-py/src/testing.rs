//! Private test hooks of `porto._core` (underscore names, not part of the public API).
//!
//! - `_make_trap(kind, args=None)` and `_make_ready(value)` build the awaitables, which have no
//!   Python constructor;
//! - `_send(iterator, value)` calls `PyIter_Send` directly, which is how the step driver resumes
//!   a coroutine, so tests reach `am_send` even where the `SEND` opcode does not use it;
//! - `_count_allocations(fn)` counts every allocation CPython makes while `fn()` runs.

use core::ffi::c_void;
use core::ptr;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;

use crate::{Ready, Trap};

/// Adds the hooks to `module`.
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(make_trap, module)?)?;
    module.add_function(wrap_pyfunction!(make_ready, module)?)?;
    module.add_function(wrap_pyfunction!(send, module)?)?;
    module.add_function(wrap_pyfunction!(count_allocations, module)?)
}

/// A fresh `_Trap(kind, args)`.
#[pyfunction(name = "_make_trap")]
#[pyo3(signature = (kind, args = None))]
fn make_trap(py: Python<'_>, kind: u32, args: Option<Py<PyAny>>) -> Trap {
    Trap::new(kind, args.unwrap_or_else(|| py.None()))
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
    value: &Bound<'py, PyAny>,
) -> PyResult<(bool, Bound<'py, PyAny>)> {
    let py = iterator.py();
    let mut result = ptr::null_mut();
    // SAFETY: attached (we hold `py`); both objects are live for the call, `result` is a valid
    // out-pointer. PyIter_Send borrows its arguments and writes a new reference to `result`.
    let status = unsafe { ffi::PyIter_Send(iterator.as_ptr(), value.as_ptr(), &raw mut result) };
    match status {
        ffi::PySendResult::PYGEN_ERROR => Err(PyErr::fetch(py)),
        // SAFETY: on PYGEN_RETURN and PYGEN_NEXT, `result` is a new, non-null reference.
        _ => Ok((status == ffi::PySendResult::PYGEN_RETURN, unsafe {
            Bound::from_owned_ptr(py, result)
        })),
    }
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
    let _serialised = COUNTING
        .lock()
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
