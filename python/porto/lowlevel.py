"""Trap constructors and lane queries for library authors (spec 20, ``porto.lowlevel``).

A porto awaitable either completes synchronously or yields one trap to the driver
(11-task-model-and-trap-protocol.md §5). These build the traps by hand, for code that
implements its own awaitables:

- ``trap_yield()``: yield to the other tasks once, as ``porto.checkpoint()`` does;
- ``trap_wait_notify(notify)``: wait for a ``porto.Notify``, as ``await notify.notified()`` does;
- ``current_lane()``: the index of the lane the calling task runs on.

All raise ``porto.NoRuntimeError`` outside a task of a porto runtime.
"""

from porto._core import current_lane, trap_wait_notify, trap_yield

__all__ = ["current_lane", "trap_wait_notify", "trap_yield"]
