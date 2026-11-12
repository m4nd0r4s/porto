"""``porto.to_thread``: run a blocking callable on the runtime's thread pool (spec 19 §1, §2).

The blocking pool is elastic (up to ``blocking_threads``, 512 by default; an idle thread exits
after ``blocking_keepalive``, 10 s). The callable runs in a copy of the caller's context, on a
thread that has its own Python thread state, and the awaiting task goes on when it returns.
"""

from __future__ import annotations

import contextvars
from collections.abc import Callable
from typing import Any, TypeVar, TypeVarTuple

from porto import _core
from porto._core import CapacityLimiter, shield

_T = TypeVar("_T")
_Ts = TypeVarTuple("_Ts")


async def to_thread(
    fn: Callable[[*_Ts], _T],
    /,
    *args: *_Ts,
    cancellable: bool = False,
    limiter: CapacityLimiter | None = None,
    **kwargs: Any,
) -> _T:
    """Calls ``fn(*args, **kwargs)`` on a pool thread and returns its result, or raises what it
    raised (the traceback includes the worker's frames).

    ``fn`` runs inside a *copy* of the calling task's ``contextvars`` context: it sees the
    values set so far, and what it sets does not reach the caller. It runs on a thread that is
    not a porto lane, so it may block, and may use the ``blocking_*`` methods of porto's
    primitives. A thread cannot be stopped, so what a cancel does to the ``await`` is a choice:

    - ``cancellable=False`` (the default) *shields* the await: a cancel or a deadline that
      arrives meanwhile waits until ``fn`` has returned, and then takes effect at the next
      checkpoint (as for any shielded section; the result is returned first). Nothing is
      abandoned: the caller never goes on while ``fn`` still runs.
    - ``cancellable=True`` lets the cancel through: the ``await`` raises ``porto.Cancelled`` at
      once, ``fn`` keeps running on its thread, and its result (or exception) is discarded
      when it finishes. Use it only for a ``fn`` that is safe to leave running.

    The default limiter and the ``limiter`` argument are not implemented yet: a ``limiter`` other
    than ``None`` raises ``NotImplementedError``. Raises ``TypeError`` if ``fn`` is not callable
    and ``porto.NoRuntimeError`` outside ``porto.run``.
    """
    if limiter is not None:
        raise NotImplementedError(
            "porto: to_thread(limiter=...) is not implemented yet (porto-szb.26); leave it as "
            "None, or bound the concurrency yourself with a porto.CapacityLimiter around the call"
        )
    context = contextvars.copy_context()
    job = _core._run_in_thread(fn, args, kwargs or None, context)  # pyright: ignore[reportPrivateUsage]
    if cancellable:
        return await job
    # A sync ``with`` cancel scope: no await, so no checkpoint before the job is queued.
    with shield():
        return await job
    raise AssertionError("a shield that has no deadline never absorbs the job's outcome")
