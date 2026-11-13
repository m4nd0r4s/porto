"""``porto.to_thread``: run a blocking callable on the runtime's thread pool (spec 19 §1, §2).

The blocking pool is elastic (up to ``blocking_threads``, 512 by default; an idle thread exits
after ``blocking_keepalive``, 10 s). The callable runs in a copy of the caller's context, on a
thread that has its own Python thread state, and the awaiting task goes on when it returns. A
``CapacityLimiter`` bounds how many calls run at once: 40 by default, the limiter of
``to_thread.default_limiter()``.
"""

from __future__ import annotations

import contextvars
import inspect
import os
import threading
from collections.abc import Callable
from typing import Any, TypeVar, TypeVarTuple, final

from porto import _core
from porto._core import CapacityLimiter, shield

_T = TypeVar("_T")
_Ts = TypeVarTuple("_Ts")

#: The tokens of ``to_thread.default_limiter()`` (AnyIO's default).
DEFAULT_LIMITER_TOKENS = 40

_default_limiter: CapacityLimiter | None = None
_default_limiter_lock = threading.Lock()


def _forget_default_limiter_in_child() -> None:
    """A forked child has none of its parent's threads, so the tokens they hold would never
    come back: it starts over with a fresh limiter."""
    global _default_limiter, _default_limiter_lock
    _default_limiter = None
    _default_limiter_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_forget_default_limiter_in_child)


@final
class _Token:
    """One token of a limiter, borrowed for a job and given back exactly once, from whichever
    thread gets there first (the worker when the job ends, the awaiting task when the job never
    ran)."""

    __slots__ = ("_borrower", "_held", "_limiter", "_lock")

    def __init__(self, limiter: CapacityLimiter, borrower: object) -> None:
        self._limiter = limiter
        self._borrower = borrower
        self._held = True
        self._lock = threading.Lock()

    def release(self) -> None:
        with self._lock:
            if not self._held:
                return
            self._held = False
        self._limiter.release_on_behalf_of(self._borrower)


@final
class _TokenHolder:
    """``fn`` as the worker runs it: it gives the token back when ``fn`` ends, even when nobody
    awaits the job any more (``cancellable=True``), and when the job is dropped without running."""

    __slots__ = ("_fn", "_token")

    def __init__(self, fn: Callable[..., Any], token: _Token) -> None:
        self._fn = fn
        self._token = token

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        try:
            return self._fn(*args, **kwargs)
        finally:
            self._token.release()

    def __del__(self) -> None:
        self._token.release()


def _callable_error(fn: object) -> TypeError:
    return TypeError(
        f"porto: to_thread needs a callable to run, not {type(fn).__name__}; pass a function "
        "(and its arguments after it), for example porto.to_thread(open_file, path)"
    )


@final
class _ToThread:
    """The type of ``porto.to_thread``: an async callable with a ``default_limiter()``."""

    def __init__(self) -> None:
        # What a function would have, for introspection and ``help()``.
        self.__name__ = self.__qualname__ = "to_thread"
        self.__module__ = "porto"
        self.__doc__ = type(self).__call__.__doc__
        mark_coroutine_function = getattr(inspect, "markcoroutinefunction", None)  # 3.12+
        if mark_coroutine_function is not None:
            mark_coroutine_function(self)

    def default_limiter(self) -> CapacityLimiter:
        """The limiter ``to_thread`` uses when it is not given one: a ``CapacityLimiter`` of 40
        tokens, shared by every call in the process (AnyIO's default).

        Its ``total_tokens`` can be changed, which is how an application sizes it, and its
        ``borrowed_tokens`` say how many calls are running (or, after a ``cancellable=True``
        cancel, still finishing).
        """
        global _default_limiter
        with _default_limiter_lock:
            if _default_limiter is None:
                _default_limiter = CapacityLimiter(DEFAULT_LIMITER_TOKENS)
            return _default_limiter

    async def __call__(
        self,
        fn: Callable[[*_Ts], _T],
        /,
        *args: *_Ts,
        cancellable: bool = False,
        limiter: CapacityLimiter | None = None,
        **kwargs: Any,
    ) -> _T:
        """Calls ``fn(*args, **kwargs)`` on a pool thread and returns its result, or raises what
        it raised (the traceback includes the worker's frames).

        ``fn`` runs inside a *copy* of the calling task's ``contextvars`` context: it sees the
        values set so far, and what it sets does not reach the caller. It runs on a thread that
        is not a porto lane, so it may block, and may use the ``blocking_*`` methods of porto's
        primitives.

        The call first borrows a token of ``limiter`` (``to_thread.default_limiter()`` when it
        is ``None``) and waits for one while all are borrowed; a cancel that arrives meanwhile
        takes effect at once, since nothing runs yet. The token goes back when ``fn`` has
        finished, whatever way it ended, so it stays borrowed for a job that is abandoned
        (``cancellable=True``) until its thread is really done.

        A thread cannot be stopped, so what a cancel does to the ``await`` is a choice:

        - ``cancellable=False`` (the default) *shields* the await: a cancel or a deadline that
          arrives meanwhile waits until ``fn`` has returned, and then takes effect at the next
          checkpoint (as for any shielded section; the result is returned first). Nothing is
          abandoned: the caller never goes on while ``fn`` still runs.
        - ``cancellable=True`` lets the cancel through: the ``await`` raises ``porto.Cancelled``
          at once, ``fn`` keeps running on its thread, and its result (or exception) is
          discarded when it finishes. Use it only for a ``fn`` that is safe to leave running.

        Raises ``TypeError`` if ``fn`` is not callable or ``limiter`` is not a
        ``porto.CapacityLimiter``, and ``porto.NoRuntimeError`` outside ``porto.run``.
        """
        if limiter is None:
            limiter = self.default_limiter()
        elif not isinstance(limiter, CapacityLimiter):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError(
                "porto: to_thread(limiter=...) needs a porto.CapacityLimiter, not "
                f"{type(limiter).__name__}; pass porto.CapacityLimiter(n) to allow n calls at a "
                "time, or leave it out for the default limiter"
            )
        if not callable(fn):  # pyright: ignore[reportUnnecessaryCallable]
            raise _callable_error(fn)
        # Copied at the call: what the task sets while it waits for a token cannot be seen.
        context = contextvars.copy_context()
        borrower = object()
        await limiter.acquire_on_behalf_of(borrower)
        token = _Token(limiter, borrower)
        try:
            job = _core._run_in_thread(  # pyright: ignore[reportPrivateUsage]
                _TokenHolder(fn, token), args, kwargs or None, context
            )
        except BaseException:
            token.release()
            raise
        if cancellable:
            # A cancel leaves the job running; the worker gives the token back when it ends.
            return await job
        # A sync ``with`` cancel scope: no await, so no checkpoint before the job is queued.
        # The token is not given back here: it is the job's, and goes back when ``fn`` ends (or
        # when a job that never ran is dropped), even if this coroutine is closed meanwhile.
        with shield():
            return await job
        raise AssertionError("a shield that has no deadline never absorbs the job's outcome")

    def __repr__(self) -> str:
        return "<porto.to_thread>"


to_thread = _ToThread()
