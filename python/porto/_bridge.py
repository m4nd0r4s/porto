"""Python code the native module calls back into, where a coroutine is simpler than Rust."""

import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from porto import _core

_T = TypeVar("_T")


async def await_awaitable(awaitable: Awaitable[_T]) -> _T:
    """A coroutine for an awaitable that is not one, so it can run as a task."""
    return await awaitable


async def start(
    scope: _core.Scope,
    fn: Callable[..., Awaitable[object]],
    args: tuple[Any, ...],
    name: str | None,
) -> Any:
    """``await scope.start(fn, *args, name=name)`` (15 §3, Trio's ``nursery.start``).

    ``fn(*args, started=...)`` runs as a task in a hidden scope nested in the caller's: the
    caller waits at that scope's exit, which ends when the task has called ``started.set(value)``
    (it was moved into ``scope``, counted out of the hidden one) or has ended. What it raised
    before ``set`` leaves the hidden scope bare; cancelling the caller meanwhile cancels the
    task, which ends the wait with the caller's own cancellation.
    """
    started = scope._new_started()  # pyright: ignore[reportPrivateUsage]
    label = name if name is not None else getattr(fn, "__qualname__", type(fn).__qualname__)
    hidden = _core._hidden_scope()  # pyright: ignore[reportPrivateUsage]
    async with hidden:
        started._bind(hidden)  # pyright: ignore[reportPrivateUsage]
        hidden.spawn(functools.partial(fn, *args, started=started), name=label)
    if not started._was_set():  # pyright: ignore[reportPrivateUsage]
        # Cancelled while it waited: the task ended with the caller's cancellation, which the
        # caller gets at this checkpoint, not a complaint that the task never reported.
        await _core.checkpoint()
    return started._result()  # pyright: ignore[reportPrivateUsage]
