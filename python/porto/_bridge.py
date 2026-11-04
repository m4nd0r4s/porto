"""Python code the native module calls back into, where a coroutine is simpler than Rust."""

from collections.abc import Awaitable
from typing import TypeVar

from porto._core import Scope

_T = TypeVar("_T")


async def finish_scope(scope: Scope, exc: BaseException | None) -> None:
    """The waiting part of ``Scope.__aexit__``: parks until no child runs, then closes the scope
    (raising the children's failures)."""
    while (trap := scope._wait_trap()) is not None:  # pyright: ignore[reportPrivateUsage]
        await trap
    scope._finish(exc)  # pyright: ignore[reportPrivateUsage]


async def await_awaitable(awaitable: Awaitable[_T]) -> _T:
    """A coroutine for an awaitable that is not one, so it can run as a task."""
    return await awaitable
