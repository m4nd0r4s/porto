"""Python code the native module calls back into, where a coroutine is simpler than Rust."""

from collections.abc import Awaitable
from typing import TypeVar

_T = TypeVar("_T")


async def await_awaitable(awaitable: Awaitable[_T]) -> _T:
    """A coroutine for an awaitable that is not one, so it can run as a task."""
    return await awaitable
