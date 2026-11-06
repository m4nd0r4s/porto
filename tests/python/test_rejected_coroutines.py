"""A coroutine object that porto refuses to run is closed (porto-szb.113).

``porto.run(coro)``, ``Scope.spawn(coro)`` and the private ``_run``/``_spawn`` raise their
documented error, and close the coroutine first, so CPython does not later add a misleading
"coroutine ... was never awaited" pointing somewhere else.
"""

import gc
import re
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

import porto
from porto import _core


async def work(n: int = 0) -> int:
    return n


@contextmanager
def raises_alone(error: type[BaseException], match: str) -> Iterator[None]:
    """Expects ``error`` (matching ``match``) and no warning at all, after a collection."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(error, match=match):
            yield
        gc.collect()
    assert [str(warning.message) for warning in caught] == []


# porto.run


def test_run_inside_a_runtime_closes_the_coroutine() -> None:
    coro = work()

    async def main() -> None:
        with raises_alone(RuntimeError, "cannot be called from inside a running porto runtime"):
            porto.run(coro)

    porto.run(main)
    assert coro.cr_frame is None, "the refused coroutine was not closed"


INVALID_RUNS: list[tuple[dict[str, Any], type[BaseException], str]] = [
    ({"lanes": 0}, ValueError, "lanes=0 is not valid"),
    ({"lanes": 4}, NotImplementedError, "lanes=4 is not supported yet"),
    ({"no_such_field": 1}, TypeError, "unknown configuration field"),
    ({"config": "fast"}, TypeError, "config must be a porto.RuntimeConfig"),
]


@pytest.mark.parametrize(("keywords", "error", "match"), INVALID_RUNS)
def test_run_with_an_invalid_config_closes_the_coroutine(
    keywords: dict[str, Any], error: type[BaseException], match: str
) -> None:
    coro = work()
    with raises_alone(error, re.escape(match)):
        porto.run(coro, **keywords)
    assert coro.cr_frame is None


def test_run_with_a_coroutine_and_arguments_closes_the_coroutine() -> None:
    coro = work()
    with raises_alone(TypeError, "got a coroutine object and arguments"):
        porto.run(coro, 1)  # pyright: ignore[reportCallIssue]
    assert coro.cr_frame is None


# Scope.spawn


def test_spawn_on_a_closed_scope_closes_the_coroutine() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            pass
        with raises_alone(RuntimeError, "scope is closed"):
            s.spawn(work())

    porto.run(main)


def test_spawn_on_a_scope_not_entered_closes_the_coroutine() -> None:
    async def main() -> None:
        with raises_alone(RuntimeError, "the scope is not entered yet"):
            porto.scope().spawn(work())

    porto.run(main)


def test_spawn_with_a_coroutine_and_arguments_closes_the_coroutine() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            with raises_alone(TypeError, "got a coroutine object and arguments"):
                s.spawn(work(), 1)  # pyright: ignore[reportCallIssue, reportArgumentType]

    porto.run(main)


def test_spawn_outside_a_runtime_closes_the_coroutine() -> None:
    scope = porto.scope()
    with raises_alone(porto.NoRuntimeError, "must be called from a task"):
        scope.spawn(work())


# The private entry points


def test_private_run_inside_a_runtime_closes_the_coroutine() -> None:
    async def main() -> None:
        with raises_alone(RuntimeError, "cannot start a runtime inside a running porto task"):
            _core._run(work())  # pyright: ignore[reportPrivateUsage]

    porto.run(main)


def test_private_spawn_outside_a_runtime_closes_the_coroutine() -> None:
    with raises_alone(porto.NoRuntimeError, "must be called from a task running under _run"):
        _core._spawn(work())  # pyright: ignore[reportPrivateUsage]
