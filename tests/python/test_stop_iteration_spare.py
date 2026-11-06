"""The spare ``StopIteration`` a GIL build raises again on 3.12+ (py_compat/stop_iteration.rs).

Reusing it must be invisible: it may not keep an await's result alive once the task has moved
on, and a reused one must look like a fresh ``StopIteration(value)`` to everything that can
see it (its arguments' hash, the cycle collector). On 3.11 and free-threaded builds there is
no spare, and these tests check the plain behavior.
"""

import gc
import weakref

import porto
from porto import _core


class Payload:
    """A result whose lifetime the tests watch."""


def caught_stop(value: object) -> StopIteration:
    """The ``StopIteration`` a ready awaitable of ``value`` raises when resumed by ``next``."""
    steps = _core._make_ready(value).__await__()  # pyright: ignore[reportPrivateUsage]
    try:
        next(steps)
    except StopIteration as stop:
        return stop
    raise AssertionError("a ready awaitable completes on its first resume")


def test_an_awaited_result_is_not_kept_alive_once_the_task_suspends() -> None:
    async def main() -> None:
        payload = Payload()
        alive = weakref.ref(payload)
        ready = _core._make_ready(payload)  # pyright: ignore[reportPrivateUsage]
        assert await ready is payload
        del payload, ready
        await porto.checkpoint()
        gc.collect()
        assert alive() is None

    porto.run(main)


def test_a_mutex_value_is_freed_with_the_mutex_after_async_with() -> None:
    # The guard `async with` evaluated to was the spare's value: guard -> mutex -> value.
    async def main() -> None:
        payload = Payload()
        alive = weakref.ref(payload)
        mutex = porto.Mutex(payload)
        del payload
        async with mutex as guard:
            pass
        del guard, mutex
        await porto.checkpoint()
        gc.collect()
        assert alive() is None

    porto.run(main)


def test_a_reused_stop_iteration_hashes_its_new_arguments() -> None:
    # 3.14 caches a tuple's hash in the tuple: a reuse must not leave the old one there.
    async def main() -> None:
        first = caught_stop(Payload())
        hash(first.args)
        del first
        value = Payload()
        second = caught_stop(value)
        assert second.args == (value,)
        assert hash(second.args) == hash((value,))
        assert second.args in {(value,)}

    porto.run(main)


def test_a_reused_stop_iteration_arguments_are_seen_by_the_collector() -> None:
    # The collector stops tracking a tuple of atoms; one that now holds a list must be tracked.
    async def main() -> None:
        caught_stop(5)
        gc.collect()
        second = caught_stop([])
        assert gc.is_tracked(second.args)

    porto.run(main)
