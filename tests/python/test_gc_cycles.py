"""porto objects that hold Python objects let the cycle collector see through them.

A ``Mutex`` owns its value, a ``MutexGuard`` its mutex and a scope's exit awaitable the
exception leaving the block, whose traceback reaches the awaiting frame: each can close a
reference cycle, which ``gc.collect()`` must free. Objects that come back from porto's
freelists (GIL builds) must be tracked by the collector again, like freshly allocated ones.
"""

import gc
import weakref
from collections.abc import Callable

import pytest

import porto
from porto import _core


class Holder:
    """An object that can point back at a porto object."""

    other: object = None


def test_a_mutex_and_its_value_in_a_cycle_are_collected() -> None:
    holder = Holder()
    holder.other = porto.Mutex(holder)
    alive = weakref.ref(holder)
    del holder
    gc.collect()
    assert alive() is None


def test_a_guard_stored_in_its_mutex_value_is_collected() -> None:
    # holder -> guard -> mutex -> holder.
    async def main() -> None:
        holder = Holder()
        alive = weakref.ref(holder)
        mutex = porto.Mutex(holder)
        async with mutex as guard:
            holder.other = guard
        del holder, mutex, guard
        await porto.checkpoint()
        gc.collect()
        assert alive() is None

    porto.run(main)


def test_a_scope_exit_holding_its_exception_in_a_cycle_is_collected() -> None:
    async def main() -> None:
        scope = porto.scope()
        await scope.__aenter__()
        scope.spawn(porto.sleep, 0.001)
        error = KeyError("leaving the block")
        exiting = scope.__aexit__(KeyError, error, None)
        payload = Holder()
        alive = weakref.ref(payload)
        error.exiting = exiting  # pyright: ignore[reportAttributeAccessIssue]
        error.payload = payload  # pyright: ignore[reportAttributeAccessIssue]
        assert await exiting is False
        del exiting, error, payload
        await porto.checkpoint()
        gc.collect()
        assert alive() is None

    porto.run(main)


def make_sync_wait() -> object:
    lock = porto.Lock()
    wait = lock.acquire()
    wait.close()  # pyright: ignore[reportAttributeAccessIssue]
    return wait


#: (the type, a function making one inside a task without leaving it pending)
FREELIST_TYPES: list[tuple[str, Callable[[], object]]] = [
    ("_Ready", lambda: _core._make_ready(object())),  # pyright: ignore[reportPrivateUsage]
    ("_Trap", _core._yield_trap),  # pyright: ignore[reportPrivateUsage]
    ("_SyncWait", make_sync_wait),
]


@pytest.mark.parametrize(("name", "make"), FREELIST_TYPES, ids=[name for name, _ in FREELIST_TYPES])
def test_an_object_reused_from_a_freelist_is_tracked_by_the_collector(
    name: str, make: Callable[[], object]
) -> None:
    async def main() -> None:
        first = make()
        assert type(first).__name__ == name
        assert gc.is_tracked(first)
        del first
        again = make()
        assert gc.is_tracked(again)

    porto.run(main)


def test_a_reused_mutex_guard_is_tracked_by_the_collector() -> None:
    async def main() -> None:
        mutex = porto.Mutex(0)
        for _ in range(3):
            async with mutex as guard:
                assert gc.is_tracked(guard)
            del guard
            await porto.checkpoint()

    porto.run(main)
