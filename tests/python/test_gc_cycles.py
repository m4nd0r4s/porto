"""porto objects that hold Python objects let the cycle collector see through them.

A ``Mutex`` owns its value, a ``MutexGuard`` its mutex and a scope's exit awaitable the
exception leaving the block, whose traceback reaches the awaiting frame: each can close a
reference cycle, which ``gc.collect()`` must free. Objects that come back from porto's
freelists (GIL builds) must be tracked by the collector again, like freshly allocated ones.
"""

import gc
import weakref
from collections.abc import Awaitable, Callable

import pytest

import porto


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


async def make_sync_wait(mutex: porto.Mutex[list[int]]) -> object:
    # Closed unawaited, it still holds the callback it would have called, and through it the
    # mutex. (`async with` on a mutex makes a `MutexGuard`, not a `_SyncWait`.)
    wait = porto.Lock()._acquire_then(lambda: mutex)
    wait.close()
    return wait


async def make_mutex_guard(mutex: porto.Mutex[list[int]]) -> object:
    async with mutex as guard:
        pass
    return guard


async def make_scope_exit(_: porto.Mutex[list[int]]) -> object:
    scope = porto.scope()
    await scope.__aenter__()
    scope.spawn(porto.checkpoint)
    exiting = scope.__aexit__(KeyError, KeyError("leaving the block"), None)
    await exiting
    return exiting


#: A function making an object that holds what it is given and objects of its own, inside a
#: task and without leaving it pending.
Make = Callable[[porto.Mutex[list[int]]], Awaitable[object]]

#: (the type, its `Make`), for every type with a freelist that the collector can see through
#: (with a ``__traverse__``): those are tracked again on reuse. ``_Trap``, ``_Ready`` and
#: ``_ChannelOp`` hold nothing it could follow.
FREELIST_TYPES: list[tuple[str, Make]] = [
    ("_SyncWait", make_sync_wait),
    ("MutexGuard", make_mutex_guard),
    ("_ScopeExit", make_scope_exit),
]

FREELIST_IDS = [name for name, _ in FREELIST_TYPES]


@pytest.mark.parametrize(("name", "make"), FREELIST_TYPES, ids=FREELIST_IDS)
def test_an_object_reused_from_a_freelist_is_tracked_by_the_collector(
    name: str, make: Make
) -> None:
    async def main() -> None:
        first = await make(porto.Mutex([]))
        assert type(first).__name__ == name
        assert gc.is_tracked(first)
        del first
        again = await make(porto.Mutex([]))
        assert gc.is_tracked(again)

    porto.run(main)


@pytest.mark.parametrize(("name", "make"), FREELIST_TYPES, ids=FREELIST_IDS)
def test_a_reused_object_is_tracked_only_once_its_new_value_is_written(
    name: str, make: Make
) -> None:
    # Each object is the last to hold what it references, so freeing it frees those too, and
    # the next mutex is made before that, so the freed memory is not handed out again at once.
    # Tracking the object again on reuse before its new value is written would make a debug
    # CPython traverse the old value and follow the freed objects (porto-txa); the
    # py-test-debug CI job runs this on one. Elsewhere it checks that churn stays sound.
    async def main() -> None:
        mutex = porto.Mutex([])
        for _ in range(50):
            made = await make(mutex)
            assert type(made).__name__ == name
            mutex = porto.Mutex([])
            del made
            gc.collect()

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
