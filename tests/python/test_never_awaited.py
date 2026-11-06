"""A porto awaitable freed without being awaited warns, like a coroutine (porto-szb.112).

``porto.sleep()``, ``porto.checkpoint()``, the sync waits and the channel operations are not
coroutines, so CPython's "coroutine ... was never awaited" does not cover them; porto emits
its own ``RuntimeWarning`` when one is freed unawaited. Awaited, cancelled, thrown-into and
closed awaitables do not warn.
"""

import gc
import sys
import warnings
from collections.abc import Awaitable, Callable, Generator, Iterator
from contextlib import contextmanager
from typing import Any

import pytest

import porto
from porto import _core


@contextmanager
def no_warnings() -> Iterator[None]:
    """Fails if anything in the block (or the garbage it leaves) warns."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield
        gc.collect()
    assert [str(warning.message) for warning in caught] == []


def steps_of(awaitable: Awaitable[Any]) -> Generator[Any, Any, Any]:
    """The awaitable's iterator, which porto's awaitables are themselves."""
    return awaitable.__await__()  # pyright: ignore[reportReturnType]


# Each kind warns, naming the call and saying to add `await`


def enter_a_held_mutex() -> Awaitable[Any]:
    """``Mutex.__aenter__()`` on a held mutex: the lazy wait. A free mutex is taken in the call
    itself and comes back ready (16-sync-primitives-and-channels.md §1), with nothing to forget.
    """
    mutex = porto.Mutex(0)
    mutex.__aenter__()  # pyright: ignore[reportUnusedCallResult]
    return mutex.__aenter__()


#: (the call as the warning names it, a function making that awaitable inside a task)
KINDS: list[tuple[str, Callable[[], Awaitable[Any]]]] = [
    ("porto.sleep(...)", lambda: porto.sleep(0.01)),
    ("porto.checkpoint()", porto.checkpoint),
    ("Event.wait()", lambda: porto.Event().wait()),
    ("Notify.notified()", lambda: porto.Notify().notified()),
    ("Lock.acquire()", lambda: porto.Lock().acquire()),
    ("Mutex.__aenter__()", enter_a_held_mutex),
    ("Sender.send(...)", lambda: porto.channel[int](1)[0].send(1)),
    ("Receiver.recv()", lambda: porto.channel[int](1)[1].recv()),
    ("Receiver.recv_many(...)", lambda: porto.channel[int](1)[1].recv_many(4)),
    ("anext(receiver)", lambda: anext(porto.channel[int](1)[1])),
]


@pytest.mark.parametrize(("call", "make"), KINDS, ids=[call for call, _ in KINDS])
def test_an_awaitable_freed_unawaited_warns(call: str, make: Callable[[], Awaitable[Any]]) -> None:
    async def main() -> None:
        with pytest.warns(RuntimeWarning) as caught:
            make()  # pyright: ignore[reportUnusedCallResult]
        [warning] = caught
        assert str(warning.message) == (
            f"porto: {call} was never awaited, so it did nothing; add `await` in front of the call"
        )

    porto.run(main)


def test_the_warning_points_at_the_line_that_forgot_the_await() -> None:
    async def main() -> None:
        line = sys._getframe().f_lineno + 2  # pyright: ignore[reportPrivateUsage]
        with pytest.warns(RuntimeWarning) as caught:
            porto.sleep(1)  # pyright: ignore[reportUnusedCoroutine]
        [warning] = caught
        assert (warning.filename, warning.lineno) == (__file__, line)

    porto.run(main)


def test_a_trap_dropped_while_an_exception_unwinds_keeps_the_exception() -> None:
    async def forgets_then_raises() -> None:
        _trap = porto.sleep(1)
        raise KeyError("raised past the forgotten await")

    async def main() -> None:
        # The trap is freed as the frame unwinds, with the KeyError in flight.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(KeyError, match="raised past the forgotten await"):
                await forgets_then_raises()
        assert ["never awaited" in str(warning.message) for warning in caught] == [True]

    porto.run(main)


def test_the_warning_as_an_error_goes_to_the_unraisable_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unraisable: list[Any] = []
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)

    async def main() -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            porto.checkpoint()  # pyright: ignore[reportUnusedCoroutine]

    porto.run(main)
    [hooked] = unraisable
    assert isinstance(hooked.exc_value, RuntimeWarning)
    assert "porto.checkpoint() was never awaited" in str(hooked.exc_value)


# Nothing warns once awaited, cancelled, thrown into or closed


def test_awaited_awaitables_do_not_warn() -> None:
    async def main() -> None:
        await porto.sleep(0)
        await porto.checkpoint()
        event = porto.Event()
        event.set()
        await event.wait()
        notify = porto.Notify()
        notify.notify_one()
        await notify.notified()
        lock = porto.Lock()
        await lock.acquire()
        lock.release()
        async with porto.Mutex(0):
            pass
        tx, rx = porto.channel[int](4)
        await tx.send(1)
        await tx.send(2)
        await tx.send(3)
        assert await rx.recv() == 1
        assert await rx.recv_many(4) == [2, 3]
        tx.close()
        async for _ in rx:
            pass

    with no_warnings():
        porto.run(main)


def test_cancelled_waits_do_not_warn() -> None:
    async def main() -> None:
        never = porto.Event()
        tx, _rx = porto.channel[int](1)
        await tx.send(0)
        _open, empty = porto.channel[int](1)
        held = porto.Lock()
        await held.acquire()
        async with porto.scope() as s:
            s.spawn(porto.sleep, 10)
            s.spawn(never.wait)
            s.spawn(porto.Notify().notified)
            s.spawn(held.acquire)
            s.spawn(tx.send, 1)
            s.spawn(empty.recv)
            await porto.sleep(0.001)
            s.cancel()

    with no_warnings():
        porto.run(main)


#: Makes each kind of awaitable, outside a task where it can be (no runtime needed).
OUTSIDE: list[Callable[[], Awaitable[Any]]] = [
    _core._yield_trap,  # pyright: ignore[reportPrivateUsage]
    lambda: porto.Event().wait(),
    lambda: porto.channel[int](1)[1].recv(),
]


def test_closed_awaitables_do_not_warn() -> None:
    async def main() -> None:
        for make in [porto.checkpoint, lambda: porto.sleep(1), *OUTSIDE]:
            steps_of(make()).close()

    with no_warnings():
        porto.run(main)


def test_awaitables_thrown_into_do_not_warn() -> None:
    async def main() -> None:
        for make in [porto.checkpoint, lambda: porto.sleep(1), *OUTSIDE]:
            with pytest.raises(ValueError, match="given up"):
                steps_of(make()).throw(ValueError("given up"))

    with no_warnings():
        porto.run(main)


def test_the_private_trap_constructors_do_not_warn() -> None:
    async def main() -> None:
        _core._yield_trap()  # pyright: ignore[reportPrivateUsage, reportUnusedCoroutine]
        _core._sleep_trap(1)  # pyright: ignore[reportPrivateUsage, reportUnusedCoroutine]

    with no_warnings():
        porto.run(main)
