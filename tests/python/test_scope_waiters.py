"""A task waiting in a cancellable wait is filed with its innermost scope, so a cancel wakes it
(15-structured-concurrency-and-cancellation.md C10); every way the wait ends takes the entry
out again, once (porto-szb.114).

``Scope._waiter_count()`` reads how many tasks are filed with the scope (0 before it is
entered). A debug build also asserts, when a scope is freed, that none is left.
"""

import subprocess
import sys
from collections.abc import Callable

import pytest

import porto


async def settle() -> None:
    """Lets every other ready task run until it waits."""
    for _ in range(10):
        await porto.checkpoint()


class Primitive:
    """A wait on one primitive, and how to end it normally: ``hold`` (run in the body first)
    makes ``wait`` wait until ``wake``."""

    async def hold(self, scope: porto.Scope) -> None:
        pass

    async def wait(self) -> None:
        raise NotImplementedError

    async def wake(self) -> None:
        raise NotImplementedError


class OnEvent(Primitive):
    def __init__(self) -> None:
        self.event = porto.Event()

    async def wait(self) -> None:
        await self.event.wait()

    async def wake(self) -> None:
        self.event.set()


class OnNotify(Primitive):
    def __init__(self) -> None:
        self.notify = porto.Notify()

    async def wait(self) -> None:
        await self.notify.notified()

    async def wake(self) -> None:
        self.notify.notify_one()


class OnLock(Primitive):
    def __init__(self) -> None:
        self.lock = porto.Lock()

    async def hold(self, scope: porto.Scope) -> None:
        await self.lock.acquire()

    async def wait(self) -> None:
        async with self.lock:
            pass

    async def wake(self) -> None:
        self.lock.release()


class OnMutex(Primitive):
    def __init__(self) -> None:
        self.mutex = porto.Mutex(0)
        self.release = porto.Event()

    async def hold(self, scope: porto.Scope) -> None:
        async def holder() -> None:
            async with self.mutex:
                await self.release.wait()

        scope.spawn(holder)
        await settle()

    async def wait(self) -> None:
        async with self.mutex as guard:
            guard.set(guard.value + 1)

    async def wake(self) -> None:
        self.release.set()


class OnReceive(Primitive):
    def __init__(self) -> None:
        self.tx, self.rx = porto.channel[int](1)

    async def wait(self) -> None:
        await self.rx.recv()

    async def wake(self) -> None:
        await self.tx.send(1)


class OnSend(Primitive):
    def __init__(self) -> None:
        self.tx, self.rx = porto.channel[int](1)

    async def hold(self, scope: porto.Scope) -> None:
        await self.tx.send(0)  # full

    async def wait(self) -> None:
        await self.tx.send(1)

    async def wake(self) -> None:
        await self.rx.recv()


PRIMITIVES: dict[str, Callable[[], Primitive]] = {
    "event": OnEvent,
    "notify": OnNotify,
    "lock": OnLock,
    "mutex": OnMutex,
    "channel-recv": OnReceive,
    "channel-send": OnSend,
}


def test_a_scope_counts_its_waiters_only_while_they_wait() -> None:
    async def main() -> None:
        go = porto.Event()
        async with porto.scope() as scope:
            assert scope._waiter_count() == 0
            for _ in range(3):
                scope.spawn(go.wait)
            await settle()
            assert scope._waiter_count() == 3
            go.set()
            await settle()
            assert scope._waiter_count() == 0

    porto.run(main())


def test_a_scope_not_entered_has_no_waiters() -> None:
    assert porto.scope()._waiter_count() == 0


def test_a_sleep_that_ends_leaves_nothing_filed() -> None:
    async def main() -> None:
        async with porto.scope() as scope:
            await porto.sleep(0.001)
            assert scope._waiter_count() == 0
            scope.spawn(porto.sleep, 0.001)
            await porto.sleep(0.01)
            assert scope._waiter_count() == 0

    porto.run(main())


@pytest.mark.parametrize("name", PRIMITIVES)
@pytest.mark.parametrize("sleep_first", [False, True], ids=["wait", "sleep-then-wait"])
def test_a_wait_woken_normally_leaves_nothing_filed(name: str, sleep_first: bool) -> None:
    async def main() -> None:
        primitive = PRIMITIVES[name]()
        async with porto.scope() as scope:
            await primitive.hold(scope)

            async def waiter() -> None:
                if sleep_first:
                    await porto.sleep(0.001)
                await primitive.wait()

            task = scope.spawn(waiter)
            await porto.sleep(0.01)
            assert scope._waiter_count() >= 1, "the waiter is filed while it waits"
            await primitive.wake()
            await settle()
            assert task.done()
            assert scope._waiter_count() == 0
        assert scope._waiter_count() == 0

    porto.run(main())


@pytest.mark.parametrize("name", PRIMITIVES)
def test_a_wait_given_up_for_a_cancel_leaves_nothing_filed(name: str) -> None:
    async def main() -> None:
        primitive = PRIMITIVES[name]()
        async with porto.scope() as outer:
            await primitive.hold(outer)
            async with porto.scope() as inner:
                inner.spawn(primitive.wait)
                await settle()
                assert inner._waiter_count() == 1
                inner.cancel()
            assert inner._waiter_count() == 0
            await primitive.wake()
            await settle()
        assert outer._waiter_count() == 0

    porto.run(main())


def test_a_wait_in_a_nested_scope_after_a_sleep_is_still_woken_by_its_cancel() -> None:
    # A sleep that ended used to leave its task marked as filed, with the outer scope: the wait
    # in the inner scope was then never filed there, and the inner cancel could not wake it.
    async def main() -> None:
        stuck = porto.Event()
        inner_scopes: list[porto.Scope] = []

        async def child() -> None:
            await porto.sleep(0.001)
            async with porto.scope() as inner:
                inner_scopes.append(inner)
                await stuck.wait()

        async with porto.scope() as outer:
            task = outer.spawn(child)
            await porto.sleep(0.01)
            filed = (outer._waiter_count(), inner_scopes[0]._waiter_count())
            inner_scopes[0].cancel()
            await porto.sleep(0.01)
            woken = task.done()
            # Lets a child the cancel missed finish: nothing above may raise before this, or
            # the scope would wait for that child forever.
            stuck.set()
        assert filed == (0, 1), "the child is filed with its innermost scope only"
        assert woken, "the inner scope's cancel did not wake its waiting task"
        assert inner_scopes[0].cancelled_caught

    porto.run(main())


def test_a_long_lived_scope_does_not_grow_with_sequential_waits() -> None:
    rounds = 10_000

    async def main() -> None:
        ping = porto.Notify()
        pong = porto.Notify()

        async def echo() -> None:
            for _ in range(rounds):
                await ping.notified()
                pong.notify_one()

        async with porto.scope() as scope:
            scope.spawn(echo)
            for _ in range(rounds):
                ping.notify_one()
                await pong.notified()
                assert scope._waiter_count() <= 1

            for _ in range(rounds // 1000):
                for _ in range(1000):
                    scope.spawn(porto.sleep, 0.0001)
                await porto.sleep(0.005)
                await settle()
                assert scope._waiter_count() == 0

    porto.run(main())


def test_a_debug_build_sees_no_waiter_outlive_its_scope() -> None:
    # The issue's reproduction: on a debug build, a waiter left filed made the scope's drop
    # print a panic to stderr. A release build checks nothing there, and stays quiet too.
    program = """
import porto

async def main():
    go = porto.Event()
    async with porto.scope() as scope:
        scope.spawn(go.wait)
        await porto.sleep(0.001)
        go.set()

porto.run(main())
"""
    completed = subprocess.run(
        [sys.executable, "-c", program], capture_output=True, text=True, timeout=60, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stderr == ""
