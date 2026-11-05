"""porto.Event and porto.Notify (16-sync-primitives-and-channels.md §1).

A wait that has to wait is a ``Notified`` trap on the primitive; one that is over at once is a
synchronous completion (a ``_Ready``). ``_interrupt(token, exc)`` throws into a waiting task
the way cancellation will (porto-szb.36): the wait is given up first (T3).
"""

import threading

import pytest

import porto
from porto import _core


async def settle() -> None:
    """Lets every other ready task run until it waits."""
    for _ in range(10):
        await porto.checkpoint()


# Event


def test_event_starts_unset_and_stays_set() -> None:
    event = porto.Event()
    assert not event.is_set()
    assert repr(event) == "<porto.Event unset>"
    event.set()
    event.set()
    assert event.is_set()
    assert repr(event) == "<porto.Event set>"


def test_event_wait_after_set_returns_without_suspending() -> None:
    async def main() -> None:
        event = porto.Event()
        event.set()
        waiting = event.wait()
        assert isinstance(waiting, _core._Ready)
        await waiting

    porto.run(main)


def test_event_set_releases_every_waiter() -> None:
    event = porto.Event()
    released: list[int] = []

    async def waiter(n: int) -> None:
        await event.wait()
        released.append(n)

    async def main() -> None:
        async with porto.scope() as s:
            for n in range(3):
                s.spawn(waiter, n)
            await settle()
            assert released == []
            event.set()
        assert released == [0, 1, 2]

    porto.run(main)


def test_event_wait_outside_run_says_where_to_await() -> None:
    with pytest.raises(porto.NoRuntimeError, match=r"porto\.Event\.wait\(\)"):
        porto.Event().wait()


def test_event_blocking_wait_from_a_thread() -> None:
    event = porto.Event()
    waiter = threading.Thread(target=event.blocking_wait)
    waiter.start()

    async def main() -> None:
        await porto.sleep(0.02)
        event.set()

    porto.run(main)
    waiter.join(timeout=5)
    assert not waiter.is_alive()


def test_event_blocking_wait_on_a_lane_raises() -> None:
    async def main() -> None:
        with pytest.raises(RuntimeError, match=r"Event\.blocking_wait\(\).*lane thread"):
            porto.Event().blocking_wait()

    porto.run(main)


# Notify


def test_notify_one_stores_one_permit_consumed_once() -> None:
    async def main() -> None:
        notify = porto.Notify()
        notify.notify_one()
        notify.notify_one()  # still one permit
        first = notify.notified()
        assert isinstance(first, _core._Ready)
        await first
        second = notify.notified()
        assert isinstance(second, _core._Trap), "the permit was consumed"

    porto.run(main)


def test_notify_one_wakes_waiters_in_arrival_order() -> None:
    notify = porto.Notify()
    woken: list[int] = []

    async def waiter(n: int) -> None:
        await notify.notified()
        woken.append(n)

    async def main() -> None:
        async with porto.scope() as s:
            for n in range(3):
                s.spawn(waiter, n)
            await settle()
            for expected in ([0], [0, 1], [0, 1, 2]):
                notify.notify_one()
                await settle()
                assert woken == expected

    porto.run(main)


def test_notify_all_wakes_only_current_waiters() -> None:
    notify = porto.Notify()
    woken: list[str] = []

    async def waiter(name: str) -> None:
        await notify.notified()
        woken.append(name)

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(waiter, "a")
            s.spawn(waiter, "b")
            await settle()
            notify.notify_all()
            s.spawn(waiter, "late")
            await settle()
            assert woken == ["a", "b"], "notify_all stores no permit"
            notify.notify_one()
        assert woken == ["a", "b", "late"]

    porto.run(main)


def test_an_interrupted_waiter_leaves_the_queue() -> None:
    notify = porto.Notify()
    tokens: list[_core._TaskToken] = []
    woken: list[str] = []

    async def first() -> None:
        tokens.append(_core._current_task_token())
        try:
            await notify.notified()
            woken.append("first")
        except KeyError:
            woken.append("first interrupted")

    async def second() -> None:
        await notify.notified()
        woken.append("second")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(first)
            s.spawn(second)
            await settle()
            _core._interrupt(tokens[0], KeyError("cancelled"))
            await settle()
            notify.notify_one()
        assert woken == ["first interrupted", "second"]

    porto.run(main)


def test_a_waiter_cancelled_after_notify_one_passes_the_wake_on() -> None:
    notify = porto.Notify()

    async def waiter() -> None:
        await notify.notified()

    async def main() -> None:
        _core._spawn_joinable(waiter())
        _core._spawn_joinable(waiter())
        await settle()
        # Grants the first waiter, which never runs again: the run ends and cancels both
        # waiters. The first passes its wake to the second, which passes it on to the permit.
        notify.notify_one()

    _core._run(main())

    async def has_permit() -> bool:
        return isinstance(notify.notified(), _core._Ready)

    assert porto.run(has_permit), "the notify_one wake was lost"


def test_notify_blocking_notified_from_a_thread_and_refused_on_a_lane() -> None:
    notify = porto.Notify()
    waiter = threading.Thread(target=notify.blocking_notified)
    waiter.start()

    async def main() -> None:
        with pytest.raises(RuntimeError, match=r"Notify\.blocking_notified\(\)"):
            notify.blocking_notified()
        await porto.sleep(0.02)
        notify.notify_one()

    porto.run(main)
    waiter.join(timeout=5)
    assert not waiter.is_alive()


def test_notify_is_shared_across_os_threads() -> None:
    notify = porto.Notify()
    notifiers = [threading.Thread(target=notify.notify_one) for _ in range(8)]
    for notifier in notifiers:
        notifier.start()
    for notifier in notifiers:
        notifier.join()
    done = threading.Event()

    def consume() -> None:
        notify.blocking_notified()
        done.set()

    threading.Thread(target=consume).start()
    assert done.wait(5)
