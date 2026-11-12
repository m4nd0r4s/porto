"""``porto.watch``: the latest value and change notification
(16-sync-primitives-and-channels.md §2).

``_interrupt(token, exc)`` throws into a waiting task the way a cancellation does, giving the
wait up first (T3).
"""

import threading
import weakref
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

import porto
from porto import _core


async def checkpoint() -> None:
    await _core._yield_trap()


async def wait_until(condition: Callable[[], bool]) -> None:
    while not condition():
        await checkpoint()


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return _core._run(coro)


# Construction


def test_watch_needs_an_initial_value_and_says_how_to_pass_one() -> None:
    with pytest.raises(
        TypeError, match=r"needs an initial value, as in porto\.watch\[bool\]\(False\)"
    ):
        porto.watch()  # pyright: ignore[reportCallIssue]
    with pytest.raises(TypeError, match="needs an initial value"):
        porto.watch[int]()  # pyright: ignore[reportCallIssue]


def test_the_constructor_makes_a_typed_pair_with_a_readable_initial_value() -> None:
    tx, rx = porto.watch[int](0)
    assert isinstance(tx, porto.WatchSender)
    assert isinstance(rx, porto.WatchReceiver)
    assert str(porto.WatchReceiver[int]) == "porto._core.WatchReceiver[int]"
    assert rx.borrow() == 0
    assert repr(tx) == "<porto.WatchSender>"
    assert repr(rx) == "<porto.WatchReceiver>"
    rx.close()
    assert repr(rx) == "<porto.WatchReceiver closed>"
    assert repr(tx) == "<porto.WatchSender closed>", "no receiver is open: a send would fail"


def test_none_is_a_valid_initial_value() -> None:
    _tx, rx = porto.watch[None](None)
    assert rx.borrow() is None


# Versions


def test_the_initial_value_is_seen_and_changed_waits_for_a_send() -> None:
    tx, rx = porto.watch[int](0)
    log: list[str] = []

    async def watcher() -> None:
        log.append("waiting")
        await rx.changed()
        log.append(f"changed to {rx.borrow()}")

    async def main() -> None:
        task = _core._spawn_joinable(watcher())
        await wait_until(lambda: log == ["waiting"])
        await checkpoint()
        assert log == ["waiting"], "the initial value does not complete changed()"
        tx.send(1)
        await task.join()

    run(main())
    assert log == ["waiting", "changed to 1"]


def test_changed_returns_once_per_call_however_many_sends_came_before() -> None:
    tx, rx = porto.watch[int](0)
    for value in range(1, 6):
        tx.send(value)

    async def main() -> None:
        assert rx.has_changed()
        await rx.changed()  # completes at once: five sends, one change
        assert rx.borrow() == 5
        assert not rx.has_changed()
        with pytest.raises(porto.WouldBlock, match=r"await rx\.changed"):
            rx.try_changed()
        tx.send(6)
        rx.try_changed()
        assert rx.borrow() == 6

    run(main())


def test_borrow_does_not_mark_the_version_seen_but_borrow_and_update_does() -> None:
    tx, rx = porto.watch[str]("a")
    tx.send("b")
    assert rx.borrow() == "b"
    assert rx.has_changed()
    assert rx.borrow_and_update() == "b"
    assert not rx.has_changed()
    with pytest.raises(porto.WouldBlock):
        rx.try_changed()


def test_every_clone_and_subscriber_has_its_own_seen_version() -> None:
    tx, rx = porto.watch[int](0)
    tx.send(1)
    clone = rx.clone()
    assert clone.has_changed(), "a clone has seen what its original had"
    rx.try_changed()
    subscriber = tx.subscribe()
    assert not subscriber.has_changed(), "a subscriber has seen the current version"
    tx.send(2)
    assert rx.has_changed()
    assert clone.has_changed()
    assert subscriber.has_changed()
    assert subscriber.borrow() == 2


def test_the_clearable_event_idiom_waits_until_the_value_is_true() -> None:
    tx, rx = porto.watch[bool](False)
    log: list[str] = []

    async def waiter() -> None:
        while not rx.borrow():
            await rx.changed()
        log.append("set")

    async def main() -> None:
        task = _core._spawn_joinable(waiter())
        await checkpoint()
        tx.send(True)
        tx.send(False)  # cleared again before the waiter ran: it keeps waiting
        await checkpoint()
        await checkpoint()
        assert log == []
        tx.send(True)
        await task.join()

    run(main())
    assert log == ["set"]


def test_async_for_yields_the_latest_value_per_change_and_ends_with_the_channel() -> None:
    tx, rx = porto.watch[int](0)
    seen: list[int] = []

    async def watcher() -> None:
        async for value in rx:
            seen.append(value)

    async def main() -> None:
        task = _core._spawn_joinable(watcher())
        await checkpoint()
        await checkpoint()
        tx.send(1)
        await wait_until(lambda: seen == [1])
        tx.send(2)
        tx.send(3)  # coalesces with 2 if the watcher has not run in between
        await wait_until(lambda: seen[-1] == 3)
        tx.close()
        await task.join()

    run(main())
    assert seen[0] == 1
    assert seen[-1] == 3
    assert 0 not in seen


# Closure


def test_changed_raises_end_of_channel_after_the_last_sender_closes() -> None:
    tx, rx = porto.watch[int](0)
    tx.send(9)
    tx.close()

    async def main() -> None:
        await rx.changed()  # the unseen change comes first
        assert rx.borrow() == 9
        with pytest.raises(porto.EndOfChannel, match=r"borrow\(\) still returns the last value"):
            await rx.changed()
        with pytest.raises(porto.EndOfChannel):
            rx.try_changed()

    run(main())
    assert rx.borrow() == 9, "borrow after close returns the last value"


def test_a_waiting_changed_ends_when_the_last_sender_is_dropped() -> None:
    tx, rx = porto.watch[int](0)

    async def watcher() -> str:
        try:
            await rx.changed()
        except porto.EndOfChannel:
            return "ended"
        return "changed"

    async def main() -> str:
        task = _core._spawn_joinable(watcher())
        await checkpoint()
        await checkpoint()
        nonlocal tx
        del tx
        return await task.join()

    assert run(main()) == "ended"


def test_the_channel_ends_only_with_the_last_sender() -> None:
    tx, rx = porto.watch[int](0)
    other = tx.clone()
    tx.close()
    other.send(1)
    rx.try_changed()
    other.close()
    with pytest.raises(porto.EndOfChannel):
        rx.try_changed()


def test_a_closed_sender_raises_closed_resource_error_and_its_clones_are_closed() -> None:
    tx, rx = porto.watch[int](0)
    tx.close()
    tx.close()
    with pytest.raises(porto.ClosedResourceError, match="watch sender was closed"):
        tx.send(1)
    with pytest.raises(porto.ClosedResourceError):
        tx.subscribe()
    clone = tx.clone()
    with pytest.raises(porto.ClosedResourceError):
        clone.send(1)
    del rx


def test_send_without_a_receiver_is_broken_until_someone_subscribes() -> None:
    tx, rx = porto.watch[int](0)
    del rx
    assert tx.is_closed()
    with pytest.raises(porto.BrokenResourceError, match=r"tx\.subscribe\(\)"):
        tx.send(1)
    rx = tx.subscribe()
    assert not tx.is_closed()
    assert rx.borrow() == 0, "the broken send stored nothing"
    tx.send(2)
    assert rx.borrow() == 2


def test_a_closed_receiver_raises_closed_resource_error_and_leaves_the_others() -> None:
    tx, rx = porto.watch[int](0)
    other = rx.clone()
    rx.close()
    rx.close()
    for use in (rx.borrow, rx.borrow_and_update, rx.has_changed, rx.try_changed):
        with pytest.raises(porto.ClosedResourceError):
            use()
    with pytest.raises(porto.ClosedResourceError):
        run(rx.changed())
    with pytest.raises(porto.ClosedResourceError):
        rx.blocking_changed()
    clone = rx.clone()
    with pytest.raises(porto.ClosedResourceError):
        clone.borrow()
    tx.send(5)
    assert other.borrow() == 5


def test_closing_the_receiver_wakes_a_waiting_changed() -> None:
    tx, rx = porto.watch[int](0)

    async def watcher() -> str:
        try:
            await rx.changed()
        except porto.ClosedResourceError:
            return "closed"
        return "changed"

    async def main() -> str:
        task = _core._spawn_joinable(watcher())
        await checkpoint()
        await checkpoint()
        rx.close()
        return await task.join()

    assert run(main()) == "closed"
    del tx


def test_async_with_closes_the_sender_on_the_way_out() -> None:
    tx, rx = porto.watch[int](0)

    async def main() -> None:
        async with tx as same:
            assert same is tx
            tx.send(1)
        with pytest.raises(porto.ClosedResourceError):
            tx.send(2)
        await rx.changed()
        with pytest.raises(porto.EndOfChannel):
            await rx.changed()

    run(main())


# Python objects


def test_values_are_released_when_replaced_and_with_the_channel() -> None:
    class Payload:
        pass

    initial = Payload()
    initial_alive = weakref.ref(initial)
    tx, rx = porto.watch[Payload](initial)
    del initial
    assert initial_alive() is not None, "the channel holds the initial value"
    second = Payload()
    second_alive = weakref.ref(second)
    tx.send(second)
    del second
    assert initial_alive() is None, "replaced by the send"
    assert second_alive() is not None
    del tx, rx
    assert second_alive() is None, "released with the channel"


def test_a_borrowed_value_outlives_the_next_send() -> None:
    class Payload:
        pass

    tx, rx = porto.watch[Payload](Payload())
    held = rx.borrow()
    alive = weakref.ref(held)
    tx.send(Payload())
    assert alive() is held
    del held
    assert alive() is None


def test_a_value_returned_by_a_failed_send_is_not_kept() -> None:
    class Payload:
        pass

    tx, rx = porto.watch[Payload](Payload())
    del rx
    payload = Payload()
    alive = weakref.ref(payload)
    with pytest.raises(porto.BrokenResourceError):
        tx.send(payload)
    del payload
    assert alive() is None


def test_an_operation_never_awaited_changes_nothing_and_warns() -> None:
    tx, rx = porto.watch[int](0)
    tx.send(1)
    pending = rx.changed()
    with pytest.warns(RuntimeWarning, match=r"WatchReceiver\.changed\(\) was never awaited"):
        del pending
    assert rx.has_changed(), "the version was not marked seen"


# Cancellation


def test_a_cancelled_changed_loses_no_change() -> None:
    tx, rx = porto.watch[int](0)
    tokens: list[_core._TaskToken] = []
    outcomes: list[str] = []

    async def watcher() -> None:
        tokens.append(_core._current_task_token())
        try:
            await rx.changed()
            outcomes.append("changed")
        except KeyError:
            outcomes.append("interrupted")

    async def main() -> None:
        task = _core._spawn_joinable(watcher())
        await wait_until(lambda: len(tokens) == 1)
        await checkpoint()
        _core._interrupt(tokens[0], KeyError("cancelled"))
        await task.join()
        tx.send(1)
        await rx.changed()

    run(main())
    assert outcomes == ["interrupted"]
    assert rx.borrow() == 1


def test_a_changed_woken_before_the_interrupt_completes() -> None:
    tx, rx = porto.watch[int](0)
    tokens: list[_core._TaskToken] = []

    async def watcher() -> list[str]:
        tokens.append(_core._current_task_token())
        seen: list[str] = []
        for _ in range(2):
            try:
                await rx.changed()
                seen.append("changed")
            except KeyError:
                seen.append("interrupted")
        return seen

    async def main() -> list[str]:
        task = _core._spawn_joinable(watcher())
        await wait_until(lambda: len(tokens) == 1)
        await checkpoint()
        tx.send(1)  # the wait is over before the interrupt: the change is not lost
        _core._interrupt(tokens[0], KeyError("cancelled"))
        return await task.join()

    assert run(main()) == ["changed", "interrupted"]


@pytest.mark.parametrize("op", ["changed", "anext"])
def test_a_cancelled_scope_raises_cancelled_at_a_changed_that_would_complete(op: str) -> None:
    tx, rx = porto.watch[int](0)
    tx.send(1)
    events: list[str] = []

    async def main() -> None:
        async with porto.scope() as s:
            s.cancel()
            try:
                await (rx.changed() if op == "changed" else anext(rx))
                events.append("completed")
            except porto.Cancelled:
                events.append("cancelled")
                raise
        assert rx.has_changed()  # the cancelled wait marked nothing seen

    porto.run(main)
    assert events == ["cancelled"]


# Threads


def test_blocking_changed_waits_for_a_send_from_the_main_thread() -> None:
    tx, rx = porto.watch[int](0)
    seen: list[int] = []

    def consume() -> None:
        rx.blocking_changed()
        seen.append(rx.borrow())

    thread = threading.Thread(target=consume)
    thread.start()
    tx.send(4)
    thread.join(10)
    assert seen == [4]


def test_blocking_changed_ends_when_the_senders_close() -> None:
    tx, rx = porto.watch[int](0)
    errors: list[BaseException] = []

    def consume() -> None:
        try:
            rx.blocking_changed()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=consume)
    thread.start()
    tx.close()
    thread.join(10)
    assert len(errors) == 1
    assert isinstance(errors[0], porto.EndOfChannel)


def test_blocking_changed_on_a_lane_raises_runtime_error_with_what_to_do() -> None:
    tx, rx = porto.watch[int](0)

    async def main() -> None:
        with pytest.raises(RuntimeError, match="Await the async form instead"):
            rx.blocking_changed()

    porto.run(main)
    del tx


def test_send_and_borrow_work_from_plain_threads() -> None:
    tx, rx = porto.watch[int](0)

    def produce() -> None:
        for value in range(1, 501):
            tx.send(value)

    threads = [threading.Thread(target=produce) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert rx.borrow() in range(1, 501)
    assert rx.has_changed()
