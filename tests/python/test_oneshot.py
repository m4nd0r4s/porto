"""``porto.oneshot``: one value from one sender to one receiver
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


def test_the_constructor_is_subscriptable_and_makes_a_typed_pair() -> None:
    tx, rx = porto.oneshot[int]()
    assert isinstance(tx, porto.OneshotSender)
    assert isinstance(rx, porto.OneshotReceiver)
    assert str(porto.OneshotSender[int]) == "porto._core.OneshotSender[int]"
    assert repr(tx) == "<porto.OneshotSender>"
    assert repr(rx) == "<porto.OneshotReceiver>"
    tx.close()
    rx.close()
    assert repr(tx) == "<porto.OneshotSender closed>"
    assert repr(rx) == "<porto.OneshotReceiver closed>"


def test_a_value_sent_before_the_receive_is_received_without_waiting() -> None:
    tx, rx = porto.oneshot[str]()

    async def main() -> str:
        tx.send("hello")  # never waits: a plain call, not awaited
        return await rx.recv()

    assert run(main()) == "hello"


def test_recv_waits_for_the_send() -> None:
    tx, rx = porto.oneshot[int]()
    log: list[str] = []

    async def receive() -> int:
        log.append("waiting")
        value = await rx.recv()
        log.append(f"got {value}")
        return value

    async def main() -> int:
        task = _core._spawn_joinable(receive())
        await wait_until(lambda: log == ["waiting"])
        await checkpoint()
        assert log == ["waiting"]
        tx.send(42)
        return await task.join()

    assert run(main()) == 42
    assert log == ["waiting", "got 42"]


def test_the_value_is_received_once_then_the_channel_has_ended() -> None:
    tx, rx = porto.oneshot[int]()
    tx.send(1)

    async def main() -> None:
        assert await rx.recv() == 1
        with pytest.raises(porto.EndOfChannel, match="already received"):
            await rx.recv()
        with pytest.raises(porto.EndOfChannel):
            rx.try_recv()

    run(main())


def test_try_recv_is_would_block_until_the_value_arrives() -> None:
    tx, rx = porto.oneshot[int]()
    with pytest.raises(porto.WouldBlock, match=r"await rx\.recv"):
        rx.try_recv()
    tx.send(5)
    assert rx.try_recv() == 5


def test_a_dropped_sender_ends_the_receive_with_end_of_channel() -> None:
    tx, rx = porto.oneshot[int]()

    async def receive() -> str:
        try:
            await rx.recv()
        except porto.EndOfChannel:
            return "ended"
        return "got a value"

    async def main() -> str:
        task = _core._spawn_joinable(receive())
        await checkpoint()
        await checkpoint()
        nonlocal tx
        del tx
        return await task.join()

    assert run(main()) == "ended"


def test_a_closed_sender_ends_the_channel_and_a_late_receive_sees_it() -> None:
    tx, rx = porto.oneshot[int]()
    tx.close()
    tx.close()
    with pytest.raises(porto.EndOfChannel, match="without sending"):
        run(rx.recv())
    with pytest.raises(porto.ClosedResourceError, match="closed or already sent"):
        tx.send(1)


def test_send_to_a_dropped_receiver_raises_broken_resource_error() -> None:
    tx, rx = porto.oneshot[int]()
    assert not tx.is_closed()
    del rx
    assert tx.is_closed()
    with pytest.raises(porto.BrokenResourceError, match="receiver of this oneshot channel"):
        tx.send(1)


def test_send_to_a_closed_receiver_raises_broken_resource_error() -> None:
    tx, rx = porto.oneshot[int]()
    rx.close()
    with pytest.raises(porto.BrokenResourceError):
        tx.send(1)


def test_a_sender_is_used_up_by_its_send() -> None:
    tx, rx = porto.oneshot[int]()
    tx.send(1)
    assert tx.is_closed()
    with pytest.raises(porto.ClosedResourceError, match=r"create another with porto\.oneshot"):
        tx.send(2)
    assert rx.try_recv() == 1


def test_a_closed_receiver_raises_closed_resource_error_on_every_use() -> None:
    tx, rx = porto.oneshot[int]()
    rx.close()
    rx.close()
    with pytest.raises(porto.ClosedResourceError):
        rx.try_recv()
    with pytest.raises(porto.ClosedResourceError):
        run(rx.recv())
    with pytest.raises(porto.ClosedResourceError):
        rx.blocking_recv()
    del tx


def test_closing_the_receiver_wakes_a_waiting_receive() -> None:
    tx, rx = porto.oneshot[int]()

    async def receive() -> str:
        try:
            await rx.recv()
        except porto.ClosedResourceError:
            return "closed"
        return "got a value"

    async def main() -> str:
        task = _core._spawn_joinable(receive())
        await checkpoint()
        await checkpoint()
        rx.close()
        return await task.join()

    assert run(main()) == "closed"
    with pytest.raises(porto.BrokenResourceError):
        tx.send(1)


def test_async_for_yields_the_one_value_then_ends() -> None:
    tx, rx = porto.oneshot[int]()
    seen: list[int] = []

    async def main() -> None:
        tx.send(9)
        async for value in rx:
            seen.append(value)

    run(main())
    assert seen == [9]


def test_async_for_over_a_sender_that_never_sent_ends_quietly() -> None:
    tx, rx = porto.oneshot[int]()
    tx.close()
    seen: list[int] = []

    async def main() -> None:
        async for value in rx:
            seen.append(value)

    run(main())
    assert seen == []


def test_async_with_closes_the_sender_on_the_way_out() -> None:
    tx, rx = porto.oneshot[int]()

    async def main() -> None:
        async with tx as same:
            assert same is tx
        assert tx.is_closed()
        with pytest.raises(porto.EndOfChannel):
            await rx.recv()

    run(main())


def test_the_value_is_released_with_the_channel() -> None:
    class Payload:
        pass

    tx, rx = porto.oneshot[Payload]()
    payload = Payload()
    tx.send(payload)
    alive = weakref.ref(payload)
    del payload
    assert alive() is not None, "the channel holds the value"
    del rx
    assert alive() is None, "dropping the receiver drops the value nobody can receive"
    del tx


def test_a_value_that_comes_back_in_an_error_is_not_leaked() -> None:
    class Payload:
        pass

    tx, rx = porto.oneshot[Payload]()
    del rx
    payload = Payload()
    alive = weakref.ref(payload)
    with pytest.raises(porto.BrokenResourceError):
        tx.send(payload)
    del payload
    assert alive() is None


def test_an_operation_never_awaited_takes_nothing_and_warns() -> None:
    tx, rx = porto.oneshot[int]()
    tx.send(1)
    pending = rx.recv()
    with pytest.warns(RuntimeWarning, match=r"OneshotReceiver\.recv\(\) was never awaited"):
        del pending
    assert rx.try_recv() == 1


def test_recv_outside_a_runtime_that_must_wait_raises_no_runtime_error() -> None:
    _tx, rx = porto.oneshot[int]()

    class Driver:
        def __await__(self) -> Any:
            return rx.recv().__await__()

    with pytest.raises(porto.NoRuntimeError, match="outside a porto task"):
        Driver().__await__().send(None)


# Cancellation


def test_a_cancelled_receive_loses_no_value() -> None:
    tx, rx = porto.oneshot[int]()
    tokens: list[_core._TaskToken] = []
    outcomes: list[object] = []

    async def receive() -> None:
        tokens.append(_core._current_task_token())
        try:
            outcomes.append(await rx.recv())
        except KeyError:
            outcomes.append("interrupted")

    async def main() -> object:
        first = _core._spawn_joinable(receive())
        await wait_until(lambda: len(tokens) == 1)
        await checkpoint()
        _core._interrupt(tokens[0], KeyError("cancelled"))
        await first.join()
        tx.send(7)
        return await rx.recv()

    assert run(main()) == 7
    assert outcomes == ["interrupted"]


def test_a_receive_woken_before_the_interrupt_keeps_its_value() -> None:
    tx, rx = porto.oneshot[int]()
    tokens: list[_core._TaskToken] = []

    async def receive() -> list[object]:
        tokens.append(_core._current_task_token())
        seen: list[object] = []
        for _ in range(2):
            try:
                seen.append(await rx.recv())
            except KeyError:
                seen.append("interrupted")
            except porto.EndOfChannel:
                seen.append("ended")
        return seen

    async def main() -> list[object]:
        task = _core._spawn_joinable(receive())
        await wait_until(lambda: len(tokens) == 1)
        await checkpoint()
        # The value lands first: the wait is over, so the value is not lost to the interrupt,
        # which the task gets at its next wait instead (a second receive: the channel ended).
        tx.send(3)
        _core._interrupt(tokens[0], KeyError("cancelled"))
        return await task.join()

    assert run(main()) == [3, "ended"]


@pytest.mark.parametrize("op", ["recv", "anext"])
def test_a_cancelled_scope_raises_cancelled_at_a_receive_that_would_complete(op: str) -> None:
    tx, rx = porto.oneshot[int]()
    tx.send(1)
    events: list[str] = []

    async def main() -> None:
        async with porto.scope() as s:
            s.cancel()
            try:
                await (rx.recv() if op == "recv" else anext(rx))
                events.append("completed")
            except porto.Cancelled:
                events.append("cancelled")
                raise
        assert rx.try_recv() == 1  # the cancelled receive took nothing

    porto.run(main)
    assert events == ["cancelled"]


def test_two_receives_on_one_receiver_split_the_value_and_the_end() -> None:
    tx, rx = porto.oneshot[int]()
    outcomes: dict[str, object] = {}

    async def receive(name: str) -> None:
        try:
            outcomes[name] = await rx.recv()
        except porto.EndOfChannel:
            outcomes[name] = "ended"

    async def main() -> None:
        a = _core._spawn_joinable(receive("a"))
        b = _core._spawn_joinable(receive("b"))
        await checkpoint()
        await checkpoint()
        tx.send(1)
        await a.join()
        await b.join()

    run(main())
    assert sorted(outcomes.values(), key=str) == [1, "ended"]


# Threads


def test_blocking_recv_waits_for_a_send_from_a_thread() -> None:
    tx, rx = porto.oneshot[int]()
    result: list[int] = []
    thread = threading.Thread(target=lambda: result.append(rx.blocking_recv()))
    thread.start()
    tx.send(5)
    thread.join(10)
    assert result == [5]


def test_blocking_recv_ends_with_end_of_channel_when_the_sender_closes() -> None:
    tx, rx = porto.oneshot[int]()
    errors: list[BaseException] = []

    def consume() -> None:
        try:
            rx.blocking_recv()
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=consume)
    thread.start()
    tx.close()
    thread.join(10)
    assert len(errors) == 1
    assert isinstance(errors[0], porto.EndOfChannel)


def test_blocking_recv_on_a_lane_raises_runtime_error_with_what_to_do() -> None:
    tx, rx = porto.oneshot[int]()

    async def main() -> None:
        with pytest.raises(RuntimeError, match="Await the async form instead"):
            rx.blocking_recv()

    porto.run(main)
    del tx


def test_send_works_from_a_plain_thread() -> None:
    tx, rx = porto.oneshot[int]()
    thread = threading.Thread(target=lambda: tx.send(8))
    thread.start()
    thread.join(10)

    async def main() -> int:
        return await rx.recv()

    assert run(main()) == 8
