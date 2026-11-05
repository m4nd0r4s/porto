"""``porto.channel`` and ``porto.unbounded_channel``: the Sender and Receiver API and the
closure rules (16-sync-primitives-and-channels.md §2.1-§2.3).

Cancellation is not delivered by scopes yet (porto-szb.36): ``_interrupt(token, exc)`` throws
into a waiting task the way it will, giving the wait up first (T3).
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


def test_channel_requires_a_capacity_and_says_how_to_pass_one() -> None:
    with pytest.raises(TypeError, match=r"needs a capacity, as in porto\.channel\[T\]\(64\)"):
        porto.channel()  # pyright: ignore[reportCallIssue]
    with pytest.raises(TypeError, match="needs a capacity"):
        porto.channel[int]()  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize("capacity", ["4", 4.0, True, None])
def test_channel_capacity_must_be_an_int(capacity: Any) -> None:
    with pytest.raises(TypeError, match="capacity must be an int"):
        porto.channel(capacity)


@pytest.mark.parametrize("capacity", [0, -1, 2**64])
def test_channel_capacity_must_be_at_least_one(capacity: int) -> None:
    with pytest.raises(ValueError, match=r"must be between 1 and .*unbounded_channel"):
        porto.channel(capacity)


def test_the_constructors_and_handles_are_subscriptable_at_runtime() -> None:
    tx, rx = porto.channel[int](3)
    assert isinstance(tx, porto.Sender)
    assert isinstance(rx, porto.Receiver)
    assert str(porto.Sender[int]) == "porto._core.Sender[int]"
    assert str(porto.Receiver[str]) == "porto._core.Receiver[str]"
    assert repr(tx) == "<porto.Sender capacity=3>"
    _utx, urx = porto.unbounded_channel[int]()
    assert repr(urx) == "<porto.Receiver unbounded>"


# Sender


def test_capacity_is_the_bound_or_none() -> None:
    tx, _rx = porto.channel[int](7)
    assert tx.capacity == 7
    utx, _urx = porto.unbounded_channel[int]()
    assert utx.capacity is None


def test_await_send_waits_while_the_channel_is_full() -> None:
    tx, rx = porto.channel[int](2)
    sent: list[int] = []

    async def produce() -> None:
        for i in range(5):
            await tx.send(i)
            sent.append(i)

    async def main() -> list[int]:
        async with porto.scope() as s:
            s.spawn(produce)
            await wait_until(lambda: len(sent) == 2)
            for _ in range(10):
                await checkpoint()
            assert sent == [0, 1], "the third send waits for room"
            received = [await rx.recv() for _ in range(5)]
        return received

    assert run(main()) == [0, 1, 2, 3, 4]


def test_try_send_raises_would_block_when_full() -> None:
    tx, rx = porto.channel[str](1)
    tx.try_send("a")
    with pytest.raises(porto.WouldBlock, match=r"full \(capacity 1\); await tx\.send"):
        tx.try_send("b")
    assert rx.try_recv() == "a"
    tx.try_send("b")


def test_an_unbounded_channel_never_blocks_a_sender() -> None:
    tx, rx = porto.unbounded_channel[int]()
    for i in range(10_000):
        tx.try_send(i)
    assert rx.try_recv() == 0


def test_a_clone_keeps_the_channel_open() -> None:
    tx, rx = porto.channel[int](4)
    tx2 = tx.clone()
    tx.close()
    assert tx.is_closed()
    assert not tx2.is_closed()
    tx2.try_send(1)
    assert rx.try_recv() == 1
    with pytest.raises(porto.WouldBlock):
        rx.try_recv()
    assert tx.clone().is_closed(), "a clone of a closed sender is closed"
    tx2.close()
    with pytest.raises(porto.EndOfChannel):
        rx.try_recv()


def test_a_closed_sender_raises_closed_resource_error() -> None:
    tx, _rx = porto.channel[int](4)
    tx.close()
    tx.close()  # idempotent
    with pytest.raises(porto.ClosedResourceError, match="this sender was closed"):
        tx.try_send(1)

    async def main() -> None:
        with pytest.raises(porto.ClosedResourceError):
            await tx.send(1)

    run(main())


def test_async_with_closes_the_sender() -> None:
    tx, rx = porto.channel[int](4)

    async def main() -> list[int]:
        async with tx as entered:
            assert entered is tx
            await tx.send(1)
        assert tx.is_closed()
        return [value async for value in rx]

    assert run(main()) == [1]


def test_is_closed_once_every_receiver_is_gone() -> None:
    tx, rx = porto.channel[int](4)
    rx2 = rx.clone()
    rx.close()
    assert not tx.is_closed()
    rx2.close()
    assert tx.is_closed()


# Receiver


def test_recv_drains_the_buffer_then_raises_end_of_channel() -> None:
    tx, rx = porto.channel[int](4)

    async def main() -> list[int]:
        await tx.send(1)
        await tx.send(2)
        tx.close()
        received = [await rx.recv(), await rx.recv()]
        with pytest.raises(porto.EndOfChannel, match="stop receiving"):
            await rx.recv()
        return received

    assert run(main()) == [1, 2]


def test_a_waiting_recv_wakes_on_the_last_sender_closing() -> None:
    tx, rx = porto.channel[int](4)

    async def close_later() -> None:
        await checkpoint()
        tx.close()

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(close_later)
            with pytest.raises(porto.EndOfChannel):
                await rx.recv()

    run(main())


def test_try_recv_raises_would_block_when_empty_and_end_of_channel_when_ended() -> None:
    tx, rx = porto.channel[int](4)
    with pytest.raises(porto.WouldBlock, match=r"empty; await rx\.recv"):
        rx.try_recv()
    tx.try_send(5)
    tx.close()
    assert rx.try_recv() == 5
    with pytest.raises(porto.EndOfChannel):
        rx.try_recv()


def test_async_for_stops_at_the_end_of_the_channel() -> None:
    tx, rx = porto.channel[int](2)

    async def produce() -> None:
        async with tx:
            for i in range(20):
                await tx.send(i)

    async def main() -> list[int]:
        received: list[int] = []
        async with porto.scope() as s:
            s.spawn(produce)
            async for value in rx:
                received.append(value)
        return received

    assert run(main()) == list(range(20))


def test_cloned_receivers_each_get_a_share_of_the_values() -> None:
    tx, rx = porto.channel[int](8)
    rx2 = rx.clone()
    shares: list[list[int]] = [[], []]

    async def consume(index: int, receiver: porto.Receiver[int]) -> None:
        async for value in receiver:
            shares[index].append(value)
            await checkpoint()

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(consume, 0, rx)
            s.spawn(consume, 1, rx2)
            async with tx:
                for i in range(100):
                    await tx.send(i)

    run(main())
    assert sorted(shares[0] + shares[1]) == list(range(100)), "each value goes to one receiver"
    assert shares[0], "both receivers got values"
    assert shares[1], "both receivers got values"


def test_a_closed_receiver_raises_closed_resource_error_and_the_last_breaks_sends() -> None:
    tx, rx = porto.channel[int](4)
    rx2 = rx.clone()
    rx.close()
    with pytest.raises(porto.ClosedResourceError, match="this receiver was closed"):
        rx.try_recv()
    tx.try_send(1)
    assert rx2.try_recv() == 1
    rx2.close()
    with pytest.raises(porto.BrokenResourceError, match="every receiver"):
        tx.try_send(2)


def test_recv_many_returns_up_to_max_buffered_values() -> None:
    tx, rx = porto.channel[int](8)

    async def main() -> list[list[int]]:
        for i in range(5):
            await tx.send(i)
        batches = [await rx.recv_many(3), await rx.recv_many(10)]
        tx.close()
        with pytest.raises(porto.EndOfChannel):
            await rx.recv_many(1)
        return batches

    assert run(main()) == [[0, 1, 2], [3, 4]]


def test_recv_many_waits_once_then_takes_everything_sent_meanwhile() -> None:
    tx, rx = porto.channel[int](16)
    batches: list[list[int]] = []

    async def consume() -> None:
        batches.append(await rx.recv_many(100))

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(consume)
            await checkpoint()
            # Five sends in one step: the waiting consumer wakes once and takes all of them.
            for i in range(5):
                tx.try_send(i)

    run(main())
    assert batches == [[0, 1, 2, 3, 4]]


def test_recv_many_needs_max_of_at_least_one() -> None:
    _tx, rx = porto.channel[int](1)
    with pytest.raises(ValueError, match=r"max >= 1"):
        rx.recv_many(0)


# Closure on refcount release (§2.3)


def test_del_rx_makes_send_raise_broken_resource_error() -> None:
    tx, rx = porto.channel[int](4)
    del rx
    assert tx.is_closed()
    with pytest.raises(porto.BrokenResourceError, match="every receiver of this channel"):
        tx.try_send(1)

    async def main() -> None:
        with pytest.raises(porto.BrokenResourceError):
            await tx.send(1)

    run(main())


def test_dropping_the_last_receiver_wakes_a_waiting_sender() -> None:
    tx, rx = porto.channel[int](1)
    tx.try_send(0)
    handles = [rx]
    del rx

    async def drop_later() -> None:
        await checkpoint()
        handles.clear()

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(drop_later)
            with pytest.raises(porto.BrokenResourceError):
                await tx.send(1)

    run(main())


def test_del_of_the_last_sender_ends_the_channel_after_the_buffer() -> None:
    tx, rx = porto.channel[int](4)
    tx.try_send(1)
    del tx
    assert rx.try_recv() == 1
    with pytest.raises(porto.EndOfChannel):
        rx.try_recv()


def test_values_nobody_can_receive_any_more_are_released() -> None:
    class Payload:
        pass

    tx, rx = porto.channel[Payload](4)
    payload = Payload()
    tx.try_send(payload)
    alive = weakref.ref(payload)
    del payload
    assert alive() is not None, "the channel holds the value"
    del rx
    assert alive() is None, "closing the last receiver drops the buffered values"
    assert tx.is_closed()


# Awaitable protocol


def test_an_operation_never_awaited_neither_sends_nor_loses_a_value() -> None:
    tx, rx = porto.channel[int](1)
    tx.try_send(1)
    pending_recv = rx.recv()
    del pending_recv
    assert rx.try_recv() == 1
    pending_send = tx.send(2)
    del pending_send
    with pytest.raises(porto.WouldBlock):
        rx.try_recv()


def test_an_operation_is_awaited_once() -> None:
    tx, rx = porto.channel[int](4)

    async def main() -> None:
        op = tx.send(1)
        await op
        with pytest.raises(RuntimeError, match="awaited again"):
            await op
        assert await rx.recv() == 1

    run(main())


def test_waiting_outside_a_porto_task_raises_no_runtime_error() -> None:
    tx, _rx = porto.channel[int](1)
    tx.try_send(0)
    op = tx.send(1).__await__()
    with pytest.raises(porto.NoRuntimeError, match="outside a porto task"):
        next(op)


def test_a_completion_without_waiting_needs_no_runtime() -> None:
    tx, rx = porto.channel[int](1)
    op = tx.send(1).__await__()
    with pytest.raises(StopIteration):
        next(op)
    assert rx.try_recv() == 1


def test_synchronous_completions_spend_the_budget_so_other_tasks_run() -> None:
    tx, rx = porto.unbounded_channel[int]()
    for i in range(1000):
        tx.try_send(i)
    ticks = [0]
    done = [False]

    async def tick() -> None:
        while not done[0]:
            ticks[0] += 1
            await checkpoint()

    async def main() -> int:
        async with porto.scope() as s:
            s.spawn(tick)
            await checkpoint()
            start = ticks[0]
            total = 0
            for _ in range(1000):
                total += await rx.recv()
            done[0] = True
            assert ticks[0] > start, "the receiving loop yielded to the ticker"
        return total

    assert run(main()) == sum(range(1000))


# Cancellation (T3): a cancelled waiter loses no value and no room


def test_a_cancelled_recv_waiter_passes_the_value_to_the_next_receiver() -> None:
    tx, rx = porto.channel[int](4)
    tokens: list[_core._TaskToken] = []
    outcomes: dict[str, object] = {}

    async def receive(name: str) -> None:
        tokens.append(_core._current_task_token())
        try:
            outcomes[name] = await rx.recv()
        except KeyError:
            outcomes[name] = "interrupted"

    async def main() -> None:
        first = _core._spawn_joinable(receive("first"))
        await wait_until(lambda: len(tokens) == 1)
        second = _core._spawn_joinable(receive("second"))
        await wait_until(lambda: len(tokens) == 2)
        await checkpoint()
        _core._interrupt(tokens[0], KeyError("cancelled"))
        await first.join()
        await tx.send(42)
        await second.join()

    run(main())
    assert outcomes == {"first": "interrupted", "second": 42}
    with pytest.raises(porto.WouldBlock):
        rx.try_recv()


def test_a_recv_woken_before_the_interrupt_keeps_its_value() -> None:
    tx, rx = porto.channel[int](4)
    tokens: list[_core._TaskToken] = []

    async def receive() -> list[object]:
        tokens.append(_core._current_task_token())
        seen: list[object] = []
        for _ in range(2):
            try:
                seen.append(await rx.recv())
            except KeyError:
                seen.append("interrupted")
        return seen

    async def main() -> list[object]:
        task = _core._spawn_joinable(receive())
        await wait_until(lambda: len(tokens) == 1)
        await checkpoint()
        # The value lands first: the wait is over, so the value is not lost to the interrupt,
        # which the task gets at its next wait instead.
        tx.try_send(7)
        _core._interrupt(tokens[0], KeyError("cancelled"))
        return await task.join()

    assert run(main()) == [7, "interrupted"]


def test_a_cancelled_send_waiter_passes_the_room_to_the_next_sender() -> None:
    tx, rx = porto.channel[str](1)
    tx.try_send("full")
    tokens: list[_core._TaskToken] = []
    outcomes: dict[str, str] = {}

    async def send(name: str) -> None:
        tokens.append(_core._current_task_token())
        try:
            await tx.send(name)
            outcomes[name] = "sent"
        except KeyError:
            outcomes[name] = "interrupted"

    async def main() -> list[str]:
        first = _core._spawn_joinable(send("first"))
        await wait_until(lambda: len(tokens) == 1)
        second = _core._spawn_joinable(send("second"))
        await wait_until(lambda: len(tokens) == 2)
        await checkpoint()
        _core._interrupt(tokens[0], KeyError("cancelled"))
        await first.join()
        received = [await rx.recv()]
        await second.join()
        received.append(rx.try_recv())
        return received

    assert run(main()) == ["full", "second"]
    assert outcomes == {"first": "interrupted", "second": "sent"}


# blocking_send / blocking_recv (non-runtime threads)


def test_blocking_send_from_threads_into_an_async_receiver() -> None:
    tx, rx = porto.channel[int](8)
    producers = 4
    per_producer = 2_000

    def produce(sender: porto.Sender[int], base: int) -> None:
        for i in range(per_producer):
            sender.blocking_send(base + i)
        sender.close()

    async def main() -> list[int]:
        threads = [
            threading.Thread(target=produce, args=(tx.clone(), p * per_producer))
            for p in range(producers)
        ]
        tx.close()
        for thread in threads:
            thread.start()
        received = [value async for value in rx]
        for thread in threads:
            thread.join()
        return received

    assert sorted(run(main())) == list(range(producers * per_producer))


def test_blocking_recv_from_a_thread_with_an_async_sender() -> None:
    tx, rx = porto.channel[int](4)
    received: list[int] = []

    def consume() -> None:
        while True:
            try:
                received.append(rx.blocking_recv())
            except porto.EndOfChannel:
                return

    async def main() -> None:
        thread = threading.Thread(target=consume)
        thread.start()
        async with tx:
            for i in range(1000):
                await tx.send(i)
        while thread.is_alive():
            await checkpoint()

    run(main())
    assert received == list(range(1000))


def test_blocking_calls_on_a_lane_raise_runtime_error_with_what_to_do() -> None:
    tx, rx = porto.channel[int](1)

    async def main() -> None:
        with pytest.raises(RuntimeError, match=r"Sender\.blocking_send cannot be called on a"):
            tx.blocking_send(1)
        with pytest.raises(RuntimeError, match=r"Await the async form instead"):
            rx.blocking_recv()

    porto.run(main)
    with pytest.raises(porto.WouldBlock):
        rx.try_recv()


def test_blocking_calls_report_the_end_and_a_closed_handle() -> None:
    tx, rx = porto.channel[int](1)
    tx.close()
    with pytest.raises(porto.EndOfChannel):
        rx.blocking_recv()
    rx.close()
    with pytest.raises(porto.ClosedResourceError):
        rx.blocking_recv()
    tx2, rx2 = porto.channel[int](1)
    del rx2
    with pytest.raises(porto.BrokenResourceError):
        tx2.blocking_send(1)
