"""``porto.broadcast``: every receiver gets every message, and a slow one is told how many it
missed (16-sync-primitives-and-channels.md §2).

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


def drain(rx: porto.BroadcastReceiver[int]) -> list[int | str]:
    """What ``rx`` has pending: messages, and ``"lag:n"`` for a lag."""
    seen: list[int | str] = []
    while True:
        try:
            seen.append(rx.try_recv())
        except porto.Lagged as lag:
            seen.append(f"lag:{lag.skipped}")
        except (porto.WouldBlock, porto.EndOfChannel):
            return seen


# Construction


def test_broadcast_needs_a_capacity_and_says_how_to_pass_one() -> None:
    with pytest.raises(TypeError, match=r"needs a capacity, as in porto\.broadcast\[T\]\(64\)"):
        porto.broadcast()  # pyright: ignore[reportCallIssue]
    with pytest.raises(TypeError, match="needs a capacity"):
        porto.broadcast[int]()  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize("capacity", ["4", 4.0, True, None])
def test_broadcast_capacity_must_be_an_int(capacity: Any) -> None:
    with pytest.raises(TypeError, match="capacity must be an int"):
        porto.broadcast(capacity)


@pytest.mark.parametrize("capacity", [0, -1, 2**64])
def test_broadcast_capacity_must_be_at_least_one(capacity: int) -> None:
    with pytest.raises(ValueError, match=r"capacity must be between 1 and"):
        porto.broadcast(capacity)


def test_the_constructor_makes_a_typed_pair() -> None:
    tx, rx = porto.broadcast[int](3)
    assert isinstance(tx, porto.BroadcastSender)
    assert isinstance(rx, porto.BroadcastReceiver)
    assert str(porto.BroadcastReceiver[int]) == "porto._core.BroadcastReceiver[int]"
    assert tx.capacity == 3
    assert repr(tx) == "<porto.BroadcastSender capacity=3>"
    assert repr(rx) == "<porto.BroadcastReceiver>"
    rx.close()
    assert repr(rx) == "<porto.BroadcastReceiver closed>"
    assert repr(tx) == "<porto.BroadcastSender capacity=3 closed>"


def test_a_huge_capacity_is_cheap_until_it_is_used() -> None:
    tx, rx = porto.broadcast[int](2**60)
    tx.send(1)
    assert rx.try_recv() == 1


# Fan-out


def test_n_receivers_each_see_every_message_in_order() -> None:
    tx, first = porto.broadcast[int](16)
    receivers = [first, first.clone(), tx.subscribe(), tx.subscribe().clone()]
    results: list[list[int]] = [[] for _ in receivers]

    async def listen(index: int) -> None:
        async for message in receivers[index]:
            results[index].append(message)

    async def main() -> None:
        tasks = [_core._spawn_joinable(listen(i)) for i in range(len(receivers))]
        await checkpoint()
        await checkpoint()
        for value in range(10):
            tx.send(value)
            await checkpoint()
        tx.close()
        for task in tasks:
            await task.join()

    run(main())
    assert results == [list(range(10))] * 4


def test_a_subscriber_sees_only_later_messages_and_a_clone_starts_where_its_original_is() -> None:
    tx, rx = porto.broadcast[int](8)
    tx.send(1)
    tx.send(2)
    assert rx.try_recv() == 1
    clone = rx.clone()
    subscriber = tx.subscribe()
    tx.send(3)
    assert drain(clone) == [2, 3]
    assert drain(subscriber) == [3]
    assert drain(rx) == [2, 3]


def test_recv_waits_and_every_waiting_receiver_wakes() -> None:
    tx, rx = porto.broadcast[str](4)
    other = rx.clone()
    outcomes: dict[str, str] = {}

    async def receive(name: str, receiver: porto.BroadcastReceiver[str]) -> None:
        outcomes[name] = await receiver.recv()

    async def main() -> None:
        a = _core._spawn_joinable(receive("a", rx))
        b = _core._spawn_joinable(receive("b", other))
        await checkpoint()
        await checkpoint()
        assert outcomes == {}
        tx.send("hello")
        await a.join()
        await b.join()

    run(main())
    assert outcomes == {"a": "hello", "b": "hello"}


# Lagged


def test_a_slow_receiver_gets_lagged_with_the_exact_count_then_the_oldest_kept() -> None:
    tx, rx = porto.broadcast[int](3)
    for value in range(1, 11):
        tx.send(value)
    with pytest.raises(porto.Lagged) as lagged:
        rx.try_recv()
    assert lagged.value.skipped == 7
    assert "missed 7 messages" in str(lagged.value)
    assert drain(rx) == [8, 9, 10]


def test_lagged_is_raised_by_recv_and_resumes_at_the_oldest_kept() -> None:
    tx, rx = porto.broadcast[int](2)
    for value in range(1, 6):
        tx.send(value)

    async def main() -> list[object]:
        seen: list[object] = []
        for _ in range(3):
            try:
                seen.append(await rx.recv())
            except porto.Lagged as lag:
                seen.append(f"lag:{lag.skipped}")
        return seen

    assert run(main()) == ["lag:3", 4, 5]


def test_a_receiver_exactly_capacity_behind_is_not_lagged() -> None:
    tx, rx = porto.broadcast[int](3)
    for value in (1, 2, 3):
        tx.send(value)
    assert drain(rx) == [1, 2, 3]
    for value in (4, 5, 6):
        tx.send(value)
    assert drain(rx) == [4, 5, 6]
    for value in (7, 8, 9, 10):
        tx.send(value)
    assert drain(rx) == ["lag:1", 8, 9, 10]


def test_lag_counts_add_up_to_everything_sent() -> None:
    tx, rx = porto.broadcast[int](2)
    received = 0
    skipped = 0
    sent = 0
    for burst in (1, 2, 3, 5, 2, 7, 1):
        for _ in range(burst):
            sent += 1
            tx.send(sent)
        for item in drain(rx):
            if isinstance(item, str):
                skipped += int(item.removeprefix("lag:"))
            else:
                received += 1
    assert received + skipped == sent
    assert skipped > 0


def test_async_for_raises_lagged_out_of_the_loop_and_a_new_loop_continues() -> None:
    tx, rx = porto.broadcast[int](2)
    for value in range(1, 6):
        tx.send(value)
    tx.close()
    seen: list[object] = []

    async def main() -> None:
        try:
            async for message in rx:
                seen.append(message)
        except porto.Lagged as lag:
            seen.append(f"lag:{lag.skipped}")
        async for message in rx:
            seen.append(message)

    run(main())
    assert seen == ["lag:3", 4, 5]


def test_lagged_survives_pickling() -> None:
    import pickle

    restored = pickle.loads(pickle.dumps(porto.Lagged(5)))
    assert isinstance(restored, porto.Lagged)
    assert restored.skipped == 5


# Closure


def test_closing_all_senders_ends_every_receiver_after_it_drains() -> None:
    tx, rx = porto.broadcast[int](4)
    other = rx.clone()
    tx.send(1)
    tx.send(2)
    second = tx.clone()
    tx.close()
    assert rx.try_recv() == 1, "another sender is open"
    second.close()
    assert drain(rx) == [2]
    with pytest.raises(porto.EndOfChannel):
        rx.try_recv()
    assert drain(other) == [1, 2]

    async def main() -> None:
        with pytest.raises(porto.EndOfChannel, match="kept message"):
            await other.recv()

    run(main())


def test_a_waiting_recv_ends_when_the_last_sender_is_dropped() -> None:
    tx, rx = porto.broadcast[int](2)

    async def receive() -> str:
        try:
            await rx.recv()
        except porto.EndOfChannel:
            return "ended"
        return "got a message"

    async def main() -> str:
        task = _core._spawn_joinable(receive())
        await checkpoint()
        await checkpoint()
        nonlocal tx
        del tx
        return await task.join()

    assert run(main()) == "ended"


def test_async_with_closes_the_sender_on_the_way_out() -> None:
    tx, rx = porto.broadcast[int](4)
    seen: list[int] = []

    async def main() -> None:
        async with tx as same:
            assert same is tx
            tx.send(1)
        with pytest.raises(porto.ClosedResourceError):
            tx.send(2)
        async for message in rx:
            seen.append(message)

    run(main())
    assert seen == [1]


def test_a_closed_sender_raises_closed_resource_error_and_its_clones_are_closed() -> None:
    tx, rx = porto.broadcast[int](2)
    tx.close()
    tx.close()
    with pytest.raises(porto.ClosedResourceError, match="broadcast sender was closed"):
        tx.send(1)
    with pytest.raises(porto.ClosedResourceError):
        tx.subscribe()
    with pytest.raises(porto.ClosedResourceError):
        tx.clone().send(1)
    del rx


def test_send_without_a_receiver_is_broken_until_someone_subscribes() -> None:
    tx, rx = porto.broadcast[int](2)
    del rx
    assert tx.is_closed()
    with pytest.raises(porto.BrokenResourceError, match=r"tx\.subscribe\(\)"):
        tx.send(1)
    rx = tx.subscribe()
    assert not tx.is_closed()
    tx.send(2)
    assert drain(rx) == [2], "the broken send kept nothing"


def test_a_closed_receiver_raises_closed_resource_error_and_leaves_the_others() -> None:
    tx, rx = porto.broadcast[int](2)
    other = rx.clone()
    rx.close()
    rx.close()
    with pytest.raises(porto.ClosedResourceError):
        rx.try_recv()
    with pytest.raises(porto.ClosedResourceError):
        run(rx.recv())
    with pytest.raises(porto.ClosedResourceError):
        rx.blocking_recv()
    with pytest.raises(porto.ClosedResourceError):
        rx.clone().try_recv()
    tx.send(1)
    assert other.try_recv() == 1


def test_closing_the_receiver_wakes_a_waiting_recv() -> None:
    tx, rx = porto.broadcast[int](2)

    async def receive() -> str:
        try:
            await rx.recv()
        except porto.ClosedResourceError:
            return "closed"
        return "got a message"

    async def main() -> str:
        task = _core._spawn_joinable(receive())
        await checkpoint()
        await checkpoint()
        rx.close()
        return await task.join()

    assert run(main()) == "closed"
    del tx


def test_try_recv_is_would_block_until_something_is_sent() -> None:
    tx, rx = porto.broadcast[int](2)
    with pytest.raises(porto.WouldBlock, match=r"await rx\.recv"):
        rx.try_recv()
    tx.send(1)
    assert rx.try_recv() == 1
    with pytest.raises(porto.WouldBlock):
        rx.try_recv()


# Python objects


def test_a_message_is_shared_not_copied_and_released_with_the_last_holder() -> None:
    class Payload:
        pass

    tx, rx = porto.broadcast[Payload](2)
    other = rx.clone()
    payload = Payload()
    alive = weakref.ref(payload)
    tx.send(payload)
    del payload
    assert alive() is not None
    a = rx.try_recv()
    b = other.try_recv()
    assert a is b, "every receiver gets the same object"
    del a, b
    assert alive() is not None, "the channel still keeps it for late receivers"
    tx.send(Payload())
    tx.send(Payload())
    assert alive() is None, "overwritten once nobody holds it"


def test_a_message_a_slow_receiver_holds_outlives_the_overwrite() -> None:
    class Payload:
        pass

    tx, rx = porto.broadcast[Payload](1)
    first = Payload()
    alive = weakref.ref(first)
    tx.send(first)
    del first
    held = rx.try_recv()
    tx.send(Payload())
    assert alive() is held
    del held
    assert alive() is None


def test_messages_are_released_with_the_channel() -> None:
    class Payload:
        pass

    tx, rx = porto.broadcast[Payload](4)
    payload = Payload()
    alive = weakref.ref(payload)
    tx.send(payload)
    del payload
    del tx, rx
    assert alive() is None


def test_a_message_returned_by_a_failed_send_is_not_kept() -> None:
    class Payload:
        pass

    tx, rx = porto.broadcast[Payload](2)
    del rx
    payload = Payload()
    alive = weakref.ref(payload)
    with pytest.raises(porto.BrokenResourceError):
        tx.send(payload)
    del payload
    assert alive() is None


def test_an_operation_never_awaited_takes_nothing_and_warns() -> None:
    tx, rx = porto.broadcast[int](2)
    tx.send(1)
    pending = rx.recv()
    with pytest.warns(RuntimeWarning, match=r"BroadcastReceiver\.recv\(\) was never awaited"):
        del pending
    assert rx.try_recv() == 1


# Cancellation


def test_a_cancelled_recv_loses_nothing_and_does_not_disturb_the_other_receivers() -> None:
    tx, rx = porto.broadcast[int](4)
    other = rx.clone()
    tokens: list[_core._TaskToken] = []
    outcomes: dict[str, object] = {}

    async def receive(name: str, receiver: porto.BroadcastReceiver[int]) -> None:
        tokens.append(_core._current_task_token())
        try:
            outcomes[name] = await receiver.recv()
        except KeyError:
            outcomes[name] = "interrupted"

    async def main() -> None:
        first = _core._spawn_joinable(receive("first", rx))
        second = _core._spawn_joinable(receive("second", other))
        await wait_until(lambda: len(tokens) == 2)
        await checkpoint()
        _core._interrupt(tokens[0], KeyError("cancelled"))
        await first.join()
        tx.send(7)
        await second.join()

    run(main())
    assert outcomes == {"first": "interrupted", "second": 7}
    assert rx.try_recv() == 7, "the cancelled receiver still gets the message"


def test_a_recv_woken_before_the_interrupt_keeps_its_message() -> None:
    tx, rx = porto.broadcast[int](4)
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
        tx.send(5)
        _core._interrupt(tokens[0], KeyError("cancelled"))
        return await task.join()

    assert run(main()) == [5, "interrupted"]


@pytest.mark.parametrize("op", ["recv", "anext"])
def test_a_cancelled_scope_raises_cancelled_at_a_recv_that_would_complete(op: str) -> None:
    tx, rx = porto.broadcast[int](4)
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


# Threads


def test_blocking_recv_waits_for_a_send_from_another_thread() -> None:
    tx, rx = porto.broadcast[int](4)
    seen: list[int] = []
    thread = threading.Thread(target=lambda: seen.append(rx.blocking_recv()))
    thread.start()
    tx.send(3)
    thread.join(10)
    assert seen == [3]


def test_blocking_recv_reports_a_lag_and_the_end() -> None:
    tx, rx = porto.broadcast[int](1)
    tx.send(1)
    tx.send(2)
    with pytest.raises(porto.Lagged):
        rx.blocking_recv()
    assert rx.blocking_recv() == 2
    tx.close()
    with pytest.raises(porto.EndOfChannel):
        rx.blocking_recv()


def test_blocking_recv_on_a_lane_raises_runtime_error_with_what_to_do() -> None:
    tx, rx = porto.broadcast[int](1)

    async def main() -> None:
        with pytest.raises(RuntimeError, match="Await the async form instead"):
            rx.blocking_recv()

    porto.run(main)
    del tx


def test_senders_on_several_threads_reach_every_receiver() -> None:
    tx, rx = porto.broadcast[int](4096)
    other = rx.clone()
    senders = 4
    per_sender = 500

    def produce(base: int) -> None:
        sender = tx.clone()
        for i in range(per_sender):
            sender.send(base + i)
        sender.close()

    threads = [threading.Thread(target=produce, args=(n * 10_000,)) for n in range(senders)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    tx.close()
    first = drain(rx)
    second = drain(other)
    assert first == second
    assert sorted(first) == sorted(
        n * 10_000 + i for n in range(senders) for i in range(per_sender)
    )
