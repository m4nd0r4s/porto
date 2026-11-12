"""The public API as a user's type checker sees it (ADR-028, pyright strict).

The ``check_*`` functions are only type-checked; ``test_*`` also run them.
"""

import asyncio
from typing import Any, Literal, assert_type

import porto


def check_version_is_str() -> None:
    assert_type(porto.__version__, str)


def square(value: int) -> int:
    return value * value


async def check_to_thread() -> None:
    assert_type(await porto.to_thread(square, 3), int)
    assert_type(await porto.to_thread(square, 3, cancellable=True), int)
    assert_type(await porto.to_thread(sum, [1, 2], start=0), int)


async def fetch(host: str, port: int) -> bytes:
    await porto.sleep(0)
    return f"{host}:{port}".encode()


async def total() -> int:
    await porto.checkpoint()
    return 3


async def main(hosts: list[str]) -> list[bytes]:
    # The exit may absorb a `Cancelled` (`__aexit__` returns a bool), so names bound in the
    # block are possibly unbound after it.
    from_function: list[porto.Task[bytes]] = []
    async with porto.scope() as s:
        assert_type(s, porto.Scope)
        from_function = [s.spawn(fetch, host, 443) for host in hosts]
        from_coroutine = s.spawn(total())
        assert_type(from_function[0], porto.Task[bytes])
        assert_type(from_coroutine, porto.Task[int])
        assert_type(from_coroutine.done(), bool)
    assert_type(s.cancelled_caught, bool)
    return [task.result() for task in from_function]


async def with_deadlines() -> bool:
    async with porto.timeout(1.5) as limited:
        assert_type(limited, porto.Scope)
        limited.deadline = porto.current_time() + 2
        assert_type(limited.deadline, float)
    async with porto.move_on_after(1) as quiet:
        await porto.sleep_forever()
    async with porto.scope(timeout=1, deadline=5.0, raise_on_timeout=False) as both:
        pass
    porto.timeout("1")  # pyright: ignore[reportArgumentType]
    with porto.shield(timeout=5) as shielded:
        assert_type(shielded, porto.Scope)
        await porto.sleep(0)
    async with porto.scope(shield=True) as also:
        assert_type(also.shield, bool)
        also.shield = False
    return limited.timed_out or quiet.timed_out or both.timed_out


def check_run_infers_the_result_type() -> None:
    assert_type(porto.run(main, ["a"]), list[bytes])
    assert_type(porto.run(main(["a"])), list[bytes])
    assert_type(porto.run(total), int)


def check_wrong_arguments_are_type_errors() -> None:
    porto.run(main, 1)  # pyright: ignore[reportArgumentType]
    porto.run(main)  # pyright: ignore[reportArgumentType]


def check_run_takes_a_config_and_overrides() -> None:
    config = porto.RuntimeConfig(lanes="auto", steal="eager", slow_step_threshold=None)
    assert_type(config.lanes, int | Literal["auto"])
    assert_type(config.slow_step_threshold, float | None)
    assert_type(config.deterministic, porto.DeterministicConfig | None)
    assert_type(porto.run(total, config=config), int)
    assert_type(porto.run(total, config=config, coop_budget=64), int)
    assert_type(porto.run(main, ["a"], lanes=1), list[bytes])


def check_config_values_are_typed() -> None:
    porto.RuntimeConfig(lanes="many")  # pyright: ignore[reportArgumentType]
    porto.RuntimeConfig(steal="always")  # pyright: ignore[reportArgumentType]
    porto.RuntimeConfig(4)  # pyright: ignore[reportCallIssue]
    porto.DeterministicConfig(clock="wall")  # pyright: ignore[reportArgumentType]
    porto.run(total, config={"lanes": 2})  # pyright: ignore[reportArgumentType]


async def check_spawn_checks_the_arguments() -> None:
    async with porto.scope() as s:
        s.spawn(fetch, "a")  # pyright: ignore[reportArgumentType]
        s.spawn(fetch, "a", "443")  # pyright: ignore[reportArgumentType]


def check_error_attributes_are_typed() -> None:
    try:
        porto.run(total)
    except porto.IncompleteReadError as error:
        assert_type(error.partial, bytes)
        assert_type(error.expected, int | None)
    except porto.Lagged as error:
        assert_type(error.skipped, int)
    except porto.RustPanic as error:
        assert_type(error.message, str)
        assert_type(error.location, str | None)


def check_errors_are_the_builtins_they_extend() -> None:
    no_runtime: RuntimeError = porto.NoRuntimeError("porto: no runtime")
    closed: OSError = porto.ClosedResourceError("porto: closed")
    end: EOFError = porto.IncompleteReadError(b"")
    cancelled: asyncio.CancelledError = porto.Cancelled()
    terminated: SystemExit = porto.Terminated(0)
    every: list[porto.PortoError] = [porto.Lagged(1), porto.RustPanic("boom"), porto.WouldBlock()]
    del no_runtime, closed, end, cancelled, terminated, every


async def sync_primitives() -> int:
    event, notify, lock = porto.Event(), porto.Notify(), porto.Lock()
    event.set()
    assert_type(event.is_set(), bool)
    await event.wait()
    notify.notify_one()
    await notify.notified()
    assert_type(lock.try_acquire(), bool)
    lock.release()
    async with lock:
        assert_type(lock.locked(), bool)
    counter = porto.Mutex[int](0)
    async with counter as guard:
        assert_type(guard, porto.MutexGuard[int])
        assert_type(guard.value, int)
        guard.set(guard.value + 1)
    async with counter as guard:
        return guard.value


async def more_sync_primitives() -> bool:
    semaphore = porto.Semaphore(4)
    async with semaphore:
        assert_type(semaphore.available, int)
    await semaphore.acquire(2)
    semaphore.release(2)
    assert_type(semaphore.try_acquire(), bool)
    limiter = porto.CapacityLimiter(2)
    async with limiter:
        assert_type(limiter.borrowed_tokens, int)
    assert_type(limiter.total_tokens, float)
    limiter.total_tokens = 3
    await limiter.acquire_on_behalf_of(object())
    rwlock = porto.RwLock()
    async with rwlock.read():
        assert_type(rwlock.readers, int)
    async with rwlock.write():
        assert_type(rwlock.write_locked(), bool)
    condition = porto.Condition(porto.Lock())
    async with condition:
        condition.notify_all()
        assert_type(await condition.wait_for(lambda: 3), int)
    return await porto.Barrier(1).wait()


async def check_a_mutex_guard_checks_the_value_type() -> None:
    async with porto.Mutex[int](0) as guard:
        guard.set("one")  # pyright: ignore[reportArgumentType]


async def pipeline(items: list[str]) -> list[str]:
    tx, rx = porto.channel[str](4)
    assert_type(tx, porto.Sender[str])
    assert_type(rx, porto.Receiver[str])
    assert_type(tx.capacity, int | None)
    assert_type(tx.is_closed(), bool)
    assert_type(tx.clone(), porto.Sender[str])
    assert_type(rx.clone(), porto.Receiver[str])

    async def produce() -> None:
        async with tx:
            for item in items:
                await tx.send(item)

    received: list[str] = []
    async with porto.scope() as s:
        s.spawn(produce)
        async for item in rx:
            assert_type(item, str)
            received.append(item)
    return received


async def check_channel_methods_are_typed() -> None:
    tx, rx = porto.unbounded_channel[int]()
    assert_type(tx.capacity, int | None)
    tx.try_send(1)
    tx.blocking_send(2)
    assert_type(await rx.recv(), int)
    assert_type(rx.try_recv(), int)
    assert_type(rx.blocking_recv(), int)
    assert_type(await rx.recv_many(8), list[int])
    tx.close()
    rx.close()


def check_channel_misuse_is_a_type_error() -> None:
    porto.channel[int]()  # pyright: ignore[reportCallIssue]
    porto.channel[int]("4")  # pyright: ignore[reportArgumentType]
    tx, _ = porto.channel[int](4)
    tx.try_send("one")  # pyright: ignore[reportArgumentType]
    # A receiver of ints is a receiver of objects; a sender of objects is a sender of ints.
    objects: porto.Receiver[object] = porto.channel[int](1)[1]
    ints: porto.Sender[int] = porto.channel[object](1)[0]
    del objects, ints


async def check_oneshot_is_typed() -> None:
    tx, rx = porto.oneshot[int]()
    assert_type(tx, porto.OneshotSender[int])
    assert_type(rx, porto.OneshotReceiver[int])
    tx.send(1)  # never waits: not awaited
    assert_type(await rx.recv(), int)
    assert_type(rx.try_recv(), int)
    assert_type(rx.blocking_recv(), int)
    async for item in rx:
        assert_type(item, int)
    async with tx:
        pass
    tx.close()
    rx.close()


def check_oneshot_misuse_is_a_type_error() -> None:
    tx, rx = porto.oneshot[int]()
    tx.send("one")  # pyright: ignore[reportArgumentType]
    tx.send()  # pyright: ignore[reportCallIssue]
    porto.oneshot[int](1)  # pyright: ignore[reportCallIssue]
    rx.recv_many(2)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    tx.clone()  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    objects: porto.OneshotReceiver[object] = porto.oneshot[int]()[1]
    ints: porto.OneshotSender[int] = porto.oneshot[object]()[0]
    del objects, ints


async def oneshot_roundtrip() -> int:
    tx, rx = porto.oneshot[int]()
    tx.send(5)
    return await rx.recv()


def test_the_oneshot_sample_runs() -> None:
    assert porto.run(oneshot_roundtrip) == 5


async def check_watch_is_typed() -> None:
    tx, rx = porto.watch[bool](False)
    assert_type(tx, porto.WatchSender[bool])
    assert_type(rx, porto.WatchReceiver[bool])
    tx.send(True)  # never waits: not awaited
    assert_type(rx.borrow(), bool)
    assert_type(rx.borrow_and_update(), bool)
    assert_type(rx.has_changed(), bool)
    await rx.changed()
    rx.try_changed()
    rx.blocking_changed()
    assert_type(rx.clone(), porto.WatchReceiver[bool])
    assert_type(tx.clone(), porto.WatchSender[bool])
    assert_type(tx.subscribe(), porto.WatchReceiver[bool])
    async for flag in rx:
        assert_type(flag, bool)
    async with tx:
        pass


def check_watch_misuse_is_a_type_error() -> None:
    porto.watch[int]()  # pyright: ignore[reportCallIssue]
    porto.watch[int]("zero")  # pyright: ignore[reportArgumentType]
    tx, rx = porto.watch[int](0)
    tx.send("one")  # pyright: ignore[reportArgumentType]
    rx.recv()  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    objects: porto.WatchReceiver[object] = porto.watch[int](0)[1]
    ints: porto.WatchSender[int] = porto.watch[object](0)[0]
    del objects, ints


async def watch_roundtrip() -> int:
    tx, rx = porto.watch[int](0)
    tx.send(1)
    tx.send(2)
    await rx.changed()
    return rx.borrow()


def test_the_watch_sample_runs() -> None:
    assert porto.run(watch_roundtrip) == 2


async def check_broadcast_is_typed() -> None:
    tx, rx = porto.broadcast[str](8)
    assert_type(tx, porto.BroadcastSender[str])
    assert_type(rx, porto.BroadcastReceiver[str])
    assert_type(tx.capacity, int)
    tx.send("hello")  # never waits: not awaited
    assert_type(await rx.recv(), str)
    assert_type(rx.try_recv(), str)
    assert_type(rx.blocking_recv(), str)
    assert_type(rx.clone(), porto.BroadcastReceiver[str])
    assert_type(tx.clone(), porto.BroadcastSender[str])
    assert_type(tx.subscribe(), porto.BroadcastReceiver[str])
    try:
        await rx.recv()
    except porto.Lagged as lag:
        assert_type(lag.skipped, int)
    async for message in rx:
        assert_type(message, str)
    async with tx:
        pass


def check_broadcast_misuse_is_a_type_error() -> None:
    porto.broadcast[int]()  # pyright: ignore[reportCallIssue]
    porto.broadcast[int]("8")  # pyright: ignore[reportArgumentType]
    tx, rx = porto.broadcast[int](8)
    tx.send("one")  # pyright: ignore[reportArgumentType]
    rx.recv_many(2)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType]
    objects: porto.BroadcastReceiver[object] = porto.broadcast[int](1)[1]
    ints: porto.BroadcastSender[int] = porto.broadcast[object](1)[0]
    del objects, ints


async def broadcast_roundtrip() -> list[int]:
    tx, first = porto.broadcast[int](4)
    second = first.clone()
    tx.send(1)
    tx.send(2)
    return [await first.recv(), await second.recv(), await second.recv()]


def test_the_broadcast_sample_runs() -> None:
    assert porto.run(broadcast_roundtrip) == [1, 1, 2]


def test_the_channel_sample_runs() -> None:
    assert porto.run(pipeline, ["a", "b", "c"]) == ["a", "b", "c"]


def test_the_sample_runs() -> None:
    assert porto.run(main, ["a", "b"]) == [b"a:443", b"b:443"]


def test_the_config_sample_runs() -> None:
    check_run_takes_a_config_and_overrides()


def test_the_sync_sample_runs() -> None:
    assert porto.run(sync_primitives) == 1


def check_buffer_is_a_buffer() -> None:
    buffer = porto.Buffer(16)
    assert_type(len(buffer), int)
    assert_type(buffer.capacity, int)
    assert_type(bytes(buffer), bytes)
    view = memoryview(buffer)
    assert_type(view, memoryview)
    view.release()
    buffer.extend(b"more")
    buffer.resize(4)


def test_buffer_is_a_buffer() -> None:
    check_buffer_is_a_buffer()


async def task_handles() -> int:
    kept: list[porto.Task[bytes]] = []
    async with porto.scope() as s:
        task = s.spawn(fetch, "a", 1)
        kept.append(task)
        assert_type(task, porto.Task[bytes])
        assert_type(task.id, int)
        assert_type(task.name, str)
        assert_type(task.pinned, bool)
        assert_type(task.lane, int | None)
        assert_type(task.done(), bool)
        assert_type(task.cancelled(), bool)
        assert_type(task.cancel(), None)
        assert_type(task.exception(), BaseException | None)
        assert_type(await task, bytes)
    assert_type(kept[0].result(), bytes)
    assert_type(porto.current_task(), porto.Task[Any])
    assert_type(porto.lowlevel.current_lane(), int)
    await porto.lowlevel.trap_yield()
    await porto.lowlevel.trap_wait_notify(porto.Notify())
    return len(kept[0].result())


def check_lowlevel_calls_are_typed() -> None:
    porto.lowlevel.trap_wait_notify(porto.Lock())  # pyright: ignore[reportArgumentType]
    porto.lowlevel.current_lane(1)  # pyright: ignore[reportCallIssue]


async def serve(port: int, *, started: porto.Started[int]) -> None:
    started.set(port)


async def serve_nothing(*, started: porto.Started[None]) -> None:
    started.set()
    started.set(None)


async def check_start_and_started_are_typed() -> None:
    async with porto.scope() as s:
        listening: int = await s.start(serve, 80)
        await s.start(serve_nothing, name="quiet")
        assert_type(listening, int)


def check_started_values_are_typed(started: porto.Started[int], quiet: porto.Started[None]) -> None:
    started.set(1)
    started.set("one")  # pyright: ignore[reportCallIssue, reportArgumentType]
    started.set()  # pyright: ignore[reportCallIssue]
    quiet.set()
