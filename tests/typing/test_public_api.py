"""The public API as a user's type checker sees it (ADR-028, pyright strict).

The ``check_*`` functions are only type-checked; ``test_*`` also run them.
"""

import asyncio
from typing import Literal, assert_type

import porto


def check_version_is_str() -> None:
    assert_type(porto.__version__, str)


async def fetch(host: str, port: int) -> bytes:
    await porto.sleep(0)
    return f"{host}:{port}".encode()


async def total() -> int:
    await porto.checkpoint()
    return 3


async def main(hosts: list[str]) -> list[bytes]:
    async with porto.scope() as s:
        assert_type(s, porto.Scope)
        from_function = [s.spawn(fetch, host, 443) for host in hosts]
        from_coroutine = s.spawn(total())
        assert_type(from_function[0], porto.Task[bytes])
        assert_type(from_coroutine, porto.Task[int])
    assert_type(from_coroutine.result(), int)
    assert_type(from_coroutine.done(), bool)
    return [task.result() for task in from_function]


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
    assert_type(porto.run(main, ["a"], lanes=2), list[bytes])


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


def test_the_channel_sample_runs() -> None:
    assert porto.run(pipeline, ["a", "b", "c"]) == ["a", "b", "c"]


def test_the_sample_runs() -> None:
    assert porto.run(main, ["a", "b"]) == [b"a:443", b"b:443"]


def test_the_config_sample_runs() -> None:
    check_run_takes_a_config_and_overrides()


def test_the_sync_sample_runs() -> None:
    assert porto.run(sync_primitives) == 1
