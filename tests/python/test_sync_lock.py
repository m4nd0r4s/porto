"""porto.Lock and porto.Mutex[T] (16-sync-primitives-and-channels.md §1).

Fair FIFO handoff, non-reentrant, cancel-safe. ``_interrupt(token, exc)`` throws into a
waiting task the way cancellation will (porto-szb.36): the wait is given up first (T3).
"""

import threading
from collections.abc import Awaitable, Callable

import pytest

import porto
from porto import _core


def completes_at_once(awaitable: Awaitable[object]) -> bool:
    """Resumes ``awaitable`` once by hand: whether it completed without suspending."""
    steps = awaitable.__await__()
    try:
        steps.send(None)
    except StopIteration:
        return True
    steps.close()
    return False


async def settle() -> None:
    """Lets every other ready task run until it waits."""
    for _ in range(10):
        await porto.checkpoint()


# Lock


def test_an_uncontended_acquire_does_not_suspend() -> None:
    async def main() -> None:
        lock = porto.Lock()
        assert completes_at_once(lock.acquire())
        assert lock.locked()
        assert repr(lock) == "<porto.Lock locked>"
        lock.release()
        assert not lock.locked()

    porto.run(main)


def test_async_with_holds_the_lock_and_releases_it_on_error() -> None:
    async def main() -> None:
        lock = porto.Lock()
        async with lock:
            assert lock.locked()
        assert not lock.locked()
        with pytest.raises(KeyError):
            async with lock:
                raise KeyError("boom")
        assert not lock.locked()

    porto.run(main)


def test_try_acquire_and_release() -> None:
    lock = porto.Lock()
    assert lock.try_acquire()
    assert not lock.try_acquire()
    lock.release()
    with pytest.raises(RuntimeError, match=r"not held"):
        lock.release()


def test_waiters_acquire_in_arrival_order_under_contention() -> None:
    lock = porto.Lock()
    order: list[int] = []

    async def worker(n: int) -> None:
        for _ in range(3):
            async with lock:
                order.append(n)
                await porto.checkpoint()

    async def main() -> None:
        async with porto.scope() as s:
            for n in range(4):
                s.spawn(worker, n)

    porto.run(main)
    # Each release hands the lock to the longest waiter: strict round robin.
    assert order == [0, 1, 2, 3] * 3


def test_a_released_lock_is_handed_over_so_nobody_barges() -> None:
    lock = porto.Lock()
    got: list[str] = []

    async def waiter() -> None:
        async with lock:
            got.append("waiter")

    async def main() -> None:
        async with porto.scope() as s:
            await lock.acquire()
            s.spawn(waiter)
            await settle()
            lock.release()
            assert lock.locked(), "handed to the waiter"
            assert not lock.try_acquire()
        assert got == ["waiter"]
        assert not lock.locked()

    porto.run(main)


def test_the_lock_is_not_reentrant() -> None:
    """Documented: acquiring a held lock from its holder waits forever (detected by timeout)."""
    lock = porto.Lock()
    tokens: list[_core._TaskToken] = []

    async def reenter() -> str:
        tokens.append(_core._current_task_token())
        async with lock:
            try:
                async with lock:
                    return "reentered"
            except TimeoutError:
                return "deadlocked"

    async def main() -> None:
        task = _core._spawn_joinable(reenter())
        await porto.sleep(0.05)
        assert not task.done
        _core._interrupt(tokens[0], TimeoutError())
        assert await task.join() == "deadlocked"
        assert not lock.locked()

    porto.run(main)


def test_an_interrupted_waiter_does_not_leak_the_lock() -> None:
    lock = porto.Lock()
    tokens: list[_core._TaskToken] = []
    got: list[str] = []

    async def first() -> None:
        tokens.append(_core._current_task_token())
        try:
            async with lock:
                got.append("first")
        except KeyError:
            got.append("first interrupted")

    async def second() -> None:
        async with lock:
            got.append("second")

    async def main() -> None:
        async with porto.scope() as s:
            await lock.acquire()
            s.spawn(first)
            s.spawn(second)
            await settle()
            _core._interrupt(tokens[0], KeyError("cancelled"))
            await settle()
            lock.release()
        assert got == ["first interrupted", "second"]
        assert not lock.locked()

    porto.run(main)


def test_a_waiter_cancelled_after_the_handoff_passes_the_lock_on() -> None:
    lock = porto.Lock()

    async def waiter() -> None:
        await lock.acquire()

    async def main() -> None:
        await lock.acquire()
        _core._spawn_joinable(waiter())
        _core._spawn_joinable(waiter())
        await settle()
        # Hands the lock to the first waiter, which never runs again: the run ends and
        # cancels both. The first passes the lock to the second, which unlocks it.
        lock.release()

    _core._run(main())
    assert not lock.locked(), "the lock leaked"


def test_acquire_outside_run_says_where_to_await() -> None:
    lock = porto.Lock()
    with pytest.raises(porto.NoRuntimeError, match=r"porto\.Lock\.acquire\(\)"):
        lock.acquire()
    assert not lock.locked()


def test_blocking_acquire_from_a_thread_and_refused_on_a_lane() -> None:
    lock = porto.Lock()
    acquired = threading.Event()

    def take() -> None:
        lock.blocking_acquire()
        acquired.set()
        lock.release()

    async def main() -> None:
        with pytest.raises(RuntimeError, match=r"Lock\.blocking_acquire\(\)"):
            lock.blocking_acquire()
        async with lock:
            threading.Thread(target=take).start()
            await porto.sleep(0.02)
            assert not acquired.is_set()

    porto.run(main)
    assert acquired.wait(5)


def test_threads_serialize_on_a_lock() -> None:
    lock = porto.Lock()
    counter = [0]

    def bump() -> None:
        for _ in range(200):
            lock.blocking_acquire()
            seen = counter[0]
            counter[0] = seen + 1
            lock.release()

    threads = [threading.Thread(target=bump) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert counter[0] == 800


# Mutex[T]


def test_mutex_is_subscriptable_and_gives_its_value_through_a_guard() -> None:
    mutex = porto.Mutex[int](1)

    async def main() -> int:
        async with mutex as guard:
            assert mutex.locked()
            guard.set(guard.value + 1)
        assert not mutex.locked()
        async with mutex as guard:
            return guard.value

    assert porto.run(main) == 2
    assert porto.Mutex[list[str]].__origin__ is porto.Mutex  # type: ignore[attr-defined]


def test_a_mutex_guard_stops_working_after_its_block() -> None:
    mutex = porto.Mutex("value")

    async def main() -> porto.MutexGuard[str]:
        async with mutex as guard:
            pass
        return guard

    guard = porto.run(main)
    with pytest.raises(RuntimeError, match=r"used after its `async with` block"):
        _ = guard.value
    with pytest.raises(RuntimeError, match=r"used after"):
        guard.set("other")
    assert repr(guard) == "<porto.MutexGuard released>"


def test_mutex_waiters_get_it_in_arrival_order() -> None:
    mutex = porto.Mutex[list[int]]([])

    async def worker(n: int) -> None:
        for _ in range(2):
            async with mutex as guard:
                guard.value.append(n)
                await porto.checkpoint()

    async def main() -> list[int]:
        async with porto.scope() as s:
            for n in range(3):
                s.spawn(worker, n)
        async with mutex as guard:
            return guard.value

    assert porto.run(main) == [0, 1, 2, 0, 1, 2]


def test_an_interrupted_mutex_waiter_does_not_leak_it() -> None:
    mutex = porto.Mutex(0)
    tokens: list[_core._TaskToken] = []

    async def waiter() -> str:
        tokens.append(_core._current_task_token())
        try:
            async with mutex as guard:
                guard.set(1)
        except KeyError:
            return "interrupted"
        return "entered"

    async def main() -> None:
        async with mutex:
            task = _core._spawn_joinable(waiter())
            await settle()
            _core._interrupt(tokens[0], KeyError("cancelled"))
            assert await task.join() == "interrupted"
        assert not mutex.locked()
        async with mutex as guard:
            assert guard.value == 0

    porto.run(main)


# A lock the task has not observed yet is never leaked


def close_a_spinner(spin: Callable[[], Awaitable[None]], extra_completions: int) -> None:
    """Runs ``spin`` in a loop in a task that the run closes after a while, wherever it is
    suspended then (often at a budget yield). ``extra_completions`` synchronous completions per
    round shift which await of ``spin`` runs the budget out."""

    async def spinner() -> None:
        while True:
            await spin()
            for _ in range(extra_completions):
                await _core._checkpoint_if_needed(None)

    async def main() -> None:
        _core._spawn(spinner())
        await porto.sleep(0.002)

    porto.run(main)


def test_a_lock_is_free_after_the_run_closes_an_acquire_release_loop() -> None:
    for trial in range(120):
        lock = porto.Lock()

        async def acquire_release(lock: porto.Lock = lock) -> None:
            await lock.acquire()
            lock.release()

        close_a_spinner(acquire_release, trial % 3)
        assert not lock.locked(), "the lock leaked when the run closed its holder"


def test_a_lock_is_free_after_the_run_closes_an_async_with_loop() -> None:
    for trial in range(120):
        lock = porto.Lock()

        async def enter_exit(lock: porto.Lock = lock) -> None:
            async with lock:
                pass

        close_a_spinner(enter_exit, trial % 3)
        assert not lock.locked(), "the lock leaked when the run closed its holder"


def test_a_mutex_is_free_after_the_run_closes_an_async_with_loop() -> None:
    for trial in range(120):
        mutex = porto.Mutex(0)

        async def increment(mutex: porto.Mutex[int] = mutex) -> None:
            async with mutex as guard:
                guard.set(guard.value + 1)

        close_a_spinner(increment, trial % 3)
        assert not mutex.locked(), "the mutex leaked when the run closed its holder"


def test_acquire_does_nothing_until_awaited() -> None:
    async def main() -> None:
        lock = porto.Lock()
        not_awaited = lock.acquire()
        with pytest.warns(RuntimeWarning, match=r"Lock\.acquire\(\) was never awaited"):
            del not_awaited
        assert not lock.locked()
        mutex = porto.Mutex(0)
        entering = mutex.__aenter__()
        with pytest.warns(RuntimeWarning, match=r"Mutex\.__aenter__\(\) was never awaited"):
            del entering
        assert not mutex.locked()

    porto.run(main)


def test_a_release_by_another_task_breaks_exclusion_and_the_holder_gets_the_error() -> None:
    # Like asyncio.Lock, the holder is not recorded: documented, not detected.
    lock = porto.Lock()
    log: list[str] = []

    async def holder() -> None:
        try:
            async with lock:
                await settle()
                log.append(f"holder inside, locked={lock.locked()}")
        except RuntimeError as error:
            log.append(f"holder: {error}")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(holder)
            await porto.checkpoint()
            assert lock.locked()
            lock.release()  # the mistake: this task never acquired it
        assert log[0] == "holder inside, locked=False"
        assert "another task released it first" in log[1]

    porto.run(main)


def test_acquire_then_gives_the_lock_back_if_its_callback_raises() -> None:
    def fail() -> None:
        raise KeyError("on_acquired")

    async def main() -> None:
        lock = porto.Lock()
        assert await lock._acquire_then(lambda: "entered") == "entered"  # pyright: ignore[reportPrivateUsage]
        lock.release()
        with pytest.raises(KeyError, match="on_acquired"):
            await lock._acquire_then(fail)  # pyright: ignore[reportPrivateUsage]
        assert not lock.locked()

    porto.run(main)
