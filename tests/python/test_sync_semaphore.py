"""porto.Semaphore and porto.CapacityLimiter (16-sync-primitives-and-channels.md §1).

Multi-permit FIFO fairness, bounded release, lazy waits, cancel safety; the limiter's
borrower rules (AnyIO's API). ``_interrupt(token, exc)`` throws into a waiting task the way
a cancellation does: the wait is given up first (T3).
"""

import math
import threading
from collections.abc import Awaitable

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


# Semaphore


def test_an_uncontended_acquire_does_not_suspend() -> None:
    async def main() -> None:
        semaphore = porto.Semaphore(3)
        assert completes_at_once(semaphore.acquire(2))
        assert semaphore.available == 1
        assert repr(semaphore) == "<porto.Semaphore available=1/3>"
        semaphore.release(2)
        assert semaphore.available == 3

    porto.run(main)


def test_async_with_holds_one_permit_and_gives_it_back_on_error() -> None:
    async def main() -> None:
        semaphore = porto.Semaphore(2)
        async with semaphore:
            assert semaphore.available == 1
            async with semaphore:
                assert semaphore.available == 0
        assert semaphore.available == 2
        with pytest.raises(KeyError):
            async with semaphore:
                raise KeyError("boom")
        assert semaphore.available == 2

    porto.run(main)


def test_a_large_request_blocks_smaller_ones_behind_it() -> None:
    # Fair: a waiter that wants more permits than are free is not overtaken by smaller ones.
    semaphore = porto.Semaphore(3)
    order: list[str] = []

    async def take(name: str, permits: int) -> None:
        await semaphore.acquire(permits)
        order.append(name)
        semaphore.release(permits)

    async def main() -> None:
        async with porto.scope() as s:
            await semaphore.acquire(2)
            s.spawn(take, "big", 3)
            await settle()
            s.spawn(take, "small", 1)
            await settle()
            assert order == [], "one permit is free, but the big request is ahead"
            assert not semaphore.try_acquire()
            semaphore.release(2)
        assert order == ["big", "small"]

    porto.run(main)


def test_waiters_are_granted_in_arrival_order() -> None:
    semaphore = porto.Semaphore(1)
    order: list[int] = []

    async def worker(n: int) -> None:
        async with semaphore:
            order.append(n)
            await porto.checkpoint()

    async def main() -> None:
        async with porto.scope() as s, semaphore:
            for n in range(5):
                s.spawn(worker, n)
            await settle()
        assert order == [0, 1, 2, 3, 4]

    porto.run(main)


def test_releasing_more_than_was_acquired_raises() -> None:
    semaphore = porto.Semaphore(2)
    with pytest.raises(ValueError, match=r"more permits than are acquired \(0\)"):
        semaphore.release()
    assert semaphore.try_acquire()
    with pytest.raises(ValueError, match=r"Semaphore\.release\(2\)"):
        semaphore.release(2)
    semaphore.release()
    assert semaphore.available == 2


@pytest.mark.parametrize("value", [0, -1, 2**31])
def test_a_semaphore_needs_at_least_one_permit(value: int) -> None:
    with pytest.raises(ValueError, match=r"needs 1 to 2147483647 permits"):
        porto.Semaphore(value)


@pytest.mark.parametrize("permits", [0, -2, 4])
def test_a_count_outside_one_to_the_value_raises(permits: int) -> None:
    async def main() -> None:
        semaphore = porto.Semaphore(3)
        with pytest.raises(ValueError, match=r"the count must be from 1 to 3"):
            semaphore.acquire(permits)
        with pytest.raises(ValueError, match=r"the count must be from 1 to 3"):
            semaphore.try_acquire(permits)
        with pytest.raises(ValueError, match=r"the count must be from 1 to 3"):
            semaphore.release(permits)
        assert semaphore.available == 3

    porto.run(main)


def test_acquire_does_nothing_until_awaited() -> None:
    async def main() -> None:
        semaphore = porto.Semaphore(1)
        not_awaited = semaphore.acquire()
        with pytest.warns(RuntimeWarning, match=r"Semaphore\.acquire\(\) was never awaited"):
            del not_awaited
        entering = semaphore.__aenter__()
        with pytest.warns(RuntimeWarning, match=r"Semaphore\.__aenter__\(\) was never awaited"):
            del entering
        assert semaphore.available == 1

    porto.run(main)


def test_entering_takes_a_free_permit_when_awaited_without_suspending() -> None:
    async def main() -> None:
        semaphore = porto.Semaphore(1)
        entering = semaphore.__aenter__()
        assert semaphore.available == 1
        assert completes_at_once(entering)
        assert semaphore.available == 0
        await semaphore.__aexit__(None, None, None)
        assert semaphore.available == 1

    porto.run(main)


def test_an_interrupted_waiter_does_not_leak_permits() -> None:
    semaphore = porto.Semaphore(2)
    tokens: list[_core._TaskToken] = []
    got: list[str] = []

    async def big() -> None:
        tokens.append(_core._current_task_token())
        try:
            await semaphore.acquire(2)
        except KeyError:
            got.append("big interrupted")

    async def small() -> None:
        async with semaphore:
            got.append("small")

    async def main() -> None:
        async with porto.scope() as s:
            await semaphore.acquire()
            s.spawn(big)
            s.spawn(small)
            await settle()
            assert got == []
            # The big request leaves the front: the small one behind it gets the free permit.
            _core._interrupt(tokens[0], KeyError("cancelled"))
            await settle()
            semaphore.release()
        assert got == ["big interrupted", "small"]
        assert semaphore.available == 2

    porto.run(main)


def test_a_waiter_cancelled_after_its_grant_passes_the_permits_on() -> None:
    semaphore = porto.Semaphore(2)

    async def waiter() -> None:
        await semaphore.acquire(2)

    async def main() -> None:
        await semaphore.acquire(2)
        _core._spawn_joinable(waiter())
        _core._spawn_joinable(waiter())
        await settle()
        # Grants the first waiter, which never runs again: the run ends and cancels both. The
        # first gives the permits to the second, which gives them back.
        semaphore.release(2)

    _core._run(main())
    assert semaphore.available == 2, "permits leaked"


def test_a_cancelled_task_entering_takes_nothing() -> None:
    semaphore = porto.Semaphore(1)

    async def main() -> None:
        async with porto.scope() as s:
            s.cancel()
            async with semaphore:
                pytest.fail("the block must not run")

    porto.run(main)
    assert semaphore.available == 1


def test_acquire_outside_run_says_where_to_await() -> None:
    with pytest.raises(porto.NoRuntimeError, match=r"porto\.Semaphore\.acquire\(\)"):
        porto.Semaphore(1).acquire()


def test_blocking_acquire_from_a_thread_and_refused_on_a_lane() -> None:
    semaphore = porto.Semaphore(1)
    acquired = threading.Event()

    def take() -> None:
        semaphore.blocking_acquire()
        acquired.set()
        semaphore.release()

    async def main() -> None:
        with pytest.raises(RuntimeError, match=r"Semaphore\.blocking_acquire\(\)"):
            semaphore.blocking_acquire()
        async with semaphore:
            threading.Thread(target=take).start()
            await porto.sleep(0.02)
            assert not acquired.is_set()

    porto.run(main)
    assert acquired.wait(5)


def test_threads_and_tasks_never_exceed_the_permits() -> None:
    semaphore = porto.Semaphore(2)
    inside = [0]
    peak = [0]
    guard = threading.Lock()

    def enter() -> None:
        with guard:
            inside[0] += 1
            peak[0] = max(peak[0], inside[0])

    def leave() -> None:
        with guard:
            inside[0] -= 1

    def thread_worker() -> None:
        for _ in range(100):
            semaphore.blocking_acquire()
            enter()
            leave()
            semaphore.release()

    async def task_worker() -> None:
        for _ in range(100):
            async with semaphore:
                enter()
                await porto.checkpoint()
                leave()

    async def main() -> None:
        threads = [threading.Thread(target=thread_worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        async with porto.scope() as s:
            for _ in range(3):
                s.spawn(task_worker)
        for thread in threads:
            thread.join()

    porto.run(main)
    assert peak[0] <= 2
    assert semaphore.available == 2


# CapacityLimiter


def test_a_limiter_lends_tokens_to_tasks() -> None:
    async def main() -> None:
        limiter = porto.CapacityLimiter(2)
        assert limiter.total_tokens == 2
        async with limiter:
            assert limiter.borrowed_tokens == 1
            assert limiter.available_tokens == 1
        assert limiter.borrowed_tokens == 0
        await limiter.acquire()
        assert repr(limiter) == "<porto.CapacityLimiter borrowed=1/2>"
        limiter.release()

    porto.run(main)


def test_a_task_borrowing_twice_raises_as_anyio_does() -> None:
    async def main() -> None:
        limiter = porto.CapacityLimiter(1)
        async with limiter:
            with pytest.raises(RuntimeError, match=r"already holds one of this"):
                await limiter.acquire()
            with pytest.raises(RuntimeError, match=r"already holds one of this"):
                limiter.acquire_nowait()
            with pytest.raises(RuntimeError, match=r"already holds one of this"):
                async with limiter:
                    pytest.fail("the block must not run")
        assert limiter.borrowed_tokens == 0

    porto.run(main)


def test_releasing_a_token_the_borrower_does_not_hold_raises() -> None:
    async def main() -> None:
        limiter = porto.CapacityLimiter(2)
        with pytest.raises(RuntimeError, match=r"does not hold any of this limiter's tokens"):
            limiter.release()
        borrower = object()
        limiter.acquire_on_behalf_of_nowait(borrower)
        with pytest.raises(RuntimeError, match=r"does not hold any"):
            limiter.release()
        with pytest.raises(RuntimeError, match=r"does not hold any"):
            limiter.release_on_behalf_of(object())
        limiter.release_on_behalf_of(borrower)
        assert limiter.borrowed_tokens == 0

    porto.run(main)


def test_tokens_are_borrowed_per_borrower_object() -> None:
    async def main() -> None:
        limiter = porto.CapacityLimiter(2)
        a, b = object(), object()
        await limiter.acquire_on_behalf_of(a)
        limiter.acquire_on_behalf_of_nowait(b)
        assert limiter.borrowed_tokens == 2
        with pytest.raises(porto.WouldBlock, match=r"no CapacityLimiter token is free"):
            limiter.acquire_nowait()
        with pytest.raises(RuntimeError, match=r"already holds"):
            await limiter.acquire_on_behalf_of(a)
        limiter.release_on_behalf_of(a)
        limiter.release_on_behalf_of(b)

    porto.run(main)


def test_raising_the_total_grants_waiting_tasks() -> None:
    limiter = porto.CapacityLimiter(1)
    got: list[int] = []

    async def worker(n: int) -> None:
        async with limiter:
            got.append(n)
            await porto.sleep(0.01)

    async def main() -> None:
        async with porto.scope() as s, limiter:
            s.spawn(worker, 1)
            s.spawn(worker, 2)
            await settle()
            assert got == []
            limiter.total_tokens = 3
            await settle()
            assert got == [1, 2]

    porto.run(main)


def test_lowering_the_total_takes_tokens_back_as_they_are_released() -> None:
    async def main() -> None:
        limiter = porto.CapacityLimiter(2)
        a, b = object(), object()
        limiter.acquire_on_behalf_of_nowait(a)
        limiter.acquire_on_behalf_of_nowait(b)
        limiter.total_tokens = 1
        assert limiter.borrowed_tokens == 2
        limiter.release_on_behalf_of(a)
        with pytest.raises(porto.WouldBlock):
            limiter.acquire_nowait()
        limiter.release_on_behalf_of(b)
        assert limiter.available_tokens == 1

    porto.run(main)


def test_an_unlimited_limiter_reports_infinity() -> None:
    limiter = porto.CapacityLimiter(math.inf)
    assert limiter.total_tokens == math.inf
    assert limiter.available_tokens == math.inf
    limiter.total_tokens = 5
    assert limiter.total_tokens == 5
    limiter.total_tokens = math.inf
    assert limiter.total_tokens == math.inf


@pytest.mark.parametrize(
    ("total", "error", "message"),
    [
        (0, ValueError, r"must be from 1"),
        (-3, ValueError, r"must be from 1"),
        (2.5, TypeError, r"must be an int or math\.inf"),
        ("4", TypeError, r"must be an int or math\.inf, got str"),
    ],
)
def test_a_bad_total_says_what_it_takes(
    total: object, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        porto.CapacityLimiter(total)  # pyright: ignore[reportArgumentType]
    limiter = porto.CapacityLimiter(1)
    with pytest.raises(error, match=message):
        limiter.total_tokens = total  # pyright: ignore[reportAttributeAccessIssue]
    assert limiter.total_tokens == 1


def test_borrowing_for_the_running_task_needs_one() -> None:
    limiter = porto.CapacityLimiter(1)
    with pytest.raises(RuntimeError, match=r"name the borrower with the"):
        limiter.acquire_nowait()
    with pytest.raises(RuntimeError, match=r"none runs here"):
        limiter.release()


def test_an_interrupted_limiter_waiter_does_not_leak_a_token() -> None:
    limiter = porto.CapacityLimiter(1)
    tokens: list[_core._TaskToken] = []
    got: list[str] = []

    async def first() -> None:
        tokens.append(_core._current_task_token())
        try:
            async with limiter:
                got.append("first")
        except KeyError:
            got.append("first interrupted")

    async def second() -> None:
        async with limiter:
            got.append("second")

    async def main() -> None:
        async with porto.scope() as s:
            await limiter.acquire()
            s.spawn(first)
            s.spawn(second)
            await settle()
            _core._interrupt(tokens[0], KeyError("cancelled"))
            await settle()
            limiter.release()
        assert got == ["first interrupted", "second"]
        assert limiter.borrowed_tokens == 0

    porto.run(main)
