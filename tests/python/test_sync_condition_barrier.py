"""porto.Condition and porto.Barrier (16-sync-primitives-and-channels.md §1).

A condition wait releases the lock, waits for a notification and takes the lock again, also
when the task is cancelled; ``wait_for`` re-checks its predicate. A barrier releases each round
together, with exactly one leader, and is reusable.
"""

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


# Condition


def test_wait_releases_the_lock_and_takes_it_again_once_notified() -> None:
    condition = porto.Condition()
    events: list[str] = []

    async def waiter() -> None:
        async with condition:
            events.append("waiting")
            await condition.wait()
            events.append(f"notified, locked={condition.locked()}")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(waiter)
            await settle()
            assert not condition.locked(), "released while waiting"
            async with condition:
                condition.notify()
                await settle()
                assert events == ["waiting"], "the waiter needs the lock back first"
        assert events == ["waiting", "notified, locked=True"]

    porto.run(main)


def test_wait_takes_the_lock_again_even_when_cancelled() -> None:
    condition = porto.Condition()
    scopes: list[porto.Scope] = []
    seen: list[str] = []

    async def waiter() -> None:
        async with porto.scope() as own:
            scopes.append(own)
            async with condition:
                try:
                    await condition.wait()
                except porto.Cancelled:
                    seen.append(f"cancelled, locked={condition.locked()}")
                    raise
        seen.append("left")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(waiter)
            await settle()
            # Hold the lock: the cancelled waiter must wait for it before it raises.
            await condition.acquire()
            scopes[0].cancel()
            await settle()
            assert seen == []
            condition.release()
            await settle()
        assert seen == ["cancelled, locked=True", "left"]
        assert not condition.locked()

    porto.run(main)


def test_a_cancel_before_the_release_leaves_the_lock_held() -> None:
    # Cancelled at the wait's own checkpoint: the lock was never let go.
    seen: list[bool] = []

    async def main() -> None:
        condition = porto.Condition()
        async with porto.scope() as s, condition:
            s.cancel()
            with pytest.raises(porto.Cancelled):
                await condition.wait()
            seen.append(condition.locked())
        seen.append(condition.locked())

    porto.run(main)
    assert seen == [True, False]


def test_wait_for_rechecks_its_predicate_after_each_notification() -> None:
    condition = porto.Condition()
    items: list[int] = []
    checks = [0]

    def ready() -> list[int] | None:
        checks[0] += 1
        return items if len(items) >= 2 else None

    async def consumer() -> list[int]:
        async with condition:
            got = await condition.wait_for(ready)
            assert condition.locked()
            return list(got)

    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(consumer)
            await settle()
            for n in range(2):
                async with condition:
                    items.append(n)
                    condition.notify()
                await settle()
        assert task.result() == [0, 1]
        assert checks[0] == 3

    porto.run(main)


def test_wait_for_a_true_predicate_does_not_suspend() -> None:
    async def main() -> None:
        condition = porto.Condition()
        async with condition:
            assert completes_at_once(condition.wait_for(lambda: "yes"))
            assert await condition.wait_for(lambda: 7) == 7

    porto.run(main)


def test_notify_wakes_the_longest_waiting_first() -> None:
    condition = porto.Condition()
    woken: list[int] = []

    async def waiter(n: int) -> None:
        async with condition:
            await condition.wait()
            woken.append(n)

    async def main() -> None:
        async with porto.scope() as s:
            for n in range(4):
                s.spawn(waiter, n)
                await settle()
            async with condition:
                condition.notify(2)
            await settle()
            assert woken == [0, 1]
            async with condition:
                condition.notify_all()
        assert woken == [0, 1, 2, 3]

    porto.run(main)


def test_an_interrupted_waiter_leaves_the_queue_and_takes_the_lock_again() -> None:
    condition = porto.Condition()
    tokens: list[_core._TaskToken] = []
    got: list[str] = []

    async def first() -> None:
        tokens.append(_core._current_task_token())
        async with condition:
            try:
                await condition.wait()
            except KeyError:
                got.append(f"first interrupted, locked={condition.locked()}")

    async def second() -> None:
        async with condition:
            await condition.wait()
            got.append("second")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(first)
            await settle()
            s.spawn(second)
            await settle()
            async with condition:
                _core._interrupt(tokens[0], KeyError("cancelled"))
                await settle()
                assert got == [], "the interrupted waiter needs the lock back first"
                # It left the queue: the one notification goes to the second waiter.
                condition.notify()
            await settle()
        assert got == ["first interrupted, locked=True", "second"]

    porto.run(main)


def test_a_notification_whose_waiter_never_ran_leaks_nothing() -> None:
    # The run ends with the first waiter notified but not resumed: its cancellation passes
    # the notification on, and nobody is left holding the lock.
    condition = porto.Condition()

    async def waiter() -> None:
        async with condition:
            await condition.wait()

    async def main() -> None:
        _core._spawn_joinable(waiter())
        _core._spawn_joinable(waiter())
        await settle()
        async with condition:
            condition.notify()

    _core._run(main())
    assert not condition.locked(), "the lock leaked"


async def wait_in_a_helper(condition: porto.Condition) -> None:
    await condition.wait()


@pytest.mark.parametrize("nested", [False, True], ids=["direct", "through-a-helper"])
def test_a_waiter_freed_while_another_task_holds_the_lock_leaves_it_held(nested: bool) -> None:
    # The run ends with a waiter parked (lock released) and main holding the lock: the freed
    # waiter cannot take the lock back, so its `async with` exit must not release main's lock.
    condition = porto.Condition()
    exits: list[str] = []

    async def waiter() -> None:
        try:
            async with condition:
                if nested:
                    await wait_in_a_helper(condition)
                else:
                    await condition.wait()
        except BaseException as error:
            exits.append(type(error).__name__)
            raise

    async def main() -> None:
        _core._spawn_joinable(waiter())
        await settle()
        await condition.acquire()

    _core._run(main())
    assert exits == ["GeneratorExit"]
    assert condition.locked(), "the freed waiter released a lock another task held"
    condition.release()  # the holder's own release still works
    assert not condition.locked()


def test_a_waiter_freed_while_relocking_leaves_the_holders_lock_held() -> None:
    # Notified, the waiter queues for the lock main holds; the run ends before it gets it.
    condition = porto.Condition()

    async def waiter() -> None:
        async with condition:
            await condition.wait()

    async def main() -> None:
        _core._spawn_joinable(waiter())
        await settle()
        await condition.acquire()
        condition.notify()
        await settle()

    _core._run(main())
    assert condition.locked(), "the freed waiter released a lock another task held"
    condition.release()
    assert not condition.locked()


def test_a_waiter_freed_with_an_unobserved_relock_grant_passes_it_on_cleanly() -> None:
    # main hands the lock to the relocking waiter, with a second task queued behind it, and the
    # run ends before either runs: the grant goes down the queue and the lock ends free, with no
    # release of a lock the freed waiter no longer holds.
    condition = porto.Condition()

    async def waiter() -> None:
        async with condition:
            await condition.wait()

    async def queued() -> None:
        async with condition:
            pass

    async def main() -> None:
        _core._spawn_joinable(waiter())
        await settle()
        await condition.acquire()
        condition.notify()
        await settle()
        _core._spawn_joinable(queued())
        await settle()
        condition.release()

    _core._run(main())
    assert not condition.locked(), "the lock leaked"
    with pytest.raises(RuntimeError, match=r"not held"):
        condition.release()


def test_waiting_or_notifying_without_the_lock_raises() -> None:
    async def main() -> None:
        condition = porto.Condition()
        with pytest.raises(RuntimeError, match=r"Condition\.wait\(\) needs the condition's lock"):
            await condition.wait()
        with pytest.raises(RuntimeError, match=r"Condition\.notify\(\) needs"):
            condition.notify()
        with pytest.raises(RuntimeError, match=r"Condition\.notify_all\(\) needs"):
            condition.notify_all()
        with pytest.raises(RuntimeError, match=r"not held"):
            condition.release()
        async with condition:
            with pytest.raises(ValueError, match=r"cannot be negative"):
                condition.notify(-1)

    porto.run(main)


def test_a_condition_takes_a_porto_lock_only() -> None:
    lock = porto.Lock()
    assert porto.Condition(lock).lock is lock
    with pytest.raises(TypeError, match=r"takes a porto\.Lock, got Event"):
        porto.Condition(porto.Event())  # pyright: ignore[reportArgumentType]


def test_wait_does_nothing_until_awaited() -> None:
    async def main() -> None:
        condition = porto.Condition()
        async with condition:
            waiting = condition.wait()
            with pytest.warns(RuntimeWarning, match=r"Condition\.wait\(\) was never awaited"):
                del waiting
            assert condition.locked()

    porto.run(main)


def test_a_condition_hands_work_from_a_thread() -> None:
    condition = porto.Condition()
    items: list[int] = []

    def produce() -> None:
        for n in range(50):
            condition.lock.blocking_acquire()
            items.append(n)
            condition.notify_all()
            condition.release()

    async def main() -> None:
        thread = threading.Thread(target=produce)
        async with condition:
            thread.start()
            await condition.wait_for(lambda: len(items) == 50)
        thread.join()

    porto.run(main)


# Barrier


def test_exactly_one_leader_per_round_and_the_barrier_is_reusable() -> None:
    barrier = porto.Barrier(3)
    leaders: list[int] = []
    rounds = 5

    async def party(n: int) -> None:
        for round_ in range(rounds):
            if await barrier.wait():
                leaders.append(round_)
            await porto.checkpoint()
        del n

    async def main() -> None:
        async with porto.scope() as s:
            for n in range(3):
                s.spawn(party, n)

    porto.run(main)
    assert leaders == list(range(rounds))
    assert barrier.n_waiting == 0


def test_nobody_goes_on_before_the_last_arrival() -> None:
    barrier = porto.Barrier(2)
    events: list[str] = []

    async def first() -> None:
        led = await barrier.wait()
        events.append(f"first led={led}")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(first)
            await settle()
            assert barrier.n_waiting == 1
            assert events == []
            assert completes_at_once(barrier.wait()), "the last arrival does not suspend"
            await settle()
        assert events == ["first led=False"]
        assert repr(barrier) == "<porto.Barrier waiting=0/2>"

    porto.run(main)


def test_a_cancelled_waiter_leaves_its_round() -> None:
    barrier = porto.Barrier(2)
    tokens: list[_core._TaskToken] = []
    got: list[str] = []

    async def leaver() -> None:
        tokens.append(_core._current_task_token())
        try:
            await barrier.wait()
        except KeyError:
            got.append("left")

    async def stayer() -> None:
        got.append(f"stayer led={await barrier.wait()}")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(leaver)
            await settle()
            _core._interrupt(tokens[0], KeyError("cancelled"))
            await settle()
            assert barrier.n_waiting == 0
            s.spawn(stayer)
            await settle()
            assert got == ["left"], "one arrival does not complete a round of two"
            assert await barrier.wait()
        assert got == ["left", "stayer led=False"]

    porto.run(main)


def test_a_barrier_needs_a_party() -> None:
    with pytest.raises(ValueError, match=r"Barrier\(0\) needs at least one party"):
        porto.Barrier(0)
    assert porto.Barrier(1).parties == 1


def test_barrier_wait_does_nothing_until_awaited() -> None:
    async def main() -> None:
        barrier = porto.Barrier(1)
        waiting = barrier.wait()
        with pytest.warns(RuntimeWarning, match=r"Barrier\.wait\(\) was never awaited"):
            del waiting
        assert barrier.n_waiting == 0
        assert await barrier.wait()

    porto.run(main)


def test_threads_and_a_task_meet_at_a_barrier() -> None:
    barrier = porto.Barrier(3)
    leaders: list[bool] = []

    def party() -> None:
        leaders.append(barrier.blocking_wait())

    async def main() -> None:
        with pytest.raises(RuntimeError, match=r"Barrier\.blocking_wait\(\)"):
            barrier.blocking_wait()
        threads = [threading.Thread(target=party) for _ in range(2)]
        for thread in threads:
            thread.start()
        leaders.append(await barrier.wait())
        for thread in threads:
            thread.join()

    porto.run(main)
    assert sorted(leaders) == [False, False, True]
