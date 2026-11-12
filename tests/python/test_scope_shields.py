"""Shield scopes (15-structured-concurrency-and-cancellation.md C8; 03 ST-9).

A shielded scope (``porto.shield()``, ``porto.scope(shield=True)``, ``Scope.shield = True``)
keeps the cancels and deadlines of the scopes it is in out; its own cancel and deadline still
count. Entered with sync ``with``, a scope is a cancel scope only: it does not await on entry or
exit, and it cannot spawn. ``Scope.shield`` is read/write, and changing it re-evaluates the
tasks in the scope at once.
"""

import contextlib
import gc
import math
import threading
import warnings

import pytest

import porto
from porto import _core

#: How late a deadline may fire on a loaded CI machine before a test calls it a hang.
LATE = 2.0


def test_C8_shield_blocks_outer_cancel() -> None:
    async def main() -> tuple[porto.Scope, list[str]]:
        steps: list[str] = []
        async with porto.scope() as outer:
            outer.cancel()
            async with porto.shield() as inner:
                await porto.sleep(0.01)
                await porto.checkpoint()
                steps.append("shielded awaits ran")
            # The shield's exit is a checkpoint of the cancelled scope around it.
            steps.append("not reached")
        assert not inner.cancelled_caught
        return outer, steps

    outer, steps = porto.run(main)
    assert steps == ["shielded awaits ran"]
    assert outer.cancelled_caught


def test_C8_an_async_with_shield_in_the_finally_of_a_cancelled_task_runs_its_cleanup() -> None:
    """ST-9's ``async with porto.scope(shield=True, timeout=5)`` form: entering the shield is not
    a checkpoint of the cancelled scope around it."""

    async def main() -> list[str]:
        log: list[str] = []
        async with porto.scope() as s:
            try:
                s.cancel()
                await porto.checkpoint()
            finally:
                async with porto.scope(shield=True, timeout=5):
                    await porto.sleep(0.005)
                    log.append("cleaned up")
        return log

    assert porto.run(main) == ["cleaned up"]


def test_C8_entering_shields_in_a_loop_still_yields_to_the_other_tasks() -> None:
    async def main() -> int:
        turns = 0
        stop = False

        async def other() -> None:
            nonlocal turns
            while not stop:
                turns += 1
                await porto.checkpoint()

        async with porto.scope() as s:
            s.spawn(other)
            for _ in range(10_000):
                async with porto.shield():
                    pass
            stop = True
        return turns

    assert porto.run(main) > 0


def test_C8_shield_own_cancel_counts() -> None:
    async def main() -> porto.Scope:
        async with porto.scope(shield=True) as s:
            s.cancel()
            await porto.sleep_forever()
        return s

    s = porto.run(main)
    assert s.cancelled_caught
    assert s.shield


def test_C8_a_shield_keeps_an_outer_deadline_out_but_its_own_counts() -> None:
    async def outer_deadline() -> tuple[float, list[str]]:
        steps: list[str] = []
        start = porto.current_time()
        async with porto.move_on_after(0.01) as outer:
            async with porto.shield():
                await porto.sleep(0.04)
                steps.append("the shield outlived the outer deadline")
            await porto.checkpoint()
            steps.append("not reached")
        assert outer.timed_out
        return porto.current_time() - start, steps

    elapsed, steps = porto.run(outer_deadline)
    assert elapsed >= 0.04
    assert steps == ["the shield outlived the outer deadline"]

    async def own_deadline() -> float:
        start = porto.current_time()
        with pytest.raises(TimeoutError):
            async with porto.shield(timeout=0.02):
                await porto.sleep_forever()
        return porto.current_time() - start

    assert 0.02 <= porto.run(own_deadline) < 0.02 + LATE


def test_C8_shielded_children_are_out_of_reach_of_the_outer_cancel() -> None:
    async def main() -> list[str]:
        finished: list[str] = []

        async def child() -> None:
            await porto.sleep(0.02)
            finished.append("child")

        async with porto.scope() as outer, porto.shield() as inner:
            inner.spawn(child)
            outer.cancel()
        return finished

    assert porto.run(main) == ["child"]


def test_C8_sync_with_cannot_spawn() -> None:
    async def work() -> None:
        await porto.checkpoint()

    async def main() -> None:
        with porto.shield() as s:
            coro = work()
            with pytest.raises(RuntimeError, match="async with"):
                s.spawn(coro)
            # The refused coroutine was closed: no "never awaited" warning.
            assert coro.cr_frame is None
            with pytest.raises(RuntimeError, match="async with"):
                s.spawn(work)

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        porto.run(main)
        gc.collect()


def test_C8_shield_timeout_in_finally() -> None:
    """ST-9: a handler cancelled while it holds a transaction rolls it back in a shielded
    ``finally``; the rollback's awaits run although the handler's scope is cancelled, and a
    rollback that hangs is cut off by the shield's own timeout."""

    async def rollback(log: list[str], seconds: float) -> None:
        await porto.sleep(seconds)
        log.append("rolled back")

    async def handler(log: list[str], seconds: float) -> None:
        try:
            log.append("in transaction")
            await porto.sleep_forever()
        finally:
            with porto.shield(timeout=0.05):
                await rollback(log, seconds)

    async def disconnect(seconds: float) -> list[str]:
        log: list[str] = []
        async with porto.scope() as request:
            request.spawn(handler, log, seconds)
            await porto.sleep(0.005)
            request.cancel()
        return log

    assert porto.run(disconnect, 0.01) == ["in transaction", "rolled back"]

    # A rollback that hangs: the shield's timeout ends it, and its TimeoutError fails the
    # handler instead of leaking the connection forever.
    with pytest.raises(ExceptionGroup) as raised:
        porto.run(disconnect, math.inf)
    assert raised.group_contains(TimeoutError)


def test_C8_sync_with_shield_in_finally_of_the_body_itself() -> None:
    async def main() -> list[str]:
        log: list[str] = []
        async with porto.scope() as s:
            try:
                s.cancel()
                await porto.checkpoint()
            finally:
                with porto.shield():
                    await porto.sleep(0.005)
                    log.append("cleaned up")
        assert s.cancelled_caught
        return log

    assert porto.run(main) == ["cleaned up"]


def test_C8_unshielding_reaches_the_tasks_inside_at_once() -> None:
    async def main() -> tuple[list[str], int]:
        log: list[str] = []

        async def waiter(shield: porto.Scope) -> None:
            async with shield:
                try:
                    await porto.sleep_forever()
                except porto.Cancelled:
                    log.append("woken by the unshield")
                    raise

        async with porto.scope() as outer:
            shield = porto.shield()
            outer.spawn(waiter, shield)
            await porto.sleep(0.005)
            # Cancels the outer scope for the body too, so let it wait shielded.
            with porto.shield():
                outer.cancel()
                await porto.sleep(0.005)
                assert log == [], "the shield let the cancel through"
                shield.shield = False
                await porto.sleep(0.005)
            waiters = shield._waiter_count()
        return log, waiters

    log, waiters = porto.run(main)
    assert log == ["woken by the unshield"]
    assert waiters == 0


def test_C8_shielding_from_another_thread_takes_effect() -> None:
    async def main() -> list[str]:
        log: list[str] = []
        async with porto.scope() as outer:
            s = porto.scope()
            async with s:
                done = threading.Event()

                def shield_then_cancel() -> None:
                    s.shield = True
                    outer.cancel()
                    done.set()

                threading.Thread(target=shield_then_cancel).start()
                while not done.is_set():
                    await porto.sleep(0.001)
                await porto.sleep(0.005)
                log.append("shielded from the outer cancel")
        return log

    assert porto.run(main) == ["shielded from the outer cancel"]


def test_the_shield_reads_back_before_and_after_the_entry() -> None:
    async def main() -> None:
        s = porto.scope()
        assert not s.shield
        s.shield = True
        assert s.shield
        async with s:
            assert s.shield
            s.shield = False
            assert not s.shield
        assert porto.shield().shield
        assert porto.scope(shield=True).shield

    porto.run(main)


def test_the_root_scope_cannot_be_shielded() -> None:
    async def main() -> None:
        root = _core._root_scope()
        assert not root.shield
        with pytest.raises(RuntimeError, match=r"porto\.shield"):
            root.shield = True

    porto.run(main)


def test_sync_with_outside_a_runtime_is_refused() -> None:
    with pytest.raises(porto.NoRuntimeError, match=r"porto\.run"), porto.shield():
        pass


def test_a_scope_cannot_be_entered_both_ways() -> None:
    async def main() -> None:
        s = porto.shield()
        with s, pytest.raises(RuntimeError, match="only once"):
            await s.__aenter__()
        t = porto.scope()
        async with t:
            with pytest.raises(RuntimeError, match="only once"), t:
                pass

    porto.run(main)


def test_exit_without_the_matching_enter_is_refused() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(RuntimeError, match="the way it was entered"):
                s.__exit__(None, None, None)
        with pytest.raises(RuntimeError, match="the way it was entered"):
            porto.shield().__exit__(None, None, None)

    porto.run(main)


def test_a_sync_shield_exited_out_of_order_is_refused() -> None:
    async def main() -> None:
        outer = porto.shield()
        inner = porto.shield()
        outer.__enter__()
        inner.__enter__()
        with pytest.raises(RuntimeError, match="innermost first"):
            outer.__exit__(None, None, None)
        assert inner.__exit__(None, None, None) is False
        assert outer.__exit__(None, None, None) is False

    porto.run(main)


def test_a_nan_shield_timeout_is_refused() -> None:
    with pytest.raises(ValueError, match="number of seconds"):
        porto.shield(timeout=math.nan)


def test_a_sync_shield_whose_task_is_closed_at_shutdown_lets_it_go() -> None:
    """A task abandoned at the end of the run inside a ``with porto.shield():`` is closed with
    the runtime: the exit lets ``GeneratorExit`` through."""

    async def stuck() -> None:
        with porto.shield():
            while True:
                with contextlib.suppress(porto.Cancelled):
                    await porto.sleep(3600)

    async def main() -> None:
        _core._root_scope().spawn(stuck)
        await porto.checkpoint()

    with pytest.warns(RuntimeWarning, match="abandoned"):
        porto.run(main, cancel_timeout=0.01)
