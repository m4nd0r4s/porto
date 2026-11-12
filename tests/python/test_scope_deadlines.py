"""Scope deadlines (15-structured-concurrency-and-cancellation.md C3, C9; 14 §2).

A scope with a deadline (``porto.scope(timeout=, deadline=)``, ``porto.timeout()``,
``porto.move_on_after()``) is cancelled once the deadline passed, never before it on
``porto.current_time()``; the exit absorbs that ``Cancelled`` and raises the builtin
``TimeoutError`` instead unless ``raise_on_timeout=False``. Deadlines nest: an outer deadline
reaches the scopes inside it. Setting ``Scope.deadline`` re-arms the timer. These tests run on
the real clock: they assert the deadline never fires early, and allow it to fire late.
"""

import contextlib
import math
import threading
from collections.abc import Callable

import pytest

import porto
from porto import _core

#: How late a deadline may fire on a loaded CI machine before a test calls it a hang.
LATE = 2.0


def test_C3_deadline_raises_TimeoutError() -> None:
    async def main() -> tuple[float, porto.Scope, BaseException]:
        baseline = _core._armed_timers()
        start = porto.current_time()
        s = porto.timeout(0.05)
        with pytest.raises(TimeoutError) as raised:
            async with s:
                await porto.sleep_forever()
        assert _core._armed_timers() == baseline, "the deadline left its timer armed"
        return porto.current_time() - start, s, raised.value

    elapsed, s, error = porto.run(main)
    assert 0.05 <= elapsed < 0.05 + LATE
    assert type(error) is TimeoutError
    assert "move_on_after" in str(error), "the error says what to do"
    assert isinstance(error.__context__, porto.Cancelled)
    assert s.timed_out
    assert s.cancelled_caught
    assert s.cancel_called


def test_C3_move_on_after_absorbs() -> None:
    async def main() -> tuple[float, porto.Scope, list[str]]:
        steps: list[str] = []
        start = porto.current_time()
        async with porto.move_on_after(0.03) as s:
            steps.append("in")
            await porto.sleep(30)
            steps.append("not reached")
        steps.append("after")
        # The task is not cancelled outside the scope.
        await porto.checkpoint()
        return porto.current_time() - start, s, steps

    elapsed, s, steps = porto.run(main)
    assert 0.03 <= elapsed < 0.03 + LATE
    assert steps == ["in", "after"]
    assert s.timed_out
    assert s.cancelled_caught


def test_C3_raise_on_timeout_false_is_move_on_after() -> None:
    async def main() -> porto.Scope:
        async with porto.scope(timeout=0.01, raise_on_timeout=False) as s:
            await porto.sleep_forever()
        return s

    assert porto.run(main).timed_out


def test_C3_children_cut_short_by_the_deadline_time_the_scope_out() -> None:
    async def main() -> None:
        async with porto.timeout(0.02) as s:
            # The body finishes at once; the deadline only reaches the child.
            s.spawn(porto.sleep_forever)

    with pytest.raises(TimeoutError):
        porto.run(main)

    async def quiet() -> porto.Scope:
        async with porto.move_on_after(0.02) as s:
            s.spawn(porto.sleep_forever)
        return s

    s = porto.run(quiet)
    assert s.timed_out
    assert s.cancelled_caught


def test_C3_a_block_that_finishes_in_time_does_not_time_out() -> None:
    async def main() -> tuple[porto.Scope, int, int]:
        baseline = _core._armed_timers()
        async with porto.timeout(30) as s:
            armed = _core._armed_timers()
            await porto.sleep(0.001)
        return s, armed - baseline, _core._armed_timers() - baseline

    s, armed, left = porto.run(main)
    assert armed == 1, "the deadline armed a timer"
    assert left == 0, "the exit disarmed it"
    assert not s.timed_out
    assert not s.cancelled_caught
    assert not s.cancel_called


def test_C3_an_explicit_cancel_before_the_deadline_is_not_a_timeout() -> None:
    async def main() -> porto.Scope:
        async with porto.timeout(0.02) as s:
            s.cancel()
            await porto.sleep_forever()
        return s

    s = porto.run(main)
    assert s.cancelled_caught
    assert not s.timed_out


def test_C4_a_body_exception_inside_a_timeout_is_raised_as_is() -> None:
    async def main() -> None:
        async with porto.timeout(30):
            raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        porto.run(main)


def test_C4_a_child_failing_after_the_deadline_raises_the_group_not_a_timeout() -> None:
    async def stubborn() -> None:
        try:
            await porto.sleep_forever()
        finally:
            raise ValueError("cleanup failed")

    async def main() -> None:
        async with porto.timeout(0.01) as s:
            s.spawn(stubborn)

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert raised.group_contains(ValueError, match="cleanup failed")


def test_a_timeout_of_zero_or_less_expires_at_the_first_checkpoint() -> None:
    async def main(seconds: float) -> tuple[porto.Scope, list[str]]:
        steps: list[str] = []
        async with porto.move_on_after(seconds) as s:
            steps.append("before the checkpoint")
            await porto.checkpoint()
            steps.append("not reached")
        return s, steps

    for seconds in (0, -1, -math.inf):
        s, steps = porto.run(main, seconds)
        assert steps == ["before the checkpoint"]
        assert s.timed_out


def test_an_infinite_timeout_arms_nothing() -> None:
    async def main() -> tuple[int, float]:
        baseline = _core._armed_timers()
        async with porto.timeout(math.inf) as s:
            return _core._armed_timers() - baseline, s.deadline

    assert porto.run(main) == (0, math.inf)


def test_C9_deadlines_nest() -> None:
    """An outer deadline reaches a scope nested in it, whose own deadline is later: the outer
    scope times out, and the inner one is not the one that caught it. A shorter inner deadline
    times only the inner scope out."""

    async def outer_first() -> tuple[float, porto.Scope, porto.Scope]:
        start = porto.current_time()
        outer = porto.timeout(0.04)
        inner = porto.move_on_after(30)

        async def nested() -> None:
            async with outer, inner:
                await porto.sleep_forever()

        with pytest.raises(TimeoutError):
            await nested()
        return porto.current_time() - start, outer, inner

    elapsed, outer, inner = porto.run(outer_first)
    assert 0.04 <= elapsed < 0.04 + LATE
    assert outer.timed_out
    assert not inner.timed_out
    assert not inner.cancelled_caught

    async def inner_first() -> tuple[float, porto.Scope, porto.Scope]:
        start = porto.current_time()
        async with porto.timeout(30) as outer:
            async with porto.move_on_after(0.02) as inner:
                await porto.sleep_forever()
            # The outer scope is not cancelled: this sleep completes.
            await porto.sleep(0.001)
        return porto.current_time() - start, outer, inner

    elapsed, outer, inner = porto.run(inner_first)
    assert 0.02 <= elapsed < 0.02 + LATE
    assert inner.timed_out
    assert not outer.timed_out
    assert not outer.cancel_called


def test_C9_an_outer_deadline_reaches_tasks_spawned_into_inner_scopes() -> None:
    async def main() -> list[str]:
        ended: list[str] = []

        async def child() -> None:
            try:
                await porto.sleep_forever()
            finally:
                ended.append("child")

        async with porto.move_on_after(0.02), porto.scope() as inner:
            inner.spawn(child)
        return ended

    assert porto.run(main) == ["child"]


def test_C9_set_deadline_rearms() -> None:
    async def sooner() -> tuple[float, porto.Scope]:
        start = porto.current_time()
        async with porto.move_on_after(30) as s:
            s.deadline = porto.current_time() + 0.03
            await porto.sleep_forever()
        return porto.current_time() - start, s

    elapsed, s = porto.run(sooner)
    assert 0.03 <= elapsed < 0.03 + LATE
    assert s.timed_out

    async def later() -> tuple[float, porto.Scope]:
        start = porto.current_time()
        async with porto.move_on_after(0.01) as s:
            s.deadline = start + 0.06
            await porto.sleep_forever()
        return porto.current_time() - start, s

    elapsed, s = porto.run(later)
    assert 0.06 <= elapsed < 0.06 + LATE, "the first deadline still fired"
    assert s.timed_out

    async def removed() -> tuple[float, porto.Scope]:
        start = porto.current_time()
        async with porto.move_on_after(0.01) as s:
            s.deadline = math.inf
            await porto.sleep(0.05)
        return porto.current_time() - start, s

    elapsed, s = porto.run(removed)
    assert elapsed >= 0.05
    assert not s.timed_out
    assert s.deadline == math.inf

    async def passed() -> None:
        async with porto.timeout(30) as s:
            s.deadline = porto.current_time() - 1
            await porto.checkpoint()

    with pytest.raises(TimeoutError):
        porto.run(passed)


def test_C9_a_deadline_set_after_it_fired_does_not_uncancel_the_scope() -> None:
    async def main() -> tuple[porto.Scope, list[str]]:
        caught: list[str] = []
        async with porto.move_on_after(0.01) as s:
            try:
                await porto.sleep_forever()
            except porto.Cancelled:
                caught.append("first")
            s.deadline = math.inf
            try:
                await porto.checkpoint()
            except porto.Cancelled:
                caught.append("still cancelled")
                raise
        return s, caught

    s, caught = porto.run(main)
    assert caught == ["first", "still cancelled"]
    assert s.timed_out


def test_C9_a_deadline_set_from_another_thread_wakes_the_parked_lane() -> None:
    async def main() -> tuple[float, porto.Scope]:
        start = porto.current_time()
        async with porto.move_on_after(30) as s:
            deadline = porto.current_time() + 0.03

            def move() -> None:
                s.deadline = deadline

            threading.Timer(0.01, move).start()
            await porto.sleep_forever()
        return porto.current_time() - start, s

    elapsed, s = porto.run(main)
    assert 0.03 <= elapsed < 0.03 + LATE
    assert s.timed_out


def test_the_deadline_reads_back_on_the_runtime_clock() -> None:
    async def main() -> None:
        assert porto.scope().deadline == math.inf
        assert porto.scope(deadline=12.5).deadline == 12.5
        # A timeout counts from the entry.
        pending = porto.timeout(5)
        assert pending.deadline == math.inf
        before = porto.current_time()
        async with pending as s:
            assert before + 5 <= s.deadline <= porto.current_time() + 5 + 1e-6
        # With both, the earlier wins.
        async with porto.scope(timeout=30, deadline=porto.current_time() + 10) as both:
            assert both.deadline < porto.current_time() + 11
        async with porto.scope(timeout=1, deadline=porto.current_time() + 100) as both:
            assert both.deadline < porto.current_time() + 2
        # Set before the entry: it replaces timeout= too.
        early = porto.timeout(0.001)
        early.deadline = math.inf
        async with early:
            await porto.sleep(0.01)
        assert not early.timed_out

    porto.run(main)


@pytest.mark.parametrize(
    "make",
    [
        lambda: porto.timeout(math.nan),
        lambda: porto.move_on_after(math.nan),
        lambda: porto.scope(timeout=math.nan),
        lambda: porto.scope(deadline=math.nan),
    ],
    ids=["timeout", "move_on_after", "scope(timeout=)", "scope(deadline=)"],
)
def test_a_nan_deadline_is_refused_with_what_to_pass(make: Callable[[], porto.Scope]) -> None:
    with pytest.raises(ValueError, match="number of seconds"):
        make()


def test_setting_a_nan_deadline_is_refused() -> None:
    s = porto.scope()
    with pytest.raises(ValueError, match="number of seconds"):
        s.deadline = math.nan
    assert s.deadline == math.inf


def test_the_root_scope_has_no_deadline() -> None:
    async def main() -> None:
        root = _core._root_scope()
        assert root.deadline == math.inf
        with pytest.raises(RuntimeError, match=r"porto\.timeout"):
            root.deadline = porto.current_time() + 1

    porto.run(main)


def test_a_scope_whose_owner_never_exits_it_is_freed_with_the_runtime() -> None:
    """The deadline's timer holds the scope while it is armed; a task abandoned at the end of
    the run, inside a scope it never exited, drops it with the runtime's wheel."""

    async def stuck() -> None:
        async with porto.timeout(3600):
            while True:
                # Swallows every Cancelled (a bug this test stands in for), so the run
                # abandons it.
                with contextlib.suppress(porto.Cancelled):
                    await porto.sleep_forever()

    async def main() -> None:
        _core._root_scope().spawn(stuck)
        await porto.checkpoint()

    with pytest.warns(RuntimeWarning, match="abandoned"):
        porto.run(main, cancel_timeout=0.01)
