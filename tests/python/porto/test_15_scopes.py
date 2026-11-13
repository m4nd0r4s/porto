"""The semantic suite of 15-structured-concurrency-and-cancellation.md (an M1 exit criterion).

One canonical test per rule: C1-C12 of §2, SP1-SP4 (the ``start`` protocol, §3), K1-K3 (task
cancellation, §4) and X1-X5 (the exceptions table, §6); the IDs are in §7, and a test claims its
rule in its name (``test_C3_...``, 52 §3), which ``scripts/rule_matrix.py --check 15`` holds CI
to. Each canonical test is the rule in its simplest form. ``RULE_INDEX`` below names the deeper
tests of every rule that live in the other ``tests/python`` files (edge cases, threads, timing),
and ``test_the_rule_index_names_tests_that_exist`` keeps the table honest.

The second half ports the relevant subset of Trio's tests of cancel scopes, nurseries and
timeouts to the porto API. Where porto deliberately differs the port is an ``xfail`` that cites
the spec; the test is kept so the difference stays visible and is noticed if it ever closes.

Adapted from the Trio test suite, trio/_core/_tests/test_run.py and trio/_tests/test_timeouts.py
(https://github.com/python-trio/trio), which Trio's contributors make available under either the
MIT or the Apache-2.0 license; used here under the MIT license:

    Copyright Contributors to the Trio project.

    Permission is hereby granted, free of charge, to any person obtaining a copy of this software
    and associated documentation files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions: The above copyright
    notice and this permission notice shall be included in all copies or substantial portions of
    the Software. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
    PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
    OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
    IN THE SOFTWARE.
"""

import ast
import asyncio
import contextlib
import gc
import math
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import porto
from porto import _core

TESTS = Path(__file__).resolve().parent.parent

#: How late a timer may fire on a loaded CI machine before a test calls it a hang.
LATE = 5.0

#: Float rounding between two reads of one clock (not scheduling slack).
CLOCK_SLACK = 0.001

#: The deeper tests of each rule, in the other files of tests/python: "file::test" (a prefix).
RULE_INDEX: dict[str, tuple[str, ...]] = {
    "C1": ("test_cancellation.py::test_C1_level_triggered",),
    "C2": ("test_cancellation.py::test_C2_only_at_checkpoints",),
    "C3": ("test_cancellation.py::test_C3_an_outer_cancel_goes_through_an_inner_scope",),
    "C4": ("test_cancellation.py::test_C4_a_failing_child_stops_a_sleeping_sibling_promptly",),
    "C5": ("test_scope_tree.py::test_C5_the_exit_waits_in_a_scope_exit_trap",),
    "C6": ("test_cancellation.py::test_C6_a_child_spawned_into_a_cancelled_scope",),
    "C7": ("test_api.py::test_spawn_after_the_scope_closed_raises",),
    "C8": ("test_scope_shields.py::test_C8_shield_blocks_outer_cancel",),
    "C9": ("test_scope_deadlines.py::test_C9_deadlines_nest",),
    "C10": ("test_cancellation.py::test_C10_cancel_from_foreign_thread",),
    "C11": ("test_cancelled_c11.py::test_C11_debug_warns_once_when_a_task_swallows",),
    "C12": ("test_root_scope.py::test_C12_background_failure_cancels_main",),
    "SP1": ("test_scope_start.py::test_start_returns_the_value_the_task_set",),
    "SP2": ("test_scope_start.py::test_cancelling_the_caller_before_set_cancels_the_task",),
    "SP3": ("test_scope_start.py::test_set_by_another_task_raises_runtime_error",),
    "SP4": ("test_scope_start.py::test_start_raises_when_the_task_finishes_without_calling_set",),
    "K1": ("test_task_cancel.py::test_cancelling_one_child_leaves_its_siblings_running",),
    "K2": ("test_task_cancel.py::test_a_shield_the_task_entered_holds_the_cancel_off",),
    "K3": ("test_task_cancel.py::test_the_cancelled_task_is_done_cancelled",),
    "X1": ("test_scope_deadlines.py::test_C9_deadlines_nest",),
    "X2": ("test_cancellation.py::test_C4_a_failing_child_stops_a_sleeping_sibling_promptly",),
    "X3": ("test_api.py::test_spawn_after_the_scope_closed_raises",),
    "X4": ("test_cancelled_c11.py::test_C11_every_porto_awaitable_raises_no_runtime_error",),
    "X5": ("test_scope_start.py::test_calling_set_twice_raises_runtime_error",),
}


def _functions(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def test_the_rule_index_names_tests_that_exist() -> None:
    missing: list[str] = []
    for tests in RULE_INDEX.values():
        for entry in tests:
            file, prefix = entry.split("::")
            path = TESTS / file
            if not path.is_file() or not any(f.startswith(prefix) for f in _functions(path)):
                missing.append(entry)
    assert not missing, f"RULE_INDEX names tests that are gone, update it: {missing}"


async def linger() -> None:
    await porto.sleep(30)


async def forever() -> None:
    await porto.sleep_forever()


async def fail_with(error: BaseException) -> None:
    await porto.checkpoint()
    raise error


class Fatal(BaseException):
    """A BaseException that is not an Exception, for the BaseExceptionGroup case."""


# --- §2 the rules ---------------------------------------------------------------------------


def test_C1_level_triggered() -> None:
    async def main() -> list[str]:
        seen: list[str] = []
        async with porto.scope() as s:
            s.cancel()
            for _ in range(3):
                try:
                    await porto.checkpoint()
                except porto.Cancelled:
                    seen.append("raised")
            with contextlib.suppress(porto.Cancelled):
                await porto.sleep(30)
                seen.append("sleep did not raise")
        return seen

    assert porto.run(main) == ["raised", "raised", "raised"]


def test_C2_delivered_only_at_checkpoints() -> None:
    async def main() -> list[str]:
        log: list[str] = []
        async with porto.scope() as s:
            s.cancel()
            log.append(f"ran {sum(range(1000))}")  # pure Python: never interrupted
            try:
                await porto.checkpoint()
            except porto.Cancelled:
                log.append("delivered at the await")
                raise
        return log

    assert porto.run(main) == ["ran 499500", "delivered at the await"]


def test_C3_the_scope_swallows_the_cancelled_that_propagates_out_of_it() -> None:
    async def main() -> tuple[bool, bool, bool]:
        async with porto.scope() as s:
            s.cancel()
            await porto.checkpoint()  # raises Cancelled out of the block
        async with porto.scope() as caught_by_body:
            caught_by_body.cancel()
            with contextlib.suppress(porto.Cancelled):
                await porto.checkpoint()
        async with porto.move_on_after(0.01) as limit:
            await porto.sleep(30)
        return s.cancelled_caught, caught_by_body.cancelled_caught, limit.timed_out

    # A Cancelled the body catches is not absorbed by the scope.
    assert porto.run(main) == (True, False, True)


def test_C3_only_the_children_reached_still_counts_as_caught() -> None:
    """The body finished before the cancel: first answer wins, the losers are cancelled."""

    async def main() -> bool:
        async with porto.scope() as s:
            s.spawn(linger)
            s.spawn(linger)
            await porto.checkpoint()
            s.cancel()
        return s.cancelled_caught

    assert porto.run(main)


def test_C4_a_child_failure_cancels_the_scope_and_raises_a_group() -> None:
    async def main() -> tuple[ExceptionGroup[Any], list[str]]:
        log: list[str] = []

        async def sibling() -> None:
            try:
                await porto.sleep(30)
            except porto.Cancelled:
                log.append("sibling cancelled")
                raise

        try:
            async with porto.scope(name="jobs") as s:
                s.spawn(sibling)
                s.spawn(fail_with, ValueError("boom"))
                await porto.sleep(30)
        except ExceptionGroup as group:
            return group, log
        raise AssertionError("no group")

    group, log = porto.run(main)
    assert str(group).startswith("porto scope 'jobs' failed")
    assert [repr(error) for error in group.exceptions] == ["ValueError('boom')"]
    assert log == ["sibling cancelled"]


def test_C4_the_body_s_exception_comes_first_and_ends_the_wait_for_children() -> None:
    async def main() -> ExceptionGroup[Any]:
        try:
            async with porto.scope() as s:
                s.spawn(linger)  # would never end on its own
                s.spawn(fail_with, KeyError("child"))
                await porto.checkpoint()
                raise ValueError("body")
        except ExceptionGroup as group:
            return group
        raise AssertionError("no group")

    group = porto.run(main)
    assert [type(error) for error in group.exceptions] == [ValueError, KeyError]


def test_C5_the_exit_waits_for_every_child() -> None:
    async def main() -> list[str]:
        log: list[str] = []

        async def child(name: str, delay: float) -> None:
            await porto.sleep(delay)
            log.append(name)

        async with porto.scope() as s:
            s.spawn(child, "slow", 0.03)
            s.spawn(child, "quick", 0.001)
            log.append("body done")
        log.append("after the block")
        return log

    assert porto.run(main) == ["body done", "quick", "slow", "after the block"]


def test_C6_a_child_spawned_into_a_cancelled_scope_runs_to_its_first_checkpoint() -> None:
    async def main() -> list[str]:
        log: list[str] = []

        async def child() -> None:
            log.append("started")
            await porto.checkpoint()
            log.append("not reached")

        async with porto.scope() as s:
            s.cancel()
            s.spawn(child)
        return log

    assert porto.run(main) == ["started"]


def test_C7_spawning_into_a_closed_scope_raises() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            pass
        with pytest.raises(RuntimeError, match="closed"):
            s.spawn(porto.checkpoint)

    porto.run(main)


def test_C8_a_shield_keeps_an_outer_cancel_out_and_sync_with_cannot_spawn() -> None:
    async def main() -> list[str]:
        log: list[str] = []
        async with porto.scope() as outer:
            outer.cancel()
            with porto.shield() as shield:
                await porto.sleep(0.001)
                log.append("cleanup ran")
                with pytest.raises(RuntimeError, match="cannot spawn"):
                    shield.spawn(porto.checkpoint)
            await porto.checkpoint()  # the outer cancel is delivered at the first await after
            log.append("not reached")
        assert outer.cancelled_caught
        return log

    assert porto.run(main) == ["cleanup ran"]


def test_C8_shield_is_read_write_and_the_root_cannot_be_shielded() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            assert not s.shield
            s.shield = True
            assert s.shield
        with pytest.raises(RuntimeError, match="root scope"):
            _core._root_scope().shield = True  # pyright: ignore[reportPrivateUsage]

    porto.run(main)


def test_C9_the_earlier_deadline_wins_and_a_later_one_re_arms() -> None:
    async def main() -> tuple[float, bool]:
        # The runtime's own clock, the one the deadlines are on: ``time.monotonic()`` ticks in
        # 15.6 ms steps on Windows, which read a 20 ms deadline as 14.99 ms (porto-41w).
        started = porto.current_time()
        with pytest.raises(TimeoutError):
            async with porto.timeout(0.02), porto.timeout(30):
                await porto.sleep(30)  # the outer deadline reaches the inner scope
        elapsed = porto.current_time() - started
        async with porto.move_on_after(30) as s:
            s.deadline = porto.current_time() + 0.01
            await porto.sleep(30)
        return elapsed, s.timed_out

    elapsed, timed_out = porto.run(main)
    assert 0.015 - CLOCK_SLACK <= elapsed < LATE
    assert timed_out


def test_C10_cancel_from_another_thread_wakes_a_sleeping_task() -> None:
    async def main() -> float:
        async with porto.scope() as s:
            thread = threading.Thread(target=lambda: (time.sleep(0.02), s.cancel()))
            started = time.monotonic()
            thread.start()
            await porto.sleep(30)
        thread.join()
        return time.monotonic() - started

    assert porto.run(main) < LATE


def test_C11_cancelled_is_an_asyncio_cancelled_error_and_a_foreign_one_is_a_failure() -> None:
    assert issubclass(porto.Cancelled, asyncio.CancelledError)

    async def main() -> BaseExceptionGroup[Any]:
        try:
            async with porto.scope() as s:
                s.spawn(fail_with, asyncio.CancelledError())
                await porto.sleep(30)
        except BaseExceptionGroup as group:
            return group
        raise AssertionError("absorbed")

    (error,) = porto.run(main).exceptions
    assert not isinstance(error, porto.Cancelled)


def test_C12_main_s_exception_is_raised_and_a_background_failure_cancels_main() -> None:
    async def failing_main() -> None:
        raise ValueError("main")

    with pytest.raises(ValueError, match="main"):
        porto.run(failing_main)

    async def main() -> None:
        _core._root_scope().spawn(fail_with, KeyError("background"))  # pyright: ignore[reportPrivateUsage]
        await porto.sleep(30)

    with pytest.raises(ExceptionGroup, match="porto root scope failed"):
        porto.run(main)


# --- §3 the start protocol ----------------------------------------------------------------------


def test_SP1_start_injects_started_and_returns_the_value_set() -> None:
    async def server(port: int, *, started: "porto.Started[int]") -> None:
        started.set(port + 1)
        await porto.sleep(0.001)

    async def main() -> int:
        async with porto.scope() as s:
            return await s.start(server, 41)

    assert porto.run(main) == 42


def test_SP1_start_takes_no_keyword_but_name() -> None:
    async def server(*, started: "porto.Started[None]") -> None:
        started.set()

    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(TypeError, match=r"functools\.partial"):
                await s.start(server, port=1)  # type: ignore[call-arg]
            await s.start(server, name="named")

    porto.run(main)


def test_SP2_an_exception_before_set_comes_out_of_start_bare() -> None:
    async def raises(*, started: "porto.Started[None]") -> None:
        raise KeyError("early")

    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(KeyError, match="early"):
                await s.start(raises)

    porto.run(main)


def test_SP2_cancelling_the_caller_of_start_cancels_the_task() -> None:
    log: list[str] = []

    async def blocks(*, started: "porto.Started[None]") -> None:
        try:
            await porto.sleep(30)
        except porto.Cancelled:
            log.append("task cancelled")
            raise

    async def main() -> None:
        async with porto.scope() as s, porto.move_on_after(0.01):
            await s.start(blocks)

    porto.run(main)
    assert log == ["task cancelled"]


def test_SP3_set_moves_the_task_into_the_scope_and_raises_when_it_cannot() -> None:
    async def twice(*, started: "porto.Started[None]") -> None:
        started.set()
        with pytest.raises(RuntimeError, match="twice"):
            started.set()
        await porto.sleep(30)

    async def main() -> None:
        async with porto.scope() as s:
            await s.start(twice)
            assert len(s.tasks) == 1  # it counts in s from now on
            s.cancel()

    porto.run(main)


def test_SP4_a_task_that_ends_without_set_makes_start_raise() -> None:
    async def forgets(*, started: "porto.Started[None]") -> None:
        return None

    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(RuntimeError, match="without calling"):
                await s.start(forgets)

    porto.run(main)


# --- §4 task cancellation ------------------------------------------------------------------------


def test_K1_task_cancel_cancels_that_task_only_and_is_not_a_failure() -> None:
    async def main() -> tuple[bool, bool, bool]:
        async with porto.scope() as s:
            victim = s.spawn(linger)
            sibling = s.spawn(porto.sleep, 0.02)
            await porto.checkpoint()
            victim.cancel()
        return victim.cancelled(), sibling.cancelled(), s.cancel_called

    assert porto.run(main) == (True, False, False)


def test_K2_task_cancel_is_held_off_by_a_shield_and_idempotent() -> None:
    async def main() -> list[str]:
        log: list[str] = []

        async def stubborn() -> None:
            with porto.shield():
                await porto.sleep(0.01)
                log.append("shielded work done")
            await porto.checkpoint()

        async with porto.scope() as s:
            task = s.spawn(stubborn)
            await porto.checkpoint()
            task.cancel()
            task.cancel()
        task.cancel()  # finished: nothing
        assert task.cancelled()
        return log

    assert porto.run(main) == ["shielded work done"]


def test_K3_reading_a_cancelled_task_raises_a_fresh_cancelled_with_the_cause() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(linger)
            await porto.checkpoint()
            task.cancel()
        with pytest.raises(porto.Cancelled) as first:
            task.result()
        with pytest.raises(porto.Cancelled) as second:
            task.result()
        assert first.value is not second.value
        assert isinstance(first.value.__cause__, porto.Cancelled)

    porto.run(main)


# --- §6 the exceptions table ----------------------------------------------------------------------


def test_X1_a_deadline_raises_timeout_error_unless_raise_on_timeout_is_false() -> None:
    async def main() -> bool:
        with pytest.raises(TimeoutError):
            async with porto.scope(timeout=0.01):
                await porto.sleep(30)
        async with porto.scope(timeout=0.01, raise_on_timeout=False) as quiet:
            await porto.sleep(30)
        return quiet.timed_out

    assert porto.run(main)


def test_X2_child_failures_raise_exception_group_or_base_exception_group() -> None:
    async def main() -> list[type]:
        kinds: list[type] = []
        for error in (ValueError("x"), Fatal("y")):
            try:
                async with porto.scope() as s:
                    s.spawn(fail_with, error)
            except BaseExceptionGroup as group:
                kinds.append(type(group))
        return kinds

    assert porto.run(main) == [ExceptionGroup, BaseExceptionGroup]


def test_X3_spawn_after_close_raises_runtime_error() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            pass
        with pytest.raises(RuntimeError):
            s.spawn(porto.checkpoint)

    porto.run(main)


def test_X4_awaiting_outside_a_runtime_raises_no_runtime_error() -> None:
    async def go() -> None:
        await porto.sleep(0)

    with pytest.raises(porto.NoRuntimeError):
        asyncio.run(go())
    assert issubclass(porto.NoRuntimeError, RuntimeError)


def test_X5_started_set_twice_raises_runtime_error() -> None:
    async def twice(*, started: "porto.Started[None]") -> None:
        started.set()
        started.set()

    async def main() -> None:
        async with porto.scope() as s:
            await s.start(twice)

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert any(isinstance(error, RuntimeError) for error in raised.value.exceptions)


# ============================================================
# Ported from Trio (see the header). `nursery` is a porto.scope() entered with async with,
# a Trio CancelScope is a porto.scope() entered with sync with (a cancel scope only), `start_soon`
# is `spawn` and `TaskStatus.started` is `Started.set`. Trio's mock clock has no porto counterpart
# yet (porto.testing, 24): the timing tests use short real deadlines.
# ============================================================


def test_trio_child_crash_basic() -> None:
    my_exc = ValueError("uh oh")

    async def erroring() -> None:
        raise my_exc

    async def main() -> None:
        with pytest.RaisesGroup(pytest.RaisesExc(check=lambda e: e is my_exc)):
            async with porto.scope() as nursery:
                nursery.spawn(erroring)

    porto.run(main)


@pytest.mark.xfail(
    strict=True,
    reason="15 C4: the body's own exception leaves the scope bare unless a child failed too; "
    "Trio wraps it in an ExceptionGroup",
)
def test_trio_nursery_main_block_error_basic() -> None:
    exc = ValueError("whoops")

    async def main() -> None:
        with pytest.RaisesGroup(pytest.RaisesExc(check=lambda e: e is exc)):
            async with porto.scope():
                raise exc

    porto.run(main)


def test_trio_basic_interleave() -> None:
    record: list[tuple[str, int]] = []

    async def looper(whoami: str) -> None:
        for i in range(3):
            record.append((whoami, i))
            await porto.checkpoint()

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(looper, "a")
            nursery.spawn(looper, "b")

    porto.run(main)
    rounds = [set(record[0:2]), set(record[2:4]), set(record[4:6])]
    assert rounds == [{("a", 0), ("b", 0)}, {("a", 1), ("b", 1)}, {("a", 2), ("b", 2)}]


def test_trio_task_crash_propagation() -> None:
    looper_record: list[str] = []

    async def looper() -> None:
        try:
            while True:
                await porto.checkpoint()
        except porto.Cancelled:
            looper_record.append("cancelled")

    async def crasher() -> None:
        raise ValueError("argh")

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(looper)
            nursery.spawn(crasher)

    with pytest.RaisesGroup(pytest.RaisesExc(ValueError, match="^argh$")):
        porto.run(main)
    assert looper_record == ["cancelled"]


def test_trio_main_and_task_both_crash() -> None:
    async def crasher() -> None:
        raise ValueError

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(crasher)
            raise KeyError

    with pytest.RaisesGroup(ValueError, KeyError):
        porto.run(main)


def test_trio_two_child_crashes() -> None:
    async def crasher(etype: type[Exception]) -> None:
        raise etype

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(crasher, KeyError)
            nursery.spawn(crasher, ValueError)

    with pytest.RaisesGroup(ValueError, KeyError):
        porto.run(main)


def test_trio_child_crash_wakes_parent() -> None:
    async def crasher() -> None:
        raise ValueError("this is a crash")

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(crasher)
            await forever()

    with pytest.RaisesGroup(pytest.RaisesExc(ValueError, match="^this is a crash$")):
        porto.run(main)


def test_trio_cancel_points() -> None:
    async def main() -> None:
        with porto.scope() as scope:
            await porto.checkpoint()
            scope.cancel()
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()
        with porto.scope() as scope:
            scope.cancel()
            with pytest.raises(porto.Cancelled):
                await forever()
        with porto.scope() as scope:
            scope.cancel()
            with porto.shield():  # Trio's cancel_shielded_checkpoint
                await porto.checkpoint()
                await porto.checkpoint()
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()

    porto.run(main)


def test_trio_cancel_edge_cases() -> None:
    async def main() -> None:
        with porto.scope() as scope:
            scope.cancel()
            scope.cancel()  # idempotent
            await porto.checkpoint()
        assert scope.cancel_called
        assert scope.cancelled_caught

        with porto.scope() as scope:
            scope.cancel()  # level-triggered
            with pytest.raises(porto.Cancelled):
                await forever()
            with pytest.raises(porto.Cancelled):
                await forever()

    porto.run(main)


def test_trio_precancelled_task() -> None:
    record: list[str] = []

    async def blocker() -> None:
        record.append("started")
        await forever()

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.cancel()
            nursery.spawn(blocker)

    porto.run(main)
    assert record == ["started"]


def test_trio_cancel_shielding() -> None:
    async def main() -> None:
        with porto.scope() as outer, porto.scope() as inner:
            await porto.checkpoint()
            outer.cancel()
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()

            assert inner.shield is False
            with pytest.raises(TypeError):
                inner.shield = "hello"  # type: ignore[assignment]
            assert inner.shield is False

            inner.shield = True
            assert inner.shield is True
            await porto.checkpoint()  # the shield protects us from `outer`

            with porto.scope() as innerest:
                innerest.cancel()
                with pytest.raises(porto.Cancelled):  # but not from a scope inside inner
                    await porto.checkpoint()
            await porto.checkpoint()

            inner.shield = False
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()

            inner.shield = True
            await porto.checkpoint()
            inner.cancel()  # the shield does not protect from inner itself
            await porto.checkpoint()  # raises, absorbed by inner
        assert inner.cancelled_caught

    porto.run(main)


def test_trio_cancel_inheritance() -> None:
    """A cancel reaches every task of the scope and of the scopes nested in it."""
    record: set[str] = set()

    async def leaf(ident: str) -> None:
        try:
            await forever()
        except porto.Cancelled:
            record.add(ident)
            raise

    async def worker(ident: str) -> None:
        async with porto.scope() as nursery:
            nursery.spawn(leaf, ident + "-l1")
            nursery.spawn(leaf, ident + "-l2")

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(worker, "w1")
            nursery.spawn(worker, "w2")
            for _ in range(5):  # Trio's cancel-at-once reads differently: see C6 and C1
                await porto.checkpoint()
            nursery.cancel()

    porto.run(main)
    assert record == {"w1-l1", "w1-l2", "w2-l1", "w2-l2"}


def test_trio_cancel_shield_abort() -> None:
    """Unshielding wakes a task an outer cancel now reaches (C8, C10)."""

    async def main() -> None:
        record: list[str] = []
        with porto.scope() as outer:
            async with porto.scope() as nursery:
                outer.cancel()
                nursery.shield = True

                async def sleeper() -> None:
                    record.append("sleeping")
                    try:
                        await forever()
                    except porto.Cancelled:
                        record.append("cancelled")

                nursery.spawn(sleeper)
                for _ in range(5):
                    await porto.checkpoint()
                assert record == ["sleeping"]
                nursery.shield = False
                with porto.shield():
                    for _ in range(5):
                        await porto.checkpoint()
                    assert record == ["sleeping", "cancelled"]

    porto.run(main)


def test_trio_basic_timeout() -> None:
    async def main() -> None:
        start = porto.current_time()
        with porto.scope() as scope:
            assert scope.deadline == math.inf
            scope.deadline = start + 1
            assert scope.deadline == start + 1
        assert not scope.cancel_called
        await porto.checkpoint()
        assert not scope.cancel_called

        with porto.scope(deadline=porto.current_time() + 0.01, raise_on_timeout=False) as scope:
            await forever()
        assert scope.cancel_called
        assert scope.cancelled_caught

        start = porto.current_time()
        with porto.scope() as scope:
            await porto.checkpoint()
            scope.deadline = start + 30
            await porto.checkpoint()
            scope.deadline = start - 1  # moved into the past: cancelled at once
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()

    porto.run(main)


@pytest.mark.xfail(
    strict=True,
    reason="15 C3 and decision porto-vw8.17: the innermost cancelled scope absorbs the "
    "Cancelled, Trio's outermost one does",
)
def test_trio_cancel_scope_nesting_outer_wins() -> None:
    async def main() -> None:
        with porto.scope() as scope1, porto.scope() as scope2, porto.scope() as scope3:
            scope3.cancel()
            scope2.cancel()
            await forever()
        assert scope3.cancel_called
        assert not scope3.cancelled_caught
        assert scope2.cancel_called
        assert scope2.cancelled_caught
        assert not scope1.cancel_called

    porto.run(main)


def test_trio_cancel_scope_nesting_innermost_absorbs() -> None:
    """The porto reading of the scene above (C3): the first cancelled scope the walk reaches."""

    async def main() -> None:
        with porto.scope() as scope1, porto.scope() as scope2, porto.scope() as scope3:
            scope3.cancel()
            scope2.cancel()
            await forever()
        assert scope3.cancelled_caught
        assert not scope2.cancelled_caught
        assert not scope1.cancel_called

    porto.run(main)


def test_trio_cancel_scope_nesting_shielding_and_popped_scopes() -> None:
    async def main() -> None:
        with porto.scope() as scope1, porto.scope() as scope2:
            scope1.cancel()
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()
            scope2.shield = True
            await porto.checkpoint()
            scope2.cancel()
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()

        # a cancel pending on a scope that gets popped off the stack is never delivered
        with porto.scope() as scope:
            scope.cancel()
            with porto.shield():
                await porto.checkpoint()
        await porto.checkpoint()
        assert not scope.cancelled_caught

    porto.run(main)


def test_trio_unshield_while_cancel_propagating() -> None:
    async def main() -> None:
        with porto.scope() as outer, porto.scope() as inner:
            outer.cancel()
            try:
                await porto.checkpoint()
            finally:
                inner.shield = True
        assert outer.cancelled_caught
        assert not inner.cancelled_caught

    porto.run(main)


def test_trio_cancel_unbound() -> None:
    async def main() -> None:
        # cancel before entry
        scope = porto.scope()
        scope.cancel()
        with scope, pytest.raises(porto.Cancelled):
            await porto.checkpoint()

        # shield before entry
        scope = porto.scope()
        scope.shield = True
        with porto.scope() as outer, scope:
            outer.cancel()
            await porto.checkpoint()
            scope.shield = False
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()

        # cannot be reused or re-entered
        with porto.scope() as scope:
            await porto.checkpoint()
        scope.cancel()
        await porto.checkpoint()
        assert scope.cancel_called
        assert not scope.cancelled_caught
        with pytest.raises(RuntimeError, match="only once"), scope:
            pass
        with porto.scope() as scope, pytest.raises(RuntimeError, match="only once"), scope:
            pass

    porto.run(main)


@pytest.mark.xfail(
    strict=True,
    reason="15 C9: the timer is armed when the block is entered, so cancel_called of a scope not "
    "entered yet reads cancel() only; Trio counts a passed deadline at once",
)
def test_trio_unentered_scope_with_a_passed_deadline_is_cancel_called() -> None:
    async def main() -> None:
        scope = porto.scope(deadline=porto.current_time() + 1)
        assert not scope.cancel_called
        scope.deadline -= 2
        assert scope.cancel_called
        scope.deadline += 2
        assert scope.cancel_called

    porto.run(main)


def test_trio_nursery_closure() -> None:
    async def child1(nursery: porto.Scope) -> None:
        nursery.spawn(child2)  # still allowed while the exit waits for tasks (C5)

    async def child2() -> None:
        pass

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(child1, nursery)
        with pytest.raises(RuntimeError):
            nursery.spawn(child2)

    porto.run(main)


def test_trio_spawn_name() -> None:
    names: list[str] = []

    async def func1() -> None:
        names.append(porto.current_task().name)

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(func1)
            nursery.spawn(func1, name="func3")

    porto.run(main)
    assert any("func1" in name for name in names)
    assert "func3" in names


def test_trio_nursery_start() -> None:
    async def no_args() -> None:  # pragma: no cover
        pass

    async def sleep_then_start(seconds: float, *, started: "porto.Started[float]") -> None:
        await porto.sleep(seconds)
        started.set(seconds)
        await porto.sleep(seconds)

    async def double_started(*, started: "porto.Started[None]") -> None:
        started.set()
        with pytest.raises(RuntimeError):
            started.set()

    async def raise_keyerror(*, started: "porto.Started[None]") -> None:
        raise KeyError("oops")

    async def nothing(*, started: "porto.Started[None]") -> None:
        return None

    async def main() -> None:
        # errors in the calling convention come out of start
        async with porto.scope() as nursery:
            with pytest.raises(TypeError):
                await nursery.start(no_args)

        # happy path: start waits for started.set, returns the value, the scope waits for the task
        for seconds in (0.01, 0.02):
            async with porto.scope() as nursery:
                assert len(nursery.tasks) == 0
                t0 = porto.monotonic()
                assert await nursery.start(sleep_then_start, seconds) == seconds
                assert porto.monotonic() - t0 >= seconds * 0.9
                assert len(nursery.tasks) == 1
            assert porto.monotonic() - t0 >= 2 * seconds * 0.9

        async with porto.scope() as nursery:
            await nursery.start(double_started)

        async with porto.scope() as nursery:
            with pytest.raises(KeyError):
                await nursery.start(raise_keyerror)

        async with porto.scope() as nursery:
            with pytest.raises(RuntimeError, match="without calling"):
                await nursery.start(nothing)

        async with porto.scope() as closed_nursery:
            pass
        with pytest.raises(RuntimeError):
            await closed_nursery.start(sleep_then_start, 7)

    porto.run(main)


def test_trio_nursery_start_in_a_cancelled_scope() -> None:
    async def just_started(*, started: "porto.Started[str]") -> None:
        started.set("hi")
        await porto.checkpoint()

    async def main() -> None:
        # the call to start is cancelled: it raises Cancelled
        async with porto.scope() as nursery:
            with porto.scope() as cs:
                cs.cancel()
                with pytest.raises(porto.Cancelled):
                    await nursery.start(just_started)

    porto.run(main)


@pytest.mark.xfail(
    strict=True,
    reason="15 §3 and C1: start() enters its hidden scope, an ordinary checkpoint of the "
    "cancelled scope around it, so it raises Cancelled before the task runs; Trio runs the task "
    "and lets one that does not checkpoint finish successfully",
)
def test_trio_nursery_start_with_no_checkpoint_in_a_cancelled_scope() -> None:
    async def started_with_no_checkpoint(*, started: "porto.Started[None]") -> None:
        started.set(None)

    async def main() -> None:
        async with porto.scope() as nursery:
            with porto.scope() as cs:
                cs.cancel()
                await nursery.start(started_with_no_checkpoint)
            assert not cs.cancelled_caught

    porto.run(main)


def test_trio_cancel_scope_deadline_duplicates() -> None:
    async def main() -> None:
        deadline = porto.current_time() + 0.02
        scopes = [porto.scope(deadline=deadline, raise_on_timeout=False) for _ in range(5)]

        async def sleeper(scope: porto.Scope) -> None:
            with scope:
                await forever()

        async with porto.scope() as nursery:
            for scope in scopes:
                nursery.spawn(sleeper, scope)
        assert all(scope.cancelled_caught and scope.timed_out for scope in scopes)

    porto.run(main)


def test_trio_move_on_after_and_timeout() -> None:
    async def main() -> None:
        async with porto.move_on_after(0.01) as scope:
            await forever()
        assert scope.cancelled_caught
        assert scope.timed_out

        async with porto.move_on_after(30) as scope:
            await porto.checkpoint()
        assert not scope.cancel_called

        with pytest.raises(TimeoutError):
            async with porto.timeout(0.01):
                await forever()
        async with porto.timeout(30):
            await porto.checkpoint()

        # a negative timeout expires at once; NaN is refused
        with pytest.raises(TimeoutError):
            async with porto.timeout(-1):
                await porto.checkpoint()
        with pytest.raises(ValueError, match=r"(?i)nan"):
            porto.timeout(math.nan)

    porto.run(main)


def test_trio_yield_briefly_checks_for_timeout() -> None:
    async def main() -> None:
        async with porto.move_on_after(0.01):
            time.sleep(0.03)  # blocks the lane past the deadline, as Trio's mock clock jump does
            await porto.sleep(0.02)
            with pytest.raises(porto.Cancelled):
                await porto.checkpoint()

    porto.run(main)


def test_trio_nursery_cancel_does_not_create_cyclic_garbage() -> None:
    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(forever)
            await porto.checkpoint()
            nursery.cancel()

    porto.run(main)
    gc.collect()
    gc.disable()
    try:
        for _ in range(3):
            porto.run(main)
        assert gc.collect() == 0
    finally:
        gc.enable()


def test_trio_locals_destroyed_promptly_on_cancel() -> None:
    destroyed: list[bool] = []

    class Marker:
        def __del__(self) -> None:
            destroyed.append(True)

    async def child() -> None:
        marker = Marker()
        try:
            await forever()
        finally:
            del marker

    async def main() -> None:
        async with porto.scope() as nursery:
            nursery.spawn(child)
            await porto.checkpoint()
            nursery.cancel()
        assert destroyed == [True]

    porto.run(main)
