"""``porto.run``'s root scope is a nursery (spec 15 C12, spec 26 §2).

``main`` is the root scope's first task. A task spawned into the root scope (the private
``_root_scope()``, the future ``Runtime.root_scope``) that fails cancels it, and so ``main``.
Once ``main`` ended, the tasks left in the root scope are cancelled and waited for, up to
``cancel_timeout``. ``porto.run`` raises main's exception as is, or a strict group of main's
exception and the root tasks' failures.
"""

import time
from typing import Any

import pytest

import porto
from porto import _core


async def fail_with(error: BaseException) -> None:
    await porto.checkpoint()
    raise error


def test_C12_background_failure_cancels_main() -> None:
    events: list[str] = []

    async def main() -> None:
        _core._root_scope().spawn(fail_with, ValueError("background"))
        try:
            await porto.sleep(30)
        except porto.Cancelled:
            events.append("main cancelled")
            raise

    started = time.monotonic()
    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert time.monotonic() - started < 5
    assert events == ["main cancelled"]
    assert [repr(error) for error in raised.value.exceptions] == ["ValueError('background')"]
    assert str(raised.value).startswith("porto root scope failed")


def test_C12_background_failure_reaches_the_tasks_of_main_s_scopes() -> None:
    events: list[str] = []

    async def worker() -> None:
        try:
            await porto.sleep(30)
        except porto.Cancelled:
            events.append("worker cancelled")
            raise

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(worker)
            await porto.checkpoint()
            _core._root_scope().spawn(fail_with, ValueError("background"))
            await porto.sleep(30)

    with pytest.raises(ExceptionGroup):
        porto.run(main)
    assert events == ["worker cancelled"]


def test_C12_main_s_exception_is_raised_as_is_and_cancels_the_root_tasks() -> None:
    events: list[str] = []

    async def background() -> None:
        try:
            await porto.sleep(30)
        except porto.Cancelled:
            events.append("background cancelled")
            raise

    async def main() -> None:
        _core._root_scope().spawn(background)
        await porto.checkpoint()
        raise KeyError("main")

    with pytest.raises(KeyError, match="main"):
        porto.run(main)
    assert events == ["background cancelled"]


def test_C12_main_s_exception_comes_first_when_root_tasks_failed_too() -> None:
    async def background() -> None:
        try:
            await porto.sleep(30)
        finally:
            raise ValueError("background cleanup")

    async def main() -> None:
        _core._root_scope().spawn(background)
        await porto.checkpoint()
        raise KeyError("main")

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert [repr(error) for error in raised.value.exceptions] == [
        "KeyError('main')",
        "ValueError('background cleanup')",
    ]


def test_C12_a_base_exception_in_a_root_task_comes_in_a_base_exception_group() -> None:
    async def main() -> None:
        _core._root_scope().spawn(fail_with, KeyboardInterrupt())
        await porto.sleep(30)

    with pytest.raises(BaseExceptionGroup) as raised:
        porto.run(main)
    assert not isinstance(raised.value, ExceptionGroup)
    assert isinstance(raised.value.exceptions[0], KeyboardInterrupt)


def test_root_tasks_left_when_main_returns_are_cancelled_and_waited_for() -> None:
    events: list[str] = []

    async def background() -> None:
        try:
            await porto.sleep(30)
        except porto.Cancelled:
            events.append("background cancelled")
            raise

    async def main() -> str:
        _core._root_scope().spawn(background)
        await porto.checkpoint()
        return "done"

    started = time.monotonic()
    assert porto.run(main) == "done"
    assert time.monotonic() - started < 5
    assert events == ["background cancelled"]


def test_a_root_task_that_ignores_cancellation_is_abandoned_after_cancel_timeout() -> None:
    events: list[str] = []

    async def stubborn() -> None:
        try:
            while True:
                try:
                    await porto.sleep(30)
                except porto.Cancelled:
                    events.append("swallowed")
        finally:
            events.append("closed")

    async def main() -> None:
        _core._root_scope().spawn(stubborn)
        await porto.checkpoint()

    started = time.monotonic()
    with pytest.warns(RuntimeWarning, match=r"1 task\(s\) of the root scope .* abandoned"):
        porto.run(main, cancel_timeout=0.2)
    assert 0.15 < time.monotonic() - started < 5
    # It was cancelled at every await until the timeout, then closed with the runtime.
    assert events[0] == "swallowed"
    assert events[-1] == "closed"


def test_root_tasks_that_succeed_do_not_change_main_s_result() -> None:
    async def main() -> int:
        task = _core._root_scope().spawn(porto.sleep, 0)
        await porto.sleep(0.01)
        assert task.done()
        return 7

    assert porto.run(main) == 7


def test_the_root_scope_is_closed_once_run_returned() -> None:
    async def main() -> porto.Scope:
        return _core._root_scope()

    root = porto.run(main)
    assert repr(root) == "<porto.Scope closed, cancelled: shutdown, 0 live tasks>"

    async def spawn_into_the_old_root() -> None:
        root.spawn(porto.sleep, 0)

    with pytest.raises(RuntimeError, match="scope is closed"):
        porto.run(spawn_into_the_old_root)


def test_the_root_scope_cannot_be_entered() -> None:
    async def main() -> None:
        async with _core._root_scope():
            pass

    with pytest.raises(RuntimeError, match="a scope can be entered only once"):
        porto.run(main)


def test_a_huge_cancel_timeout_is_accepted() -> None:
    async def main() -> int:
        return 1

    assert porto.run(main, cancel_timeout=1e300) == 1


def test_root_scope_outside_porto_run_raises_and_says_what_to_do() -> None:
    with pytest.raises(porto.NoRuntimeError, match=r"call it inside main"):
        _core._root_scope()

    async def main() -> Any:
        return _core._root_scope()

    # The private `_run` has no root scope.
    with pytest.raises(porto.NoRuntimeError, match=r"porto.run\(main\)"):
        _core._run(main())
