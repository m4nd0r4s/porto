"""``porto.Cancelled`` and what is not it (15-structured-concurrency-and-cancellation.md C11).

``porto.Cancelled`` is an ``asyncio.CancelledError``; in debug mode a task that catches it and
finishes normally is reported ("cancellation was swallowed", once); an ``asyncio.CancelledError``
that porto did not raise is a plain failure, never an absorbed cancellation; awaiting a porto
awaitable with no runtime raises ``porto.NoRuntimeError``.
"""

import asyncio
import contextlib
import sys
import warnings
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

import porto

SWALLOWED = "cancellation was swallowed"


def run_debug(main: Callable[[], Coroutine[Any, Any, object]], **options: Any) -> list[str]:
    """Runs ``main`` and returns the text of every ``RuntimeWarning`` it emitted."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        porto.run(main, **options)
    return [str(w.message) for w in caught if issubclass(w.category, RuntimeWarning)]


def swallowed(texts: list[str]) -> list[str]:
    return [text for text in texts if SWALLOWED in text]


async def swallow_and_return() -> str:
    """The mistake C11 warns about: catch the cancellation and carry on."""
    with contextlib.suppress(porto.Cancelled):
        await porto.sleep(30)
    return "survived"


# --- the type ----------------------------------------------------------------------------------


def test_C11_cancelled_is_an_asyncio_cancelled_error() -> None:
    assert issubclass(porto.Cancelled, asyncio.CancelledError)
    assert isinstance(porto.Cancelled(), asyncio.CancelledError)
    # As CancelledError itself: a BaseException, so `except Exception` does not eat it.
    assert issubclass(porto.Cancelled, BaseException)
    assert not issubclass(porto.Cancelled, Exception)
    assert porto.Cancelled.__module__ == "porto.errors"


def test_C11_the_cancelled_a_task_gets_is_caught_by_except_asyncio_cancelled_error() -> None:
    async def main() -> list[type[BaseException]]:
        seen: list[type[BaseException]] = []
        async with porto.scope() as s:
            s.cancel()
            try:
                await porto.checkpoint()
            except asyncio.CancelledError as error:
                seen.append(type(error))
                raise
        return seen

    assert porto.run(main) == [porto.Cancelled]


# --- the swallow warning (debug mode) ------------------------------------------------------------


def test_C11_debug_warns_once_when_a_task_swallows_the_cancellation() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(swallow_and_return)
            await porto.checkpoint()
            s.cancel()
        assert task.result() == "survived"

    texts = swallowed(run_debug(main, debug=True))
    assert len(texts) == 1
    assert "swallow_and_return" in texts[0]
    assert "let the Cancelled propagate" in texts[0]


def test_C11_a_task_that_swallows_repeatedly_is_warned_about_once() -> None:
    """Level-triggered (C1): it is delivered the cancellation again at each await."""

    async def stubborn() -> int:
        caught = 0
        for _ in range(5):
            try:
                await porto.sleep(30)
            except porto.Cancelled:
                caught += 1
        return caught

    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(stubborn)
            await porto.checkpoint()
            s.cancel()
        assert task.result() == 5

    assert len(swallowed(run_debug(main, debug=True))) == 1


def test_C11_each_swallowing_task_is_warned_about_once() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            for _ in range(3):
                s.spawn(swallow_and_return)
            await porto.checkpoint()
            s.cancel()

    assert len(swallowed(run_debug(main, debug=True))) == 3


def test_C11_main_swallowing_a_cancellation_is_warned_about() -> None:
    async def main() -> str:
        porto.current_task().cancel()
        with contextlib.suppress(porto.Cancelled):
            await porto.checkpoint()
        return "carried on"

    texts = swallowed(run_debug(main, debug=True))
    assert len(texts) == 1


def test_C11_a_task_cancelled_with_task_cancel_that_swallows_it_is_warned_about() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(swallow_and_return)
            await porto.checkpoint()
            task.cancel()
        assert task.result() == "survived"

    assert len(swallowed(run_debug(main, debug=True))) == 1


def test_C11_a_body_that_swallows_the_cancel_of_its_scope_is_warned_about() -> None:
    """The scope does not absorb a cancellation its body caught (C3), and the task goes on."""

    async def main() -> None:
        async with porto.scope() as s:
            s.cancel()
            with contextlib.suppress(porto.Cancelled):
                await porto.checkpoint()

    assert len(swallowed(run_debug(main, debug=True))) == 1


def test_C11_the_warning_is_off_without_debug() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(swallow_and_return)
            await porto.checkpoint()
            s.cancel()

    assert swallowed(run_debug(main, debug=False)) == []
    assert swallowed(run_debug(main)) == []  # the default is off outside `python -X dev`


def test_C11_debug_is_per_run() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(swallow_and_return)
            await porto.checkpoint()
            s.cancel()

    assert len(swallowed(run_debug(main, debug=True))) == 1
    assert swallowed(run_debug(main, debug=False)) == []
    assert len(swallowed(run_debug(main, debug=True))) == 1


def test_C11_a_task_that_lets_the_cancellation_out_is_not_reported() -> None:
    async def polite() -> None:
        try:
            await porto.sleep(30)
        except porto.Cancelled:
            raise

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(polite)
            await porto.checkpoint()
            s.cancel()

    assert swallowed(run_debug(main, debug=True)) == []


def test_C11_a_cancellation_absorbed_by_a_scope_exit_is_not_a_swallow() -> None:
    """The task goes on after the scope that absorbed it, which is what the scope is for."""

    async def main() -> None:
        async with porto.move_on_after(0.01) as limit:
            await porto.sleep(30)
        assert limit.timed_out
        await porto.sleep(0.001)

    assert swallowed(run_debug(main, debug=True)) == []


def test_C11_cleanup_in_a_shield_before_reraising_is_not_reported() -> None:
    async def main() -> None:
        log: list[str] = []

        async def worker() -> None:
            try:
                await porto.sleep(30)
            except porto.Cancelled:
                with porto.shield():
                    await porto.sleep(0.001)
                    log.append("cleaned")
                raise

        async with porto.scope() as s:
            s.spawn(worker)
            await porto.checkpoint()
            s.cancel()
        assert log == ["cleaned"]

    assert swallowed(run_debug(main, debug=True)) == []


def test_C11_a_task_that_fails_after_catching_the_cancellation_is_not_a_swallow() -> None:
    async def converts() -> None:
        try:
            await porto.sleep(30)
        except porto.Cancelled:
            raise ValueError("while stopping") from None

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(converts)
            await porto.checkpoint()
            s.cancel()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(ExceptionGroup):
            porto.run(main, debug=True)
    assert swallowed([str(w.message) for w in caught]) == []


def test_C11_a_task_never_cancelled_is_not_reported() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(porto.sleep, 0.001)

    assert swallowed(run_debug(main, debug=True)) == []


def test_C11_the_warning_turned_into_an_error_does_not_break_the_run() -> None:
    """With ``-W error`` the warning cannot raise into the task that already ended: the run
    finishes, and the error goes to the unraisable hook."""

    async def main() -> str:
        async with porto.scope() as s:
            task = s.spawn(swallow_and_return)
            await porto.checkpoint()
            s.cancel()
        return task.result()

    unraisable: list[object] = []
    previous = sys.unraisablehook
    sys.unraisablehook = lambda hook_args: unraisable.append(hook_args.exc_value)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert porto.run(main, debug=True) == "survived"
    finally:
        sys.unraisablehook = previous
    assert len(unraisable) == 1
    assert SWALLOWED in str(unraisable[0])


# --- asyncio.CancelledError that porto did not raise ---------------------------------------------


async def raise_foreign() -> None:
    await porto.checkpoint()
    raise asyncio.CancelledError("not from porto")


def test_C11_a_foreign_cancelled_error_in_a_child_is_a_failure_in_the_group() -> None:
    async def main() -> BaseExceptionGroup[Any]:
        try:
            async with porto.scope() as s:
                s.spawn(raise_foreign)
                await porto.sleep(30)
        except BaseExceptionGroup as group:
            return group
        raise AssertionError("the scope absorbed it")

    group = porto.run(main)
    (error,) = group.exceptions
    assert type(error) is asyncio.CancelledError
    assert not isinstance(error, porto.Cancelled)
    assert str(group).startswith("porto scope failed")


def test_C11_a_foreign_cancelled_error_fails_the_scope_and_cancels_the_siblings() -> None:
    """The failure cancels the scope (C4), as any other child failure does."""

    async def main() -> list[str]:
        log: list[str] = []

        async def sibling() -> None:
            try:
                await porto.sleep(30)
            except porto.Cancelled:
                log.append("sibling cancelled")
                raise

        with contextlib.suppress(BaseExceptionGroup):
            async with porto.scope() as s:
                s.spawn(sibling)
                s.spawn(raise_foreign)
        assert s.cancel_called
        return log

    assert porto.run(main) == ["sibling cancelled"]


def test_C11_a_foreign_cancelled_error_leaving_the_body_is_not_absorbed() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            assert not s.cancel_called
            raise asyncio.CancelledError("not from porto")

    with pytest.raises(asyncio.CancelledError) as raised:
        porto.run(main)
    assert not isinstance(raised.value, porto.Cancelled)


def test_C11_a_foreign_cancelled_error_in_the_body_joins_the_failures_of_the_children() -> None:
    async def main() -> BaseExceptionGroup[Any]:
        async def fail() -> None:
            raise ValueError("child")

        try:
            async with porto.scope() as s:
                s.spawn(fail)
                await porto.checkpoint()
                raise asyncio.CancelledError("not from porto")
        except BaseExceptionGroup as group:
            return group
        raise AssertionError("no group")

    group = porto.run(main)
    assert [type(error) for error in group.exceptions] == [asyncio.CancelledError, ValueError]
    assert not isinstance(group, ExceptionGroup)  # it holds a BaseException


def test_C11_a_foreign_cancelled_error_ends_the_task_as_failed_not_cancelled() -> None:
    async def main() -> tuple[bool, bool]:
        task = None
        try:
            async with porto.scope() as s:
                task = s.spawn(raise_foreign)
        except BaseExceptionGroup:
            pass
        assert task is not None
        return task.cancelled(), isinstance(task.exception(), asyncio.CancelledError)

    assert porto.run(main) == (False, True)


# --- awaiting a porto awaitable with no runtime ------------------------------------------------


OPEN_CHANNEL = porto.channel[int](1)  # the sender lives on: recv has to wait


def drive(make: Callable[[], Any]) -> None:
    """Awaits ``make()`` in a bare coroutine, outside any runtime."""

    async def go() -> None:
        await make()

    coroutine = go()
    try:
        coroutine.send(None)
    finally:
        coroutine.close()


async def sleep_in_asyncio() -> None:
    await porto.sleep(0)


def test_C11_awaiting_porto_sleep_outside_a_runtime_raises_no_runtime_error() -> None:
    with pytest.raises(porto.NoRuntimeError, match=r"porto\.sleep\(\) was called outside"):
        drive(lambda: porto.sleep(0))
    with pytest.raises(porto.NoRuntimeError):
        asyncio.run(sleep_in_asyncio())


@pytest.mark.parametrize(
    "make",
    [
        lambda: porto.sleep(0.1),
        lambda: porto.checkpoint(),
        lambda: porto.sleep_forever(),
        lambda: porto.Lock().acquire(),
        lambda: porto.Event().wait(),
        lambda: porto.Semaphore(1).acquire(),
        lambda: porto.Notify().notified(),
        lambda: OPEN_CHANNEL[1].recv(),
    ],
    ids=["sleep", "checkpoint", "sleep_forever", "lock", "event", "semaphore", "notify", "recv"],
)
def test_C11_every_porto_awaitable_raises_no_runtime_error_outside_a_runtime(
    make: Callable[[], Any],
) -> None:
    with pytest.raises(porto.NoRuntimeError):
        drive(make)


def test_C11_no_runtime_error_is_a_runtime_error_and_says_what_to_do() -> None:
    with pytest.raises(RuntimeError, match=r"start the program with porto\.run\(main\)"):
        drive(lambda: porto.sleep(1))
    assert issubclass(porto.NoRuntimeError, RuntimeError)
    assert issubclass(porto.NoRuntimeError, porto.PortoError)
