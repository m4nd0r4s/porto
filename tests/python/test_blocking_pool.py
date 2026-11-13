"""The blocking pool and ``porto.to_thread`` (spec 19 §1, §2; porto-szb.25): an elastic pool of
threads that run Python callables with a thread state of their own.

Timing assertions have generous limits: CI machines are slow and loaded."""

import contextvars
import ctypes
import functools
import math
import subprocess
import sys
import textwrap
import threading
import time
import traceback
import warnings
from collections.abc import Callable
from typing import Any, ClassVar, cast

import pytest

import porto
from porto import _core  # pyright: ignore[reportPrivateUsage]


def stats() -> tuple[int, int, int]:
    return _core._blocking_stats()  # pyright: ignore[reportPrivateUsage]


async def eventually(condition: Callable[[], bool], what: str, seconds: float = 20.0) -> None:
    deadline = porto.current_time() + seconds
    while not condition():
        assert porto.current_time() < deadline, f"timed out waiting for {what}"
        await porto.sleep(0.01)


def test_a_job_runs_off_the_lane_on_a_thread_with_an_attached_thread_state() -> None:
    def job() -> tuple[int, int]:
        return (
            threading.get_ident(),
            cast(int, ctypes.pythonapi.PyGILState_Check()),
        )

    async def main() -> tuple[int, tuple[int, int]]:
        return threading.get_ident(), await porto.to_thread(job)

    lane, (worker, attached) = porto.run(main)
    assert worker != lane
    assert attached == 1


def test_results_arguments_and_keywords_travel() -> None:
    def job(first: int, second: int, *, scale: int = 1, fn: str = "") -> tuple[int, str]:
        return (first + second) * scale, fn

    async def main() -> tuple[int, str]:
        # `fn` as a keyword is the callable's own, not to_thread's.
        return await porto.to_thread(job, 1, 2, scale=10, fn="mine")

    assert porto.run(main) == (30, "mine")


class JobError(Exception):
    pass


def failing_job_in_a_worker(value: int) -> int:
    raise JobError(f"failed with {value}")


def test_an_exception_propagates_with_the_traceback_of_the_worker() -> None:
    async def main() -> JobError:
        try:
            await porto.to_thread(failing_job_in_a_worker, 7)
        except JobError as error:
            return error
        raise AssertionError("the job's exception was lost")

    error = porto.run(main)
    assert str(error) == "failed with 7"
    frames = [frame.name for frame in traceback.extract_tb(error.__traceback__)]
    assert "failing_job_in_a_worker" in frames, frames
    assert "main" in frames, frames


def test_a_base_exception_from_the_job_reaches_the_awaiter_as_an_exception() -> None:
    def job() -> None:
        raise KeyboardInterrupt("from a worker")

    async def main() -> str:
        try:
            await porto.to_thread(job)
        except KeyboardInterrupt as error:
            return str(error)
        return "no exception"

    assert porto.run(main) == "from a worker"


def test_the_callable_runs_in_a_copy_of_the_callers_context() -> None:
    seen: contextvars.ContextVar[str] = contextvars.ContextVar("seen", default="unset")
    written: contextvars.ContextVar[str] = contextvars.ContextVar("written", default="unset")

    def job() -> str:
        value = seen.get()
        written.set("from the job")
        return value

    async def main() -> tuple[str, str]:
        seen.set("the callers value")
        value = await porto.to_thread(job)
        return value, written.get()

    value, leaked = porto.run(main)
    assert value == "the callers value"
    assert leaked == "unset", "the job's context was not a copy"


def test_one_thread_state_serves_every_job_of_its_thread() -> None:
    # One worker: a thread-local set by one job is there for the next, because the thread keeps
    # its Python thread state between jobs (25 P0).
    local = threading.local()

    def first() -> int:
        local.value = 41
        return threading.get_ident()

    def second() -> tuple[int, int]:
        return threading.get_ident(), cast(int, local.value)

    async def main() -> tuple[int, tuple[int, int]]:
        return await porto.to_thread(first), await porto.to_thread(second)

    ident, (again, value) = porto.run(main, blocking_threads=1)
    assert ident == again
    assert value == 41


def test_the_pool_grows_under_100_concurrent_jobs_and_shrinks_after_the_keepalive() -> None:
    barrier = threading.Barrier(100, timeout=60)

    # More than the default limiter's 40 tokens: this test is about the pool, not the limiter.
    unbounded = porto.CapacityLimiter(math.inf)

    async def job() -> int:
        return await porto.to_thread(barrier.wait, limiter=unbounded)

    async def main() -> tuple[int, int, int]:
        assert stats() == (0, 0, 0), "a thread exists before the first job"
        peak = 0
        async with porto.scope() as s:
            tasks = [s.spawn(job) for _ in range(100)]
            # All 100 wait for each other at the barrier: they pass only with 100 threads.
            await eventually(lambda: stats()[0] >= 100, "100 workers")
            peak = stats()[0]
        indexes = sorted(task.result() for task in tasks)
        assert indexes == list(range(100))
        await eventually(lambda: stats()[0] == 0, "the idle workers to exit")
        return peak, stats()[1], stats()[2]

    peak, idle, queued = porto.run(main, blocking_keepalive=0.2)
    assert peak >= 100
    assert (idle, queued) == (0, 0)


def test_blocking_threads_caps_the_pool_and_the_rest_queue() -> None:
    lock = threading.Lock()
    running = 0
    most = 0

    def job(number: int) -> int:
        nonlocal running, most
        with lock:
            running += 1
            most = max(most, running)
        time.sleep(0.01)
        with lock:
            running -= 1
        return number

    async def main() -> list[int]:
        async with porto.scope() as s:
            tasks = [s.spawn(porto.to_thread, job, number) for number in range(20)]
        assert stats()[0] <= 3
        return [task.result() for task in tasks]

    assert porto.run(main, blocking_threads=3) == list(range(20))
    assert 1 <= most <= 3


def test_the_pool_settings_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTO_BLOCKING_THREADS", "2")
    monkeypatch.setenv("PORTO_BLOCKING_KEEPALIVE", "0.2")
    lock = threading.Lock()
    running = 0
    most = 0

    def job() -> None:
        nonlocal running, most
        with lock:
            running += 1
            most = max(most, running)
        time.sleep(0.05)
        with lock:
            running -= 1

    async def main() -> None:
        async with porto.scope() as s:
            for _ in range(8):
                s.spawn(porto.to_thread, job)
        await eventually(lambda: stats()[0] == 0, "the keepalive from the environment")

    porto.run(main)
    assert most == 2


def test_a_cancel_waits_for_the_job_by_default() -> None:
    finished = threading.Event()
    seen: dict[str, object] = {}

    def job() -> int:
        time.sleep(0.3)
        finished.set()
        return 5

    async def worker() -> None:
        value = await porto.to_thread(job)
        # The await is shielded: the job is over before the task goes on, with its result.
        seen["after"] = (finished.is_set(), value)
        await porto.checkpoint()
        seen["unreachable"] = True  # the cancel lands at the first checkpoint after

    async def main() -> bool:
        async with porto.scope() as s:
            task = s.spawn(worker)
            await porto.sleep(0.05)
            s.cancel()
        return task.cancelled()

    assert porto.run(main)
    assert seen == {"after": (True, 5)}


def test_a_deadline_waits_for_the_job_by_default() -> None:
    def job() -> str:
        time.sleep(0.3)
        return "done"

    async def main() -> str:
        started = porto.current_time()
        with porto.move_on_after(0.05):
            result = await porto.to_thread(job)
        assert porto.current_time() - started >= 0.25, "the deadline abandoned the job"
        return result

    assert porto.run(main) == "done"


def test_cancellable_raises_at_once_and_the_job_runs_on_with_its_result_discarded() -> None:
    finished = threading.Event()
    lane = threading.get_ident()

    class Result:
        released_on: ClassVar[list[int]] = []

        def __del__(self) -> None:
            Result.released_on.append(threading.get_ident())

    def job() -> Result:
        time.sleep(0.5)
        finished.set()
        return Result()

    async def main() -> float:
        started = porto.current_time()
        with porto.move_on_after(0.05) as scope:
            await porto.to_thread(job, cancellable=True)
        assert scope.timed_out
        elapsed = porto.current_time() - started
        assert not finished.is_set(), "the await waited for the job"
        # The job ends later; its result is released by the lane, not the worker.
        await eventually(finished.is_set, "the job to finish")
        await eventually(lambda: bool(Result.released_on), "the result to be released")
        return elapsed

    assert porto.run(main) < 0.4
    assert Result.released_on == [lane]


def test_run_waits_for_a_running_job_without_holding_the_interpreter_from_it() -> None:
    # The run ends while a job still sleeps: the pool's shutdown waits for it, and the lane
    # thread must have let go of the interpreter meanwhile, or the job cannot finish after its
    # sleep (it takes the interpreter back as it returns) and the wait runs out its second.
    finished = threading.Event()

    def job() -> None:
        time.sleep(0.2)
        finished.set()

    async def main() -> None:
        with porto.move_on_after(0.02):
            await porto.to_thread(job, cancellable=True)

    started = time.monotonic()
    porto.run(main)
    elapsed = time.monotonic() - started
    assert finished.is_set(), "the run did not wait for the job it was told to leave running"
    assert elapsed < 0.6, f"the shutdown waited out its second: {elapsed:.2f} s"


def test_a_trap_that_is_never_awaited_starts_nothing() -> None:
    ran = threading.Event()

    async def main() -> None:
        trap = _core._run_in_thread(ran.set, (), None, contextvars.copy_context())  # pyright: ignore[reportPrivateUsage]
        del trap
        await porto.sleep(0.1)
        assert stats() == (0, 0, 0)

    porto.run(main)
    assert not ran.is_set()


def test_a_job_can_use_the_blocking_methods_of_the_primitives() -> None:
    # ST-17: a pool thread, not a lane, may block on porto's primitives.
    async def main() -> bool:
        event = porto.Event()
        async with porto.scope() as s:
            task = s.spawn(porto.to_thread, event.blocking_wait)
            await porto.sleep(0.05)
            event.set()
        return task.result() is None

    assert porto.run(main)


def test_a_job_that_waits_does_not_stall_the_lane() -> None:
    release = threading.Event()

    async def main() -> int:
        ticks = 0
        async with porto.scope() as s:
            task = s.spawn(porto.to_thread, release.wait)
            for _ in range(20):
                await porto.sleep(0.005)
                ticks += 1
            release.set()
        assert task.result() is True
        return ticks

    assert porto.run(main) == 20


def test_to_thread_says_what_to_do_for_its_mistakes() -> None:
    async def not_callable() -> None:
        with pytest.raises(TypeError, match=r"needs a callable.*not int"):
            await porto.to_thread(cast(Any, 3))

    async def with_a_limiter_that_is_not_one() -> None:
        for wrong in (3, porto.Semaphore(1), "x"):
            with pytest.raises(
                TypeError, match=r"needs a porto\.CapacityLimiter.*CapacityLimiter\(n\)"
            ):
                await porto.to_thread(len, "x", limiter=cast(Any, wrong))

    async def a_mistake_borrows_nothing() -> None:
        limiter = porto.CapacityLimiter(1)
        with pytest.raises(TypeError, match="needs a callable"):
            await porto.to_thread(cast(Any, 3), limiter=limiter)
        assert limiter.borrowed_tokens == 0

    porto.run(with_a_limiter_that_is_not_one)
    porto.run(a_mistake_borrows_nothing)
    with pytest.raises(porto.NoRuntimeError, match=r"outside porto\.run"):
        _ = _core._run_in_thread(len, ("abc",), None, contextvars.copy_context())  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(porto.NoRuntimeError, match=r"outside porto\.run"):
        _ = stats()


def test_a_job_unfinished_at_shutdown_is_left_to_finish_after_a_second() -> None:
    # In a process of its own: the run must return while the job still runs, and the process
    # must exit cleanly with a worker thread alive.
    script = textwrap.dedent(
        """
        import time
        import porto

        # The default await is shielded, so the scope would wait for the job: abandon it.
        async def sleeper() -> None:
            await porto.to_thread(time.sleep, 4.0, cancellable=True)

        async def abandoned() -> None:
            async with porto.scope() as s:
                s.spawn(sleeper)
                await porto.sleep(0.1)
                s.cancel()

        started = time.monotonic()
        porto.run(abandoned)
        elapsed = time.monotonic() - started
        print(f"{elapsed:.2f}")
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=60, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stderr == ""
    elapsed = float(completed.stdout.strip())
    # The pool waits up to a second for the running job, then detaches it.
    assert 0.9 <= elapsed < 3.5, elapsed


def test_a_warning_free_run_with_many_short_jobs() -> None:
    async def main() -> int:
        async with porto.scope() as s:
            tasks = [s.spawn(porto.to_thread, pow, 2, number) for number in range(200)]
        return sum(task.result() for task in tasks) & 0xFF

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert porto.run(main) == sum(2**number for number in range(200)) & 0xFF


# --- the limiter (spec 19 §2; porto-szb.26) ---


def test_the_default_limiter_has_40_tokens_and_is_shared() -> None:
    limiter = porto.to_thread.default_limiter()
    assert isinstance(limiter, porto.CapacityLimiter)
    assert limiter.total_tokens == 40
    assert porto.to_thread.default_limiter() is limiter
    assert limiter.borrowed_tokens == 0


def test_the_41st_concurrent_call_waits_on_the_default_limiter() -> None:
    release = threading.Event()
    started = threading.Semaphore(0)
    running = 0
    lock = threading.Lock()

    def job() -> None:
        nonlocal running
        with lock:
            running += 1
        started.release()
        release.wait(30)

    async def main() -> None:
        limiter = porto.to_thread.default_limiter()
        async with porto.scope() as s:
            tasks = [s.spawn(porto.to_thread, job) for _ in range(41)]
            await eventually(lambda: limiter.borrowed_tokens == 40, "40 tokens to be borrowed")
            await porto.sleep(0.1)
            assert running == 40, "the 41st call started while all 40 tokens were borrowed"
            assert limiter.available_tokens == 0
            release.set()
        assert running == 41, "the 41st call never ran"
        assert all(task.result() is None for task in tasks)
        assert limiter.borrowed_tokens == 0

    porto.run(main)


def test_a_custom_limiter_is_honored_and_leaves_the_default_alone() -> None:
    lock = threading.Lock()
    running = 0
    most = 0

    def job() -> None:
        nonlocal running, most
        with lock:
            running += 1
            most = max(most, running)
        time.sleep(0.02)
        with lock:
            running -= 1

    async def main() -> None:
        limiter = porto.CapacityLimiter(2)
        async with porto.scope() as s:
            for _ in range(8):
                s.spawn(functools.partial(porto.to_thread, job, limiter=limiter))
            await eventually(lambda: limiter.borrowed_tokens == 2, "two tokens to be borrowed")
            assert porto.to_thread.default_limiter().borrowed_tokens == 0
        assert limiter.borrowed_tokens == 0

    porto.run(main)
    assert most == 2


def test_a_limiter_can_be_shared_with_the_caller_and_raised_while_calls_wait() -> None:
    release = threading.Event()

    async def main() -> None:
        limiter = porto.CapacityLimiter(1)
        async with porto.scope() as s:
            first = s.spawn(functools.partial(porto.to_thread, release.wait, 30, limiter=limiter))
            second = s.spawn(functools.partial(porto.to_thread, release.wait, 30, limiter=limiter))
            await eventually(lambda: limiter.borrowed_tokens == 1, "the first call to start")
            await porto.sleep(0.05)
            assert limiter.borrowed_tokens == 1
            assert stats()[0] == 1, "the second call started without a token"
            limiter.total_tokens = 2  # grants the waiting call
            await eventually(lambda: stats()[0] == 2, "the second call to start")
            release.set()
        assert first.result() is True
        assert second.result() is True

    porto.run(main)


class Boom(Exception):
    pass


def raise_boom() -> None:
    raise Boom


@pytest.mark.parametrize("cancellable", [False, True])
def test_the_token_is_released_when_the_function_raises(cancellable: bool) -> None:
    async def main() -> None:
        limiter = porto.CapacityLimiter(1)
        for _ in range(3):  # a leak would block the second call forever
            with pytest.raises(Boom):
                await porto.to_thread(raise_boom, cancellable=cancellable, limiter=limiter)
            await eventually(lambda: limiter.borrowed_tokens == 0, "the token to be released")

    porto.run(main)


def test_the_token_is_released_after_a_cancel_that_waited_for_the_job() -> None:
    async def main() -> None:
        limiter = porto.CapacityLimiter(1)
        with porto.move_on_after(0.05) as scope:
            await porto.to_thread(time.sleep, 0.3, limiter=limiter)
            assert limiter.borrowed_tokens == 0, "the token is still borrowed after the job"
            await porto.sleep(0)  # the deferred deadline takes effect here
        assert scope.timed_out
        assert limiter.borrowed_tokens == 0

    porto.run(main)


def test_a_cancelled_abandoned_job_keeps_its_token_until_the_thread_is_done() -> None:
    release = threading.Event()

    async def main() -> None:
        limiter = porto.CapacityLimiter(1)
        with porto.move_on_after(0.05) as scope:
            await porto.to_thread(release.wait, 30, cancellable=True, limiter=limiter)
        assert scope.timed_out
        # The await is gone, the thread is not: its token is still borrowed, so the next call
        # waits (a limiter that counted only awaiting tasks would let it start).
        assert limiter.borrowed_tokens == 1
        with porto.move_on_after(0.1) as waiting:
            await porto.to_thread(len, "x", limiter=limiter)
        assert waiting.timed_out, "a call started on the token of a thread that still runs"
        release.set()
        await eventually(lambda: limiter.borrowed_tokens == 0, "the token to come back")
        assert await porto.to_thread(len, "abc", limiter=limiter) == 3

    porto.run(main)


@pytest.mark.parametrize("cancellable", [False, True])
def test_a_cancel_while_waiting_for_a_token_takes_effect_at_once(cancellable: bool) -> None:
    release = threading.Event()
    ran = threading.Event()

    async def main() -> None:
        limiter = porto.CapacityLimiter(1)
        async with porto.scope() as s:
            s.spawn(functools.partial(porto.to_thread, release.wait, 30, limiter=limiter))
            await eventually(lambda: limiter.borrowed_tokens == 1, "the first call to start")
            started = porto.current_time()
            with porto.move_on_after(0.05) as scope:
                # No thread runs for this call yet: even the shielded mode lets the cancel in.
                await porto.to_thread(ran.set, cancellable=cancellable, limiter=limiter)
            assert scope.timed_out
            assert porto.current_time() - started < 2.0
            assert limiter.borrowed_tokens == 1, "the cancelled call took or lost a token"
            release.set()
        assert limiter.borrowed_tokens == 0
        assert not ran.is_set()

    porto.run(main)


def test_the_token_of_an_abandoned_job_comes_back_after_the_run_ended() -> None:
    release = threading.Event()
    limiter = porto.CapacityLimiter(2)

    def stuck() -> None:
        release.wait(30)

    async def main() -> None:
        async with porto.scope() as s:
            # One worker: the second job waits in the queue behind the first and is dropped,
            # unrun, when the run ends.
            for _ in range(2):
                s.spawn(
                    functools.partial(porto.to_thread, stuck, cancellable=True, limiter=limiter)
                )
            await eventually(lambda: limiter.borrowed_tokens == 2, "both tokens to be borrowed")
            s.cancel()

    porto.run(main, blocking_threads=1)
    # The running job's token waits for its thread; the dropped job's token is already back.
    assert limiter.borrowed_tokens == 1
    release.set()
    deadline = time.monotonic() + 20
    while limiter.borrowed_tokens and time.monotonic() < deadline:
        time.sleep(0.01)
    assert limiter.borrowed_tokens == 0
