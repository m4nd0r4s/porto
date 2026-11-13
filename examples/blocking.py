"""Blocking code on the runtime's thread pool: `porto.to_thread`.

A lane runs every task of the runtime, so a task that blocks (a file read, a DNS lookup, a
library that has no async API) stalls them all. `await porto.to_thread(fn, *args)` runs `fn` on
a pool thread instead and goes on when it returns. This example shows:

- the result, the arguments, and that `fn` runs on another thread while the lane keeps working;
- an exception raised in `fn` arriving with the traceback of the worker's frames;
- `fn` running in a *copy* of the caller's `contextvars` context;
- what a cancel or a deadline does to the await: by default it waits for `fn` (a thread cannot
  be stopped), and `cancellable=True` gives the caller back at once and discards the result;
- the limiter: at most 40 calls run at once by default (`porto.to_thread.default_limiter()`),
  or as many as the `porto.CapacityLimiter` passed as `limiter=` allows; a call waits for a
  token, and the token goes back when its thread is done.

The pool is elastic: no thread exists until the first job, a thread is added for each job
that no idle thread can take up to `blocking_threads` (default 512), and a thread idle for
`blocking_keepalive` seconds (default 10) exits. Both are `RuntimeConfig` fields, so also the
`PORTO_BLOCKING_THREADS` and `PORTO_BLOCKING_KEEPALIVE` variables.
"""

import contextvars
import threading
import time
import traceback

import porto

request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


def blocking_square(value: int) -> tuple[int, int]:
    time.sleep(0.01)  # a call that blocks its thread
    return value * value, threading.get_ident()


def blocking_failure(value: int) -> int:
    raise ValueError(f"cannot handle {value}")


def read_request_id() -> str:
    seen = request_id.get()
    request_id.set("set in the worker")
    return seen


async def result_and_threads() -> None:
    lane = threading.get_ident()
    square, worker = await porto.to_thread(blocking_square, 7)
    assert square == 49
    assert worker != lane, "the call ran on another thread"

    # While jobs block, the lane keeps running its tasks.
    release = threading.Event()
    ticks = 0
    waiting: list[porto.Task[bool]] = []
    async with porto.scope() as s:
        waiting = [s.spawn(porto.to_thread, release.wait) for _ in range(3)]
        for _ in range(10):
            await porto.sleep(0.001)
            ticks += 1
        release.set()
    assert ticks == 10 and all(task.result() is True for task in waiting)


async def errors() -> None:
    try:
        await porto.to_thread(blocking_failure, 3)
    except ValueError as error:
        frames = [frame.name for frame in traceback.extract_tb(error.__traceback__)]
        assert str(error) == "cannot handle 3"
        assert "blocking_failure" in frames, "the worker's frame is in the traceback"
    else:
        raise AssertionError("the exception was lost")


async def context() -> None:
    request_id.set("req-42")
    assert await porto.to_thread(read_request_id) == "req-42", "the worker sees the caller's value"
    assert request_id.get() == "req-42", "and cannot change it: it ran in a copy"


async def cancellation() -> None:
    def slow() -> str:
        time.sleep(0.15)
        return "finished"

    # Default: the await is shielded. The deadline passes while `slow` runs, and takes effect
    # once the thread is done: the result is returned first.
    started = porto.current_time()
    with porto.move_on_after(0.02):
        assert await porto.to_thread(slow) == "finished"
    assert porto.current_time() - started >= 0.14, "the caller waited for the thread"

    # cancellable=True: the deadline ends the await at once; the thread runs on, and its
    # result is dropped when it is done.
    started = porto.current_time()
    with porto.move_on_after(0.02) as scope:
        await porto.to_thread(slow, cancellable=True)
    assert scope.timed_out
    assert porto.current_time() - started < 0.12, "the caller did not wait for the thread"


async def limiting() -> None:
    default = porto.to_thread.default_limiter()
    # (A thread abandoned by `cancellable=True` above may still hold one: its token comes back
    # when the thread is done, not when the caller stops waiting.)
    assert default.total_tokens == 40 and default.borrowed_tokens <= 1

    # A limiter of its own: three jobs, two tokens, so the third waits for one to come back.
    limiter = porto.CapacityLimiter(2)
    release = threading.Event()

    async def limited_wait() -> bool:
        return await porto.to_thread(release.wait, limiter=limiter)

    async with porto.scope() as s:
        for _ in range(3):
            s.spawn(limited_wait)
        while limiter.borrowed_tokens < 2:
            await porto.sleep(0.001)
        await porto.sleep(0.02)
        assert limiter.available_tokens == 0, "two calls run, the third waits"
        release.set()
    assert limiter.borrowed_tokens == 0, "every token is back"


async def main() -> None:
    await result_and_threads()
    await errors()
    await context()
    await cancellation()
    await limiting()


if __name__ == "__main__":
    # A small pool whose idle threads leave quickly, as a program with bursts might want.
    porto.run(main, blocking_threads=8, blocking_keepalive=0.05)
    print("blocking: ok")
