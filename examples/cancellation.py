"""Cancellation: a failing child stops its siblings, ``Scope.cancel``, and cancelling from a thread.

    uv run python examples/cancellation.py

Every scope is a cancel scope. Once cancelled, each task in it gets ``porto.Cancelled`` at its
awaits (all of them, not only the next one: cancellation is level-triggered) until it leaves the
scope, whose exit absorbs the ``Cancelled``. ``Cancelled`` is an ``asyncio.CancelledError``:
clean up in ``finally`` and let it propagate.
"""

import threading

import porto


async def failing_child_stops_siblings() -> None:
    """One failure cancels the scope: the slow sibling stops at once, not after 10 s."""
    cleaned_up: list[str] = []

    async def download() -> None:
        try:
            await porto.sleep(10)
        finally:
            cleaned_up.append("download")  # finally blocks run on cancellation

    async def parse() -> None:
        await porto.sleep(0.01)
        raise ValueError("bad header")

    started = porto.current_time()
    slow: porto.Task[None] | None = None
    s = porto.scope()  # made before the try, so it is bound in the except* and after
    try:
        async with s:
            slow = s.spawn(download)
            s.spawn(parse)
            await porto.sleep(10)  # the body is cancelled too
    except* ValueError as group:
        assert [str(error) for error in group.exceptions] == ["bad header"]
    elapsed = porto.current_time() - started
    assert elapsed < 1.0, f"siblings took {elapsed:.2f} s to stop"
    assert cleaned_up == ["download"]
    assert s.cancel_called  # a failing child cancels its scope
    # The cancelled sibling's result is its Cancelled.
    assert slow is not None
    try:
        slow.result()
    except porto.Cancelled:
        pass
    else:
        raise AssertionError("the cancelled task has a result")


async def first_answer_wins() -> str:
    """Asks several mirrors and keeps the first answer; cancel() stops the others."""
    answers: list[str] = []

    async def ask(mirror: str, seconds: float, s: porto.Scope) -> None:
        await porto.sleep(seconds)
        answers.append(mirror)
        s.cancel("got an answer")

    async with porto.scope() as s:
        for mirror, seconds in [("eu", 0.03), ("us", 0.01), ("asia", 5.0)]:
            s.spawn(ask, mirror, seconds, s)
        # The body is done; the exit waits for the race.
    # The cancel was ours, and it ended the losers: the scope absorbed their Cancelled, so
    # cancelled_caught is true although the body itself was never cancelled.
    assert s.cancel_called and s.cancelled_caught
    return answers[0]


async def cancel_from_body() -> None:
    """cancel() from the block itself: its next await raises Cancelled, the exit absorbs it."""
    reached: list[str] = []
    async with porto.scope() as s:
        s.cancel()
        reached.append("cancel() returned")  # cancel() itself does not raise
        await porto.checkpoint()
        reached.append("after the await")  # never runs
    assert reached == ["cancel() returned"]
    assert s.cancelled_caught

    # A scope that was not cancelled reports nothing.
    async with porto.scope() as quiet:
        await porto.checkpoint()
    assert not quiet.cancel_called and not quiet.cancelled_caught


async def level_triggered() -> None:
    """Catching Cancelled does not clear it: every later await in the scope raises again."""
    deliveries = 0
    async with porto.scope() as s:
        s.cancel()
        for _ in range(3):
            try:
                await porto.sleep(1)
            except porto.Cancelled:
                deliveries += 1  # swallowing it is a bug; this only counts the deliveries
    assert deliveries == 3
    # The block swallowed every Cancelled, so the exit had nothing to absorb.
    assert s.cancel_called and not s.cancelled_caught


async def nested_scopes_inherit() -> None:
    """Cancelling a scope cancels the scopes nested in it; the outer exit absorbs it."""
    inner_cancelled = False
    inner = porto.scope()
    async with porto.scope() as outer, inner:  # inner is entered inside outer
        outer.cancel()
        try:
            await porto.checkpoint()
        except porto.Cancelled:
            inner_cancelled = True
            raise
    assert inner_cancelled
    assert outer.cancelled_caught and not inner.cancelled_caught


async def cancel_from_another_thread() -> None:
    """cancel() is thread-safe; it wakes a task sleeping in the scope at once."""
    s = porto.scope()
    timer = threading.Timer(0.02, s.cancel, args=("operator pressed stop",))
    started = porto.current_time()
    timer.start()
    async with s:
        await porto.sleep(10)
    timer.join()
    elapsed = porto.current_time() - started
    assert 0.02 <= elapsed < 1.0, elapsed
    assert s.cancelled_caught


async def main() -> None:
    await failing_child_stops_siblings()
    assert await first_answer_wins() == "us"
    await cancel_from_body()
    await level_triggered()
    await nested_scopes_inherit()
    await cancel_from_another_thread()
    print("cancellation: ok")


if __name__ == "__main__":
    porto.run(main)
