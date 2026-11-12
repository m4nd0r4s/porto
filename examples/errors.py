"""Porto's exceptions: what each means, what raises it, and how to catch it.

    uv run python examples/errors.py

Every error porto raises is a ``porto.PortoError``, except ``porto.Cancelled`` (an
``asyncio.CancelledError``) and ``porto.Terminated`` (a ``SystemExit``), which keep their
standard meaning. Several also subclass the builtin you would catch for the same situation,
and every message starts with ``porto:`` and says what to do.
"""

import asyncio

import porto


def check_hierarchy() -> None:
    assert issubclass(porto.NoRuntimeError, porto.PortoError)
    assert issubclass(porto.NoRuntimeError, RuntimeError)
    assert issubclass(porto.RuntimeClosed, RuntimeError)
    assert issubclass(porto.ClosedResourceError, OSError)
    assert issubclass(porto.IncompleteReadError, EOFError)
    assert issubclass(porto.Cancelled, asyncio.CancelledError)
    assert not issubclass(porto.Cancelled, porto.PortoError)  # nor an Exception
    assert issubclass(porto.Terminated, SystemExit)
    for error in (porto.WouldBlock, porto.EndOfChannel, porto.BrokenResourceError, porto.Lagged):
        assert issubclass(error, porto.PortoError)
    # Deadlines raise the builtin TimeoutError, not a porto type (see deadlines_and_shields.py).
    assert not issubclass(TimeoutError, porto.PortoError)


def check_message(error: BaseException) -> None:
    assert str(error).startswith("porto: "), f"unhelpful message: {error}"


async def channel_errors() -> list[str]:
    """The channel errors, in the order a program meets them."""
    seen: list[str] = []
    tx, rx = porto.channel[str](1)

    try:
        rx.try_recv()  # nothing buffered yet
    except porto.WouldBlock as error:
        seen.append("WouldBlock")
        check_message(error)

    tx.try_send("first")
    try:
        tx.try_send("second")  # the one slot is taken
    except porto.WouldBlock:
        seen.append("WouldBlock")

    tx.close()
    try:
        await tx.send("late")  # this handle was closed here, by us
    except porto.ClosedResourceError as error:
        seen.append("ClosedResourceError")
        check_message(error)

    assert await rx.recv() == "first"  # what was buffered is still delivered
    try:
        await rx.recv()  # every sender is closed and the buffer is empty
    except porto.EndOfChannel:
        seen.append("EndOfChannel")

    tx2, rx2 = porto.channel[str](1)
    rx2.close()
    try:
        await tx2.send("anyone?")  # nobody can receive any more
    except porto.BrokenResourceError as error:
        seen.append("BrokenResourceError")
        check_message(error)
    return seen


async def more_channel_errors() -> list[str]:
    """A broadcast receiver that fell behind is told, with the exact count (``Lagged``)."""
    seen: list[str] = []
    tx, rx = porto.broadcast[int](2)
    for message in range(5):
        tx.send(message)  # the channel keeps two: 0, 1 and 2 are overwritten
    try:
        await rx.recv()
    except porto.Lagged as lag:
        seen.append("Lagged")
        check_message(lag)
        assert lag.skipped == 3
    assert [await rx.recv(), await rx.recv()] == [3, 4]  # then it carries on

    # A oneshot sender closed without a value ends the channel for the receiver.
    reply_tx, reply_rx = porto.oneshot[int]()
    reply_tx.close()
    try:
        await reply_rx.recv()
    except porto.EndOfChannel:
        seen.append("EndOfChannel")

    # Sync primitives raise WouldBlock from their *_nowait forms, too.
    limiter = porto.CapacityLimiter(1)
    limiter.acquire_on_behalf_of_nowait("a")
    try:
        limiter.acquire_on_behalf_of_nowait("b")
    except porto.WouldBlock:
        seen.append("WouldBlock")
    return seen


async def task_errors() -> list[str]:
    seen: list[str] = []
    async with porto.scope() as s:
        task = s.spawn(porto.sleep, 0.001)
        try:
            task.result()  # still running
        except porto.InvalidStateError as error:
            seen.append("InvalidStateError")
            check_message(error)

        # Reading the result of a cancelled task raises Cancelled, as asyncio does.
        victim = s.spawn(porto.sleep_forever)
        await porto.checkpoint()
        victim.cancel()
        await porto.checkpoint()
        try:
            victim.result()
        except porto.Cancelled:
            seen.append("Cancelled")
    return seen


def no_runtime_errors() -> list[str]:
    seen: list[str] = []
    try:
        porto.current_time()
    except RuntimeError as error:  # NoRuntimeError is a RuntimeError
        assert isinstance(error, porto.NoRuntimeError)
        seen.append("NoRuntimeError")
        check_message(error)
    return seen


async def main() -> None:
    check_hierarchy()
    assert await channel_errors() == [
        "WouldBlock",
        "WouldBlock",
        "ClosedResourceError",
        "EndOfChannel",
        "BrokenResourceError",
    ]
    assert await more_channel_errors() == ["Lagged", "EndOfChannel", "WouldBlock"]
    assert await task_errors() == ["InvalidStateError", "Cancelled"]


if __name__ == "__main__":
    assert no_runtime_errors() == ["NoRuntimeError"]
    porto.run(main)
    print("errors: ok")
