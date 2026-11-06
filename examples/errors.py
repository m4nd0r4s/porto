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
    for error in (porto.WouldBlock, porto.EndOfChannel, porto.BrokenResourceError):
        assert issubclass(error, porto.PortoError)


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


async def task_errors() -> list[str]:
    seen: list[str] = []
    async with porto.scope() as s:
        task = s.spawn(porto.sleep, 0.001)
        try:
            task.result()  # still running
        except porto.InvalidStateError as error:
            seen.append("InvalidStateError")
            check_message(error)
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
    assert await task_errors() == ["InvalidStateError"]


if __name__ == "__main__":
    assert no_runtime_errors() == ["NoRuntimeError"]
    porto.run(main)
    print("errors: ok")
