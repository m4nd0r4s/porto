"""Three more channels: ``porto.oneshot``, ``porto.watch`` and ``porto.broadcast``.

    uv run python examples/more_channels.py

``channels.py`` covers the work-queue channel (each value goes to one receiver). These three
fit other shapes, and none of their ``send`` calls waits, so ``send`` is a plain call:

- ``oneshot[T]()``: one value from one sender to one receiver: a reply to a request.
- ``watch[T](initial)``: the latest value of something that changes (a setting, a status).
  Receivers read it, and wait for it to change; they never queue up old values.
- ``broadcast[T](capacity)``: every receiver gets every message (an event bus). A receiver that
  falls too far behind is told how many messages it missed (``porto.Lagged``).
"""

import porto


async def request_reply() -> None:
    """A oneshot is the reply slot of a request: the server answers into the one the client sent."""
    requests_tx, requests_rx = porto.channel[tuple[str, porto.OneshotSender[int]]](4)

    async def server() -> None:
        async for name, reply in requests_rx:
            reply.send(len(name))  # never waits: not awaited

    async def ask(name: str) -> int:
        reply_tx, reply_rx = porto.oneshot[int]()
        await requests_tx.send((name, reply_tx))
        return await reply_rx.recv()

    answers: dict[str, int] = {}
    async with porto.scope() as s:
        s.spawn(server)
        for name in ("porto", "douro", "oxiform"):
            answers[name] = await ask(name)
        requests_tx.close()  # the server's loop ends
    assert answers == {"porto": 5, "douro": 5, "oxiform": 7}


async def oneshot_edges() -> None:
    tx, rx = porto.oneshot[str]()
    try:
        rx.try_recv()  # not sent yet
    except porto.WouldBlock:
        pass
    else:
        raise AssertionError("try_recv found a value that was never sent")
    tx.send("once")
    try:
        tx.send("twice")  # a oneshot sends once
    except porto.ClosedResourceError:
        pass
    else:
        raise AssertionError("sent two values on a oneshot")
    assert await rx.recv() == "once"

    # A sender closed without sending ends the channel: the receiver is not left waiting.
    tx, rx = porto.oneshot[str]()
    tx.close()
    try:
        await rx.recv()
    except porto.EndOfChannel:
        pass
    else:
        raise AssertionError("recv after a closed sender did not raise EndOfChannel")

    # A receiver that went away first makes the send fail: nobody will read the reply.
    tx, rx = porto.oneshot[str]()
    rx.close()
    try:
        tx.send("nobody home")
    except porto.BrokenResourceError:
        pass
    else:
        raise AssertionError("send without a receiver did not raise BrokenResourceError")


async def watch_latest_value() -> None:
    """A watch holds the latest value; receivers wait for it to change and read it."""
    tx, rx = porto.watch[dict[str, int]]({"workers": 2})
    assert rx.borrow() == {"workers": 2}  # the initial value, readable at once
    assert not rx.has_changed()  # and counts as seen

    seen: list[int] = []

    async def follow_config() -> None:
        # changed() waits for a version this receiver has not seen, and marks it seen.
        while True:
            try:
                await rx.changed()
            except porto.EndOfChannel:
                return  # every sender closed: the config will not change again
            seen.append(rx.borrow()["workers"])

    async with porto.scope() as s:
        s.spawn(follow_config)
        await porto.checkpoint()
        tx.send({"workers": 4})
        await porto.sleep(0.002)
        # Three sends before the follower runs again are one change: it sees the latest only.
        tx.send({"workers": 5})
        tx.send({"workers": 6})
        tx.send({"workers": 8})
        await porto.sleep(0.002)
        tx.close()
    assert seen == [4, 8], seen  # 5 and 6 were replaced before anyone looked

    # After the channel closed, borrow() still has the last value.
    assert rx.borrow() == {"workers": 8}


async def watch_as_a_clearable_event() -> None:
    """``porto.Event`` cannot be cleared; ``watch[bool](False)`` can: send(True) / send(False)."""
    paused_tx, paused_rx = porto.watch[bool](False)
    log: list[str] = []

    async def worker() -> None:
        for step in range(4):
            while paused_rx.borrow():  # wait out a pause, however many times it is toggled
                await paused_rx.changed()
            log.append(f"step {step}")
            await porto.sleep(0.003)

    async with porto.scope() as s:
        s.spawn(worker)
        await porto.sleep(0.005)  # about two steps
        paused_tx.send(True)
        before = len(log)
        await porto.sleep(0.01)
        assert len(log) <= before + 1  # at most the step that was running when it paused
        paused_tx.send(False)  # cleared: the worker goes on
    assert log == ["step 0", "step 1", "step 2", "step 3"], log


async def watch_iteration() -> None:
    """``async for`` over a receiver yields the latest value after each change."""
    tx, rx = porto.watch[str]("starting")
    states: list[str] = []

    async def monitor() -> None:
        async for state in rx:  # the initial value is not yielded, only changes
            states.append(state)

    async with porto.scope() as s:
        s.spawn(monitor)
        for state in ("ready", "busy", "stopped"):
            await porto.checkpoint()
            tx.send(state)
            await porto.checkpoint()
        tx.close()  # ends the loop
    assert states == ["ready", "busy", "stopped"], states


async def broadcast_fan_out() -> None:
    """Every receiver gets every message; a late subscriber only gets what comes after."""
    tx, first = porto.broadcast[str](8)
    assert tx.capacity == 8
    audit = tx.subscribe()  # a receiver made now sees the messages sent from now on
    metrics = first.clone()  # a clone starts where its source is
    tx.send("started")

    late = tx.subscribe()  # joins after "started"
    tx.send("stopped")
    tx.close()

    async def drain(receiver: porto.BroadcastReceiver[str]) -> list[str]:
        return [message async for message in receiver]  # ends once the channel is closed

    assert await drain(first) == ["started", "stopped"]
    assert await drain(metrics) == ["started", "stopped"]
    assert await drain(audit) == ["started", "stopped"]
    assert await drain(late) == ["stopped"]


async def broadcast_lagged() -> None:
    """A receiver more than ``capacity`` behind is told what it missed, then carries on."""
    tx, slow = porto.broadcast[int](3)
    for message in range(1, 8):  # seven messages into a channel that keeps three
        tx.send(message)
    try:
        await slow.recv()
    except porto.Lagged as lag:
        assert lag.skipped == 4  # 1, 2, 3 and 4 were overwritten before it read them
    else:
        raise AssertionError("a receiver that fell behind was not told")
    # The next receive returns the oldest message still kept.
    assert [await slow.recv() for _ in range(3)] == [5, 6, 7]

    # A subscriber that keeps up never sees Lagged.
    keeps_up = tx.subscribe()
    tx.send(8)
    assert await keeps_up.recv() == 8
    tx.close()


async def main() -> None:
    await request_reply()
    await oneshot_edges()
    await watch_latest_value()
    await watch_as_a_clearable_event()
    await watch_iteration()
    await broadcast_fan_out()
    await broadcast_lagged()
    print("more_channels: ok")


if __name__ == "__main__":
    porto.run(main)
