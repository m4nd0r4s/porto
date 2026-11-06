"""Channels: ``porto.channel[T](capacity)`` and ``porto.unbounded_channel[T]()``.

    uv run python examples/channels.py

A channel is a queue between tasks with a sending end and a receiving end; clone either end for
more producers or consumers (each value goes to exactly one receiver). A bounded channel makes a
fast producer wait for a slow consumer (backpressure). Closing is per handle: once every sender
is closed, receivers drain what is buffered and then see the end (``async for`` stops).
"""

import porto


async def backpressure() -> list[str]:
    """With capacity 2, the producer can run at most two values ahead of the consumer."""
    tx, rx = porto.channel[int](2)
    assert tx.capacity == 2
    log: list[str] = []

    async def producer() -> None:
        async with tx:  # closes the sender when the block exits
            for value in range(4):
                await tx.send(value)  # waits while the channel is full
                log.append(f"sent {value}")

    async def consumer() -> None:
        await porto.sleep(0.01)  # a slow start: the producer fills the buffer, then waits
        log.append("consumer starts")
        async for value in rx:  # ends when every sender is closed and the buffer is drained
            log.append(f"got {value}")

    async with porto.scope() as s:
        s.spawn(producer)
        s.spawn(consumer)
    return log


async def unbounded_and_batches() -> list[list[int]]:
    """An unbounded channel never makes send wait; recv_many takes what is buffered at once."""
    tx, rx = porto.unbounded_channel[int]()
    assert tx.capacity is None
    for value in range(10):
        tx.try_send(value)  # never WouldBlock on an unbounded channel
    tx.close()
    batches: list[list[int]] = []
    while True:
        try:
            batches.append(await rx.recv_many(4))  # at least one, at most four
        except porto.EndOfChannel:
            return batches


async def fan_in() -> list[str]:
    """Several producers share one channel through clones; it ends when all of them close."""
    tx, rx = porto.channel[str](8)

    async def sensor(name: str, sender: porto.Sender[str]) -> None:
        async with sender:
            for reading in range(3):
                await sender.send(f"{name}:{reading}")
                await porto.sleep(0.001)

    readings: list[str] = []
    async with porto.scope() as s:
        s.spawn(sensor, "temp", tx.clone())
        s.spawn(sensor, "humidity", tx.clone())
        tx.close()  # the original is not needed; the clones keep the channel open
        readings = [reading async for reading in rx]
    return sorted(readings)


async def close_semantics() -> None:
    tx, rx = porto.channel[str](4)
    await tx.send("a")
    await tx.send("b")
    tx.close()
    assert tx.is_closed()
    # Values sent before the close are still delivered, then the channel ends.
    assert await rx.recv() == "a"
    assert [value async for value in rx] == ["b"]
    try:
        await rx.recv()
    except porto.EndOfChannel:
        pass
    else:
        raise AssertionError("recv after the end did not raise EndOfChannel")

    # Closing every receiver breaks the channel for senders: nobody will ever read.
    tx, rx = porto.channel[str](4)
    rx.close()
    assert tx.is_closed()
    try:
        await tx.send("lost")
    except porto.BrokenResourceError:
        pass
    else:
        raise AssertionError("send without receivers did not raise BrokenResourceError")


async def main() -> None:
    log = await backpressure()
    # The producer filled the two slots and was waiting before the consumer started.
    assert log[:3] == ["sent 0", "sent 1", "consumer starts"], log
    assert [entry for entry in log if entry.startswith("got")] == [f"got {i}" for i in range(4)]

    batches = await unbounded_and_batches()
    assert batches == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]], batches

    assert await fan_in() == [f"{name}:{i}" for name in ("humidity", "temp") for i in range(3)]
    await close_semantics()
    print("channels: ok")


if __name__ == "__main__":
    porto.run(main)
