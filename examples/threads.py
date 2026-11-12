"""Bridging OS threads and async code: ``blocking_send``, ``blocking_recv`` and friends.

    uv run python examples/threads.py

A plain thread (a legacy callback library, a hardware poller, a CPU-bound worker) cannot
``await``. The ``blocking_*`` methods block only that thread, and wake the porto tasks on the
other side. They raise ``RuntimeError`` on a porto lane thread, where blocking would stall
every task; there, await the async form.
"""

import threading

import porto


def sensor_thread(readings: porto.Sender[float], count: int) -> None:
    """A thread that polls a device and pushes readings into async code."""
    for index in range(count):
        readings.blocking_send(20.0 + index / 10)  # blocks while the channel is full
    readings.close()  # the async side's `async for` ends


def logger_thread(lines: porto.Receiver[str], written: list[str], done: porto.Event) -> None:
    """A thread that writes what async code sends it, as a blocking file API would."""
    while True:
        try:
            line = lines.blocking_recv()  # blocks while the channel is empty
        except porto.EndOfChannel:
            break
        written.append(line)
    done.set()  # wakes the task waiting for the thread to finish


async def thread_to_async() -> list[float]:
    # A small capacity: the thread waits whenever the async side falls behind.
    tx, rx = porto.channel[float](2)
    thread = threading.Thread(target=sensor_thread, args=(tx, 10))
    thread.start()
    readings = [reading async for reading in rx]
    thread.join()  # already finished: it closed the channel before the loop above ended
    return readings


async def async_to_thread() -> list[str]:
    tx, rx = porto.channel[str](4)
    written: list[str] = []
    done = porto.Event()
    thread = threading.Thread(target=logger_thread, args=(rx, written, done))
    thread.start()
    async with tx:
        for index in range(20):
            await tx.send(f"line {index}")
    await done.wait()  # do not join() a running thread here: that would block the lane
    thread.join()
    return written


async def thread_waits_for_task() -> str:
    """A thread blocks on an Event and a Notify that tasks fire."""
    ready = porto.Event()
    go = porto.Notify()
    finished = porto.Event()
    trace: list[str] = []

    def helper() -> None:
        ready.blocking_wait()
        trace.append("ready")
        go.blocking_notified()
        trace.append("go")
        finished.set()

    thread = threading.Thread(target=helper)
    thread.start()
    await porto.sleep(0.005)
    assert trace == [], "the thread must wait for the event"
    ready.set()
    go.notify_one()  # stores a permit if the thread is not waiting yet
    await finished.wait()
    thread.join()
    return " ".join(trace)


async def thread_uses_the_other_primitives() -> list[str]:
    """Every primitive with a waiting side has a ``blocking_*`` twin for plain threads."""
    reply_tx, reply_rx = porto.oneshot[str]()
    status_tx, status_rx = porto.watch[str]("starting")
    slots = porto.Semaphore(1)
    seen: list[str] = []
    finished = porto.Event()

    def helper() -> None:
        seen.append(f"reply: {reply_rx.blocking_recv()}")  # blocks until the task replies
        status_rx.blocking_changed()  # blocks until a newer value than "starting" exists
        seen.append(f"status: {status_rx.borrow()}")
        slots.blocking_acquire()  # blocks while a task holds the only permit
        seen.append("got the permit")
        slots.release()
        finished.set()

    await slots.acquire()  # held while the thread starts, released below
    thread = threading.Thread(target=helper)
    thread.start()
    await porto.sleep(0.005)
    assert seen == [], "the thread must wait for the reply"
    reply_tx.send("pong")
    await porto.sleep(0.005)
    status_tx.send("ready")
    await porto.sleep(0.005)
    assert seen == ["reply: pong", "status: ready"], seen  # now blocked on the permit
    slots.release()
    await finished.wait()
    thread.join()
    return seen


async def blocking_calls_refused_on_lane() -> None:
    tx, _rx = porto.channel[int](1)
    try:
        tx.blocking_send(1)
    except RuntimeError as error:
        assert "lane thread" in str(error), error
    else:
        raise AssertionError("blocking_send on a lane thread did not raise")


async def main() -> None:
    readings = await thread_to_async()
    assert readings == [20.0 + index / 10 for index in range(10)], readings
    assert await async_to_thread() == [f"line {index}" for index in range(20)]
    assert await thread_waits_for_task() == "ready go"
    assert await thread_uses_the_other_primitives() == [
        "reply: pong",
        "status: ready",
        "got the permit",
    ]
    await blocking_calls_refused_on_lane()
    print("threads: ok")


if __name__ == "__main__":
    porto.run(main)
