"""Channel benchmarks: ``porto.channel`` against ``asyncio.Queue`` doing the same work.

Each porto program and its asyncio twin move the same number of items through a buffer of the
same capacity, with the same number of producers and consumers (53-benchmarks.md §2 "channel
ping-pong (bounded 1)", 16-sync-primitives-and-channels.md §2). asyncio has no closing, so where
the porto consumer ends on the channel's end (``async for``) the asyncio consumer counts items
or reads one sentinel per consumer: one wake-up per item either way.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Coroutine
from typing import Any

import porto
from programs import Benchmark, Main, variants

#: Items per batch of the throughput, fan-in, fan-out and cross-thread rows.
ITEMS = 100_000
#: The buffer of the throughput, fan-in and fan-out rows, and the batch size of ``recv_many``.
CAPACITY = 64
#: Producers of the fan-in row, consumers of the fan-out row.
FAN = 8
#: OS threads sending in the cross-thread row.
THREADS = 4

PortoConsumer = Callable[[porto.Receiver[int], list[int]], Coroutine[Any, Any, None]]
AsyncioConsumer = Callable[[asyncio.Queue[int], int, list[int]], Coroutine[Any, Any, None]]


def _check(received: int, expected: int) -> None:
    if received != expected:
        raise RuntimeError(f"expected {expected} items, received {received}: a task stopped early")


# channel_pingpong: one round trip over two capacity-1 channels, per op -------------------------
# A request goes out on one channel and the echo comes back on the other: two sends, two
# receives and two task switches. Half of it is the §11 "oneshot send/recv between two tasks".


async def porto_echo(requests: porto.Receiver[int], replies: porto.Sender[int], n: int) -> None:
    recv, send = requests.recv, replies.send
    for _ in range(n):
        await send(await recv())


async def porto_channel_pingpong(loops: int) -> float:
    requests_tx, requests_rx = porto.channel[int](1)
    replies_tx, replies_rx = porto.channel[int](1)
    send, recv = requests_tx.send, replies_rx.recv
    elapsed = 0.0
    async with porto.scope() as s:
        s.spawn(porto_echo, requests_rx, replies_tx, loops)
        started = time.perf_counter()
        for i in range(loops):
            await send(i)
            await recv()
        elapsed = time.perf_counter() - started
    return elapsed


async def asyncio_echo(requests: asyncio.Queue[int], replies: asyncio.Queue[int], n: int) -> None:
    get, put = requests.get, replies.put
    for _ in range(n):
        await put(await get())


async def asyncio_channel_pingpong(loops: int) -> float:
    requests: asyncio.Queue[int] = asyncio.Queue(1)
    replies: asyncio.Queue[int] = asyncio.Queue(1)
    put, get = requests.put, replies.get
    async with asyncio.TaskGroup() as group:
        group.create_task(asyncio_echo(requests, replies, loops))
        started = time.perf_counter()
        for i in range(loops):
            await put(i)
            await get()
        elapsed = time.perf_counter() - started
    return elapsed


# channel_throughput(_batch): one producer, one consumer, capacity 64, per item ----------------
# The consumer takes one item per await (``async for`` / ``Queue.get``), or, in the _batch
# row, waits for one and takes up to 64 buffered ones (``recv_many(64)`` / ``get`` then
# ``get_nowait`` while not empty).


async def porto_produce(tx: porto.Sender[int], n: int) -> None:
    async with tx:
        send = tx.send
        for i in range(n):
            await send(i)


async def porto_consume_async_for(rx: porto.Receiver[int], counter: list[int]) -> None:
    received = 0
    async for _ in rx:
        received += 1
    counter[0] += received


async def porto_consume_recv_many(rx: porto.Receiver[int], counter: list[int]) -> None:
    received = 0
    recv_many = rx.recv_many
    try:
        while True:
            received += len(await recv_many(CAPACITY))
    except porto.EndOfChannel:
        pass
    counter[0] += received


def porto_throughput(consume: PortoConsumer) -> Main:
    async def main(loops: int) -> float:
        elapsed = 0.0
        for _ in range(loops):
            tx, rx = porto.channel[int](CAPACITY)
            counter = [0]
            started = time.perf_counter()
            async with porto.scope() as s:
                s.spawn(porto_produce, tx, ITEMS)
                s.spawn(consume, rx, counter)
            elapsed += time.perf_counter() - started
            _check(counter[0], ITEMS)
        return elapsed

    return main


async def asyncio_produce(queue: asyncio.Queue[int], n: int) -> None:
    put = queue.put
    for i in range(n):
        await put(i)


async def asyncio_consume_get(queue: asyncio.Queue[int], n: int, counter: list[int]) -> None:
    get = queue.get
    for _ in range(n):
        await get()
    counter[0] += n


async def asyncio_consume_batch(queue: asyncio.Queue[int], n: int, counter: list[int]) -> None:
    get, get_nowait, empty = queue.get, queue.get_nowait, queue.empty
    received = 0
    while received < n:
        taken = 1
        await get()
        while taken < CAPACITY and not empty():
            get_nowait()
            taken += 1
        received += taken
    counter[0] += received


def asyncio_throughput(consume: AsyncioConsumer) -> Main:
    async def main(loops: int) -> float:
        elapsed = 0.0
        for _ in range(loops):
            queue: asyncio.Queue[int] = asyncio.Queue(CAPACITY)
            counter = [0]
            started = time.perf_counter()
            async with asyncio.TaskGroup() as group:
                group.create_task(asyncio_produce(queue, ITEMS))
                group.create_task(consume(queue, ITEMS, counter))
            elapsed += time.perf_counter() - started
            _check(counter[0], ITEMS)
        return elapsed

    return main


# channel_fan_in: 8 producers (cloned senders) into one consumer, capacity 64, per item --------


async def porto_channel_fan_in(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        tx, rx = porto.channel[int](CAPACITY)
        counter = [0]
        started = time.perf_counter()
        async with porto.scope() as s:
            for _ in range(FAN):
                s.spawn(porto_produce, tx.clone(), ITEMS // FAN)
            tx.close()
            s.spawn(porto_consume_async_for, rx, counter)
        elapsed += time.perf_counter() - started
        _check(counter[0], ITEMS)
    return elapsed


async def asyncio_channel_fan_in(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        queue: asyncio.Queue[int] = asyncio.Queue(CAPACITY)
        counter = [0]
        started = time.perf_counter()
        async with asyncio.TaskGroup() as group:
            for _ in range(FAN):
                group.create_task(asyncio_produce(queue, ITEMS // FAN))
            group.create_task(asyncio_consume_get(queue, ITEMS, counter))
        elapsed += time.perf_counter() - started
        _check(counter[0], ITEMS)
    return elapsed


# channel_fan_out: one producer into 8 consumers (cloned receivers), capacity 64, per item -----


async def porto_channel_fan_out(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        tx, rx = porto.channel[int](CAPACITY)
        counter = [0]
        started = time.perf_counter()
        async with porto.scope() as s:
            s.spawn(porto_produce, tx, ITEMS)
            for _ in range(FAN):
                s.spawn(porto_consume_async_for, rx.clone(), counter)
            rx.close()
        elapsed += time.perf_counter() - started
        _check(counter[0], ITEMS)
    return elapsed


async def asyncio_produce_then_stop(queue: asyncio.Queue[int | None], n: int) -> None:
    """The producer of the fan-out row: ``n`` items, then one ``None`` per consumer."""
    put = queue.put
    for i in range(n):
        await put(i)
    for _ in range(FAN):
        await put(None)


async def asyncio_consume_until_none(queue: asyncio.Queue[int | None], counter: list[int]) -> None:
    get = queue.get
    received = 0
    while await get() is not None:
        received += 1
    counter[0] += received


async def asyncio_channel_fan_out(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        queue: asyncio.Queue[int | None] = asyncio.Queue(CAPACITY)
        counter = [0]
        started = time.perf_counter()
        async with asyncio.TaskGroup() as group:
            group.create_task(asyncio_produce_then_stop(queue, ITEMS))
            for _ in range(FAN):
                group.create_task(asyncio_consume_until_none(queue, counter))
        elapsed += time.perf_counter() - started
        _check(counter[0], ITEMS)
    return elapsed


# channel_cross_thread: 4 OS threads send into one task, per item ------------------------------
# Unbounded on both sides: asyncio has no bounded put that blocks a foreign thread short of a
# future per item (``run_coroutine_threadsafe``), so the twin of ``blocking_send`` is
# ``call_soon_threadsafe(queue.put_nowait, item)``. Starting the 4 threads is inside the time
# on both sides. On 3.14t the threads and the runtime run in parallel.


def porto_send_from_thread(tx: porto.Sender[int], n: int) -> None:
    send = tx.blocking_send
    for i in range(n):
        send(i)
    tx.close()


async def porto_channel_cross_thread(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        tx, rx = porto.unbounded_channel[int]()
        started = time.perf_counter()
        threads = [
            threading.Thread(target=porto_send_from_thread, args=(tx.clone(), ITEMS // THREADS))
            for _ in range(THREADS)
        ]
        tx.close()
        for thread in threads:
            thread.start()
        received = 0
        async for _ in rx:
            received += 1
        elapsed += time.perf_counter() - started
        for thread in threads:
            thread.join()
        _check(received, ITEMS)
    return elapsed


def asyncio_send_from_thread(
    loop: asyncio.AbstractEventLoop, queue: asyncio.Queue[int], n: int
) -> None:
    call_soon_threadsafe, put_nowait = loop.call_soon_threadsafe, queue.put_nowait
    for i in range(n):
        call_soon_threadsafe(put_nowait, i)


async def asyncio_channel_cross_thread(loops: int) -> float:
    loop = asyncio.get_running_loop()
    elapsed = 0.0
    for _ in range(loops):
        queue: asyncio.Queue[int] = asyncio.Queue()
        get = queue.get
        started = time.perf_counter()
        threads = [
            threading.Thread(target=asyncio_send_from_thread, args=(loop, queue, ITEMS // THREADS))
            for _ in range(THREADS)
        ]
        for thread in threads:
            thread.start()
        for _ in range(ITEMS):
            await get()
        elapsed += time.perf_counter() - started
        for thread in threads:
            thread.join()
    return elapsed


BENCHMARKS: list[Benchmark] = [
    *variants("channel_pingpong", porto_channel_pingpong, asyncio_channel_pingpong),
    *variants(
        "channel_throughput",
        porto_throughput(porto_consume_async_for),
        asyncio_throughput(asyncio_consume_get),
        inner_loops=ITEMS,
    ),
    *variants(
        "channel_throughput_batch",
        porto_throughput(porto_consume_recv_many),
        asyncio_throughput(asyncio_consume_batch),
        inner_loops=ITEMS,
    ),
    *variants("channel_fan_in", porto_channel_fan_in, asyncio_channel_fan_in, inner_loops=ITEMS),
    *variants("channel_fan_out", porto_channel_fan_out, asyncio_channel_fan_out, inner_loops=ITEMS),
    # The asyncio loop sleeps in its poll until a thread wakes it: no asyncio-nopoll variant.
    *variants(
        "channel_cross_thread",
        porto_channel_cross_thread,
        asyncio_channel_cross_thread,
        inner_loops=ITEMS,
        nopoll=False,
    ),
]
