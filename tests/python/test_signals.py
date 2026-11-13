"""``porto.signals.receive`` (18-fs-process-signals.md §3): signals through the driver's
self-pipe, without Python-level handlers."""

import os
import signal
import sys
import threading
import time
from types import FrameType

import pytest

import porto
from porto import _core

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="signals use a self-pipe (Unix)")


def send(signum: int) -> None:
    os.kill(os.getpid(), signum)


@posix_only
def test_a_signal_sent_to_the_process_is_delivered_to_the_iterator() -> None:
    async def main() -> int:
        async with porto.signals.receive(signal.SIGUSR1) as signals:
            send(signal.SIGUSR1)
            async for signum in signals:
                return signum
        raise AssertionError("the iterator ended")

    assert porto.run(main) == signal.SIGUSR1


@posix_only
def test_two_receivers_both_get_the_signal() -> None:
    async def receiver(seen: list[int]) -> None:
        async with porto.signals.receive(signal.SIGUSR1, signal.SIGUSR2) as signals:
            async for signum in signals:
                seen.append(signum)
                return

    async def main() -> tuple[list[int], list[int]]:
        first: list[int] = []
        second: list[int] = []
        async with porto.scope() as scope:
            scope.spawn(receiver, first)
            scope.spawn(receiver, second)
            await porto.sleep(0.1)
            send(signal.SIGUSR2)
        return first, second

    assert porto.run(main) == ([signal.SIGUSR2], [signal.SIGUSR2])


@posix_only
def test_a_signal_raised_from_another_thread_is_still_delivered() -> None:
    async def main() -> list[int]:
        received: list[int] = []
        async with porto.signals.receive(signal.SIGUSR1) as signals:
            for _ in range(3):
                thread = threading.Thread(target=send, args=(signal.SIGUSR1,))
                thread.start()
                thread.join()
                async for signum in signals:
                    received.append(signum)
                    break
        return received

    assert porto.run(main) == [signal.SIGUSR1] * 3


@posix_only
def test_leaving_the_context_restores_the_previous_disposition() -> None:
    seen: list[int] = []

    def handler(signum: int, frame: FrameType | None) -> None:
        seen.append(signum)

    previous = signal.signal(signal.SIGUSR1, handler)
    try:

        async def main() -> None:
            async with porto.signals.receive(signal.SIGUSR1) as signals:
                send(signal.SIGUSR1)
                async for _ in signals:
                    break
            # Back to the Python handler: the signal reaches it, not the receiver.
            send(signal.SIGUSR1)
            time.sleep(0.05)

        porto.run(main)
        assert seen == [signal.SIGUSR1], "the Python handler ran once, after the block"
        assert signal.getsignal(signal.SIGUSR1) is handler
    finally:
        signal.signal(signal.SIGUSR1, previous)


@posix_only
def test_a_receiver_left_open_by_an_abandoned_task_is_undone_by_the_end_of_the_run() -> None:
    seen: list[int] = []

    def handler(signum: int, frame: FrameType | None) -> None:
        seen.append(signum)

    previous = signal.signal(signal.SIGUSR1, handler)
    try:

        async def holder() -> None:
            async with porto.signals.receive(signal.SIGUSR1):
                await porto.sleep(30)

        async def main() -> None:
            porto.runtime().root_scope.spawn(holder)
            await porto.sleep(0.02)

        porto.run(main)
        send(signal.SIGUSR1)
        time.sleep(0.05)
        assert seen == [signal.SIGUSR1]
    finally:
        signal.signal(signal.SIGUSR1, previous)


@posix_only
def test_each_receiver_of_a_signal_restores_only_when_the_last_one_leaves() -> None:
    seen: list[int] = []

    def handler(signum: int, frame: FrameType | None) -> None:
        seen.append(signum)

    previous = signal.signal(signal.SIGUSR1, handler)
    try:

        async def main() -> list[int]:
            got: list[int] = []
            async with porto.signals.receive(signal.SIGUSR1) as outer:
                async with porto.signals.receive(signal.SIGUSR1):
                    pass
                send(signal.SIGUSR1)
                async for signum in outer:
                    got.append(signum)
                    break
            return got

        assert porto.run(main) == [signal.SIGUSR1]
        assert seen == [], "the inner receiver leaving did not give the signal to Python"
    finally:
        signal.signal(signal.SIGUSR1, previous)


@posix_only
def test_a_signal_that_cannot_be_caught_is_refused() -> None:
    async def main() -> None:
        with pytest.raises(ValueError, match="SIGKILL and SIGSTOP"):
            async with porto.signals.receive(signal.SIGKILL):
                pass

    porto.run(main)


def test_receiving_no_signal_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one signal"):
        porto.signals.receive()


@posix_only
def test_receiving_outside_a_runtime_is_refused() -> None:
    with pytest.raises(porto.NoRuntimeError, match=r"signals\.receive"):
        _core._signals_subscribe((signal.SIGUSR1,), print)  # pyright: ignore[reportPrivateUsage]


@pytest.mark.skipif(sys.platform != "win32", reason="Windows only")
def test_windows_says_it_cannot_receive_signals_yet() -> None:
    with pytest.raises(NotImplementedError, match="Windows"):
        porto.signals.receive(signal.SIGINT)
