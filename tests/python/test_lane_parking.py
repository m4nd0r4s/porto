"""The idle lane parks in the I/O driver (12-scheduler-and-lanes.md §9, 25 P2).

Each ``porto.run`` opens an I/O driver for its ``io_backend``. With nothing to run, the lane
blocks in the driver's turn until its next timer, with its thread state detached: a wake from
another thread interrupts the turn through the driver's waker, and other Python threads run
meanwhile.
"""

import statistics
import subprocess
import sys
import threading
import time
import warnings

import pytest

import porto

#: How long a waker thread waits before it wakes the lane, so the lane is parked by then.
PARK_FIRST = 0.02


async def wake_latency_from_a_thread() -> float:
    """Seconds from a foreign thread's ``Event.set()`` to the woken task running."""
    event = porto.Event()
    set_at: list[float] = []

    def waker() -> None:
        time.sleep(PARK_FIRST)
        set_at.append(time.perf_counter())
        event.set()
        # Holds no GIL from here on, so the lane can take it back at once.
        time.sleep(0.05)

    thread = threading.Thread(target=waker)
    thread.start()
    await event.wait()
    woken_at = time.perf_counter()
    thread.join()
    return woken_at - set_at[0]


def test_a_wake_from_a_foreign_thread_unparks_the_lane_within_a_millisecond() -> None:
    async def main() -> list[float]:
        return [await wake_latency_from_a_thread() for _ in range(7)]

    latencies = porto.run(main)
    # The best of several: a CI machine may deschedule the lane thread for any one of them.
    assert min(latencies) < 0.001, f"wake latencies {latencies}"
    assert statistics.median(latencies) < 0.05, f"wake latencies {latencies}"


def test_a_python_thread_runs_while_the_lane_is_parked() -> None:
    counted = 0
    stop = threading.Event()

    def count() -> None:
        nonlocal counted
        while not stop.is_set():
            counted += 1

    async def main() -> int:
        thread = threading.Thread(target=count)
        thread.start()
        await porto.sleep(0.01)
        before = counted
        # Parked in the driver until the timer: the counting thread has the GIL.
        await porto.sleep(0.2)
        progress = counted - before
        stop.set()
        thread.join()
        return progress

    assert porto.run(main) > 10_000


def test_a_timer_still_ends_the_park_in_the_driver() -> None:
    async def main() -> float:
        started = porto.monotonic()
        await porto.sleep(0.05)
        return porto.monotonic() - started

    assert 0.05 <= porto.run(main) < 2.0


async def nothing() -> None:
    return None


def test_an_unavailable_io_backend_falls_back_with_a_warning() -> None:
    # io_uring is not built yet: every platform falls back to its native backend.
    with pytest.warns(RuntimeWarning, match=r"io_backend='io_uring' is unavailable.*instead"):
        porto.run(nothing, io_backend="io_uring")


def test_an_unavailable_io_backend_is_an_error_when_strict() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(RuntimeError, match=r"io_backend_strict=True forbids falling back"):
            porto.run(nothing, io_backend="io_uring", io_backend_strict=True)


def test_the_native_io_backend_runs_without_a_warning() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        porto.run(nothing, io_backend="auto")


@pytest.mark.skipif(sys.platform == "win32", reason="RLIMIT_NOFILE is POSIX")
def test_running_out_of_file_descriptors_says_what_to_do() -> None:
    # In a child process: with no file descriptor left, the I/O driver cannot open its poller.
    program = """
import os, resource
import porto

async def main():
    return None

free = os.open(os.devnull, os.O_RDONLY)
os.close(free)
_, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (free, hard))
try:
    porto.run(main)
except OSError as error:
    print(type(error).__name__, error)
"""
    result = subprocess.run(
        [sys.executable, "-c", program], capture_output=True, text=True, check=True, timeout=60
    )
    assert "could not open the I/O driver" in result.stdout, result.stdout + result.stderr
    assert "open-file limit" in result.stdout
    assert "never awaited" not in result.stderr
