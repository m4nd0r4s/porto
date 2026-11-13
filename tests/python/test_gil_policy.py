"""Lane 0's attachment policy on a GIL build (12-scheduler-and-lanes.md §6, 25 §2 P0/P2).

The lane keeps the GIL across consecutive Python tasks and gives it up only to park, and, while
other Python threads exist, for a moment every ``sys.getswitchinterval()`` of continuous Python
execution. ``_core._gil_stats()`` counts both. Each check runs in its own interpreter, so
threads left behind by other tests do not count as "other threads".
"""

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from porto._config import is_gil_build

REPO = Path(__file__).resolve().parents[2]

# A busy chain: 50 tasks that yield 200 times each, over and over for `seconds`, so the lane
# never parks. Prints (park detaches, periodic releases) the chain caused.
CHAIN = """
import sys, time, threading
import porto
from porto import _core

async def spin(rounds):
    for _ in range(rounds):
        await porto.checkpoint()

async def busy_chain(seconds):
    stop = time.monotonic() + seconds
    while time.monotonic() < stop:
        async with porto.scope() as scope:
            for _ in range(50):
                scope.spawn(spin, 200)
"""


def _run_child(body: str) -> list[str]:
    code = textwrap.dedent(CHAIN) + textwrap.dedent(body)
    child = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        cwd=REPO,
    )
    assert child.returncode == 0, child.stderr
    return child.stdout.split()


def test_the_lane_keeps_the_gil_between_tasks_when_no_other_thread_exists() -> None:
    parks, releases = _run_child(
        """
        async def main():
            before = _core._gil_stats()
            assert threading.active_count() == 1
            await busy_chain(0.15)
            after = _core._gil_stats()
            print(after[0] - before[0], after[1] - before[1])

        porto.run(main)
        """
    )
    assert (parks, releases) == ("0", "0"), "no GIL release between tasks"


@pytest.mark.skipif(not is_gil_build(), reason="a free-threaded build has no GIL to hand over")
def test_a_background_thread_makes_progress_while_lane_zero_runs_a_busy_chain() -> None:
    progress, parks, releases = _run_child(
        """
        ticks = [0]
        stop = threading.Event()

        def background():
            while not stop.is_set():
                ticks[0] += 1

        thread = threading.Thread(target=background)
        thread.start()

        async def main():
            before = _core._gil_stats()
            seen = ticks[0]
            await busy_chain(0.3)
            after = _core._gil_stats()
            print(ticks[0] - seen, after[0] - before[0], after[1] - before[1])

        porto.run(main)
        stop.set()
        thread.join()
        """
    )
    assert int(progress) > 0, "the background thread ran while the lane was busy"
    assert parks == "0", "the lane never parked: it released the GIL for the other thread"
    assert int(releases) > 0


@pytest.mark.skipif(is_gil_build(), reason="the policy applies to GIL builds")
def test_the_periodic_release_is_a_no_op_on_a_free_threaded_build() -> None:
    _, releases = _run_child(
        """
        stop = threading.Event()
        thread = threading.Thread(target=stop.wait)
        thread.start()

        async def main():
            before = _core._gil_stats()
            await busy_chain(0.1)
            after = _core._gil_stats()
            print(after[0] - before[0], after[1] - before[1])

        porto.run(main)
        stop.set()
        thread.join()
        """
    )
    assert releases == "0"


def test_the_lane_detaches_to_park_and_a_thread_runs_meanwhile() -> None:
    ticks, parks = _run_child(
        """
        ticks = [0]
        stop = threading.Event()

        def background():
            while not stop.is_set():
                ticks[0] += 1

        thread = threading.Thread(target=background)
        thread.start()

        async def main():
            before = _core._gil_stats()
            seen = ticks[0]
            await porto.sleep(0.1)
            after = _core._gil_stats()
            print(ticks[0] - seen, after[0] - before[0])

        porto.run(main)
        stop.set()
        thread.join()
        """
    )
    assert int(ticks) > 0
    assert int(parks) >= 1
