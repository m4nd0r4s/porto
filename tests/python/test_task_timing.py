"""The task registry's introspection switch (23-observability.md §1, 11 §2 ``TaskStats``): a
step's timing is recorded only while it is on, and the default hot path reads no clock.
"""

from collections.abc import Coroutine
from typing import Any

import porto
from porto import _core


async def _record(timed: list[tuple[int, int, int] | None]) -> None:
    async def worker() -> None:
        for _ in range(5):
            await porto.checkpoint()

    target = _core._spawn_joinable(worker())
    await target.join()
    timed.append(target.timing)


def _run(coro: Coroutine[Any, Any, Any], **overrides: Any) -> None:
    porto.run(coro, **overrides)


def test_a_task_records_no_timing_by_default() -> None:
    timed: list[tuple[int, int, int] | None] = []
    _run(_record(timed))
    assert timed == [None]


def test_debug_mode_turns_the_introspection_switch_on() -> None:
    timed: list[tuple[int, int, int] | None] = []
    _run(_record(timed), debug=True)
    (timing,) = timed
    assert timing is not None
    first_poll, total_poll, last_step_start = timing
    assert 0 < first_poll <= last_step_start, "it started, and its latest step is no earlier"
    assert total_poll > 0, "five steps took some time"
