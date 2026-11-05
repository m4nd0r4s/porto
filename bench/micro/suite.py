"""The pyperf entry point of the micro-benchmark suite: run it with the Python under test.

    <venv>/bin/python bench/micro/suite.py -o out.json [--select REGEX] [pyperf options]

The venv needs porto (a release build), pyperf, uvloop, trio and anyio; ``bench/micro/run.py``
builds those venvs and runs this script once per Python. pyperf re-runs this file in worker
processes, so it is a script, not a module: it imports its siblings (``catalog``,
``programs``) from its own directory.
"""

from __future__ import annotations

import importlib
import importlib.util
import re
import sys
from collections.abc import Callable
from typing import Any, Protocol, cast

from catalog import BENCHMARKS
from programs import Benchmark


class _ArgumentParser(Protocol):
    def add_argument(self, *names: str, **options: Any) -> object: ...


class _Runner(Protocol):
    """The part of ``pyperf.Runner`` this script uses (pyperf ships no type information)."""

    argparser: _ArgumentParser
    metadata: dict[str, object]

    def parse_args(self) -> Any: ...

    def bench_time_func(
        self,
        name: str,
        time_func: Callable[[int], float],
        *,
        inner_loops: int | None = None,
        metadata: dict[str, object] | None = None,
    ) -> object: ...


def _pass_select(command: list[str], options: Any) -> None:
    """Gives pyperf's worker processes the same ``--select`` as the main process."""
    command.extend(("--select", cast(str, options.select)))


def _runnable(benchmark: Benchmark) -> bool:
    return sys.version_info >= benchmark.min_python and all(
        importlib.util.find_spec(module) is not None for module in benchmark.requires
    )


def main() -> None:
    make_runner = cast(Callable[..., _Runner], importlib.import_module("pyperf").Runner)
    runner = make_runner(add_cmdline_args=_pass_select)
    runner.argparser.add_argument(
        "--select",
        default="",
        help="run only the benchmarks whose name (bench/variant) matches this regex",
    )
    options = runner.parse_args()
    selected = re.compile(cast(str, options.select))
    for benchmark in BENCHMARKS:
        if not selected.search(benchmark.name) or not _runnable(benchmark):
            continue
        runner.bench_time_func(
            benchmark.name,
            benchmark.function,
            inner_loops=benchmark.inner_loops,
            metadata={"porto_bench_role": benchmark.role},
        )


if __name__ == "__main__":
    main()
