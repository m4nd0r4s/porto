"""Runs the micro-benchmark suite on every Python and writes the result files.

    uv run python bench/micro/run.py [--pythons 3.14+gil,3.11,3.14t] [--select REGEX] [--fast]
        [--select-for 3.14t=REGEX]

For each Python it makes a venv in ``bench/.venvs/py<tag>``, builds a porto wheel for it with
``maturin build --release`` (the workspace's release profile: opt-level 3, fat LTO, one codegen
unit) and installs it with pyperf, uvloop, trio and anyio, then runs ``bench/micro/suite.py``
under pyperf. A warm-up pass (pyperf ``--fast``, discarded) runs first. On the gating Python it
then runs ``bench/micro/memory.py`` (RSS per idle task) per backend. It writes:

- ``bench/results/<date>-<commit>.json``: fingerprint, and per Python per benchmark every
  pyperf value and its mean, median and standard deviation;
- ``bench/results/<date>-<commit>.md``: the report (``bench/micro/report.py``);
- ``bench/results/raw/<date>-<commit>/<tag>.json``: pyperf's own files (not committed).

The run rules of 53-benchmarks.md §1 apply: AC power, Low Power Mode off, nothing else heavy.
"""

from __future__ import annotations

import argparse
import datetime
import importlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import fingerprint
import report

REPOSITORY = Path(__file__).resolve().parents[2]
VENVS = REPOSITORY / "bench" / ".venvs"
RESULTS = REPOSITORY / "bench" / "results"
SUITE = Path(__file__).resolve().parent / "suite.py"
MEMORY = Path(__file__).resolve().parent / "memory.py"
#: The memory probe runs this many times per backend, each in a fresh process; the report shows
#: the median.
MEMORY_REPEATS = 3

#: The gating Python first (ADR-034: CPython 3.14 with the GIL), then the informative ones.
#: "3.14+gil", not "3.14": uv may resolve a bare "3.14" to the free-threaded build.
DEFAULT_PYTHONS = ["3.14+gil", "3.11", "3.14t"]

PORTO_BUILD = (
    "a wheel from `maturin build --release` (Cargo profile `release`: opt-level 3, fat LTO, "
    "codegen-units 1, no PGO), installed non-editable in a venv per Python"
)


def tag_of(python: str) -> str:
    """``3.14+gil`` → ``3.14``; other requests are their own tag (``3.11``, ``3.14t``)."""
    return python.removesuffix("+gil")


def step(message: str) -> None:
    print(f"==> {message}", flush=True)


def run(command: list[str], **options: Any) -> None:
    subprocess.run(command, check=True, **options)


def installed_version(distribution: str) -> str:
    import importlib.metadata

    return importlib.metadata.version(distribution)


def prepare_venv(python: str) -> Path:
    """A fresh venv for ``python`` with a release porto wheel, pyperf, uvloop, trio and anyio."""
    venv = VENVS / f"py{tag_of(python)}"
    step(f"venv {venv.relative_to(REPOSITORY)} for Python {python}")
    run(["uv", "venv", "--quiet", "--clear", "--managed-python", "-p", python, str(venv)])
    interpreter = venv / "bin" / "python"
    # The same pyperf, uvloop, trio and anyio as the dev environment, on every Python.
    run(
        [
            "uv",
            "pip",
            "install",
            "--quiet",
            "--python",
            str(interpreter),
            f"pyperf=={installed_version('pyperf')}",
            f"uvloop=={installed_version('uvloop')}",
            f"trio=={installed_version('trio')}",
            f"anyio=={installed_version('anyio')}",
        ]
    )
    with tempfile.TemporaryDirectory() as wheels:
        step(f"porto release wheel for Python {python}")
        run(
            ["maturin", "build", "--release", "--quiet", "-i", str(interpreter), "--out", wheels],
            cwd=REPOSITORY / "packaging" / "porto",
        )
        (wheel,) = Path(wheels).glob("porto-*.whl")
        run(["uv", "pip", "install", "--quiet", "--python", str(interpreter), str(wheel)])
    return interpreter


def run_suite(interpreter: Path, output: Path | None, select: str, fast: bool) -> None:
    command = [str(interpreter), str(SUITE), "--select", select]
    if fast:
        command.append("--fast")
    if output is None:
        command.append("--quiet")
    else:
        command += ["-o", str(output)]
    # Workers inherit nothing from this venv: pyperf starts them with the venv's interpreter.
    run(command, cwd=REPOSITORY)


def summarize(raw: Path) -> dict[str, dict[str, Any]]:
    """Every benchmark of a pyperf file: its role, values (seconds per op) and statistics."""
    pyperf: Any = importlib.import_module("pyperf")
    suite: Any = pyperf.BenchmarkSuite.load(str(raw))
    found: dict[str, dict[str, Any]] = {}
    for benchmark in suite.get_benchmarks():
        name: str = benchmark.get_name()
        bench, variant = name.split("/", 1)
        values: list[float] = list(benchmark.get_values())
        found[name] = {
            "bench": bench,
            "variant": variant,
            "role": benchmark.get_metadata()["porto_bench_role"],
            "n": len(values),
            "mean": benchmark.mean(),
            "median": benchmark.median(),
            "stdev": benchmark.stdev() if len(values) > 1 else 0.0,
            "values": values,
        }
    return found


def measure_memory(interpreter: Path) -> dict[str, Any]:
    """``memory.py`` per backend, ``MEMORY_REPEATS`` fresh processes each: every run's numbers
    and the median bytes per idle task."""
    found: dict[str, Any] = {}
    for backend in ("porto", "asyncio", "uvloop"):
        runs: list[dict[str, Any]] = []
        for _ in range(MEMORY_REPEATS):
            output = subprocess.run(
                [str(interpreter), str(MEMORY), backend],
                check=True,
                capture_output=True,
                text=True,
                cwd=REPOSITORY,
            ).stdout
            runs.append(json.loads(output))
        per_task = sorted(run["bytes_per_task"] for run in runs)
        found[backend] = {
            "tasks": runs[0]["tasks"],
            "bytes_per_task": per_task[len(per_task) // 2],
            "st11_bytes_per_task": runs[0]["st11_bytes_per_task"],
            "runs": runs,
        }
    return found


def parse_select_for(entries: list[str]) -> dict[str, str]:
    """``--select-for 3.14t=REGEX`` entries as {tag: regex}."""
    found: dict[str, str] = {}
    for entry in entries:
        tag, separator, regex = entry.partition("=")
        if not separator:
            sys.exit(f"--select-for {entry!r}: write it as TAG=REGEX, as in 3.14t=^channel_")
        found[tag_of(tag)] = regex
    return found


def result_name(commit: str | None) -> str:
    base = f"{datetime.date.today().isoformat()}-{(commit or 'nocommit')[:9]}"
    name, attempt = base, 1
    while (RESULTS / f"{name}.json").exists():
        attempt += 1
        name = f"{base}-{attempt}"
    return name


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the micro-benchmark suite.")
    parser.add_argument(
        "--pythons",
        default=",".join(DEFAULT_PYTHONS),
        help="comma-separated uv Python requests; the first one gates (default: %(default)s)",
    )
    parser.add_argument("--select", default="", help="only benchmarks matching this regex")
    parser.add_argument(
        "--select-for",
        action="append",
        default=[],
        metavar="TAG=REGEX",
        help="on that Python only, run the benchmarks matching REGEX instead of --select "
        "(repeatable), as in 3.14t=^channel_cross_thread/",
    )
    parser.add_argument("--no-memory", action="store_true", help="skip the memory probe")
    parser.add_argument(
        "--fast", action="store_true", help="pyperf --fast: a smoke test, not a run that counts"
    )
    parser.add_argument("--skip-build", action="store_true", help="reuse the bench/.venvs")
    parser.add_argument("--no-warmup", action="store_true", help="skip the warm-up pass")
    parser.add_argument(
        "--allow-battery", action="store_true", help="run even if a 53 §1 power rule is broken"
    )
    options = parser.parse_args()
    pythons: list[str] = options.pythons.split(",")
    select_for = parse_select_for(options.select_for)
    select: str = options.select

    power = fingerprint.power_state()
    problems = fingerprint.run_rule_violations(power)
    if problems and not options.allow_battery:
        sys.exit(
            "this run would not count (53-benchmarks.md §1): "
            + "; ".join(problems)
            + ". Fix that, or pass --allow-battery for a run that does not count."
        )
    if shutil.which("maturin") is None:
        sys.exit("maturin is not on PATH: run this with `uv run python bench/micro/run.py`")

    interpreters: dict[str, Path] = {}
    for python in pythons:
        interpreter = VENVS / f"py{tag_of(python)}" / "bin" / "python"
        if not options.skip_build or not interpreter.exists():
            interpreter = prepare_venv(python)
        interpreters[tag_of(python)] = interpreter

    if not options.no_warmup:
        for tag, interpreter in interpreters.items():
            step(f"warm-up pass (pyperf --fast, discarded), Python {tag}")
            run_suite(interpreter, None, select_for.get(tag, select), fast=True)

    toolchain = fingerprint.toolchain(REPOSITORY)
    name = result_name(toolchain["commit"])
    raw_directory = RESULTS / "raw" / name
    raw_directory.mkdir(parents=True, exist_ok=True)
    gating = tag_of(pythons[0])
    result: dict[str, Any] = {
        "schema": 1,
        "name": name,
        "started": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "fast": options.fast,
        # Non-empty only with --allow-battery: the run is indicative and does not count.
        "run_rule_violations": problems,
        "gating_python": gating,
        "machine": fingerprint.machine(),
        "toolchain": toolchain,
        "porto_build": PORTO_BUILD,
        "system_tune": "not applied: pyperf has no tuning operation on macOS "
        "(`pyperf system show`: no operation available for your platform)",
        "poll_cost_ns": fingerprint.poll_cost(interpreters[gating]),
        "busiest_processes": fingerprint.busiest_processes(),
        "load_before": [round(load, 2) for load in os.getloadavg()],
        "pythons": {},
    }
    for tag, interpreter in interpreters.items():
        step(f"suite, Python {tag}")
        raw = raw_directory / f"{tag}.json"
        raw.unlink(missing_ok=True)
        run_suite(interpreter, raw, select_for.get(tag, select), options.fast)
        result["pythons"][tag] = {
            "build": fingerprint.python_build(interpreter),
            "select": select_for.get(tag, select),
            "benchmarks": summarize(raw),
        }
    if not options.no_memory:
        step(f"memory probe (RSS per idle task), Python {gating}")
        result["memory"] = measure_memory(interpreters[gating])
    result["load_after"] = [round(load, 2) for load in os.getloadavg()]
    result["finished"] = datetime.datetime.now().astimezone().isoformat(timespec="seconds")

    summary = RESULTS / f"{name}.json"
    summary.write_text(json.dumps(result, indent=1) + "\n")
    markdown = RESULTS / f"{name}.md"
    markdown.write_text(report.render(result))
    step(f"wrote {summary.relative_to(REPOSITORY)} and {markdown.relative_to(REPOSITORY)}")


if __name__ == "__main__":
    main()
