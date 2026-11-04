"""Renders a micro-benchmark result (``bench/results/<name>.json``) as a Markdown report.

    uv run python bench/micro/report.py bench/results/<name>.json [-o out.md]
    uv run python bench/micro/report.py bench/results/<a>.json --compare bench/results/<b>.json

The first form writes the results table, the M0 gate verdicts (ADR-034) and the fingerprint;
``bench/micro/run.py`` calls it after a run. The second compares two runs, benchmark by
benchmark, to show how far apart they are.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: The M0 kill criterion: porto must be at least this many times faster than asyncio+uvloop on
#: spawn+join and task switch (ADR-034, 54-roadmap.md M0).
M0_SPEEDUP = 2.0
#: M0 allows the §11 absolute targets to be missed by this much (ADR-034).
M0_TARGET_SLACK = 1.3


@dataclass(frozen=True)
class Row:
    bench: str
    title: str
    per: str
    #: The §11 absolute target (11-task-model-and-trap-protocol.md §11), seconds per ``per``.
    target: float | None = None


ROWS = [
    Row("checkpoint", "checkpoint: `await checkpoint()` vs `await asyncio.sleep(0)`", "op", 60e-9),
    Row("switch", "task switch: two tasks yielding in turn", "switch", 250e-9),
    Row("spawn_one", "spawn + run-to-completion of `async def f(): return 1`", "task", 900e-9),
    Row("spawn_join_100k", "spawn+join 100k trivial tasks", "100k tasks"),
    Row("ping_pong", "yield ping-pong (two players, a shared table)", "round trip"),
]

#: The rows the M0 kill criterion is about, as (row, label).
GATE_ROWS = [
    ("spawn_join_100k", "spawn+join 100k trivial tasks"),
    ("switch", "task switch (two tasks yielding)"),
    ("ping_pong", "task switch (yield ping-pong, the 53 §2 row)"),
]

Benchmarks = dict[str, dict[str, Any]]

#: The multiplication sign, spelled out so that ruff does not take it for an "x".
TIMES = "\N{MULTIPLICATION SIGN}"


def format_time(seconds: float) -> str:
    for unit, scale in (("s", 1.0), ("ms", 1e-3), ("µs", 1e-6)):
        if seconds >= scale:
            return f"{seconds / scale:.3g} {unit}"
    return f"{seconds / 1e-9:.3g} ns"


def fastest(benchmarks: Benchmarks, bench: str, loop: str) -> tuple[str, dict[str, Any]] | None:
    """The fastest gating baseline of ``bench`` on ``loop`` ("asyncio" or "uvloop")."""
    candidates = [
        (name, data)
        for name, data in benchmarks.items()
        if data["bench"] == bench
        and data["role"] == "baseline"
        and (data["variant"] == loop or data["variant"].startswith(f"{loop}-"))
    ]
    return min(candidates, key=lambda item: item[1]["mean"], default=None)


def porto_of(benchmarks: Benchmarks, bench: str) -> dict[str, Any] | None:
    return benchmarks.get(f"{bench}/porto")


def _baseline_cell(found: tuple[str, dict[str, Any]] | None, loop: str) -> str:
    if found is None:
        return "—"
    name, data = found
    variant = name.split("/", 1)[1]
    detail = f" ({variant.removeprefix(loop + '-')})" if variant != loop else ""
    return format_time(data["mean"]) + detail


def _ratio(baseline: tuple[str, dict[str, Any]] | None, porto: dict[str, Any]) -> str:
    return "—" if baseline is None else f"{baseline[1]['mean'] / porto['mean']:.1f}{TIMES}"


def results_table(benchmarks: Benchmarks) -> list[str]:
    lines = [
        "| Benchmark | per | porto | asyncio | uvloop | vs uvloop | vs asyncio "
        "| §11 target | within 30% |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in ROWS:
        porto = porto_of(benchmarks, row.bench)
        if porto is None:
            continue
        asyncio_best = fastest(benchmarks, row.bench, "asyncio")
        uvloop_best = fastest(benchmarks, row.bench, "uvloop")
        if row.target is None:
            target, within = "—", "—"
        else:
            target = format_time(row.target)
            within = "yes" if porto["mean"] <= row.target * M0_TARGET_SLACK else "**no**"
        lines.append(
            f"| {row.title} | {row.per} | {format_time(porto['mean'])} "
            f"| {_baseline_cell(asyncio_best, 'asyncio')} "
            f"| {_baseline_cell(uvloop_best, 'uvloop')} "
            f"| {_ratio(uvloop_best, porto)} | {_ratio(asyncio_best, porto)} "
            f"| {target} | {within} |"
        )
    lines.append(
        "| oneshot channel send/recv | — | N/A in M0 (no channels) | | | | | 600 ns | N/A |"
    )
    return lines


def gate_table(benchmarks: Benchmarks) -> list[str]:
    lines = [
        f"| M0 gate (ADR-034): ≥ {M0_SPEEDUP:g}{TIMES} asyncio+uvloop | porto | asyncio+uvloop "
        "| speedup | meets |",
        "|---|---:|---:|---:|---|",
    ]
    for bench, label in GATE_ROWS:
        porto = porto_of(benchmarks, bench)
        uvloop_best = fastest(benchmarks, bench, "uvloop")
        if porto is None or uvloop_best is None:
            lines.append(f"| {label} | — | — | — | not measured |")
            continue
        speedup = uvloop_best[1]["mean"] / porto["mean"]
        meets = "yes" if speedup >= M0_SPEEDUP else "**no**"
        lines.append(
            f"| {label} | {format_time(porto['mean'])} "
            f"| {_baseline_cell(uvloop_best, 'uvloop')} | {speedup:.1f}{TIMES} | {meets} |"
        )
    lines += [
        "",
        f"| M0 gate (ADR-034): §11 targets within {M0_TARGET_SLACK - 1:.0%} | porto "
        "| target | limit | meets |",
        "|---|---:|---:|---:|---|",
    ]
    for row in ROWS:
        porto = porto_of(benchmarks, row.bench)
        if row.target is None or porto is None:
            continue
        limit = row.target * M0_TARGET_SLACK
        meets = "yes" if porto["mean"] <= limit else "**no**"
        lines.append(
            f"| {row.title} | {format_time(porto['mean'])} | {format_time(row.target)} "
            f"| {format_time(limit)} | {meets} |"
        )
    lines.append("| oneshot channel send/recv | N/A in M0 (no channels) | 600 ns | 780 ns | N/A |")
    return lines


def variants_table(benchmarks: Benchmarks) -> list[str]:
    lines = [
        f"| Benchmark | role | mean ± std dev | values | {TIMES} porto |",
        "|---|---|---:|---:|---:|",
    ]
    for name, data in benchmarks.items():
        porto = porto_of(benchmarks, data["bench"])
        relative = "—" if porto is None else f"{data['mean'] / porto['mean']:.2f}"
        lines.append(
            f"| `{name}` | {data['role']} | {format_time(data['mean'])} ± "
            f"{format_time(data['stdev'])} | {data['n']} | {relative} |"
        )
    return lines


def _fingerprint(result: dict[str, Any]) -> list[str]:
    machine = result["machine"]
    power = machine["power"]
    tools = result["toolchain"]
    lines = [
        f"- Machine: {machine['model']}, {machine['cpu']}, {machine['performance_cores']} "
        f"performance + {machine['efficiency_cores']} efficiency cores, "
        f"{machine['memory_gib']} GiB, {machine['os']}",
        f"- Power: {power['source']}, powermode {power['powermode']}, lowpowermode "
        f"{power['lowpowermode']}; load average before {result['load_before']}, after "
        f"{result['load_after']}",
        f"- Busiest processes at the start: {'; '.join(result['busiest_processes'])}",
        f"- porto commit `{tools['commit']}`{' (dirty tree)' if tools['dirty'] else ''}; "
        f"{tools['rustc']}; {tools['uv']}",
        f"- porto build: {result['porto_build']}",
        f"- `pyperf system tune`: {result['system_tune']}",
        "- Poll probe, gating Python (one call; every asyncio/uvloop loop iteration makes one, "
        "porto M0 none): "
        + ", ".join(
            f"{probe.removesuffix('_ns').replace('_', ' ')} {format_time(ns * 1e-9)}"
            for probe, ns in result["poll_cost_ns"].items()
        ),
    ]
    for tag, python in result["pythons"].items():
        build = python["build"]
        lines.append(
            f"- Python {tag}: {build['version'].split()[0]} "
            f"({'free-threaded' if build['free_threaded_build'] else 'GIL'}, "
            f"GIL enabled after imports: {build['gil_enabled_after_imports']}, "
            f"PGO {build['pgo']}, LTO {build['lto']}), `{build['executable']}`; "
            f"porto {build['porto']}, pyperf {build['pyperf']}, uvloop {build['uvloop']}"
        )
    return lines


def render(result: dict[str, Any]) -> str:
    gating = result["gating_python"]
    lines = [
        f"# Micro-benchmarks {result['name']}",
        "",
        "porto vs asyncio (default loop) and asyncio+uvloop, `bench/micro/` "
        "(53-benchmarks.md §2, 11-task-model-and-trap-protocol.md §11, ADR-034). "
        "Times are pyperf means per operation; a ratio is baseline time ÷ porto time, "
        f"so 2.0{TIMES} means porto is twice as fast. "
        "Each baseline column shows the fastest gating variant.",
        "",
    ]
    if result.get("fast"):
        lines += ["**pyperf `--fast` run: not a run that counts (53 §1).**", ""]
    lines += [f"## M0 gate verdict (Python {gating}, the gating Python)", ""]
    lines += gate_table(result["pythons"][gating]["benchmarks"])
    for tag, python in result["pythons"].items():
        role = "gating" if tag == gating else "informative"
        lines += ["", f"## Python {tag} ({role})", ""]
        lines += results_table(python["benchmarks"])
    lines += ["", "## Machine fingerprint", ""]
    lines += _fingerprint(result)
    for tag, python in result["pythons"].items():
        lines += ["", f"## Every variant, Python {tag}", ""]
        lines += variants_table(python["benchmarks"])
    return "\n".join(lines) + "\n"


def render_comparison(first: dict[str, Any], second: dict[str, Any]) -> str:
    lines = [
        f"# Run-to-run spread: {first['name']} vs {second['name']}",
        "",
        "Difference of the second run's mean from the first's, per benchmark.",
        "",
    ]
    for tag, python in first["pythons"].items():
        other = second["pythons"].get(tag)
        if other is None:
            continue
        lines += [
            f"## Python {tag}",
            "",
            "| Benchmark | role | first | second | difference |",
            "|---|---|---:|---:|---:|",
        ]
        spreads: dict[str, list[float]] = {}
        for name, data in python["benchmarks"].items():
            twin = other["benchmarks"].get(name)
            if twin is None:
                continue
            difference = twin["mean"] / data["mean"] - 1
            spreads.setdefault(data["role"], []).append(abs(difference))
            lines.append(
                f"| `{name}` | {data['role']} | {format_time(data['mean'])} "
                f"| {format_time(twin['mean'])} | {difference:+.1%} |"
            )
        lines.append("")
        for role, values in sorted(spreads.items()):
            lines.append(
                f"- {role}: largest difference {max(values):.1%}, "
                f"median {sorted(values)[len(values) // 2]:.1%}"
            )
        speedups: list[str] = []
        for bench, label in GATE_ROWS:
            pair = [
                (porto_of(run["benchmarks"], bench), fastest(run["benchmarks"], bench, "uvloop"))
                for run in (python, other)
            ]
            if any(porto is None or best is None for porto, best in pair):
                continue
            ratios = [best[1]["mean"] / porto["mean"] for porto, best in pair if porto and best]
            speedups.append(f"{label}: {ratios[0]:.1f}{TIMES} then {ratios[1]:.1f}{TIMES}")
        if speedups:
            lines.append(f"- Gate speedups vs uvloop: {'; '.join(speedups)}")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a micro-benchmark result as Markdown.")
    parser.add_argument("result", type=Path, help="a bench/results/<name>.json file")
    parser.add_argument("--compare", type=Path, help="a second result to compare against")
    parser.add_argument("-o", "--output", type=Path, help="write here instead of stdout")
    options = parser.parse_args()
    first = json.loads(options.result.read_text())
    if options.compare is None:
        text = render(first)
    else:
        text = render_comparison(first, json.loads(options.compare.read_text()))
    if options.output is None:
        print(text, end="")
    else:
        options.output.write_text(text)


if __name__ == "__main__":
    main()
