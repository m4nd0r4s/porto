"""Renders a micro-benchmark result (``bench/results/<name>.json``) as a Markdown report.

    uv run python bench/micro/report.py bench/results/<name>.json [-o out.md]
    uv run python bench/micro/report.py bench/results/<a>.json --compare bench/results/<b>.json

The first form writes the gate verdicts (53 §2, the §11 targets), the memory probe, the results
tables and the fingerprint; ``bench/micro/run.py`` calls it after a run. The second compares
two runs, benchmark by benchmark, to show how far apart they are.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: The variant that is asyncio+uvloop without the empty-kevent cost (see programs.py).
POLL_FREE = "uvloop-readyfd"


@dataclass(frozen=True)
class Row:
    bench: str
    title: str
    per: str
    #: The §11 absolute target (11-task-model-and-trap-protocol.md §11), seconds per ``per``.
    target: float | None = None


@dataclass(frozen=True)
class Gate:
    """A speedup porto must reach over asyncio+uvloop on one row."""

    bench: str
    label: str
    speedup: float
    source: str
    #: Gate on the poll-free uvloop variant instead of the fastest uvloop baseline.
    poll_free: bool = False


@dataclass(frozen=True)
class Target:
    """A §11 absolute target, read from a row's porto mean divided by ``divisor``."""

    bench: str
    label: str
    seconds: float
    divisor: int = 1


ROWS = [
    Row("checkpoint", "checkpoint: `await checkpoint()` vs `await asyncio.sleep(0)`", "op", 60e-9),
    Row("switch", "task switch: two tasks yielding in turn", "switch", 250e-9),
    Row("spawn_one", "spawn + run-to-completion of `async def f(): return 1`", "task", 900e-9),
    Row("spawn_join_100k", "spawn+join 100k trivial tasks", "100k tasks"),
    Row("ping_pong", "yield ping-pong (two players, a shared table)", "round trip"),
    Row("channel_pingpong", "channel ping-pong, capacity 1 (vs `Queue(1)`)", "round trip"),
    Row("channel_throughput", "channel 1→1, capacity 64, `async for` (vs `Queue(64)`)", "item"),
    Row("channel_throughput_batch", "channel 1→1, capacity 64, `recv_many(64)`", "item"),
    Row("channel_fan_in", "channel fan-in 8→1, capacity 64", "item"),
    Row("channel_fan_out", "channel fan-out 1→8 (cloned receivers), capacity 64", "item"),
    Row("channel_cross_thread", "4 OS threads `blocking_send` → one task (unbounded)", "item"),
    Row("lock_uncontended", "Lock acquire+release, uncontended", "op"),
    Row("lock_contended", "Lock, 100 tasks, a yield inside the critical section", "acquisition"),
    Row("mutex_value", "`Mutex[int]` read+set (vs Lock + attribute)", "op"),
    Row("event_wakeup_10k", "Event.set() → 10k waiters resumed", "waiter"),
    Row("notify_handoff", "Notify handoff between two tasks (vs Condition)", "handoff"),
    Row("scope_enter_exit", "empty scope (vs TaskGroup)", "scope"),
    Row("scope_nested_10", "10 nested empty scopes", "nest of 10"),
    Row("cancel_fanout_10k", "cancel a scope of 10k waiting tasks (vs `task.cancel()`)", "task"),
    Row("fail_fast_10k", "one child fails among 10k waiting → ExceptionGroup", "task"),
    Row("sleep_100k", "`sleep(0.001)` x 100k tasks, spawned and joined", "100k tasks"),
    Row("sleep_cancel_churn", "spawn, arm a sleep, cancel (1000 per batch)", "sleep"),
    Row("worker_pool", "worker pool: jobs channel → 8 workers + Lock → results", "job"),
]

#: The speedups the spec asks of porto over asyncio+uvloop (3.14, GIL).
GATES = [
    Gate("spawn_one", "spawn + run-to-completion", 2.5, "§11 comparison row (M1)", poll_free=True),
    Gate("spawn_join_100k", "spawn+join 100k trivial tasks", 3.0, "53 §2"),
    Gate("ping_pong", "task switch (yield ping-pong)", 2.5, "53 §2"),
    Gate("channel_pingpong", "channel ping-pong (bounded 1)", 3.0, "53 §2"),
    Gate("channel_throughput", "channel throughput, `async for`", 3.0, "53 §2 channel"),
    Gate("channel_throughput_batch", "channel throughput, `recv_many`", 3.0, "53 §2 channel"),
    Gate("channel_fan_in", "channel fan-in 8→1", 3.0, "53 §2 channel"),
    Gate("channel_fan_out", "channel fan-out 1→8", 3.0, "53 §2 channel"),
    Gate("lock_contended", "Lock contention 100 tasks", 2.0, "53 §2"),
    Gate("lock_uncontended", "Lock uncontended", 2.0, "53 §2"),
    Gate("mutex_value", "Mutex read+set", 1.8, "53 §2"),
    Gate("sleep_100k", "sleep(0.001) x 100k tasks", 2.0, "53 §2"),
    Gate("sleep_cancel_churn", "sleep arm + cancel churn", 2.0, "53 §2 timers"),
]

#: The §11 absolute targets (M1 must meet them, ADR-034).
TARGETS = [
    Target("checkpoint", "`await porto.checkpoint()`", 60e-9),
    Target("switch", "task switch via `Yield`", 250e-9),
    Target("spawn_one", "spawn + run-to-completion", 900e-9),
    # No oneshot channel yet: half a capacity-1 round trip is one send/recv between two tasks.
    Target("channel_pingpong", "oneshot send/recv (proxy: ½ channel ping-pong)", 600e-9, 2),
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


def poll_free(benchmarks: Benchmarks, bench: str) -> tuple[str, dict[str, Any]] | None:
    name = f"{bench}/{POLL_FREE}"
    data = benchmarks.get(name)
    return None if data is None else (name, data)


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
        "| Benchmark | per | porto | asyncio | uvloop | uvloop poll-free | vs uvloop "
        "| vs poll-free | vs asyncio |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in ROWS:
        porto = porto_of(benchmarks, row.bench)
        if porto is None:
            continue
        asyncio_best = fastest(benchmarks, row.bench, "asyncio")
        uvloop_best = fastest(benchmarks, row.bench, "uvloop")
        free = poll_free(benchmarks, row.bench)
        lines.append(
            f"| {row.title} | {row.per} | {format_time(porto['mean'])} "
            f"| {_baseline_cell(asyncio_best, 'asyncio')} "
            f"| {_baseline_cell(uvloop_best, 'uvloop')} "
            f"| {'—' if free is None else format_time(free[1]['mean'])} "
            f"| {_ratio(uvloop_best, porto)} | {_ratio(free, porto)} "
            f"| {_ratio(asyncio_best, porto)} |"
        )
    return lines


def gate_verdicts(benchmarks: Benchmarks) -> list[tuple[Gate, float | None, float | None]]:
    """Each gate with porto's speedup over the fastest uvloop baseline and over poll-free
    uvloop (``None`` when not measured)."""
    found: list[tuple[Gate, float | None, float | None]] = []
    for gate in GATES:
        porto = porto_of(benchmarks, gate.bench)
        uvloop_best = fastest(benchmarks, gate.bench, "uvloop")
        free = poll_free(benchmarks, gate.bench)
        found.append(
            (
                gate,
                None
                if porto is None or uvloop_best is None
                else uvloop_best[1]["mean"] / porto["mean"],
                None if porto is None or free is None else free[1]["mean"] / porto["mean"],
            )
        )
    return found


def _meets(speedup: float | None, needed: float) -> str:
    if speedup is None:
        return "—"
    return f"{speedup:.1f}{TIMES} " + ("yes" if speedup >= needed else "**no**")


def gate_table(benchmarks: Benchmarks) -> list[str]:
    lines = [
        "| Speedup gate | source | needed | porto | vs uvloop | vs poll-free uvloop | verdict |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for gate, over_uvloop, over_free in gate_verdicts(benchmarks):
        porto = porto_of(benchmarks, gate.bench)
        if porto is None:
            lines.append(
                f"| {gate.label} | {gate.source} | ≥ {gate.speedup:g}{TIMES} | not measured "
                "| | | — |"
            )
            continue
        gating = over_free if gate.poll_free else over_uvloop
        verdict = "—" if gating is None else ("meets" if gating >= gate.speedup else "**miss**")
        against = " (gated on poll-free)" if gate.poll_free else ""
        lines.append(
            f"| {gate.label}{against} | {gate.source} | ≥ {gate.speedup:g}{TIMES} "
            f"| {format_time(porto['mean'])} | {_meets(over_uvloop, gate.speedup)} "
            f"| {_meets(over_free, gate.speedup)} | {verdict} |"
        )
    lines += [
        "",
        "| §11 absolute target (M1) | porto | target | verdict |",
        "|---|---:|---:|---|",
    ]
    for target in TARGETS:
        porto = porto_of(benchmarks, target.bench)
        if porto is None:
            lines.append(f"| {target.label} | not measured | {format_time(target.seconds)} | — |")
            continue
        value = porto["mean"] / target.divisor
        verdict = "meets" if value <= target.seconds else "**miss**"
        lines.append(
            f"| {target.label} | {format_time(value)} | {format_time(target.seconds)} | {verdict} |"
        )
    return lines


def memory_table(memory: dict[str, Any]) -> list[str]:
    lines = [
        "| Backend | idle tasks | RSS per idle task (median of runs) | ST-11 budget | within |",
        "|---|---:|---:|---:|---|",
    ]
    for backend, data in memory.items():
        per_task, budget = data["bytes_per_task"], data["st11_bytes_per_task"]
        within = "yes" if per_task <= budget else "**no**"
        lines.append(
            f"| {backend} | {data['tasks']:,} | {per_task:,.0f} B | {budget:,.0f} B | {within} |"
        )
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
        "porto makes none while tasks are ready): "
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
            f"porto {build['porto']}, pyperf {build['pyperf']}, uvloop {build['uvloop']}, "
            f"trio {build.get('trio')}, anyio {build.get('anyio')}"
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
        "Each baseline column shows the fastest gating variant; *poll-free* is "
        f"`{POLL_FREE}`, uvloop without the empty-kevent cost of an idle poll.",
        "",
    ]
    if result.get("fast"):
        lines += ["**pyperf `--fast` run: not a run that counts (53 §1).**", ""]
    if result.get("run_rule_violations"):
        lines += [
            "**Indicative (battery): this run broke a 53 §1 rule ("
            + "; ".join(result["run_rule_violations"])
            + "). It does not count for a gate; rerun it plugged in.**",
            "",
        ]
    lines += [f"## Gate verdicts (Python {gating}, the gating Python)", ""]
    lines += gate_table(result["pythons"][gating]["benchmarks"])
    if "memory" in result:
        lines += ["", f"## Idle task memory (Python {gating}, `bench/micro/memory.py`)", ""]
        lines += memory_table(result["memory"])
    for tag, python in result["pythons"].items():
        role = "gating" if tag == gating else "informative"
        select = python.get("select")
        only = f", only `{select}`" if select else ""
        lines += ["", f"## Python {tag} ({role}{only})", ""]
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
        for gate in GATES:
            bench, label = gate.bench, gate.label
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
