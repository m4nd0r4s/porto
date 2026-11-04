"""The machine and toolchain fingerprint every result records (53-benchmarks.md §1).

Each probe that cannot run on this machine records ``None`` instead of failing the run: the
fingerprint describes the run, it does not gate it. The run rules that do gate it (AC power,
Low Power Mode off) are checked by :func:`run_rule_violations`.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

#: Run with the Python under test: its build, the GIL state once porto and uvloop are imported,
#: and the package versions.
_PYTHON_PROBE = r"""
import importlib.metadata, json, platform, sys, sysconfig
import porto
try:
    import uvloop
    uvloop_version = uvloop.__version__
except ImportError:
    uvloop_version = None
config_args = sysconfig.get_config_var("CONFIG_ARGS") or ""
gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
print(json.dumps({
    "executable": sys.executable,
    "version": sys.version,
    "implementation": platform.python_implementation(),
    "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
    "gil_enabled_after_imports": gil_enabled,
    "pgo": "--enable-optimizations" in config_args,
    "lto": next((a.strip("'") for a in config_args.split() if "--with-lto" in a), None),
    "tail_call_interp": "--with-tail-call-interp" in config_args,
    "porto": porto.__version__,
    "pyperf": importlib.metadata.version("pyperf"),
    "uvloop": uvloop_version,
}))
"""

#: The cost of one empty poll, which every asyncio and uvloop loop iteration pays (``kevent`` or
#: ``poll`` with a zero timeout) while porto M0 makes no I/O system call at all.
_POLL_PROBE = r"""
import json, os, select, time
N = 20000
def per_call(function):
    best = float("inf")
    for _ in range(5):
        started = time.perf_counter()
        for _ in range(N):
            function()
        best = min(best, (time.perf_counter() - started) / N)
    return round(best * 1e9, 1)
result = {}
empty = select.kqueue() if hasattr(select, "kqueue") else None
read_end, write_end = os.pipe()
os.write(write_end, b"x")
if empty is not None:
    ready = select.kqueue()
    ready.control([select.kevent(read_end, select.KQ_FILTER_READ, select.KQ_EV_ADD)], 0, 0)
    result["kevent_timeout0_nothing_ready_ns"] = per_call(lambda: empty.control(None, 64, 0))
    result["kevent_timeout0_one_ready_ns"] = per_call(lambda: ready.control(None, 64, 0))
if hasattr(select, "epoll"):
    epoll = select.epoll()
    result["epoll_wait_timeout0_nothing_ready_ns"] = per_call(lambda: epoll.poll(0))
idle_read, _idle_write = os.pipe()
result["select_timeout0_nothing_ready_ns"] = per_call(lambda: select.select([idle_read], [], [], 0))
print(json.dumps(result))
"""


def _output(*command: str) -> str | None:
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip()


def _sysctl(name: str) -> str | None:
    return _output("sysctl", "-n", name)


def _pmset_value(text: str | None, key: str) -> str | None:
    if text is None:
        return None
    match = re.search(rf"^\s*{key}\s+(\S+)", text, re.MULTILINE)
    return match.group(1) if match else None


def power_state() -> dict[str, Any]:
    """The power source and the energy mode (``pmset``; macOS only)."""
    battery = _output("pmset", "-g", "batt")
    settings = _output("pmset", "-g")
    source = None
    if battery is not None:
        match = re.search(r"drawing from '([^']+)'", battery)
        source = match.group(1) if match else None
    return {
        "source": source,
        # Older macOS reports `lowpowermode 0|1`; newer `powermode 0|1|2` (automatic, low, high).
        "lowpowermode": _pmset_value(settings, "lowpowermode"),
        "powermode": _pmset_value(settings, "powermode"),
        "thermal": _output("pmset", "-g", "therm"),
    }


def run_rule_violations(power: dict[str, Any]) -> list[str]:
    """The 53 §1 run rules this machine breaks right now, each with what to do about it."""
    problems: list[str] = []
    if power["source"] is not None and power["source"] != "AC Power":
        problems.append(f"running on {power['source']}: plug the machine into AC power")
    if power["lowpowermode"] == "1" or power["powermode"] == "1":
        problems.append("Low Power Mode is on: turn it off in System Settings > Battery")
    return problems


def busiest_processes(count: int = 5) -> list[str]:
    """The processes using the most CPU right now, to judge "no other heavy process"."""
    listing = _output("ps", "-Ao", "%cpu=,comm=", "-r")
    if listing is None:
        return []
    busiest: list[str] = []
    for line in listing.splitlines()[:count]:
        cpu, _, command = line.strip().partition(" ")
        busiest.append(f"{Path(command.strip()).name} {cpu}%")
    return busiest


def machine() -> dict[str, Any]:
    memory = _sysctl("hw.memsize")
    return {
        "model": _sysctl("hw.model"),
        "cpu": _sysctl("machdep.cpu.brand_string"),
        "logical_cores": os.cpu_count(),
        "performance_cores": _sysctl("hw.perflevel0.physicalcpu"),
        "efficiency_cores": _sysctl("hw.perflevel1.physicalcpu"),
        "memory_gib": int(memory) // 2**30 if memory and memory.isdigit() else None,
        "os": " ".join(
            part
            for part in (
                _output("sw_vers", "-productName"),
                _output("sw_vers", "-productVersion"),
                _output("sw_vers", "-buildVersion"),
            )
            if part
        )
        or _output("uname", "-sr"),
        "kernel": _output("uname", "-rv"),
        "power": power_state(),
    }


def toolchain(repository: Path) -> dict[str, Any]:
    status = _output("git", "-C", str(repository), "status", "--porcelain")
    return {
        "commit": _output("git", "-C", str(repository), "rev-parse", "HEAD"),
        "dirty": bool(status),
        "rustc": _output("rustc", "-V"),
        "uv": _output("uv", "--version"),
        "runner_python": sys.version,
    }


def python_build(python: Path) -> dict[str, Any]:
    """What the Python under test is, with porto and uvloop imported."""
    loaded: dict[str, Any] = json.loads(
        subprocess.run(
            [str(python), "-c", _PYTHON_PROBE], capture_output=True, text=True, check=True
        ).stdout
    )
    return loaded


def poll_cost(python: Path) -> dict[str, float]:
    loaded: dict[str, float] = json.loads(
        subprocess.run(
            [str(python), "-c", _POLL_PROBE], capture_output=True, text=True, check=True
        ).stdout
    )
    return loaded
