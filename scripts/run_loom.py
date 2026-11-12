"""Runs the loom models of a crate one process each, the way the loom CI jobs do.

51-build-ci-release.md §2 (loom) and 52-testing-strategy.md item 2. Locally the whole suite is
``RUSTFLAGS="--cfg loom" cargo test -p porto-core --release``; this script is the same models
with what CI needs on top:

* **A process per model**, so each has its own ``LOOM_CHECKPOINT_FILE`` (loom writes one file per
  search; tests sharing a process would overwrite each other's) and a failure names its model.
* **A trace on failure**: the model is run again from its last checkpoint (at most
  ``--checkpoint-interval`` iterations before the failing one) with ``LOOM_LOG=trace`` and
  ``LOOM_LOCATION=1``, and the output is written to ``--trace-dir/<model>.trace.log`` next to the
  first run's ``<model>.log``. CI uploads that directory.
* **Resume** (nightly): with ``--checkpoint-dir`` the checkpoints survive the run, so a search
  that did not finish within ``--budget`` seconds continues where it stopped the next time; a
  model that finishes deletes its checkpoint.

    uv run python scripts/run_loom.py --max-preemptions 2                    # the PR job
    uv run python scripts/run_loom.py --checkpoint-dir ckpt --budget 18000   # the nightly job

Exit status: 0 when every model passed (or the budget ran out with none failed), 1 when a model
failed, 2 when the models could not be built or listed.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Outcome:
    """How one model ended: ``passed``, ``failed`` or ``interrupted`` (the budget ran out)."""

    name: str
    status: str
    seconds: float


def build_test_binary(package: str) -> Path:
    """Builds the library tests of ``package`` with ``--cfg loom`` and returns the executable."""
    flags = f"--cfg loom {os.environ.get('RUSTFLAGS', '')}".strip()
    command = ["cargo", "test", "-p", package, "--release", "--lib", "--no-run"]
    result = subprocess.run(
        [*command, "--message-format=json"],
        cwd=REPOSITORY_ROOT,
        env=os.environ | {"RUSTFLAGS": flags},
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(f"run_loom: `{' '.join(command)}` failed (status {result.returncode})")
        raise SystemExit(2)
    executables = test_executables(result.stdout)
    if len(executables) != 1:
        print(f"run_loom: expected one library test binary, found {executables}")
        raise SystemExit(2)
    return Path(executables[0])


def test_executables(cargo_json: str) -> list[str]:
    """The library test executables in the ``--message-format=json`` output of ``cargo test``."""
    executables: list[str] = []
    for line in cargo_json.splitlines():
        if not line.startswith("{"):
            continue
        message = json.loads(line)
        if (
            message.get("reason") == "compiler-artifact"
            and message.get("executable")
            and message.get("profile", {}).get("test")
            and "lib" in message["target"]["kind"]
        ):
            executables.append(message["executable"])
    return executables


def parse_test_list(listing: str) -> list[str]:
    """The test names of ``<binary> --list --format terse`` (``name: test`` lines)."""
    return [line[: -len(": test")] for line in listing.splitlines() if line.endswith(": test")]


def run_model(
    binary: Path,
    name: str,
    *,
    env: dict[str, str],
    checkpoint: Path,
    log: Path,
    trace: Path,
    deadline: float | None,
) -> Outcome:
    """Runs one model; on failure runs it again with loom's trace on."""
    environment = env | {"LOOM_CHECKPOINT_FILE": checkpoint.as_posix()}
    command = [binary.as_posix(), "--exact", name, "--nocapture"]
    started = time.monotonic()
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            command, env=environment, stdout=output, stderr=subprocess.STDOUT, cwd=REPOSITORY_ROOT
        )
        while True:
            try:
                status = process.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                if deadline is not None and time.monotonic() > deadline:
                    process.terminate()
                    process.wait()
                    return Outcome(name, "interrupted", time.monotonic() - started)
    seconds = time.monotonic() - started
    if status == 0:
        log.unlink()  # only the logs of failures are kept
        checkpoint.unlink(missing_ok=True)
        return Outcome(name, "passed", seconds)
    traced = environment | {
        "LOOM_LOG": "trace",
        "LOOM_LOCATION": "1",
        "RUST_BACKTRACE": "1",
        "NO_COLOR": "1",
    }
    with trace.open("w", encoding="utf-8") as output:
        subprocess.run(
            command,
            env=traced,
            stdout=output,
            stderr=subprocess.STDOUT,
            cwd=REPOSITORY_ROOT,
            check=False,
        )
    return Outcome(name, "failed", seconds)


def main() -> int:
    summary = (__doc__ or "").split("\n\n", maxsplit=1)[0]
    parser = argparse.ArgumentParser(description=summary)
    parser.add_argument("--package", default="porto-core", help="the crate (default porto-core)")
    parser.add_argument("--filter", default="", help="only the tests whose name contains this")
    parser.add_argument(
        "--max-preemptions", type=int, help="LOOM_MAX_PREEMPTIONS (models cap themselves lower)"
    )
    parser.add_argument("--checkpoint-dir", type=Path, help="keep checkpoints here to resume")
    parser.add_argument("--checkpoint-interval", type=int, default=100)
    parser.add_argument("--trace-dir", type=Path, default=Path("loom-trace"))
    parser.add_argument("--budget", type=float, help="seconds after which to stop (exit 0)")
    parser.add_argument("--jobs", type=int, default=os.cpu_count() or 1)
    arguments = parser.parse_args()

    binary = build_test_binary(arguments.package)
    listing = subprocess.run(
        [binary.as_posix(), "--list", "--format", "terse"],
        capture_output=True,
        text=True,
        check=True,
        cwd=REPOSITORY_ROOT,
    ).stdout
    names = [name for name in parse_test_list(listing) if arguments.filter in name]
    if not names:
        print(f"run_loom: no test of {arguments.package} matches {arguments.filter!r}")
        return 2

    arguments.trace_dir = arguments.trace_dir.resolve()
    arguments.trace_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as scratch:
        checkpoints = Path(arguments.checkpoint_dir or scratch).resolve()
        checkpoints.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ) | {"LOOM_CHECKPOINT_INTERVAL": str(arguments.checkpoint_interval)}
        if arguments.max_preemptions is not None:
            env["LOOM_MAX_PREEMPTIONS"] = str(arguments.max_preemptions)
        deadline = time.monotonic() + arguments.budget if arguments.budget else None

        def one(name: str) -> Outcome:
            stem = name.replace("::", ".")
            outcome = run_model(
                binary,
                name,
                env=env,
                checkpoint=checkpoints / f"{stem}.json",
                log=arguments.trace_dir / f"{stem}.log",
                trace=arguments.trace_dir / f"{stem}.trace.log",
                deadline=deadline,
            )
            print(f"{outcome.status:<11} {outcome.seconds:7.1f}s  {name}", flush=True)
            return outcome

        with ThreadPoolExecutor(max_workers=arguments.jobs) as pool:
            outcomes = list(pool.map(one, names))
    failed = [outcome for outcome in outcomes if outcome.status == "failed"]
    interrupted = [outcome for outcome in outcomes if outcome.status == "interrupted"]
    print(
        f"run_loom: {len(outcomes) - len(failed) - len(interrupted)} passed, "
        f"{len(failed)} failed, {len(interrupted)} interrupted by the budget"
    )
    for outcome in failed:
        print(f"run_loom: FAILED {outcome.name}; trace in {arguments.trace_dir.as_posix()}")
    write_step_summary(arguments.package, outcomes)
    return 1 if failed else 0


def write_step_summary(package: str, outcomes: list[Outcome]) -> None:
    """Appends the run's result to the GitHub Actions job summary, when there is one."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    slowest = sorted(outcomes, key=lambda outcome: outcome.seconds, reverse=True)[:5]
    lines = [f"### loom models of `{package}`", ""]
    for status in ("passed", "failed", "interrupted"):
        count = sum(outcome.status == status for outcome in outcomes)
        lines.append(f"- {status}: {count}")
    lines += ["", "Slowest models:", ""]
    lines += [
        f"- `{outcome.name}`: {outcome.seconds:.1f}s ({outcome.status})" for outcome in slowest
    ]
    lines += ["", ""]
    with open(path, "a", encoding="utf-8") as summary:
        summary.write("\n".join(lines))


if __name__ == "__main__":
    sys.exit(main())
