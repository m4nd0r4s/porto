"""scripts/run_loom.py, the runner of the loom CI jobs (51-build-ci-release.md §2): the parts that
read cargo's and the test harness's output and run one model, tested without building any loom
model (the models themselves run in the loom jobs)."""

import importlib.util
import json
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "run_loom", REPOSITORY_ROOT / "scripts" / "run_loom.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_loom"] = module
    spec.loader.exec_module(module)
    return module


run_loom: Any = load_script()


def test_parse_test_list_keeps_the_tests_and_drops_the_benchmarks() -> None:
    listing = "sync::loom_tests::a: test\nsync::loom_tests::b: test\nbench_one: benchmark\n"
    assert run_loom.parse_test_list(listing) == ["sync::loom_tests::a", "sync::loom_tests::b"]


def test_test_executables_picks_the_library_test_binary() -> None:
    def artifact(kind: str, test: bool, executable: str | None) -> str:
        return json.dumps(
            {
                "reason": "compiler-artifact",
                "target": {"kind": [kind]},
                "profile": {"test": test},
                "executable": executable,
            }
        )

    output = "\n".join(
        [
            "Compiling something",
            artifact("lib", False, None),
            artifact("lib", True, "/target/release/deps/porto_core-abc"),
            artifact("bench", True, "/target/release/deps/bench-abc"),
            json.dumps({"reason": "build-finished", "success": True}),
        ]
    )
    assert run_loom.test_executables(output) == ["/target/release/deps/porto_core-abc"]


def run_fake(directory: Path, body: str, deadline: float | None = None) -> Any:
    """Runs ``body`` as the test binary: a launcher that ignores the harness's arguments
    (``--exact NAME --nocapture``) and runs the Python script."""
    script = directory / "model.py"
    script.write_text(body, encoding="utf-8")
    launcher = directory / ("model.cmd" if sys.platform == "win32" else "model.sh")
    if sys.platform == "win32":
        launcher.write_text(f'@"{sys.executable}" "{script}"\n', encoding="utf-8")
    else:
        launcher.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}"\n', encoding="utf-8")
        launcher.chmod(0o755)
    return run_loom.run_model(
        launcher,
        "a::model",
        env={},
        checkpoint=directory / "a.model.json",
        log=directory / "a.model.log",
        trace=directory / "a.model.trace.log",
        deadline=deadline,
    )


def test_a_passing_model_keeps_no_log_and_no_checkpoint(tmp_path: Path) -> None:
    (tmp_path / "a.model.json").write_text("{}", encoding="utf-8")
    outcome = run_fake(tmp_path, "print('ok')")
    assert outcome.status == "passed"
    assert not (tmp_path / "a.model.log").exists()
    assert not (tmp_path / "a.model.json").exists(), "a finished search deletes its checkpoint"


def test_a_failing_model_keeps_its_log_and_a_trace_from_a_second_run(tmp_path: Path) -> None:
    body = (
        "import os, sys\n"
        "print('LOOM_LOG=' + os.environ.get('LOOM_LOG', 'unset'))\n"
        "print('checkpoint=' + os.path.basename(os.environ['LOOM_CHECKPOINT_FILE']))\n"
        "sys.exit(101)\n"
    )
    outcome = run_fake(tmp_path, body)
    assert outcome.status == "failed"
    first = (tmp_path / "a.model.log").read_text(encoding="utf-8")
    trace = (tmp_path / "a.model.trace.log").read_text(encoding="utf-8")
    assert "LOOM_LOG=unset" in first
    assert "checkpoint=a.model.json" in first
    assert "LOOM_LOG=trace" in trace, "the second run turns loom's trace on"


@pytest.mark.skipif(
    sys.platform == "win32", reason="terminating the .cmd launcher leaves its Python child running"
)
def test_a_model_past_the_budget_is_stopped_and_keeps_its_checkpoint(tmp_path: Path) -> None:
    (tmp_path / "a.model.json").write_text("{}", encoding="utf-8")
    outcome = run_fake(tmp_path, "import time\ntime.sleep(60)\n", deadline=time.monotonic() - 1)
    assert outcome.status == "interrupted"
    assert (tmp_path / "a.model.json").exists(), "the next run resumes from it"
