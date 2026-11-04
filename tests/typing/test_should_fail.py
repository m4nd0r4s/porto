"""Pyright strict rejects the samples in should_fail/, so a passing check means something."""

import json
import subprocess
import sys
from pathlib import Path

SHOULD_FAIL = Path(__file__).parent / "should_fail"
REPO_ROOT = Path(__file__).parents[2]


def test_untyped_sample_fails_pyright_strict() -> None:
    completed = subprocess.run(
        # should_fail/pyrightconfig.json extends the root config and undoes its exclude.
        [sys.executable, "-m", "pyright", "--outputjson", "--project", str(SHOULD_FAIL)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )
    report = json.loads(completed.stdout)
    rules = {diagnostic.get("rule") for diagnostic in report["generalDiagnostics"]}
    assert report["summary"]["errorCount"] > 0, (
        "pyright strict accepted tests/typing/should_fail/; check [tool.pyright] in pyproject.toml"
    )
    assert "reportMissingParameterType" in rules, f"expected strict-only rules, got {rules}"
