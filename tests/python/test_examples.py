"""Every example in examples/ runs to completion: each one asserts its own results, so a runtime
change that breaks a documented behavior fails here."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
TIMEOUT_SECONDS = 60


def example_files() -> list[Path]:
    return sorted(EXAMPLES.glob("*.py"))


def test_examples_are_found() -> None:
    assert len(example_files()) >= 10, f"no examples found in {EXAMPLES}"


@pytest.mark.parametrize("example", example_files(), ids=lambda path: path.name)
def test_example_runs(example: Path) -> None:
    # Without PORTO_* variables from the caller's shell, which would change the examples' config.
    env = {name: value for name, value in os.environ.items() if not name.startswith("PORTO_")}
    completed = subprocess.run(
        [sys.executable, str(example)],
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        env=env,
        check=False,
    )
    assert completed.returncode == 0, (
        f"{example.name} exited with {completed.returncode}\n"
        f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    )
    assert completed.stderr == "", f"{example.name} wrote to stderr:\n{completed.stderr}"
