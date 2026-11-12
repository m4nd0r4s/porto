"""scripts/rule_matrix.py, the traceability matrix of spec rule IDs and test names
(52-testing-strategy.md §3): it reads the IDs from the specs, the claims from test names, and
fails the gate when a checked rule has no test."""

import subprocess
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "scripts" / "rule_matrix.py"

SPEC = """\
# 99 - Example

- **R1 First:** a bullet rule.
- **R2 Second:** another one.

| ID | Rule |
|---|---|
| T1 | a table rule |

```
- **Z9 Not a rule:** inside a fence
```
"""


def make_tree(tmp_path: Path, tests: str) -> tuple[Path, Path]:
    specs = tmp_path / "spec"
    specs.mkdir()
    (specs / "99-example.md").write_text(SPEC, encoding="utf-8")
    suite = tmp_path / "tests"
    (suite / "sub").mkdir(parents=True)
    (suite / "sub" / "test_example.py").write_text(tests, encoding="utf-8")
    return specs, suite


def run(specs: Path, suite: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--specs", str(specs), "--tests", str(suite), *extra],
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_matrix_lists_bullet_and_table_rules_with_their_tests(tmp_path: Path) -> None:
    specs, suite = make_tree(
        tmp_path,
        "def test_R1_first(): ...\n"
        "def test_R1_again(): ...\n"
        "async def test_T1_table(): ...\n"
        "class TestX:\n"
        "    def test_R2_method(self): ...\n",
    )
    result = run(specs, suite)
    assert result.returncode == 0, result.stderr
    assert "R1" in result.stdout
    assert "2 tests" in result.stdout
    assert "sub/test_example.py::test_T1_table" in result.stdout
    assert "Z9" not in result.stdout  # inside a code fence


def test_check_fails_for_a_rule_with_no_test_and_says_what_to_name_it(tmp_path: Path) -> None:
    specs, suite = make_tree(tmp_path, "def test_R1_first(): ...\n")
    result = run(specs, suite, "--check", "99")
    assert result.returncode == 1
    assert "rule R2 has no test; name one test_R2_" in result.stderr
    assert "rule T1 has no test" in result.stderr
    assert "R1 has no test" not in result.stderr


def test_check_passes_when_every_rule_has_a_test(tmp_path: Path) -> None:
    specs, suite = make_tree(
        tmp_path, "def test_R1_a(): ...\ndef test_R2_b(): ...\ndef test_T1_c(): ...\n"
    )
    assert run(specs, suite, "--check", "99").returncode == 0


def test_a_test_named_after_an_undeclared_rule_is_reported(tmp_path: Path) -> None:
    specs, suite = make_tree(tmp_path, "def test_R7_typo(): ...\n")
    result = run(specs, suite)
    assert result.returncode == 0
    assert "names rule R7, which no spec declares" in result.stderr


def test_checking_a_spec_that_declares_no_rules_fails(tmp_path: Path) -> None:
    specs, suite = make_tree(tmp_path, "")
    result = run(specs, suite, "--check", "42")
    assert result.returncode == 1
    assert "spec 42 declares no rule IDs" in result.stderr


def test_a_missing_directory_exits_2(tmp_path: Path) -> None:
    result = run(tmp_path / "nope", tmp_path)
    assert result.returncode == 2


def test_a_rule_whose_only_tests_are_skipped_or_xfailed_does_not_count(tmp_path: Path) -> None:
    specs, suite = make_tree(
        tmp_path,
        "import pytest\n"
        "@pytest.mark.skip(reason='later')\n"
        "def test_R1_skipped(): ...\n"
        "@pytest.mark.xfail(strict=True)\n"
        "def test_R2_xfailed(): ...\n"
        "@pytest.mark.skipif(True, reason='never')\n"
        "def test_T1_skipif(): ...\n",
    )
    result = run(specs, suite, "--check", "99")
    assert result.returncode == 1
    for rule in ("R1", "R2", "T1"):
        assert f"rule {rule} has no test" in result.stderr


def test_marks_on_a_class_or_the_module_exclude_its_tests_too(tmp_path: Path) -> None:
    specs, suite = make_tree(
        tmp_path,
        "import pytest\n"
        "@pytest.mark.xfail\n"
        "class TestAll:\n"
        "    def test_R1_method(self): ...\n"
        "class TestSome:\n"
        "    @pytest.mark.skip\n"
        "    def test_R2_skipped(self): ...\n"
        "    def test_T1_runs(self): ...\n",
    )
    result = run(specs, suite, "--check", "99")
    assert result.returncode == 1
    assert "rule R1 has no test" in result.stderr
    assert "rule R2 has no test" in result.stderr
    assert "rule T1 has no test" not in result.stderr
    (suite / "sub" / "test_example.py").write_text(
        "import pytest\n"
        "pytestmark = [pytest.mark.skip]\n"
        "def test_R1_a(): ...\n"
        "def test_R2_b(): ...\n"
        "def test_T1_c(): ...\n",
        encoding="utf-8",
    )
    assert "rule T1 has no test" in run(specs, suite, "--check", "99").stderr


def test_a_test_that_can_run_still_counts_next_to_marked_ones(tmp_path: Path) -> None:
    specs, suite = make_tree(
        tmp_path,
        "import pytest\n"
        "@pytest.mark.parametrize('x', [1])\n"
        "def test_R1_runs(x): ...\n"
        "@pytest.mark.skipif(False, reason='runs')\n"
        "def test_R2_runs(): ...\n"
        "def test_T1_runs(): ...\n"
        "@pytest.mark.xfail\n"
        "def test_T1_also_xfailed(): ...\n",
    )
    assert run(specs, suite, "--check", "99").returncode == 0


def test_tests_inside_module_level_if_try_and_with_blocks_count(tmp_path: Path) -> None:
    # pytest collects them, so a rule whose only test sits in such a block is checked.
    specs, suite = make_tree(
        tmp_path,
        "import sys\n"
        "if sys.platform != 'never':\n"
        "    def test_R1_in_if(): ...\n"
        "try:\n"
        "    def test_R2_in_try(): ...\n"
        "except ImportError:\n"
        "    pass\n"
        "with open(__file__):\n"
        "    def test_T1_in_with(): ...\n",
    )
    assert run(specs, suite, "--check", "99").returncode == 0
