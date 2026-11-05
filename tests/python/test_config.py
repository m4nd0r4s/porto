"""``porto.RuntimeConfig``: its fields and defaults (20-python-api-reference.md), the
``PORTO_*`` environment variables and the precedence of the sources
(26-errors-config-lifecycle.md §1), and ``lanes="auto"`` (12-scheduler-and-lanes.md §7).
"""

import dataclasses
import os
import subprocess
import sys
import sysconfig
import warnings
from typing import Any

import pytest

import porto
from porto import _config


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """No ``PORTO_*`` variable from the outer environment leaks into a test."""
    for name in list(os.environ):
        if name.startswith("PORTO_"):
            monkeypatch.delenv(name)


async def answer() -> int:
    return 42


# Fields and defaults

DEFAULTS: dict[str, object] = {
    "lanes": 1,
    "io_backend": "auto",
    "io_backend_strict": False,
    "steal": "adaptive",
    "python_steal": "adaptive",
    "python_steal_threshold": 8,
    "coop_budget": 128,
    "blocking_threads": 512,
    "blocking_keepalive": 10.0,
    "timer_resolution": 0.001,
    "asyncio_compat": True,
    "anyio_backend": "native",
    "handle_signals": True,
    "debug": bool(sys.flags.dev_mode),
    "slow_step_threshold": 0.1,
    "console": None,
    "metrics": True,
    "deterministic": None,
    "graceful_timeout": 10.0,
    "cancel_timeout": 5.0,
}


def test_the_fields_and_defaults_are_the_reference_table() -> None:
    assert dataclasses.asdict(porto.RuntimeConfig()) == DEFAULTS
    assert tuple(DEFAULTS) == _config.FIELD_NAMES


def test_the_config_is_frozen() -> None:
    config = porto.RuntimeConfig()
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.lanes = 2  # pyright: ignore[reportAttributeAccessIssue]


def test_fields_are_keyword_only() -> None:
    with pytest.raises(TypeError):
        porto.RuntimeConfig(2)  # pyright: ignore[reportCallIssue]


def test_every_field_but_deterministic_has_an_environment_variable() -> None:
    expected = {f"PORTO_{name.upper()}" for name in DEFAULTS if name != "deterministic"}
    assert expected | {"PORTO_SEED"} == _config.ENV_NAMES


# Precedence: overrides > RuntimeConfig fields > PORTO_* > defaults

PRECEDENCE = [
    # (PORTO_COOP_BUDGET, RuntimeConfig(coop_budget=...), override coop_budget=..., expected)
    (None, None, None, 128),
    ("64", None, None, 64),
    (None, 32, None, 32),
    ("64", 32, None, 32),
    (None, None, 16, 16),
    ("64", None, 16, 16),
    (None, 32, 16, 16),
    ("64", 32, 16, 16),
]


@pytest.mark.parametrize(("env", "field", "override", "expected"), PRECEDENCE)
def test_precedence(
    monkeypatch: pytest.MonkeyPatch,
    env: str | None,
    field: int | None,
    override: int | None,
    expected: int,
) -> None:
    if env is not None:
        monkeypatch.setenv("PORTO_COOP_BUDGET", env)
    config = None if field is None else porto.RuntimeConfig(coop_budget=field)
    overrides = {} if override is None else {"coop_budget": override}
    assert _config.resolve(config, overrides).coop_budget == expected


def test_a_config_keeps_its_other_fields_under_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTO_STEAL", "eager")
    config = porto.RuntimeConfig(lanes=3)
    resolved = _config.resolve(config, {"coop_budget": 7})
    assert (resolved.lanes, resolved.steal, resolved.coop_budget) == (3, "eager", 7)


def test_the_environment_is_read_when_the_config_is_created(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = porto.RuntimeConfig()
    monkeypatch.setenv("PORTO_COOP_BUDGET", "64")
    assert _config.resolve(config, {}).coop_budget == 128
    assert _config.resolve(None, {}).coop_budget == 64


def test_an_explicit_field_does_not_read_a_bad_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PORTO_LANES", "many")
    assert porto.RuntimeConfig(lanes=2).lanes == 2


def test_run_accepts_a_config_and_overrides() -> None:
    config = porto.RuntimeConfig(timer_resolution=0.005)
    assert porto.run(answer, config=config) == 42
    assert porto.run(answer, config=config, timer_resolution=0.0001, lanes="auto") == 42
    assert porto.run(answer(), lanes=2) == 42


# Parsing each type from the environment

PARSED = [
    # ints
    ("PORTO_COOP_BUDGET", "64", "coop_budget", 64),
    ("PORTO_BLOCKING_THREADS", " 4 ", "blocking_threads", 4),
    ("PORTO_PYTHON_STEAL_THRESHOLD", "1_000", "python_steal_threshold", 1000),
    # lanes: an int or auto
    ("PORTO_LANES", "4", "lanes", 4),
    ("PORTO_LANES", "auto", "lanes", "auto"),
    ("PORTO_LANES", "AUTO", "lanes", "auto"),
    # floats
    ("PORTO_GRACEFUL_TIMEOUT", "2.5", "graceful_timeout", 2.5),
    ("PORTO_CANCEL_TIMEOUT", "0", "cancel_timeout", 0.0),
    ("PORTO_BLOCKING_KEEPALIVE", "1e1", "blocking_keepalive", 10.0),
    ("PORTO_TIMER_RESOLUTION", "0.0001", "timer_resolution", 0.0001),
    ("PORTO_SLOW_STEP_THRESHOLD", "0.25", "slow_step_threshold", 0.25),
    ("PORTO_SLOW_STEP_THRESHOLD", "none", "slow_step_threshold", None),
    # bools
    *[("PORTO_DEBUG", text, "debug", True) for text in ("1", "true", "yes", "TRUE", "Yes")],
    *[("PORTO_METRICS", text, "metrics", False) for text in ("0", "false", "no", "False", "NO")],
    ("PORTO_IO_BACKEND_STRICT", "yes", "io_backend_strict", True),
    ("PORTO_ASYNCIO_COMPAT", "0", "asyncio_compat", False),
    ("PORTO_HANDLE_SIGNALS", "false", "handle_signals", False),
    # enums by lowercase name
    ("PORTO_IO_BACKEND", "io_uring", "io_backend", "io_uring"),
    ("PORTO_IO_BACKEND", "KQUEUE", "io_backend", "kqueue"),
    ("PORTO_STEAL", "off", "steal", "off"),
    ("PORTO_PYTHON_STEAL", "off", "python_steal", "off"),
    ("PORTO_ANYIO_BACKEND", "facade", "anyio_backend", "facade"),
    # strings
    ("PORTO_CONSOLE", "127.0.0.1:6669", "console", "127.0.0.1:6669"),
    ("PORTO_CONSOLE", "[::1]:6669", "console", "[::1]:6669"),
    ("PORTO_CONSOLE", "None", "console", None),
]


@pytest.mark.parametrize(("variable", "text", "field", "expected"), PARSED)
def test_environment_values_parse(
    monkeypatch: pytest.MonkeyPatch, variable: str, text: str, field: str, expected: object
) -> None:
    monkeypatch.setenv(variable, text)
    value = getattr(porto.RuntimeConfig(), field)
    assert value == expected
    assert type(value) is type(expected)


INVALID_ENV = [
    # (variable, text, what the message says is expected, the fix it suggests)
    ("PORTO_LANES", "0", "an integer >= 1, or 'auto'", "PORTO_LANES=4"),
    ("PORTO_LANES", "many", "an integer >= 1, or auto", "PORTO_LANES=4"),
    ("PORTO_LANES", "", "an integer >= 1, or auto", "PORTO_LANES=4"),
    ("PORTO_COOP_BUDGET", "1.5", "an integer", "PORTO_COOP_BUDGET=4"),
    ("PORTO_COOP_BUDGET", "0", "an integer >= 1", "PORTO_COOP_BUDGET=128"),
    ("PORTO_BLOCKING_THREADS", "-1", "an integer >= 1", "PORTO_BLOCKING_THREADS=64"),
    ("PORTO_GRACEFUL_TIMEOUT", "soon", "a number of seconds", "PORTO_GRACEFUL_TIMEOUT=0.5"),
    ("PORTO_GRACEFUL_TIMEOUT", "-1", "a finite number of seconds >= 0", "=10"),
    ("PORTO_CANCEL_TIMEOUT", "inf", "a finite number of seconds >= 0", "=5"),
    ("PORTO_BLOCKING_KEEPALIVE", "nan", "a finite number of seconds >= 0", "=10"),
    ("PORTO_TIMER_RESOLUTION", "0.05", "a number of seconds between 0.0001 and 0.01", "=0.001"),
    ("PORTO_SLOW_STEP_THRESHOLD", "0", "a positive, finite number of seconds, or None", "=0.5"),
    ("PORTO_DEBUG", "on", "one of 1, 0, true, false, yes, no", "PORTO_DEBUG=1"),
    ("PORTO_METRICS", "2", "one of 1, 0, true, false, yes, no", "PORTO_METRICS=1"),
    ("PORTO_IO_BACKEND", "select", "one of 'auto', 'epoll', 'kqueue', 'iocp'", "=auto"),
    ("PORTO_STEAL", "always", "one of 'adaptive', 'off', 'eager'", "PORTO_STEAL=adaptive"),
    ("PORTO_ANYIO_BACKEND", "asyncio", "one of 'native', 'facade', 'off'", "=native"),
    ("PORTO_CONSOLE", "6669", "a 'host:port' address", "PORTO_CONSOLE=127.0.0.1:6669"),
    ("PORTO_CONSOLE", "localhost:99999", "a 'host:port' address", "=127.0.0.1:6669"),
]


@pytest.mark.parametrize(("variable", "text", "expected", "fix"), INVALID_ENV)
def test_invalid_environment_values_say_what_to_set(
    monkeypatch: pytest.MonkeyPatch, variable: str, text: str, expected: str, fix: str
) -> None:
    monkeypatch.setenv(variable, text)
    with pytest.raises(ValueError, match="porto: ") as caught:
        porto.RuntimeConfig()
    message = str(caught.value)
    assert f"{variable}={text!r} is not valid" in message
    assert f"expected {expected}" in message
    assert fix in message
    assert "or unset it for the default" in message


def test_run_raises_for_an_invalid_environment_before_calling_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[bool] = []

    async def main() -> None:
        called.append(True)

    monkeypatch.setenv("PORTO_LANES", "zero")
    with pytest.raises(ValueError, match="PORTO_LANES='zero' is not valid"):
        porto.run(main)
    assert called == []


# Invalid values in code

INVALID_FIELDS: list[tuple[dict[str, Any], type[Exception], str]] = [
    ({"lanes": 0}, ValueError, "expected an integer >= 1, or 'auto'; pass lanes=4"),
    ({"lanes": "AUTO"}, TypeError, "expected an int or 'auto', got str"),
    ({"lanes": True}, TypeError, "expected an int or 'auto', got bool"),
    ({"lanes": 2.0}, TypeError, "expected an int or 'auto', got float"),
    ({"coop_budget": 0}, ValueError, "pass coop_budget=128"),
    ({"python_steal_threshold": -3}, ValueError, "pass python_steal_threshold=8"),
    ({"blocking_threads": "8"}, TypeError, "expected an int, got str"),
    ({"timer_resolution": 0.00001}, ValueError, "0.01; pass timer_resolution=0.001"),
    ({"graceful_timeout": -1}, ValueError, ">= 0; pass graceful_timeout=10"),
    ({"cancel_timeout": float("inf")}, ValueError, "pass cancel_timeout=5"),
    ({"blocking_keepalive": None}, TypeError, "expected a number of seconds, got NoneType"),
    ({"slow_step_threshold": 0}, ValueError, "or None to turn it off; pass slow_step_threshold"),
    ({"steal": "Eager"}, ValueError, "'eager'; pass steal='adaptive'"),
    ({"io_backend": None}, TypeError, "expected one of 'auto', 'epoll'"),
    ({"debug": 1}, TypeError, "expected a bool, got int"),
    ({"console": "localhost"}, ValueError, "pass console='127.0.0.1:6669'"),
    ({"deterministic": {"seed": 1}}, TypeError, "expected a porto.DeterministicConfig or None"),
]


@pytest.mark.parametrize(("fields", "error", "fragment"), INVALID_FIELDS)
def test_invalid_fields_say_what_to_pass(
    fields: dict[str, Any], error: type[Exception], fragment: str
) -> None:
    with pytest.raises(error) as caught:
        porto.RuntimeConfig(**fields)
    message = str(caught.value)
    assert message.startswith(f"porto: RuntimeConfig {next(iter(fields))}=")
    assert fragment in message


@pytest.mark.parametrize(("fields", "error", "fragment"), INVALID_FIELDS)
def test_invalid_overrides_raise_from_run(
    fields: dict[str, Any], error: type[Exception], fragment: str
) -> None:
    with pytest.raises(error, match="porto: RuntimeConfig") as caught:
        porto.run(answer, **fields)
    assert fragment in str(caught.value)


def test_float_fields_take_ints_and_store_floats() -> None:
    config = porto.RuntimeConfig(graceful_timeout=3, slow_step_threshold=1)
    assert type(config.graceful_timeout) is float
    assert type(config.slow_step_threshold) is float
    assert config.graceful_timeout == 3.0


def test_an_unknown_override_lists_the_valid_names() -> None:
    with pytest.raises(TypeError, match="unknown configuration field") as caught:
        porto.run(answer, lane=2)
    message = str(caught.value)
    assert "field(s) lane;" in message
    assert ", ".join(DEFAULTS) in message


def test_a_config_of_the_wrong_type_says_what_to_pass() -> None:
    with pytest.raises(TypeError, match=r"config must be a porto\.RuntimeConfig or None, got dict"):
        porto.run(answer, config={"lanes": 2})  # pyright: ignore[reportArgumentType]


# Typo protection


def test_an_unknown_variable_warns_with_the_valid_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTO_LANS", "4")
    with pytest.warns(RuntimeWarning) as caught:
        assert porto.run(answer) == 42
    [warning] = caught
    message = str(warning.message)
    assert "unknown environment variable PORTO_LANS; did you mean PORTO_LANES?" in message
    assert ", ".join(sorted(_config.ENV_NAMES)) in message
    assert warning.filename == __file__


def test_each_unknown_variable_warns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTO_ZZZ", "1")
    monkeypatch.setenv("PORTO_COOP_BUGDET", "1")
    with pytest.warns(RuntimeWarning) as caught:
        _config.warn_unknown_env()
    messages = [str(warning.message) for warning in caught]
    assert len(messages) == 2
    assert "PORTO_COOP_BUGDET; did you mean PORTO_COOP_BUDGET?" in messages[0]
    assert "PORTO_ZZZ; valid names:" in messages[1]


def test_known_variables_do_not_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _config.ENV_NAMES:
        monkeypatch.setenv(name, "1")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _config.warn_unknown_env()


# Deterministic mode and PORTO_SEED


def test_porto_seed_overrides_the_deterministic_seed(monkeypatch: pytest.MonkeyPatch) -> None:
    config = porto.RuntimeConfig(deterministic=porto.DeterministicConfig(seed=1))
    assert _config.resolve(config, {}).deterministic == porto.DeterministicConfig(seed=1)
    monkeypatch.setenv("PORTO_SEED", "99")
    assert _config.resolve(config, {}).deterministic == porto.DeterministicConfig(seed=99)
    assert _config.resolve(None, {}).deterministic is None


@pytest.mark.parametrize("seed", ["x", "-1"])
def test_an_invalid_porto_seed_says_what_to_set(monkeypatch: pytest.MonkeyPatch, seed: str) -> None:
    monkeypatch.setenv("PORTO_SEED", seed)
    config = porto.RuntimeConfig(deterministic=porto.DeterministicConfig())
    with pytest.raises(ValueError, match=f"PORTO_SEED={seed!r} is not valid: .* set PORTO_SEED=0"):
        _config.resolve(config, {})


@pytest.mark.parametrize(
    ("fields", "error", "fragment"),
    [
        ({"seed": -1}, ValueError, "pass seed=0"),
        ({"seed": "1"}, TypeError, "expected an int, got str"),
        ({"clock": "wall"}, ValueError, "pass clock='virtual'"),
        ({"autojump": None}, TypeError, "expected a bool"),
        ({"max_steps": 0}, ValueError, "or None for no limit; pass max_steps=10_000_000"),
    ],
)
def test_invalid_deterministic_fields_say_what_to_pass(
    fields: dict[str, Any], error: type[Exception], fragment: str
) -> None:
    with pytest.raises(error, match="porto: DeterministicConfig") as caught:
        porto.DeterministicConfig(**fields)
    assert fragment in str(caught.value)


# lanes="auto"

AUTO_LANES = [
    # (gil_build, physical cores, lanes)
    (True, 1, 1),
    (True, 2, 2),
    (True, 64, 2),
    (False, 1, 1),
    (False, 12, 12),
    (False, 64, 64),
    (False, 128, 64),
]


@pytest.mark.parametrize(("gil_build", "cores", "expected"), AUTO_LANES)
def test_auto_lanes(gil_build: bool, cores: int, expected: int) -> None:
    assert _config.resolve_lanes("auto", gil_build=gil_build, cores=cores) == expected


def test_a_lane_count_is_kept() -> None:
    assert _config.resolve_lanes(5, gil_build=True, cores=1) == 5


def test_auto_lanes_on_this_interpreter() -> None:
    gil_build = not sysconfig.get_config_var("Py_GIL_DISABLED")
    assert _config.is_gil_build() == gil_build
    assert 1 <= _config.resolve_lanes("auto") <= (2 if gil_build else 64)


# debug


def test_dev_mode_turns_debug_on() -> None:
    env = {name: value for name, value in os.environ.items() if not name.startswith("PORTO_")}
    script = "import porto; print(porto.RuntimeConfig().debug)"
    for flags, expected in [([], "False"), (["-X", "dev"], "True")]:
        output = subprocess.run(
            [sys.executable, *flags, "-c", script],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        assert output == expected


def test_run_refuses_a_coop_budget_the_runtime_cannot_hold() -> None:
    async def main() -> None:
        pass

    with pytest.raises(ValueError, match="coop_budget must be at most 4294967295"):
        porto.run(main, coop_budget=2**32)
