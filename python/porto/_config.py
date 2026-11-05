"""``RuntimeConfig``: the runtime's settings, and where they come from.

Precedence, highest first (26-errors-config-lifecycle.md §1): the keyword overrides of
``porto.run(main, **overrides)``, the fields given to ``RuntimeConfig(...)``, the ``PORTO_*``
environment variables, the defaults. A field left out of ``RuntimeConfig(...)`` reads its
``PORTO_<FIELD>`` variable when the config is created, so ``RuntimeConfig()`` alone is the
environment over the defaults.

The fields and defaults are in 20-python-api-reference.md (RuntimeConfig); porto_core's
``config`` module mirrors them for Rust.
"""

from __future__ import annotations

import dataclasses
import difflib
import math
import os
import sys
import sysconfig
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal, TypeVar, cast, get_args

from porto import _core

IoBackend = Literal["auto", "epoll", "kqueue", "iocp", "io_uring"]
Steal = Literal["adaptive", "off", "eager"]
PythonSteal = Literal["adaptive", "off"]
AnyioBackend = Literal["native", "facade", "off"]
DeterministicClock = Literal["virtual", "real"]

ENV_PREFIX: Final = "PORTO_"
#: Overrides ``DeterministicConfig.seed`` (24-deterministic-testing.md §2); not a field.
SEED_ENV: Final = "PORTO_SEED"

#: The finest and coarsest ``timer_resolution``, in seconds (porto_core's
#: ``MIN_TIMER_RESOLUTION`` and ``MAX_TIMER_RESOLUTION``).
MIN_TIMER_RESOLUTION: Final = 0.0001
MAX_TIMER_RESOLUTION: Final = 0.01

_T = TypeVar("_T")


class _Invalid(Exception):
    """A value out of range: ``expected`` says what is accepted, ``example`` is a valid value."""

    def __init__(self, expected: str, example: str) -> None:
        super().__init__(expected)
        self.expected = expected
        self.example = example


@dataclass(frozen=True, slots=True)
class _Kind:
    """How a field's values are checked, and parsed from the environment.

    ``check`` returns the value to store, or raises ``TypeError`` (wrong type) or ``_Invalid``
    (out of range). ``parse`` turns an environment string into a value for ``check``.
    """

    check: Callable[[object], object]
    parse: Callable[[str], object]


# Checks


def _type_name(value: object) -> str:
    return type(value).__name__


def _int_at_least_one(example: str) -> Callable[[object], object]:
    def check(value: object) -> object:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"expected an int, got {_type_name(value)}")
        if value < 1:
            raise _Invalid("an integer >= 1", example)
        return value

    return check


def _seconds(*, low: float, high: float = math.inf, example: str) -> Callable[[object], object]:
    expected = (
        f"a number of seconds between {low} and {high}"
        if high != math.inf
        else f"a finite number of seconds >= {low}"
    )

    def check(value: object) -> object:
        if not isinstance(value, int | float) or isinstance(value, bool):
            raise TypeError(f"expected a number of seconds, got {_type_name(value)}")
        if not math.isfinite(value) or not low <= value <= high:
            raise _Invalid(expected, example)
        return float(value)

    return check


def _positive_seconds_or_none(value: object) -> object:
    if value is None:
        return None
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise TypeError(f"expected a number of seconds or None, got {_type_name(value)}")
    if not math.isfinite(value) or value <= 0:
        raise _Invalid("a positive, finite number of seconds, or None to turn it off", "0.5")
    return float(value)


def _check_bool(value: object) -> object:
    if not isinstance(value, bool):
        raise TypeError(f"expected a bool, got {_type_name(value)}")
    return value


def _choice(names: tuple[str, ...]) -> Callable[[object], object]:
    def check(value: object) -> object:
        if not isinstance(value, str):
            raise TypeError(f"expected one of {_quoted(names)}, got {_type_name(value)}")
        if value not in names:
            raise _Invalid(f"one of {_quoted(names)}", names[0])
        return value

    return check


def _check_lanes(value: object) -> object:
    if value == "auto":
        return value
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"expected an int or 'auto', got {_type_name(value)}")
    if value < 1:
        raise _Invalid("an integer >= 1, or 'auto'", "4")
    return value


def _check_console(value: object) -> object:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"expected a 'host:port' string or None, got {_type_name(value)}")
    host, _, port = value.rpartition(":")
    if not host or not port.isdigit() or int(port) > 65535:
        raise _Invalid("a 'host:port' address, or None for no console", "127.0.0.1:6669")
    return value


def _quoted(names: tuple[str, ...]) -> str:
    return ", ".join(repr(name) for name in names)


def _choices(literal: object) -> tuple[str, ...]:
    return tuple(str(name) for name in get_args(literal))


# Parsers (26 §1: ints, floats, bools as 1/0/true/false/yes/no, enums by lowercase name)

_TRUE: Final = frozenset({"1", "true", "yes"})
_FALSE: Final = frozenset({"0", "false", "no"})


def _parse_int(text: str) -> object:
    try:
        return int(text)
    except ValueError:
        raise _Invalid("an integer", "4") from None


def _parse_float(text: str) -> object:
    try:
        return float(text)
    except ValueError:
        raise _Invalid("a number of seconds", "0.5") from None


def _parse_bool(text: str) -> object:
    lowered = text.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise _Invalid("one of 1, 0, true, false, yes, no", "1")


def _parse_name(text: str) -> object:
    return text.lower()


def _parse_lanes(text: str) -> object:
    if text.lower() == "auto":
        return "auto"
    try:
        return int(text)
    except ValueError:
        raise _Invalid("an integer >= 1, or auto", "4") from None


def _parse_optional_float(text: str) -> object:
    return None if text.lower() == "none" else _parse_float(text)


def _parse_optional_str(text: str) -> object:
    return None if text.lower() == "none" else text


_BOOL: Final = _Kind(_check_bool, _parse_bool)

#: Every field that has an environment variable, and its kind. ``deterministic`` has none: it
#: is set in code (``PORTO_SEED`` overrides its seed).
_KINDS: Final[Mapping[str, _Kind]] = {
    "lanes": _Kind(_check_lanes, _parse_lanes),
    "io_backend": _Kind(_choice(_choices(IoBackend)), _parse_name),
    "io_backend_strict": _BOOL,
    "steal": _Kind(_choice(_choices(Steal)), _parse_name),
    "python_steal": _Kind(_choice(_choices(PythonSteal)), _parse_name),
    "python_steal_threshold": _Kind(_int_at_least_one("8"), _parse_int),
    "coop_budget": _Kind(_int_at_least_one("128"), _parse_int),
    "blocking_threads": _Kind(_int_at_least_one("64"), _parse_int),
    "blocking_keepalive": _Kind(_seconds(low=0, example="10"), _parse_float),
    "timer_resolution": _Kind(
        _seconds(low=MIN_TIMER_RESOLUTION, high=MAX_TIMER_RESOLUTION, example="0.001"),
        _parse_float,
    ),
    "asyncio_compat": _BOOL,
    "anyio_backend": _Kind(_choice(_choices(AnyioBackend)), _parse_name),
    "handle_signals": _BOOL,
    "debug": _BOOL,
    "slow_step_threshold": _Kind(_positive_seconds_or_none, _parse_optional_float),
    "console": _Kind(_check_console, _parse_optional_str),
    "metrics": _BOOL,
    "graceful_timeout": _Kind(_seconds(low=0, example="10"), _parse_float),
    "cancel_timeout": _Kind(_seconds(low=0, example="5"), _parse_float),
}


def env_name(field_name: str) -> str:
    """The environment variable of a field: ``lanes`` -> ``PORTO_LANES``."""
    return ENV_PREFIX + field_name.upper()


#: Every ``PORTO_*`` variable porto reads.
ENV_NAMES: Final = frozenset({*(env_name(name) for name in _KINDS), SEED_ENV})


def _from_env(field_name: str, default: Callable[[], _T]) -> Callable[[], _T]:
    """A ``default_factory``: the field's ``PORTO_*`` variable if set, else ``default()``."""

    def factory() -> _T:
        variable = env_name(field_name)
        text = os.environ.get(variable)
        if text is None:
            return default()
        kind = _KINDS[field_name]
        try:
            value = kind.check(kind.parse(text.strip()))
        except (_Invalid, TypeError) as error:
            raise _env_error(variable, text, error) from None
        return cast("_T", value)

    return factory


def _env_error(variable: str, text: str, error: _Invalid | TypeError) -> ValueError:
    if isinstance(error, _Invalid):
        expected, example = error.expected, error.example
    else:
        # A parser hands `check` a value of the field's type, so this is a bug if it happens.
        expected, example = str(error), "a valid value"
    return ValueError(
        f"porto: {variable}={text!r} is not valid: expected {expected}; set "
        f"{variable}={example}, or unset it for the default"
    )


def _dev_mode() -> bool:
    return bool(sys.flags.dev_mode)


def _check_field(owner: str, name: str, value: object, check: Callable[[object], object]) -> object:
    """``check(value)``, with errors that name ``owner``'s field and a valid value."""
    try:
        return check(value)
    except TypeError as error:
        raise TypeError(f"porto: {owner} {name}={value!r} is not valid: {error}") from None
    except _Invalid as error:
        raise ValueError(
            f"porto: {owner} {name}={value!r} is not valid: expected {error.expected}; pass "
            f"{name}={_example_literal(error.example)}"
        ) from None


def _example_literal(example: str) -> str:
    try:
        float(example)
    except ValueError:
        return repr(example)
    return example


def _check_seed(value: object) -> object:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"expected an int, got {_type_name(value)}")
    if value < 0:
        raise _Invalid("an integer >= 0", "0")
    return value


def _check_max_steps(value: object) -> object:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"expected an int or None, got {_type_name(value)}")
    if value < 1:
        raise _Invalid("an integer >= 1, or None for no limit", "10_000_000")
    return value


_DETERMINISTIC_CHECKS: Final[Mapping[str, Callable[[object], object]]] = {
    "seed": _check_seed,
    "clock": _choice(_choices(DeterministicClock)),
    "autojump": _check_bool,
    "max_steps": _check_max_steps,
}


@dataclass(frozen=True, slots=True, kw_only=True)
class DeterministicConfig:
    """Deterministic mode (24-deterministic-testing.md §2): one lane, a seeded scheduler and a
    virtual clock. ``PORTO_SEED`` overrides ``seed``.

    Stored and checked; the runtime does not run deterministic mode yet.
    """

    seed: int = 0
    clock: DeterministicClock = "virtual"
    autojump: bool = True
    max_steps: int | None = 10_000_000

    def __post_init__(self) -> None:
        for name, check in _DETERMINISTIC_CHECKS.items():
            _check_field("DeterministicConfig", name, getattr(self, name), check)


@dataclass(frozen=True, slots=True, kw_only=True)
class RuntimeConfig:
    """The runtime's settings (20-python-api-reference.md, RuntimeConfig).

    A field left out reads ``PORTO_<FIELD>`` (``PORTO_LANES=4``) when the config is created,
    else takes its default. ``porto.run(main, config=..., **overrides)`` overrides fields
    again. Invalid values raise ``ValueError`` (``TypeError`` for a wrong type) naming the
    field, or the variable, and a valid value.
    """

    lanes: int | Literal["auto"] = field(default_factory=_from_env("lanes", lambda: 1))
    io_backend: IoBackend = field(default_factory=_from_env("io_backend", lambda: "auto"))
    io_backend_strict: bool = field(default_factory=_from_env("io_backend_strict", lambda: False))
    steal: Steal = field(default_factory=_from_env("steal", lambda: "adaptive"))
    python_steal: PythonSteal = field(default_factory=_from_env("python_steal", lambda: "adaptive"))
    python_steal_threshold: int = field(
        default_factory=_from_env("python_steal_threshold", lambda: 8)
    )
    coop_budget: int = field(default_factory=_from_env("coop_budget", lambda: 128))
    blocking_threads: int = field(default_factory=_from_env("blocking_threads", lambda: 512))
    blocking_keepalive: float = field(default_factory=_from_env("blocking_keepalive", lambda: 10.0))
    timer_resolution: float = field(default_factory=_from_env("timer_resolution", lambda: 0.001))
    asyncio_compat: bool = field(default_factory=_from_env("asyncio_compat", lambda: True))
    anyio_backend: AnyioBackend = field(
        default_factory=_from_env("anyio_backend", lambda: "native")
    )
    handle_signals: bool = field(default_factory=_from_env("handle_signals", lambda: True))
    debug: bool = field(default_factory=_from_env("debug", _dev_mode))
    slow_step_threshold: float | None = field(
        default_factory=_from_env("slow_step_threshold", lambda: 0.1)
    )
    console: str | None = field(default_factory=_from_env("console", lambda: None))
    metrics: bool = field(default_factory=_from_env("metrics", lambda: True))
    deterministic: DeterministicConfig | None = None
    graceful_timeout: float = field(default_factory=_from_env("graceful_timeout", lambda: 10.0))
    cancel_timeout: float = field(default_factory=_from_env("cancel_timeout", lambda: 5.0))

    def __post_init__(self) -> None:
        for name, kind in _KINDS.items():
            value = _check_field("RuntimeConfig", name, getattr(self, name), kind.check)
            object.__setattr__(self, name, value)
        deterministic = cast(object, self.deterministic)
        if deterministic is not None and not isinstance(deterministic, DeterministicConfig):
            raise TypeError(
                f"porto: RuntimeConfig deterministic={deterministic!r} is not valid: expected "
                "a porto.DeterministicConfig or None"
            )


FIELD_NAMES: Final = tuple(f.name for f in dataclasses.fields(RuntimeConfig))


def warn_unknown_env(environ: Mapping[str, str] | None = None) -> None:
    """Warns about each ``PORTO_*`` variable porto does not read: a typo's guard (26 §1)."""
    environ = os.environ if environ is None else environ
    for variable in sorted(environ):
        if not variable.startswith(ENV_PREFIX) or variable in ENV_NAMES:
            continue
        # Compared without the prefix every name shares, which would make anything look close.
        suffixes = [name.removeprefix(ENV_PREFIX) for name in ENV_NAMES]
        close = difflib.get_close_matches(variable.removeprefix(ENV_PREFIX), suffixes, n=1)
        hint = f"did you mean {ENV_PREFIX}{close[0]}? " if close else ""
        warnings.warn(
            f"porto: ignoring the unknown environment variable {variable}; {hint}"
            f"valid names: {', '.join(sorted(ENV_NAMES))}",
            RuntimeWarning,
            # warn_unknown_env < resolve < porto.run's caller (porto.run is native: no frame).
            stacklevel=3,
        )


def resolve(config: RuntimeConfig | None, overrides: Mapping[str, Any]) -> RuntimeConfig:
    """The configuration ``porto.run(main, config=config, **overrides)`` runs with."""
    warn_unknown_env()
    given = cast(object, config)
    if given is None:
        config = RuntimeConfig()
    elif not isinstance(given, RuntimeConfig):
        raise TypeError(
            f"porto: config must be a porto.RuntimeConfig or None, got {_type_name(given)}; "
            "pass config=porto.RuntimeConfig(...), or the fields as keywords: "
            "porto.run(main, lanes=4)"
        )
    else:
        config = given
    if overrides:
        unknown = sorted(set(overrides) - set(FIELD_NAMES))
        if unknown:
            raise TypeError(
                f"porto: unknown configuration field(s) {', '.join(unknown)}; valid names: "
                f"{', '.join(FIELD_NAMES)}"
            )
        config = dataclasses.replace(config, **overrides)
    return _with_env_seed(config)


def _with_env_seed(config: RuntimeConfig) -> RuntimeConfig:
    """``config`` with ``PORTO_SEED`` as its deterministic seed, when both are set."""
    seed = os.environ.get(SEED_ENV)
    if seed is None or config.deterministic is None:
        return config
    try:
        value = int(seed.strip())
        deterministic = dataclasses.replace(config.deterministic, seed=value)
    except ValueError:
        raise ValueError(
            f"porto: {SEED_ENV}={seed!r} is not valid: expected an integer >= 0; set "
            f"{SEED_ENV}=0, or unset it"
        ) from None
    return dataclasses.replace(config, deterministic=deterministic)


def is_gil_build() -> bool:
    """Whether this interpreter is a GIL build (not free-threaded)."""
    return not sysconfig.get_config_var("Py_GIL_DISABLED")


def resolve_lanes(
    lanes: int | Literal["auto"], *, gil_build: bool | None = None, cores: int | None = None
) -> int:
    """The lane count ``lanes`` means here (12-scheduler-and-lanes.md §7): ``'auto'`` is the
    physical core count capped at 64, and at most 2 on a GIL build. ``gil_build`` and
    ``cores`` default to this interpreter and machine."""
    if lanes != "auto":
        return lanes
    gil_build = is_gil_build() if gil_build is None else gil_build
    cores = _core._physical_cores() if cores is None else cores  # pyright: ignore[reportPrivateUsage]
    return _core._auto_lanes(gil_build, cores)  # pyright: ignore[reportPrivateUsage]
