"""Running porto: ``porto.run``, ``RuntimeConfig`` and the ``PORTO_*`` environment variables.

    uv run python examples/run_and_config.py

A setting comes from, highest priority first: a keyword override to ``porto.run``, the
``config=`` object, the ``PORTO_<FIELD>`` variable, the default. Every value is checked before
``main`` runs, and an invalid one raises an error that names a valid value.
"""

import os
from collections.abc import Generator
from contextlib import contextmanager

import porto


async def greet(name: str, times: int) -> str:
    await porto.sleep(0.001)
    return ", ".join([f"hello {name}"] * times)


async def elapsed_after(seconds: float) -> float:
    started = porto.current_time()
    await porto.sleep(seconds)
    return porto.current_time() - started


@contextmanager
def environment(name: str, value: str) -> Generator[None]:
    """Sets an environment variable for the duration of the block."""
    previous = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if previous is None:
            del os.environ[name]
        else:
            os.environ[name] = previous


def run_with_arguments() -> None:
    # An async function and its arguments; run returns what main returns.
    assert porto.run(greet, "porto", 2) == "hello porto, hello porto"
    # Or an awaitable (here a coroutine object).
    assert porto.run(greet("douro", 1)) == "hello douro"
    # The runtime clock starts with the runtime; sleeps are measured on it.
    assert porto.run(elapsed_after, 0.01) >= 0.01


def config_precedence() -> None:
    with environment("PORTO_COOP_BUDGET", "32"):
        # A field left out reads PORTO_<FIELD> when the config is created...
        assert porto.RuntimeConfig().coop_budget == 32
        # ...and a field given explicitly wins over the variable.
        assert porto.RuntimeConfig(coop_budget=64).coop_budget == 64
    assert porto.RuntimeConfig().coop_budget == 128  # the default

    config = porto.RuntimeConfig(coop_budget=64, timer_resolution=0.0005)
    # Keyword overrides to run win over config=, which wins over the environment.
    assert porto.run(greet, "config", 1, config=config, coop_budget=16) == "hello config"


def invalid_settings_fail_early() -> None:
    calls: list[str] = []

    async def main() -> None:
        calls.append("main ran")

    try:
        porto.run(main, lanes=0)
    except ValueError as error:
        assert "lanes=0" in str(error) and "pass lanes=" in str(error), error
    else:
        raise AssertionError("lanes=0 was accepted")
    assert calls == [], "main must not run with an invalid config"

    with environment("PORTO_COOP_BUDGET", "lots"):
        try:
            porto.RuntimeConfig()
        except ValueError as error:
            # The message names the variable and a value that works.
            assert "PORTO_COOP_BUDGET='lots'" in str(error), error
        else:
            raise AssertionError("PORTO_COOP_BUDGET=lots was accepted")


def runtime_only_apis() -> None:
    # The clock belongs to the runtime: outside porto.run it raises NoRuntimeError, which is
    # also a RuntimeError.
    try:
        porto.current_time()
    except porto.NoRuntimeError as error:
        assert "time.monotonic()" in str(error), error
    else:
        raise AssertionError("current_time() worked outside porto.run")

    async def nested() -> None:
        porto.run(greet, "inner", 1)

    # porto.run does not nest: inside a runtime, await the coroutine instead.
    try:
        porto.run(nested)
    except RuntimeError:
        pass
    else:
        raise AssertionError("porto.run nested inside porto.run")


def main() -> None:
    run_with_arguments()
    config_precedence()
    invalid_settings_fail_early()
    runtime_only_apis()
    print("run_and_config: ok")


if __name__ == "__main__":
    main()
