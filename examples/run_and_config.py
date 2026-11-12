"""Running porto: ``porto.run``, ``RuntimeConfig`` and the ``PORTO_*`` environment variables.

    uv run python examples/run_and_config.py

A setting comes from, highest priority first: a keyword override to ``porto.run``, the
``config=`` object, the ``PORTO_<FIELD>`` variable, the default. Every value is checked before
``main`` runs, and an invalid one raises an error that names a valid value.

``main`` is the first task of the runtime's root scope. When it ends, ``porto.run`` cancels what
is left in that scope and waits up to ``cancel_timeout`` seconds (default 5) for it; a task that
ignores the cancel is abandoned, with a ``RuntimeWarning`` that counts them. Spawning into the
root scope needs ``Runtime.root_scope``, which is not public yet (porto-szb.20): until then
nothing a program does leaves tasks there, and this example shows the setting and what ``run``
raises (porto-szb.136 tracks showing the drain itself).
"""

import os
import warnings
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

    # cancel_timeout bounds how long run waits for the root scope's tasks after main ended.
    assert porto.RuntimeConfig().cancel_timeout == 5.0
    with environment("PORTO_CANCEL_TIMEOUT", "0.5"):
        assert porto.RuntimeConfig().cancel_timeout == 0.5
    assert porto.RuntimeConfig(cancel_timeout=1).cancel_timeout == 1

    config = porto.RuntimeConfig(coop_budget=64, timer_resolution=0.0005)
    # Keyword overrides to run win over config=, which wins over the environment.
    assert porto.run(greet, "config", 1, config=config, coop_budget=16) == "hello config"


def run_raises_what_main_raises() -> None:
    """``porto.run`` raises what ``main`` raised, as it is: no wrapper, no group."""

    async def failing() -> None:
        await porto.sleep(0.001)
        raise KeyError("no such user")

    try:
        porto.run(failing)
    except KeyError as error:
        assert error.args == ("no such user",)
    else:
        raise AssertionError("porto.run swallowed main's exception")

    # A scope inside main reports its children's failures as a group; run passes it through.
    async def group() -> None:
        async with porto.scope() as s:
            s.spawn(failing)

    try:
        porto.run(group)
    except ExceptionGroup as error:
        assert [type(e) for e in error.exceptions] == [KeyError]
    else:
        raise AssertionError("porto.run swallowed the group")


def warnings_do_not_stop_the_run() -> None:
    """An unknown ``PORTO_*`` variable is ignored, with a RuntimeWarning listing the valid ones."""
    with environment("PORTO_COOP_BUGDET", "64"), warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")  # record it here, so it does not reach stderr
        assert porto.run(greet, "typo", 1) == "hello typo"
    messages = [str(warning.message) for warning in caught]
    assert any(
        "PORTO_COOP_BUGDET" in message and "PORTO_COOP_BUDGET" in message for message in messages
    )


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

    # Multi-lane scheduling is not implemented yet: more than one lane is refused, not
    # silently run on one. lanes="auto" lets porto choose, and runs on one lane for now.
    try:
        porto.run(main, lanes=4)
    except NotImplementedError as error:
        assert "pass lanes=1" in str(error), error
    else:
        raise AssertionError("lanes=4 was accepted")
    assert calls == [], "main must not run with an unsupported config"

    # A negative cancel_timeout cannot work either; the message says what to pass.
    try:
        porto.RuntimeConfig(cancel_timeout=-1)
    except ValueError as error:
        assert "pass cancel_timeout=5" in str(error), error
    else:
        raise AssertionError("cancel_timeout=-1 was accepted")

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
        # The refused coroutine is closed: no "coroutine ... was never awaited" follows.
        porto.run(greet("inner", 1))

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
    run_raises_what_main_raises()
    warnings_do_not_stop_the_run()
    invalid_settings_fail_early()
    runtime_only_apis()
    print("run_and_config: ok")


if __name__ == "__main__":
    main()
